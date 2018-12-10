// Author(s): Michael Kaufmann <kau@zurich.ibm.com>
//            Adrian Schuepbach <dri@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "server/server.h"
#include "app/app.h"
#include "common.h"
#include "event/engine.h"
#include "resourcemanager/executor.h"
#include "resourcemanager/resourcemanager.h"
#include "scheduler/heteroscheduler.h"

#include <cerrno>
#include <chrono>
#include <exception>
#include <iostream>
#include <map>
#include <regex>
#include <sstream>

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

using boost::property_tree::ptree;

Server::Server(Engine *engine, Cluster *cluster, ResourceManager *resourcemanager)
{
    assert(engine != NULL);
    assert(cluster != NULL);
    assert(resourcemanager != NULL);

    this->cluster(cluster);

    engine->listen(0, EventType::Kill, std::bind(&Server::event, this, std::placeholders::_1));
}

void Server::run(Server *server)
{
    assert(server != NULL);
    server->run();
}

void Server::event(Event *ev)
{
    switch (ev->type()) {
    case EventType::Kill:
        if (this->_server) {
            logCInfo(LC_API) << "Shutting down REST server...";
            // FIXME: This function doesn't really work. It doesn't seem to close open connections!
            this->_server->stop();
        }
        break;
    default:
        break;
    }
}

ptree Server::parse(const served::request &req)
{
    ptree             data;
    std::stringstream ss;
    ss << req.body();
    read_json(ss, data);
    return data;
}

void Server::appsHandler(served::response &res, const served::request &req)
{
    try {
        logCInfo(LC_API) << "Request URL  : " << req.url().path();
        logCInfo(LC_API) << "Request Data : " << req.body();

        AppId appId = static_cast<AppId>(req.params["appId"]);

        ptree       data  = parse(req);
        std::string event = data.get<std::string>("event", "unknown");

        // Application start event (add new application)
        if (event == "start") {
            long        timestamp = data.get<long>("timestamp");
            std::string appName   = data.get<std::string>("name");
            std::string driverUrl = data.get<std::string>("driverUrl");

            if (engine->application(appId)) {
                logError << "Not adding application " << appId << " (" << appName
                         << ") because it already exists";
                res.set_status(served::status_4XX::CONFLICT);
                return;
            }

            logCInfo(LC_API) << "Adding application " << appId << " (" << appName << ")";

            App *app = new App(appId, appName, driverUrl);
            engine->application(app);

            app->scheduler(new HeteroScheduler(app, resourcemanager));
            engine->signal(new ApplicationSubmittedEvent(app));

            this->schedulers.emplace(app, new std::thread(&SchedulerBase::run, app->scheduler()));
            //	    app->scheduler()->start();

            logCInfo(LC_API) << "Application " << appId << " added";

            res.set_status(served::status_2XX::OK);
            return;
        } else if (event == "end") { // Application finish event
            App *app = engine->application(appId);
            if (!app) {
                logError << "Application " << appId << " not found";
                res.set_status(served::status_4XX::NOT_FOUND);
                return;
            }

            long timestamp = data.get<long>("timestamp");
            logCInfo(LC_API) << "Application " << appId << " finished at " << Duration(timestamp) << ".";
            engine->signal(new ApplicationFinishedEvent(app));
            res.set_status(served::status_2XX::OK);
            return;
        } else {
            res << "Unkown event " << event;
            res.set_status(served::status_4XX::BAD_REQUEST);
            return;
        }
    } catch (std::exception const &e) {
        logError << e.what();
        res.set_status(served::status_4XX::BAD_REQUEST);
        return;
    }
}

void Server::jobsHandler(served::response &res, const served::request &req)
{
    try {
        logCInfo(LC_API) << "Request URL  : " << req.url().path();
        logCInfo(LC_API) << "Request Data : " << req.body();

        AppId appId = static_cast<AppId>(req.params["appId"]);
        App * app   = engine->application(appId);
	// FIXME: Workaround for improper locking/signalling
        for (int i = 0; i < 100 && !app; i++) {
            logCWarn(LC_API) << "Application " << appId << " not found...retrying (" << i << ")";
	    std::this_thread::sleep_for(std::chrono::milliseconds(100));
            app = engine->application(appId);
        }
        if (!app) {
            logError << "Application " << appId << " not found";
            res.set_status(served::status_4XX::NOT_FOUND);
            return;
        }

        int jobId = stoi(req.params["jobId"]);

        ptree       data  = parse(req);
        std::string event = data.get<std::string>("event", "unknown");

        if (event == "submit") {
            logCDebug(LC_API) << "Job " << jobId << " submitted (append application)";

            const ptree &stageData = data.get_child("stages");

            std::map<Stage *, std::list<StageId>> stages;

            for (auto &stage : stageData) {
                std::string stageId       = "s" + stage.second.get<std::string>("id", "-1");
                StageId     stageNid      = stage.second.get<StageId>("id", static_cast<StageId>(-1));
                TaskType    stageType     = stage.second.get<TaskType>("type", TaskType::unknown);
                std::string stageFunction = stage.second.get<std::string>("function", "unknown");
                StageKey    stageKey      = stage.second.get<StageKey>("key", static_cast<StageKey>(0));
                int         stageSize     = stage.second.get<int>("size", 0);

                Stage *tmp = new Stage(stageId, stageNid, stageFunction, stageKey, app, stageType, stageSize);

                if (config->getFlag(ConfigSchedulerFlags::consider_io_size)) {
                    if (tmp->type() == TaskType::load) {
                        size_t tidx   = 0;
                        auto   inputs = stage.second.get_child_optional("input");
                        if (inputs) {
                            for (const ptree::value_type &input : inputs.get()) {
                                Task *task = tmp->task(tidx);
                                for (const ptree::value_type &l0 : input.second) {
                                    for (const ptree::value_type &l1 : l0.second) {
                                        Node * node   = this->cluster()->node(l1.first.data());
                                        size_t nBytes = std::stol(l1.second.data());

                                        if (!node) {
                                            logError << "Node " << l1.first.data() << " node found!";
                                            continue;
                                        }

                                        task->metrics().inData(node, nBytes);
                                        tmp->metrics().inData(nBytes);
                                    }
                                }
                                tidx++;
                            }
                        }
                    }
                }

                stages.emplace(tmp, std::list<StageId>());

                for (const ptree::value_type &parentId : stage.second.get_child("parents")) {
                    StageId sid = static_cast<StageId>(stoi(parentId.second.data()));
                    stages[tmp].push_back(sid);
                }
            }

            //            app->lock(0);
            engine->signal(new StageAddedEvent(app, stages));

            res.set_status(served::status_2XX::OK);
            return;
        } else if (event == "end") {
            logCDebug(LC_API) << "Job " << jobId << " ended (ignored)";
            res.set_status(served::status_2XX::OK);
            return;
        } else {
            res.set_status(served::status_4XX::BAD_REQUEST);
            return;
        }
    } catch (std::exception const &e) {
        logError << e.what();
        res.set_status(served::status_4XX::BAD_REQUEST);
        return;
    }
}

void Server::stagesHandler(served::response &res, const served::request &req)
{
    try {
        logCInfo(LC_API) << "Request URL  : " << req.url().path();
        logCInfo(LC_API) << "Request Data : " << req.body();

        AppId appId = static_cast<AppId>(req.params["appId"]);
        App * app   = engine->application(appId);
        if (!app) {
            logError << "Application " << appId << " not found";
            res.set_status(served::status_4XX::NOT_FOUND);
            return;
        }
        StageId sid   = static_cast<StageId>(stoi(req.params["stageId"]));
        Stage * stage = app->stage(sid);

        // 	while (stage == NULL) {
        // //	    auto guard = app->guard(0);
        // 	    stage = app->stage(sid);
        // 	}

        if (!stage) {
            logError << "Stage s" << sid << " of application " << app->id() << " not found";
            res.set_status(served::status_4XX::NOT_FOUND);
            return;
        }

        ptree       data  = parse(req);
        std::string event = data.get<std::string>("event");

        if (event == "ready") {
            if (config->getFlag(ConfigSchedulerFlags::consider_io_size)) {
                auto inputs = data.get_child_optional("input");
                if (inputs) {
                    for (const ptree::value_type &input : inputs.get()) {
                        size_t tidx = input.second.get<size_t>("index", 0);
                        if (stage->size() > tidx) {
                            Task *task = stage->task(tidx);
                            for (const ptree::value_type &l0 : input.second.get_child("locs")) {
                                for (const ptree::value_type &l1 : l0.second) {
                                    int       eid      = std::stoi(l1.first.data()) / 1000;
                                    size_t    nBytes   = std::stol(l1.second.data());
                                    Executor *executor = resourcemanager->executor(appId, eid);

                                    if (!executor)
                                        continue;

                                    task->metrics().inData(executor, nBytes);
                                    stage->metrics().inData(nBytes);
                                }
                            }
                        }
                    }
                }
            }

            engine->signal(new StageReadyEvent(app, stage));
            res.set_status(served::status_2XX::OK);
            return;
        } else if (event == "finished") {
            engine->signal(new StageFinishedEvent(app, stage));
            res.set_status(served::status_2XX::OK);
            return;
        } else {
            res << "Unkown event " << event;
            res.set_status(served::status_4XX::BAD_REQUEST);
            return;
        }
    } catch (std::exception const &e) {
        logError << e.what();
        res.set_status(served::status_4XX::BAD_REQUEST);
        return;
    }
}

void Server::tasksHandler(served::response &res, const served::request &req)
{
    // FIXME: Technically we would need a task lock here. Maybe try to put the entire data into the
    //        event and let the scheduler do the update.

    try {
        logCInfo(LC_API) << "Request URL  : " << req.url().path();
        logCInfo(LC_API) << "Request Data : " << req.body();

        AppId appId = static_cast<AppId>(req.params["appId"]);
        App * app   = engine->application(appId);
        if (!app) {
            logError << "Application " << appId << " not found";
            res.set_status(served::status_4XX::NOT_FOUND);
            return;
        }

        StageId sid   = static_cast<StageId>(std::stoi(req.params["stageId"]));
        Stage * stage = app->stage(sid);
        if (!stage) {
            logError << "Stage s" << sid << " not found";
            res.set_status(served::status_4XX::NOT_FOUND);
            return;
        }

        int   taskId = stoi(req.params["taskId"]);
        Task *task   = stage->task(taskId);
        if (!task) {
            logError << "Task " << stage->id() << "t" << taskId << " not found";
            res.set_status(served::status_4XX::NOT_FOUND);
            return;
        }

        ptree       data  = parse(req);
        std::string event = data.get<std::string>("event");

        if (event == "end") {
            int       execId    = data.get<int>("executor", -1) / 1000;
            auto      guard     = task->guard(); // guard task->allocation()
            Executor *executor  = resourcemanager->executor(app->id(), execId);
            Time      timestamp = Time(data.get<uint64_t>("timestamp", 0UL));

            task->metrics().gbcTime(Duration(data.get<uint64_t>("gbcTime", 0UL)));
            task->metrics().cpuTime(Duration(data.get<uint64_t>("cpuTime", 0UL)));
            task->metrics().bytesRead(data.get<size_t>("bytesRead", 0UL));
            task->metrics().bytesWritten(data.get<size_t>("bytesWritten", 0UL));
            // task->metrics().heapSize(data.get<size_t>("heapSize", 0UL)/1024);
            // task->metrics().heapFree(data.get<size_t>("heapFree", 0UL)/1024);

            if (stage->prev().size() == 0) {
                if (task->metrics().bytesRead() != task->metrics().inData()) {
                    logCWarn(LC_API) << "Task " << task->id()
                                     << " input data prediction was wrong: " << task->metrics().inData()
                                     << " != " << task->metrics().bytesRead()
                                     << ". I don't know what to do with that information, though.";
                    task->metrics().inData(task->metrics().bytesRead(), true);
                }
            }

            logCInfo(LC_API) << "task " << task->id() << " finished at " << timestamp
                             << " (cpu: " << task->metrics().cpuTime() << " gbc: " << task->metrics().gbcTime()
                             << " bytes read/written: " << task->metrics().bytesRead() << "/"
                             << task->metrics().bytesWritten() << ")";

            if (!executor) {
                logError << "No executor for task " << task->id();
                res.set_status(served::status_4XX::BAD_REQUEST);
            } else {
                executor->done();

                engine->signal(new TaskFinishedEvent(app, task, executor));

                // Give executor new job
                logCInfo(LC_API) << "Executor " << executor->id() << " on host "
                                 << executor->worker()->node()->id() << " is idle";

                guard.unlock();
                Time        t0   = Time::now();
                const Task *task = executor->get(); // blocking call
                Time        t1   = Time::now();

                if (task->type() == TaskType::idle) {
                    res << "{\"event\" : \"idle\""
                        << ", \"duration\" : " << std::to_string(0) << "}";
                    logCInfo(LC_API) << "Telling " << executor->id() << " to do maintenance";
                } else if (task->type() == TaskType::disconnect) {
                    res << "{\"event\" : \"disconnect\""
                        << ", \"duration\" : " << std::to_string(0) << "}";
                    logCInfo(LC_API) << "Telling " << executor->id() << " to disconnect from its master";
                    // Request master can overtake disconnect end if we
                    // send it only once the disconnect task came back,
                    // because it comes directly from the workers on the
                    // remote nodes and doesn't go via the drvier, as 'disconnect emd' does.

                    for (Resource *resource : executor->resources()) {
                        Task *task = resource->disconnectTask();
                        executor->done();
                        engine->signal(new TaskFinishedEvent(app, task, executor));
                    }
                } else {
                    res << "{\"event\" : \"execute\""
                        << ", \"task\" : {\"stageId\" : " << std::to_string(task->stage()->nid())
                        << ", \"taskId\" : " << std::to_string(task->index()) << "}}";
                }

                logCInfo(LC_API) << "Executor " << executor->id() << " on host "
                                 << executor->worker()->node()->id() << " got task " << task->id() << " after "
                                 << (t1 - t0);

                res.set_status(served::status_2XX::OK);
                return;
            }
        } else {
            res << "Unkown event " << event;
            res.set_status(served::status_4XX::BAD_REQUEST);
            return;
        }
    } catch (std::exception const &e) {
        logError << e.what();
        res.set_status(served::status_4XX::BAD_REQUEST);
        return;
    }
}

void Server::executorsHandler(served::response &res, const served::request &req)
{
    try {
        AppId appId = static_cast<AppId>(req.params["appId"]);
        App * app   = engine->application(appId);
        if (!app) {
            logError << "Application " << appId << " not found";
            res.set_status(served::status_4XX::NOT_FOUND);
            return;
        }

        int execId = stoi(req.params["execId"]) / 1000;
        logCInfo(LC_API) << "Request URL  : " << req.url().path();
        logCInfo(LC_API) << "Request Data : " << req.body();

        ptree       data  = parse(req);
        std::string event = data.get<std::string>("event", "unknown");

        if (event == "register") {
            int cores = data.get<int>("cores", 0);
            // std::string host  = data.get<std::string>("host", "unknown");

            Worker *worker = resourcemanager->worker(static_cast<WorkerId>(execId));
            if (!worker) {
                logCInfo(LC_API) << "Cannot register new executor for application " << app->id()
                                 << " on because worker " << execId << " is unknown.";
                res.set_status(served::status_4XX::BAD_REQUEST);
                return;
            } else if (worker->app() != app) {
                logCInfo(LC_API) << "Cannot register new executor for application " << app->id()
                                 << " on because worker " << worker->id()
                                 << " is assigned to another application" << worker->app()->id();
                res.set_status(served::status_4XX::BAD_REQUEST);
                return;
            } else {
                Executor *executor = worker->executor();
                logCInfo(LC_API) << "Registering new executor on worker " << execId << " for application "
                                 << worker->app()->id();

                engine->signal(new ExecutorAddedEvent(app, executor));

                // Get that executor something to work.
                Time        t0   = Time::now();
                const Task *task = executor->get(); // blocking call
                Time        t1   = Time::now();

                if (task->type() == TaskType::idle) {
                    res << "{\"event\" : \"idle\""
                        << ", \"duration\" : " << std::to_string(0) << "}";
                } else if (task->type() == TaskType::disconnect) {
                    res << "{\"event\" : \"disconnect\""
                        << ", \"duration\" : " << std::to_string(0) << "}";
                    // Request master can overtake disconnect end if we
                    // send it only once the disconnect task came back,
                    // because it comes directly from the workers on the
                    // remote nodes and doesn't go via the drvier, as 'disconnect emd' does.

                    for (Resource *resource : executor->resources()) {
                        Task *task = resource->disconnectTask();
                        executor->done();
                        engine->signal(new TaskFinishedEvent(app, task, executor));
                    }
                } else {
                    res << "{\"event\" : \"execute\""
                        << ", \"task\" : {\"stageId\" : " << std::to_string(task->stage()->nid())
                        << ", \"taskId\" : " << std::to_string(task->index()) << "}}";
                }

                logCDebug(LC_API) << "Executor " << executor->id() << " on host "
                                  << executor->worker()->node()->id() << " got task " << task->id() << " after "
                                  << (t1 - t0);

                res.set_status(served::status_2XX::OK);
                return;
            }
        } else if (event == "idleEnd") {
            Executor *executor = resourcemanager->executor(appId, execId);
            if (!executor) {
                res.set_status(served::status_4XX::NOT_FOUND);
                return;
            }

            logCInfo(LC_API) << "Executor " << executor->id() << " on host " << executor->worker()->node()->id()
                             << " is idle";

            for (Resource *resource : executor->resources()) {
                Task *task = resource->idleTask();
                executor->done();
                engine->signal(new TaskFinishedEvent(app, task, executor));
            }

            Time        t0   = Time::now();
            const Task *task = executor->get(); // blocking call
            Time        t1   = Time::now();

            if (task->type() == TaskType::idle) {
                res << "{\"event\" : \"idle\""
                    << ", \"duration\" : " << std::to_string(0) << "}";
                logCInfo(LC_API) << "Telling " << executor->id() << " to do maintenance";
            } else if (task->type() == TaskType::disconnect) {
                res << "{\"event\" : \"disconnect\""
                    << ", \"duration\" : " << std::to_string(0) << "}";
                logCInfo(LC_API) << "Telling " << executor->id() << " to disconnect from its master";

                // Request master can overtake disconnect end if we
                // send it only once the disconnect task came back,
                // because it comes directly from the workers on the
                // remote nodes and doesn't go via the drvier, as 'disconnect emd' does.

                for (Resource *resource : executor->resources()) {
                    Task *task = resource->disconnectTask();
                    executor->done();
                    engine->signal(new TaskFinishedEvent(app, task, executor));
                }
            } else {
                res << "{\"event\" : \"execute\""
                    << ", \"task\" : {\"stageId\" : " << std::to_string(task->stage()->nid())
                    << ", \"taskId\" : " << std::to_string(task->index()) << "}}";
            }

            logCInfo(LC_API) << "Executor " << executor->id() << " on host " << executor->worker()->node()->id()
                             << " got task " << task->id() << " after " << (t1 - t0);

            res.set_status(served::status_2XX::OK);
            return;
        } else if (event == "disconnectEnd") {
            Executor *executor = resourcemanager->executor(appId, execId);
            if (!executor) {
                res.set_status(served::status_2XX::OK);
                return;
            }

            logCInfo(LC_API) << "Executor " << executor->id() << " on host " << executor->worker()->node()->id()
                             << " is was disconnected from its driver.";

            // Request master can overtake disconnect end if we send
            // it here so we need to send 'disconnect end' as soon as
            // we ask for it.

            // for (Resource *resource : executor->resources()) {
            //     Task *task = resource->disconnectTask();
            //     engine->signal(new TaskFinishedEvent(app, task));
            // }

            res.set_status(served::status_2XX::OK);
            return;
        } else if (event == "enable" || event == "disable") {
            Executor *executor = resourcemanager->executor(appId, execId);
            if (!executor) {
                res.set_status(served::status_4XX::NOT_FOUND);
                return;
            }

            logCWarn(LC_API) << "Executor " << executor->id() << " on host " << executor->worker()->node()->id()
                             << " is about to be " << event << "d";

            if (event == "enable")
                engine->signal(new ExecutorEnabledEvent(app, executor));
            //                executor->state(ResourceState::idle);
            else
                engine->signal(new ExecutorDisabledEvent(app, executor));
            //              executor->state(ResourceState::disabled);

            res.set_status(served::status_2XX::OK);
            return;
        } else {
            res << "Unknown event " << event;
            res.set_status(served::status_4XX::BAD_REQUEST);
            return;
        }
    } catch (std::exception const &e) {
        logError << e.what();
        res.set_status(served::status_4XX::BAD_REQUEST);
        return;
    }
}

void Server::workersHandler(served::response &res, const served::request &req)
{
    try {
        logCInfo(LC_API) << "Request URL  : " << req.url().path();
        logCInfo(LC_API) << "Request Data : " << req.body();

        ptree       data  = parse(req);
        std::string event = data.get<std::string>("event", "unknown");

        if (event == "register") {
            int         cores  = data.get<int>("cores", 0);
            int         memory = data.get<int>("memory", 0);
            std::string host   = data.get<std::string>("host", "unknown");
            int         port   = data.get<int>("port", 0);

            Node *node = this->cluster()->node(host);
            if (!node) {
                logCInfo(LC_API) << "Cannot register worker at " << host << ":" << port << ". Node " << host
                                 << " is unknown.";
                res.set_status(served::status_4XX::BAD_REQUEST);
                return;
            } else {
                logCInfo(LC_API) << "Registering worker " << host << ":" << port << " with " << cores
                                 << " cores and " << memory << " memory";

                Worker *worker = new Worker(node, port, cores, memory);

                worker->lock(); // unlocked by resource manager after registering this worker.
                engine->signal(new WorkerAddedEvent(worker));

                auto     guard = worker->guard();
                WorkerId id    = worker->id();

                res << "{\"id\" : " << std::to_string(id) << "}";
                res.set_status(served::status_2XX::OK);

                //                engine->signal(new WorkerIdleEvent(worker));
            }
        } else if (event == "requestMaster") {
            WorkerId id     = static_cast<WorkerId>(std::stoi(req.params["workerId"]));
            Worker * worker = resourcemanager->worker(id);

            if (worker) {
                logCInfo(LC_API) << "Worker " << worker->id() << " on " << worker->node()->id() << ":"
                                 << worker->port() << " with " << worker->cores() << " cores and "
                                 << worker->memory() << " memory requests a spark master.";

                engine->signal(new WorkerIdleEvent(worker));
                const App *app = worker->get();

                logCInfo(LC_API) << "Worker " << worker->id() << " got application " << app->id();

                res << "{\"driverUrl\":\"" << app->driverUrl() << "\""
                    << ", \"appId\":\"" << app->id() << "\"}";

                res.set_status(served::status_2XX::OK);
            } else {
                res.set_status(served::status_4XX::BAD_REQUEST);
            }
        } else {
            res << "Unknown event " << event;
            res.set_status(served::status_4XX::BAD_REQUEST);
            return;
        }
    } catch (std::exception const &e) {
        logError << e.what();
        res.set_status(served::status_4XX::BAD_REQUEST);
        return;
    }
}

void Server::signalsHandler(served::response &res, const served::request &req)
{
    try {
        logCInfo(LC_API) << "Request URL  : " << req.url().path();
        logCInfo(LC_API) << "Request Data : " << req.body();

        int signal = std::stoi(req.params["signal"]);

        switch (signal) {
        case 9:
            engine->signal(new KillEvent());
            break;
        default:
            break;
        }

        res.set_status(served::status_2XX::OK);
    } catch (std::exception const &e) {
        logError << e.what();
        res.set_status(served::status_4XX::BAD_REQUEST);
        return;
    }
}

void Server::run()
{
    logCInfo(LC_API) << "Starting REST server";

    try {
        served::multiplexer mux;

        mux.handle("/v0/apps/{appId}/stages/{stageId:[-0-9]+}/tasks/{taskId:[-0-9]+}")
            .post([this](served::response &res, const served::request &req) { this->tasksHandler(res, req); });
        mux.handle("/v0/apps/{appId}/stages/{stageId:[0-9]+}")
            .post([this](served::response &res, const served::request &req) { this->stagesHandler(res, req); });
        mux.handle("/v0/apps/{appId}/jobs/{jobId:[0-9]+}")
            .post([this](served::response &res, const served::request &req) { this->jobsHandler(res, req); });
        mux.handle("/v0/apps/{appId}/executors/{execId:[0-9]+}")
            .post([this](served::response &res, const served::request &req) {
                this->executorsHandler(res, req);
            });
        mux.handle("/v0/apps/{appId}").post([this](served::response &res, const served::request &req) {
            this->appsHandler(res, req);
        });
        mux.handle("/v0/workers/{workerId:[0-9]+}")
            .post(
                [this](served::response &res, const served::request &req) { this->workersHandler(res, req); });
        mux.handle("/v0/workers").post([this](served::response &res, const served::request &req) {
            this->workersHandler(res, req);
        });
        mux.handle("/v0/signals/{signal:[0-9]+}")
            .post(
                [this](served::response &res, const served::request &req) { this->signalsHandler(res, req); });

        std::string hostname;
        std::string port;

        if (config->listenHost() != "") {
            hostname = config->listenHost();
        } else {
            char hn[HOST_NAME_MAX];
            gethostname(hn, HOST_NAME_MAX);
            hostname = std::string(hn);
        }

        if (config->listenPort() > 0) {
            port = std::to_string(config->listenPort());
        } else {
            port = "4242";
        }

        logCInfo(LC_API) << "Listening on http://" << hostname << ":" << port << "/v0";

        // Create the server and run with 10 handler threads.
        this->_server = new served::net::server(hostname, port, mux);
        this->_server->run(200);

        if (this->_server)
            delete this->_server;
    } catch (std::exception const &e) {
        logError << "Failed to start REST server API: " << e.what();
        this->_server = NULL;
        engine->signal(new KillEvent());
    }

    logCInfo(LC_API) << "Stopped REST server";
}
