// Author(s): Michael Kaufmann <kau@zurich.ibm.com>
//            Adrian Schuepbach <dri@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "resourcemanager/resourcemanager.h"
#include "app/app.h"
#include "event/engine.h"
#include "helper/logger.h"

#include <fstream>

#include <boost/property_tree/json_parser.hpp>
namespace pt = boost::property_tree;

ResourceManager::ResourceManager(Cluster *cluster)
    : numApplicationsSubmitted(0UL), numApplicationsFinished(0UL), numWorkerAssignments(0UL),
      applicationsTotalDemand(0UL)
{
    logInfo(1) << "Initializing resource manager for cluster " << cluster->id();
    this->numResources = 0;

    // absorb resources of the cluster
    this->cluster(cluster);
    this->absorb(cluster);
    this->load();

    logInfo(1) << " - managing " << this->numResources << " resources.";
    for (auto &type : this->numTypes) {
        logInfo(1) << " - " << std::setfill(' ') << std::setw(2) << type.second << " of type " << type.first;
    }

    engine->listen(0,
                   EventType::ApplicationSubmitted,
                   std::bind(&ResourceManager::event, this, std::placeholders::_1));
    engine->listen(0, EventType::WorkerAdded, std::bind(&ResourceManager::event, this, std::placeholders::_1));
    engine->listen(0, EventType::WorkerIdle, std::bind(&ResourceManager::event, this, std::placeholders::_1));
    engine->listen(0, EventType::Kill, std::bind(&ResourceManager::event, this, std::placeholders::_1));
    engine->listen(0, EventType::Status, std::bind(&ResourceManager::event, this, std::placeholders::_1));
}

void ResourceManager::run(ResourceManager *rm)
{
    assert(rm != NULL);
    rm->run();
}

void ResourceManager::run()
{
    logCInfo(LC_RM) << "Starting resource manager for cluster " << this->cluster()->id();

    bool done = false;

    while (!done) {
        this->waitForEvent();
        bool reschedule = false;

        auto lock = std::unique_lock<std::mutex>(this->_eventMutex);
        logCInfo(LC_RM | LC_EVENT) << "Processing " << this->_eventQueue.size() << " event(s).";
        while (!this->_eventQueue.empty()) {
            Event *ev = this->_eventQueue.front();
            lock.unlock();

            logCDebug(LC_RM | LC_EVENT) << "Processing event " << *ev;

            auto guard = this->guard();

            switch (ev->type()) {
            case EventType::ApplicationSubmitted:
                reschedule |= this->applicationSubmittedEventHandler(
                    static_cast<ApplicationSubmittedEvent *>(ev));
                break;
            case EventType::ApplicationFinished:
                reschedule |= this->applicationFinishedEventHandler(
                    static_cast<ApplicationFinishedEvent *>(ev));
                break;
            case EventType::ApplicationDemandsChanged:
                reschedule |= this->applicationDemandsChangedEventHandler(
                    static_cast<ApplicationDemandsChangedEvent *>(ev));
                break;

            case EventType::ExecutorAdded:
                reschedule |= this->executorAddedEventHandler(static_cast<ExecutorAddedEvent *>(ev));
                break;

            case EventType::WorkerIdle:
                reschedule |= this->workerIdleEventHandler(static_cast<WorkerIdleEvent *>(ev));
                break;
            case EventType::WorkerAdded:
                reschedule |= this->workerAddedEventHandler(static_cast<WorkerAddedEvent *>(ev));
                break;

            case EventType::Status:
                reschedule |= this->statusEventHandler(static_cast<StatusEvent *>(ev));
                break;
            case EventType::Kill:
                logCInfo(LC_RM | LC_EVENT)
                    << "Shutting down resource manager for cluster " << this->cluster()->id();
                this->store();
                done = true;
                break;

            default:
                logError << "Unhandled event " << *ev;
                break;
            }

            delete ev;
            lock.lock();
            this->_eventQueue.pop();
        }
        lock.unlock();

        if (reschedule) {
            logCInfo(LC_RM) << "Recomputing resource allocations.";
            auto guard = this->guard();
            this->updateResourceShares();
            this->assignResourceShares();
            logCInfo(LC_RM) << "Done recomputing resource allocations.";
        }
    }
}

void ResourceManager::event(Event *ev)
{
    logCDebug(LC_RM | LC_EVENT) << "Received event " << *ev;

    auto lock = std::unique_lock<std::mutex>(this->_eventMutex);
    this->_eventQueue.push(ev->copy());
    this->_eventSignal.notify_all();
}

void ResourceManager::waitForEvent()
{
    auto lock = std::unique_lock<std::mutex>(this->_eventMutex);
    if (!this->_eventQueue.empty())
        return;

    logCDebug(LC_RM | LC_EVENT) << "Resource manager waits for events";

    this->_eventSignal.wait(lock, [this] { return !this->_eventQueue.empty(); });

    logCDebug(LC_RM | LC_EVENT) << "Resource manager continues with " << this->_eventQueue.size()
                                << " event(s)";
}

// void ResourceManager::event(Event *ev)
// {
//     switch (ev->type()) {

//     // case EventType::ExecutorRemoved: {
//     //     auto _ev = static_cast<ExecutorRemovedEvent *>(ev);
//     //     this->deregisterExecutor(_ev->executor());
//     // } break;
//     // case EventType::ExecutorDisconnectLocallyDone: {
//     //     auto _ev = static_cast<ExecutorDisconnectLocallyDoneEvent *>(ev);

//     //     // send the termination, instead of waiting for the reply to avoid a race-condition of spark
//     //     // sending the reply and shutting down at the same time
//     //     for (Resource *resource : _ev->executor()->resources()) {
//     //         Task *task = resource->disconnectTask();
//     //         engine->signal(new TaskFinishedEvent(_ev->app(), task));
//     //     }
//     // } break;

//     case EventType::Nop: {
//         auto _ev = static_cast<NopEvent *>(ev);
//     } break;

//     default:
//         logCWarn(LC_RM) << "Received unhandled event " << ev->type();
//         break;
//     }
// }

void ResourceManager::load()
{
    try {
        std::string   rkeysFileName = config->perfdbdir() + "/" + this->cluster()->id() + "-rkeys.json";
        std::ifstream rkeysFile(rkeysFileName, std::ios::in);
        if (!rkeysFile.is_open())
            return;

        ptree rkeys;
        read_json(rkeysFile, rkeys);
        rkeysFile.close();
        this->_resourceKeyMapper.load(rkeys);

        logInfo(0) << "Loaded " << this->_resourceKeyMapper.size() << " resource key mappings from "
                   << rkeysFileName;
    } catch (std::exception const &e) {
        logError << "Error while loading resource mappings: " << e.what();
        return;
    }
}

void ResourceManager::store()
{
    try {
        std::string   rkeysFileName = config->perfdbdir() + "/" + this->cluster()->id() + "-rkeys.json";
        std::ofstream rkeysFile(rkeysFileName, std::ios::out | std::ios::trunc);
        ptree         rkeys = this->_resourceKeyMapper.store();
        write_json(rkeysFile, rkeys);
        rkeysFile.close();

        logInfo(0) << "Stored " << this->_resourceKeyMapper.size() << " resource key mappings to "
                   << rkeysFileName;
    } catch (std::exception const &e) {
        logError << "Error while storing resource mappings: " << e.what();
        return;
    }
}

void ResourceManager::absorb(Resource *resource)
{
    assert(resource != NULL);

    if (resource->children().size() > 0) {
        for (auto child : resource->children()) {
            this->absorb(child);
        }
    } else {
        resource->idleTask(new IdleTask());
        resource->disconnectTask(new DisconnectTask());
        this->pool[std::make_pair(resource->type(), resource->model())].push_back(resource);
        this->_models[resource->type()].emplace(resource->model());
        this->numTypes[resource->type()]++;
        this->numModels[resource->model()]++;
        this->numResources++;

        std::hash<std::pair<ResourceType, ResourceModel>> hasher;

        ResourceKey rkey = this->_resourceKeyMapper.add(
            hasher(std::make_pair(resource->type(), resource->model())));

        resource->key(rkey);
        logDebug << "Resource " << resource->id() << " of " << resource->type() << "/" << resource->model()
                 << " gets key " << resource->key();
    }
}

std::unique_lock<std::timed_mutex> ResourceManager::guard()
{
    logCDebug(LC_LOCK) << "About to acquire resource manager lock";
    auto lock = std::unique_lock<std::timed_mutex>(this->_mutex, std::defer_lock);
    while (!lock.try_lock_for(10s)) {
        logWarn << "Unable to acquire resource manager lock within the last 10s";
    }
    logCDebug(LC_LOCK) << "Acquired resource manager lock";
    return lock;
}

bool ResourceManager::statusEventHandler(StatusEvent *ev)
{
    assert(ev != NULL);

    logCInfo(LC_RM) << "Resource Manager status:";
    logCInfo(LC_RM) << " - " << this->_applications.size() << " active applications";
    logCInfo(LC_RM) << " - " << this->numApplicationsSubmitted << "/" << this->numApplicationsFinished
                    << " total submitted/finished applications";
    logCInfo(LC_RM) << " - " << this->workerPool.size() << " total workers";
    logCInfo(LC_RM) << " - " << this->workerFreePool.size() << " free workers ("
                    << this->workerFreePool.getLoad() << ")";
    logCInfo(LC_RM) << " - " << this->applicationsTotalDemand << " total executor demand";
    logCInfo(LC_RM) << " - " << this->numWorkerAssignments << " total number of worker assignments";
    logCInfo(LC_RM) << "Application resource allocations:";
    for (auto &entry : this->_applicationWorkers) {
        AppId appId = entry.first;
        App * app   = this->_applications.find(appId) != this->_applications.end() ? this->_applications[appId]
                                                                                : NULL;
        TaskState appState = this->_applications.find(appId) != this->_applications.end()
                                 ? this->_applications[appId]->state()
                                 : TaskState::unknown;
        size_t appDemand = this->_applicationDemands.find(appId) != this->_applicationDemands.end()
                               ? this->_applicationDemands[appId]
                               : 0UL;
        size_t appTarget = this->_applicationTargets.find(appId) != this->_applicationTargets.end()
                               ? this->_applicationTargets[appId]
                               : 0UL;
        size_t appAllocation = this->_applicationAllocations.find(appId) != this->_applicationAllocations.end()
                                   ? this->_applicationAllocations[appId]
                                   : 0UL;
        logCInfo(LC_RM) << " - " << appId << " (" << appState << ")";
        logCInfo(LC_RM) << "   - demand     : " << appDemand;
        logCInfo(LC_RM) << "   - target     : " << appTarget;
        logCInfo(LC_RM) << "   - allocation : " << appAllocation << " / " << entry.second.size();

        std::ostringstream os;
        for (Worker *worker : entry.second) {
            os << " " << worker->id();
            if (worker->app() != app && app != NULL && worker->app() != NULL)
                os << " (" << worker->app()->id() << ")";
        }
        logCInfo(LC_RM) << "   - workers    :" << os.str();
    }

    return false;
}

bool ResourceManager::workerAddedEventHandler(WorkerAddedEvent *ev)
{
    Worker *worker = ev->worker();

    worker->id(static_cast<WorkerId>(this->workerNextId++));

    this->workerPool.put(worker);
    worker->unlock(); // used as synchronization mechanism with Server::workersHandler

    logCInfo(LC_RM) << "Registered worker " << worker->id() << " at " << worker->node()->id() << ":"
                    << worker->port();

    return true;
}

bool ResourceManager::workerIdleEventHandler(WorkerIdleEvent *ev)
{
    Worker *  worker   = ev->worker();
    Executor *executor = worker->executor();

    App *app = worker->app();

    if (app) { // FIXME: Remove resources from allocation once they're idle again.
        if (this->_applications.find(app->id()) == this->_applications.end()) {
            logCWarn(LC_RM) << "Worker " << worker->id() << " used to belong to application " << app->id()
                            << " which has since been removed";
            this->_applicationWorkers.at(app->id()).erase(worker);
            if (this->_applicationWorkers.at(app->id()).empty())
                this->_applicationWorkers.erase(app->id());
        } else {
            this->_applicationAllocations.at(app->id())--;
            this->_applicationWorkers.at(app->id()).erase(worker);
            this->_executors.at(app->id()).erase(executor->id());
            logCInfo(LC_RM) << "Worker " << worker->id() << " used to belong to application " << app->id()
                            << " (" << this->_applicationAllocations.at(app->id())
                            << " workers allocated) and is now idle.";
            // This was the last worker of this application and the application is finished -> signal deferred application finish so that we can clean up
            if (this->_applicationAllocations.at(app->id()) == 0 &&
                this->_applicationsFinished.count(app->id()) > 0)
                engine->signal(new ApplicationFinishedEvent(app));
        }
    }

    if (executor) {
        for (auto resource : executor->resources()) {
            resource->state(ResourceState::idle);
        }
    }

    this->workerFreePool.put(worker);
    return true;
}

bool ResourceManager::applicationDemandsChangedEventHandler(ApplicationDemandsChangedEvent *ev)
{
    App *  app    = ev->app();
    size_t demand = ev->demand();

    if (this->_applicationDemands.find(app->id()) != this->_applicationDemands.end()) {
        if (this->_applicationDemands.at(app->id()) != demand) {
            logCInfo(LC_RM) << "Application " << app->id() << " demands " << demand << " resources (previously "
                            << this->_applicationDemands.at(app->id()) << ")";
            this->_applicationDemands.at(app->id()) = demand;

            // If we already got that application as much as it wants then there's no need to
            // reconsider resource shares.
            if (demand == this->_applicationAllocations.at(app->id()))
                return false;

            // // If the application demands more but we don't have more, then we don't need to
            // // reconsider resource shares either.
            // if (demand > this->_applicationAllocations.at(app->id()) && this->workerFreePool.size() == 0)
            //     return false;

            // Here, we either have idle resources and the application wants them or the application
            // wants less than it has, then we'll take them away.
            return true;
        } else {
            logCInfo(LC_RM) << "Application " << app->id() << " demands unchanged at "
                            << this->_applicationDemands.at(app->id()) << " resources";
            return false;
        }
    }

    return false;
}

void ResourceManager::assignResourceShares()
{
    // Actually assign free workers to applications according to the just computed shares.
    bool done = false;
    while (this->workerFreePool.size() > 0 && !done) {
        done = true;
        for (auto &entry : this->_applications) {
            AppId id  = entry.first;
            App * app = entry.second;

            size_t &nCurrentAllocations = this->_applicationAllocations.at(id);
            size_t &nCurrentTarget      = this->_applicationTargets.at(id);

            if (nCurrentTarget > nCurrentAllocations) {
                Worker *worker = this->workerFreePool.take();

                if (worker) {
                    worker->add(app);
                    this->numWorkerAssignments++;
                    this->_applicationWorkers[app->id()].emplace(worker);
                    worker->executor()->state(ResourceState::idle);
                    nCurrentAllocations++;
                    logCInfo(LC_RM) << "Assigning worker " << worker->id() << " to application " << app->id()
                                    << " (new allocation=" << nCurrentAllocations
                                    << " target=" << nCurrentTarget << ")";
                    done = false;
                }
            }
        }
    }
}

void ResourceManager::updateResourceShares()
{
    size_t nTotalWorkers     = this->workerPool.size();
    size_t nRemainingWorkers = nTotalWorkers;
    size_t nAssignedWorkers  = 0;

    std::map<AppId, size_t> appWorkerTargets;
    std::map<AppId, double> appWeights;

    double   totalWeight     = 0.0;
    double   remainingWeight = 0.0;
    uint64_t totalDemand     = 0UL;

    for (auto &entry : this->_applicationDemands) {
        AppId id             = entry.first;
        appWeights[id]       = 1.0;
        appWorkerTargets[id] = 0;
        totalWeight += appWeights[id];
        totalDemand += entry.second;
    }

    remainingWeight               = totalWeight;
    this->applicationsTotalDemand = totalDemand;

    // If we have more applications than workers, not all applications will get workers assigned and
    // appWorkerTargets will be 0 for those.

    while (nRemainingWorkers > nAssignedWorkers && appWeights.size() > 0) {
        auto entry = appWeights.begin();
        while (entry != appWeights.end() && nRemainingWorkers > nAssignedWorkers) {
            AppId  id     = entry->first;
            double weight = entry->second;
            size_t demand = this->_applicationDemands.at(id);

            double allowedShare = weight / remainingWeight;

            double usedShare = static_cast<double>(appWorkerTargets[id]) / nRemainingWorkers;
            double nextShare = static_cast<double>(appWorkerTargets[id] + 1) / nRemainingWorkers;
            double usedDiff  = abs(usedShare - allowedShare);
            double nextDiff  = abs(nextShare - allowedShare);

            logCDebug(LC_RM) << "app=" << id << " demand=" << demand << " weight=" << weight
                             << " usedShare=" << usedShare << " nextShare=" << nextShare
                             << " usedDiff=" << usedDiff << " nextDiff=" << nextDiff
                             << " nRemainingWorkers=" << nRemainingWorkers
                             << " nAssignedWorkers=" << nAssignedWorkers;

            if (nextDiff < usedDiff && appWorkerTargets[id] < demand) {
                appWorkerTargets[id]++;
                nAssignedWorkers++;
                entry++;
            } else {
                // We're done with this app.
                remainingWeight -= weight;
                nRemainingWorkers -= appWorkerTargets[id];
                nAssignedWorkers -= appWorkerTargets[id];
                entry = appWeights.erase(entry);
            }
        }
    }

    size_t assignedWorkers = 0;

    for (auto &entry : this->_applicationTargets) {
        AppId   appId     = entry.first;
        size_t &appTarget = entry.second;
        App *   app       = this->_applications.at(appId);

        assignedWorkers += appTarget;

        if (appWorkerTargets.find(appId) == appWorkerTargets.end())
            logError << "AppId " << appId << " not found int appWorkerTargets";

        if (this->_applicationAllocations.find(appId) == this->_applicationAllocations.end())
            logError << "AppId " << appId << " not found int this->_applicationAllocations";

        logCInfo(LC_RM) << "Application " << app->id() << " worker share is " << appWorkerTargets.at(appId)
                         << " workers, demands " << this->_applicationDemands.at(appId) << ", has "
                         << this->_applicationAllocations.at(appId);
        if (appTarget != appWorkerTargets.at(appId)) {
            appTarget = appWorkerTargets.at(appId);
            engine->signal(new WorkerAllocationUpdateEvent(app, appTarget));
        }
    }

    if (assignedWorkers != this->workerPool.size())
        logCWarn(LC_RM) << "Applications use " << assignedWorkers << " of " << this->workerPool.size()
                        << " workers";
}

bool ResourceManager::applicationSubmittedEventHandler(ApplicationSubmittedEvent *ev)
{
    App *app = ev->app();

    if (this->_applications.find(app->id()) == this->_applications.end()) {
        this->_applications.emplace(app->id(), app);
        this->_applicationDemands.emplace(app->id(), 0UL);
        this->_applicationAllocations.emplace(app->id(), 0UL);
        this->_applicationTargets.emplace(app->id(), 0UL);
        this->_applicationWorkers.emplace(app->id(), std::set<Worker *>());

        engine->listen(app->id(),
                       0,
                       EventType::ApplicationFinished,
                       std::bind(&ResourceManager::event, this, std::placeholders::_1));
        engine->listen(app->id(),
                       0,
                       EventType::ExecutorAdded,
                       std::bind(&ResourceManager::event, this, std::placeholders::_1));
        engine->listen(app->id(),
                       0,
                       EventType::ExecutorRemoved,
                       std::bind(&ResourceManager::event, this, std::placeholders::_1));
        engine->listen(app->id(),
                       0,
                       EventType::ExecutorDisabled,
                       std::bind(&ResourceManager::event, this, std::placeholders::_1));
        engine->listen(app->id(),
                       0,
                       EventType::ExecutorEnabled,
                       std::bind(&ResourceManager::event, this, std::placeholders::_1));

        engine->listen(app->id(),
                       0,
                       EventType::ExecutorDisconnect,
                       std::bind(&ResourceManager::event, this, std::placeholders::_1));
        engine->listen(app->id(),
                       0,
                       EventType::ExecutorDisconnectLocallyDone,
                       std::bind(&ResourceManager::event, this, std::placeholders::_1));
        engine->listen(app->id(),
                       0,
                       EventType::ApplicationDemandsChanged,
                       std::bind(&ResourceManager::event, this, std::placeholders::_1));

        this->numApplicationsSubmitted++;
        logCInfo(LC_RM) << "Added application " << app->id();
        this->_applicationsCondVar.notify_all();
    } else {
        logCWarn(LC_RM) << "Not adding application " << app->id() << " because it already exists.";
    }

    return false;
}

bool ResourceManager::applicationFinishedEventHandler(ApplicationFinishedEvent *ev)
{
    App *app = ev->app();

    if (this->_applications.find(app->id()) != this->_applications.end()) {
        if (this->_applicationWorkers.at(app->id()).size() > 0) {
            this->_applicationsFinished.emplace(app->id());
            logCInfo(LC_RM) << "Application " << app->id() << " is finished but still holds "
                            << this->_applicationAllocations.at(app->id())
                            << " workers. Waiting for them to disconnect...";
        } else {
            logCInfo(LC_RM) << "Application " << app->id()
                            << " is finished and holds on to no workers anymore. Removing it. ";

            this->_applicationsFinished.erase(app->id());
            this->_applicationDemands.erase(app->id());
            this->_applicationWorkers.erase(app->id());
            this->_applicationAllocations.erase(app->id());
            this->_applicationTargets.erase(app->id());
            this->_applications.erase(app->id());
            this->_executors.erase(app->id());

            this->numApplicationsFinished++;

            logCInfo(LC_RM) << "Application " << app->id() << " removed";
        }
    } else {
        logCWarn(LC_RM) << "Not removing application " << app->id() << " because it does not exists.";
    }

    return false;
}

bool ResourceManager::executorAddedEventHandler(ExecutorAddedEvent *ev)
{
    Executor *executor = ev->executor();
    App *     app      = ev->app();
    // App *  app  = executor->worker()->app();
    Worker *worker = executor->worker();
    // size_t nres = executor->cores();

    // for (auto resource : node->resources(ResourceType::cpu)) {
    //     if (nres == 0)
    //         break;
    //     if (!resource->executor()) {
    //         executor->addResource(resource);
    //         nres--;
    //     }
    // }

    // Register executor
    this->_executors[app->id()][executor->id()] = executor;

    logCDebug(LC_RM) << "Registered executor " << executor->id() << " with " << executor->cores()
                     << " cores on worker " << worker->id() << " for application " << app->id();

    return false;
}

Executor *ResourceManager::executor(AppId app, int id)
{
    auto guard = this->guard();

    if (this->_executors.find(app) != this->_executors.end() &&
        this->_executors.at(app).find(id) != this->_executors.at(app).end())
        return this->_executors.at(app).at(id);
    else
        return NULL;
}

Worker *ResourceManager::worker(WorkerId id)
{
    auto guard = this->guard();

    return this->workerPool.get(id);
}
