// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "scheduler/heteroscheduler.h"
#include "common.h"
#include "event/engine.h"
#include "event/event.h"
#include "helper/sorter.h"
#include "resourcemanager/resourcemanager.h"
#include "scheduler/oracle.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <future>
#include <iostream>
#include <string>

#include <boost/functional/hash.hpp>

#ifdef WITHGPERFTOOLS
#include <gperftools/profiler.h>
#endif

#define RES_BAD (ULLONG_MAX / 2)

/*
There are 3 events that may cause a task to be executed.

(1) An Executor becomes idle
    -> Interrupt scheduler (we need to run tabuCost on the best schedule to get
       the allocation for the best schedule).
    -> check the allocation list of the resource (executer) that is idle
       -> if there is a task, pick it, mark it as scheduled and enqueue it into
          the executor's task queue.
       -> if there is no task, add executor/resource to idleResource map.

(2) A stage becomes ready
    -> Interrupt scheduler (we need to run tabuCost on the best schedule to get
       the allocation for the best schedule).
    -> Match executors from the idleResources map with the now-ready tasks from
       this stage.
       -> if there is a match, pick the task, mark it as scheduled and enqueue
          it into the executor's task queue.

(3) A new best schedule is found
    -> Match all ready tasks with idle executors
       -> if there is a match, pick the task, mark it as scheduled and enqueue
          it into the executor's task queue.
 */

/*
  TODO:
  - All external events go into queues
  - Events are processed in a single thread
  - Only after all events have been processed we compute a new schedule.
*/

HeteroScheduler::HeteroScheduler(App *app, ResourceManager *resourcemanager)
    : SchedulerBase(app, resourcemanager), _schedule(NULL), numAssignedExecutors(0UL),
      numRequestedExecutors(0UL)
{
    this->schedule(app->schedule());
    this->oracle(new Oracle(app));
    this->_time  = baseTime;
    this->_train = config->getFlag(ConfigSchedulerFlags::training_mode);

    this->cfgIoTaskWeight          = config->get(ConfigVariable::io_task_weight);
    this->cfgIoTaskIoWeight        = config->get(ConfigVariable::io_task_io_weight);
    this->cfgCmpTaskWeight         = config->get(ConfigVariable::cmp_task_weight);
    this->cfgCmpTaskIoWeight       = config->get(ConfigVariable::cmp_task_io_weight);
    this->cfgNodeLoadWeight        = config->get(ConfigVariable::node_load_weight);
    this->cfgInterferenceMode      = config->get(ConfigVariable::interference_mode);
    this->cfgResourceScalingFactor = config->get(ConfigVariable::resource_scaling_factor);

    this->cfgLevelWeight = static_cast<double>(config->lweight()) / 10.0;

    engine->listen(EventType::Clock, std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(EventType::Kill, std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(EventType::Status, std::bind(&HeteroScheduler::event, this, std::placeholders::_1));

    engine->listen(app->id(),
                   EventType::ApplicationFinished,
                   std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(app->id(),
                   0,
                   EventType::StageAdded,
                   std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(app->id(),
                   0,
                   EventType::StageReady,
                   std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(app->id(),
                   0,
                   EventType::StageFinished,
                   std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(app->id(),
                   0,
                   EventType::TaskTimeout,
                   std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(app->id(),
                   0,
                   EventType::TaskStarted,
                   std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(app->id(),
                   0,
                   EventType::TaskFinished,
                   std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(app->id(),
                   1,
                   EventType::ExecutorAdded,
                   std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(app->id(),
                   1,
                   EventType::ExecutorDisabled,
                   std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(app->id(),
                   1,
                   EventType::ExecutorEnabled,
                   std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
    engine->listen(app->id(),
                   1,
                   EventType::WorkerAllocationUpdate,
                   std::bind(&HeteroScheduler::event, this, std::placeholders::_1));
}

HeteroScheduler::~HeteroScheduler()
{
    if (this->_oracle) {
        delete this->_oracle;
        this->_oracle = NULL;
    }
}

void HeteroScheduler::event(Event *ev)
{
    if (ev->type() != EventType::Clock)
        logCDebug(LC_SCHED | LC_EVENT) << "Received event " << *ev;

    auto lock = std::unique_lock<std::mutex>(this->_eventMutex);
    this->_eventQueue.push(ev->copy());
    this->_eventSignal.notify_all();
}

void HeteroScheduler::waitForEvent()
{
    auto lock = std::unique_lock<std::mutex>(this->_eventMutex);
    if (!this->_eventQueue.empty())
        return;
    logInfo(2) << "Scheduler pauses...";

    this->_eventSignal.wait(lock, [this] { return !this->_eventQueue.empty(); });

    logInfo(2) << "Scheduler continues with " << this->_eventQueue.size() << " events...";
}

bool HeteroScheduler::clockEventHandler(ClockEvent *ev)
{
    assert(ev != NULL);

    this->_time = ev->time();

    return false;
}

bool HeteroScheduler::statusEventHandler(StatusEvent *ev)
{
    assert(ev != NULL);

    logCInfo(LC_SCHED) << "Scheduler status for " << this->app()->id() << " (" << this->app()->name() << ")";
    // 1. Determine which stages need to be scheduled.
    for (auto &entry : this->_schedule->stages) {
        Stage *stage = entry.second.stage;

        StageSchedule *schedule = &this->_schedule->stages.at(stage->nid());

        if (stage->state() >= TaskState::finished) {
            if (schedule->usedPool.size() + schedule->freePool.size() > 0) {
                logCWarn(LC_SCHED) << " - stage " << stage->id() << " (" << stage->state() << ") with "
                                   << schedule->nFinishedTasks << "/" << schedule->nTasks
                                   << " finished/total tasks has " << schedule->usedPool.size() << "/"
                                   << schedule->freePool.size() << " used/free executors with a target of "
                                   << schedule->targetPoolSize;
            }
            continue;
        }
        if (stage->unsatDeps() != 0)
            continue;

        logCInfo(LC_SCHED) << " - stage " << stage->id() << " (" << stage->state() << ") with "
                           << schedule->nFinishedTasks << "/" << schedule->nTasks
                           << " finished/total tasks has " << schedule->usedPool.size() << "/"
                           << schedule->freePool.size() << " used/free executors with a target of "
                           << schedule->targetPoolSize;
    }

    return false;
}

bool HeteroScheduler::taskStartedEventHandler(TaskStartedEvent *ev)
{
    assert(ev != NULL);

    bool      reschedule = false;
    Executor *executor   = ev->executor();
    Task *    task       = ev->task();
    auto      guard      = task->guard();

    if (task->type() == TaskType::idle || task->type() == TaskType::disconnect)
        return false;

    Stage *stage = task->stage();
    //    Resource *resource = task->allocation()->resource();

    Duration runtime   = task->allocation()->duration();
    Time     planned   = task->allocation()->from();
    Time     actual    = Time::now();
    Duration tolerance = Duration(std::min(10000us, runtime / 10));

    if (planned + tolerance < actual) {
        // Starts a bit too late
        reschedule = true;
    } else if (planned - tolerance > actual) {
        // Starts a bit too early
        reschedule = true;
    } else {
        // just fine.
    }

    task->metrics().started(actual);

    Time from = task->metrics().started();
    Time to   = from + runtime;

    return reschedule;
}

bool HeteroScheduler::taskTimeoutEventHandler(TaskTimeoutEvent *ev)
{
    assert(ev != NULL);

    bool      reschedule = false;
    Executor *executor   = ev->executor();
    Task *    task       = ev->task();
    auto      guard      = task->guard();

    if (task->type() == TaskType::idle || task->type() == TaskType::disconnect)
        return false;

    if (task->state() == TaskState::running) {
        logCInfo(LC_SCHED) << "Task " << task->id() << " on "
                           << "executor " << executor->id() << " timed out.";

        Duration planned = task->allocation()->duration();
        Duration extra   = std::max(Duration(10ms), Duration(planned.count() / 5));
    }

    return reschedule;
}

bool HeteroScheduler::taskFinishedEventHandler(TaskFinishedEvent *ev)
{
    assert(ev != NULL);

    bool      reschedule = false;
    Executor *executor   = ev->executor();
    Task *    task       = ev->task();
    auto      guard      = task->guard();
    Stage *   stage      = task->stage();

    if (task->type() == TaskType::idle) {
        logCInfo(LC_SCHED) << "Task " << task->id() << " on executor " << executor->id() << " finished";
        // Idle tasks are always ready
        // Disconnect tasks are always ready
        task->state(TaskState::finished);
        task->state(TaskState::ready);
    } else if (task->type() == TaskType::disconnect) {
        logCInfo(LC_SCHED) << "Task " << task->id() << " on executor " << executor->id() << " finished";
        task->state(TaskState::finished);
        task->state(TaskState::ready);

        // Remove executor from pool. numAssignedExecutors was decremented once the free was
        // initiated. So don't do it here again.
        logCInfo(LC_SCHED) << "Removing executor " << executor->id() << " from executor pool";
        this->_schedule->executorPool.get(executor);

        if (this->app()->state() == TaskState::finished && this->_schedule->executorPool.size() == 0) {
            engine->signal(new ApplicationFinishedEvent(this->app()));
        }
    } else {
        logCInfo(LC_SCHED) << "Task " << task->id() << " on executor " << executor->id() << " finished";
        this->_schedule->finish(task);
        task->state(TaskState::finished);

        Node *node = executor->node();

        switch (this->cfgInterferenceMode) {
        case 0: // ignore interference
            task->metrics().ioLoad(0);
            break;
        case 1: // count each task
            task->metrics().ioLoad(1);
            break;
        case 2: // count each I/O task
            task->metrics().ioLoad(task->type() == TaskType::load);
            break;
        case 3: // count total task input data volume
            task->metrics().ioLoad(task->metrics().inData());
            break;
        case 4: // same as (3) but over time
            task->metrics().ioLoad(task->metrics().inData() / task->metrics().runtime().count());
            break;
        case 5: // count remote task input data volume
            task->metrics().ioLoad(task->metrics().inData() - task->metrics().inData(node));
            break;
        case 6: // same as (5) but over time
            task->metrics().ioLoad((task->metrics().inData() - task->metrics().inData(node)) /
                                   task->metrics().runtime().count());
            break;
        }

        this->_oracle->update(task);

        reschedule = true; // TODO: Here the now idle executor should just take a task from the
                           // queue. Also, recompute stage weights.
    }

    if (!(task->type() == TaskType::idle || task->type() == TaskType::disconnect)) {
        // // If we have more tasks than we should have, free this one.
        // if (schedule->targetPoolSize > this->_schedule->maxNumExecutors) {
        // } else {
        StageSchedule &ssched = this->_schedule->stages.at(stage->nid());

        // Check whether to release an executor to (1) the application, in case the stage exceeds
        // its allowed share and (2) to the resource manager, in case the application also exceeds
        // its allowed share.
        if (ssched.targetPoolSize < ssched.usedPool.size() + ssched.freePool.size()) {
            ssched.usedPool.get(executor);
            this->release(executor);
        } else {
            ssched.usedPool.get(executor);
            ssched.freePool.put(executor);
        }

        if (stage->state() == TaskState::finished) {
            // Execute deferred stage finish in case the finish for this task came in after the finish
            // event for the stage.
            engine->signal(new StageFinishedEvent(stage->app(), stage));
        }
        //        }
    }

    this->requestResources();
    return true; //reschedule;
}

bool HeteroScheduler::stageAddedEventHandler(StageAddedEvent *ev)
{
    assert(ev != NULL);

    this->app()->addStages(ev->stages());

    for (auto &entry : ev->stages()) {
        Stage *stage = entry.first;

        stage->state(TaskState::unready);

        // Add new stage to the oracle cache.
        if (config->getFlag(ConfigSchedulerFlags::consider_io_size)) {
            if (stage->type() == TaskType::load) {
                this->oracle()->preset(stage);
            }

            stage->sortTasks(); // sort tasks according to their input size so that we can schedule
                                // the "big" ones first.
        }

        if (this->_oracleCache.find(stage->key()) == this->_oracleCache.end()) {
            this->_oracleCache.emplace(stage->key(), OracleCache(this->oracle(), stage->key()));
        }

        for (Task *task : stage->tasks()) {
            task->scheduler(this);
        }
#if 0
        std::set<Node *> nodes;
        for (auto &resourceClass : this->_schedule->resources) {
            for (Resource *resource : resourceClass.second) {
                nodes.emplace(resource->node());
            }
        }

        for (Task *task : stage->tasks()) {
            logInfo(0) << "Task " << task->id() << " reads " << task->metrics().inData() << " in total " << " (" << &task->metrics() << ")";;
            for (Node *node : nodes) {
                logInfo(0) << " - " << task->metrics().inData(node) << " from node " << node->id();
            }
            for (auto &resourceClass : this->_schedule->resources) {
                for (Resource *resource : resourceClass.second) {
                    logInfo(0) << " - " << task->metrics().inData(resource->executor()) << " from executor "
                               << resource->executor()->id() << " (" << resource->id() << ")";
                }
            }
        }

#endif
        logInfo(0) << "Stage " << stage->id() << " has been added to " << this->app()->id();

        stage->mark(0);
        stage->weight(this->getStageWeight(stage));
    }

    //    this->app()->unlock(1);
    this->requestResources();
    return false;
}

bool HeteroScheduler::stageReadyEventHandler(StageReadyEvent *ev)
{
    assert(ev != NULL);

    Stage *stage = ev->stage();

    logInfo(0) << "Stage " << stage->id() << " is ready";

    if (config->getFlag(ConfigSchedulerFlags::consider_io_size)) {
        stage->sortTasks();
    }

#if 0
    std::set<Node *> nodes;
    for (auto &resourceClass : this->_schedule->resources) {
        for (Resource *resource : resourceClass.second) {
            nodes.emplace(resource->node());
        }
    }

    for (Task *task : stage->tasks()) {
        logInfo(0) << "Task " << task->id() << " reads " << task->metrics().inData() << " in total";
        for (Node *node : nodes) {
            logInfo(0) << " - " << task->metrics().inData(node) << " from node " << node->id();
        }
        for (auto &resourceClass : this->_schedule->resources) {
            for (Resource *resource : resourceClass.second) {
                logInfo(0) << " - " << task->metrics().inData(resource->executor()) << " from executor "
                           << resource->executor()->id() << " (" << resource->id() << ")";
            }
        }
    }
#endif

    this->_schedule->nextStages.erase(stage);
    stage->state(TaskState::ready);
    this->requestResources();

    for (auto out : stage->next()) {
        Stage *cs      = out->dst();
        bool   nextSet = true;
        for (auto out : stage->next()) {
            Stage *ps = out->dst();
            if (ps->state() < TaskState::ready) {
                nextSet = false;
                break;
            }
        }

        if (nextSet)
            this->_schedule->nextStages.emplace(cs);
    }

    return true;
}

bool HeteroScheduler::stageFinishedEventHandler(StageFinishedEvent *ev)
{
    assert(ev != NULL);

    Stage *        stage    = ev->stage();
    StageSchedule &schedule = this->_schedule->stages.at(stage->nid());

    if (schedule.nTasks > schedule.nFinishedTasks) {
        if (schedule.nAssignedTasks > 0) {
            stage->state(TaskState::finished);
            logCWarn(LC_SCHED) << "Deferring stage " << stage->id()
                               << " finish event. Some tasks are not done yet: " << schedule.nFreeTasks << "/"
                               << schedule.nAssignedTasks << schedule.nFinishedTasks;
        }
        //        this->dumpAllocations();
        assert(schedule.nAssignedTasks == (schedule.nTasks - schedule.nFinishedTasks));
        return false;
    } else {
        stage->state(TaskState::finished);

        logCInfo(LC_SCHED) << "Stage " << stage->id() << " has finished. Remaining resources are";

        assert(schedule.usedPool.size() == 0);
        // for (Executor *executor : schedule.usedPool.pool()) {
        //     logCInfo(LC_SCHED) << " - " << executor->id() << " (" << executor->state() << ")";
        // }

        Executor *executor = schedule.freePool.take();

        while (executor != NULL) {
            logCInfo(LC_SCHED) << " - " << executor->id() << " (" << executor->state() << ") -> releasing";
            this->release(executor);
            executor = schedule.freePool.take();
        }
    }

    this->requestResources();
    return true;
}

bool HeteroScheduler::applicationFinishedEventHandler(ApplicationFinishedEvent *ev)
{
    assert(ev != NULL);

    App *app = ev->app();
    //    engine->signal(new ApplicationDemandsChangedEvent(this->app(), 0));
    app->state(TaskState::finished);

    size_t nex = this->_schedule->executorPool.size();
    if (nex > 0) {
        logCInfo(LC_SCHED) << "Application " << app->id() << " is finished. Disconnecting remaining " << nex
                           << " executors.";
        Executor *executor = this->_schedule->freePool.take();

        while (executor != NULL) {
            Worker *worker = executor->worker();
            logCInfo(LC_RM | LC_SCHED)
                << " - disconnecting worker " << worker->id() << " from finished application " << app->id();

            logCInfo(LC_SCHED) << "Removing executor " << executor->id() << " from free pools";
            this->numAssignedExecutors--;
            executor->disconnect();
            executor = this->_schedule->freePool.take();
        }
    } else {
        logCInfo(LC_SCHED) << "Application " << app->id()
                           << " is finished. All executors have been disconnected.";
    }

    return false;
}

bool HeteroScheduler::executorAddedEventHandler(ExecutorAddedEvent *ev)
{
    assert(ev != NULL);

    Executor *executor = ev->executor();

    if (executor->worker()->app() == this->app()) {
        logCInfo(LC_SCHED) << "Adding executor " << executor->id() << " to both pools";
        //        assert(this->numAssignedExecutors == this->_schedule->executorPool.size());

        this->_schedule->executorPool.put(executor);
        this->_schedule->freePool.put(executor);
        this->numAssignedExecutors++;

        Worker *worker = executor->worker();
        logCInfo(LC_SCHED) << "Adding worker " << worker->id() << " with executor " << executor->id() << " ("
                           << executor << ")"
                           << " to application " << this->app()->id() << " which has now "
                           << this->_schedule->executorPool.size() << " (" << this->numAssignedExecutors
                           << ") executors";

        //        assert(this->numAssignedExecutors == this->_schedule->executorPool.size());
        return true;
    } else
        return false;
}

bool HeteroScheduler::executorDisabledEventHandler(ExecutorDisabledEvent *ev)
{
    assert(ev != NULL);

    bool      reschedule = false;
    Executor *executor   = ev->executor();

    // for (Resource *resource : executor->resources()) {
    //     reschedule |= resource->getNumAllocations() > 0;
    // }

    return reschedule;
}

bool HeteroScheduler::executorEnabledEventHandler(ExecutorEnabledEvent *ev)
{
    assert(ev != NULL);

    return true;
}

bool HeteroScheduler::workerAllocationUpdateEventHandler(WorkerAllocationUpdateEvent *ev)
{
    assert(ev != NULL);

    size_t allowed = ev->target();
    size_t current = this->_schedule->maxNumExecutors;

    this->_schedule->maxNumExecutors = allowed;

    logCInfo(LC_RM | LC_SCHED) << "Global executor share of application " << this->app()->id()
                               << " updated from " << current << " to " << allowed << " (currently used "
                               << this->_schedule->executorPool.size() << ")";

    // Release executors if we have too many
    while (this->numAssignedExecutors > this->_schedule->maxNumExecutors) {
        Executor *executor = this->_schedule->freePool.take();
        if (executor)
            this->release(executor);
        else
            break;
    }

    return true;
}

void HeteroScheduler::run()
{
#ifdef WITHGPERFTOOLS
    ProfilerStart("gperftools.log");
#endif
    App *app = this->app();
    logCInfo(LC_SCHED) << "Starting scheduler for " << app->id();
    app->state(TaskState::running);
    bool appFinished       = false;
    bool executorsReleased = false;

    while (!appFinished || !executorsReleased) {
        bool reschedule = false;
        uint nevents    = 0;
        // Idle while there's no work.
        this->waitForEvent();
        logInfo(2) << "Processing events...";

        auto lock = std::unique_lock<std::mutex>(this->_eventMutex);
        while (!this->_eventQueue.empty()) {
            Event *ev = this->_eventQueue.front();
            lock.unlock();

            if (ev->type() != EventType::Clock)
                logCDebug(LC_SCHED | LC_EVENT) << "Processing event " << *ev;

            switch (ev->type()) {
            case EventType::Clock:
                reschedule |= this->clockEventHandler(static_cast<ClockEvent *>(ev));
                break;
            case EventType::Status:
                reschedule |= this->statusEventHandler(static_cast<StatusEvent *>(ev));
                break;
            case EventType::TaskStarted:
                reschedule |= this->taskStartedEventHandler(static_cast<TaskStartedEvent *>(ev));
                break;
            case EventType::TaskTimeout:
                reschedule |= this->taskTimeoutEventHandler(static_cast<TaskTimeoutEvent *>(ev));
                break;
            case EventType::TaskFinished:
                reschedule |= this->taskFinishedEventHandler(static_cast<TaskFinishedEvent *>(ev));
                break;
            case EventType::StageAdded:
                reschedule |= this->stageAddedEventHandler(static_cast<StageAddedEvent *>(ev));
                break;
            case EventType::StageReady:
                reschedule |= this->stageReadyEventHandler(static_cast<StageReadyEvent *>(ev));
                break;
            case EventType::StageFinished:
                reschedule |= this->stageFinishedEventHandler(static_cast<StageFinishedEvent *>(ev));
                break;
            case EventType::ApplicationFinished:
                reschedule |= this->applicationFinishedEventHandler(
                    static_cast<ApplicationFinishedEvent *>(ev));
                engine->signal(new ApplicationDemandsChangedEvent(this->app(), 0));
                appFinished = true;
                break;
            case EventType::ExecutorAdded:
                reschedule |= this->executorAddedEventHandler(static_cast<ExecutorAddedEvent *>(ev));
                break;
            case EventType::ExecutorDisabled:
                reschedule |= this->executorDisabledEventHandler(static_cast<ExecutorDisabledEvent *>(ev));
                break;
            case EventType::ExecutorEnabled:
                reschedule |= this->executorEnabledEventHandler(static_cast<ExecutorEnabledEvent *>(ev));
                break;
            case EventType::WorkerAllocationUpdate:
                reschedule |= this->workerAllocationUpdateEventHandler(
                    static_cast<WorkerAllocationUpdateEvent *>(ev));
                break;
            case EventType::Kill:
                logInfo(0) << "Shutting down scheduler for " << this->app()->id() << "...";
                engine->signal(new ApplicationDemandsChangedEvent(this->app(), 0));
                appFinished = true;
                break;
            default:
                logError << "Unhandled event " << *ev;
                break;
            }

            executorsReleased = this->_schedule->executorPool.size() == 0;

            delete ev;
            lock.lock();
            this->_eventQueue.pop();
            nevents++;
        }
        lock.unlock();

        if (reschedule) {
            //            auto guard = this->app()->guard(1);

            Time t0 = Time::now();
            this->updateExecutorShares();
            this->assignExecutors();
            this->scheduleApp();
            Time t1 = Time::now();
            if ((t1 - t0).count() > 1000)
                logCDebug(LC_SCHED) << "Schedule recomputed (" << (t1 - t0) << ") after processing " << nevents
                                    << " events";
            logCInfo(LC_SCHED) << "Releasing guard 1";
            //            guard.unlock();
        }
    }

    logInfo(0) << "Stopping scheduler for " << app->id();

#ifdef WITHGPERFTOOLS
    ProfilerStop();
#endif
}

void HeteroScheduler::dumpAllocations()
{
    if (this->app()->state() == TaskState::finished) {
        logCInfo(LC_SCHED) << "All allocations for " << this->app()->id() << ". Application finished. Skipping";
        return;
    }

    logCInfo(LC_SCHED) << "All allocations for " << this->app()->id();
    // for (auto &resourceClass : this->_schedule->resourcePool.pool()) {
    //     for (Resource *resource : resourceClass.second) {
    //         logCInfo(LC_SCHED) << " >>> " << resource->id() << " (" << resource->state()
    //                            << ") managed by executor " << resource->executor()->id() << " on host "
    //                            << resource->executor()->worker()->node()->id() << " (utilization " << std::fixed
    //                            << std::setprecision(3) << resource->utilization() << ")";
    //         resource->dumpAllocations();
    //     }
    // }
}

double HeteroScheduler::getStageWeight(Stage *stage)
{
    double weight = 0UL;

    StageId        sid    = stage->nid();
    StageKey       skey   = stage->key();
    StageSchedule &ssched = this->_schedule->stages.at(sid);
    //    OracleCache &  oc                 = this->_oracleCache.at(skey);
    uint64_t numUnassignedTasks = ssched.nTasks - ssched.nFinishedTasks;

    // for (auto ru : utilization) {
    //     ResourceKey rkey    = ru.first;
    //     size_t      rsize   = ru.second.first;
    //     double      rutil   = ru.second.second;
    //     uint64_t    runtime = EXECOST(rkey, stage->metrics().meanInData(), 0, 0, 0.0, 0).count();

    //     weight += (rsize * runtime * numUnassignedTasks) / (rsize - rutil);
    // }

    weight += numUnassignedTasks;

    return std::max(1.0, weight);
}

double HeteroScheduler::getPathWeight(Stage *stage)
{
    double pw = stage->weight();

    for (auto next : stage->next()) {
        Stage *cs = next->dst();
        double cw = getPathWeight(cs);
        pw += cw + (cw * std::max(0, (cs->level() - 1)) * this->cfgLevelWeight);
    }

    return pw;
}

double HeteroScheduler::getPathWeights(std::multimap<double, Stage *, std::greater<double>> startStages)
{
    double totPathWeights = 0.0;
    // Stages in 'startStages' may already be executed so their weight might change. All other
    // stages cannot be executed just yet, hence their weight will remain constant and doesn't need
    // to be recomputed (it is done once in stageAddedEventHandler and valid until they're being
    // executed).

    auto it = startStages.begin();
    while (it != startStages.end()) {
        double weight = it->first;
        if (weight != -1.0) { // != -1 means we already looked at it.IBM
            it++;
            continue;
        }

        Stage *stage = it->second;
        // 1. Update this stage's weight
        stage->weight(this->getStageWeight(stage));
        // 2. Update/compute the weight of the path from this stage to the end
        stage->weight(getPathWeight(stage));
        // 3. Accumulate weights to compute relative weights later
        totPathWeights += stage->weight();
        //        // 4.
        //        absPathWeights.emplace(stage->weight(), stage);
        it = startStages.erase(it);
        startStages.emplace(stage->weight(), stage);
    }

    // // 5. Compute relative weights
    // for (Stage *stage : startStages) {
    //     double weight = stage->weight() / totalWeight;
    //     relPathWeights.emplace(weight, stage);
    // }

    return totPathWeights;
}

void HeteroScheduler::scheduleApp()
{
    logCInfo(LC_SCHED) << "Scheduling " << this->app()->id();
    std::set<Stage *> toSchedule;

    // 1. Determine which stages need to be scheduled.
    for (auto &entry : this->_schedule->stages) {
        Stage *stage = entry.second.stage;

        if (stage->state() >= TaskState::finished)
            continue;
        if (stage->state() < TaskState::ready)
            continue;
        if (stage->unsatDeps() != 0)
            continue;

        toSchedule.emplace(stage);
    }

    // 3. Schedule stages
    this->_schedule->age++;

    for (Stage *stage : toSchedule) {
        StageId        sid    = stage->nid();
        StageSchedule *ssched = &this->_schedule->stages.at(sid);

        logCDebug(LC_SCHED) << "Scheduling stage " << stage->id();
        logCDebug(LC_SCHED) << " - free/assigned/finished tasks = " << ssched->nFreeTasks << " / "
                            << ssched->nAssignedTasks << " / " << ssched->nFinishedTasks;
        logCDebug(LC_SCHED) << " - stage weight                 = " << std::setprecision(4) << stage->weight();
        logCDebug(LC_SCHED) << " - free/used executors          = " << ssched->freePool.size() << " / "
                            << ssched->usedPool.size();

        scheduleStage(stage);
    }

    // Release executors if we have too many
    while (this->numAssignedExecutors > this->_schedule->maxNumExecutors) {
        Executor *executor = this->_schedule->freePool.take();
        if (executor)
            this->release(executor);
        else
            break;
    }

    this->_schedule->time = Time::now();
}

void HeteroScheduler::scheduleStage(Stage *stage)
{
    StageId        sid      = stage->nid();
    StageSchedule *schedule = &this->_schedule->stages.at(sid);

    logCInfo(LC_SCHED) << "Scheduling " << stage->id() << " on " << schedule->freePool.size() << "+"
                       << schedule->usedPool.size() << " idle/busy executors";

    for (Task *task : stage->tasks()) {
        if (schedule->freePool.size() == 0)
            break;
        if (task->state() == TaskState::ready)
            this->scheduleTask(task, schedule);
    }
}

void HeteroScheduler::scheduleTask(Task *task, StageSchedule *schedule)
{
    assert(task != NULL);
    assert(schedule != NULL);

    logCInfo(LC_SCHED) << "Scheduling "
                       << "/" << task->id();

    if (schedule->freePool.empty())
        return;

    auto      guard    = task->guard();
    Executor *executor = schedule->freePool.take();
    schedule->usedPool.put(executor);
    schedule->assign(task, executor);

    task->state(TaskState::scheduled);
    task->allocation()->executor(executor);

    executor->add(task);
}

void HeteroScheduler::requestResources()
{
    size_t currPar       = 0;
    size_t nextPar       = 0;
    size_t maxPar        = 0;
    size_t nReqResources = 0;
    size_t nGotResources = this->_schedule->maxNumExecutors;

    // Determine the current demand
    for (auto &entry : this->_schedule->stages) {
        StageSchedule &ssched = entry.second;
        Stage *        stage  = ssched.stage;

        // TODO: Create edge set of all next stages
        if (stage->unsatDeps() == 0 && stage->state() >= TaskState::ready &&
            stage->state() < TaskState::finished) {
            currPar += ssched.nTasks - ssched.nFinishedTasks;
            maxPar += ssched.nTasks;
        }
    }

    // Determine the near-term future demand
    for (auto stage : this->_schedule->nextStages) {
        nextPar += stage->size();
    }

    // Keep at least 1 resource around (todo: replace by config option for minimal number of
    // executors)
    nReqResources = this->app()->state() == TaskState::finished ? 0UL : 1UL;

    // Try to get enough resources as we currently need for all running stages
    nReqResources = std::max(currPar, nReqResources);

    // Limit the number of resources depending on the resource scaling factor, but if we already
    // have it, use them - in theory that might be bad, but I think (!) in practice this makes jobs
    // (not stages) finish faster. Otherwise, towards the end of a job, we start releasing executors
    // though we still have pending tasks and I think that's not good - it just prolongs the end of
    // a job.
    // Sidenote: maxPar is being reduced if stages finish.
    nReqResources = std::min(static_cast<size_t>(maxPar * this->cfgResourceScalingFactor), nReqResources);

    if (currPar >= nGotResources)
        nReqResources = std::max(nReqResources, nGotResources);

    nReqResources = std::max(1UL, nReqResources);

    // TODO: Sometimes, when we reduce the number of executors it happens that we have some tasks
    // left but they can't be executed because we free them (because of RSF). I think we should only
    // reduce the number of executors once we really don't have any work for them anymore, i.e. if
    // maxPar < nGotResources to avoid this and to potentially speed up computation.

    // Try to keep around enough resources for the near term, but don't acquire extra resources!
    nReqResources = std::max(std::min(static_cast<size_t>(nextPar * this->cfgResourceScalingFactor),
                                      nGotResources),
                             nReqResources);

    logCInfo(LC_RM | LC_SCHED) << "Updating resource requirements for application " << this->app()->id()
                               << " to " << nReqResources << " executors for " << maxPar << "/" << nextPar
                               << " current/future tasks (rsf=" << this->cfgResourceScalingFactor << ")";

    if (nReqResources != this->numRequestedExecutors)
        engine->signal(new ApplicationDemandsChangedEvent(this->app(), nReqResources));
    this->numRequestedExecutors = nReqResources;
}

// void HeteroScheduler::execute(Task *task, ResourceAllocationEntry *allocation)
// {
//     assert(task != NULL);
//     assert(allocation != NULL);
//     assert(task->state() == TaskState::ready);

//     Executor *executor = resource->executor();

//     if (task->type() != TaskType::idle && task->type() != TaskType::disconnect) {
//         Stage *stage = task->stage();
//         //        StageId sid   = stage->nid();
//         int tidx = task->index();

//         logInfo(10) << "Executing task " << task->id() << " (" << task->state() << ") on " << resource->id()
//                     << " (" << resource->state() << ") executor " << executor->id();

//         this->_schedule->pin(task);
//     }

//     task->scheduler(this);
//     task->state(TaskState::scheduled);
//     executor->add(task);
// }

void HeteroScheduler::updateExecutorShares()
{
    std::multimap<double, Stage *, std::greater<double>> toSchedule;

    // 1. Determine which stages need to be scheduled.
    for (auto &entry : this->_schedule->stages) {
        Stage *stage = entry.second.stage;

        if (stage->state() >= TaskState::finished)
            continue;
        if (stage->state() < TaskState::ready)
            continue;
        if (stage->unsatDeps() != 0)
            continue;

        toSchedule.emplace(-1.0, stage); // -1.0 means not computed yet for getPathWeights

        StageSchedule *schedule  = &this->_schedule->stages.at(stage->nid());
        schedule->targetPoolSize = 0;

        logCInfo(LC_SCHED) << "Updating executor share for stage " << stage->id();
    }

    // 2. Determine/update weight of the paths that starts at each of those stages.
    double totalWeight     = getPathWeights(toSchedule);
    double remainingWeight = totalWeight;

    size_t nTotalExecutors     = std::min(this->_schedule->maxNumExecutors, this->numAssignedExecutors);
    size_t nRemainingExecutors = nTotalExecutors;
    size_t nAssignedExecutors  = 0;

    logCInfo(LC_SCHED) << "Distributing " << nTotalExecutors << " executor(s) among " << toSchedule.size()
                       << " stages";

    // This algorithm does nothing else but match the relative shares (between 0 and 1) of each
    // stage to a number of executors. It's doing it very crude and inefficiently (it's looping
    // rather than determining it directly). The challenge here is to correctly handle fractions of
    // executors. We never want executors to go idle and we also don't want to deprive some stages
    // of their fair share. It's more or less identical to the one in the resource manager that
    // assigns workers to applications.
    while (nRemainingExecutors > nAssignedExecutors && toSchedule.size() > 0) {
        auto entry = toSchedule.begin();
        while (entry != toSchedule.end() && nRemainingExecutors > nAssignedExecutors) {
            Stage *        stage    = entry->second;
            StageSchedule *schedule = &this->_schedule->stages.at(stage->nid());
            uint           demand   = schedule->nTasks - schedule->nFinishedTasks;
            uint &         assigned = schedule->targetPoolSize;

            double allowedShare = stage->weight() / remainingWeight;

            double currShare = static_cast<double>(assigned) / nRemainingExecutors;
            double nextShare = static_cast<double>(assigned + 1) / nRemainingExecutors;
            double currDiff  = abs(currShare - allowedShare);
            double nextDiff  = abs(nextShare - allowedShare);

            // FIXME: Problem: The number of resources for each stage can fluctuate by +/-1 as we
            // add more executors since of minor changes in the next/curr diff relative to the total
            // number of executors. So large stages may lose 1 executor to small stages.
            if (nextDiff < currDiff && assigned < demand) {
                logCDebug(LC_SCHED) << "  - adding 1 executor to " << stage->id() << " (currDiff=" << currDiff
                                    << " nextDiff=" << nextDiff << ")";
                assigned++;
                nAssignedExecutors++;
                entry++;
            } else {
                logCDebug(LC_SCHED) << "  - not adding 1 executor to " << stage->id()
                                    << " (currDiff=" << currDiff << " nextDiff=" << nextDiff << ")";
                // We're done with this stage
                remainingWeight -= stage->weight();
                nRemainingExecutors -= assigned;
                nAssignedExecutors -= assigned;
                schedule->targetPoolSize = assigned;
                entry                    = toSchedule.erase(entry);
            }
        }
    }

    for (auto &entry : this->_schedule->stages) {
        Stage *stage = entry.second.stage;

        if (stage->state() >= TaskState::finished)
            continue;
        if (stage->state() < TaskState::ready)
            continue;
        if (stage->unsatDeps() != 0)
            continue;

        StageSchedule *schedule = &this->_schedule->stages.at(stage->nid());
        logCInfo(LC_SCHED) << "- stage " << stage->id() << " (" << stage->state() << ") with weight "
                           << stage->weight() << " has "
                           << (schedule->freePool.size() + schedule->usedPool.size()) << " executors and gets "
                           << schedule->targetPoolSize << " executors";
    }
}

void HeteroScheduler::assignExecutors()
{
    std::set<Stage *> toSchedule;

    // 1. Determine which stages need to be scheduled.
    for (auto &entry : this->_schedule->stages) {
        Stage *stage = entry.second.stage;

        if (stage->state() >= TaskState::finished)
            continue;
        if (stage->state() < TaskState::ready)
            continue;
        if (stage->unsatDeps() != 0)
            continue;

        toSchedule.emplace(stage);
    }

    // 2. Release resources if necessary and possible.
    for (Stage *stage : toSchedule) {
        StageSchedule *schedule = &this->_schedule->stages.at(stage->nid());

        size_t nCurrentAllocations = schedule->usedPool.size() + schedule->freePool.size();
        size_t nCurrentTarget      = schedule->targetPoolSize;

        if (nCurrentTarget < nCurrentAllocations) {
            uint diff = nCurrentAllocations - nCurrentTarget;
            logCInfo(LC_RM) << "Releasing up to " << diff << " executors from stage " << stage->id()
                            << " (new allocation=" << (nCurrentAllocations + 1) << " target=" << nCurrentTarget
                            << " free=" << schedule->freePool.size() << ")";

            while (!schedule->freePool.empty() && diff > 0) {
                Executor *executor = schedule->freePool.take();
                if (executor) {
                    this->release(executor);
                    diff--;
                }
            }
        }
    }

    // 3. Distribute free executors among stages that need them.
    bool done = false;
    while (this->_schedule->freePool.size() > 0 && !done) {
        done = true;
        for (Stage *stage : toSchedule) {
            StageSchedule *schedule = &this->_schedule->stages.at(stage->nid());

            size_t nCurrentAllocations = schedule->usedPool.size() + schedule->freePool.size();
            size_t nCurrentTarget      = schedule->targetPoolSize;

            if (nCurrentTarget > nCurrentAllocations) {
                Executor *executor = this->_schedule->freePool.take();

                if (executor) {
                    schedule->freePool.put(executor);
                    logCInfo(LC_RM) << "Assigning executor " << executor->id() << " to stage " << stage->id()
                                    << " (new allocation=" << (nCurrentAllocations + 1)
                                    << " target=" << nCurrentTarget << ")";
                    done = false;
                }
            }
        }
    }
}

void HeteroScheduler::release(Executor *executor)
{
    if (this->numAssignedExecutors <= this->_schedule->maxNumExecutors) {
        // Case 1: Release executor to the application so that other stages may use it.
        logCInfo(LC_SCHED) << "Releasing executor " << executor->id() << " back to the application "
                           << this->app()->id();
        this->_schedule->freePool.put(executor);
    } else {
        // Case 2: Release executor to the resoruce manager.
        this->numAssignedExecutors--;
        executor->disconnect();
        logCInfo(LC_SCHED) << "Releasing executor " << executor->id() << " back to the resource manager";
    }
}
