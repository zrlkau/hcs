// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//          Adrian Schuepbach <dri@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "resourcemanager/executor.h"
#include "app/app.h"
#include "app/stage.h"
#include "app/task.h"
#include "event/engine.h"
#include "helper/logger.h"
#include "resourcemanager/resource.h"
#include "resourcemanager/worker.h"

Executor::Executor(Worker *worker, int cores) : _taskQueueLevel(0), _state(ResourceState::unknown)
{
    this->id(0);
    this->worker(worker);
    this->cores(cores);
    this->state(ResourceState::idle);
    this->used(false);
    this->tasksSinceIdle(0);
}

void Executor::addResource(Resource *resource)
{
    assert(resource != NULL);

    if (resource->executor() != NULL) {
        logCWarn(LC_RM) << "Not adding resource " << resource->id() << " to executor "
                        << this->worker()->node()->id() << "/" << this->id()
                        << " because resource already belongs to executor " << resource->executor()->id();
        return;
    }

    logCInfo(LC_RM) << "Adding resource " << resource->id() << " (" << this->_resources.size() << ")"
                    << " to executor " << this->worker()->node()->id() << "/" << this->id();

    resource->executor(this);
    resource->state(ResourceState::idle);
    this->_resources.push_back(resource);
}

void Executor::delResource(Resource *resource)
{
    assert(resource != NULL);

    if (resource->executor() != this) {
        logCWarn(LC_RM) << "Not removing resource " << resource->id() << " from executor "
                        << this->worker()->node()->id() << "/" << this->id()
                        << " because resource belongs to executor " << resource->executor()->id();
        return;
    }

    logCInfo(LC_RM) << "Removing resource " << resource->id() << " from executor "
                    << this->worker()->node()->id() << "/" << this->id();

    resource->executor(NULL);
    resource->state(ResourceState::idle);
    for (auto it = this->_resources.begin(); it != this->_resources.end(); it++) {
        if (*it == resource) {
            this->_resources.erase(it);
            break;
        }
    }
}

Node *Executor::node()
{
    return this->worker()->node();
}

const Node *Executor::node() const
{
    return this->worker()->node();
}

std::unique_lock<std::mutex> Executor::guard()
{
    return std::unique_lock<std::mutex>(this->_taskQueueMutex);
}

// void Executor::state(ResourceState state)
// {
//     if (this->_state == state)
//         return;

//     switch (state) {
//     case ResourceState::disabled:
//         logCDebug(LC_RM) << "Disabling executor " << this->id() << " with resources";
//         for (Resource *resource : this->_resources) {
//             resource->state(ResourceState::disabled);
//             logCDebug(LC_RM) << " - " << resource->id() << " (" << resource->state() << ")";
//         }
//         //        this->_scheduler->isDisabled(this);
//         break;
//     case ResourceState::idle: // also enable
//         logCDebug(LC_RM) << "Enabling executor " << this->id() << " with resources";
//         if (this->_state == ResourceState::disabled) {
//             for (Resource *resource : this->_resources) {
//                 resource->state(ResourceState::idle);
//                 logCDebug(LC_RM) << " - " << resource->id() << " (" << resource->state() << ")";
//             }
//             //            this->_scheduler->isIdle(this);
//         }
//         break;
//     case ResourceState::busy:
//         break;
//     case ResourceState::error:
//         break;
//     default:
//         break;
//     }

//     this->_state = state;
// }

void Executor::disconnect()
{
    this->_disconnectTask.allocation()->executor(this);
    this->add(&this->_disconnectTask);
}

const Task *Executor::get()
{
    Time t0    = Time::now();
    auto guard = this->guard();

    this->_taskQueueMutexCondVar.wait(guard, [this] { return !this->_taskQueue.empty(); });
    this->_state = ResourceState::busy;
    this->_taskQueueLevel--;

    assert(this->_taskQueue.empty() == false);

    Task *task = this->_taskQueue.front();
    this->_taskQueue.pop();
    Time t1 = Time::now();

    if (Duration(t1 - t0) > Duration(std::chrono::microseconds(1)))
        logCDebug(LC_RM | LC_SCHED) << "Executor " << this->worker()->node()->id() << "/" << this->id()
                                    << " was idle for " << (t1 - t0);

    if (task->type() == TaskType::idle) {
        logCDebug(LC_RM | LC_SCHED) << "Executor " << this->id() << " is going to perform maintenance";
    }
    if (task->type() == TaskType::disconnect) {
        this->state(ResourceState::disabled);
    }

    if (task->type() != TaskType::idle && task->type() != TaskType::disconnect) {
        assert(this->worker()->app() == task->stage()->app());
    }
    task->state(TaskState::running);
    if (task->stage() != NULL) {
        logCInfo(LC_SCHED) << "Executor " << this->id() << " is going to execute task "
                           << task->stage()->app()->id() << "/" << task->id();
	if (this->worker()->getUsageCounter() == 0) {
	    logCWarn(LC_RM) << "Executor " << this->id() << " is cold.";
	}
	    
	this->worker()->incUsageCounter();
    } else {
        logCInfo(LC_SCHED) << "Executor " << this->id() << " is going to execute task "
                           << "noapp/" << task->id();
    }

    // for (auto resource : this->_resources) {
    // 	logCInfo(LC_RM) << "resource " << resource->id() << " / " << &this->_resources << " / " << resource->getNumAllocations();
    // 	resource->dumpAllocations();
    // }

    engine->signal(new TaskStartedEvent(this->worker()->app(), task, this));
    engine->event(new TaskTimeoutEvent(this->worker()->app(), task, this, task->allocation()->to()));
    return task;
}

void Executor::add(Task *task)
{
    logCInfo(LC_SCHED) << "Adding task " << task->id() << " to executor " << this->id() << " (" << this->state()
                       << ")";

    // task can be NULL in which case we'll do maintenance on the executor
    auto guard = this->guard();

    if (this->state() == ResourceState::disabled) {
        logCWarn(LC_SCHED) << "Assigned task " << task->id() << " to disabled executor " << this->id();
        return;
    }

    if (task->type() != TaskType::idle)
        this->_tasksSinceIdle++;
    else
        this->_tasksSinceIdle = 0;

    // Set to true upon first usage.
    this->_used = true;

    this->_taskQueueLevel++;
    this->_taskQueue.push(task);
    this->_taskQueueMutexCondVar.notify_all();

    logCDebug(LC_SCHED) << "Done adding task " << task->id() << " to executor " << this->id();

    // for (auto resource : this->_resources) {
    // 	logCInfo(LC_RM) << "resource " << resource->id() << " / " << &this->_resources << " / " << resource->getNumAllocations();
    // 	resource->dumpAllocations();
    // }
}

void Executor::done()
{
    this->_state = ResourceState::idle;
}
