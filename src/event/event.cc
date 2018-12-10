// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//          Adrian Schuepbach <dri@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "event/event.h"
#include "app/app.h"
#include "app/task.h"
#include "event/engine.h"
#include "helper/logger.h"
#include "helper/time.h"
#include "resourcemanager/resourcemanager.h"

#include <iostream>

std::ostream &operator<<(std::ostream &os, const EventType &obj)
{
    switch (obj) {
    case EventType::TaskScheduled:
        os << "TaskScheduled";
        break;
    case EventType::TaskStarted:
        os << "TaskStarted";
        break;
    case EventType::TaskTimeout:
        os << "TaskTimeout";
        break;
    case EventType::TaskFinished:
        os << "TaskFinished";
        break;

    case EventType::StageAdded:
        os << "StageAdded";
        break;
    case EventType::StageReady:
        os << "StageReady";
        break;
    case EventType::StageScheduled:
        os << "StageScheduled";
        break;
    case EventType::StageStarted:
        os << "StageStarted";
        break;
    case EventType::StageFinished:
        os << "StageFinished";
        break;

    case EventType::ApplicationSubmitted:
        os << "ApplicationSubmitted";
        break;
    case EventType::ApplicationStarted:
        os << "ApplicationStarted";
        break;
    case EventType::ApplicationFinished:
        os << "ApplicationFinished";
        break;
    case EventType::ApplicationDemandsChanged:
        os << "ApplicationDemandsChanged";
        break;

    case EventType::ExecutorAdded:
        os << "ExecutorAdded";
        break;
    case EventType::ExecutorRemoved:
        os << "ExecutorRemoved";
        break;
    case EventType::ExecutorDisabled:
        os << "ExecutorDisabled";
        break;
    case EventType::ExecutorDisconnect:
        os << "ExecutorDisconnect";
        break;
    case EventType::ExecutorDisconnectLocallyDone:
        os << "ExecutorDisconnectLocallyDone";
        break;
    case EventType::ExecutorEnabled:
        os << "ExecutorEnabled";
        break;

    case EventType::WorkerAdded:
        os << "WorkerAdded";
        break;
    case EventType::WorkerIdle:
        os << "WorkerIdle";
        break;
    case EventType::WorkerAllocationUpdate:
        os << "WorkerAllocationUpdate";
        break;

    case EventType::Kill:
        os << "Kill";
        break;
    case EventType::Clock:
        os << "Clock";
        break;
    case EventType::Status:
        os << "Status";
        break;

    case EventType::Nop:
        os << "Nop";
        break;

    default:
        os << "Unknown";
        break;
    }
    return os;
}

std::ostream &operator<<(std::ostream &os, const Event &obj)
{
    return obj.print(os);
}

/****************************************************************************************************
 *                                             Task Events                                          *
 ****************************************************************************************************/
Event *TaskScheduledEvent::copy() const
{
    return static_cast<Event *>(new TaskScheduledEvent(*this));
}
std::ostream &TaskScheduledEvent::print(std::ostream &os) const
{
    const App *app = this->app();
    if (app)
        os << this->type() << " for task " << app->id() << " / " << this->task()->id() << " on "
           << this->executor()->id();
    else
        os << this->type() << " for task " << this->task()->id() << " on " << this->executor()->id();
    return os;
}

Event *TaskStartedEvent::copy() const
{
    return static_cast<Event *>(new TaskStartedEvent(*this));
}
std::ostream &TaskStartedEvent::print(std::ostream &os) const
{
    const App *app = this->app();
    if (app)
        os << this->type() << " for task " << app->id() << " / " << this->task()->id() << " on "
           << this->executor()->id();
    else
        os << this->type() << " for task " << this->task()->id() << " on " << this->executor()->id();
    return os;
}

Event *TaskTimeoutEvent::copy() const
{
    return static_cast<Event *>(new TaskTimeoutEvent(*this));
}
std::ostream &TaskTimeoutEvent::print(std::ostream &os) const
{
    const App *app = this->app();
    if (app)
        os << this->type() << " for task " << app->id() << " / " << this->task()->id() << " on "
           << this->executor()->id();
    else
        os << this->type() << " for task " << this->task()->id() << " on " << this->executor()->id();
    return os;
}

Event *TaskFinishedEvent::copy() const
{
    return static_cast<Event *>(new TaskFinishedEvent(*this));
}
std::ostream &TaskFinishedEvent::print(std::ostream &os) const
{
    const App *app = this->app();
    if (app)
        os << this->type() << " for task " << app->id() << " / " << this->task()->id() << " on "
           << this->executor()->id();
    else
        os << this->type() << " for task " << this->task()->id() << " on " << this->executor()->id();
    return os;
}

/****************************************************************************************************
 *                                            Stage Events                                          *
 ****************************************************************************************************/
Event *StageAddedEvent::copy() const
{
    return static_cast<Event *>(new StageAddedEvent(*this));
}
std::ostream &StageAddedEvent::print(std::ostream &os) const
{
    os << this->type() << " for stages {";
    for (auto &entry : this->stages()) {
        os << entry.first->id() << ", ";
    }
    os.seekp(-2, std::ios_base::end) << "} ";
    return os;
}

Event *StageReadyEvent::copy() const
{
    return static_cast<Event *>(new StageReadyEvent(*this));
}
std::ostream &StageReadyEvent::print(std::ostream &os) const
{
    const App *app = this->app();
    if (app)
        os << this->type() << " for stage " << app->id() << " / " << this->stage()->id();
    else
        os << this->type() << " for stage " << this->stage()->id();
    return os;
}

Event *StageScheduledEvent::copy() const
{
    return static_cast<Event *>(new StageScheduledEvent(*this));
}
std::ostream &StageScheduledEvent::print(std::ostream &os) const
{
    const App *app = this->app();
    if (app)
        os << this->type() << " for stage " << app->id() << " / " << this->stage()->id();
    else
        os << this->type() << " for stage " << this->stage()->id();
    return os;
}

Event *StageStartedEvent::copy() const
{
    return static_cast<Event *>(new StageStartedEvent(*this));
}
std::ostream &StageStartedEvent::print(std::ostream &os) const
{
    const App *app = this->app();
    if (app)
        os << this->type() << " for stage " << app->id() << " / " << this->stage()->id();
    else
        os << this->type() << " for stage " << this->stage()->id();
    return os;
}

Event *StageFinishedEvent::copy() const
{
    return static_cast<Event *>(new StageFinishedEvent(*this));
}
std::ostream &StageFinishedEvent::print(std::ostream &os) const
{
    const App *app = this->app();
    if (app)
        os << this->type() << " for stage " << app->id() << " / " << this->stage()->id();
    else
        os << this->type() << " for stage " << this->stage()->id();
    return os;
}

/****************************************************************************************************
 *                                           Application Events                                     *
 ****************************************************************************************************/
Event *ApplicationSubmittedEvent::copy() const
{
    return static_cast<Event *>(new ApplicationSubmittedEvent(*this));
}
std::ostream &ApplicationSubmittedEvent::print(std::ostream &os) const
{
    os << this->type() << " for application " << this->app()->id();
    return os;
}

Event *ApplicationStartedEvent::copy() const
{
    return static_cast<Event *>(new ApplicationStartedEvent(*this));
}
std::ostream &ApplicationStartedEvent::print(std::ostream &os) const
{
    os << this->type() << " for application " << this->app()->id();
    return os;
}
Event *ApplicationFinishedEvent::copy() const
{
    return static_cast<Event *>(new ApplicationFinishedEvent(*this));
}
std::ostream &ApplicationFinishedEvent::print(std::ostream &os) const
{
    os << this->type() << " for application " << this->app()->id();
    return os;
}

/****************************************************************************************************
 *                                             Misc Events                                          *
 ****************************************************************************************************/
Event *KillEvent::copy() const
{
    return static_cast<Event *>(new KillEvent(*this));
}
std::ostream &KillEvent::print(std::ostream &os) const
{
    os << this->type();
    return os;
}

Event *ClockEvent::copy() const
{
    return static_cast<Event *>(new ClockEvent(*this));
}
std::ostream &ClockEvent::print(std::ostream &os) const
{
    os << this->type();
    return os;
}

Event *StatusEvent::copy() const
{
    return static_cast<Event *>(new StatusEvent(*this));
}
std::ostream &StatusEvent::print(std::ostream &os) const
{
    os << this->type();
    return os;
}

/****************************************************************************************************
 *                                          Resource Events                                         *
 ****************************************************************************************************/
Event *ExecutorAddedEvent::copy() const
{
    return static_cast<Event *>(new ExecutorAddedEvent(*this));
}
std::ostream &ExecutorAddedEvent::print(std::ostream &os) const
{
    os << this->type() << " for executor " << this->executor()->id();
    return os;
}

Event *ExecutorRemovedEvent::copy() const
{
    return static_cast<Event *>(new ExecutorRemovedEvent(*this));
}
std::ostream &ExecutorRemovedEvent::print(std::ostream &os) const
{
    os << this->type() << " for executor " << this->executor()->id();
    return os;
}

Event *ExecutorEnabledEvent::copy() const
{
    return static_cast<Event *>(new ExecutorEnabledEvent(*this));
}
std::ostream &ExecutorEnabledEvent::print(std::ostream &os) const
{
    os << this->type() << " for executor " << this->executor()->id();
    return os;
}

Event *ExecutorDisabledEvent::copy() const
{
    return static_cast<Event *>(new ExecutorDisabledEvent(*this));
}
std::ostream &ExecutorDisabledEvent::print(std::ostream &os) const
{
    os << this->type() << " for executor " << this->executor()->id();
    return os;
}

Event *ExecutorDisconnectEvent::copy() const
{
    return static_cast<Event *>(new ExecutorDisconnectEvent(*this));
}
std::ostream &ExecutorDisconnectEvent::print(std::ostream &os) const
{
    os << this->type() << " for executor " << this->executor()->id();
    return os;
}

Event *ExecutorDisconnectLocallyDoneEvent::copy() const
{
    return static_cast<Event *>(new ExecutorDisconnectLocallyDoneEvent(*this));
}
std::ostream &ExecutorDisconnectLocallyDoneEvent::print(std::ostream &os) const
{
    os << this->type() << " for executor " << this->executor()->id();
    return os;
}

Event *NopEvent::copy() const
{
    return static_cast<Event *>(new NopEvent(*this));
}
std::ostream &NopEvent::print(std::ostream &os) const
{
    os << this->type() << " for executor " << this->executor()->id();
    return os;
}

Event *ApplicationDemandsChangedEvent::copy() const
{
    return static_cast<Event *>(new ApplicationDemandsChangedEvent(*this));
}
std::ostream &ApplicationDemandsChangedEvent::print(std::ostream &os) const
{
    os << this->type() << " for application " << this->app()->id() << " to " << this->demand();
    return os;
}

Event *WorkerAddedEvent::copy() const
{
    return static_cast<Event *>(new WorkerAddedEvent(*this));
}
std::ostream &WorkerAddedEvent::print(std::ostream &os) const
{
    os << this->type() << " for worker " << this->worker()->id();
    return os;
}

Event *WorkerIdleEvent::copy() const
{
    return static_cast<Event *>(new WorkerIdleEvent(*this));
}
std::ostream &WorkerIdleEvent::print(std::ostream &os) const
{
    os << this->type() << " for worker " << this->worker()->id();
    return os;
}

Event *WorkerAllocationUpdateEvent::copy() const
{
    return static_cast<Event *>(new WorkerAllocationUpdateEvent(*this));
}
std::ostream &WorkerAllocationUpdateEvent::print(std::ostream &os) const
{
    os << this->type() << " for application " << this->app()->id() << " with target " << this->target();
    return os;
}
