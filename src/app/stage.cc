// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "app/stage.h"
#include "app/app.h"
#include "common.h"
#include "event/engine.h"
#include "event/event.h"

#include <map>

Stage::Stage(const std::string &id,
             StageId            nid,
             const std::string &function,
             StageKey           key,
             App *              app,
             TaskType           type,
             size_t             size)
    : _state(TaskState::unknown), _level(0), _weight(0.0), _mark(0)
{
    std::ostringstream os;
    os << app->id() << "/" << id;
    this->id(os.str());
    this->nid(nid);
    this->app(app);
    this->type(type);
    this->function(function);
    this->key(key);
    this->size(size);
    this->initialized(false);
    this->unsatDeps(0);

    for (uint ti = 0; ti < this->size(); ti++) {
        std::ostringstream os;
        os << this->id() << "t" << ti;
        this->_allTasks.emplace(std::piecewise_construct,
                                std::forward_as_tuple(ti),
                                std::forward_as_tuple(os.str(), static_cast<TaskIdx>(ti), this));
        this->_tasks.push_back(&this->_allTasks.at(ti));
    }

    this->state(TaskState::unready);

    this->_metrics.submitted(Time::now());

    logInfo(1) << "Adding new stage " << this->id() << " with " << this->size() << " tasks for "
               << this->function() << " hash " << this->key();
}

Stage::~Stage()
{
}

std::unique_lock<std::timed_mutex> Stage::guard()
{
    auto lock = std::unique_lock<std::timed_mutex>(this->_mutex, std::defer_lock);
    while (!lock.try_lock_for(10s)) {
        logWarn << "Unable to acquire lock for stage " << this->id() << " within the last 10s";
    }
    return lock;
}
void Stage::prev(Io *in)
{
    size_t unsat = 0;

    logInfo(1) << "Connecting " << *in->src() << " -> " << *this;

    this->_prev.insert(in);

    for (auto prev : this->_prev) {
        if (prev->src()->state() < TaskState::finished)
            unsat++;
    }

    this->_unsatDeps = unsat;
}

void Stage::next(Io *out)
{
    this->_next.insert(out);
}

void Stage::level(int level)
{
    this->_level = level;
    for (auto prev : this->_prev) {
        prev->src()->level(std::max(prev->src()->level(), level + 1));
    }
}

Task *Stage::task(size_t index)
{
    if (index < this->_allTasks.size())
        return &this->_allTasks.at(index);
    else
        return NULL;
}

void Stage::taskReady(Task *task)
{
    assert(task != NULL);

    logInfo(2) << "Task " << task->id() << " notified me (" << this->id() << ") that it's ready";
}

void Stage::taskScheduled(Task *task)
{
    assert(task != NULL);

    logInfo(2) << "Task " << task->id() << " notified me (" << this->id() << ") that it's scheduled";

    if (this->state() < TaskState::scheduled)
        this->state(TaskState::scheduled);
}

void Stage::taskRunning(Task *task)
{
    assert(task != NULL);

    logInfo(2) << "Task " << task->id() << " notified me (" << this->id() << ") that it's running";

    if (this->state() < TaskState::running) {
        this->state(TaskState::running);
    }
}

void Stage::taskFinished(Task *task)
{
    assert(task != NULL);

    logInfo(2) << "Task " << task->id() << " notified me (" << this->id() << ") that it's finished";
}

void Stage::sortTasks()
{
    logInfo(2) << "Sorting tasks";
    if (!config->getFlag(ConfigSchedulerFlags::training_mode))
        this->_tasks.sort(Task::sortByInputSizeFunc);
    //	std::shuffle(this->_tasks.begin(), this->_tasks.end());
    // for (Task *task : this->_tasks) {
    // 	logInfo(0) << " - " << task->id() << " reads " << task->metrics().inData();
    // }
}

void Stage::state(TaskState state)
{
    auto guard = this->guard();

    logCDebug(LC_SCHED) << "Transitioning stage " << this->id() << " from " << this->state() << " to " << state;

    if (this->_state == state)
        return;

    switch (state) {
    case TaskState::unknown:
        break;

    case TaskState::unready:
        break;

    case TaskState::ready:
        assert(this->_state == TaskState::unready);

        this->_metrics.ready(Time::now());
        // Spark sometimes skips stages and unfortunately there doesn't seem to be a dedicated
        // signal to do so. What it does is just mark the next stage as ready. This means that if
        // this stage was marked ready, all ancestors must be finished. If not, then they have been
        // skipped.
        for (auto in : this->prev()) {
            Stage *prev = in->src();
            // There might be a small delay before the previous stage transitions from running to
            // finished. We must not interrupt that but if the previous stage was skipped it will
            // never have been in the 'running' stage in the first place.
            if (prev->state() < TaskState::running)
                prev->state(TaskState::finished);
        }

        for (auto task : this->tasks()) {
            task->state(TaskState::ready);
        }
        //	engine->signal(new StageReadyEvent(this));
        break;

    case TaskState::scheduled:
        assert(this->_state == TaskState::ready);
        this->_metrics.scheduled(Time::now());
        //	engine->signal(new StageScheduledEvent(this));
        break;

    case TaskState::running:
        assert(this->_state == TaskState::scheduled);
        this->_metrics.started(Time::now());
        //	engine->signal(new StageStartedEvent(this));
        break;

    case TaskState::finished:
        // Handle skipped stages (the predecessors, which are skipped, are marked finished right
        // away).
        this->_metrics.finished(Time::now());
        this->_metrics.runtime(this->_metrics.finished() - this->_metrics.started());

        for (auto in : this->prev()) {
            Stage *prev = in->src();
            if (prev->state() < TaskState::finished)
                prev->state(TaskState::finished);
        }

        for (auto next : this->next()) {
            next->dst()->decrUnsatDeps();
        }

        //	engine->signal(new StageFinishedEvent(this));
        break;
    }

    this->_state = state;
}

void Stage::decrUnsatDeps()
{
    this->_unsatDeps--;
}

void Stage::restype(ResourceType restype)
{
    this->_restype.emplace(restype);
}

std::ostream &operator<<(std::ostream &os, const Stage &obj)
{
    std::ostringstream out;

    out << "Stage " << obj.id() << " (#t=" << obj.tasks().size() << " state=" << obj.state() << " prev={";
    size_t num = obj.prev().size();
    for (const auto &prev : obj.prev()) {
        if (--num > 0)
            out << prev->src()->id() << " ";
        else
            out << prev->src()->id();
    }

    out << "} next={";
    num = obj.next().size();
    for (const auto &next : obj.next()) {
        if (--num > 0)
            out << next->dst()->id() << " ";
        else
            out << next->dst()->id();
    }

    out << "})";
    out << "}";

    os << out.str();
    return os;
}
