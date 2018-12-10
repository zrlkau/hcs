// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "schedule.h"
#include "app/stage.h"
#include "resourcemanager/executor.h"

void StageSchedule::clear()
{
    logCInfo(LC_SCHED) << "Clearing schedule of stage " << this->stage->id();

    // Remove all non-pinnned tasks.
    for (int tidx = 0; tidx < this->stage->size(); tidx++) {
        if (!this->assignedTasks[tidx]) {
            this->freeTasks[tidx] = true;
            this->nFreeTasks++;

            this->assignedTasks[tidx] = false;
            this->nAssignedTasks--;

            Task *    task     = this->stage->task(tidx);
            Executor *executor = task->allocation()->executor();

            this->history[executor]--;
        }
    }
}

void StageSchedule::assign(Task *task, Executor *executor)
{
    TaskIdx tidx = task->index();
    logCInfo(LC_SCHED) << "Assigning task " << task->id() << " to executor " << executor->id() << " ("
                       << this->freeTasks[tidx] << "/" << this->assignedTasks[tidx] << "/"
                       << this->finishedTasks[tidx] << ")";

    assert(this->freeTasks[tidx] == true);
    assert(this->assignedTasks[tidx] == false);
    assert(this->finishedTasks[tidx] == false);

    this->freeTasks[tidx]     = false;
    this->assignedTasks[tidx] = true;

    this->nAssignedTasks++;
    this->nFreeTasks--;
    this->history[executor]++;
}

void StageSchedule::finish(Task *task)
{
    TaskIdx   tidx     = task->index();
    Executor *executor = task->allocation()->executor();

    if (!executor) {
        logError << "Finishing task " << task->id() << " on unknown executor!";
    } else if (this->finishedTasks[tidx]) {
        logError << "Finishing task " << task->id() << " that's already finished!";
    } else {
        logCInfo(LC_SCHED) << "Finishing task " << task->id() << " on executor " << executor->id() << " ("
                           << this->freeTasks[tidx] << "/" << this->assignedTasks[tidx] << "/"
                           << this->finishedTasks[tidx] << ")";

        assert(this->freeTasks[tidx] == false);
        assert(this->assignedTasks[tidx] == true);
        assert(this->finishedTasks[tidx] == false);

        this->assignedTasks[tidx] = false;
        this->finishedTasks[tidx] = true;

        this->nAssignedTasks--;
        this->nFinishedTasks++;
    }
}

Schedule::Schedule() : time(Time::now()), age(0), maxNumExecutors(0)
{
    logCInfo(LC_SCHED) << "executor pool = " << &this->executorPool;
    logCInfo(LC_SCHED) << "free pool     = " << &this->freePool;
}

void Schedule::addStage(Stage *stage)
{
    this->stages.emplace(std::piecewise_construct,
                         std::forward_as_tuple(stage->nid()),
                         std::forward_as_tuple(this, stage, this->nodeIoLoad, this->nodeLoad));
}

void Schedule::delStage(Stage *stage)
{
    this->stages.erase(stage->nid());
}

void Schedule::assign(Task *task, Executor *executor)
{
    Stage *stage = task->stage();
    this->stages.at(stage->nid()).assign(task, executor);
}

void Schedule::finish(Task *task)
{
    Stage *stage = task->stage();
    this->stages.at(stage->nid()).finish(task);
}

void Schedule::clear()
{
    for (auto &load : this->nodeIoLoad) {
        load.second.clear();
    }

    for (auto &load : this->nodeLoad) {
        load.second.clear();
    }

    for (auto &stage : stages) {
        StageSchedule &ssched = stage.second;
        ssched.clear();
    }
}
