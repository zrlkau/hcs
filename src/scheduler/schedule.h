// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef scheduler_schedule_h
#define scheduler_schedule_h

#include "app/stage.h"
#include "common.h"
#include "resourcemanager/executor.h"
#include "resourcemanager/load.h"
#include "resourcemanager/resourcepool.h"

#include <functional>
#include <map>
#include <vector>

class StageSchedule
{
  public:
    StageSchedule(Schedule *                        schedule,
                  Stage *                           stage,
                  std::unordered_map<Node *, Load> &nodeIoLoad,
                  std::unordered_map<Node *, Load> &nodeLoad)
        : stage(stage), freeTasks(stage->size(), true), assignedTasks(stage->size(), false),
          finishedTasks(stage->size(), false), nFreeTasks(stage->size()), nAssignedTasks(0), nFinishedTasks(0),
          nTasks(stage->size()), startTime(Time::zero()), finishTime(Time::zero()),
          taskFinishTime(stage->size()), nodeIoLoad(nodeIoLoad), nodeLoad(nodeLoad), targetPoolSize(0),
          global(schedule){};

    StageSchedule(const StageSchedule &other) = delete;

    void clear();
    void assign(Task *task, Executor *executor); // temporary
    void finish(Task *task);

    Stage *stage;

    std::vector<bool> freeTasks;     // per stage task pinned flag
    std::vector<bool> assignedTasks; // per stage task assigned flag
    std::vector<bool> finishedTasks; // per stage task pinned flag

    int nFreeTasks;     // number of floating (non-pinned) tasks of this stage
    int nAssignedTasks; // number of assigned (but not pinned) tasks of this stage
    int nFinishedTasks; // number of finished tasks of this stage
    int nTasks;         // total number of tasks of this stage

    Time startTime;
    Time finishTime;

    std::vector<Time> taskFinishTime;

    // // Permanent history of used resources. This is used to check whether shared stage data might
    // // already be cached on said resource. Uses "reference counting".
    std::unordered_map<Executor *, uint> history;
    // // Counts currently active resources. 0 means resource isn't used (anymore).
    // std::unordered_map<Resource *, uint> activeResourcesCounter;

    std::unordered_map<Node *, uint>  usedNodes;
    std::unordered_map<Node *, Load> &nodeIoLoad;
    std::unordered_map<Node *, Load> &nodeLoad;

    ExecutorPool usedPool;
    ExecutorPool freePool;
    uint         targetPoolSize;

    Schedule *global;
};

class Schedule
{
  public:
    Schedule();
    Schedule(const Schedule &other) = delete;

    void clear();
    void assign(Task *task, Executor *executor);
    void finish(Task *task);

    void addStage(Stage *stage);
    void delStage(Stage *stage);

    double getNodeIoLoad(Node *node, Time from, Time to);
    //    double getNodeCmpLoad(Node*, Time from, Time to);
    void addNodeIoLoad(Node *node, Time from, Time to, double load);

    Time time;    // time reference for this schedule (i.e. the current time this schedule was
                  // computed).
    uint64_t age; // number of iterations that went into this schedule.

    std::unordered_map<Node *, Load> nodeIoLoad;
    std::unordered_map<Node *, Load> nodeLoad;
    std::map<StageId, StageSchedule> stages;

    // The stages that will be executed next.
    std::set<Stage *> nextStages;

    ExecutorPool executorPool;
    ExecutorPool freePool;

    size_t maxNumExecutors;
};

#endif
