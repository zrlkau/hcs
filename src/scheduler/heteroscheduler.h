// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef scheduler_heteroscheduler_h
#define scheduler_heteroscheduler_h

#include "app/app.h"
#include "common.h"
#include "event/event.h"
#include "resourcemanager/resource.h"
#include "resourcemanager/resourcemanager.h"
#include "scheduler/oracle.h"
#include "scheduler/schedule.h"
#include "scheduler/schedulerbase.h"

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>

class HeteroScheduler : public SchedulerBase
{
  public:
    HeteroScheduler(App *app, ResourceManager *resourcemanager);
    ~HeteroScheduler();
    HeteroScheduler(const HeteroScheduler &) = delete;
    HeteroScheduler(HeteroScheduler &&)      = delete;
    HeteroScheduler &operator=(const HeteroScheduler &) = delete;
    HeteroScheduler &operator=(const HeteroScheduler &&) = delete;

    void run();
    void event(Event *ev);

  protected:
    void updateExecutorShares();
    void assignExecutors();

    void scheduleApp();
    void scheduleStage(Stage *stage);
    void scheduleTask(Task *task, StageSchedule* schedule);

    void dumpAllocations();

    void execute(Task *task, ResourceAllocationEntry *allocation);
    void release(Executor* executor);
    
    double getPathWeights(std::multimap<double, Stage *, std::greater<double>> stages);
    double getPathWeight(Stage *stage);
    double getStageWeight(Stage *stage);


    
    SETGET(Schedule *, schedule);
    SETGET(Oracle *, oracle);

  private:
    void waitForEvent();

    bool clockEventHandler(ClockEvent *ev);
    bool statusEventHandler(StatusEvent *ev);
    bool taskTimeoutEventHandler(TaskTimeoutEvent *ev);
    bool taskStartedEventHandler(TaskStartedEvent *ev);
    bool taskFinishedEventHandler(TaskFinishedEvent *ev);
    bool stageAddedEventHandler(StageAddedEvent *ev);
    bool stageReadyEventHandler(StageReadyEvent *ev);
    bool stageFinishedEventHandler(StageFinishedEvent *ev);
    bool applicationFinishedEventHandler(ApplicationFinishedEvent *ev);
    bool executorAddedEventHandler(ExecutorAddedEvent *ev);
    bool executorDisabledEventHandler(ExecutorDisabledEvent *ev);
    bool executorEnabledEventHandler(ExecutorEnabledEvent *ev);
    bool workerAllocationUpdateEventHandler(WorkerAllocationUpdateEvent *ev);

    void requestResources();

    Schedule *                                _schedule;
    std::unordered_map<StageKey, OracleCache> _oracleCache;
    Oracle *                                  _oracle;

    size_t numAssignedExecutors;
    size_t numRequestedExecutors;
    
    Time _time;

    bool _train;

    std::condition_variable _eventSignal;
    std::mutex              _eventMutex;
    std::queue<Event *>     _eventQueue;

    double cfgIoTaskWeight;
    double cfgIoTaskIoWeight;
    double cfgCmpTaskWeight;
    double cfgCmpTaskIoWeight;
    double cfgNodeLoadWeight;
    int    cfgInterferenceMode;
    double cfgResourceScalingFactor;

    double cfgLevelWeight;
};

#endif
