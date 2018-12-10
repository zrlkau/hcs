// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//          Adrian Schuepbach <dri@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef __scheduler__resource_manager
#define __scheduler__resource_manager

#include "common.h"
#include "event/event.h"
#include "helper/idmapper.h"
#include "helper/types.h"
#include "resourcemanager/executor.h"
#include "resourcemanager/resource.h"
#include "resourcemanager/worker.h"
#include "resourcemanager/workerpool.h"

#include <list>
#include <unordered_map>
#include <unordered_set>

class ResourceManager
{
  public:
    ResourceManager(Cluster *cluster);
    static void                        run(ResourceManager *rm);
    void                               event(Event *ev);
    std::unique_lock<std::timed_mutex> guard();

    Executor *executor(AppId app, int id);
    Worker *  worker(WorkerId id);

    GET(Cluster *, cluster);

  protected:
    GET(IDMapper<size_t COMMA ResourceKey> &, resourceKeyMapper);
    SET(Cluster *, cluster);

  private:
    void run();

    void load();
    void store();

    void absorb(Resource *resource);

    void waitForEvent();

    bool statusEventHandler(StatusEvent *ev);

    bool workerAddedEventHandler(WorkerAddedEvent *ev);
    bool workerIdleEventHandler(WorkerIdleEvent *ev);

    bool executorAddedEventHandler(ExecutorAddedEvent *ev);

    bool applicationSubmittedEventHandler(ApplicationSubmittedEvent *ev);
    bool applicationFinishedEventHandler(ApplicationFinishedEvent *ev);
    bool applicationDemandsChangedEventHandler(ApplicationDemandsChangedEvent *ev);

    void updateResourceShares();
    void assignResourceShares();

    Cluster *_cluster;

    // List of all resource models by type.
    std::unordered_map<ResourceType, std::unordered_set<ResourceModel>> _models;

    std::unordered_map<std::pair<ResourceType, ResourceModel>, std::list<Resource *>> pool;

    // statistics
    std::unordered_map<ResourceType, uint64_t>  numTypes;
    std::unordered_map<ResourceModel, uint64_t> numModels;
    uint64_t                                    numResources;
    IDMapper<size_t, ResourceKey>               _resourceKeyMapper;

    // A list of all applications known to this resource manager
    std::unordered_map<AppId, App *>              _applications;
    std::set<AppId>                               _applicationsFinished;
    std::unordered_map<AppId, size_t>             _applicationDemands;
    std::unordered_map<AppId, size_t>             _applicationAllocations;
    std::unordered_map<AppId, size_t>             _applicationTargets;
    std::unordered_map<AppId, std::set<Worker *>> _applicationWorkers;
    std::condition_variable_any                   _applicationsCondVar;
    std::timed_mutex                              _mutex;

    uint64_t numApplicationsSubmitted; // total number of applications submitted
    uint64_t numApplicationsFinished; // total number of applications finished
    uint64_t numWorkerAssignments; // total number of worker assignments
    uint64_t applicationsTotalDemand; // total number of resources requested by applications
    
    WorkerPool workerPool;
    WorkerPool workerFreePool;
    WorkerId   workerNextId;

    std::condition_variable _eventSignal;
    std::mutex              _eventMutex;
    std::queue<Event *>     _eventQueue;

    // A list of all executors known to this resource manager
    std::mutex                                                     _executorsMutex;
    std::unordered_map<AppId, std::unordered_map<int, Executor *>> _executors;
};

#endif
