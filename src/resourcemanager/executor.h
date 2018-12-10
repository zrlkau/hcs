// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef scheduler_executor_h
#define scheduler_executor_h

#include "app/task.h"
#include "helper/classes.h"
#include "scheduler/schedulerbase.h"

#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <queue>
#include <string>

class Executor
{
  public:
    Executor(Worker *worker, int cores);
    Executor(const Executor &) = delete;
    Executor(Executor &&)      = delete;
    Executor &operator=(const Executor &) = delete;
    Executor &operator=(const Executor &&) = delete;

    GET(int, id);
    GET(Worker *, worker);
    GET(int, cores);
    GET(bool, used);

    Node *      node();
    const Node *node() const;

    void addResource(Resource *resource);
    void delResource(Resource *resource);
    GET(std::list<Resource *> &, resources);

    std::unique_lock<std::mutex> guard();
    const Task *                 get();
    void                         add(Task *task);
    void                         done();

    SETGET(ResourceState, state);
    //    void state(ResourceState state);

    void disconnect();

    GET(uint, tasksSinceIdle);
    GET(uint, taskQueueLevel);

  protected:
    SET(int, id);
    SET(Worker *, worker);
    SET(int, cores);
    SET(bool, used);
    SET(uint, tasksSinceIdle);

  private:
    int     _id;
    int     _cores;
    Worker *_worker;

    std::list<Resource *> _resources;

    std::queue<Task *>      _taskQueue;
    std::condition_variable _taskQueueMutexCondVar;
    std::mutex              _taskQueueMutex;
    std::atomic<uint>       _taskQueueLevel;

    std::atomic<ResourceState> _state;
    DisconnectTask             _disconnectTask;

    bool _used;
    uint _tasksSinceIdle;

    friend Worker;
};

#endif
