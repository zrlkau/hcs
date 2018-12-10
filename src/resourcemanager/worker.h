// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef worker_h
#define worker_h

#include "common.h"
#include "resourcemanager/executor.h"

#include <condition_variable>
#include <list>
#include <mutex>
#include <queue>

class Worker
{
  public:
    Worker(Node *node, uint port, uint cores, size_t memory);

    void id(WorkerId id);
    GET(WorkerId, id);
    GET(uint, cores);
    GET(size_t, memory);
    GET(Node *, node);
    GET(uint, port);
    GET(App *, app);

    GETPTR(Executor, executor);
    //    void executor(Executor* executor);

    void incUsageCounter();
    uint getUsageCounter() const;
    
    std::unique_lock<std::mutex> guard();
    void                         lock();
    void                         unlock();

    const App *get();
    void       add(App *app);

  protected:
    SET(uint, cores);
    SET(size_t, memory);
    SET(Node *, node);
    SET(uint, port);
    SET(App *, app);

  private:
    WorkerId _id;
    uint     _cores;
    size_t   _memory;
    Node *   _node;
    uint     _port;
    App *    _app;

    std::mutex _mutex;
    std::atomic<uint> _usageCounter;
    
    Executor _executor;

    std::queue<App *>       _appQueue;
    std::condition_variable _appQueueMutexCondVar;
};

#endif
