// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef worker_pool_h
#define worker_pool_h

#include "common.h"

#include <map>
#include <mutex>
#include <set>
#include <unordered_map>

class WorkerPool
{
  public:
    WorkerPool() : _nextNode(_nodepool.begin())
    {
    }

    void    put(Worker *worker);     // add a worker to this pool
    Worker *get(WorkerId wid); // get a worker from this pool
    Worker *take(WorkerId wid);      // take a worker out of this pool
    Worker *take(void);              // take any worker out of this pool

    size_t size(void);
    void   clear(void);

    std::unique_lock<std::timed_mutex> guard(void);

    std::string getLoad(void);
    
    //CGET(std::unordered_map<WorkerId COMMA Worker *> &, pool);

  private:
    std::unordered_map<WorkerId, Worker *>                                        _workerpool;
    std::map<Node *, std::multimap<uint, Worker *, std::greater<uint>>>           _nodepool;
    std::map<Node *, std::multimap<uint, Worker *, std::greater<uint>>>::iterator _nextNode;

    std::timed_mutex _mutex;
};

#endif
