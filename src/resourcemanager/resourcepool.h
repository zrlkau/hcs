// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef executor_pool_h
#define executor_pool_h

#include "common.h"

#include <map>
#include <mutex>
#include <set>

class ExecutorPool
{
  public:
    ExecutorPool()
    {
    }

    void      put(Executor *executor); // add a executor to this pool
    bool      get(Executor *executor); // get/take a executor from this pool
    Executor *take();                  // take any executor

    size_t size();
    bool   empty();

    bool contains(Executor *executor) const;

    void clear();

    std::unique_lock<std::timed_mutex> guard();

//    CGET(std::set<Executor *> &, pool);

  private:
    std::set<Executor *> _pool;
    std::timed_mutex     _mutex;
};

#endif
