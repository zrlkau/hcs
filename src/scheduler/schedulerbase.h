// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef scheduler_schedulerbase_h
#define scheduler_schedulerbase_h

#include <atomic>
#include <random>
#include <tuple>

#include "cluster/cluster.h"

enum class SchedulerState {
    unknown,
    idle,   // nothing to do (or interrupted)
    active, // actively computing a schedule right now
};

std::ostream &operator<<(std::ostream &os, const SchedulerState &obj);

class SchedulerBase
{
  public:
    SchedulerBase(App *app, ResourceManager *resourcemanager);
    SchedulerBase(const SchedulerBase &) = delete;
    SchedulerBase(SchedulerBase &&)      = delete;
    SchedulerBase &operator=(const SchedulerBase &) = delete;
    SchedulerBase &operator=(const SchedulerBase &&) = delete;

    size_t getAppID();
    size_t getTaskID();

    GET(App *, app);
    GET(ResourceManager *, resourcemanager);
    GET(SchedulerState, state);

    virtual void run()            = 0;
    virtual void event(Event *ev) = 0;

protected:
    SET(App *, app);
    SET(ResourceManager *, resourcemanager);
    SET(SchedulerState, state);

    uint64_t rand(uint64_t max);
    void     seed(uint64_t seed); // reseed

  private:
    App *            _app;
    ResourceManager *_resourcemanager;

    std::default_random_engine              _rd;
    std::uniform_int_distribution<uint64_t> _rng;

    SchedulerState _state;
};

#endif
