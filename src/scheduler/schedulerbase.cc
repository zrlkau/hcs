// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "schedulerbase.h"
#include "common.h"

SchedulerBase::SchedulerBase(App *app, ResourceManager *resourcemanager) : _rng(0, ULLONG_MAX)
{
    this->app(app);
    this->resourcemanager(resourcemanager);
    this->state(SchedulerState::idle);
    this->seed(config->seed());
}

void SchedulerBase::seed(uint64_t seed)
{
    this->_rd.seed(seed);
}

uint64_t SchedulerBase::rand(uint64_t max)
{
    return (_rng(_rd) % max);
}

std::ostream &operator<<(std::ostream &os, const SchedulerState &obj)
{
    switch (obj) {
    case SchedulerState::idle:
        os << "idle";
        break;
    case SchedulerState::active:
        os << "active";
        break;
    default:
        os << "unknown";
        break;
    }
    return os;
}
