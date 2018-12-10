// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "config.h"
#include "helper/logger.h"
#include "helper/time.h"

#include <iostream>

Config::Config()
{
    this->seed(0);
    this->listenPort(0);
    this->lweight(4);

    this->set(ConfigVariable::resource_scaling_factor, 1.0);
}

void Config::setFlag(ConfigSchedulerFlags flag)
{
    this->_schedulerFlags = this->_schedulerFlags | (1UL << static_cast<uint64_t>(flag));
}

void Config::clearFlag(ConfigSchedulerFlags flag)
{
    this->_schedulerFlags = this->_schedulerFlags & ~(1UL << static_cast<uint64_t>(flag));
}

bool Config::getFlag(ConfigSchedulerFlags flag) const
{
    return (this->_schedulerFlags & (1UL << static_cast<uint64_t>(flag))) ==
           static_cast<ConfigSchedulerFlags>(1UL << static_cast<uint64_t>(flag));
}

double Config::get(ConfigVariable var) const
{
    if (this->_variables.find(var) != this->_variables.end())
	return this->_variables.at(var);
    else
	return 0.0;
}

void Config::set(ConfigVariable var, double value)
{
    this->_variables[var] = value;
}

void Config::print() const
{
    logInfo(0) << "config/seed = " << this->seed();
    logInfo(0) << "config/scheduler/flags/random_neighbor_search = "
               << this->getFlag(ConfigSchedulerFlags::random_neighbor_search);
    logInfo(0) << "config/scheduler/flags/balanced_neighbor_search = "
               << this->getFlag(ConfigSchedulerFlags::balanced_neighbor_search);
    logInfo(0) << "config/scheduler/flags/consider_io_time = "
               << this->getFlag(ConfigSchedulerFlags::consider_io_time);
    logInfo(0) << "config/scheduler/flags/consider_io_size = "
               << this->getFlag(ConfigSchedulerFlags::consider_io_size);
    logInfo(0) << "config/scheduler/flags/compact_schedule = "
               << this->getFlag(ConfigSchedulerFlags::compact_schedule);
    logInfo(0) << "config/scheduler/flags/best_fit = " << this->getFlag(ConfigSchedulerFlags::best_fit);
    logInfo(0) << "config/scheduler/flags/signal_executor_idle = "
               << this->getFlag(ConfigSchedulerFlags::signal_executor_idle);
    logInfo(0) << "config/scheduler/flags/training_mode = "
               << this->getFlag(ConfigSchedulerFlags::training_mode);

    logInfo(0) << "config/variable/io_task_weight = " << this->get(ConfigVariable::io_task_weight);
    logInfo(0) << "config/variable/io_task_io_weight = " << this->get(ConfigVariable::io_task_io_weight);
    logInfo(0) << "config/variable/cmp_task_weight = " << this->get(ConfigVariable::cmp_task_weight);
    logInfo(0) << "config/variable/cmp_task_io_weight = " << this->get(ConfigVariable::cmp_task_io_weight);
    logInfo(0) << "config/variable/node_load_weight = " << this->get(ConfigVariable::node_load_weight);
    logInfo(0) << "config/variable/interference_mode = " << this->get(ConfigVariable::interference_mode);
    logInfo(0) << "config/variable/resource_scaling_factor = " << this->get(ConfigVariable::resource_scaling_factor);
    logInfo(0) << "config/scheduler/level weight = " << this->lweight();
    logInfo(0) << "config/api/hostname = " << this->listenHost();
    logInfo(0) << "config/api/port = " << this->listenPort();
}

ConfigSchedulerFlags operator|(ConfigSchedulerFlags lhs, uint64_t rhs)
{
    return static_cast<ConfigSchedulerFlags>(static_cast<uint64_t>(lhs) | static_cast<uint64_t>(rhs));
}

ConfigSchedulerFlags operator&(ConfigSchedulerFlags lhs, uint64_t rhs)
{
    return static_cast<ConfigSchedulerFlags>(static_cast<uint64_t>(lhs) & static_cast<uint64_t>(rhs));
}

ConfigSchedulerFlags operator&(ConfigSchedulerFlags lhs, ConfigSchedulerFlags rhs)
{
    return static_cast<ConfigSchedulerFlags>(static_cast<uint64_t>(lhs) & static_cast<uint64_t>(rhs));
}

ConfigSchedulerFlags operator~(ConfigSchedulerFlags lhs)
{
    return static_cast<ConfigSchedulerFlags>(~static_cast<uint64_t>(lhs));
}
