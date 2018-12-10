// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef config_h
#define config_h

#include "helper/classes.h"
#include "helper/macros.h"
#include "helper/types.h"

#include <map>
#include <string>

enum class ConfigSchedulerFlags : uint64_t {
    balanced_neighbor_search = 0,
    random_neighbor_search   = 1,
    consider_io_time         = 2,
    consider_io_size         = 3,
    compact_schedule         = 4,
    best_fit                 = 5,
    signal_executor_idle     = 6,
    training_mode            = 7
};

enum class ConfigVariable : int {
    io_task_weight          = 0,
    io_task_io_weight       = 1,
    cmp_task_weight         = 2,
    cmp_task_io_weight      = 3,
    node_load_weight        = 4,
    interference_mode       = 5,
    resource_scaling_factor = 6
};

ConfigSchedulerFlags operator|(ConfigSchedulerFlags lhs, uint64_t rhs);
ConfigSchedulerFlags operator&(ConfigSchedulerFlags lhs, uint64_t rhs);
ConfigSchedulerFlags operator&(ConfigSchedulerFlags lhs, ConfigSchedulerFlags rhs);
ConfigSchedulerFlags operator~(ConfigSchedulerFlags lhs);

class Config
{
  public:
    Config();
    void setFlag(ConfigSchedulerFlags flag);
    void clearFlag(ConfigSchedulerFlags flag);
    bool getFlag(ConfigSchedulerFlags flag) const;

    double get(ConfigVariable flag) const;
    void   set(ConfigVariable flag, double value);

    void print() const;

    SETGET(uint64_t, seed);
    SETGET(int, lweight);
    SETGET(std::string, perfdbdir);
    SETGET(std::string, listenHost);
    SETGET(uint, listenPort);

  protected:
  private:
    uint64_t    _seed;
    std::string _perfdbdir;
    std::string _listenHost;
    uint        _listenPort;
    int         _lweight;

    ConfigSchedulerFlags             _schedulerFlags;
    std::map<ConfigVariable, double> _variables;
};

#endif
