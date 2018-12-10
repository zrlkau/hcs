// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef resource_load_h
#define resource_load_h

#include "common.h"

#include <list>

class LoadEntry
{
  public:
    LoadEntry(Time from, Time to, double load) : from(from), to(to), load(load){};

    Time   from;
    Time   to;
    double load;
};

class Load
{
  public:
    Load()
    {
        this->_load.emplace_back(epoch_start, Time::max(), 0.0);
    }

    double getLoad(Time from, Time to);              // get load
    void   addLoad(Time from, Time to, double load); // add load

    void clear();

    GET(std::list<LoadEntry> &, load);

  private:
    std::list<LoadEntry> _load;
};
#endif
