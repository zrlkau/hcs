// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef base_vertex_h
#define base_vertex_h

#include "common.h"
#include "connector-simple.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace graph_simple {
using boost::property_tree::ptree;

/*
    1. May work well for the cluster
    IoPort {
      IoPort& peer;    // remote I/O port
      Host&   host;    // host where this I/O port is physically
      size_t  idx;     // globally unique index, used to compute
                       // costs between two ioports.
      vector<IoPort*> hierarchy; // the location of this IoPort in the
                                    graph hierarch.
      Domain& domain;  // local memory domain (NUMA)
      IoDev&  iodev;   // local I/O device (NIC, RNIC)

      struct properties { // may be extended later.
        int latency;   // initial latency
        int bandwidth; // max. mandwidth.
      }
    }

    Io {
      Task& next;
      struct properties {
        int volume;
        int bandwidth;
        int chunksize;
      }
    }
   */

template <class V, class C> class BaseVertex
{
  public:
    BaseVertex();
    BaseVertex(const ptree &data);
    ptree store() const;

    C &add_connector(std::string iodev);
    C &add_connector(std::string iodev, const ptree &data);

  protected:
  private:
    std::map<std::string, C> connectors;
};

template <class V, class C> BaseVertex<V, C>::BaseVertex()
{
}

template <class V, class C> BaseVertex<V, C>::BaseVertex(const ptree &data)
{
}

template <class V, class C> ptree BaseVertex<V, C>::store() const
{
    ptree data;
    return data;
}

template <class V, class C> C &BaseVertex<V, C>::add_connector(std::string iodev)
{
    connectors.emplace(iodev, *this);
    return connectors.back();
}

template <class V, class C> C &BaseVertex<V, C>::add_connector(std::string iodev, const ptree &data)
{
    connectors.emplace_back(iodev, *this, data);
    return connectors.back();
}

} // namespace graph_simple
#endif
