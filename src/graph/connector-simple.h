// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef connector_simple_h
#define connector_simple_h

#include "common.h"

#include <boost/property_tree/ptree.hpp>

namespace graph_simple {
using boost::property_tree::ptree;

template <class V, class C> class BaseConnector
{
  public:
    BaseConnector(V &owner);
    BaseConnector(V &owner, const ptree &data);
    const ptree store() const;

    V &get_owner() const
    {
        return this->owner;
    }

    void set_peer(C &peer)
    {
        this->peer = boost::optional<C &>(peer);
    }
    boost::optional<C &> get_peer() const
    {
        return this->peer;
    }

    SETGET(std::string, id);

  protected:
  private:
    /*
      The connector of a peer node. This allows to implement various
      topologies, e.g.

      1. Tree and Multi-Tree Topology.
         - Connectors have parent/child relationship.
         
                              +---[0]---+
                Root Switch   |    A    |
                              +[1]---[2]+
                               /       \
                       .------'         '------.
                      /                         \
                +---[0]---+                 +---[0]---+
                |    B    |   TofR Switch   |    C    |
                +[1]---[2]+                 +[1]---[2]+
                 /       \                   /       \
                /         \                 /         \
               /           \               /           \
         +---[0]---+   +---[0]---+   +---[0]---+   +---[0]---+
         |    D    |   |    E    |   |    F    |   |    G    |   Hosts
         +[1]---[2]+   +[1]---[2]+   +[1]---[2]+   +[1]---[2]+

      2. Mesh/Torus Topology
         - Connectors have plus/minus relationship (like links in
           BG/Q).

                            |              |         
                       +---[0]---+    +---[0]---+           
                       |         |    |         |          
                    --[1]   A   [2]--[1]   B   [2]--       
                       |         |    |         |          
                       +---[3]---+    +---[3]---+    
                            |              |         
                       +---[0]---+    +---[0]---+          
                       |         |    |         |          
                    --[1]   E   [2]--[1]   F   [2]--       
                       |         |    |         |          
                       +---[3]---+    +---[3]---+    
                            |              |         

       However we need dedicated path finding algorithms for each of
       those topologies and potentially a generic path finding
       algorithm, but that might be inefficient.
     */
    boost::optional<C &> peer;

    /*
          The vertex this connector belongs to.
        */
    V &owner;

    /* 
           Connector ID (e.g. the name of the I/O device it's connected
           to.
        */
    std::string _id;
};

template <class V, class C> BaseConnector<V, C>::BaseConnector(V &owner) : owner(owner)
{
}

template <class V, class C> BaseConnector<V, C>::BaseConnector(V &owner, const ptree &data) : owner(owner)
{
    this->id(data.get<std::string>("id"));
}

template <class V, class C> const ptree BaseConnector<V, C>::store() const
{
    ptree data;
    data.put("id", this->id());
    /*
          data.put("owner", owner.get_id());
          if (peer)
          data.put("peer", peer->get_owner().get_id());
        */
    return data;
}
} // namespace graph_simple
#endif
