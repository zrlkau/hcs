// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef cluster_link_h
#define cluster_link_h

#include "cluster/node.h"
#include "graph/connector-tree.h"
#include "graph/vertex-tree.h"

#include "common.h"

using namespace graph_simple;
using boost::property_tree::ptree;

class Link : public TreeConnector<Node, Link>
{
  public:
    Link(Node &owner, TreeConnectorDir dir);
    Link(Node &owner, const ptree &data);

    static std::string type_to_string(LinkType type);
    static LinkType    string_to_type(std::string type);

    void set_type(LinkType type)
    {
        this->type = type;
    };
    LinkType get_type() const
    {
        return this->type;
    };

    void set_capacity(size_t capacity)
    {
        this->capacity = capacity;
    };
    size_t get_capacity() const
    {
        return this->capacity;
    };

    void set_cost(size_t cost)
    {
        this->cost = cost;
    }
    size_t get_cost() const
    {
        return cost;
    }

    ptree store() const;

  protected:
  private:
    void init(); // called by all constructors because C++11 delegating
    // constructors don't work here.

    LinkType type;
    size_t   capacity;
    size_t   cost;
};
#endif
