// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef cluster_h
#define cluster_h

#include <array>
#include <list>
#include <unordered_map>
#include <vector>

#include "link.h"
#include "node.h"
#include "resourcemanager/resource.h"
#include <common.h>
#include <graph/graph-tree.h>

using namespace graph_simple;
using boost::property_tree::ptree;

class Cluster : public TreeGraph<Node, Link>, public Resource
{
  public:
    Cluster();
    Cluster(ptree &data);
    ~Cluster();

    Node &add_node();
    Node &add_node(const ptree &data);
    Node *node(std::string id);

    NodeClass &               add_nodeClass();
    NodeClass *               nodeClass(size_t index);
    [[deprecated]] NodeClass *get_nodeClass(size_t index);

    void                    update_resToNodeClassMap(NodeClass &nc);
    boost::dynamic_bitset<> get_resToNodeClassMap(ResourceType rt);

    std::list<Node *> get_nodes(std::list<std::list<ResourceType>> type);

    std::list<Node> &nodes()
    {
        return TreeGraph::vertices();
    }
    const std::list<Node> &nodes() const
    {
        return TreeGraph::vertices();
    }

    void init_node_classes();

    ptree store();

    SETGET(int, max_node_index);
    SETGET(int, max_nodeclass_index);

    size_t getModelNum(ResourceType rt, ResourceModel model) const;
    size_t getResourceNum(ResourceType rt) const;

  protected:
  private:
    void init(); // called by all constructors because C++11 delegating constructors don't work
                 // here.

    std::vector<std::vector<int>> dist;

    std::unordered_map<size_t, NodeClass>                                     _node_classes;
    std::array<boost::dynamic_bitset<>, static_cast<int>(ResourceType::size)> _resToNCMap;

    std::unordered_map<ResourceType, size_t>                                    _resdb;
    std::unordered_map<ResourceType, std::unordered_map<ResourceModel, size_t>> _moddb;

    int _max_node_index;
    int _max_nodeclass_index;
};
#endif
