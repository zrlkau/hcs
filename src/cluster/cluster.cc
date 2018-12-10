// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include <string.h>

#include "app/app.h"
#include "cluster.h"
#include "common.h"
#include "event/engine.h"
#include "event/event.h"

using namespace graph_simple;
using boost::property_tree::ptree;

Cluster::Cluster() : TreeGraph<Node, Link>(), Resource(ResourceType::cluster)
{
    init();
}

Cluster::Cluster(ptree &data) : TreeGraph<Node, Link>(data.get_child("graph")), Resource(ResourceType::cluster)
{
    init();

    this->id(data.get<std::string>("id"));
    this->name(this->id());

    for (auto &node : this->_vertices) {
        node.index(max_node_index());
        this->max_node_index(max_node_index() + 1);
        std::ostringstream os;

        size_t i = node.resources().size();
        os << "Node " << node.id() << " has ";
        for (auto &res : node.resources()) {
            os << res.second.size() << " " << res.first << " device(s)";
            if (--i > 0)
                os << ", ";
        }
        logInfo(5) << os.str();

        // Register network interfaces of the node so that we can
        // consider them when we compute the io cost matrix.
        std::list<Resource *> nets = node.resources(ResourceType::network);
        for (auto &nif : nets) {
            nif->index(this->_resIndex.size());
            this->_resIndex.emplace(static_cast<Resource *>(nif), this->_resIndex.size());
        }

        this->_res2res.resize(this->_resIndex.size(), this->_resIndex.size());

        // Count resources and models. Needed to compute the average
        // weighted execution time of tasks.
        for (auto const &resmap : node.resources()) {
            this->_resdb[resmap.first] += resmap.second.size();
            this->_moddb[resmap.first][(*resmap.second.begin())->model()] += resmap.second.size();
        }
    }

    this->init_node_classes();
}

void Cluster::init()
{
    this->id("");
    this->max_node_index(0);
    this->max_nodeclass_index(0);
}

Cluster::~Cluster()
{
}

size_t Cluster::getResourceNum(ResourceType rt) const
{
    auto res = this->_resdb.find(rt);
    if (res == this->_resdb.end())
        return 0;
    return res->second;
}

size_t Cluster::getModelNum(ResourceType rt, ResourceModel model) const
{
    auto res = this->_moddb.find(rt);
    if (res == this->_moddb.end())
        return 0;

    auto mod = res->second.find(model);
    if (mod == res->second.end())
        return 0;

    return mod->second;
}

void Cluster::init_node_classes()
{
    // Nodes are in the same equivalence class if
    // 1. They are identical hw-wise (i.e. same breed)
    // 2. They have pair-wise equivalent communication costs (and
    //    the network allows full-speed all-to-all communication).

    for (auto node0 = nodes().begin(); node0 != nodes().end(); node0++) {
        auto   node1 = node0;

        if (node0->eqClass() == NULL) {
            this->add_nodeClass().add(*node0);
            this->update_resToNodeClassMap(*node0->eqClass());
        }

        for (++node1; node1 != nodes().end(); node1++) {
            if (node1->eqClass() != NULL)
                continue;

            if (node0->breed() == node1->breed())
                node0->eqClass()->add(*node1);
        }
    }

    logInfo(2) << "This cluster has " << this->_node_classes.size() << " node equivalence classes";
    for (auto const &ec : this->_node_classes) {
        logInfo(3) << " - " << ec.second << "(" << &ec << ")";
    }
}

NodeClass &Cluster::add_nodeClass()
{
    auto       rc = this->_node_classes.emplace(std::piecewise_construct,
                                          std::forward_as_tuple(_max_nodeclass_index),
                                          std::forward_as_tuple(_max_nodeclass_index, max_node_index()));
    NodeClass &nc = rc.first->second;
    this->child(nc.type(), &nc);

    _max_nodeclass_index++;

    for (auto &map : this->_resToNCMap)
        map.resize(_max_nodeclass_index);

    return nc;
}

NodeClass *Cluster::nodeClass(size_t index)
{
    try {
        return &this->_node_classes.at(index);
    } catch (const std::out_of_range &err) {
        return NULL;
    }
}

[[deprecated]] NodeClass *Cluster::get_nodeClass(size_t index)
{
    return this->nodeClass(index);
}

void Cluster::update_resToNodeClassMap(NodeClass &nc)
{
    for (size_t ri = 0; ri < nc.resources().size(); ri++) {
        if (nc.resources()[ri] > 0)
            this->_resToNCMap[ri][nc.index()] = 1;
    }
}

boost::dynamic_bitset<> Cluster::get_resToNodeClassMap(ResourceType rt)
{
    return this->_resToNCMap[static_cast<int>(rt)];
}

Node &Cluster::add_node()
{
    Node &node = add_vertex();
    node.index(max_node_index());
    max_node_index(max_node_index() + 1);

    Resource *resource = static_cast<Resource *>(&node);
    resource->parent(this);
    resource->type(ResourceType::node);

    return node;
}

Node &Cluster::add_node(const ptree &data)
{
    Node &node = add_vertex(data);
    node.index(max_node_index());
    max_node_index(max_node_index() + 1);

    Resource *resource = static_cast<Resource *>(&node);
    resource->parent(this);
    resource->type(ResourceType::node);

    return node;
}

Node *Cluster::node(std::string id)
{
    boost::optional<Node &> node = get_vertex(id);
    if (node)
        return &(*node);
    else
        return NULL;
}

std::list<Node *> Cluster::get_nodes(std::list<std::list<ResourceType>> type_list)
{
    std::list<Node *> res;
    std::list<Node> & nodes = this->vertices();

    for (auto &node : nodes) {
        if (node.type() == NodeType::network)
            continue;

        for (auto &and_list : type_list) {
            bool has = true;
            for (auto &item : and_list) {
                if (!node.has_resource(item))
                    has = false;
            }
            if (has) {
                res.emplace_back(&node);
                break;
            }
        }
    }

    return res;
}

ptree Cluster::store()
{
    ptree data;
    data.put("type", "cluster");
    data.put("id", this->id());
    data.add_child("graph", TreeGraph::store());

    return data;
}
