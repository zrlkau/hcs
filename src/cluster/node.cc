// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "common.h"

#include "cluster/cluster.h"
#include "cluster/node.h"
#include "scheduler/oracle.h"

#include <string>

using boost::property_tree::ptree;

/******************************************************************************/
/*                                NODE CLASS                                  */
/******************************************************************************/
NodeClass::NodeClass(size_t index, size_t size)
    : boost::dynamic_bitset<>(size, 0), Resource(ResourceType::nodeclass)
{
    std::stringstream ss;
    ss << this->type() << index;

    this->index(index);
    this->name(ss.str());
    this->_resources.fill(0);
}

NodeClass::~NodeClass()
{
}

void NodeClass::add(Node &node)
{
    logInfo(1) << "Adding node " << node.id() << " (" << node.index() << "/" << this->size()
               << ") to node class " << this->index();

    for (auto const &resmap : node.resources()) {
        this->_resources[static_cast<size_t>(resmap.first)] += resmap.second.size();
        logInfo(1) << "- total " << this->_resources[static_cast<size_t>(resmap.first)] << " resources of type "
                   << resmap.first << " (" << (*resmap.second.begin())->model() << ")";
    }

    (*this)[node.index()] = 1;
    this->_nodes.insert(std::make_pair(node.index(), &node));
    node.eqClass(this);

    if (this->_nodes.size() == 1)
        this->nic(node.nic());

    this->child(ResourceType::node, &node);
}

Node *NodeClass::get(size_t index)
{
    auto n = this->_nodes.find(index);
    if (n != this->_nodes.end())
        return n->second;
    else
        return NULL;
}

/******************************************************************************/
/*                             NODE ABSTRACTION                               */
/******************************************************************************/
Node::Node() : Resource(ResourceType::node), TreeVertex<Node, Link>()
{
    init();
}

void Node::dump(int depth, Resource *res)
{
    if (res == NULL) {
        logInfo(11) << "-> " << this->id() << " (" << this->type() << ")";
        dump(depth + 1, this);
    } else {
        uint ti = res->index();
        for (auto &child : res->children()) {
            uint ci = child->index();
            logInfo(11) << "-> " << child->id() << " (" << child->type() << ") = ("
                        << this->_res2res(ti, ci).first << "b/s+" << (this->_res2res(ti, ci).second).count()
                        << ")";
            dump(depth + 1, child);
        }
    }
}

Node::Node(const ptree &data) : Resource(ResourceType::node), TreeVertex<Node, Link>(data)
{
    init();

    this->name(data.get<std::string>("id"));
    this->id(this->name());
    this->type(data.get<NodeType>("type"));
    this->breed(data.get<std::string>("breed"));

    logInfo(1) << "Loaded node " << this->id() << " " << this;

    boost::optional<const ptree &> domains = data.get_child_optional("domains");
    if (domains) {
        for (auto const &entry : *domains) {
            Domain *domain = new Domain(this, entry.second);
            this->child(ResourceType::numanode, domain);
        }
    }

    this->computeIoCostMatrix();

    dump();
}

void Node::init()
{
    this->type(NodeType::unknown);
    this->eqClass(NULL);
    this->nic(NULL);
    this->add_resource(ResourceType::node, static_cast<Resource *>(this));
}

void Node::computeIoCostMatrix()
{
    this->completeIoCostMatrix();
}

Node::~Node()
{
}

ptree Node::store() const
{
    ptree data = TreeVertex::store();

    data.add("id", this->id());
    data.add("type", this->type());
    // TODO: Fix
    return data;
}

void Node::add_resource(ResourceType type, Resource *resource)
{
    logInfo(20) << "adding resource of type " << type << " to node " << this->name();

    // We don't support that anymore. That's just network now.
    assert(resource->type() != ResourceType::rdma);

    if (type == ResourceType::network && this->nic() == NULL)
        this->nic(resource);

    this->_resources[type].push_back(resource);

    uint resIndex = this->_resIndex.size();
    this->_resIndex.emplace(resource, resIndex);
    this->_res2res.resize(resIndex + 1, resIndex + 1);
    resource->index(resIndex);

    for (size_t i = 0; i < resIndex + 1; i++) {
        this->_res2res(i, resIndex) = std::make_pair(0, Duration::zero());
        this->_res2res(resIndex, i) = std::make_pair(0, Duration::zero());
    }

    uint      ti     = resource->index();
    Resource *parent = resource->parent();

    this->_res2res(ti, ti) = std::make_pair(ULLONG_MAX, Duration::zero());
    if (parent) {
        uint pi = parent->index();

        if (resource->type() == ResourceType::numanode && parent->type() == ResourceType::node) {
            this->_res2res(pi, ti) = std::make_pair(52000000000LL, Duration(std::chrono::nanoseconds(75)));
            this->_res2res(ti, pi) = std::make_pair(52000000000LL, Duration(std::chrono::nanoseconds(75)));
        }
        if (resource->type() == ResourceType::core && parent->type() == ResourceType::numanode) {
            this->_res2res(pi, ti) = std::make_pair(ULLONG_MAX, Duration(std::chrono::nanoseconds(75 / 2)));
            this->_res2res(ti, pi) = std::make_pair(ULLONG_MAX, Duration(std::chrono::nanoseconds(75 / 2)));
        }
        if (resource->type() == ResourceType::cpu && parent->type() == ResourceType::core) {
            this->_res2res(pi, ti) = std::make_pair(ULLONG_MAX, Duration(std::chrono::nanoseconds(75 / 4)));
            this->_res2res(ti, pi) = std::make_pair(ULLONG_MAX, Duration(std::chrono::nanoseconds(75 / 4)));
            // Workaround:
            Resource *numanode     = parent->parent(ResourceType::numanode);
            uint      ni           = numanode->index();
            this->_res2res(pi, ti) = std::make_pair(ULLONG_MAX,
                                                    Duration(std::chrono::nanoseconds(75 / 4 + 75 / 2)));
            this->_res2res(ti, pi) = std::make_pair(ULLONG_MAX,
                                                    Duration(std::chrono::nanoseconds(75 / 4 + 75 / 2)));
        }
        if (resource->type() == ResourceType::gpu && parent->type() == ResourceType::numanode) {
            this->_res2res(pi, ti) = std::make_pair(7880000000ULL, Duration(5));
            this->_res2res(ti, pi) = std::make_pair(7880000000ULL, Duration(5));
        }
        if (resource->type() == ResourceType::network && parent->type() == ResourceType::numanode) {
            this->_res2res(pi, ti) = std::make_pair(7880000000ULL, Duration(5));
            this->_res2res(ti, pi) = std::make_pair(7880000000ULL, Duration(5));
        }
        if (resource->type() == ResourceType::storage && parent->type() == ResourceType::numanode) {
            this->_res2res(pi, ti) = std::make_pair(100000000ULL, Duration(100));
            this->_res2res(ti, pi) = std::make_pair(100000000ULL, Duration(100));
        }
    }
}

std::list<Resource *> Node::resources(ResourceType type) const
{
    auto res = this->_resources.find(type);
    if (res != this->_resources.end())
        return res->second;
    else
        return std::list<Resource *>();
}

/******************************************************************************/
/*                           NUMA NODE RESOURCE                               */
/******************************************************************************/
Domain::Domain(Resource *parent, const ptree &data) : Resource(ResourceType::numanode, parent)
{
    this->os_index(data.get<uint16_t>("os_index"));
    this->mem_size(data.get<uint64_t>("memory"));

    this->node()->add_resource(this->type(), this);

    boost::optional<const ptree &> cores = data.get_child_optional("cores");
    if (cores) {
        for (auto const &entry : *cores)
            this->child(ResourceType::core, new Core(this, entry.second));
    }

    boost::optional<const ptree &> iodevs = data.get_child_optional("iodevs");
    if (iodevs) {
        for (auto const &entry : *iodevs) {
            std::string type = entry.second.get<std::string>("type");
            if (type == "storage") {
                this->child(ResourceType::storage, new IoDev(this, ResourceType::storage, entry.second));
            } else if (type == "network") {
                this->child(ResourceType::network, new IoDev(this, ResourceType::network, entry.second));
            } else if (type == "gpu") {
                this->child(ResourceType::gpu, new IoDev(this, ResourceType::gpu, entry.second));
            }
        }
    }
}

/******************************************************************************/
/*                                CORE RESOURCE                               */
/******************************************************************************/
Core::Core(Resource *parent, const ptree &data) : Resource(ResourceType::core, parent)
{
    this->os_index(data.get<uint16_t>("os_index"));
    this->model(data.get<std::string>("model", "generic"));

    this->node()->add_resource(this->type(), this);

    boost::optional<const ptree &> pus = data.get_child_optional("pus");
    if (pus) {
        for (auto const &entry : *pus)
            this->child(ResourceType::cpu, new Pu(this, entry.second));
    }
}

/******************************************************************************/
/*                                 PU RESOURCE                                */
/******************************************************************************/
Pu::Pu(Resource *parent, const ptree &data) : Resource(ResourceType::cpu, parent)
{
    this->os_index(data.get<uint16_t>(""));
    this->id("cpu" + std::to_string(this->os_index()));
    this->model(parent->model());
    this->node()->add_resource(this->type(), this);
}

/******************************************************************************/
/*                                 IO RESOURCE                                */
/******************************************************************************/
IoDev::IoDev(Resource *parent, ResourceType type, const ptree &data) : Resource(type, parent)
{
    this->os_index(data.get<uint16_t>("os_index"));
    this->model(data.get<std::string>("model", "generic"));

    this->bw(data.get<uint64_t>("bw", 0));
    this->lat(Duration(data.get<uint64_t>("lat", 0)));

    this->node()->add_resource(this->type(), this);
}

/****************************************************************************************************
 *                                                 NODE TYPE                                        *
 ****************************************************************************************************/
std::ostream &operator<<(std::ostream &os, const NodeType &obj)
{
    switch (obj) {
    case NodeType::regular:
        os << "regular";
        break;
    case NodeType::network:
        os << "network";
        break;
    case NodeType::group:
        os << "group";
        break;
    default:
        os << "unknown";
        break;
    }
    return os;
}

std::istream &operator>>(std::istream &is, NodeType &obj)
{
    std::string s(std::istreambuf_iterator<char>(is), {});

    if (s == "regular")
        obj = NodeType::regular;
    else if (s == "network")
        obj = NodeType::network;
    else if (s == "group")
        obj = NodeType::group;
    else
        is.setstate(std::ios::failbit);

    return is;
}
