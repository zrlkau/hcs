// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef cluster_node_h
#define cluster_node_h

#include "graph/vertex-tree.h"
#include "resourcemanager/load.h"
#include "resourcemanager/resource.h"

#include <array>
#include <list>
#include <set>
#include <unordered_map>
#include <vector>

#include <boost/dynamic_bitset.hpp>

using namespace graph_simple;
using boost::property_tree::ptree;

class Pu;
class Core;
class Memory;
class IoDev;
class IoPort;
class Domain;
class Resource;

/******************************************************************************/
/*                                CORE RESOURCE                               */
/******************************************************************************/

class Core : public Resource
{
  public:
    Core(Resource *parent, const ptree &data);
    ~Core();
    Core(const Core &) = delete;
    Core(Core &&)      = delete;
    Core &operator=(const Core &) = delete;
    Core &operator=(const Core &&) = delete;

    ptree store() const;

    SETGET(uint16_t, os_index);

  protected:
  private:
    uint16_t _os_index;
};

/******************************************************************************/
/*                                 PU RESOURCE                                */
/******************************************************************************/
class Pu : public Resource
{
  public:
    Pu(Resource *parent, const ptree &data);
    ~Pu();
    Pu(const Pu &) = delete;
    Pu(Pu &&)      = delete;
    Pu &operator=(const Pu &) = delete;
    Pu &operator=(const Pu &&) = delete;

    ptree store() const;

    SETGET(uint16_t, os_index);

  protected:
  private:
    uint16_t _os_index;
};

/******************************************************************************/
/*                                 IO RESOURCE                                */
/******************************************************************************/
class IoDev : public Resource
{
  public:
    IoDev(Resource *parent, ResourceType type, const ptree &data);
    ~IoDev();
    IoDev(const IoDev &) = delete;
    IoDev(IoDev &&)      = delete;
    IoDev &operator=(const IoDev &) = delete;
    IoDev &operator=(const IoDev &&) = delete;

    ptree store() const;

    SETGET(uint64_t, bw);
    SETGET(Duration, lat);
    SETGET(uint16_t, os_index);

  protected:
  private:
    uint64_t _bw;
    Duration _lat;
    uint16_t _os_index;
};

/******************************************************************************/
/*                            NUMA NODE RESOURCE                              */
/******************************************************************************/
class Domain : public Resource
{
  public:
    Domain(Resource *parent, const ptree &data);
    ~Domain();
    Domain(const Domain &) = delete;
    Domain(Domain &&)      = delete;
    Domain &operator=(const Domain &) = delete;
    Domain &operator=(const Domain &&) = delete;

    ptree store() const;

    SETGET(uint16_t, os_index);
    SETGET(uint64_t, mem_size);

  protected:
  private:
    uint16_t _os_index;
    uint64_t _mem_size;
};

/******************************************************************************/
/*                                NODE CLASS                                  */
/******************************************************************************/
class NodeClass : public boost::dynamic_bitset<>, public Resource
{
  public:
    NodeClass(size_t index, size_t size);
    ~NodeClass();
    NodeClass(const NodeClass &) = delete;
    NodeClass(NodeClass &&)      = delete;
    NodeClass &operator=(const NodeClass &) = delete;
    NodeClass &operator=(const NodeClass &&) = delete;

    void  add(Node &node);
    Node *get(size_t index);
    SETGET(size_t, index);
    SETGET(std::array<size_t COMMA static_cast<size_t>(ResourceType::size)>, resources);

    GET(std::unordered_map<size_t COMMA Node *> &, nodes);

    GET(Resource *, nic); // Network Interface of first (any) node.
  protected:
  private:
    size_t _index;

    std::array<size_t, static_cast<size_t>(ResourceType::size)> _resources;
    std::unordered_map<size_t, Node *>                          _nodes;

    SET(Resource *, nic);
    Resource *_nic;
};

/******************************************************************************/
/*                             NODE ABSTRACTION                               */
/******************************************************************************/
class Node : public Resource, public TreeVertex<Node, Link>
{
    // problem: the graph connects links, but here we want to connect
    // to I/O ports. How do we reconcile that? Maybe give the cluster
    // io class a property letting them now to which I/O port they're
    // connected?
  public:
    Node();
    Node(const ptree &data);
    ~Node();
    Node(const Node &) = delete;
    Node(Node &&)      = delete;
    Node &operator=(const Node &) = delete;
    Node &operator=(const Node &&) = delete;

    void set_type(NodeType _type)
    {
        type(_type);
    }
    NodeType get_type() const
    {
        return type();
    }

    void type(NodeType _type)
    {
        this->_type = _type;
    }
    NodeType type() const
    {
        return this->_type;
    }

    ptree store() const;

    void add_resource(ResourceType type, Resource *resource);
    bool has_resource(ResourceType type)
    {
        return this->_resources.find(type) != this->_resources.end();
    }

    std::list<Resource *>                                resources(ResourceType type) const;
    const std::map<ResourceType, std::list<Resource *>> &resources() const
    {
        return this->_resources;
    }

    void computeIoCostMatrix();

    SETGET(std::string, breed);
    SETGET(int, index);
    SETGET(NodeClass *, eqClass);
    GET(Resource *, nic);

    GET(Load &, ioLoad);

    void dump(int depth = 0, Resource *res = NULL);

  protected:
  private:
    void init(); // called by all constructors because C++11
                 // delegating constructors don't work here.

    SET(Resource *, nic);
    Resource *_nic;

    std::map<ResourceType, std::list<Resource *>> _resources;

    NodeType _type;

    std::string _breed; // node breed (class)
    int         _index; // node index (0-N)

    NodeClass *_eqClass;

    Load _ioLoad;
};

std::ostream &operator<<(std::ostream &os, const NodeType &obj);
std::istream &operator>>(std::istream &is, NodeType &obj);

#endif
