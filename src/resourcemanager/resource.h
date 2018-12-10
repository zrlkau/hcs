// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//          Adrian Schuepbach <dri@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef resource_h
#define resource_h

#include "common.h"

#include <boost/circular_buffer.hpp>

#include <chrono>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>

/****************************************************************************************************
 *                                                                                                  *
 * RESOURCE TYPE : The general type of class of a resource, such as CPU, Network, FPGA, ...         *
 *                                                                                                  *
 ****************************************************************************************************/
enum class ResourceType {
    unknown,
    cluster,
    nodeclass,
    node,
    numanode,
    // compute resources
    core,
    cpu,
    gpu,
    fpga,
    // memory resources
    memory,
    // storage resources
    storage,
    // communication
    network,
    rdma,
    size
};

/****************************************************************************************************
 *                                                                                                  *
 * RESOURCE STATE : The state (idle, busy, etc.) of a resource                                      *
 *                                                                                                  *
 ****************************************************************************************************/
enum class ResourceState { unknown, disabled, idle, busy, error, maintenance };

/****************************************************************************************************
 *                                                                                                  *
 * RESOURCE MODEL : The concrete model of a resource, e.g. a specific CPU model                     *
 *                                                                                                  *
 ****************************************************************************************************/
class ResourceModel
{
  public:
    ResourceModel() : _id("generic"){};
    ResourceModel(std::string id) : _id(id){};

    GET(std::string, id);

    inline bool operator<(const ResourceModel &rhs) const
    {
        return this->id() < rhs.id();
    }
    inline bool operator>(const ResourceModel &rhs) const
    {
        return rhs < *this;
    }
    inline bool operator==(const ResourceModel &rhs) const
    {
        return this->id() == rhs.id();
    }
    inline bool operator!=(const ResourceModel &rhs) const
    {
        return !(*this == rhs);
    }

  private:
    std::string _id;
};

namespace std {
template <> struct hash<ResourceModel> {
    size_t operator()(const ResourceModel &obj) const
    {
        return (hash<string>()(obj.id()));
    }
};

template <> struct hash<std::pair<ResourceType, ResourceModel>> {
    size_t operator()(const std::pair<ResourceType, ResourceModel> &obj) const
    {
        return (hash<ResourceType>()(obj.first) ^ hash<ResourceModel>()(obj.second));
    }
};
} // namespace std

/****************************************************************************************************
 *                                                                                                  *
 * RESOURCE ALLOCATION ENTRY : Object used to represent that a task is using a specific resource    *
 *                             for a certain time frame.                                            *
 *                                                                                                  *
 ****************************************************************************************************/

class ResourceAllocationEntry
{
  public:
    ResourceAllocationEntry()
        : allocated(0), cleared(0), _task(NULL), _executor(NULL), _from(Time::min()), _to(Time::max()),
          _valid(false), _firstOnExecutorOfStage(false), _firstOnExecutor(false), _cmpLoad(0.0), _ioLoad(0.0),
          _nodeIoLoad(0.0)
    {
    }
    ResourceAllocationEntry(const ResourceAllocationEntry &) = delete;
    ResourceAllocationEntry(ResourceAllocationEntry &&)      = delete;
    ResourceAllocationEntry &operator=(const ResourceAllocationEntry &) = delete;
    ResourceAllocationEntry &operator=(const ResourceAllocationEntry &&) = delete;

  public:
    inline bool operator<(const ResourceAllocationEntry &rhs) const
    {
        Time        s0, s1, e0, e1;
        const void *r0, *r1;
        const void *p0, *p1;
        s0 = this->from();
        e0 = this->to();
        r0 = this->executor();
        p0 = this;
        s1 = rhs.from();
        e1 = rhs.to();
        r1 = rhs.executor();
        p1 = &rhs;

        if (s0 < s1)
            return true;
        if (s0 > s1)
            return false;

        if (e0 > e1)
            return true;
        if (e0 < e1)
            return false;

        if (r0 < r1)
            return true;
        if (r0 > r1)
            return false;

        if (p0 < p1)
            return true;
        if (p0 > p1)
            return false;
        return false;
    }

    inline bool operator>(const ResourceAllocationEntry &rhs) const
    {
        return rhs < *this;
    }
    inline bool operator==(const ResourceAllocationEntry &rhs) const
    {
        return (this->to() == rhs.to() && this->from() == rhs.from() && this->task() == rhs.task() &&
                this->executor() == rhs.executor());
    }
    inline bool operator!=(const ResourceAllocationEntry &rhs) const
    {
        return !(*this == rhs);
    }

    void set(Task *task, Executor *executor, Time from, Time to);
    void update(Time from, Time to);
    GET(Task *, task);
    SETGET(Executor *, executor);
    SETGET(Time, from);
    SETGET(Time, to);
    SETGET(Time, modified);
    SETGET(bool, valid);
    SETGET(bool, firstOnExecutorOfStage);
    SETGET(bool, firstOnExecutor);

    SETGET(double, cmpLoad);
    SETGET(double, ioLoad);
    SETGET(double, nodeIoLoad);

    Duration duration()
    {
        return (this->to() - this->from());
    }

    uint64_t allocated;
    uint64_t cleared;

  private:
    Task *    _task;
    Executor *_executor;
    Time      _from;
    Time      _to;
    Time      _modified;
    bool      _valid;
    bool      _firstOnExecutorOfStage;
    bool      _firstOnExecutor;
    double    _cmpLoad;
    double    _ioLoad;
    double    _nodeIoLoad;
};

std::ostream &operator<<(std::ostream &os, const ResourceAllocationEntry &a);

class Resource
{
  public:
    Resource(ResourceType type);
    Resource(ResourceType type, Resource *parent);
    Resource(const Resource &) = delete;
    Resource(Resource &&)      = delete;
    Resource &operator=(const Resource &) = delete;
    Resource &operator=(const Resource &&) = delete;

    bool allocate(Task *task, Time from, Time to);
    void update(Task *task, Time from, Time to);
    void clear(Task *task);
    void clear();

    double updateUtilization(Duration timeframe);
    SETGET(double, utilization);

    SETGET(Resource *, parent);
    SETGET(Executor *, executor);
    GET(Cluster *, cluster);
    GET(NodeClass *, nodeclass);
    GET(Node *, node);
    SETGET(IdleTask *, idleTask);
    SETGET(DisconnectTask *, disconnectTask);

    GET(Time, busyUntil);
    std::unique_lock<std::timed_mutex> guard();

    void cluster(Cluster *cluster);
    void nodeclass(NodeClass *nodeclass);
    void node(Node *node);

    void                  child(ResourceType type, Resource *resource);
    std::list<Resource *> children(ResourceType type) const;
    std::list<Resource *> children();

    SETGET(std::string, name);
    SETGET(std::string, id);
    SETGET(size_t, index);

    SETGET(ResourceKey, key);
    SETGET(ResourceType, type);
    SETGET(ResourceModel, model);
    SETGET(ResourceState, state);

    // Find special parents
    Resource *      parent(ResourceType type);
    const Resource *parent(ResourceType type) const;

    // Bandwidth and Latency between children of this resource.
    uint64_t                             bandwidth(Resource *const c0, Resource *const c1) const;
    Duration                             latency(Resource *const c0, Resource *const c1) const;
    const std::pair<uint64_t, Duration> &metrics(Resource *const c0, Resource *const c1) const;

    void                     dumpAllocations();
    ResourceAllocationEntry *getFirstAllocation();
    size_t                   getNumAllocations();

    GET(boost::circular_buffer<ResourceAllocationEntry *> &, allocations);

    static bool compare(const Resource *lhs, const Resource *rhs)
    {
        if (lhs->busyUntil() == rhs->busyUntil())
            return static_cast<const void *>(lhs) < static_cast<const void *>(rhs);
        else
            return lhs->busyUntil() < rhs->busyUntil();
    }

  protected:
    // Some i/o performance numbers between resources of this node.
    std::unordered_map<Resource *, uint>  _resIndex;
    Matrix<std::pair<uint64_t, Duration>> _res2res;

    void computePathCosts(Resource *res, Resource *pred, uint idx, uint64_t bw, Duration lat);
    void completeIoCostMatrix();

    SET(Time, busyUntil);

  private:
    void update(Resource *parent, Cluster *cluster, NodeClass *nodeclass, Node *node);

    Resource *                                              _parent;
    std::unordered_map<ResourceType, std::list<Resource *>> _children;

    Cluster *  _cluster;
    NodeClass *_nodeclass;
    Node *     _node;

    std::string _id;
    size_t      _index;

    std::string _name;

    ResourceKey   _key;
    ResourceType  _type;
    ResourceModel _model;
    ResourceState _state;
    double        _utilization;

    Executor *_executor;

    std::timed_mutex _mutex;

    boost::circular_buffer<ResourceAllocationEntry *> _allocations;

    Time _busyUntil;

    // Task that's begin "executed" while the resource is idle.
    IdleTask *_idleTask;

    DisconnectTask *_disconnectTask;
};

struct compareResources : public std::binary_function<Resource *, Resource *, bool> {
    bool operator()(const Resource *lhs, const Resource *rhs) const
    {
        if (lhs->busyUntil() == rhs->busyUntil())
            return static_cast<const void *>(lhs) < static_cast<const void *>(rhs);
        else
            return lhs->busyUntil() < rhs->busyUntil();
    }
};

std::ostream &operator<<(std::ostream &os, const ResourceType &obj);
std::istream &operator>>(std::istream &is, ResourceType &obj);
std::ostream &operator<<(std::ostream &os, const ResourceState &obj);
std::ostream &operator<<(std::ostream &os, const ResourceModel &obj);
std::ostream &operator<<(std::ostream &os, const ResourceAllocationEntry &obj);
#endif
