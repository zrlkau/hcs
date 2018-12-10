// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "resourcemanager/resource.h"
#include "app/stage.h"
#include "app/task.h"
#include "cluster/cluster.h"
#include "cluster/node.h"
#include "common.h"
#include "event/engine.h"
#include "resourcemanager/executor.h"
#include "resourcemanager/worker.h"

// #undef dlevel
// #define dlevel 20

/******************************************************************************/
/*                         NEW RESOURCE ABSTRACTION                           */
/******************************************************************************/
Resource::Resource(ResourceType type) : _busyUntil(epoch_start)
{
    this->type(type);
    this->model(ResourceModel("generic"));
    this->parent(NULL);
    this->executor(NULL);
    this->state(ResourceState::busy); // busy until it tells us it's idle
    this->utilization(0.0);

    if (this->type() == ResourceType::cluster)
        this->cluster(static_cast<Cluster *>(this));
    else
        this->cluster(NULL);

    if (this->type() == ResourceType::nodeclass)
        this->nodeclass(static_cast<NodeClass *>(this));
    else
        this->nodeclass(NULL);

    if (this->type() == ResourceType::node)
        this->node(static_cast<Node *>(this));
    else
        this->node(NULL);

    this->_allocations.set_capacity(1024);

    logInfo(20) << "Creating new resource of type=" << this->type() << " with"
                << " node=" << this->node() << " nodeclass=" << this->nodeclass()
                << " cluster=" << this->cluster() << " parent=" << this->parent();
}

Resource::Resource(ResourceType type, Resource *parent) : _busyUntil(epoch_start)
{
    this->type(type);
    this->model(ResourceModel("generic"));
    this->parent(parent);
    this->executor(NULL);
    this->state(ResourceState::busy); // busy until it tells us it's idle
    this->utilization(0.0);

    if (this->type() == ResourceType::cluster)
        this->cluster(static_cast<Cluster *>(this));
    else
        this->cluster(parent->cluster());

    if (this->type() == ResourceType::nodeclass)
        this->nodeclass(static_cast<NodeClass *>(this));
    else
        this->nodeclass(parent->nodeclass());

    if (this->type() == ResourceType::node)
        this->node(static_cast<Node *>(this));
    else
        this->node(parent->node());

//    this->_allocations.set_capacity(32);

    logInfo(20) << "Creating new resource of type=" << this->type() << " with"
                << " node=" << this->node() << " nodeclass=" << this->nodeclass()
                << " cluster=" << this->cluster() << " parent=" << this->parent();
}

// Time Resource::busyUntil() const
// {
//     if (this->_allocations.size() > 0 && this->_allocations.back() != NULL &&
//         this->_allocations.back()->valid())
//         return this->_allocations.back()->to();
//     else
//         return baseTime;
// }

std::unique_lock<std::timed_mutex> Resource::guard()
{
    auto lock = std::unique_lock<std::timed_mutex>(this->_mutex, std::defer_lock);
    while (!lock.try_lock_for(Duration(std::chrono::seconds(10)))) {
        logWarn << "Unable to acquire lock for resource " << this->id() << " within the last 10s";
    }
    return lock;
    //    return std::unique_lock<std::mutex>(this->_mutex);
}

// double Resource::updateUtilization(Duration timeframe)
// {
//     Duration utilTime = Duration::zero();

//     if (timeframe == Duration::zero())
//         return 0.0;

//     for (auto allocation : this->_allocations) {
//         auto from = std::max(baseTime, allocation->from());
//         auto to   = std::min(baseTime + timeframe, allocation->to());

//         if (from >= baseTime + timeframe)
//             break;

//         utilTime += to - from;
//     }

//     this->_utilization = static_cast<double>(utilTime.count()) / static_cast<double>(timeframe.count());
//     return this->_utilization;
// }

// void Resource::dumpAllocations()
// {
//     for (auto entry : this->_allocations) {
//         if (entry != NULL) {
//             logCInfo(LC_RM | LC_SCHED) << "     - " << *entry; // << " " << this->id();
//         } else {
//             logCWarn(LC_RM | LC_SCHED) << "     - INVALID ALLOCATION"; // << this->id();
//         }
//     }
// }

// ResourceAllocationEntry *Resource::getFirstAllocation()
// {
//     if (this->_allocations.size() > 0 && this->_allocations.front() != NULL &&
//         this->_allocations.front()->valid())
//         return this->_allocations.front();
//     else
//         return NULL;
// }

// size_t Resource::getNumAllocations()
// {
//     return this->_allocations.size();
// }

// void Resource::update(Task *task, Time from, Time to)
// {
//     assert(task != NULL);
//     assert(to >= from);

//     auto entry = task->allocation();

//     if (!entry->valid())
//         logError << "Allocation entry for task " << task->id() << " is not valid!";

//     assert(entry->valid() == true);

//     // Time     origTo   = entry->to();
//     // Time     origFrom = entry->from();
//     // Duration diff     = to - origTo;

//     entry->update(from, to);

//     assert(entry->valid() == true);
//     assert(entry->resource() == this);

//     logInfo(1) << "Updated allocation " << this->id() << " for task " << task->id() << " from " << from
//                << " --> " << to;

//     this->busyUntil(this->_allocations.back()->to());
// }

// bool Resource::allocate(Task *task, Time from, Time to)
// {
//     assert(task != NULL);
//     assert(to >= from);

// #ifndef NDEBUG
//     if (this->_allocations.size() > 0 && this->_allocations.back() != NULL &&
//         this->_allocations.back()->to() > from) {
//         logError << "Cannot allocate " << this->id() << " for task " << task->id() << " from " << from
//                  << " --> " << to << " because last allocation only ends at "
//                  << this->_allocations.back()->to();
//         return false;
//     }
// #endif

//     auto entry = task->allocation();

//     if (entry->valid()) {
//         switch (task->type()) {
//         case TaskType::idle:
//         case TaskType::disconnect:
//             logError << "Allocation entry for task " << task->id() << " on resource " << this->id() << "/"
//                      << this->executor()->worker()->id() << " is still valid!";
//             break;
//         default:
//             logError << "Allocation entry for task " << task->stage()->app()->id() << "/" << task->id()
//                      << " on resource " << this->id() << "/" << this->executor()->worker()->id()
//                      << " is still valid!";
//             break;
//         }
//     }
//     assert(entry->valid() == false);

//     entry->set(task, this, from, to);
//     entry->modified(Time::now());
//     entry->allocated++;

//     assert(entry->valid() == true);
//     assert(entry->resource() == this);

//     if (this->_allocations.full())
//         this->_allocations.set_capacity(this->_allocations.capacity() * 2);

//     this->_allocations.push_back(entry);
//     this->_busyUntil = this->_allocations.back()->to();

//     logInfo(1) << "Allocated " << this->id() << " for task " << task->id() << " from " << from << " --> " << to;

//     return true;
// }

// void Resource::clear()
// {
//     logCDebug(LC_SCHED) << "Clearing all " << this->_allocations.size() << "/" << this->_allocations.capacity()
//                         << " of resource " << this->id();

//     for (auto entry : this->_allocations) {
//         logCDebug(LC_SCHED) << " -> " << *entry;
//     }

//     while (this->_allocations.size() > 0 && this->_allocations.back() != NULL &&
//            this->_allocations.back()->task()->state() < TaskState::scheduled) {
//         auto entry = this->_allocations.back();
//         this->_allocations.pop_back();

//         logCDebug(LC_SCHED) << "Clearing entry " << *entry;

//         entry->valid(false);
//         entry->modified(Time::now());
//         entry->cleared++;
//     }

//     if (!this->_allocations.empty())
//         this->_busyUntil = this->_allocations.back()->to();
//     else
//         this->_busyUntil = epoch_start;
// }

// void Resource::clear(Task *task)
// {
//     try {
//         assert(task != NULL);
//         auto entry = task->allocation();
//         if (!entry->valid())
//             logError << "Task " << task->id() << " allocation is INVALID! Last modified at "
//                      << entry->modified() << " alloc/cleared " << entry->allocated << "/" << entry->cleared;

//         assert(entry->valid() == true);
//         assert(entry->resource() == this);

//         if (this->_allocations.front() == entry) {
//             this->_allocations.pop_front();

//             entry->valid(false);
//             entry->modified(Time::now());
//             entry->cleared++;

//             logCDebug(LC_SCHED) << "Cleared allocation of " << this->id() << " for task " << task->id();
//         } else {

//             logError << "Cannot clear intermediate allocation for task " << task->id() << " from "
//                      << this->id();
//         }
//     } catch (std::exception const &e) {
//         logError << e.what();
//     }

//     if (!this->_allocations.empty())
//         this->_busyUntil = this->_allocations.back()->to();
//     else
//         this->_busyUntil = epoch_start;
// }

void Resource::update(Resource *parent, Cluster *cluster, NodeClass *nodeclass, Node *node)
{
    assert(parent != NULL);

    this->id(parent->id() + "/" + this->name());
    this->parent(parent);

    if (this->type() != ResourceType::cluster)
        this->cluster(cluster);
    if (this->type() != ResourceType::nodeclass)
        this->nodeclass(nodeclass);
    if (this->type() != ResourceType::node)
        this->node(node);

    for (Resource *child : this->children()) {
        child->update(this, this->cluster(), this->nodeclass(), this->node());
    }
}

void Resource::child(ResourceType type, Resource *child)
{
    child->parent(this);
    this->_children[type].push_back(child);

    if (type == ResourceType::node) {
        Node *node = static_cast<Node *>(child);
        child->name(node->id());
    } else {
        std::stringstream ss;
        ss << type << this->_children[type].size() - 1;
        child->name(ss.str());
    }

    child->update(this, this->cluster(), this->nodeclass(), this->node());
    logInfo(11) << "Added child " << child->id() << " to parent " << this->id() << " (" << this->type() << ")";
}

std::list<Resource *> Resource::children(ResourceType type) const
{
    auto res = this->_children.find(type);
    if (res != this->_children.end())
        return res->second;
    else
        return std::list<Resource *>();
}

std::list<Resource *> Resource::children()
{
    std::list<Resource *> all;
    for (auto &res : this->_children)
        all.insert(all.end(), res.second.begin(), res.second.end());
    return all;
}

Resource *Resource::parent(ResourceType type)
{
    if (this->type() == type)
        return this;
    if (this->parent() != NULL)
        return this->parent()->parent(type);

    return NULL;
}

const Resource *Resource::parent(ResourceType type) const
{
    if (this->type() == type)
        return this;
    if (this->parent() != NULL)
        return this->parent()->parent(type);

    return NULL;
}

void Resource::cluster(Cluster *cluster)
{
    this->_cluster = cluster;
    for (Resource *child : this->children()) {
        child->cluster(cluster);
    }
}

void Resource::nodeclass(NodeClass *nodeclass)
{
    this->_nodeclass = nodeclass;
    for (Resource *child : this->children()) {
        child->nodeclass(nodeclass);
    }
}

void Resource::node(Node *node)
{
    this->_node = node;
    for (Resource *child : this->children()) {
        child->node(node);
    }
}

const std::pair<uint64_t, Duration> &Resource::metrics(Resource *const c0, Resource *const c1) const
{
    uint c0idx = c0->index();
    uint c1idx = c1->index();
    return this->_res2res(c0idx, c1idx);
}

uint64_t Resource::bandwidth(Resource *const c0, Resource *const c1) const
{
    try {
        uint     c0idx = c0->index();
        uint     c1idx = c1->index();
        uint64_t res   = this->_res2res(c0idx, c1idx).first;
        logInfo(21) << "bandwidth from " << c0->id() << " -> " << c1->id() << " = " << res << "b/s";

        return res;
    } catch (const std::out_of_range &e) {
        logError << "Out of range  exception (" << e.what() << ") : "
                 << "No " << c0->id() << " or " << c1->id() << " in resource " << this->id();
        return 0;
    }
}

Duration Resource::latency(Resource *const c0, Resource *const c1) const
{
    try {
        uint     c0idx = c0->index();
        uint     c1idx = c1->index();
        Duration res   = this->_res2res(c0idx, c1idx).second;
        logInfo(21) << "latency from " << c0->id() << " -> " << c1->id() << " = " << res;

        return res;
    } catch (const std::out_of_range &e) {
        logError << "Out of range  exception (" << e.what() << ") : "
                 << "No " << c0->id() << " or " << c1->id() << " in resource " << this->id();
        return Duration::max();
    }
}

void Resource::computePathCosts(Resource *res, Resource *pred, uint idx, uint64_t bw, Duration lat)
{
    logInfo(20) << (pred != NULL ? pred->id() : "n/a") << " --> " << res->id() << " = " << bw << "b/s + "
                << lat;

    if (res->parent() && res->parent() != pred) {
        // Get parent of this resource
        uint ti = res->index();
        uint pi = res->parent()->index();

        // Check bandwidth and latency between this resource and its parent.
        uint64_t cur_bw  = this->_res2res(ti, pi).first;
        Duration cur_lat = this->_res2res(ti, pi).second;

        // The bandwidth/latency from the origin is in bw/lat.
        uint64_t new_bw  = std::min(bw, cur_bw);
        Duration new_lat = lat + cur_lat;

        this->_res2res(idx, pi) = std::make_pair(new_bw, new_lat);

        computePathCosts(res->parent(), res, idx, new_bw, new_lat);
    }

    for (auto &c : res->children()) {
        if (c == pred)
            continue;

        // Get parent of this resource
        uint ti = res->index();
        uint ci = c->index();

        // Check bandwidth and latency between this resource and its parent.
        uint64_t cur_bw  = this->_res2res(ti, ci).first;
        Duration cur_lat = this->_res2res(ti, ci).second;

        // The bandwidth/latency from the origin is in bw/lat.
        uint64_t new_bw  = std::min(bw, cur_bw);
        Duration new_lat = lat + cur_lat;

        this->_res2res(idx, ci) = std::make_pair(new_bw, new_lat);

        computePathCosts(c, res, idx, new_bw, new_lat);
    }
}

void Resource::completeIoCostMatrix()
{
    for (auto &m : this->_resIndex) {
        uint      idx = m.second;
        Resource *res = m.first;
        logInfo(10) << " I/O costs from " << res->id();
        this->computePathCosts(res, NULL, idx, ULLONG_MAX, Duration::zero());
    }
}

void ResourceAllocationEntry::update(Time from, Time to)
{
    this->_from = from;
    this->_to   = to;
    this->modified(Time::now());
}

void ResourceAllocationEntry::set(Task *task, Executor* executor, Time from, Time to)
{
    assert(this->_valid == false);
//    logInfo(10) "Setting " << task->id() << " to " << resource->id();
    this->_task     = task;
    this->_executor = executor;
    this->_from     = from;
    this->_to       = to;
    this->_valid    = true;
}

/****************************************************************************************************
 *                                              RESOURCE TYPE                                       *
 ****************************************************************************************************/
std::ostream &operator<<(std::ostream &os, const ResourceType &obj)
{
    switch (obj) {
    case ResourceType::cluster:
        os << "cluster";
        break;
    case ResourceType::nodeclass:
        os << "nodeclass";
        break;
    case ResourceType::node:
        os << "node";
        break;
    case ResourceType::numanode:
        os << "numanode";
        break;
    case ResourceType::core:
        os << "core";
        break;
    case ResourceType::cpu:
        os << "cpu";
        break;
    case ResourceType::gpu:
        os << "gpu";
        break;
    case ResourceType::fpga:
        os << "fpga";
        break;
    case ResourceType::memory:
        os << "memory";
        break;
    case ResourceType::storage:
        os << "storage";
        break;
    case ResourceType::network:
        os << "network";
        break;
    case ResourceType::rdma:
        os << "rdma";
        break;
    default:
        os << "unknown";
        break;
    }
    return os;
}

std::istream &operator>>(std::istream &is, ResourceType &obj)
{
    std::string s(std::istreambuf_iterator<char>(is), {});

    if (s == "cluster")
        obj = ResourceType::cluster;
    else if (s == "nodeclass")
        obj = ResourceType::nodeclass;
    else if (s == "node")
        obj = ResourceType::node;
    else if (s == "numanode")
        obj = ResourceType::numanode;
    else if (s == "core")
        obj = ResourceType::core;
    else if (s == "cpu")
        obj = ResourceType::cpu;
    else if (s == "gpu")
        obj = ResourceType::gpu;
    else if (s == "fpga")
        obj = ResourceType::fpga;
    else if (s == "memory")
        obj = ResourceType::memory;
    else if (s == "storage")
        obj = ResourceType::storage;
    else if (s == "network")
        obj = ResourceType::network;
    else if (s == "rdma")
        obj = ResourceType::rdma;
    else
        is.setstate(std::ios::failbit);

    return is;
}

/****************************************************************************************************
 *                                             RESOURCE STATE                                       *
 ****************************************************************************************************/
std::ostream &operator<<(std::ostream &os, const ResourceState &obj)
{
    switch (obj) {
    case ResourceState::idle:
        os << "idle";
        break;
    case ResourceState::disabled:
        os << "disabled";
        break;
    case ResourceState::busy:
        os << "busy";
        break;
    case ResourceState::error:
        os << "error";
        break;
    case ResourceState::maintenance:
        os << "maintenance";
        break;
    default:
        os << "unknown";
        break;
    }
    return os;
}

/****************************************************************************************************
 *                                             RESOURCE MODEL                                       *
 ****************************************************************************************************/
std::ostream &operator<<(std::ostream &os, const ResourceModel &obj)
{
    os << obj.id();
    return os;
}

/****************************************************************************************************
 *                                        RESOURCE ALLOCATION ENTRY                                 *
 ****************************************************************************************************/
// std::ostream &operator<<(std::ostream &os, const ResourceAllocationEntry &a)
// {
//     std::ostringstream appid;
//     if (a.task() != NULL && a.task()->stage() != NULL) {
//         appid << a.task()->stage()->app()->id();
//     } else {
//         appid << "noapp";
//     }

//     os << "[" << a.from() << "-" << a.to() << " for task " << appid.str() << "/"
//        << (a.task() != NULL ? a.task()->id() : "unassigned") << " ("
//        << (a.task() != NULL ? a.task()->state() : TaskState::unknown) << ") on resource "
//        << (a.resource() != NULL ? a.resource()->id() : "unassigned") << "]";
//     if (a.firstOnExecutorOfStage())
//         os << " P";
//     return os;
// }
