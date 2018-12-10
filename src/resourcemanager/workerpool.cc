// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "resourcemanager/workerpool.h"
#include "resourcemanager/worker.h"

void WorkerPool::put(Worker *worker)
{
    if (!worker)
        throw(std::invalid_argument("NULL pointer"));

    auto     guard = this->guard();
    WorkerId wid   = worker->id();

    if (this->_workerpool.find(wid) != this->_workerpool.end())
        return;
    this->_workerpool.emplace(wid, worker);

    Node *node = worker->node();

    auto np = this->_nodepool.find(node);
    if (np == this->_nodepool.end()) {
        np = this->_nodepool.emplace(node, std::multimap<uint, Worker *, std::greater<uint>>()).first;
    }
    np->second.emplace(worker->getUsageCounter(), worker);
}

Worker *WorkerPool::get(WorkerId wid)
{
    auto guard = this->guard();
    auto it    = this->_workerpool.find(wid);
    if (it == this->_workerpool.end())
        return NULL;
    else
        return it->second;
}

// Worker *WorkerPool::take(WorkerId wid)
// {
//     Worker *worker = NULL;

//     auto it = this->_workerpool.find(wid);
//     if (it == this->_workerpool.end())
//         return NULL;
//     else {
//         worker     = it->second;
//         Node *node = worker->node();

// 	this->_nodepool.at(node).erase(worker);
// 	this->_workerpool.erase(worker->id());
//     }

//     return worker;
// }

// Worker *WorkerPool::take()
// {
//     Worker *worker = NULL;

//     auto guard = this->guard();
//     auto start = this->_nextNode;

//     do {
//         if (this->_nextNode->second.size() > 0) {
//             auto it = this->_nextNode->second.begin();
//             worker  = it->second;
//             this->_nextNode->second.erase(it);
//             this->_workerpool.erase(worker->id());
//         }

//         this->_nextNode++;
//         if (this->_nextNode == this->_nodepool.end())
//             this->_nextNode = this->_nodepool.begin();
//     } while (worker == NULL && this->_nextNode != start);

//     return worker;
// }

Worker *WorkerPool::take()
{
    Worker *worker = NULL;

    auto guard = this->guard();

    auto   it           = this->_nodepool.begin();
    auto   node         = this->_nodepool.end();
    size_t nFreeWorkers = 0UL;

    while (it != this->_nodepool.end()) {
        if (it->second.size() > nFreeWorkers) {
            node         = it;
            nFreeWorkers = it->second.size();
        }
        it++;
    }

    if (node != this->_nodepool.end() && node->second.size() > 0) {
        auto it = node->second.begin();
        worker  = it->second;
        node->second.erase(it);
        this->_workerpool.erase(worker->id());
    }
    return worker;
}

size_t WorkerPool::size(void)
{
    auto guard = this->guard();
    return this->_workerpool.size();
}

void WorkerPool::clear(void)
{
    auto guard = this->guard();
    this->_workerpool.clear();
    this->_nodepool.clear();
    this->_nextNode = this->_nodepool.begin();
}

std::unique_lock<std::timed_mutex> __attribute__((warn_unused_result)) WorkerPool::guard()
{
//    logCDebug(LC_LOCK | LC_RM) << "About to acquire worker pool " << this << " guard";
    auto lock = std::unique_lock<std::timed_mutex>(this->_mutex, std::defer_lock);
    while (!lock.try_lock_for(10s)) {
        logCWarn(LC_LOCK | LC_SCHED) << "Unable to acquire guard for worker pool " << this
                                     << " within the last 10s";
    }
//    logCDebug(LC_LOCK | LC_APP) << "Acquired worker pool " << this << " guard";
    return lock;
}

std::string WorkerPool::getLoad(void)
{
    std::ostringstream os;
    auto guard = this->guard();

    for (auto ne : this->_nodepool) {
        Node * node = ne.first;
        size_t load = ne.second.size();
        os << " " << node->id() << " (" << load << ")";
    }

    return os.str();
}
