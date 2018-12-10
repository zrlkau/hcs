// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "resourcemanager/resourcepool.h"
#include "resourcemanager/executor.h"

void ExecutorPool::put(Executor *executor)
{
    if (!executor)
        throw(std::invalid_argument("NULL pointer"));

    auto guard = this->guard();

//    logCInfo(LC_RM) << "About to add executor " << executor->id() << " to pool " << this;
    this->_pool.emplace(executor);
}

bool ExecutorPool::get(Executor *executor)
{
    if (!executor)
        throw(std::invalid_argument("NULL pointer"));

    auto guard = this->guard();

//    logCInfo(LC_RM) << "About to remove executor " << executor->id() << " from pool " << this;

    assert(this->_pool.find(executor) != this->_pool.end());

    if (this->_pool.find(executor) != this->_pool.end()) {
        this->_pool.erase(executor);
        return true;
    } else {
        logCWarn(LC_RM) << "Cannot remove non-existing executor " << executor->id() << " from pool " << this;
        return false;
    }
}

Executor *ExecutorPool::take()
{
    auto guard = this->guard();

    if (this->_pool.size() > 0) {
        auto      it       = this->_pool.begin();
        Executor *executor = *it;
        this->_pool.erase(it);
        return executor;
    } else {
        return NULL;
    }
}

size_t ExecutorPool::size()
{
    auto guard = this->guard();
    return this->_pool.size();
}

bool ExecutorPool::empty()
{
    auto guard = this->guard();
    return this->_pool.empty();
}

void ExecutorPool::clear()
{
    auto guard = this->guard();
    this->_pool.clear();
}

// bool ExecutorPool::contains(Executor *executor) const
// {
//     if (!executor)
//         throw(std::invalid_argument("NULL pointer"));

//     auto guard = this->guard();

//     if (this->_pool.find(executor) != this->_pool.end()) {
//         return true;
//     } else
//         return false;
// }

std::unique_lock<std::timed_mutex> __attribute__((warn_unused_result)) ExecutorPool::guard()
{
//    logCDebug(LC_LOCK | LC_RM) << "About to acquire executor pool " << this << " guard";
    auto lock = std::unique_lock<std::timed_mutex>(this->_mutex, std::defer_lock);
    while (!lock.try_lock_for(10s)) {
        logCWarn(LC_LOCK | LC_SCHED) << "Unable to acquire guard for executor pool " << this
                                     << " within the last 10s";
    }
//    logCDebug(LC_LOCK | LC_APP) << "Acquired executor pool " << this << " guard";
    return lock;
}
