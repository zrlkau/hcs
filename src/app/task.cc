// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//          Adrian Schuepbach <dri@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "app/task.h"
#include "app/app.h"
#include "app/io.h"
#include "app/stage.h"
#include "common.h"
#include "event/engine.h"
#include "resourcemanager/executor.h"
#include "resourcemanager/worker.h"

#include <chrono>
#include <regex>
#include <string>
#include <thread>

Task::Task(const std::string &id, TaskIdx index, Stage *stage) : _state(TaskState::unknown)
{
    this->id(id);
    this->index(index);
    this->stage(stage);
    this->state(TaskState::unready);
    this->type(stage->type());
}

Task::~Task()
{
}

std::unique_lock<std::timed_mutex> __attribute__((warn_unused_result)) Task::guard()
{
    logCDebug(LC_LOCK | LC_APP) << "About to acquire task " << this->id() << " guard";
    auto lock = std::unique_lock<std::timed_mutex>(this->_mutex, std::defer_lock);
    while (!lock.try_lock_for(10s)) {
        logCWarn(LC_LOCK | LC_SCHED) << "Unable to acquire guard for task " << this->id()
                                     << " within the last 10s";
    }
    logCDebug(LC_LOCK | LC_APP) << "Acquired task " << this->id() << " guard";
    return lock;
}

void Task::state(TaskState state)
{
    logInfo(1) << "Transitioning task " << this->id() << " from " << this->_state << " to " << state;

    if (this->_state == state)
        return;

    switch (state) {
    case TaskState::unknown:
        break;
    case TaskState::unready:
        break;
    case TaskState::ready:
        assert(this->_state == TaskState::unready);
        this->_metrics.ready(Time::now());
        break;
    case TaskState::scheduled:
        assert(this->_state == TaskState::ready);
        this->_metrics.scheduled(Time::now());
        this->stage()->taskScheduled(this);
        //	engine->signal(new TaskScheduledEvent(this));
        break;
    case TaskState::running:
        assert(this->_state == TaskState::scheduled);
        this->_metrics.started(Time::now());
        this->stage()->taskRunning(this);
        //	engine->signal(new TaskStartedEvent(this));
        break;
    case TaskState::finished:
        assert(this->_state == TaskState::running);
        this->_metrics.finished(Time::now());
        this->_metrics.runtime(this->_metrics.finished() - this->_metrics.started());
        this->stage()->taskFinished(this);
        //	engine->signal(new TaskFinishedEvent(this));
        break;
    }

    this->_state = state;
}

std::ostream &operator<<(std::ostream &os, const Task &obj)
{
    os << obj.id() << " (state=" << obj.state() << " stage=" << obj.stage()->id() << ")";
    return os;
}

std::ostream &operator<<(std::ostream &os, const TaskType &obj)
{
    switch (obj) {
    case TaskType::idle:
        os << "idle";
        break;
    case TaskType::source:
        os << "source";
        break;
    case TaskType::sink:
        os << "sink";
        break;
    case TaskType::compute:
        os << "compute";
        break;
    case TaskType::cuda:
        os << "cuda";
        break;
    case TaskType::opencl:
        os << "opencl";
        break;
    case TaskType::openmp:
        os << "openmp";
        break;
    case TaskType::mpi:
        os << "mpi";
        break;
    case TaskType::fpga:
        os << "fpga";
        break;
    case TaskType::send:
        os << "send";
        break;
    case TaskType::receive:
        os << "receive";
        break;
    case TaskType::load:
        os << "load";
        break;
    case TaskType::store:
        os << "store";
        break;
    case TaskType::disconnect:
        os << "disconnect";
        break;
    default:
        os << "unknown";
        break;
    }
    return os;
}

std::istream &operator>>(std::istream &is, TaskType &obj)
{
    std::string s(std::istreambuf_iterator<char>(is), {});

    if (s == "idle")
        obj = TaskType::idle;
    else if (s == "source")
        obj = TaskType::source;
    else if (s == "sink")
        obj = TaskType::sink;
    else if (s == "compute")
        obj = TaskType::compute;
    else if (s == "cuda")
        obj = TaskType::cuda;
    else if (s == "opencl")
        obj = TaskType::opencl;
    else if (s == "openmp")
        obj = TaskType::openmp;
    else if (s == "mpi")
        obj = TaskType::mpi;
    else if (s == "fpga")
        obj = TaskType::fpga;
    else if (s == "send")
        obj = TaskType::send;
    else if (s == "receive")
        obj = TaskType::receive;
    else if (s == "load")
        obj = TaskType::load;
    else if (s == "store")
        obj = TaskType::store;
    else
        is.setstate(std::ios::failbit);

    return is;
}

std::istream &operator>>(std::istream &is, TaskState &obj)
{
    std::string s(std::istreambuf_iterator<char>(is), {});

    if (s == "unknown")
        obj = TaskState::unknown;
    else if (s == "unready")
        obj = TaskState::unready;
    else if (s == "ready")
        obj = TaskState::ready;
    else if (s == "scheduled")
        obj = TaskState::scheduled;
    else if (s == "running")
        obj = TaskState::running;
    else if (s == "finished")
        obj = TaskState::finished;
    else
        is.setstate(std::ios::failbit);

    return is;
}

std::ostream &operator<<(std::ostream &os, const TaskState &obj)
{
    switch (obj) {
    case TaskState::unready:
        os << "unready";
        break;
    case TaskState::ready:
        os << "ready";
        break;
    case TaskState::scheduled:
        os << "scheduled";
        break;
    case TaskState::running:
        os << "running";
        break;
    case TaskState::finished:
        os << "finished";
        break;
    default:
        os << "unknown";
        break;
    }
    return os;
}

TaskMetrics::TaskMetrics()
    : _gbcTime(Duration::zero()), _cpuTime(Duration::zero()), _bytesRead(0UL), _bytesWritten(0UL), _ioLoad(0.0),
      /*_heapSize(0UL), _heapFree(0UL), */ _inData(0UL), _outData(0UL), _inDataEstimated(true){};

void TaskMetrics::inData(Executor *executor, size_t nBytes)
{
    assert(executor != NULL);

    if (nBytes == 0)
        return;

    Node *node = executor->worker()->node();
    if (this->_inDataEstimated) {
        // per-executor data does not count towards per-node data. Otherwise it would look as if the
        // total data increases (to the LR algo) and this won't lead to the desired results that the
        // on-executor data is cheaper to access than the on-node data.
        this->_inDataPerExecutor[executor] = nBytes;
        this->_inDataPerNode[node]         = nBytes;
        this->_inData                      = nBytes;
        this->_inDataEstimated             = false;
    } else {
        this->_inDataPerExecutor[executor] += nBytes;
        this->_inDataPerNode[node] += nBytes;
        this->_inData += nBytes;
    }
}

void TaskMetrics::inData(Node *node, size_t nBytes)
{
    assert(node != NULL);

    if (nBytes == 0)
        return;

    if (this->_inDataEstimated) {
        this->_inDataPerNode[node] = nBytes;
        this->_inData              = nBytes;
        this->_inDataEstimated     = false;
    } else {
        this->_inDataPerNode[node] += nBytes;
        this->_inData += nBytes;
    }
}

void TaskMetrics::inData(size_t nBytes, bool reset)
{
    if (reset) {
        this->_inData = nBytes;

        // We know the entire in data but not where it is so a simple assumption is to adjust data
        // so that the total amount of data matches nBytes and the ratio between executors/nodes
        // stays the same as before.
        size_t dataOnExecutors = 0UL;
        size_t dataOnNodes     = 0UL;
        for (auto &entry : this->_inDataPerExecutor)
            dataOnExecutors += entry.second;
        for (auto &entry : this->_inDataPerNode)
            dataOnNodes += entry.second;

        size_t tmpBytes = nBytes;
        if (dataOnExecutors > 0) {
            for (auto &entry : this->_inDataPerExecutor) {
                double fraction = entry.second / dataOnExecutors;
                entry.second    = tmpBytes * fraction;
                tmpBytes -= entry.second;
                dataOnExecutors -= entry.second;
            }
        }

        tmpBytes = nBytes;
        if (dataOnNodes > 0) {
            for (auto &entry : this->_inDataPerNode) {
                double fraction = entry.second / dataOnNodes;
                entry.second    = tmpBytes * fraction;
                tmpBytes -= entry.second;
                dataOnNodes -= entry.second;
            }
        }

        // for (auto &entry : this->_inDataPerExecutor) {
        //     logInfo(0) << "Adjusting input from executor " << entry.first->id() << " to " << entry.second;
        // }
        // for (auto &entry : this->_inDataPerNode) {
        //     logInfo(0) << "Adjusting input from node " << entry.first->id() << " to " << entry.second;
        // }

        this->_inDataEstimated = false;
    } else if (this->_inDataEstimated) {
        this->_inData          = nBytes;
        this->_inDataEstimated = false;
    } else {
        this->_inData += nBytes;
    }
}

size_t TaskMetrics::inData(Executor *executor) const
{
    //    logWarn << "this=" << this << " executor=" << executor;
    auto it = this->_inDataPerExecutor.find(executor);
    if (it != this->_inDataPerExecutor.end())
        return it->second;
    else
        return 0UL;
}

size_t TaskMetrics::inData(Node *node) const
{
    auto it = this->_inDataPerNode.find(node);
    if (it != this->_inDataPerNode.end())
        return it->second;
    else
        return 0UL;
}

void IdleTask::state(TaskState state)
{
    switch (state) {
    case TaskState::running:
        assert(this->_state == TaskState::scheduled);
        this->_metrics.started(Time::now());
        break;
    default:
        break;
    }

    this->_state = state;
}

void DisconnectTask::state(TaskState state)
{
    switch (state) {
    case TaskState::running:
        //        assert(this->_state == TaskState::scheduled);
        this->_metrics.started(Time::now());
        break;
    default:
        break;
    }

    this->_state = state;
}
