// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//          Adrian Schuepbach <dri@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef app_task_h
#define app_task_h

#include "cluster/node.h"
#include "common.h"

#include <atomic>
#include <chrono>
#include <future>
#include <list>
#include <map>
#include <mutex>
#include <set>

// TODO:
// - Protect task structures with mutexes (AppPartition as well).
// - Evolve Partitions. Two options:
//   (1) merge two partitions as soon as they would overlap. This can
//       potentially result in a almost 3x the original size (if
//       partitions A and B from one side and C from the other side
//       join back to bac)k
//   (2) chip nodes off of the other dependent partition. That keeps
//       the size down to max. what we specified.
//   (3) Do a mix. If the combined size would be too large, we can do
//       (2) otherwise (1).

using boost::property_tree::ptree;

class TaskMetrics
{
  public:
    TaskMetrics();
    TaskMetrics(const TaskMetrics &) = delete;
    TaskMetrics(TaskMetrics &&)      = delete;
    TaskMetrics &operator=(const TaskMetrics &) = delete;
    TaskMetrics &operator=(const TaskMetrics &&) = delete;

    SETGET(Time, ready);
    SETGET(Time, scheduled);
    SETGET(Time, started);
    SETGET(Time, finished);

    SETGET(Duration, runtime);

    SETGET(Duration, gbcTime);
    SETGET(Duration, cpuTime);

    SETGET(size_t, bytesRead);
    SETGET(size_t, bytesWritten);

    SETGET(double, ioLoad);

    // SETGET(size_t, heapSize);
    // SETGET(size_t, heapFree);

    GET(size_t, inData);
    SETGET(size_t, outData);

    GET(bool, inDataEstimated);

    void   inData(Executor *executor, size_t nBytes);
    void   inData(Node *node, size_t nBytes);
    void   inData(size_t nBytes, bool reset = false);
    size_t inData(Executor *executor) const;
    size_t inData(Node *node) const;

  protected:
  private:
    Time _ready;
    Time _scheduled;
    Time _started;
    Time _finished;

    Duration _runtime;

    Duration _gbcTime;
    Duration _cpuTime;

    size_t _bytesRead;
    size_t _bytesWritten;

    double _ioLoad;

    // size_t _heapSize;
    // size_t _heapFree;

    std::unordered_map<Executor *, size_t> _inDataPerExecutor;
    std::unordered_map<Node *, size_t>     _inDataPerNode;
    size_t                                 _inData;
    size_t                                 _outData;

    bool _inDataEstimated;
};

class Task // : public Vertex<Task, Io>, public std::recursive_mutex
{
  public:
    Task(const std::string &id, TaskIdx index, Stage *stage);
    Task(const Task &) = delete;
    Task(Task &&)      = delete;
    ~Task();
    Task &operator=(const Task &) = delete;
    Task &operator=(const Task &&) = delete;

    std::unique_lock<std::timed_mutex> guard();

    SETGET(std::string, id);
    SETGET(TaskIdx, index); // index of this task within its stage

    SETGET(Stage *, stage);

    virtual void state(TaskState state);
    GET(TaskState, state);
    GET(TaskType, type);

    GET(TaskMetrics &, metrics);

    SETGET(SchedulerBase *, scheduler);
    GETPTR(ResourceAllocationEntry, allocation);

    static bool sortByInputSizeFunc(const Task *t0, const Task *t1)
    {
        return (t0->metrics().inData() > t1->metrics().inData());
    }

  protected:
    Task(){};
    TaskState   _state;
    TaskMetrics _metrics;

    SET(TaskType, type);

  private:
    ResourceAllocationEntry _allocation;

    Stage *_stage;

    SchedulerBase *_scheduler;

    std::timed_mutex _mutex;

    std::string _id;
    TaskIdx     _index; // task index within its stage
    TaskType    _type;
};

std::ostream &operator<<(std::ostream &os, const Task &obj);
std::ostream &operator<<(std::ostream &os, const TaskType &obj);
std::istream &operator>>(std::istream &is, TaskType &obj);
std::ostream &operator<<(std::ostream &os, const TaskState &obj);
std::istream &operator>>(std::istream &is, TaskState &obj);

namespace std {
template <> struct hash<Task> {
    size_t operator()(const Task &obj) const
    {
        return (hash<string>()(obj.id()));
    }
};
} // namespace std

class IdleTask : public Task
{
  public:
    IdleTask()
    {
        this->type(TaskType::idle);
        this->state(TaskState::ready);
        this->id("idle");
        this->index(static_cast<TaskIdx>(0));
        this->stage(NULL);
    }

    void state(TaskState state);

  private:
};

class DisconnectTask : public Task
{
  public:
    DisconnectTask()
    {
        this->type(TaskType::disconnect);
        this->state(TaskState::ready);
        this->id("disconnect");
        this->index(static_cast<TaskIdx>(0));
        this->stage(NULL);
    }

    void state(TaskState state);

  private:
};

#endif
