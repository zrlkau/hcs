// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef app_stage_h
#define app_stage_h

#include "app/io.h"
#include "app/task.h"
#include "helper/macros.h"

#include <mutex>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

class StageMetrics
{
  public:
    StageMetrics()
        : _submitted(Time::zero()), _ready(Time::zero()), _scheduled(Time::zero()), _started(Time::zero()),
          _finished(Time::zero()), _runtime(Duration::zero()), _meanInData(0UL), _inData(0UL){};
    StageMetrics(const StageMetrics &) = delete;
    StageMetrics(StageMetrics &&)      = delete;
    StageMetrics &operator=(const StageMetrics &) = delete;
    StageMetrics &operator=(const StageMetrics &&) = delete;

    SETGET(Time, submitted);
    SETGET(Time, ready);
    SETGET(Time, scheduled);
    SETGET(Time, started);
    SETGET(Time, finished);

    SETGET(Duration, runtime);

    SETGET(size_t, meanInData);
    GET(size_t, inData);
    void inData(size_t nBytes)
    {
        this->_inData += nBytes;
    }

  private:
    Time _submitted;
    Time _ready;
    Time _scheduled;
    Time _started;
    Time _finished;

    Duration _runtime;

    size_t _meanInData;
    size_t _inData;
};

class Stage
{
  public:
    Stage(const std::string &id,
          StageId            nid,
          const std::string &function,
          StageKey           key,
          App *              app,
          TaskType           type,
          size_t             size);
    ~Stage();
    Stage(const Stage &) = delete;
    Stage(Stage &&)      = delete;
    Stage &operator=(const Stage &) = delete;
    Stage &operator=(const Stage &&) = delete;

    std::unique_lock<std::timed_mutex> guard();

    GET(size_t, size);
    GET(std::string, id);  // TODO: id -> name
    GET(StageId, nid);     // TODO: nid -> id
    SETGET(StageKey, key); // TODO: key -> uid ?

    // Information about tasks in this tage
    Task *task(size_t index);

    GET(std::list<Task *> &, tasks); // get all tasks

    // Event handler for task events
    void taskReady(Task *task);
    void taskScheduled(Task *task);
    void taskRunning(Task *task);
    void taskFinished(Task *task);

    // DAG-related information
    void prev(Io *in);
    GET(std::set<Io *> &, prev);
    void next(Io *out);
    GET(std::set<Io *> &, next);

    void sortTasks();

    // In which partition and in what state is this stage.
    GET(App *, app);
    SETGET(SchedulerBase *, scheduler);

    GET(TaskState, state);
    void state(TaskState state);

    // Misc stuff.
    void decrUnsatDeps(); // TODO: Rename

    GET(StageMetrics &, metrics);

    SETGET(boost::dynamic_bitset<> &, nodeClasses); // TODO -> stage

    GET(TaskType, type);
    GET(std::string, function);
    void restype(ResourceType restype);
    GET(std::set<ResourceType> &, restype);

    GET(int, unsatDeps);
    GET(int, level);
    void level(int level);
    SETGET(double, weight);
    SETGET(int, mark);

    SETGET(bool, initialized);

  protected:
    SET(App *, app);

    SET(std::string, id);
    SET(StageId, nid);

    SET(size_t, size);
    SET(int, unsatDeps);
    SET(TaskType, type);
    SET(std::string, function);

    void load(const ptree &data);

    bool check(TaskState state = TaskState::unknown); // check if this object is ok in the given 'state'
  private:
    std::string _id;
    StageId     _nid;
    StageKey    _key; // hash value as key for oracle to look up performance records.

    // All tasks
    std::unordered_map<uint, Task> _allTasks;
    // All tasks ordered by their input size
    std::list<Task *> _tasks;

    // Previous stages on the same path(s)
    std::set<Io *> _prev; // -> in
    // Next stages on the same path(s)
    std::set<Io *> _next; // -> out

    App *_app;
    // Stage state - I'm not sure yet what this means, i.e. if not all tasks have the same state.
    TaskState _state;
    bool      _initialized;

    int    _unsatDeps;
    int    _level; // number of levels of descendents
    size_t _size;
    double _weight; // stage weight for scheduling
    int    _mark; // for algorithms that need to mark a stage
    
    
    // Timing related (during/past execution);
    StageMetrics _metrics;

    // Resource related (same for all tasks)
    boost::dynamic_bitset<> _nodeClasses;
    TaskType                _type; // General task type
    std::string             _function;
    std::set<ResourceType>  _restype;

    SchedulerBase *_scheduler;

    std::timed_mutex _mutex;
};

std::ostream &operator<<(std::ostream &os, const Stage &obj);

namespace std {
template <> struct hash<Stage> {
    size_t operator()(const Stage &obj) const
    {
        return (hash<std::string>()(obj.id()));
    }
};
} // namespace std

#endif
