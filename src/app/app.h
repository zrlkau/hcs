// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef app_h
#define app_h

#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <string>

#include "app/io.h"
#include "app/stage.h"
#include "app/task.h"
#include "common.h"
#include "helper/idmapper.h"
#include "scheduler/schedule.h"

class AppMetrics
{
  public:
    AppMetrics()
        : _submitted(Time::zero()), _started(Time::zero()), _finished(Time::zero()),
          _runtime(Duration::zero()){};

    SETGET(Time, submitted);
    SETGET(Time, started);
    SETGET(Time, finished);

    SETGET(Duration, runtime);

  private:
    Time _submitted;
    Time _started;
    Time _finished;

    Duration _runtime;
};

class App
{
  public:
    App(AppId id, std::string name, std::string driverUrl);
    ~App();

    std::unique_lock<Mutex> guard(int id);
    void                    lock(int id);
    void                    unlock(int id);

    GET(AppId, id);
    GET(std::string, name);

    GET(TaskState, state);
    void state(TaskState state);

    // Misc
    GETPTR(Schedule, schedule);
    SETGET(SchedulerBase *, scheduler);
    GET(AppMetrics &, metrics);

    GET(IDMapper<size_t COMMA StageKey> &, stageKeyMapper);
    GET(std::map<StageId COMMA Stage *> &, stages);
    Stage *stage(StageId id);
    void   addStages(std::map<Stage *, std::list<StageId>> stages);

    SETGET(std::string, driverUrl);

    void    addWorker(Worker *worker);
    Worker *removeWorker(void);
    int     getNrAssignedWorkers(void);

  protected:
    SET(std::string, id);
    SET(std::string, name);

    void load();
    void store();

  private:
    TaskState _state;

    AppId       _id;   // unique identified of this app instance
    std::string _name; // name of this app that stays the same between various runs.

    std::map<StageId, Stage *>                _stages; // only active stages
    std::map<std::pair<Stage *, Stage *>, Io> _io;

    AppMetrics _metrics;

    Schedule                   _schedule;
    SchedulerBase *            _scheduler;
    IDMapper<size_t, StageKey> _stageKeyMapper;

    Mutex _mutex;

    std::string _driverUrl;
};

std::ostream &operator<<(std::ostream &os, const App &obj);
#endif
