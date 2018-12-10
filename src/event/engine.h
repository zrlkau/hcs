// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef engine_h
#define engine_h

#include "common.h"
#include "event.h"

#include <boost/signals2/signal.hpp>

#include <condition_variable>
#include <mutex>
#include <queue>
#include <unordered_map>

enum class EngineMode { Unknown, Simulation, Real };

extern Time baseTime;

class Engine
{
  public:
    Engine(EngineMode mode);

    static void run(Engine *sim);
    void        run();
    void        event(Event *ev);

    void listen(EventType type, std::function<void(Event *)> func);
    void listen(int priority, EventType type, std::function<void(Event *)> func);
    void signal(Event *ev);

    void listen(AppId app, EventType type, std::function<void(Event *)> func);
    void listen(AppId app, int priority, EventType type, std::function<void(Event *)> func);

    GET(EngineMode, mode);
    GET(Time, start);

    Time now() const;

    void application(App *app);
    App *application(AppId id);

    GET(std::unordered_map<AppId COMMA App *> &, apps);

    void event2(Event *ev);

  protected:
  private:
    SET(Time, start);

    SETGET(SchedulerBase *, scheduler);
    SET(EngineMode, mode);

    std::priority_queue<Event *, std::vector<Event *>, compare<Event>> events;
    std::mutex                                                         eventsMutex;
    std::condition_variable                                            eventsSignal;

    Time _start;

    SchedulerBase *_scheduler;

    EngineMode _mode;

    std::unordered_map<AppId, App *> _apps;
    std::mutex                       _appsMutex;

    std::unordered_map<AppId, std::unordered_map<EventType, boost::signals2::signal<void(Event *)>>>
                                                                          _appSignals;
    std::unordered_map<EventType, boost::signals2::signal<void(Event *)>> _globalSignals;
};

std::ostream &operator<<(std::ostream &os, const EngineMode &obj);

#endif
