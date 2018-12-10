// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//          Adrian Schuepbach <dri@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "event/engine.h"
#include "app/app.h"
#include "event/event.h"
#include "helper/logger.h"
#include "helper/time.h"

#include <exception>
#include <iostream>

Time baseTime = Time::now() + Duration(10ms);

Engine::Engine(EngineMode mode) : _mode(mode)
{
    this->start(Time::now());

    this->_globalSignals.emplace(EventType::Kill, boost::signals2::signal<void(Event *)>());
    this->_globalSignals.emplace(EventType::Clock, boost::signals2::signal<void(Event *)>());
    this->_globalSignals.emplace(EventType::Status, boost::signals2::signal<void(Event *)>());

    this->_globalSignals.emplace(EventType::ApplicationSubmitted, boost::signals2::signal<void(Event *)>());
    this->_globalSignals.emplace(EventType::WorkerAdded, boost::signals2::signal<void(Event *)>());
    this->_globalSignals.emplace(EventType::WorkerIdle, boost::signals2::signal<void(Event *)>());

    this->listen(0, EventType::Clock, std::bind(&Engine::event2, this, std::placeholders::_1));
    this->listen(0, EventType::Kill, std::bind(&Engine::event2, this, std::placeholders::_1));
    this->listen(0, EventType::Status, std::bind(&Engine::event2, this, std::placeholders::_1));

    this->listen(0, EventType::ApplicationSubmitted, std::bind(&Engine::event2, this, std::placeholders::_1));
}

void Engine::event2(Event *ev)
{
    switch (ev->type()) {
    case EventType::ApplicationSubmitted:
        this->application(static_cast<ApplicationSubmittedEvent *>(ev)->app());
        break;
    case EventType::Clock:
        baseTime += Duration(10ms);
        engine->event(new ClockEvent(baseTime));
        break;
    case EventType::Status:
        engine->event(new StatusEvent(baseTime + Duration(1000ms)));
        break;
    case EventType::Kill:
        engine->event(new KillEvent());
        break;
    default:
        break;
    }
}

void Engine::listen(EventType type, std::function<void(Event *)> func)
{
    this->listen(99, type, func);
}

void Engine::listen(int priority, EventType type, std::function<void(Event *)> func)
{
    try {
        this->_globalSignals.at(type).connect(priority, func);
    } catch (const std::exception &e) {
        std::exception_ptr p = std::current_exception();
        logError << "Cannot listen to unknown global event " << type << " (" << e.what() << ")";
    }
}

void Engine::listen(AppId app, EventType type, std::function<void(Event *)> func)
{
    this->listen(app, 99, type, func);
}

void Engine::listen(AppId app, int priority, EventType type, std::function<void(Event *)> func)
{
    try {
        this->_appSignals.at(app).at(type).connect(priority, func);
    } catch (const std::exception &e) {
        std::exception_ptr p = std::current_exception();
        logError << "Cannot listen to unknown app " << app << " event " << type << " (" << e.what() << ")";
    }
}

void Engine::signal(Event *ev)
{
    if (!ev)
        return;

    if (ev->app() == NULL) {
        try {
            this->_globalSignals.at(ev->type())(ev);
            delete ev;
        } catch (const std::exception &e) {
            std::exception_ptr p = std::current_exception();
            logError << "Cannot signal unknown event type " << ev->type() << " (" << e.what() << ")";
            delete ev;
        }
    } else {
        try {
            this->_appSignals.at(ev->app()->id()).at(ev->type())(ev);
            delete ev;
        } catch (const std::exception &e) {
            std::exception_ptr p = std::current_exception();
            logError << "Cannot signal app " << ev->app()->id() << " event " << ev->type()
                     << " : Unknown app id or event type (" << e.what() << ")";
            delete ev;
        }
    }
}

Time Engine::now() const
{
    return Time::now();
}

void Engine::event(Event *ev)
{
    assert(ev != NULL);

    logInfo(3) << "Registering event " << (*ev) << " for " << ev->time() << " (now=" << Time::now() << ")";
    auto lock = std::unique_lock<std::mutex>(this->eventsMutex);

    this->events.emplace(ev);
    this->eventsSignal.notify_all();
}

void Engine::run(Engine *engine)
{
    assert(engine != NULL);
    engine->run();
}

void Engine::run()
{
    bool run = true;

    logCInfo(LC_EVENT) << "Starting event engine...";
    // Start clock
    this->event(new ClockEvent(baseTime));
    this->event(new StatusEvent(baseTime));

    while (run) {
        auto lock    = std::unique_lock<std::mutex>(this->eventsMutex);
        Time timeout = this->events.empty() ? Time::max() : this->events.top()->time();

        logCInfo(LC_EVENT) << "Waiting for events until " << timeout << "...";

        this->eventsSignal.wait_until(lock, timeout, [&timeout, this] {
            if (this->events.empty()) {
                logCDebug(LC_EVENT) << "Queue is empty -> sleep";
                return false;
            }

            // timeout is longer than this event so we need to wake up to reset the timeout (or
            // actually do the work) -> wake up
            if (this->events.top()->time() < timeout) {
                logCDebug(LC_EVENT) << "Event " << *this->events.top() << " time : " << this->events.top()->time() << " < " << timeout
                                   << " -> wake up";
                return true;
            }

            // time for this event has come -> wake up
	    Time now = Time::now();
            if (this->events.top()->time() > now) {
                logCDebug(LC_EVENT) << "Event " << *this->events.top() << " time : " << this->events.top()->time() << " > " << now
                                   << " -> sleep";
                return false;
            }

	    logCDebug(LC_EVENT) << "Event " << *this->events.top() << " time : " << this->events.top()->time() << " <= " << now
			       << " -> wake up";
            return true;
        });

        Event *ev = this->events.top();

        if (this->events.top()->time() <= Time::now()) {
            this->events.pop();
            lock.unlock();

            logCDebug(LC_EVENT) << "Executing event " << *ev;
            if (ev->type() == EventType::Kill) {
                logCInfo(LC_EVENT) << "Shutting down event engine...";
                run = false;
            }
            this->signal(ev);
        } else {
            logCWarn(LC_EVENT) << "Event " << *ev << " at " << ev->time() << " (too early)";
	    // event is re-considered in the next loop iteration. So nothing has to be done here.
        }
    }

    logCInfo(LC_EVENT) << "Stopped event engine.";
    //    std::terminate();
}

void Engine::application(App *app)
{
    assert(app != NULL);
    logInfo(1) << "Adding application " << app->id();
    auto guard = std::unique_lock<std::mutex>(this->_appsMutex);

    this->_apps.emplace(app->id(), app);

    logInfo(1) << "All known applications ";

    for (auto app : this->_apps) {
        logInfo(1) << " - " << app.first << " / " << app.second->id();
    }

    AppId id = app->id();

    this->_appSignals[id].emplace(EventType::TaskScheduled, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::TaskStarted, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::TaskTimeout, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::TaskFinished, boost::signals2::signal<void(Event *)>());

    this->_appSignals[id].emplace(EventType::StageAdded, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::StageReady, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::StageScheduled, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::StageStarted, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::StageFinished, boost::signals2::signal<void(Event *)>());

    this->_appSignals[id].emplace(EventType::ExecutorAdded, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::ExecutorRemoved, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::ExecutorDisabled, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::ExecutorEnabled, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::ExecutorDisconnect, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::ExecutorDisconnectLocallyDone,
                                  boost::signals2::signal<void(Event *)>());

    this->_appSignals[id].emplace(EventType::ApplicationStarted, boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::ApplicationFinished, boost::signals2::signal<void(Event *)>());

    this->_appSignals[id].emplace(EventType::ApplicationDemandsChanged,
                                  boost::signals2::signal<void(Event *)>());
    this->_appSignals[id].emplace(EventType::WorkerAllocationUpdate, boost::signals2::signal<void(Event *)>());
}

App *Engine::application(AppId id)
{
    auto guard = std::unique_lock<std::mutex>(this->_appsMutex);

    logInfo(1) << "Looking for application " << id;
    for (auto app : this->_apps) {
        logInfo(1) << " - " << app.first << " / " << app.second->id();
    }

    auto app = this->_apps.find(id);
    if (app != this->_apps.end())
        return app->second;
    else
        return NULL;
}

std::ostream &operator<<(std::ostream &os, const EngineMode &obj)
{
    switch (obj) {
    case EngineMode::Simulation:
        os << "simulation";
        break;
    case EngineMode::Real:
        os << "real";
        break;
    default:
        os << "unknown";
        break;
    }
    return os;
}
