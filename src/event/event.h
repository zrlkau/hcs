// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//          Adrian Schuepbach <dri@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef event_h
#define event_h

#include "helper/classes.h"
#include "helper/macros.h"
#include "helper/time.h"
#include "helper/types.h"

#include <list>
#include <map>

enum class EventType {
    Kill,
    Clock,
    Status,
    //
    TaskScheduled,
    TaskStarted,
    TaskTimeout,
    TaskFinished,
    //
    StageAdded,
    StageReady,
    StageScheduled,
    StageStarted,
    StageFinished,
    //
    ApplicationSubmitted,
    ApplicationStarted,
    ApplicationFinished,
    ApplicationDemandsChanged,
    //
    ExecutorAdded,
    ExecutorRemoved,
    ExecutorDisabled,
    ExecutorEnabled,
    ExecutorDisconnect,
    ExecutorDisconnectLocallyDone,
    //
    WorkerAdded,
    WorkerIdle,
    WorkerAllocationUpdate,
    //
    Nop
};

std::ostream &operator<<(std::ostream &os, const EventType &obj);

class Event
{
  public:
    friend class Engine;
    Event(EventType type, App *app, Time time, int priority = 0)
        : _type(type), _time(time), _priority(priority), _app(app)
    {
    }

    virtual ~Event()
    {
    }

    virtual Event *       copy() const                  = 0;
    virtual std::ostream &print(std::ostream &os) const = 0;

    GET(EventType, type);
    GET(Time, time);
    GET(int, priority);
    GET(App *, app);

    inline bool operator<(const Event &rhs) const
    {
        if (rhs.time() == this->time()) {
            if (rhs.priority() == this->priority()) {
                return rhs.type() < this->type();
            } else
                return rhs.priority() < this->priority();
        } else
            return rhs.time() < this->time();
    }
    inline bool operator>(const Event &rhs) const
    {
        return rhs < *this;
    }
    inline bool operator==(const Event &rhs) const
    {
        return this->time() == rhs.time();
    }
    inline bool operator!=(const Event &rhs) const
    {
        return !(*this == rhs);
    }

  protected:
    SET(EventType, type);
    SET(Time, time);
    SET(int, priority);
    SET(App *, app);

  private:
    EventType _type;
    Time      _time;
    int       _priority;
    App *     _app;
};

std::ostream &operator<<(std::ostream &os, const Event &obj);

/****************************************************************************************************
 *                                             Task Events                                          *
 ****************************************************************************************************/
class TaskScheduledEvent : public Event
{
  public:
    TaskScheduledEvent(App *app, Task *task, Executor *executor, Time time = Time::now())
        : Event(EventType::TaskScheduled, app, time), _task(task), _executor(executor)
    {
    }

    std::ostream &print(std::ostream &os) const;
    Event *       copy() const;

    GET(Task *, task);
    GET(Executor *, executor);

  protected:
    SET(Task *, task);
    SET(Executor *, executor);

  private:
    Task *    _task;
    Executor *_executor;
};

class TaskStartedEvent : public Event
{
  public:
    TaskStartedEvent(App *app, Task *task, Executor *executor, Time time = Time::now())
        : Event(EventType::TaskStarted, app, time), _task(task), _executor(executor)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Task *, task);
    GET(Executor *, executor);

  protected:
    SET(Task *, task);
    SET(Executor *, executor);

  private:
    Task *    _task;
    Executor *_executor;
};

class TaskFinishedEvent : public Event
{
  public:
    TaskFinishedEvent(App *app, Task *task, Executor *executor, Time time = Time::now())
        : Event(EventType::TaskFinished, app, time), _task(task), _executor(executor)
    {
    }

    //    void          exec();
    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Task *, task);
    GET(Executor *, executor);

  protected:
    SET(Task *, task);
    SET(Executor *, executor);

  private:
    Task *    _task;
    Executor *_executor;
};

class TaskTimeoutEvent : public Event
{
  public:
    TaskTimeoutEvent(App *app, Task *task, Executor *executor, Time time = Time::now())
        : Event(EventType::TaskTimeout, app, time), _task(task), _executor(executor)
    {
    }

    //    void          exec();
    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Task *, task);
    GET(Executor *, executor);

  protected:
    SET(Task *, task);
    SET(Executor *, executor);

  private:
    Task *    _task;
    Executor *_executor;
};

/****************************************************************************************************
 *                                            Stage Events                                          *
 ****************************************************************************************************/
class StageAddedEvent : public Event
{
  public:
    StageAddedEvent(App *app, std::map<Stage *, std::list<StageId>> stages, Time time = Time::now())
        : Event(EventType::StageAdded, app, time), _stages(stages)
    {
    }

    //    void          exec();
    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(std::map<Stage * COMMA std::list<StageId>> &, stages);

  protected:
    SET(std::map<Stage * COMMA std::list<StageId>>, stages);

  private:
    std::map<Stage *, std::list<StageId>> _stages;
};

class StageReadyEvent : public Event
{
  public:
    StageReadyEvent(App *app, Stage *stage, Time time = Time::now())
        : Event(EventType::StageReady, app, time), _stage(stage)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Stage *, stage);

  protected:
    SET(Stage *, stage);

  private:
    Stage *_stage;
};

class StageScheduledEvent : public Event
{
  public:
    StageScheduledEvent(App *app, Stage *stage, Time time = Time::now())
        : Event(EventType::StageScheduled, app, time), _stage(stage)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Stage *, stage);

  protected:
    SET(Stage *, stage);

  private:
    Stage *_stage;
};

class StageStartedEvent : public Event
{
  public:
    StageStartedEvent(App *app, Stage *stage, Time time = Time::now())
        : Event(EventType::StageStarted, app, time), _stage(stage)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Stage *, stage);

  protected:
    SET(Stage *, stage);

  private:
    Stage *_stage;
};

class StageFinishedEvent : public Event
{
  public:
    StageFinishedEvent(App *app, Stage *stage, Time time = Time::now())
        : Event(EventType::StageFinished, app, time), _stage(stage)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Stage *, stage);

  protected:
    SET(Stage *, stage);

  private:
    Stage *_stage;
};

/****************************************************************************************************
 *                                           Application Events                                     *
 ****************************************************************************************************/
class ApplicationSubmittedEvent : public Event
{
  public:
    ApplicationSubmittedEvent(App *app, Time time = Time::now())
        : Event(EventType::ApplicationSubmitted, NULL, time), _app(app)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(App *, app);

  protected:
    SET(App *, app);

  private:
    // IMPORTANT: This is not the same _app pointer as in the Event base class (it's not virtual!)! This is on purpose
    // because this event is a global event (i.e. Event::app == NULL) but at the same time the
    // resource manager, who listenes to this event needs to know the app.
    App *_app;
};

class ApplicationStartedEvent : public Event
{
  public:
    ApplicationStartedEvent(App *app, Time time = Time::now()) : Event(EventType::ApplicationStarted, app, time)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

  protected:
  private:
};

class ApplicationFinishedEvent : public Event
{
  public:
    ApplicationFinishedEvent(App *app, Time time = Time::now())
        : Event(EventType::ApplicationFinished, app, time)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

  protected:
  private:
};

/****************************************************************************************************
 *                                             Misc Events                                          *
 ****************************************************************************************************/
class KillEvent : public Event
{
  public:
    KillEvent(Time time = Time::now()) : Event(EventType::Kill, NULL, time)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

  protected:
  private:
};

class ClockEvent : public Event
{
  public:
    ClockEvent(Time time = Time::now()) : Event(EventType::Clock, NULL, time)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

  protected:
  private:
};

class StatusEvent : public Event
{
  public:
    StatusEvent(Time time = Time::now()) : Event(EventType::Status, NULL, time)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

  protected:
  private:
};

/****************************************************************************************************
 *                                          Resource Events                                         *
 ****************************************************************************************************/
class ExecutorAddedEvent : public Event
{
  public:
    ExecutorAddedEvent(App *app, Executor *executor, Time time = Time::now())
        : Event(EventType::ExecutorAdded, app, time), _executor(executor)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Executor *, executor);

  protected:
  private:
    SET(Executor *, executor);

    Executor *_executor;
};

class ExecutorRemovedEvent : public Event
{
  public:
    ExecutorRemovedEvent(App *app, Executor *executor, Time time = Time::now())
        : Event(EventType::ExecutorRemoved, app, time), _executor(executor)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Executor *, executor);

  protected:
  private:
    SET(Executor *, executor);

    Executor *_executor;
};

class ExecutorDisabledEvent : public Event
{
  public:
    ExecutorDisabledEvent(App *app, Executor *executor, Time time = Time::now())
        : Event(EventType::ExecutorDisabled, app, time), _executor(executor)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Executor *, executor);

  protected:
  private:
    SET(Executor *, executor);

    Executor *_executor;
};

class ExecutorEnabledEvent : public Event
{
  public:
    ExecutorEnabledEvent(App *app, Executor *executor, Time time = Time::now())
        : Event(EventType::ExecutorEnabled, app, time), _executor(executor)
    {
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Executor *, executor);

  protected:
  private:
    SET(Executor *, executor);

    Executor *_executor;
};

class ExecutorDisconnectEvent : public Event
{
  public:
    ExecutorDisconnectEvent(App *app, Executor *executor, Time time = Time::now())
        : Event(EventType::ExecutorDisconnect, app, time)
    {
        this->executor(executor);
    }

    //    void          exec();
    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Executor *, executor);

  protected:
  private:
    SET(Executor *, executor);

    Executor *_executor;
};

class ExecutorDisconnectLocallyDoneEvent : public Event
{
  public:
    ExecutorDisconnectLocallyDoneEvent(App *app, Executor *executor, Time time = Time::now())
        : Event(EventType::ExecutorDisconnectLocallyDone, app, time)
    {
        this->executor(executor);
    }

    //    void          exec();
    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Executor *, executor);

  protected:
  private:
    SET(Executor *, executor);

    Executor *_executor;
};

class NopEvent : public Event
{
  public:
    NopEvent(App *app, Executor *executor = NULL, Time time = Time::now()) : Event(EventType::Nop, app, time)
    {
        this->executor(executor);
    }

    //    void          exec();
    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Executor *, executor);

  protected:
  private:
    SET(Executor *, executor);

    Executor *_executor;
};

class ApplicationDemandsChangedEvent : public Event
{
  public:
    ApplicationDemandsChangedEvent(App *app, size_t demand, Time time = Time::now())
        : Event(EventType::ApplicationDemandsChanged, app, time)
    {
        this->demand(demand);
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(size_t, demand);

  protected:
  private:
    SET(size_t, demand);

    size_t _demand;
};

class WorkerAddedEvent : public Event
{
  public:
    WorkerAddedEvent(Worker *worker, Time time = Time::now()) : Event(EventType::WorkerAdded, NULL, time)
    {
        this->worker(worker);
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Worker *, worker);

  protected:
  private:
    SET(Worker *, worker);

    Worker *_worker;
};

class WorkerIdleEvent : public Event
{
  public:
    WorkerIdleEvent(Worker *worker, Time time = Time::now()) : Event(EventType::WorkerIdle, NULL, time)
    {
        this->worker(worker);
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(Worker *, worker);

  protected:
  private:
    SET(Worker *, worker);

    Worker *_worker;
};

class WorkerAllocationUpdateEvent : public Event
{
  public:
    WorkerAllocationUpdateEvent(App *app, size_t target, Time time = Time::now())
        : Event(EventType::WorkerAllocationUpdate, app, time)
    {
        this->target(target);
    }

    Event *       copy() const;
    std::ostream &print(std::ostream &os) const;

    GET(size_t, target);

  protected:
  private:
    SET(size_t, target);

    size_t _target;
};

#endif
