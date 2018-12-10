// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "resourcemanager/worker.h"
#include "app/app.h"
#include "resourcemanager/executor.h"
#include "resourcemanager/resource.h"

#include "helper/logger.h"

Worker::Worker(Node *node, uint port, uint cores, size_t memory) : _usageCounter(0), _executor(this, cores)
{
    this->id(static_cast<WorkerId>(-1));
    this->cores(cores);
    this->memory(memory);
    this->node(node);
    this->port(port);
    this->app(NULL);

    auto nCores = cores;
    for (auto resource : node->resources(ResourceType::cpu)) {
        if (nCores == 0)
            break;
        if (!resource->executor()) {
            this->_executor.addResource(resource);
            nCores--;
        }
    }
}

std::unique_lock<std::mutex> Worker::guard()
{
    return std::unique_lock<std::mutex>(this->_mutex);
}

void Worker::lock()
{
    this->_mutex.lock();
}

void Worker::unlock()
{
    this->_mutex.unlock();
}

void Worker::id(WorkerId id)
{
    this->_id = id;
    this->_executor.id(id);
}

const App *Worker::get()
{
    Time t0    = Time::now();
    auto guard = this->guard();

    App *app = NULL;
    do {
        this->_appQueueMutexCondVar.wait(guard, [this] { return !this->_appQueue.empty(); });

        assert(this->_appQueue.empty() == false);

        app = this->_appQueue.front();
        this->_appQueue.pop();
        Time t1 = Time::now();

        if (app->state() > TaskState::running) {
            logError << "Ignoring invalid assignment for worker " << this->node()->id() << "/" << this->id()
                     << " which is " << app->id() << " (" << app->state() << ")";
            app = NULL;
            continue;
        }

        if (Duration(t1 - t0) > Duration(std::chrono::microseconds(1)))
            logCDebug(LC_RM | LC_SCHED)
                << "Worker " << this->node()->id() << "/" << this->id() << " was idle for " << (t1 - t0);
        this->app(app);
    } while (!app);

    return app;
}

void Worker::add(App *app)
{
    auto guard = this->guard();

    this->_appQueue.push(app);
    this->_appQueueMutexCondVar.notify_all();
}

void Worker::incUsageCounter()
{
    this->_usageCounter++;
}

uint Worker::getUsageCounter() const
{
    return this->_usageCounter;
}
