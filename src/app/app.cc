// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include <climits>
#include <fstream>
#include <map>
#include <random>
#include <set>
#include <thread>
#include <unordered_map>

#include "app/app.h"
#include "common.h"
#include "event/engine.h"

#include <boost/property_tree/json_parser.hpp>

using boost::property_tree::ptree;

App::App(AppId id, std::string name, std::string driverUrl)
{
    this->state(TaskState::unknown);
    this->_metrics.submitted(Time::now());

    this->id(id);
    this->name(name);
    this->driverUrl(driverUrl);
    this->load();
}

App::~App()
{
}

std::unique_lock<Mutex> __attribute__((warn_unused_result)) App::guard(int id)
{
    logCInfo(LC_LOCK | LC_APP) << "About to acquire app " << this->id() << " guard " << id;
    auto lock = std::unique_lock<Mutex>(this->_mutex, std::defer_lock);
    while (!lock.try_lock_for(10s)) {
        logWarn << "Unable to acquire guard " << id << " for application " << this->id()
                << " within the last 10s";
    }
    logCInfo(LC_LOCK | LC_APP) << "Acquired app " << this->id() << " guard " << id;
    return lock;
}

void App::lock(int id)
{
    logCDebug(LC_LOCK | LC_APP) << "About to acquire app " << this->id() << " lock " << id;
    while (!this->_mutex.try_lock_for(10s)) {
        logWarn << "Unable to acquire lock " << id << " for application " << this->id()
                << " within the last 10s";
    }
    logCDebug(LC_LOCK | LC_APP) << "Acquired app " << this->id() << " lock " << id;
}

void App::unlock(int id)
{
    this->_mutex.unlock();
    logCDebug(LC_LOCK | LC_APP) << "Released app " << this->id() << " lock " << id;
}

void App::addStages(std::map<Stage *, std::list<StageId>> stages)
{
    try {
        // 1. Add stages
        for (auto &entry : stages) {
            Stage *stage = entry.first;
            stage->scheduler(this->scheduler());
            // Map the raw key into out continuous key space
            stage->key(this->stageKeyMapper().add(stage->key()));

            this->_stages.emplace(stage->nid(), stage);
            //        logInfo(0) << "Added stage " << stage->id() << " (key=" << stage->key() << ")";
        }

        // 2. Connect stages
        for (auto &entry : stages) {
            Stage *            dst          = entry.first;
            auto &             dependencies = entry.second;
            std::ostringstream os;
            os << "{";
            for (StageId snid : dependencies) {
                Stage *src = this->_stages.at(snid);

                auto it = this->_io.emplace(std::piecewise_construct,
                                            std::forward_as_tuple(std::make_pair(src, dst)),
                                            std::forward_as_tuple(src, dst, 1));

                src->next(&it.first->second);
                dst->prev(&it.first->second);

                os << src->id() << ", ";
            }

            if (os.str().length() > 1)
                os.seekp(-2, std::ios_base::end) << "} -> ";
            else
                os << "} -> ";

            logInfo(0) << os.str() << dst->id();
        }

        // 3. Compute level of each stage (distance to output)
        for (auto &entry : stages) {
            Stage *stage = entry.first;
            if (stage->next().empty())
                stage->level(0);
        }

        // 4. Add stages to schedule
        for (auto &entry : stages) {
            Stage *stage = entry.first;
            this->_schedule.addStage(stage);
            logInfo(0) << "Added stage " << stage->id() << " (key=" << stage->key()
                       << ", level=" << stage->level() << ", type=" << stage->type() << ")";
        }
    } catch (std::out_of_range const &e) {
        std::ostringstream os;
        for (auto &entry : stages) {
            Stage *stage = entry.first;
            if (stage)
                os << stage->id() << " (";
            else
                os << "NULL"
                   << " (";

            for (StageId dep : entry.second) {
                os << dep << " ";
            }

            os << ")";
        }

        logError << "Failed to add stages " << os.str() << " : " << e.what();
    }
}

Stage *App::stage(StageId id)
{
    auto s = this->_stages.find(id);
    if (s != this->_stages.end())
        return s->second;
    else
        return NULL;
}

void App::load()
{
    try {
        std::string   skeysFileName = config->perfdbdir() + "/" + this->name() + "-skeys.json";
        std::ifstream skeysFile(skeysFileName, std::ios::in);

        if (skeysFile.is_open()) {
            ptree skeys;
            read_json(skeysFile, skeys);
            skeysFile.close();
            this->_stageKeyMapper.load(skeys);

            logInfo(0) << "Loaded " << this->_stageKeyMapper.size() << " stage key mappings from "
                       << skeysFileName;
        }
    } catch (std::exception const &e) {
        logError << "Error while loading other stuff: " << e.what();
        return;
    }
}

void App::store()
{
    try {
        std::string   skeysFileName = config->perfdbdir() + "/" + this->name() + "-skeys.json";
        std::ofstream skeysFile(skeysFileName, std::ios::out | std::ios::trunc);
        ptree         skeys = this->_stageKeyMapper.store();
        write_json(skeysFile, skeys);
        skeysFile.close();

        logInfo(0) << "Stored " << this->_stageKeyMapper.size() << " stage key mappings to " << skeysFileName;
    } catch (std::exception const &e) {
        logError << "Error while storing stage key mappings: " << e.what();
        return;
    }
}

void App::state(TaskState state)
{
    this->_state = state;

    switch (this->state()) {
    case TaskState::unknown:
        break;
    case TaskState::unready:
        break;
    case TaskState::ready:
        break;
    case TaskState::scheduled:
        break;
    case TaskState::running:
        this->_metrics.started(Time::now());
        break;
    case TaskState::finished:
        this->_metrics.finished(Time::now());
        this->_metrics.runtime(this->_metrics.finished() - this->_metrics.started());
        this->store();
        break;
    }
}

std::ostream &operator<<(std::ostream &os, const App &obj)
{
    os << obj.id() << " (" << obj.name() << ")";
    return os;
}
