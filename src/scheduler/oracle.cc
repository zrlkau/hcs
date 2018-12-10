// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "scheduler/oracle.h"
#include "app/app.h"
#include "app/io.h"
#include "app/task.h"
#include "cluster/cluster.h"
#include "cluster/node.h"
#include "common.h"
#include "event/engine.h"
#include "resourcemanager/resourcemanager.h"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <regex>
#include <stdexcept>

#include <boost/property_tree/json_parser.hpp>
namespace pt = boost::property_tree;

OracleCache::OracleCache(Oracle *oracle, StageKey skey) : _oracle(oracle), _skey(skey)
{
}

Duration OracleCache::execost(const std::vector<float> &feats)
{
    assert(feats.size() == 8);
    // std::ostringstream os;
    // os << feats;
    // logInfo(0) << "Asking oracle for " << os.str();
    const size_t hash  = std::hash<std::vector<float>>{}(feats);
    auto         entry = this->_cache.find(hash);

    if (entry != this->_cache.end()) {
        return entry->second;
    } else {
        std::vector<std::string> cat(3);
        std::vector<float>       cont(5);
        cat[0]  = std::to_string(static_cast<int>(feats[0])); // resource key
        cat[1]  = std::to_string(static_cast<int>(feats[1])); // first task of stage on executor?
        cat[2]  = std::to_string(static_cast<int>(feats[2])); // first task on executor at all?
        cont[0] = feats[3];                                   // total input
        cont[1] = feats[4];                                   // input on executor
        cont[2] = feats[5];                                   // input on node
        cont[3] = feats[6];                                   // input on remote
        cont[4] = feats[7];                                   // node ip load
        return this->_cache[hash] = this->_oracle->execost(this->_skey, cat, cont);
    }
}

Oracle::Oracle(App *app)
{
    this->app(app);
    this->numFeatures(0);
    this->numResources(0);

    this->load();

    engine->listen(app->id(),
                   0,
                   EventType::ApplicationFinished,
                   std::bind(&Oracle::event, this, std::placeholders::_1));
}

void Oracle::event(Event *ev)
{
    switch (ev->type()) {
    case EventType::TaskFinished: {
        auto _ev = static_cast<TaskFinishedEvent *>(ev);
        this->update(_ev->task());
    } break;
    case EventType::ApplicationFinished: {
        auto _ev = static_cast<ApplicationFinishedEvent *>(ev);
        //        if (_ev->app() == this->app() && config->getFlag(ConfigSchedulerFlags::training_mode))
        this->store();
    } break;
    default:
        break;
    }
}

void Oracle::load()
{
#ifdef USE_CATBOOST
    try {
        std::string   modelsFileName = config->perfdbdir() + "/" + this->app()->name() + "-models.json";
        std::ifstream modelsFile(modelsFileName, std::ios::in);
        if (modelsFile.is_open()) {
            ptree modelsData;
            read_json(modelsFile, modelsData);
            modelsFile.close();
            this->numResources(modelsData.get<int>("numResources", 0));
            this->numFeatures(modelsData.get<int>("numFeatures", 0));

            logInfo(0) << "oracle=" << this << " numResource=" << this->numResources()
                       << " numFeatures=" << this->numFeatures();

            for (const auto &entry : modelsData.get_child("models")) {
                StageKey    skey  = static_cast<StageKey>(std::stoull(entry.first.data()));
                std::string fname = config->perfdbdir() + "/" + entry.second.data();

                this->_models[skey] = ModelCalcerWrapper(fname);

                logInfo(0) << "Loaded model for stage key " << skey << " from " << fname;
            }
        }
    } catch (std::exception const &e) {
        logError << "Error while opening models file or loading models: " << e.what();
        return;
    }
#else
    logWarn << "Not loading ML models because no ML library was enabled during compilation.";
#endif
    
    try {
        std::string   ioFileName = config->perfdbdir() + "/" + this->app()->name() + "-io.json";
        std::ifstream ioFile(ioFileName, std::ios::in);

        if (ioFile.is_open()) {
            ptree ioData;
            read_json(ioFile, ioData);
            ioFile.close();

            for (const auto &entry : ioData.get_child("")) {
                StageKey skey = static_cast<StageKey>(std::stoi(entry.first.data()));
                //                logInfo(0) << "Loading I/O data for stage key " << skey << " with " << entry.second.size() << " tasks.";
                this->_ioPresets.emplace(skey, std::vector<size_t>(entry.second.size()));
                for (const auto &entry2 : entry.second) {
                    TaskIdx tidx = static_cast<TaskIdx>(std::stoi(entry2.first.data()));
                    //                    logInfo(0) << "Loading I/O data for task " << tidx << " -> " << entry2.second.data();
                    size_t inData                = std::stoul(entry2.second.data());
                    this->_ioPresets[skey][tidx] = inData;
                }
            }
        }
    } catch (std::exception const &e) {
        logError << "Error while loading I/O information: " << e.what();
        return;
    }
}

void Oracle::store()
{
    try {
        std::string   perfRecordsFileName = config->perfdbdir() + "/" + this->app()->name() + "-perf.csv";
        std::ofstream perfRecordsFile(perfRecordsFileName, std::ios::out | std::ios::app);
        for (auto entry : this->_perfRecords) {
            size_t   totalInput = entry.inputOnExecutor() + entry.inputOnNode() + entry.inputOnRemote();
            double   nodeIoLoad = std::max(0.0,
                                         entry.node()->ioLoad().getLoad(entry.from(), entry.to()) -
                                             entry.ioLoad());
            uint64_t runtime    = (entry.to() - entry.from()).count();
            perfRecordsFile << runtime << "," << entry.stageKey() << "," << entry.resourceKey() << ","
                            << totalInput << "," << entry.inputOnExecutor() << "," << entry.inputOnNode() << ","
                            << entry.inputOnRemote() << "," << nodeIoLoad << ","
                            << entry.firstOnExecutorOfStage() << "," << entry.firstOnExecutor() << std::endl;
        }
        perfRecordsFile.close();

        logInfo(0) << "Stored " << this->_perfRecords.size() << " performance records for application "
                   << this->app()->name();
    } catch (std::exception const &e) {
        logError << "Error while storing performance records for application " << this->app()->name() << " : "
                 << e.what();
        return;
    }

    try {
        std::string   ioFileName = config->perfdbdir() + "/" + this->app()->name() + "-io.json";
        std::ofstream ioFile(ioFileName, std::ios::out | std::ios::trunc);

        ptree ioData;

        for (auto &entry : this->app()->stages()) {
            Stage *stage = entry.second;
            if (stage->type() != TaskType::load)
                continue;

            ptree stageData;
            for (size_t tidx = 0; tidx < stage->size(); tidx++) {
                Task *task = stage->task(tidx);
                stageData.put(std::to_string(tidx), task->metrics().inData());
            }
            ioData.add_child(std::to_string(stage->key()), stageData);
        }

        write_json(ioFile, ioData);
        ioFile.close();
    } catch (std::exception const &e) {
        logError << "Error while storing I/O information: " << e.what();
        return;
    }
}

void Oracle::update(Task *task)
{
    assert(task != NULL);
    Duration  runtime  = task->metrics().runtime();
    Executor *executor = task->allocation()->executor();
    Node *    node     = executor->node();

    size_t executorInData = task->metrics().inData(executor);
    size_t nodeInData     = task->metrics().inData(node) - executorInData;
    size_t remoteInData   = task->metrics().inData() - nodeInData - executorInData;

    // Determine actual I/O load and add it to the node's io load history. This is used later to
    // determine what the I/O load was when each task was executed.
    node->ioLoad().addLoad(task->metrics().started(), task->metrics().finished(), task->metrics().ioLoad());
    double nodeIoLoad = node->ioLoad().getLoad(task->metrics().started(), task->metrics().finished());

    logInfo(1) << "task=" << task->id() << " stage key=" << task->stage()->key()
//               << " resource key=" << task->allocation()->resource()->key()
               << " start=" << task->metrics().started() << " finish=" << task->metrics().finished()
               << " runtime=" << runtime.count() // << " resource=" << resource->id()
               << " executor=" << executor->id() << " executorInData=" << executorInData
               << " nodeInData=" << nodeInData << " remoteInData=" << remoteInData
               << " nodeIoLoad=" << nodeIoLoad << " taskIoLoad=" << task->metrics().ioLoad()
               << " first=" << task->allocation()->firstOnExecutorOfStage() << "/"
               << task->allocation()->firstOnExecutor();

    this->_perfRecords.emplace_back(task->metrics().started(),
                                    task->metrics().finished(),
                                    node,
                                    task->stage()->key(),
                                    static_cast<ResourceKey>(0), //task->allocation()->resource()->key(),
                                    executorInData,
                                    nodeInData,
                                    remoteInData, // TODO: Split up in remote locations per node class
                                    task->metrics().ioLoad(),
                                    task->allocation()->firstOnExecutorOfStage(),
                                    task->allocation()->firstOnExecutor());
}

void Oracle::preset(Stage *stage)
{
    try {
        // This function sets some values that we know but haven't gotten yet (i.e. total input data for tasks)
        if (stage->type() == TaskType::load) {
            StageKey skey = stage->key();
            if (this->_ioPresets.find(skey) != this->_ioPresets.end()) {
                for (size_t tidx = 0; tidx < stage->size(); tidx++) {
                    Task *task = stage->task(tidx);
                    task->metrics().inData(this->_ioPresets.at(skey).at(tidx), true);
                }
            } else {
                logWarn << "Cannot apply input presets to stage " << stage->id() << " because it's unknown.";
            }
        } else {
            if (this->_presets.find(stage->key()) != this->_presets.end()) {
                size_t meanInData = this->_presets.at(stage->key());
                logDebug << " Presetting input data size of stage " << stage->id() << " (" << stage->key()
                         << ") to " << meanInData;

                stage->metrics().meanInData(meanInData);

                for (Task *task : stage->tasks()) {
                    task->metrics().inData(meanInData);
                }
            } else {
                logWarn << "Cannot apply input presets to stage " << stage->id() << " because it's unknown.";
            }
        }
    } catch (std::exception const &e) {
        logError << "Error while applying stage input presets for stage " << stage->id() << " (key "
                 << stage->key() << ") : " << e.what();
        return;
    }
}

uint64_t Oracle::execost(StageKey skey, const std::vector<std::string> &cat, const std::vector<float> &cont)
{
    //    std::ostringstream os;
    //    os << cat << " + " << cont;
    //    logInfo(0) << "Question for skey=" << skey << " is " << os.str() << " numFeatures=" << this->_numFeatures; //<< cat << " + " << cont;

    if (this->_numFeatures == 0)
        return Duration(1ms).count(); // + cat[1]*Duration(1ms).count() + cat[2]*Duration(5ms);

#ifdef USE_CATBOOST
    auto & model = this->_models.at(skey);
    double pred  = model.Calc(cont, cat);

    uint64_t runtime = std::max(Duration(1ms).count(), static_cast<int64_t>(pred));
    return runtime;
#else
    return Duration(1ms).count(); // + cat[1]*Duration(1ms).count() + cat[2]*Duration(5ms);
#endif
}

/*
  Returns a list of alternative resources. Each list item is a
  list of required resources, e.g. {{8 CPU, 8 GB Mem}, {1 GPU, 1
  CPU, 1 GB Mem}}.
*/
std::list<std::list<ResourceType>> Oracle::constraints(Stage *stage)
{
    std::list<std::list<ResourceType>> res;

    switch (stage->type()) {
    case TaskType::compute:
        stage->restype(ResourceType::cpu);
        break;
    case TaskType::cuda:
        stage->restype(ResourceType::cpu);
        stage->restype(ResourceType::gpu);
        break;
    case TaskType::opencl:
        stage->restype(ResourceType::cpu);
        stage->restype(ResourceType::gpu);
        break;
    case TaskType::openmp:
        stage->restype(ResourceType::cpu);
        break;
    case TaskType::mpi:
        stage->restype(ResourceType::cpu);
        break;
    case TaskType::fpga:
        stage->restype(ResourceType::fpga);
        break;
    case TaskType::send:
        stage->restype(ResourceType::network);
        stage->restype(ResourceType::rdma);
        break;
    case TaskType::receive:
        stage->restype(ResourceType::network);
        stage->restype(ResourceType::rdma);
        break;
    case TaskType::load:
        stage->restype(ResourceType::storage);
        break;
    case TaskType::store:
        stage->restype(ResourceType::storage);
        break;
    default:
        break;
    }

    for (auto &type : stage->restype()) {
        std::list<ResourceType> l;
        l.push_back(type);
        res.push_back(l);
    }

    return res;
}
