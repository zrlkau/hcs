// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef oracle_h
#define oracle_h

#include "common.h"

#include <boost/functional/hash.hpp>
#include <chrono>
#include <list>
#include <map>
#include <string>
#include <unordered_map>

#ifdef USE_CATBOOST
#include <catboost/wrapped_calcer.h>
#endif

#define EXECOST(rkey, inputOnExecutor, inputOnNode, inputOnRemote, nodeIoLoad, first)                        \
    ({                                                                                                       \
        std::vector<float> feats = {static_cast<float>(rkey),                                                \
                                    static_cast<float>(first),                                               \
                                    static_cast<float>(0),                                                   \
                                    static_cast<float>((inputOnExecutor) + (inputOnNode) + (inputOnRemote)), \
                                    static_cast<float>(inputOnExecutor),                                     \
                                    static_cast<float>(inputOnNode),                                         \
                                    static_cast<float>(inputOnRemote),                                       \
                                    static_cast<float>(nodeIoLoad)};                                         \
        oc.execost(feats);                                                                                   \
    })

namespace std {
template <> struct hash<std::pair<ResourceKey, size_t>> {
    size_t operator()(const std::pair<ResourceKey, size_t> &obj) const
    {
        return (static_cast<size_t>(obj.first) + obj.second);
    }
};

template <> struct hash<std::vector<float>> {
    size_t operator()(const std::vector<float> &obj) const
    {
        std::size_t seed = 0;

        for (size_t i = 0; i < obj.size(); i++)
            boost::hash_combine(seed, obj[i]);

        return seed;
    }
};
} // namespace std

class OracleCache
{
  public:
    OracleCache(Oracle *oracle, StageKey skey);
    Duration execost(const std::vector<float> &feats);

  private:
    std::unordered_map<size_t, size_t /* runtime in us*/> _cache;

    Oracle * _oracle;
    StageKey _skey;
};

class OracleRecord
{
  public:
    OracleRecord(Time        from,
                 Time        to,
                 Node *      node,
                 StageKey    stageKey,
                 ResourceKey resourceKey,
                 size_t      inputOnExecutor,
                 size_t      inputOnNode,
                 size_t      inputOnRemote,
                 double      ioLoad,
                 bool        firstOnExecutorOfStage,
                 bool        firstOnExecutor)
        : _from(from), _to(to), _node(node), _stageKey(stageKey), _resourceKey(resourceKey),
          _inputOnExecutor(inputOnExecutor), _inputOnNode(inputOnNode), _inputOnRemote(inputOnRemote),
          _ioLoad(ioLoad), _firstOnExecutorOfStage(firstOnExecutorOfStage), _firstOnExecutor(firstOnExecutor)
    {
    }

    GET(Time, from);
    GET(Time, to);
    GET(Node *, node);
    GET(StageKey, stageKey);
    GET(ResourceKey, resourceKey);
    GET(size_t, inputOnExecutor);
    GET(size_t, inputOnNode);
    GET(size_t, inputOnRemote);
    GET(double, ioLoad);
    GET(bool, firstOnExecutorOfStage);
    GET(bool, firstOnExecutor);

  private:
    Time        _from;
    Time        _to;
    Node *      _node;
    StageKey    _stageKey;
    ResourceKey _resourceKey;
    size_t      _inputOnExecutor;
    size_t      _inputOnNode;
    size_t      _inputOnRemote;
    double      _ioLoad;
    bool        _firstOnExecutorOfStage;
    bool        _firstOnExecutor;
};

class Oracle
{
  public:
    Oracle(App *app);

    void load();
    void store();

    void event(Event *ev);

    void update(Task *task);   // update task oracle data
    void preset(Stage *stage); // pre-initialize with mean io data

    uint64_t execost(StageKey skey, const std::vector<std::string> &cat, const std::vector<float> &cont);

    static std::list<std::list<ResourceType>> constraints(Stage *stage);

    GET(int, numResources);
    GET(int, numFeatures);

  protected:
    SETGET(std::string, appdbPath);
    SETGET(App *, app);

    SET(int, numResources);
    SET(int, numFeatures);

  private:
#ifdef USE_CATBOOST
    std::map<StageKey, ModelCalcerWrapper> _models;
#endif
    int                                    _numResources;
    int                                    _numFeatures;

    std::map<StageKey, size_t>              _presets;   // so far only mean inData per Stage
    std::map<StageKey, std::vector<size_t>> _ioPresets; // preset data for I/O stages

    std::list<OracleRecord>                   _perfRecords;
    std::unordered_map<StageKey, std::string> _keyToFunction;

    std::string _appdbPath;
    App *       _app;
};
#endif
