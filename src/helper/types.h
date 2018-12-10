// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef _types_h
#define _types_h
#include <boost/serialization/strong_typedef.hpp>

// Define types to make it more difficult to confuse those values.
// #ifdef NDEBUG
// using StageId   = int;
// using StageName = std::string;
// using StageIdx  = size_t;
// using StageKey  = uint64_t;
// #else
BOOST_STRONG_TYPEDEF(int, StageId);
BOOST_STRONG_TYPEDEF(std::string, StageName);
BOOST_STRONG_TYPEDEF(size_t, StageIdx);
BOOST_STRONG_TYPEDEF(uint64_t, StageKey);
namespace std {
template <> struct hash<StageKey> {
    size_t operator()(const StageKey &obj) const
    {
        //	size_t hash = 0;
        //	boost::hash_combine(hash, schedule.at(i));
        return hash<uint64_t>()(static_cast<uint64_t>(obj));
    }
};
} // namespace std
//#endif

// #ifdef NDEBUG
// using TaskIdx = int;
// #else
BOOST_STRONG_TYPEDEF(int, TaskIdx);
//#endif

// #ifdef NDEBUG
// using ResourceIdx = int;
// using ResourceKey = uint64_t;
// using NodeClassId = int;
// #else
BOOST_STRONG_TYPEDEF(int, WorkerId);
namespace std {
template <> struct hash<WorkerId> {
    size_t operator()(const WorkerId &obj) const
    {
        return hash<int>()(static_cast<int>(obj));
    }
};
} // namespace std
BOOST_STRONG_TYPEDEF(int, ResourceIdx);
BOOST_STRONG_TYPEDEF(uint64_t, ResourceKey);
namespace std {
template <> struct hash<ResourceKey> {
    size_t operator()(const ResourceKey &obj) const
    {
        //	size_t hash = 0;
        //	boost::hash_combine(hash, schedule.at(i));
        return hash<uint64_t>()(static_cast<uint64_t>(obj));
    }
};
} // namespace std

BOOST_STRONG_TYPEDEF(int, NodeClassId);

BOOST_STRONG_TYPEDEF(std::string, AppId);
namespace std {
template <> struct hash<AppId> {
    size_t operator()(const AppId &obj) const
    {
        return hash<std::string>()(static_cast<std::string>(obj));
    }
};
} // namespace std

//#endif

#endif
