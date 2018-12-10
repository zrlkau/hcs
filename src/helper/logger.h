// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef logger_h
#define logger_h

#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>

#include <iomanip>
#if hcs_debug > 0
#define dlevel 1
#define mutex_dlevel 100
#else
#define dlevel 0
#define mutex_dlevel 0
#endif

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define LC_OFF (0UL)
#define LC_ON (1UL << 0)

#define LC_LOCK (1UL << 1)
#define LC_API (1UL << 2)
#define LC_APP (1UL << 3)
#define LC_SCHED (1UL << 4)
#define LC_RM (1UL << 5)
#define LC_EVENT (1UL << 6)

#define LC_INFO_LEVEL (LC_RM | LC_SCHED | LC_API | LC_LOCK)
//#define LC_INFO_LEVEL (0UL)
#define LC_DEBUG_LEVEL (LC_RM | LC_SCHED | LC_API | LC_LOCK | LC_EVENT)
#define LC_WARN_LEVEL (LC_RM | LC_SCHED | LC_API | LC_LOCK | LC_EVENT)

#define logInfo(level)                                                                                     \
    if (level <= dlevel)                                                                                   \
    BOOST_LOG_TRIVIAL(info) << Time::now() << " [" << std::setw(18) << __FILENAME__ << ":" << std::setw(4) \
                            << std::setfill('0') << __LINE__ << " " << std::setw(2) << std::setfill('0')   \
                            << level << "] "
#define logDebug                                                                                            \
    BOOST_LOG_TRIVIAL(debug) << Time::now() << " [" << std::setw(18) << __FILENAME__ << ":" << std::setw(4) \
                             << std::setfill('0') << __LINE__ << " " << std::setw(2) << std::setfill('0')   \
                             << "DD] "
#define logWarn                                                                                               \
    BOOST_LOG_TRIVIAL(warning) << Time::now() << " [" << std::setw(18) << __FILENAME__ << ":" << std::setw(4) \
                               << std::setfill('0') << __LINE__ << " " << std::setw(2) << std::setfill('0')   \
                               << "WW] "
#define logError                                                                                            \
    BOOST_LOG_TRIVIAL(error) << Time::now() << " [" << std::setw(18) << __FILENAME__ << ":" << std::setw(4) \
                             << std::setfill('0') << __LINE__ << " " << std::setw(2) << std::setfill('0')   \
                             << "EE] "

#define logCInfo(category)                                                                                 \
    if (LC_INFO_LEVEL & (category))                                                                        \
    BOOST_LOG_TRIVIAL(info) << Time::now() << " [" << std::setw(18) << __FILENAME__ << ":" << std::setw(4) \
                            << std::setfill('0') << __LINE__ << " " << std::setw(2) << std::setfill('0')   \
                            << "II] "
#define logCDebug(category)                                                                                 \
    if (LC_DEBUG_LEVEL & (category))                                                                        \
    BOOST_LOG_TRIVIAL(debug) << Time::now() << " [" << std::setw(18) << __FILENAME__ << ":" << std::setw(4) \
                             << std::setfill('0') << __LINE__ << " " << std::setw(2) << std::setfill('0')   \
                             << "DD] "
#define logCWarn(category)                                                                                    \
    if (LC_WARN_LEVEL & (category))                                                                           \
    BOOST_LOG_TRIVIAL(warning) << Time::now() << " [" << std::setw(18) << __FILENAME__ << ":" << std::setw(4) \
                               << std::setfill('0') << __LINE__ << " " << std::setw(2) << std::setfill('0')   \
                               << "WW] "

#define dassert(check, msg)                                                                   \
    if (!(check)) {                                                                           \
        derr("ASSERT[" << __FUNCTION__ << ":" << __LINE__ << "] " #check " FAILED: " << msg); \
        assert(check);                                                                        \
    }

#define dtime(level, id, cmd)                                                                                 \
    const std::chrono::high_resolution_clock::time_point t0_##id = std::chrono::high_resolution_clock::now(); \
    cmd;                                                                                                      \
    const std::chrono::high_resolution_clock::time_point t1_##id = std::chrono::high_resolution_clock::now(); \
    logInfo(level) << "Execution of " #cmd " took " << std::fixed << std::setprecision(0)                     \
                   << (std::chrono::duration_cast<std::chrono::duration<double>>(t1_##id - t0_##id).count() * \
                       1000000.0)                                                                             \
                   << " us";

#endif
