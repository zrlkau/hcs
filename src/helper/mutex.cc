// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "mutex.h"
#include "logger.h"
#include "time.h"

Mutex::Mutex()
{
    logCWarn(LC_LOCK) << "Constructing mutex " << this;
    std::timed_mutex::timed_mutex();
}

Mutex::~Mutex()
{
    logCWarn(LC_LOCK) << "Destructing mutex " << this;
    std::timed_mutex::~timed_mutex();
}

void Mutex::lock()
{
    logCWarn(LC_LOCK) << "Locking mutex " << this;
    std::timed_mutex::lock();
    logCWarn(LC_LOCK) << "Locked mutex " << this;
}

bool Mutex::try_lock()
{
    logCWarn(LC_LOCK) << "Try locking mutex " << this;
    return std::timed_mutex::try_lock();
}

bool Mutex::try_lock_for(const std::chrono::duration<long,std::ratio<1l, 1l>> &timeout_duration)
{
    logCWarn(LC_LOCK) << "Try locking mutex " << this << " for "; // << timeout_duration;
    return std::timed_mutex::try_lock_for(timeout_duration);
}

template <class Clock, class Duration>
bool Mutex::try_lock_until(const std::chrono::time_point<Clock, Duration> &timeout_time)
{
    logCWarn(LC_LOCK) << "Try locking mutex " << this << " until " << timeout_time;
    return std::timed_mutex::try_lock_until(timeout_time);
}

void Mutex::unlock()
{
    logCWarn(LC_LOCK) << "Unlocking mutex " << this;
    std::timed_mutex::unlock();
}
