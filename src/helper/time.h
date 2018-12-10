// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef __helper_time_h
#define __helper_time_h

#include "helper/classes.h"

#include <chrono>
#include <ostream>

using namespace std::literals::chrono_literals;

extern Time epoch_start;
//typedef std::chrono::microseconds Duration;

class Duration : public std::chrono::microseconds
{
  public:
    Duration() : std::chrono::microseconds(0)
    {
    }

    Duration(uint64_t us) : std::chrono::microseconds(us)
    {
    }

    Duration(std::chrono::seconds s)
        : std::chrono::microseconds(std::chrono::duration_cast<std::chrono::microseconds>(s))
    {
    }

    Duration(std::chrono::milliseconds ms)
        : std::chrono::microseconds(std::chrono::duration_cast<std::chrono::microseconds>(ms))
    {
    }

    Duration(std::chrono::microseconds us) : std::chrono::microseconds(us)
    {
    }

    Duration(std::chrono::nanoseconds ns)
        : std::chrono::microseconds(std::chrono::duration_cast<std::chrono::microseconds>(ns))
    {
    }

    static Duration zero()
    {
        return std::chrono::microseconds::zero();
    }
    static Duration min()
    {
        return std::chrono::microseconds::min();
    }
    static Duration max()
    {
        return std::chrono::microseconds::max();
    }
};
std::ostream &operator<<(std::ostream &os, const Duration &obj);

namespace std {
template <> struct hash<Duration> {
    uint64_t operator()(const Duration &obj) const
    {
        return static_cast<uint64_t>(obj.count());
    }
};
} // namespace std

class Time : public std::chrono::system_clock::time_point
{
  public:
    Time(uint64_t microseconds) : std::chrono::system_clock::time_point(std::chrono::microseconds(microseconds))
    {
    }

    Time(Duration duration) : std::chrono::system_clock::time_point(duration)
    {
    }

    Time(std::chrono::system_clock::time_point timepoint) : std::chrono::system_clock::time_point(timepoint)
    {
    }

    Time() : std::chrono::system_clock::time_point(std::chrono::system_clock::time_point::min())
    {
    }

    static Time zero()
    {
        return std::chrono::system_clock::time_point::min();
    }

    static Time now()
    {
        return std::chrono::system_clock::now();
    }

    static Time abs(Time rel)
    {
        return Time(rel + epoch_start);
    }

    uint64_t count() const
    {
        return (*this - epoch_start).count() / 1000; // microseconds since beginning of time (1970-1-1)
    }

    inline Time operator()(const Time &obj)
    {
        *this = obj;
        return *this;
    }

    inline Time operator+(const Duration &rhs) const
    {
        return static_cast<std::chrono::system_clock::time_point>(*this) + rhs;
    }

    inline Time operator-(const Duration &rhs) const
    {
        return static_cast<std::chrono::system_clock::time_point>(*this) - rhs;
    }

    inline Duration operator-(const Time &rhs) const
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            static_cast<std::chrono::system_clock::time_point>(*this) -
            static_cast<std::chrono::system_clock::time_point>(rhs));
    }

    inline Duration operator+(const Time &rhs) const
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            static_cast<std::chrono::system_clock::time_point>(*this) -
            static_cast<std::chrono::system_clock::time_point>(rhs));
    }

    inline bool operator<(const Time &other) const
    {
        return count() < other.count();
    }

    inline bool operator>(const Time &other) const
    {
        return other < *this;
    }

    inline bool operator<=(const Time &other) const
    {
        return !(*this > other);
    }

    inline bool operator>=(const Time &other) const
    {
        return !(*this < other);
    }

    inline bool operator==(const Time &other) const
    {
        return count() == other.count();
    }

    inline bool operator!=(const Time &other) const
    {
        return !(*this == other);
    }

  protected:
  private:
};
std::ostream &operator<<(std::ostream &os, const Time &obj);

#endif
