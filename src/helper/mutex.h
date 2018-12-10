// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include <mutex>

class Mutex : public std::timed_mutex
{
  public:
    Mutex();
    ~Mutex();

    void lock();

    bool try_lock();
    bool try_lock_for(const std::chrono::duration<long,std::ratio<1l, 1l>> &timeout_duration);
    template <class Clock, class Duration>
    bool try_lock_until(const std::chrono::time_point<Clock, Duration> &timeout_time);

    void unlock();

  protected:
  private:
};
