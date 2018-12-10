// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "helper/time.h"

#include <iomanip>
#include <ostream>
#include <string>

Time epoch_start = Time::now();

std::ostream &operator<<(std::ostream &os, const Duration &obj)
{
#if hcs_debug == 0
    if (obj.count() >= 1000000)
        os << std::fixed << std::right << std::setw(7) << std::setprecision(3)
           << (double)obj.count() / (double)1000000 << "s ";
    else if (obj.count() >= 1000)
        os << std::fixed << std::right << std::setw(7) << std::setprecision(3)
           << (double)obj.count() / (double)1000 << "ms";
    else
        os << std::fixed << std::right << std::setw(7) << std::setprecision(3) << (double)obj.count() << "us";
#else
    os << std::fixed << std::right << std::setw(0) << std::setprecision(0) << (double)obj.count() << "us";
#endif
    return os;
}

std::ostream &operator<<(std::ostream &os, const Time &obj)
{
    os << obj.count() << "ms";
    return os;
}
