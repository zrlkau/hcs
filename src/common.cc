// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "common.h"
#include "event/engine.h"

#include <boost/property_tree/json_parser.hpp>
#include <iomanip>

size_t counter = 0;

// const Duration start = time();
// //Duration time() { return std::chrono::duration_cast<Duration>(std::chrono::system_clock::now().time_since_epoch() - start.time_since_epoch()); }
// Duration time()
// {
//     return std::chrono::duration_cast<Duration>(std::chrono::system_clock::now().time_since_epoch());
// }

std::string to_string(const boost::property_tree::ptree &obj)
{
    std::ostringstream os;
    write_json(os, obj, false);
    return os.str();
}
