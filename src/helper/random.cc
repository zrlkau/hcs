// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "helper/random.h"

std::default_random_engine              random_rd;
std::uniform_int_distribution<uint64_t> random_rng;


uint64_t rand(uint64_t max)
{
    return (random_rng(random_rd) % max);
}
