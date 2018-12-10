// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef __helper__operators_h
#define __helper__operators_h

#include "helper/classes.h"
#include "helper/types.h"

#include <iostream>
#include <set>
#include <vector>

// misc
std::ostream &operator<<(std::ostream &os, const std::vector<bool> &obj);
std::ostream &operator<<(std::ostream &os, const std::vector<int> &obj);
std::ostream &operator<<(std::ostream &os, const std::vector<uint> &obj);
std::ostream &operator<<(std::ostream &os, const std::vector<uint64_t> &obj);

std::ostream &operator<<(std::ostream &os, const std::set<size_t> &obj);

//#ifndef NDEBUG
std::ostream &operator<<(std::ostream &os, const std::vector<StageId> &obj);
std::ostream &operator<<(std::ostream &os, const std::vector<float> &obj);
std::ostream &operator<<(std::ostream &os, const std::vector<std::string> &obj);
//#endif
std::ostream &operator<<(std::ostream &os, const AppId &obj);
std::ostream &operator<<(std::ostream &os, const Load &obj);
std::ostream &operator<<(std::ostream &os, const LoadEntry &obj);

template <typename Type, typename Compare = std::less<Type>>
struct compare : public std::binary_function<Type *, Type *, bool> {
    bool operator()(const Type *x, const Type *y) const
    {
        return Compare()(*x, *y);
    }
};

#endif
