// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "helper/operators.h"
#include "scheduler/schedule.h"

#include <iomanip>
#include <string>

std::ostream &operator<<(std::ostream &os, const std::vector<bool> &obj)
{
    os << "(";
    for (size_t i = 0; i < obj.size(); i++) {
        os << std::setw(2) << obj[i];
        if (i < obj.size() - 1)
            os << " ";
    }
    os << ")";
    return os;
}

std::ostream &operator<<(std::ostream &os, const std::vector<int> &obj)
{
    os << "(";
    for (size_t i = 0; i < obj.size(); i++) {
        os << std::setw(2) << obj[i];
        if (i < obj.size() - 1)
            os << " ";
    }
    os << ")";
    return os;
}

std::ostream &operator<<(std::ostream &os, const std::vector<uint> &obj)
{
    os << "(";
    for (size_t i = 0; i < obj.size(); i++) {
        os << std::setw(2) << obj[i];
        if (i < obj.size() - 1)
            os << " ";
    }
    os << ")";
    return os;
}

std::ostream &operator<<(std::ostream &os, const std::vector<uint64_t> &obj)
{
    os << "(";
    for (size_t i = 0; i < obj.size(); i++) {
        os << std::setw(2) << obj[i];
        if (i < obj.size() - 1)
            os << " ";
    }
    os << ")";
    return os;
}

std::ostream &operator<<(std::ostream &os, const std::set<size_t> &obj)
{
    os << "(";
    for (auto i : obj) {
        os << std::setw(2) << i;
        //        if (i < obj.size() - 1)
        os << " ";
    }
    os << ")";
    return os;
}

//#ifndef NDEBUG
std::ostream &operator<<(std::ostream &os, const std::vector<StageId> &obj)
{
    os << "(";
    for (StageId i = static_cast<StageId>(0); i < obj.size(); i++) {
        os << std::setw(2) << obj[i];
        if (i < obj.size() - 1)
            os << " ";
    }
    os << ")";
    return os;
}
//#endif

std::ostream &operator<<(std::ostream &os, const std::vector<std::string> &obj)
{
    os << "(";
    for (size_t i = 0; i < obj.size(); i++) {
        os << obj[i];
        if (i < obj.size() - 1)
            os << " ";
    }
    os << ")";
    return os;
}

std::ostream &operator<<(std::ostream &os, const std::vector<float> &obj)
{
    os << "(";
    for (size_t i = 0; i < obj.size(); i++) {
        os << std::setw(2) << std::setprecision(5) << obj[i];
        if (i < obj.size() - 1)
            os << " ";
    }
    os << ")";
    return os;
}

std::ostream &operator<<(std::ostream &os, const Load &obj)
{
    for (auto &entry : obj.load()) {
        os << entry << " ";
    }
    return os;
}

std::ostream &operator<<(std::ostream &os, const LoadEntry &obj)
{
    os << "(" << obj.from.count() << " - " << obj.to.count() << ", " << std::setprecision(6) << obj.load
       << ")";
    return os;
}

std::ostream &operator<<(std::ostream &os, const AppId &obj)
{
    os << static_cast<std::string>(obj);
    return os;
}
