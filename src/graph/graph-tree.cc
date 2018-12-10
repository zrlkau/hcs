// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include <istream>
#include <ostream>

#include "graph-tree-common.h"

namespace graph_simple {
using namespace std;

ostream &operator<<(std::ostream &os, TreeConnectorDir dir)
{
    switch (dir) {
    case TreeConnectorDir::up:
        return os << "up";
    case TreeConnectorDir::down:
        return os << "down";
    default:
        return os << "invalid";
    };
}

istream &operator>>(std::istream &is, TreeConnectorDir &dir)
{

    char str[5];
    is.getline(str, 5);

    if (str == string("up"))
        dir = TreeConnectorDir::up;
    if (str == string("down"))
        dir = TreeConnectorDir::down;

    return is;
}
} // namespace graph_simple
