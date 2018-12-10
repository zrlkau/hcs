// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef graph_tree_common_h
#define graph_tree_common_h

#include <istream>
#include <ostream>

namespace graph_simple {
enum class TreeConnectorDir : char { up, down };
std::ostream &operator<<(std::ostream &os, TreeConnectorDir dir);
std::istream &operator>>(std::istream &is, TreeConnectorDir &dir);
} // namespace graph_simple

#endif
