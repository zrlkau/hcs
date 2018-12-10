// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef sorter_h
#define sorter_h

#include <algorithm>
#include <utility>

#include "resourcemanager/resource.h"

class Sorter
{
  public:
    /*
  2, 4, 6, 8, 10, 12, 14, 16, 18

  14 -> 5

  2,  4,  6,  8, 10, 12,  5, 16, 18
  2,  4,  6,  8, 10,  5, 12, 16, 18
  2,  4,  6,  8,  5, 10, 12, 16, 18
  2,  4,  6,  5,  8, 10, 12, 16, 18
  2,  4,  5,  6,  8, 10, 12, 16, 18

 */
    // static inline void swap(C &container, size_t i0, size_t i1) {
    // 	auto tmp = container[i0];
    // 	container[i0] = container[i1];
    // 	container[i1] = tmp;
    // }

    static void sort(std::vector<Resource *> &container)
    {
        std::sort(container.begin(), container.end(), Resource::compare);
    }

    static void decrease(std::vector<Resource *> &container, size_t idx)
    {
        for (size_t _idx = idx - 1; _idx > 0; _idx--) {
            if (Resource::compare(container[idx], container[_idx])) {
                std::swap(container[idx], container[_idx]);
                idx = _idx;
            } else {
                break;
            }
        }
    }

    static void increase(std::vector<Resource *> &container, size_t idx)
    {
        for (size_t _idx = idx + 1; _idx < container.size(); _idx++) {
            if (Resource::compare(container[_idx], container[idx])) {
                std::swap(container[idx], container[_idx]);
                idx = _idx;
            } else {
                break;
            }
        }
    }
};

#endif
