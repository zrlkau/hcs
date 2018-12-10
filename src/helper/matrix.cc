// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "matrix.h"
#include "common.h"

std::ostream &operator<<(std::ostream &os, const Matrix<uint64_t> &obj)
{
    for (size_t x = 0; x < obj.x_dim(); x++) {
        for (size_t y = 0; y < obj.y_dim(); y++) {
            os << std::setw(20) << obj(x, y) << " ";
        }
        os << std::endl;
    }
    return os;
}

std::ostream &operator<<(std::ostream &os, const Matrix<int> &obj)
{
    for (size_t x = 0; x < obj.x_dim(); x++) {
        for (size_t y = 0; y < obj.y_dim(); y++) {
            os << std::setw(1) << obj(x, y) << " ";
        }
        os << std::endl;
    }
    return os;
}

std::ostream &operator<<(std::ostream &os, const Matrix<short> &obj)
{
    for (size_t x = 0; x < obj.x_dim(); x++) {
        for (size_t y = 0; y < obj.y_dim(); y++) {
            os << std::setw(1) << obj(x, y) << " ";
        }
        os << std::endl;
    }
    return os;
}

std::ostream &operator<<(std::ostream &os, const Matrix<bool> &obj)
{
    for (size_t x = 0; x < obj.x_dim(); x++) {
        for (size_t y = 0; y < obj.y_dim(); y++) {
            os << std::setw(1) << obj.get(x, y) << " ";
        }
        os << std::endl;
    }
    return os;
}
