// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef app_io_h
#define app_io_h

#include "common.h"

#include <iostream>
#include <utility>

class Io
{
  public:
    Io(Stage *src, Stage *dst, size_t volume);

    SETGET(size_t, volume);
    SETGET(std::pair<size_t COMMA size_t>&, weight);

    GET(Stage *, src);
    GET(Stage *, dst);

  protected:
    SET(Stage *, src);
    SET(Stage *, dst);

  private:
    size_t                    _volume;
    std::pair<size_t, size_t> _weight;

    Stage *_src;
    Stage *_dst;
};

std::ostream &operator<<(std::ostream &os, const Io &obj);
std::ostream &operator<<(std::ostream &os, const Direction &obj);
std::istream &operator>>(std::istream &is, Direction &obj);

#endif
