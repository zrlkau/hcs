// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "app/io.h"
#include "app/stage.h"

#include <iomanip>

Io::Io(Stage *src, Stage *dst, size_t volume)
{
    assert(src != NULL);
    assert(dst != NULL);

    this->src(src);
    this->dst(dst);
    this->volume(volume);
    this->weight() = std::make_pair(0UL, 0UL);
}

std::ostream &operator<<(std::ostream &os, const Io &obj)
{
    std::ostringstream out;

    if (obj.volume() > (1024 * 1024 * 1024))
        out << "Stage IO " << obj.src()->id() << " --[" << std::setw(3) << obj.volume() / (1024 * 1024 * 1024)
            << " GB]--> " << obj.dst()->id();
    else if (obj.volume() > (1024 * 1024))
        out << "Stage IO " << obj.src()->id() << " --[" << std::setw(3) << obj.volume() / (1024 * 1024)
            << " MB]--> " << obj.dst()->id();
    else if (obj.volume() > (1024))
        out << "Stage IO " << obj.src()->id() << " --[" << std::setw(3) << obj.volume() / (1024) << " KB]--> "
            << obj.dst()->id();
    else
        out << "Stage IO " << obj.src()->id() << " --[" << std::setw(3) << obj.volume() << " B]--> "
            << obj.dst()->id();
    os << out.str();
    return os;
}

std::ostream &operator<<(std::ostream &os, const Direction &obj)
{
    switch (obj) {
    case Direction::up:
        os << "up";
        break;
    case Direction::down:
        os << "down";
        break;
    default:
        os << "unknown";
        break;
    }
    return os;
}

std::istream &operator>>(std::istream &is, Direction &obj)
{
    std::string s(std::istreambuf_iterator<char>(is), {});

    if (s == "up")
        obj = Direction::up;
    else if (s == "down")
        obj = Direction::down;
    else
        is.setstate(std::ios::failbit);

    return is;
}
