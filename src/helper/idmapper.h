// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef idmapper_h
#define idmapper_h

#include "common.h"

#include <boost/property_tree/ptree.hpp>
#include <map>
#include <vector>
using boost::property_tree::ptree;

template <class I, class O> class IDMapper
{
  public:
    IDMapper()
    {
    }

    void load(const ptree &data)
    {
        for (const auto &entry : data.get_child("entries")) {
            I val = entry.second.get<I>("val");
            O uid = entry.second.get<O>("uid");

            this->_toUniqueId.emplace(val, uid);
            this->_fromUniqueId.emplace(uid, val);
        }
    }

    ptree store() const
    {
        ptree data;
        ptree entries;

        for (auto &entry : this->_toUniqueId) {
            ptree tmp;
            tmp.put("val", entry.first);
            tmp.put("uid", entry.second);
            entries.push_back(std::make_pair("", tmp));
        }

        data.add_child("entries", entries);
        return data;
    }

    size_t size() const
    {
        return this->_fromUniqueId.size();
    }

    bool exists(I obj) const
    {
        return this->_toUniqueId.find(obj) != this->_toUniqueId.end();
    }

    O add(I obj)
    {
        if (this->_toUniqueId.find(obj) == this->_toUniqueId.end()) {
            this->_toUniqueId.emplace(obj, this->_toUniqueId.size());
            this->_fromUniqueId.emplace(this->_fromUniqueId.size(), obj);
        }

        return this->_toUniqueId.at(obj);
    }

    I get(O id) const
    {
        return this->_fromUniqueId.at(id);
    }

    O get(I obj) const
    {
        return this->_toUniqueId.at(obj);
    }

  private:
    std::map<I, O> _toUniqueId;
    std::map<O, I> _fromUniqueId;
};
#endif
