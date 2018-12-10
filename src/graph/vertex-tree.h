// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef vertex_tree_h
#define vertex_tree_h

#include <graph/graph-tree-common.h>
#include <graph/vertex-simple.h>

namespace graph_simple {
using boost::property_tree::ptree;

template <class V, class C> class TreeVertex : public BaseVertex<V, C>
{
  public:
    TreeVertex();
    TreeVertex(const ptree &data);
    ptree store() const;

    C &                             add_uplink(std::string iodev);
    boost::optional<C &>            get_uplink(std::string iodev);
    const std::map<std::string, C> &get_uplinks()
    {
        return up;
    }

    C &                             add_downlink(std::string iodev);
    boost::optional<C &>            get_downlink(std::string iodev);
    const std::map<std::string, C> &get_downlinks() const
    {
        return down;
    }

    C &add_link(const ptree &data);

    void is_root(bool is_root)
    {
        this->m_is_root = is_root;
    }
    bool is_root() const
    {
        return this->m_is_root;
    }

  protected:
  private:
    std::map<std::string, C> up;
    std::map<std::string, C> down;

    bool m_is_root;
};

template <class V, class C> TreeVertex<V, C>::TreeVertex() : BaseVertex<V, C>()
{
    this->m_is_root = false;
}

template <class V, class C> TreeVertex<V, C>::TreeVertex(const ptree &data) : BaseVertex<V, C>(data)
{
    this->m_is_root = false;

    ptree clist = data.get_child("connectors");
    for (auto &c : clist) {
        this->add_link(c.second);
    }
}

template <class V, class C> ptree TreeVertex<V, C>::store() const
{
    ptree data = BaseVertex<V, C>::store();

    ptree clist;

    for (auto const &c : up)
        clist.push_back(ptree::value_type("", c.second.store()));
    for (auto const &c : down)
        clist.push_back(ptree::value_type("", c.second.store()));

    data.add_child("connectors", clist);
    return data;
}

template <class V, class C> C &TreeVertex<V, C>::add_uplink(std::string iodev)
{
    auto c = up.emplace(std::piecewise_construct,
                        std::forward_as_tuple(iodev),
                        std::forward_as_tuple(*static_cast<V *>(this), TreeConnectorDir::up));
    c.first->second.set_id(iodev);
    return c.first->second;
}

template <class V, class C> boost::optional<C &> TreeVertex<V, C>::get_uplink(std::string iodev)
{
    try {
        return boost::optional<C &>(up.at(iodev));
    } catch (const std::out_of_range &err) {
        return boost::none;
    }
}

template <class V, class C> C &TreeVertex<V, C>::add_downlink(std::string iodev)
{
    auto c = down.emplace(std::piecewise_construct,
                          std::forward_as_tuple(iodev),
                          std::forward_as_tuple(*static_cast<V *>(this), TreeConnectorDir::down));
    c.first->second.set_id(iodev);
    c.first->second.level(0);
    return c.first->second;
}

template <class V, class C> boost::optional<C &> TreeVertex<V, C>::get_downlink(std::string iodev)
{
    try {
        return boost::optional<C &>(down.at(iodev));
    } catch (const std::out_of_range &err) {
        return boost::none;
    }
}

template <class V, class C> C &TreeVertex<V, C>::add_link(const ptree &data)
{
    std::string id = data.get<std::string>("id");

    if (data.get<TreeConnectorDir>("dir") == TreeConnectorDir::up) {
        auto c = up.emplace(std::piecewise_construct,
                            std::forward_as_tuple(id),
                            std::forward_as_tuple(*static_cast<V *>(this), data));
        //      c.first->second.set_id(id);
        return c.first->second;
    } else {
        auto c = down.emplace(std::piecewise_construct,
                              std::forward_as_tuple(id),
                              std::forward_as_tuple(*static_cast<V *>(this), data));
        //    c.first->second.set_id(id);
        return c.first->second;
    }
}
} // namespace graph_simple
#endif
