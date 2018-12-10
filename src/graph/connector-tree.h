// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef connector_tree_h
#define connector_tree_h

#include "connector-simple.h"
#include "graph-tree-common.h"

namespace graph_simple {
using boost::property_tree::ptree;

template <class V, class C> class TreeConnector : public BaseConnector<V, C>
{
  public:
    TreeConnector(V &owner, TreeConnectorDir dir);
    TreeConnector(V &owner, const ptree &data);
    ptree store() const;

    void level(int level)
    {
        this->m_level = level;
    }
    int level() const
    {
        return this->m_level;
    }

    void dir(TreeConnectorDir dir)
    {
        this->m_dir = dir;
    }
    int dir() const
    {
        return this->m_dir;
    }

  protected:
  private:
    TreeConnectorDir m_dir;
    int              m_level;
};

template <class V, class C>
TreeConnector<V, C>::TreeConnector(V &owner, TreeConnectorDir dir) : BaseConnector<V, C>(owner)
{
    this->m_level = -1;
    this->m_dir   = dir;
}

template <class V, class C>
TreeConnector<V, C>::TreeConnector(V &owner, const ptree &data) : BaseConnector<V, C>(owner, data)
{
    this->m_dir   = data.get<TreeConnectorDir>("dir");
    this->m_level = data.get<int>("level");
}

template <class V, class C> ptree TreeConnector<V, C>::store() const
{
    ptree data = BaseConnector<V, C>::store();

    data.put("dir", this->m_dir);
    data.put("level", this->m_level);

    return data;
}
} // namespace graph_simple
#endif
