// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef graph_simple_h
#define graph_simple_h

#include <list>
#include <map>
#include <string>

#include "common.h"
#include "vertex-simple.h"
#include "vertex-tree.h"

// TODO:
// 1. Add connect method to BaseGraph (TreeGraph has one)

namespace graph_simple {
using namespace boost::property_tree;
//    using namespace boost::numeric::ublas;

/*
      Requirements
      1. needs to store vertices and edges between those vertices
      2. vertices have I/O ports that contain all information about
    */
template <class V, class C> class BaseGraph
{
  public:
    BaseGraph();
    BaseGraph(const ptree &data);

    V &                  add_vertex();
    V &                  add_vertex(const ptree &data);
    boost::optional<V &> get_vertex(std::string id);
    const std::list<V> & get_vertices() const
    {
        return this->_vertices;
    }

    std::list<V> &vertices()
    {
        return this->_vertices;
    }
    const std::list<V> &vertices() const
    {
        return this->_vertices;
    }

    //    void connect(C& src, C& dst);

    ptree store() const;

  protected:
    /*
          1. std::vector of pointers to Vertex objects
             - need to take care of destruction
             - potential memory leaks
             + stable pointers
          2. boost::stable_vector of Vertex objects
             - internally also uses pointers
             + stable references and pointers
             + no memory leaks
             + don't need to take care of destruction
          3. std::vector of Vertex objects
             - no stable references and pointers possible
             - need to use vector index for a stable reference.
             - loose type safety due to index access.
             + efficient memory pattern for linear access.
             + no memory leaks but potentially broken references and
               pointers
        */

    /*
          Vertices are rarely, if at all, accessed through this
          list. They are usually referenced using direct
          pointers/references. If they are accessed at all, the access
          is linear, hence the list is ok.
        */
    std::list<V>               _vertices;
    std::map<std::string, V *> vmap;

  private:
    /*
          Cost matrix. Initial values (i.e. costs between direct
          neighbors) are set using measured or extracted values. Other
          costs are computed from those values.  TODO: I am not sure if
          this helps, because we always have to consider load and for a
          normal cluster (tree/star topology), the number of paths to
          cover are very small anyway.
        */
    // matrix<uint64_t> costs;
};

template <class V, class C> BaseGraph<V, C>::BaseGraph()
{
}

template <class V, class C> BaseGraph<V, C>::BaseGraph(const ptree &data)
{
}

template <class V, class C> ptree BaseGraph<V, C>::store() const
{
    ptree data;

    return data;
}

template <class V, class C> V &BaseGraph<V, C>::add_vertex()
{
    _vertices.emplace_back();
    V &v = _vertices.back();
    vmap.insert(make_pair(v.id(), &v));
    return v;
}

template <class V, class C> V &BaseGraph<V, C>::add_vertex(const ptree &data)
{
    _vertices.emplace_back(data);
    V &v = _vertices.back();
    vmap.insert(make_pair(v.id(), &v));
    return v;
}

// Todo: continue with graph load...
template <class V, class C> boost::optional<V &> BaseGraph<V, C>::get_vertex(std::string id)
{
    try {
        auto v = vmap.at(id);
        return boost::optional<V &>(*v);
    } catch (const std::out_of_range &err) {
        return boost::none;
    }
}
} // namespace graph_simple
#endif
