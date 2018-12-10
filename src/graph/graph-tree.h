// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef graph_tree_h
#define graph_tree_h

#include <boost/optional.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/tokenizer.hpp>
#include <common.h>

#include <graph/graph-simple.h>
#include <graph/graph-tree-common.h>

namespace graph_simple {
template <class V, class C> class TreeGraph : public BaseGraph<V, C>
{
  public:
    TreeGraph();
    TreeGraph(const ptree &data);
    ptree store() const;

    void connect(V &parent, std::string parent_iodev, V &child, std::string child_iodev);
    std::list<std::pair<V &, C &>>
    get_path(V &parent, std::string parent_iodev, V &child, std::string child_iodev);

    V &            add_root();
    std::list<V *> root()
    {
        return this->m_root;
    }
    const std::list<V *> root() const
    {
        return this->m_root;
    }

  protected:
  private:
    std::list<V *> m_root;
};

template <class V, class C> TreeGraph<V, C>::TreeGraph() : BaseGraph<V, C>()
{
}

template <class V, class C> TreeGraph<V, C>::TreeGraph(const ptree &data) : BaseGraph<V, C>(data)
{
    std::string type = data.get<std::string>("type");
    if (type != "tree")
        throw "wrong graph type " + type + " (expected \"tree\")";

    boost::optional<const ptree &> vlist = data.get_child_optional("vertices");
    if (vlist) {
        for (auto const &entry : *vlist) {
            this->add_vertex(entry.second);
        }
    }

    boost::optional<const ptree &> rlist = data.get_child_optional("root");
    if (rlist) {
        for (auto const &id : *rlist) {
            std::string          idstr = id.second.get<std::string>("");
            boost::optional<V &> root  = this->get_vertex(idstr);
            if (root)
                m_root.push_back(&(*root));
        }
    }

    boost::optional<const ptree &> elist = data.get_child_optional("edges");
    if (elist) {
        boost::char_separator<char> sep{":"};
        for (auto const &entry : *elist) {
            std::string up[3], down[3];
            {
                up[0] = entry.second.get<std::string>("up");
                boost::tokenizer<boost::char_separator<char>> tok{up[0], sep};
                auto                                          it = tok.begin();
                for (int i = 1; it != tok.end(); it++, i++) {
                    up[i] = *it;
                }
            }
            {
                down[0] = entry.second.get<std::string>("down");
                boost::tokenizer<boost::char_separator<char>> tok{down[0], sep};
                auto                                          it = tok.begin();
                for (int i = 1; it != tok.end(); it++, i++) {
                    down[i] = *it;
                }
            }

            /*
	  connectors : [
	    {
	      owner      : "parent hostname", // 
	      peer       : "child hostname",  //
	      properties : {                  // properties from derived classes
                iodev    : "io dev id",
                type     : "ethernet",        // type identifiers from hwloc
                capacity : "1000000000",      // in bytes per second
                latency  : "10",              // in us
              }
	    }, ...
	  ]
	 */

            boost::optional<V &> vup = this->get_vertex(up[1]);
            boost::optional<C &> cup = vup->get_downlink(up[2]);

            boost::optional<V &> vdown = this->get_vertex(down[1]);
            boost::optional<C &> cdown = vdown->get_uplink(down[2]);

            if (!vup || !cup || !vdown || !cdown) {
                //                    dout(0) << "Error loading file" << endl;
                continue;
            }

            cup->set_peer(*cdown);
            cdown->set_peer(*cup);
        }
    }
}

template <class V, class C> V &TreeGraph<V, C>::add_root()
{
    V &v = BaseGraph<V, C>::add_vertex();
    v.is_root(true);
    this->m_root.push_back(&v);
    return v;
}

template <class V, class C> ptree TreeGraph<V, C>::store() const
{
    ptree data = BaseGraph<V, C>::store();
    // 1.0 Store what type (i.e. topology) of graph this is.
    data.put("type", "tree");

    ptree vlist;
    for (auto const &v : BaseGraph<V, C>::vertices()) {
        // 2.0. Get vertex data (including connectors)
        ptree vdata = v.store();
        // 2.1. Store all vertices
        vlist.push_back(ptree::value_type("", vdata));
    }
    // 2.2. Finally add vertices and connectors to property tree.
    data.add_child("vertices", vlist);

    ptree rlist;
    for (auto const &r : root()) {
        // 2.0. Get vertex data (including connectors)
        std::string id = r->id();
        // 2.1. Store all vertices
        rlist.put("id", id);
    }
    // 2.2. Finally add vertices and connectors to property tree.
    data.add_child("root", rlist);

    ptree elist;
    for (auto &v : BaseGraph<V, C>::vertices()) {
        // 2.0. Get vertex data (including connectors)
        for (auto const &c : v.get_downlinks()) {
            ptree    edge;
            const C &up_c = c.second;
            const V &up   = up_c.get_owner();
            edge.put("up", up.id() + ":" + up_c.id());

            if (up_c.get_peer()) {
                const C &down_c = *up_c.get_peer();
                const V &down   = down_c.get_owner();
                edge.put("down", down.id() + ":" + down_c.id());
            }
            elist.push_back(ptree::value_type("", edge));
        }
    }
    // 2.2. Finally add vertices and connectors to property tree.
    data.add_child("edges", elist);

    return data;
}

template <class V, class C>
void TreeGraph<V, C>::connect(V &parent, std::string parent_link, V &child, std::string child_link)
{
    C &pconn = parent.add_downlink(parent_link);
    C &cconn = child.add_uplink(child_link);

    pconn.set_peer(cconn);
    cconn.set_peer(pconn);

    cconn.level(pconn.level() + 1);
}

template <class V, class C>
std::list<std::pair<V &, C &>> TreeGraph<V, C>::get_path(V &n0, std::string n0l, V &n1, std::string n1l)
{
    /*
          For each node, get up one level until you find a common
          level or hit root.
         */

    boost::optional<C &> n0link = n0.get_uplink(n0l);
    boost::optional<C &> n1link = n1.get_uplink(n1l);

    std::list<std::pair<V &, C &>> n0path, n1path;

    if (!(n0link && n1link))
        return n0path;

    // cout << n0.id() << ":" << n0l << " is on level " << n0link->level() << endl;
    // cout << n1.id() << ":" << n1l << " is on level " << n1link->level() << endl;

    n0path.push_back(std::pair<V &, C &>(n0, *n0link));
    n1path.push_back(std::pair<V &, C &>(n1, *n1link));

    if (n0link->level() > n1link->level()) {
        // go up from n0 until you arrived at the same level as n1
    } else if (n1link->level() > n0link->level()) {
        // go up from n1 until you arrived at the same level as n0
    }

    for (int l = n0link->level(); l != 0; l--) {
        boost::optional<C &> p0link, p1link;

        p0link = n0link->get_peer();
        p1link = n1link->get_peer();

        if (p0link && p1link) {
            n0path.push_back(std::pair<V &, C &>(p0link->get_owner(), *p0link));
            n1path.push_back(std::pair<V &, C &>(p1link->get_owner(), *p1link));

            // if (p0link->get_owner().id() == p1link->get_owner().id())
            //     cout << "Found common ancestor " << p0link->get_owner().id() << endl;
        }
    }

    // for (auto const& step : n0path) {
    //     cout << "-> " << step.first.id() << ":" << step.second.id() << endl;
    // }

    // for (auto const& step : n1path) {
    //     cout << "-> " << step.first.id() << ":" << step.second.id() << endl;
    // }

    n0path.insert(n0path.end(), n1path.begin(), n1path.end());
    return n0path;
}
} // namespace graph_simple

#endif
