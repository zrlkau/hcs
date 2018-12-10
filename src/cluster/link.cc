// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "helper/classes.h"
#include "helper/logger.h"
#include "helper/types.h"

#include "cluster/link.h"
#include "graph/connector-tree.h"

using boost::property_tree::ptree;

Link::Link(Node &owner, TreeConnectorDir dir) : TreeConnector<Node, Link>(owner, dir)
{
    init();
}

Link::Link(Node &owner, const ptree &data) : TreeConnector<Node, Link>(owner, data)
{
    init();

    this->set_type(string_to_type(data.get<std::string>("type")));
    this->set_capacity(data.get<size_t>("capacity"));
    this->set_cost(data.get<size_t>("cost"));

    logInfo(5) << "Link capacity is " << this->get_capacity();
}

void Link::init()
{
    this->type     = LinkType::unknown;
    this->capacity = -1;
    this->cost     = -1;
}

ptree Link::store() const
{
    ptree data = TreeConnector::store();

    data.add("type", type_to_string(type));
    data.add("cost", get_cost());
    data.add("capacity", get_capacity());

    return data;
}

LinkType Link::string_to_type(std::string type)
{
    if (type == "local")
        return LinkType::local;
    else if (type == "interconnect")
        return LinkType::interconnect;
    return LinkType::unknown;
}

std::string Link::type_to_string(LinkType type)
{
    switch (type) {
    case LinkType::local:
        return "local";
    case LinkType::interconnect:
        return "interconnect";
    default:
        return "unknown";
    }
}
