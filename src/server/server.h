// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef server_server_h
#define server_server_h

#include "cluster/cluster.h"
#include "common.h"
#include <served/served.hpp>

#include <map>
#include <thread>

class Server // : public http_resource
{
  public:
    Server(Engine *engine, Cluster *cluster, ResourceManager *resourcemanager);
    static void run(Server *server);

    void event(Event *ev);

  protected:
    void run();

    SETGET(Cluster *, cluster);

    void appsHandler(served::response &res, const served::request &req);
    void jobsHandler(served::response &res, const served::request &req);
    void stagesHandler(served::response &res, const served::request &req);
    void tasksHandler(served::response &res, const served::request &req);
    void executorsHandler(served::response &res, const served::request &req);
    void workersHandler(served::response &res, const served::request &req);
    void signalsHandler(served::response &res, const served::request &req);

    static ptree parse(const served::request &req);

  private:
    Cluster *            _cluster;
    served::net::server *_server;

    std::map<App *, std::thread *> schedulers;
};
#endif
