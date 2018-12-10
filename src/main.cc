// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "cluster/cluster.h"
#include "common.h"
#include "event/engine.h"
#include "resourcemanager/resourcemanager.h"
#include "server/server.h"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <boost/program_options.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace pt = boost::property_tree;
namespace po = boost::program_options;

Config        cfg; // global configuration
Config *const config = &cfg;

Engine *         engine          = NULL;
ResourceManager *resourcemanager = NULL;

int main(int argc, const char **argv)
{
    try {
        std::string cls_filename;
        std::string perfdb_filename;

 	po::options_description global_opts("Global options");
	global_opts.add_options()
	    ("cluster",            po::value<std::string>(), "Path to cluster model")
	    ("perfdb",             po::value<std::string>(), "Path to app/task performance database directory")
	    ("seed",               po::value<uint64_t>(),    "Seed for random number generator(s)")
	    ("log",                po::value<std::string>(), "Log level: trace, debug, info, warning, error, fatal")
	    ("io_task_weight",     po::value<double>(),      "")
	    ("io_task_io_weight",  po::value<double>(),      "")
	    ("cmp_task_weight",    po::value<double>(),      "")
	    ("cmp_task_io_weight", po::value<double>(),      "")
	    ("node_load_weight",   po::value<double>(),      "")
	    ("interference_mode",  po::value<int>(),         "0-6")
	    ("rsf",                po::value<double>(),      "Resource scaling factor (0.0 - 1.0), default = 1.0")
	    ("hostname",           po::value<std::string>(), "Which hostname to listen on")
	    ("port",               po::value<uint>(),        "Which port to listen on")
	    ("lweight",            po::value<int>(),         "Weight of the stage level (distance to final stage).")
	    ("train",                                        "Enable training mode")
	    ("help",                                         "Print this message");

	po::variables_map vm;
        po::parsed_options
            parsed = po::command_line_parser(argc, argv).options(global_opts).allow_unregistered().run();
        po::store(parsed, vm);

        if (vm.count("cluster")) {
            cls_filename = vm["cluster"].as<std::string>();
        } else {
            throw std::invalid_argument("Cluster definition file (--cluster) missing");
        }

        if (vm.count("perfdb")) {
            cfg.perfdbdir(vm["perfdb"].as<std::string>());
        } else {
            throw std::invalid_argument("Performance database directory (--perfdb) missing");
        }

        if (vm.count("seed")) {
            cfg.seed(vm["seed"].as<uint64_t>());
	} else {
            cfg.seed(std::chrono::system_clock::now().time_since_epoch().count());
	}

        if (vm.count("log")) {
            if (vm["log"].as<std::string>() == "trace") {
                std::cout << "Setting output level to trace" << std::endl;
                boost::log::core::get()->set_filter(boost::log::trivial::severity >=
                                                    boost::log::trivial::trace);
            } else if (vm["log"].as<std::string>() == "debug") {
                std::cout << "Setting output level to debug" << std::endl;
                boost::log::core::get()->set_filter(boost::log::trivial::severity >=
                                                    boost::log::trivial::debug);
            } else if (vm["log"].as<std::string>() == "info") {
                std::cout << "Setting output level to info" << std::endl;
                boost::log::core::get()->set_filter(boost::log::trivial::severity >=
						    boost::log::trivial::info);
            } else if (vm["log"].as<std::string>() == "warning") {
                std::cout << "Setting output level to warning" << std::endl;
                boost::log::core::get()->set_filter(boost::log::trivial::severity >=
                                                    boost::log::trivial::warning);
            } else if (vm["log"].as<std::string>() == "error") {
                std::cout << "Setting output level to error" << std::endl;
                boost::log::core::get()->set_filter(boost::log::trivial::severity >=
                                                    boost::log::trivial::error);
            } else if (vm["log"].as<std::string>() == "fatal") {
                std::cout << "Setting output level to fatal" << std::endl;
                boost::log::core::get()->set_filter(boost::log::trivial::severity >=
                                                    boost::log::trivial::fatal);
            }
        }

        if (vm.count("train")) {
            config->setFlag(ConfigSchedulerFlags::training_mode);
        }

        if (vm.count("io_task_weight")) {
            config->set(ConfigVariable::io_task_weight, vm["io_task_weight"].as<double>());
        }

        if (vm.count("io_task_io_weight")) {
            config->set(ConfigVariable::io_task_io_weight, vm["io_task_io_weight"].as<double>());
        }

        if (vm.count("cmp_task_weight")) {
            config->set(ConfigVariable::cmp_task_weight, vm["cmp_task_weight"].as<double>());
        }

        if (vm.count("cmp_task_io_weight")) {
            config->set(ConfigVariable::cmp_task_io_weight, vm["cmp_task_io_weight"].as<double>());
        }

        if (vm.count("node_load_weight")) {
            config->set(ConfigVariable::node_load_weight, vm["node_load_weight"].as<double>());
        }

        if (vm.count("interference_mode")) {
            config->set(ConfigVariable::interference_mode, vm["interference_mode"].as<int>());
        }

        if (vm.count("rsf")) {
            config->set(ConfigVariable::resource_scaling_factor, vm["rsf"].as<double>());
        }

	if (vm.count("hostname")) {
            config->listenHost(vm["hostname"].as<std::string>());
        }
	if (vm.count("port")) {
            config->listenPort(vm["port"].as<uint>());
        }

	if (vm.count("lweight")) {
            config->lweight(vm["lweight"].as<int>());
        }

        pt::ptree  clusterData;

	try {
	    std::ifstream file;
	    file.open(cls_filename);
	    pt::read_json(file, clusterData);
	    file.close();
	} catch (std::exception const &e) {
	    throw(std::invalid_argument("Cannot read read cluster definition file " + cls_filename + " : " + e.what()));
	}

        config->setFlag(ConfigSchedulerFlags::consider_io_time);
        config->setFlag(ConfigSchedulerFlags::consider_io_size);
        //    config->setFlag(ConfigSchedulerFlags::signal_executor_idle);
        //    config->setFlag(ConfigSchedulerFlags::compact_schedule);
        //    config->setFlag(ConfigSchedulerFlags::best_fit);
        config->print();

        Cluster cluster(clusterData);

        Engine  myengine(EngineMode::Real);
        engine  = &myengine; // global object
        ResourceManager myresourcemanager(&cluster);
        resourcemanager = &myresourcemanager; // global object

        Server myserver(&myengine, &cluster, &myresourcemanager);

        std::thread engineThread(static_cast<void (*)(Engine *)>(Engine::run), &myengine);
        std::thread resourcemanagerThread(static_cast<void (*)(ResourceManager *)>(ResourceManager::run), &myresourcemanager);
        std::thread serverThread(static_cast<void (*)(Server *)>(Server::run), &myserver);

        serverThread.join();
        resourcemanagerThread.join();
        engineThread.join();

        fflush(stdout);
        fflush(stderr);
        fclose(stdout);
        fclose(stderr);

        return 0;
    } catch (std::exception const &e) {
        logError << "Error: " << e.what();
        return -1;
    }
}
