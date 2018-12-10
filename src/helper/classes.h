// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//          Adrian Schuepbach <dri@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef classes_h
#define classes_h
class Oracle;
class OracleCache;

class App;
class AppPartition;
class Stage;
class Task;
class IdleTask;
class DisconnectTask;
enum class TaskState {
    unknown,   // default state
    unready,   // dependencies not met yet
    ready,     // all dependencies met
    scheduled, // resource allocated
    running,   // executing
    finished   // done executing
};
enum class TaskType {
    unknown,
    idle,   // idle task that can be executed if nothing else needs to be executed.
    source, // dummy source task
    sink,   // dummy sink task
    // generic computation
    compute,
    // acceleratable computation (can also run on a
    // CPU).
    cuda,
    opencl,
    openmp,
    mpi,
    // requires an FPGA
    fpga,
    // network I/O
    send,
    receive,
    // storage I/O
    load,
    store,
    disconnect,  // indicates that this executor should disconnect from the driver
    nop
};

class Cluster;
class Node;
enum class NodeType {
    unknown,
    // regular nodes
    regular,
    // network resources (switch)
    network,
    // node groups
    group
};
class NodeClass;
class Link;
enum class LinkType { unknown, local, interconnect };
class Io;
enum class Direction { up, down };

class Worker;
class Executor;
class Resource;
class ResourcePool;
enum class ResourceType;
enum class ResourceState;
class ResourceModel;
class ResourceAllocationEntry;
class ResourceManager;

class SchedulerBase;
class Schedule;
class Load;
class LoadEntry;

class Time;
class Duration;

class Config;
class Engine;
class Event;
#endif
