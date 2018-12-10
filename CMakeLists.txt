# Specify the minimum version for CMake
cmake_minimum_required(VERSION 3.0)

# Project's name
project(hcs)


option(USE_CATBOOST "1 = enable catboost, 0 = disable catboost (default=1)" 0)
# Set the output folder where your program will be created
set(PROJECT_SOURCE_DIR ${CMAKE_SOURCE_DIR}/src)
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/bin)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(CMAKE_CXX_COMPILER clang++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_LOG_DYN_LINK -Wno-comment -Wno-deprecated-declarations -Werror -Wall -Wno-unused-variable -Wuninitialized -Wconditional-uninitialized -Wsometimes-uninitialized -Wno-error=comment -Wno-error=sign-compare -Dhcs_debug=0")
#set(CMAKE_CXX_COMPILER g++)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_LOG_DYN_LINK -Wno-comment -Wno-deprecated-declarations -Werror -Wall -Wno-unused-variable -Wuninitialized -Wmaybe-uninitialized -Wno-error=comment -Wno-error=sign-compare -Dhcs_debug=0")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")
#set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pg")

# Fast
#set(CMAKE_BUILD_TYPE Release)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -ffast-math -funroll-loops -march=native")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DWITHGPERFTOOLS")

# Debug
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -ffast-math -funroll-loops -march=native")

# The following folder will be included
#include_directories("/usr/include/coin")
include_directories("${PROJECT_SOURCE_DIR}/../deps/served/include")
link_directories("${PROJECT_SOURCE_DIR}/../deps/served/lib")

include_directories("${PROJECT_SOURCE_DIR}")

find_package (Boost 1.58.0 REQUIRED COMPONENTS system program_options log serialization)

link_libraries(boost_system boost_program_options pthread boost_log served boost_serialization)# profiler)
if (${USE_CATBOOST})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_CATBOOST")
  include_directories("${PROJECT_SOURCE_DIR}/../deps/catboost/include")
  link_directories("${PROJECT_SOURCE_DIR}/../deps/catboost/lib")
  link_libraries(catboostmodel)
endif(${USE_CATBOOST})
  
add_compile_options("-std=c++14")
add_executable(hcs  ${PROJECT_SOURCE_DIR}/main.cc
                    ${PROJECT_SOURCE_DIR}/common.cc
                    ${PROJECT_SOURCE_DIR}/helper/config.cc
                    ${PROJECT_SOURCE_DIR}/helper/matrix.cc
		    ${PROJECT_SOURCE_DIR}/helper/operators.cc
		    ${PROJECT_SOURCE_DIR}/helper/time.cc
		    ${PROJECT_SOURCE_DIR}/helper/random.cc
		    ${PROJECT_SOURCE_DIR}/helper/mutex.cc
		    ${PROJECT_SOURCE_DIR}/server/server.cc
                    ${PROJECT_SOURCE_DIR}/resourcemanager/resourcemanager.cc
                    ${PROJECT_SOURCE_DIR}/resourcemanager/resource.cc
                    ${PROJECT_SOURCE_DIR}/resourcemanager/executor.cc
                    ${PROJECT_SOURCE_DIR}/resourcemanager/worker.cc
                    ${PROJECT_SOURCE_DIR}/resourcemanager/workerpool.cc
                    ${PROJECT_SOURCE_DIR}/resourcemanager/resourcepool.cc
                    ${PROJECT_SOURCE_DIR}/resourcemanager/load.cc
                    ${PROJECT_SOURCE_DIR}/event/engine.cc
                    ${PROJECT_SOURCE_DIR}/event/event.cc
                    ${PROJECT_SOURCE_DIR}/scheduler/oracle.cc
                    ${PROJECT_SOURCE_DIR}/scheduler/schedule.cc
                    ${PROJECT_SOURCE_DIR}/scheduler/schedulerbase.cc
                    ${PROJECT_SOURCE_DIR}/scheduler/heteroscheduler.cc
                    ${PROJECT_SOURCE_DIR}/app/io.cc
                    ${PROJECT_SOURCE_DIR}/app/task.cc
                    ${PROJECT_SOURCE_DIR}/app/stage.cc
                    ${PROJECT_SOURCE_DIR}/app/app.cc
                    ${PROJECT_SOURCE_DIR}/graph/graph-tree.cc
                    ${PROJECT_SOURCE_DIR}/cluster/cluster.cc
                    ${PROJECT_SOURCE_DIR}/cluster/node.cc
                    ${PROJECT_SOURCE_DIR}/cluster/link.cc)
