Overview
=====================================
This is the source code of the Heterogeneous Cluster Scheduler (HCS). The code in this repository has been used in the paper __"Mira: Sharing Resources for Distributed Analytics at Small Timescales"__, published at the __2018 IEEE International Conference on Big Data__.

HCS has been integrated into Apache Spark. A modified version of Spark with HCS integration can be found at https://github.ibm.com/KAU/hcs-spark.

Disclaimer
==========
This is not production quality code and contains many bugs. It is solely provided such that other researchers can study and examine its concepts and reproduce the results published in the above mentioned paper. It is provided as is, under a BSD-style license (see LICENSE file). 

Build Instructions
===================
1. Download source code
    ```
    git clone git@github.com:zrlkau/hcs.git
    cd hcs
    ```
2. Download and build `served` RESTful api library
    ```
    mkdir deps
    cd deps
    git clone git@github.com:meltwater/served.git
    cd served
    mkdir build
    cmake -DCMAKE_INSTALL_PREFIX:PATH=../
    make && make install
    cd ../
    ```
3. Build HCS
    ```
    cd build
    cmake ../
    make
    ```
4. Download and build Spark with HCS integration
    ```
    git clone https://github.ibm.com/KAU/hcs-spark
    ```
    Build as usual, according to the Spark instructions.

Configuration and Usage
===================
1. Create a cluster definition file. Use data/cluster/example.json as template
2. Start hcs with scripts/start-hcs.sh, stop it with scripts/stop-hcs.sh
3. Start your Spark application only after HCS is up and running.

Issues
======
In case of issues, please feel free to send me an email (kau@zurich.ibm.com).
