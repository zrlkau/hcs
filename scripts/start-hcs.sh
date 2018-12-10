#!/bin/bash
OPTIONS=m:c:p:l:
LONGOPTIONS=mode:,cluster:,perfdbdir:,interference-mode:,io-task-weight:,io-task-io-weight:,compute-task-weight:,compute-task-io-weight:,level-weight:,log:,loglevel:,rsf:

INTERFERENCE_MODE=0
IO_TASK_WEIGHT=0
IO_TASK_IO_WEIGHT=0
CMP_TASK_WEIGHT=0
CMP_TASK_IO_WEIGHT=0
LEVEL_WEIGHT=0
MODE=normal
LOGFILE="/tmp/hcs.log"
PIDFILE="/tmp/hcs.pid"
LOGLEVEL="info"
RSF=1.0

PREFIX=/opt/hcs
CLUSTER=$PREFIX/data/clusters/example.json
PERFDBDIR=$PREFIX/data/perf

PARSED=$(getopt --options=$OPTIONS --longoptions=$LONGOPTIONS --name "$0" -- "$@")
eval set -- "$PARSED"
while true; do
    case "$1" in
	-m|--mode)
	    MODE="$2"
	    shift 2
	    ;;
	-l|--log)
	    LOGFILE="$2"
	    shift 2
	    ;;
	--loglevel)
	    LOGLEVEL="$2"
	    shift 2
	    ;;
	--cluster)
	    CLUSTER="$2"
	    shift 2
	    ;;
	--perfdbdir)
	    PERFDBDIR="$2"
	    shift 2
	    ;;
	--interference-mode)
	    INTERFERENCE_MODE="$2"
	    shift 2
	    ;;
	--io-task-weight)
	    IO_TASK_WEIGHT="$2"
	    shift 2
	    ;;
	--io-task-io-weight)
	    IO_TASK_IO_WEIGHT="$2"
	    shift 2
	    ;;
	--compute-task-weight)
	    CMP_TASK_WEIGHT="$2"
	    shift 2
	    ;;
	--compute-task-io-weight)
	    CMP_TASK_IO_WEIGHT="$2"
	    shift 2
	    ;;
	--level-weight)
	    LEVEL_WEIGHT="$2"
	    shift 2
	    ;;
	--rsf)
	    RSF="$2"
	    shift 2
	    ;;
	--)
	    shift
	    break
	    ;;
	*)
	    echo "error"
	    exit 3
	    ;;
    esac
done


CMDLINE="$(dirname $0)/../bin/hcs --cluster=$CLUSTER --perfdb=$PERFDBDIR --io_task_weight=$IO_TASK_WEIGHT --io_task_io_weight=$IO_TASK_IO_WEIGHT --cmp_task_weight=$CMP_TASK_WEIGHT --cmp_task_io_weight=$CMP_TASK_IO_WEIGHT --interference_mode=$INTERFERENCE_MODE --lweight=$LEVEL_WEIGHT --rsf=$RSF"

set -e
make -j -C $(dirname $0)/../build

case "$MODE" in
    normal)
	echo "$(date) [II] Starting HCS scheduler"
	$CMDLINE --log=info &> $LOGFILE &
	echo $! > $PIDFILE
	;;
    debug)
	echo "$(date) [II] Starting HCS scheduler in debug mode"
	gdb -ex run -ex bt -ex q --args $CMDLINE --log=$LOGLEVEL &> $LOGFILE &
	echo $! > $PIDFILE
	;;
    *)
	echo "Error: unknown mode $MODE"
	exit -1
	;;
esac
