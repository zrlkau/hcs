#!/bin/bash
PIDFILE="/tmp/hcs.pid"
PID=$(cat $PIDFILE)
TRY=10

echo -n "$(date) [II] Stopping HCS scheduler (pid $PID)"

kill $PID
while kill $PID &>/dev/null && [ $TRY -gt 0 ]; do
    sleep 1
    TRY=$[$TRY-1]
    echo -n "."
done
kill -9 $PID
echo ""
echo "$(date) [II] Stopped HCS scheduler."

