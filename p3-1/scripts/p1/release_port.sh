#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <port>"
    exit 1
fi

PORT=$1

# Find the process ID (PID) using the specified port
PID=$(lsof -i tcp:$PORT -t)

if [ -z "$PID" ]; then
    echo "No process found running on port $PORT"
    exit 1
fi

# Kill the process
kill -9 $PID

echo "Process on port $PORT (PID: $PID) has been terminated."
