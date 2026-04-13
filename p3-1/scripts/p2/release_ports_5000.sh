#!/bin/bash

# Define the port range
START_PORT=5000
END_PORT=5030

echo "Killing processes listening on ports ${START_PORT}-${END_PORT}..."

# Loop through each port in the range
for (( port=$START_PORT; port<=$END_PORT; port++ ))
do
    # Find the PID of the process listening on the port
    pid=$(lsof -ti tcp:$port)

    if [ -n "$pid" ]; then
        echo "Killing process $pid listening on port $port"
        kill -9 $pid
    else
        echo "No process found on port $port"
    fi
done

echo "Done."
