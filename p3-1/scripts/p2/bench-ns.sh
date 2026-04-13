#!/bin/bash

set -e  # Exit immediately if any command fails
set -x  # Print executed commands for debugging

# Build the project
just p2 build

# Define constants
MANAGER_PORT="3666"
MANAGER_ADDR="127.0.0.1:$MANAGER_PORT"
N_CLIENT=10

# Choose the number of servers (change this value to 3 or 5)
N_SERVER=5  # Set to 3 or 5 based on your requirement

# Define server ports dynamically
SERVERS=""
for (( i=0; i<N_SERVER; i++ )); do
    PORT=$((3777 + i))
    SERVERS+="127.0.0.1:$PORT,"
done
SERVERS=${SERVERS::-1}  # Remove trailing comma

# Run benchmarks for workloads a-f
for WORKLOAD in {a..f}; do
    # Remove any previous data files
    rm -rf ./backer.*
    sleep 1

    # Define log files
    MANAGER_LOG="manager_${N_CLIENT}_${WORKLOAD}_${N_SERVER}.log"
    BENCH_LOG="bench_${N_CLIENT}_${WORKLOAD}_${N_SERVER}.log"

    # Start Manager
    echo "Starting Manager... just p2 manager $MANAGER_PORT $SERVERS"
    just p2 manager "$MANAGER_PORT" "$SERVERS" > "$MANAGER_LOG" 2>&1 &
    MANAGER_PID=$!

    # Start Servers
    declare -A SERVER_PIDS
    for (( i=0; i<N_SERVER; i++ )); do
        PORT=$((3777 + i))
        SERVER_LOG="server${i}_${N_CLIENT}_${WORKLOAD}_${N_SERVER}.log"

        echo "Starting Server $i on port $PORT..."
        just p2 server "$i" "$MANAGER_ADDR" "$PORT" "./backer.s$i" > "$SERVER_LOG" 2>&1 &
        SERVER_PIDS[$i]=$!
    done

    # Allow servers to stabilize
    sleep 3

    # Run benchmark and save output
    echo "Running: just p2::bench $N_CLIENT $WORKLOAD $N_SERVER $MANAGER_PORT"
    just p2::bench "$N_CLIENT" "$WORKLOAD" "$N_SERVER" "$MANAGER_PORT" > "$BENCH_LOG" 2>&1

    sleep 3

    # Cleanup: Check if Manager and Servers are alive before killing
    echo "Checking if Manager ($MANAGER_PID) is still running..."
    if kill -0 $MANAGER_PID 2>/dev/null; then
        echo "Stopping Manager ($MANAGER_PID)..."
        kill -TERM $MANAGER_PID 2>/dev/null || true
        sleep 2
        kill -9 $MANAGER_PID 2>/dev/null || true
    else
        echo "Manager is not running."
    fi

    for (( i=0; i<N_SERVER; i++ )); do
        SERVER_PID=${SERVER_PIDS[$i]}
        echo "Checking if Server $i ($SERVER_PID) is still running..."
        
        if kill -0 $SERVER_PID 2>/dev/null; then
            echo "Stopping Server $i ($SERVER_PID)..."
            kill -TERM $SERVER_PID 2>/dev/null || true
            sleep 2
            kill -9 $SERVER_PID 2>/dev/null || true
        else
            echo "Server $i is not running."
        fi
    done

    # Ensure processes are properly cleaned up
    wait $MANAGER_PID 2>/dev/null || true
    for (( i=0; i<N_SERVER; i++ )); do
        wait ${SERVER_PIDS[$i]} 2>/dev/null || true
    done

done

echo "All benchmarks completed successfully!"
