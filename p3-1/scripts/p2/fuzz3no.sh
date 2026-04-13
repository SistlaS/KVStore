#!/bin/bash

set -e  # Exit immediately if any command fails

just p2 build > /dev/null 2>&1
rm -rf ./backer.*

# Log files
MANAGER_LOG="manager.log"
SERVER0_LOG="server0.log"
SERVER1_LOG="server1.log"
SERVER2_LOG="server2.log"
FUZZ_LOG="fuzz.log"

# Start manager and servers in the background, redirect output to log files
echo "Starting Manager..."
just p2 manager 3666 "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779" > "$MANAGER_LOG" 2>&1 &
MANAGER_PID=$!

echo "Starting Server 0..."
just p2 server 0 "127.0.0.1:3666" "3777" "./backer.s0" > "$SERVER0_LOG" 2>&1 &
SERVER0_PID=$!

echo "Starting Server 1..."
just p2 server 1 "127.0.0.1:3666" "3778" "./backer.s1" > "$SERVER1_LOG" 2>&1 &
SERVER1_PID=$!

echo "Starting Server 2..."
just p2 server 2 "127.0.0.1:3666" "3779" "./backer.s2" > "$SERVER2_LOG" 2>&1 &
SERVER2_PID=$!

# Allow servers to stabilize
sleep 3

# Start fuzz test in the background
echo "Starting Fuzz Test..."
just p2::fuzz 3 no > "$FUZZ_LOG" 2>&1 &
FUZZ_PID=$!

sleep 10

# Monitor fuzz test and wait for completion
echo "Waiting for Fuzz Test to complete..."
(
    tail -f "$FUZZ_LOG" &
    TAIL_PID=$!

    while true; do
        if grep -qE "passed|killed" "$FUZZ_LOG"; then
            echo "Fuzz Test completed!"
            break
        fi
        
        # Check if fuzz test process is still running
        if ! kill -0 $FUZZ_PID 2>/dev/null; then
            echo "Fuzz Test process was terminated!"
            break
        fi

        sleep 1
    done

    # Cleanup
    kill -9 $TAIL_PID 2>/dev/null || true
    pkill -P $$  # Kill all background jobs started by this script
    exit 0
) &
wait


