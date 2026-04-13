#!/bin/bash

set -e  # Exit immediately if any command fails

just p2 build
rm -rf ./backer.*

# Log files
MANAGER_LOG="manager.log"
SERVER0_LOG="server0.log"
SERVER1_LOG="server1.log"
SERVER2_LOG="server2.log"
SERVER3_LOG="server3.log"
SERVER4_LOG="server4.log"
FUZZ_LOG="fuzz.log"

# Server addresses
SERVERS="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781"

# Start manager
echo "Starting Manager..."
just p2 manager 3666 "$SERVERS" > "$MANAGER_LOG" 2>&1 &
MANAGER_PID=$!

# Start 5 servers in the background
for i in {0..4}; do
    echo "Starting Server $i..."
    just p2 server "$i" "127.0.0.1:3666" "37$((77 + i))" "./backer.s$i" > "server$i.log" 2>&1 &
    eval "SERVER${i}_PID=$!"
done

# Allow servers to stabilize
sleep 3

# Start fuzz test in the background
echo "Starting Fuzz Test..."
just p2::fuzz 5 yes > "$FUZZ_LOG" 2>&1 &
FUZZ_PID=$!

# Wait 3 seconds before restarting Server 1
sleep 10
tail -n 2 "$FUZZ_LOG"
sync; printf "\n\n"  # Ensures newline is written

# Kill and restart Server 1
echo "Restarting Server 1 and 2..."
kill -9 $SERVER1_PID > /dev/null 2>&1
kill -9 $SERVER2_PID > /dev/null 2>&1
sleep 2  # Allow time before restarting
just p2 server 1 "127.0.0.1:3666" "3778" "./backer.s1" > "$SERVER1_LOG" 2>&1 &
SERVER1_PID=$!
just p2 server 2 "127.0.0.1:3666" "3779" "./backer.s2" > "$SERVER2_LOG" 2>&1 &
SERVER2_PID=$!

sleep 2
tail -n 2 "$FUZZ_LOG"
sync; printf "\n\n"  # Ensures newline is written

# Monitor fuzz test and wait for completion
echo "Waiting for Fuzz Test to complete..."
(
    while true; do
        # Check if fuzz test process is still running
        if ! kill -0 $FUZZ_PID 2>/dev/null; then
            tail -n 20 "$FUZZ_LOG"
            echo "Fuzz Test process was terminated!"
            break
        fi
        sleep 1
    done
    pkill -P $$  # Kill all background jobs started by this script
    exit 0
) &
wait
