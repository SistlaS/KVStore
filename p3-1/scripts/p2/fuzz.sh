#!/bin/bash

set -e  # Exit immediately if any command fails

# Usage: ./script.sh <num_servers> <kill_mode>
# num_servers: 3 or 5
# kill_mode: "kill" to terminate specific servers, "no_kill" to keep them running

NUM_SERVERS=$1
KILL_MODE=$2

if [[ "$NUM_SERVERS" != "3" && "$NUM_SERVERS" != "5" ]]; then
    echo "Error: Number of servers must be 3 or 5."
    exit 1
fi

if [[ "$KILL_MODE" != "kill" && "$KILL_MODE" != "no_kill" ]]; then
    echo "Error: Kill mode must be 'kill' or 'no_kill'."
    exit 1
fi

just p2 build
rm -rf ./backer.*

# Log files
MANAGER_LOG="manager.log"
FUZZ_LOG="fuzz.log"

# Server Addresses (Base ports)
BASE_PORT=3777
SERVERS="127.0.0.1:$BASE_PORT"

for ((i=1; i<NUM_SERVERS; i++)); do
    SERVERS+=",127.0.0.1:$((BASE_PORT + i))"
done

# Start manager
echo "Starting Manager..."
just p2 manager 3666 "$SERVERS" > "$MANAGER_LOG" 2>&1 &
MANAGER_PID=$!

# Start servers
declare -A SERVER_PIDS
for ((i=0; i<NUM_SERVERS; i++)); do
    echo "Starting Server $i..."
    just p2 server "$i" "127.0.0.1:3666" "$((BASE_PORT + i))" "./backer.s$i" > "server$i.log" 2>&1 &
    SERVER_PIDS[$i]=$!
done

# Allow servers to stabilize
sleep 3

# Start fuzz test
echo "Starting Fuzz Test..."
just p2::fuzz 3 yes > "$FUZZ_LOG" 2>&1 &
FUZZ_PID=$!

sleep 10
tail -n 2 "$FUZZ_LOG"
sync; printf "\n\n"  # Ensures newline is written

# Kill and restart servers based on arguments
if [[ "$KILL_MODE" == "kill" ]]; then
    if [[ "$NUM_SERVERS" == "3" ]]; then
        echo "Restarting Server 1..."
        kill -9 ${SERVER_PIDS[1]} > /dev/null 2>&1
        sleep 2
        just p2 server 1 "127.0.0.1:3666" "3778" "./backer.s1" > "server1.log" 2>&1 &
        SERVER_PIDS[1]=$!
    elif [[ "$NUM_SERVERS" == "5" ]]; then
        echo "Restarting Server 1 and Server 2..."
        kill -9 ${SERVER_PIDS[1]} ${SERVER_PIDS[2]} > /dev/null 2>&1
        sleep 2
        just p2 server 1 "127.0.0.1:3666" "3778" "./backer.s1" > "server1.log" 2>&1 &
        SERVER_PIDS[1]=$!
        just p2 server 2 "127.0.0.1:3666" "3779" "./backer.s2" > "server2.log" 2>&1 &
        SERVER_PIDS[2]=$!
    fi
fi

sleep 2
tail -n 2 "$FUZZ_LOG"
sync; printf "\n\n"

# Monitor fuzz test and wait for completion
echo "Waiting for Fuzz Test to complete..."
(
    while true; do
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
