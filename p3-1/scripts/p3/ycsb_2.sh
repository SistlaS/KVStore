#!/bin/bash

set -e  # Exit immediately if any command fails
set -x  # Print executed commands for debugging

# Build the project
just p3 build

LOG_DIR="log_ycsb_2"
mkdir -p "$LOG_DIR"

# Define constants
MANAGER_PORT="3666"
MANAGER_ADDR="127.0.0.1:$MANAGER_PORT"
MANAGER_P2P_PORT="3606"
MANAGER_PEERS="127.0.0.1:3607,127.0.0.1:3608" # this is hardcoded for now
OTHER_MANAGER_ADDR="127.0.0.1:3667,127.0.0.1:3668"
MANAGER_ALL_ADDRS=$MANAGER_ADDR,$OTHER_MANAGER_ADDR

N_PARTITION=1 # Number of partitions, set to 1 for simplicity
REP_FACTOR=5 # Choose server replication factor (1, 3 or 5)
N_SERVER=$((REP_FACTOR * N_PARTITION))


# Define server ports dynamically
SERVER_ADDRS=""
declare -A SERVER_PORT_ARRAY
declare -A SERVER_P2P_PORT_ARRAY
for (( i=0; i<N_SERVER; i++ )); do
    PORT=$((4666 + i))
    P2P_PORT=$((4777 + i))
    ADDR="127.0.0.1:$PORT"

    SERVER_ADDRS+="$ADDR,"

    SERVER_PORT_ARRAY[$i]=$PORT
    SERVER_P2P_PORT_ARRAY[$i]=$P2P_PORT
done

# Remove trailing comma if it exists
SERVER_ADDRS=${SERVER_ADDRS%,}  # Remove trailing comma
# SERVER_ADDRS=${SERVER_ADDRS::-1}  # Remove trailing comma


WORKLOAD="a" # Default workload for initial run
# scaling number of clients fro 1 to 30, with 5 increments
for N_CLIENT in {1..30..5}; do
    # Remove any previous data files
    rm -rf ./backer.*
    sleep 1

    EXP_TAG="${N_CLIENT}_${WORKLOAD}_${N_PARTITION}_${REP_FACTOR}"

    # Define log files
    MANAGER_LOG="${LOG_DIR}/manager_${EXP_TAG}.log"
    BENCH_LOG="${LOG_DIR}/bench_${EXP_TAG}.log"

    # Start Manager
    echo "Starting Manager... just p3 manager $MANAGER_PORT $SERVER_ADDRS"
    just p3 manager "0" "$MANAGER_PORT" "$MANAGER_P2P_PORT" "$MANAGER_PEERS" \
        "$REP_FACTOR" "$SERVER_ADDRS" "./backer.m.0" > "$MANAGER_LOG" 2>&1 &
    MANAGER_PID=$!

    # Start Servers
    declare -A SERVER_PIDS

    for (( i=0; i<N_PARTITION; i++ )); do
        for (( j=0; j<REP_FACTOR; j++ )); do
        

            PEEP_SERVER_ADDRS=""
            for (( k=0; k<REP_FACTOR; k++ )); do
                if [ $k -ne $j ]; then
                    PEEP_SERVER_ADDRS+="127.0.0.1:${SERVER_P2P_PORT_ARRAY[$((i * REP_FACTOR + k))]},"
                fi
            done

            # Remove trailing comma if it exists
            PEEP_SERVER_ADDRS=${PEEP_SERVER_ADDRS%,}  # Remove trailing comma
            # PEEP_SERVER_ADDRS=${PEEP_SERVER_ADDRS::-1}  # Remove trailing comma

            if [ -z "$PEEP_SERVER_ADDRS" ]; then
                PEEP_SERVER_ADDRS="none"
            fi

            SERVER_IDX=$((i * REP_FACTOR + j))
            PORT=${SERVER_PORT_ARRAY[$SERVER_IDX]}
            P2P_PORT=${SERVER_P2P_PORT_ARRAY[$SERVER_IDX]}


            SERVER_LOG="${LOG_DIR}/server_${EXP_TAG}_${i}.${j}.log"

            echo "Starting Server $i.$j on port $PORT..."

            just p3 server "$i" "$j" "$MANAGER_ALL_ADDRS" \
                "$PORT" "$P2P_PORT" "$PEEP_SERVER_ADDRS" \
                "./backer.s$i.$j" > "$SERVER_LOG" 2>&1 &
            
            SERVER_PIDS[$SERVER_IDX]=$!

        done
    done

    # Allow servers to stabilize
    sleep 3

    # Run benchmark and save output
    echo "Running: just p3::bench $N_CLIENT $WORKLOAD $N_SERVER $MANAGER_PORT"
    just p3::bench "$N_CLIENT" "$WORKLOAD" \
        "$REP_FACTOR" "$MANAGER_ALL_ADDRS" > "$BENCH_LOG" 2>&1

    # Allow some time for the benchmark to complete
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
