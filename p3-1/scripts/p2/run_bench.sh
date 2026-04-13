#!/bin/bash

set -e  # Exit on any error and print executed commands

just p2 build > /dev/null 2>&1

MANAGER_PORT="3666"
MANAGER_ADDR="127.0.0.1:$MANAGER_PORT"
OUTPUT_DIR="bench_logs"
mkdir -p $OUTPUT_DIR

# Function to generate server addresses
generate_server_addresses() {
    local N_SERVER=$1
    local ADDRESSES=""
    for (( i=0; i<N_SERVER; i++ )); do
        PORT=$((3777 + i))
        ADDRESSES+="127.0.0.1:$PORT,"
    done
    echo "${ADDRESSES::-1}"  # remove trailing comma
}

# Function to start the manager
start_manager() {
    local MANAGER_LOG=$1
    local SERVER_LIST=$2

    echo "Starting Manager... just p2 manager $MANAGER_PORT $SERVER_LIST"
    just p2 manager "$MANAGER_PORT" "$SERVER_LIST" > "$MANAGER_LOG" 2>&1 &
    echo $!     # return manager PID
}

# Function to start a server
start_server() {
    local ID=$1
    local PORT=$2
    local LOG_FILE=$3

    echo "Starting Server $ID on port $PORT...  just p2 server $ID $MANAGER_ADDR $PORT ./backer.s$ID"
    just p2 server "$ID" "$MANAGER_ADDR" "$PORT" "./backer.s$ID" > "$LOG_FILE" 2>&1 &
    echo $!     # return server PID
}

# Function to stop a process gracefully
stop_process() {
    local PID=$1
    local NAME=$2

    if kill -0 "$PID" 2>/dev/null; then
        echo "Stopping $NAME ($PID)..."
        kill -TERM "$PID" 2>/dev/null || true
        sleep 2
        kill -9 "$PID" 2>/dev/null || true
    else
        echo "$NAME is not running."
    fi
}

# Function to run a benchmark
run_benchmark() {
    local WORKLOAD=$1
    local N_CLIENT=$2
    local N_SERVER=$3

    printf "\n\nBenchmark: $N_CLIENT $WORKLOAD $N_SERVER\n"
    local EXP_START_TIME=$(date +%s)

    local SERVER_LIST
    SERVER_LIST=$(generate_server_addresses "$N_SERVER")

    # Remove previous data
    rm -rf ./backer.*
    sleep 1

    local MANAGER_LOG="${OUTPUT_DIR}/manager_${N_CLIENT}_${WORKLOAD}_${N_SERVER}.log"
    local BENCH_LOG="${OUTPUT_DIR}/bench_${N_CLIENT}_${WORKLOAD}_${N_SERVER}.log"

    # Start manager
    local MANAGER_PID
    MANAGER_PID=$(start_manager "$MANAGER_LOG" "$SERVER_LIST")

    # Start servers
    declare -A SERVER_PIDS
    for (( i=0; i<N_SERVER; i++ )); do
        PORT=$((3777 + i))
        local SERVER_LOG="${OUTPUT_DIR}/server${i}_${N_CLIENT}_${WORKLOAD}_${N_SERVER}.log"
        SERVER_PIDS[$i]=$(start_server "$i" "$PORT" "$SERVER_LOG")
        sleep 1  # Avoid race conditions
    done
    sleep 3

    # Run benchmark
    echo "Running: just p2::bench $N_CLIENT $WORKLOAD $N_SERVER $MANAGER_ADDR"
    just p2::bench "$N_CLIENT" "$WORKLOAD" "$N_SERVER" "$MANAGER_ADDR" > "$BENCH_LOG" 2>&1 || {
        echo "Benchmark failed..."
        cat "$BENCH_LOG"
    }
    sleep 3

    # Stop servers and managers
    stop_process "$MANAGER_PID" "Manager"
    for (( i=0; i<N_SERVER; i++ )); do
        stop_process "${SERVER_PIDS[$i]}" "Server $i"
    done
    wait "$MANAGER_PID" 2>/dev/null || true
    for (( i=0; i<N_SERVER; i++ )); do
        wait "${SERVER_PIDS[$i]}" 2>/dev/null || true
    done

    EXP_END_TIME=$(date +%s)
    EXP_RUNTIME=$((EXP_END_TIME - EXP_START_TIME))
    echo "Runtime: ${EXP_RUNTIME} seconds"

    tail -n 20 "$BENCH_LOG"
}

START_TIME=$(date +%s)

# 10 clients, 1 server
for WORKLOAD in {a..f}; do
    run_benchmark "$WORKLOAD" 10 1
done

# 10 clients, 3 servers
for WORKLOAD in {a..f}; do
    run_benchmark "$WORKLOAD" 10 3
done

# 10 clients, 5 servers
for WORKLOAD in {a..f}; do
    run_benchmark "$WORKLOAD" 10 5
done


WORKLOAD="a"
# n clients, 1 server
run_benchmark "$WORKLOAD" 1 1
run_benchmark "$WORKLOAD" 20 1
run_benchmark "$WORKLOAD" 30 1
# n clients, 5 server
run_benchmark "$WORKLOAD" 1 5
run_benchmark "$WORKLOAD" 20 5
run_benchmark "$WORKLOAD" 30 5

END_TIME=$(date +%s)
TOTAL_RUNTIME=$((END_TIME - START_TIME))
echo "✅ All benchmarks completed successfully!"
echo "Total runtime: ${TOTAL_RUNTIME} seconds"
