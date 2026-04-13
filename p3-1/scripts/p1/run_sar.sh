#!/bin/bash

# This script assumes server and client are running on two different machines,
# and client has ssh access to the server

# Usage
if [ $# -ne 4 ]; then
    echo "Usage: $0 <server_ip> <server_port> <server_user> <server_dirpath>"
    echo "Example: $0 128.105.144.32 3333 fariha ~/cs739-madkv/"
    exit 1
fi

SERVER_IP=$1
SERVER_PORT=$2
SERVER_USER=$3
SERVER_DIR=$4
ADDR="$SERVER_IP:$SERVER_PORT"

echo "Curr dir: $(pwd)"

# Time-stamped output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="bench_results/${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"

# Ensure server-side log directory exists
SERVER_LOG_DIR="${SERVER_DIR}/perf_logs/${TIMESTAMP}"
ssh "$SERVER_USER@$SERVER_IP" "mkdir -p $SERVER_LOG_DIR"

# Ensure sysstat (sar) is installed on the server
echo "Checking sysstat installation on server..."
ssh "$SERVER_USER@$SERVER_IP" <<EOF
    if ! command -v sar &> /dev/null; then
        echo "sysstat not found, installing..."
        sudo apt update && sudo apt install sysstat -y || sudo yum install sysstat -y
    fi
EOF

# Vary these
N_CLIENTS_LIST=(1 10 25 40 55 70 85)
WORKLOAD_NAMES=("a" "c" "e")

# Function to restart the server
restart_server() {
    echo "Restarting server ..."
    ssh -q "$SERVER_USER@$SERVER_IP" <<EOF
        cd $SERVER_DIR
        just p1 kill
        sleep 2
        nohup just p1 server > server.log 2>&1 &
        disown
        sleep 2
EOF
}

run_ycsb() {
    local n_clients="$1"
    local workload="$2"
    local OUTPUT_FILE="${OUTPUT_DIR}/bench_${n_clients}_${workload}.txt"
    local SAR_FILE="${SERVER_LOG_DIR}/sar_${n_clients}_${workload}.txt"
    local EXP_START_TIME=$(date +%s)

    restart_server

    echo "Starting sar on server..."
    ssh "$SERVER_USER@$SERVER_IP" "nohup sar -u -r 1 > $SAR_FILE 2>&1 &"

    echo "Running: just p1::bench $n_clients $workload $ADDR"
    just p1::bench "$n_clients" "$workload" "$ADDR" | tee "$OUTPUT_FILE"

    echo "Stopping sar..."
    ssh "$SERVER_USER@$SERVER_IP" "pkill -f 'sar -u -r 1'"
    sleep 5  # Ensure sar flushes logs

    EXP_END_TIME=$(date +%s)
    EXP_RUNTIME=$((EXP_END_TIME - EXP_START_TIME))
    echo "Experiment runtime: ${EXP_RUNTIME} seconds" | tee -a "$OUTPUT_FILE"
    echo "Results saved to $OUTPUT_FILE and $SAR_FILE (on the server)"
}

START_TIME=$(date +%s)

# Running missing workloads
run_ycsb 1 b
run_ycsb 1 d
run_ycsb 1 f

# Running all other workloads
for n_clients in "${N_CLIENTS_LIST[@]}"; do
    for workload in "${WORKLOAD_NAMES[@]}"; do
        run_ycsb "$n_clients" "$workload"
    done
done

END_TIME=$(date +%s)
TOTAL_RUNTIME=$((END_TIME - START_TIME))
echo "All benchmarks completed!"
echo "Total runtime: ${TOTAL_RUNTIME} seconds" | tee -a "${OUTPUT_DIR}/benchmark_summary.txt"

# Kill server
ssh "$SERVER_USER@$SERVER_IP" <<EOF
            cd $SERVER_DIR
            just p1 kill
            sleep 2
EOF

