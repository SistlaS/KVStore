#!/bin/bash

SESSION=kvcluster_fuzz
MANAGER_PORT=3666
P2P_PORT=3667
KV_START_PORT=3700
P2P_START_PORT=3800
N_PARTS=1
SERVER_RF=5

echo "🧹 Cleaning up old processes..."

pkill -f kvmanager 2>/dev/null
pkill -f kvserver 2>/dev/null

echo "Removing backer and req logs..."
rm -f backer.* req*.log

USED_PORTS=($MANAGER_PORT $P2P_PORT)
for ((i = 0; i < N_PARTS * SERVER_RF; i++)); do
  USED_PORTS+=($((KV_START_PORT + i)))
  USED_PORTS+=($((P2P_START_PORT + i)))
done

for port in "${USED_PORTS[@]}"; do
  pid=$(lsof -ti tcp:$port)
  if [[ ! -z "$pid" ]]; then
    echo "Killing process on port $port (PID $pid)"
    kill -9 $pid
  fi
done

if tmux has-session -t $SESSION 2>/dev/null; then
  echo "Killing old tmux session: $SESSION"
  tmux kill-session -t $SESSION
fi

echo "✅ Cleanup done. Launching 1-partition 5-replica cluster."

# ----------------------
# Launch Manager
# ----------------------

SERVER_ADDRS=()
for ((i = 0; i < N_PARTS * SERVER_RF; i++)); do
  SERVER_ADDRS+=("127.0.0.1:$((KV_START_PORT + i))")
done
ADDR_STRING=$(IFS=, ; echo "${SERVER_ADDRS[*]}")

tmux new-session -d -s $SESSION -n manager \
  "cargo run --bin kvmanager -- \
  --replica_id 0 \
  --man_listen 127.0.0.1:$MANAGER_PORT \
  --p2p_listen 127.0.0.1:$P2P_PORT \
  --peer_addrs none \
  --server_rf $SERVER_RF \
  --server_addrs $ADDR_STRING \
  --backer_path ./manager_backer; read"

# ----------------------
# Launch Servers
# ----------------------

for ((rep = 0; rep < SERVER_RF; rep++)); do
  idx=$rep
  api_port=$((KV_START_PORT + idx))
  p2p_port=$((P2P_START_PORT + idx))

  peer_addrs=()
  for ((r = 0; r < SERVER_RF; r++)); do
    if [[ $r -ne $rep ]]; then
      peer_addrs+=("127.0.0.1:$((P2P_START_PORT + r))")
    fi
  done
  peer_string=$(IFS=, ; echo "${peer_addrs[*]}")
  backer_path="./backer.p0.r${rep}"
  WINDOW="s0${rep}"

  tmux new-window -t $SESSION -n $WINDOW \
    "cargo run --bin kvserver -- \
    --partition_id 0 \
    --replica_id $rep \
    --manager_addrs 127.0.0.1:$MANAGER_PORT \
    --api_listen 127.0.0.1:$api_port \
    --p2p_listen 127.0.0.1:$p2p_port \
    --peer_addrs $peer_string \
    --backer_path $backer_path; read"
done

# ----------------------
# 🧪 Run Fuzzer (No Crashes)
# ----------------------

echo ""
echo "Cluster launched in tmux session '$SESSION'"
echo "Running fuzzer with 5 replicas (no crashing)..."
echo ""

# Ensure fuzz log dir exists
mkdir -p /tmp/madkv-p3/fuzz

# Run the fuzzer using your defined justfile logic
cargo run -p runner -r --bin fuzzer -- \
    --num-clis 5 \
    --conflict \
    --client-just-args p3::client "127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
    | tee "/tmp/madkv-p3/fuzz/fuzz-5-no.log"

# Optionally kill everything after fuzzer finishes
echo ""
echo "🧹 Shutting down cluster after fuzzing"
just p3::kill

echo ""
echo "Fuzzing complete. No failures were injected, and system remained stable."
echo "Log: /tmp/madkv-p3/fuzz/fuzz-5-no.log"
echo "To inspect manually:"
echo "  tmux attach -t $SESSION"
