#!/bin/bash

SESSION=kvcluster_unavailable
MANAGER_PORT=3666
P2P_PORT=3667
KV_START_PORT=3700
P2P_START_PORT=3800
N_PARTS=2
SERVER_RF=3

# ----------------------
# Cleanup Phase
# ----------------------

echo "🧹 Cleaning up old processes..."

pkill -f kvmanager 2>/dev/null
pkill -f kvserver 2>/dev/null

USED_PORTS=($MANAGER_PORT $P2P_PORT)
for ((i = 0; i < N_PARTS * SERVER_RF; i++)); do
  USED_PORTS+=($((KV_START_PORT + i)))
  USED_PORTS+=($((P2P_START_PORT + i)))
done

for port in "${USED_PORTS[@]}"; do
  pid=$(lsof -ti tcp:$port)
  if [[ ! -z "$pid" ]]; then
    kill -9 $pid
  fi
done

if tmux has-session -t $SESSION 2>/dev/null; then
  echo "Killing old tmux session: $SESSION"
  tmux kill-session -t $SESSION
fi

echo "Clean slate ready."

# ----------------------
# 🚀 Launch cluster in tmux
# ----------------------

SERVER_ADDRS=()
for ((i = 0; i < N_PARTS * SERVER_RF; i++)); do
  SERVER_ADDRS+=("127.0.0.1:$((KV_START_PORT + i))")
done
ADDR_STRING=$(IFS=, ; echo "${SERVER_ADDRS[*]}")

echo "🎬 Starting tmux session: $SESSION"
tmux new-session -d -s $SESSION -n manager \
  "cargo run --bin kvmanager -- \
  --replica_id 0 \
  --man_listen 127.0.0.1:$MANAGER_PORT \
  --p2p_listen 127.0.0.1:$P2P_PORT \
  --peer_addrs none \
  --server_rf $SERVER_RF \
  --server_addrs $ADDR_STRING \
  --backer_path ./manager_backer; read"

# Start servers
for ((part = 0; part < N_PARTS; part++)); do
  for ((rep = 0; rep < SERVER_RF; rep++)); do
    idx=$((part * SERVER_RF + rep))
    api_port=$((KV_START_PORT + idx))
    p2p_port=$((P2P_START_PORT + idx))

    peer_addrs=()
    for ((r = 0; r < SERVER_RF; r++)); do
      if [[ $r -ne $rep ]]; then
        peer_addrs+=("127.0.0.1:$((P2P_START_PORT + part * SERVER_RF + r))")
      fi
    done
    peer_string=$(IFS=, ; echo "${peer_addrs[*]}")
    backer_path="./backer.p${part}.r${rep}"
    WINDOW="s${part}${rep}"

    tmux new-window -t $SESSION -n $WINDOW \
      "cargo run --bin kvserver -- \
      --partition_id $part \
      --replica_id $rep \
      --manager_addrs 127.0.0.1:$MANAGER_PORT \
      --api_listen 127.0.0.1:$api_port \
      --p2p_listen 127.0.0.1:$p2p_port \
      --peer_addrs $peer_string \
      --backer_path $backer_path; read"
  done
done

# ----------------------
# Fail more than f replicas in a partition
# ----------------------
echo ""
read -p "Pick a partition to crash (> f replicas) [0 or 1]: " target_part
read -p "Which replica IDs to crash in partition $target_part (space-separated, e.g., '0 2'): " -a CRASHED

for rep_id in "${CRASHED[@]}"; do
  WIN="s${target_part}${rep_id}"
  echo "Killing replica $rep_id in partition $target_part (window $WIN)..."
  tmux send-keys -t $SESSION:$WIN C-c
  sleep 0.3
  tmux kill-window -t $SESSION:$WIN
done

echo ""
echo "Partition $target_part is now below quorum."
echo "Client requests to keys in that partition should now hang or timeout."
echo ""

# ----------------------
# ✅ Wrap up
# ----------------------
echo "Cluster running in tmux session '$SESSION'"
echo "Attach using:"
echo "  tmux attach -t $SESSION"
echo ""
echo "Then in a new terminal run the client:"
echo "  cargo run --bin kvclient -- --manager_addrs 127.0.0.1:$MANAGER_PORT --verbose"
