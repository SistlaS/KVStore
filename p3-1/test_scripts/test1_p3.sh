#!/bin/bash

SESSION=kvcluster
MANAGER_PORT=3666
P2P_PORT=3667
KV_START_PORT=3700
P2P_START_PORT=3800
N_PARTS=2
SERVER_RF=3

echo "Cleaning up existing processes..."

# Kill previous Rust processes if any
pkill -f kvmanager 2>/dev/null
pkill -f kvserver 2>/dev/null

# Kill anything listening on used ports
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

# Kill existing tmux session
if tmux has-session -t $SESSION 2>/dev/null; then
  echo "Killing existing tmux session: $SESSION"
  tmux kill-session -t $SESSION
fi

echo "✅ Cleanup complete. Launching fresh cluster..."

# Build full server address list for manager
SERVER_ADDRS=()
for ((i = 0; i < N_PARTS * SERVER_RF; i++)); do
  SERVER_ADDRS+=("127.0.0.1:$((KV_START_PORT + i))")
done
ADDR_STRING=$(IFS=, ; echo "${SERVER_ADDRS[*]}")

# Kill existing session if exists
tmux has-session -t $SESSION 2>/dev/null
if [ $? -eq 0 ]; then
  echo "Killing existing tmux session $SESSION..."
  tmux kill-session -t $SESSION
fi

# Start new tmux session
echo "Starting tmux session: $SESSION"
tmux new-session -d -s $SESSION -n manager \
  "cargo run --bin kvmanager -- \
  --replica_id 0 \
  --man_listen 127.0.0.1:$MANAGER_PORT \
  --p2p_listen 127.0.0.1:$P2P_PORT \
  --peer_addrs none \
  --server_rf $SERVER_RF \
  --server_addrs $ADDR_STRING \
  --backer_path ./manager_backer; read"

# Launch servers in separate windows
for ((part = 0; part < N_PARTS; part++)); do
  for ((rep = 0; rep < SERVER_RF; rep++)); do
    idx=$((part * SERVER_RF + rep))
    api_port=$((KV_START_PORT + idx))
    p2p_port=$((P2P_START_PORT + idx))

    # build peer_addrs
    peer_addrs=()
    for ((r = 0; r < SERVER_RF; r++)); do
      if [[ $r -ne $rep ]]; then
        peer_addrs+=("127.0.0.1:$((P2P_START_PORT + part * SERVER_RF + r))")
      fi
    done
    peer_string=$(IFS=, ; echo "${peer_addrs[*]}")
    backer_path="./backer.p${part}.r${rep}"

    WINDOW_NAME="s${part}${rep}"

    tmux new-window -t $SESSION -n $WINDOW_NAME \
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

echo ""
echo "Cluster launched in tmux session '$SESSION'"
echo "Run this to attach:"
echo "  tmux attach -t $SESSION"
echo ""
echo "Then run the client separately with:"
echo "  cargo run --bin kvclient -- --manager_addrs 127.0.0.1:$MANAGER_PORT --verbose"


# ------------------------
# Optional Failure Injection
# ------------------------

read -p "Do you want to fail <= f followers in multiple partitions? [y/N] " answer

if [[ "$answer" == "y" || "$answer" == "Y" ]]; then
  echo " Preparing to kill one follower in each of two partitions..."

  declare -A KILLED

  for part in 0 1; do
    read -p " For Partition $part, which replica can I kill? (not the leader) [1/2]: " FOL
    WINDOW="s${part}${FOL}"

    echo "   Killing follower replica $FOL in partition $part (window: $WINDOW)..."

    # Kill that tmux window (stops the server)
    tmux send-keys -t $SESSION:$WINDOW C-c
    sleep 0.5
    tmux kill-window -t $SESSION:$WINDOW

    KILLED["$part"]=$FOL
  done

  echo ""
  echo "☠️  Followers killed:"
  for part in "${!KILLED[@]}"; do
    echo "  - Partition $part: Replica ${KILLED[$part]}"
  done
  echo ""
  echo "Cluster degraded, but client should still succeed on requests."
else
  echo " No servers killed. Cluster is fully healthy."
fi

