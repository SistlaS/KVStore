#!/bin/bash
set -e
set -o pipefail

cleanup() {
  echo "Cleaning up..."
  kill "${MANAGER_PID:-}" 2>/dev/null || true
  for pid in "${SERVER_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

MANAGER_IP_ADDR="192.168.1.229"
SERVER_IP_ADDR="192.168.1.229"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manager-ip)
      MANAGER_IP_ADDR="$2"
      shift 2
      ;;
    --server-ip)
      SERVER_IP_ADDR="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--manager-ip <IP>] [--server-ip <IP>]"
      exit 1
      ;;
  esac
done

# --- CONFIGURATION ---
MANAGER_ADDR="${MANAGER_IP_ADDR}:3666"
SERVER_ADDRS=("${SERVER_IP_ADDR}:5001" "${SERVER_IP_ADDR}:5002" "${SERVER_IP_ADDR}:5003")
CLIENT_BIN="target/release/kvclient"
MANAGER_BIN="target/release/kvmanager"
SERVER_BIN="target/release/kvserver"

# --- DETECT PLATFORM-SPECIFIC TIMEOUT ---
TIMEOUT_CMD="timeout"
if [[ "$OSTYPE" == "darwin"* ]]; then
  if ! command -v gtimeout &>/dev/null; then
    echo "Missing 'gtimeout'. Install with: brew install coreutils"
    exit 1
  fi
  TIMEOUT_CMD="gtimeout"
fi

echo "=== 1. Starting Partitioned Cluster ==="
$MANAGER_BIN --man_listen $MANAGER_ADDR --servers $(IFS=,; echo "${SERVER_ADDRS[*]}") &
MANAGER_PID=$!
echo "Manager PID: $MANAGER_PID"
sleep 1

SERVER_PIDS=()
for i in "${!SERVER_ADDRS[@]}"; do
  ADDR="${SERVER_ADDRS[$i]}"
  SERVER_ID="$i"
  BACKER_PATH="./data/server_$i"
  mkdir -p "$BACKER_PATH"

  echo "Starting server $ADDR (ID=$SERVER_ID)..."
  $SERVER_BIN \
    --manager_addr "$MANAGER_ADDR" \
    --api_listen "$ADDR" \
    --server_id "$SERVER_ID" \
    --backer_path "$BACKER_PATH" > "server_${SERVER_ID}.log" 2>&1 &

  SERVER_PIDS[$i]=$!
  echo "Server $SERVER_ID PID: ${SERVER_PIDS[$i]}"
done

sleep 2

echo
echo "=== 2. Doing PUTs and SWAPs ==="

# Assumptions (adjusted based on your partition layout):
# - zeta goes to Server 2 (partition [t6m - {))
# - 7zz goes to Server 1 (partition [6t6 - DmC)), which we will kill
# - gamma goes to Server 2 (partition [fKa - mDg])

$CLIENT_BIN --manager_addr "$MANAGER_ADDR" <<EOF
PUT zeta 1
PUT 7zz 2    
PUT gamma 3

SWAP zeta 11
SWAP 7zz 22
SWAP gamma 33
STOP
EOF

echo
echo "=== 3. Doing GETs and SCANs ==="
$CLIENT_BIN --manager_addr "$MANAGER_ADDR" <<EOF
GET zeta
GET 7zz
GET gamma
SCAN zeta gamma
STOP
EOF

echo
echo "=== 4. Killing Server 1 (responsible for 7zz) ==="
SERVER_IDX=1
kill "${SERVER_PIDS[$SERVER_IDX]}"
unset SERVER_PIDS[$SERVER_IDX]
echo "Server $SERVER_IDX killed."

sleep 1

echo
echo "=== 5. GET and SCAN on unaffected partition (should succeed) ==="
$CLIENT_BIN --manager_addr "$MANAGER_ADDR" <<EOF
GET gamma
SCAN gamma gammaa
STOP
EOF

echo
echo "=== 6. GET and SCAN on failed partition (should timeout) ==="
echo "Expecting timeout on GET 7zz..."
if $TIMEOUT_CMD 5 $CLIENT_BIN --manager_addr "$MANAGER_ADDR" <<< "GET 7zz"; then
  echo "ERROR: Expected timeout but GET 7zz succeeded!"
  exit 1
else
  echo "SUCCESS: GET 7zz timed out as expected"
fi

echo "Expecting timeout on SCAN 7zz gamma..."
if $TIMEOUT_CMD 5 $CLIENT_BIN --manager_addr "$MANAGER_ADDR" <<< "SCAN 7zz gamma"; then
  echo "ERROR: Expected timeout but SCAN 7zz gamma succeeded!"
  exit 1
else
  echo "SUCCESS: SCAN 7zz gamma timed out as expected"
fi

echo
echo "=== 7. Restarting Server 1 ==="
BACKER_PATH="./data/server_$SERVER_IDX"
mkdir -p "$BACKER_PATH"
$SERVER_BIN \
  --manager_addr "$MANAGER_ADDR" \
  --api_listen "${SERVER_ADDRS[$SERVER_IDX]}" \
  --server_id "$SERVER_IDX" \
  --backer_path "$BACKER_PATH" > "server_${SERVER_IDX}_restarted.log" 2>&1 &

SERVER_PIDS[$SERVER_IDX]=$!
echo "Server $SERVER_IDX restarted with PID: ${SERVER_PIDS[$SERVER_IDX]}"

sleep 2

echo
echo "=== 8. GET from recovered partition (should succeed) ==="
$CLIENT_BIN --manager_addr "$MANAGER_ADDR" <<EOF
GET 7zz
STOP
EOF

echo
echo "Test completed successfully!"
