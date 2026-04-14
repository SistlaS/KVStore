#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$(pwd)}"
MANAGER="127.0.0.1:3666"
SERVER_ADDRS=("127.0.0.1:3777" "127.0.0.1:3778" "127.0.0.1:3779" "127.0.0.1:3780" "127.0.0.1:3781")
P2P_ADDRS=("127.0.0.1:3707" "127.0.0.1:3708" "127.0.0.1:3709" "127.0.0.1:3710" "127.0.0.1:3711")
WORKDIR="$ROOT/tmp/madkv-p3-ycsb"
LOG_DIR="$ROOT/tmp/madkv-p3/ycsb"
BENCH_ROOT="$ROOT/tmp/madkv-p3/bench"

join_by() {
  local sep="$1"
  shift
  local out=""
  local item
  for item in "$@"; do
    if [[ -n "$out" ]]; then
      out+="$sep"
    fi
    out+="$item"
  done
  printf '%s' "$out"
}

server_addrs_for_rf() {
  local rf="$1"
  join_by , "${SERVER_ADDRS[@]:0:rf}"
}

peers_for_replica() {
  local rf="$1"
  local rid="$2"
  local peers=()
  local i
  for ((i = 0; i < rf; i++)); do
    if [[ "$i" != "$rid" ]]; then
      peers+=("${P2P_ADDRS[$i]}")
    fi
  done
  if ((${#peers[@]} == 0)); then
    printf '%s' none
  else
    join_by , "${peers[@]}"
  fi
}

clean_paths() {
  rm -f "$LOG_DIR"/madkv-p3-manager.pid "$LOG_DIR"/madkv-p3-s{0,1,2,3,4}.pid
  rm -rf "$WORKDIR"
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout_sec="${3:-30}"
  local waited=0
  while (( waited < timeout_sec )); do
    if nc -z "$host" "$port" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

stop_cluster() {
  local pid_file pid
  for pid_file in "$LOG_DIR"/madkv-p3-manager.pid "$LOG_DIR"/madkv-p3-s{0,1,2,3,4}.pid; do
    if [[ -f "$pid_file" ]]; then
      pid="$(cat "$pid_file")"
      if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
        kill "$pid" >/dev/null 2>&1 || true
      fi
      rm -f "$pid_file"
    fi
  done
  pkill -f '(^|/)\./bin/manager' >/dev/null 2>&1 || true
  pkill -f '(^|/)\./bin/server' >/dev/null 2>&1 || true
  pkill -f '(^|/)\./bin/client' >/dev/null 2>&1 || true
  pkill -f 'kvstore/bin/manager' >/dev/null 2>&1 || true
  pkill -f 'kvstore/bin/server' >/dev/null 2>&1 || true
  pkill -f 'kvstore/bin/client' >/dev/null 2>&1 || true
}

start_cluster() {
  local rf="$1"
  local tag="$2"
  local server_addrs
  server_addrs="$(server_addrs_for_rf "$rf")"

  mkdir -p "$WORKDIR"
  mkdir -p "$LOG_DIR"
  cd kvstore

  ./bin/manager \
    --replica_id 0 \
    --man_listen "$MANAGER" \
    --p2p_listen 127.0.0.1:3606 \
    --peer_addrs none \
    --server_rf "$rf" \
    --server_addrs "$server_addrs" \
    --backer_path "./backer.m.${tag}" \
    >"$LOG_DIR/madkv-p3-manager.log" 2>&1 &
  echo $! >"$LOG_DIR/madkv-p3-manager.pid"

  if ! wait_for_port 127.0.0.1 3666 30; then
    echo "manager did not become reachable on ${MANAGER}" >&2
    tail -n 50 "$LOG_DIR/madkv-p3-manager.log" >&2 || true
    return 1
  fi

  local rid
  for ((rid = 0; rid < rf; rid++)); do
    local api_addr="${SERVER_ADDRS[$rid]}"
    local p2p_addr="${P2P_ADDRS[$rid]}"
    local peers
    peers="$(peers_for_replica "$rf" "$rid")"
    ./bin/server \
      --partition_id 0 \
      --replica_id "$rid" \
      --manager_addrs "$MANAGER" \
      --api_listen "$api_addr" \
      --p2p_listen "$p2p_addr" \
      --peer_addrs "$peers" \
      --backer_path "./backer.s0.${rid}.${tag}" \
    >"$LOG_DIR/madkv-p3-s${rid}.log" 2>&1 &
    echo $! >"$LOG_DIR/madkv-p3-s${rid}.pid"
  done

  cd "$ROOT"

  local api_addr
  for api_addr in "${SERVER_ADDRS[@]:0:rf}"; do
    local host="${api_addr%:*}"
    local port="${api_addr##*:}"
    if ! wait_for_port "$host" "$port" 30; then
      echo "server did not become reachable on ${api_addr}" >&2
      for rid in $(seq 0 $((rf - 1))); do
        tail -n 50 "$LOG_DIR/madkv-p3-s${rid}.log" >&2 || true
      done
      return 1
    fi
  done
}

run_bench() {
  local nclis="$1"
  local workload="$2"
  local rf="$3"
  mkdir -p "$BENCH_ROOT"
  CARGO_HOME="$ROOT/.cargo-home" cargo run -p runner -r --bin bencher -- \
    --num-clis "$nclis" \
    --workload "$workload" \
    --client-just-args p3::client "$MANAGER" \
    | tee "${BENCH_ROOT}/bench-${nclis}-${workload}-rf${rf}.log"
}

bench_matrix_10_clients() {
  local rf workload tag
  for rf in 1 3 5; do
    for workload in a b c d e f; do
      tag="rf${rf}-${workload}-10"
      stop_cluster
      start_cluster "$rf" "$tag"
      run_bench 10 "$workload" "$rf"
    done
  done
}

bench_scale_a() {
  local rf nclis tag
  for rf in 1 5; do
    for nclis in $(seq 1 30); do
      tag="rf${rf}-a-${nclis}"
      stop_cluster
      start_cluster "$rf" "$tag"
      run_bench "$nclis" a "$rf"
    done
  done
}

usage() {
  cat <<'EOF'
Usage: ./local-ycsb.sh [all|matrix|scale-a|clean|down]

  all      Run the requested benchmark set:
           - workloads A-F with 10 clients at RF 1, 3, and 5
           - workload A with client counts 1..30 at RF 1 and 5
  matrix   Only run workloads A-F with 10 clients at RF 1, 3, and 5
  scale-a  Only run workload A with client counts 1..30 at RF 1 and 5
  clean    Stop any running cluster and remove local benchmark artifacts
  down     Stop any running cluster
EOF
}

main() {
  local cmd="${1:-all}"
  case "$cmd" in
    all)
      just p3::build
      just utils::build
      stop_cluster
      bench_matrix_10_clients
      bench_scale_a
      stop_cluster
      ;;
    matrix)
      just p3::build
      just utils::build
      stop_cluster
      bench_matrix_10_clients
      stop_cluster
      ;;
    scale-a)
      just p3::build
      just utils::build
      stop_cluster
      bench_scale_a
      stop_cluster
      ;;
    clean)
      stop_cluster
      clean_paths
      rm -rf "$BENCH_ROOT"
      ;;
    down)
      stop_cluster
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
}

trap stop_cluster EXIT
main "$@"
