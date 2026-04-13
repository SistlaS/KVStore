#!/usr/bin/env bash
set -euo pipefail

# Local launcher for the 3-manager / 5-partition topology.
#
# This runs the full cluster on a single machine using localhost ports.
# It:
#   1. cleans up old KVStore processes
#   2. builds the project
#   3. starts the 3 managers
#   4. starts all 15 servers
#   5. runs YCSB workloads a..f via `just p3::bench`
#
# Usage:
#   bash local-run-5p.sh all
#   bash local-run-5p.sh start
#   bash local-run-5p.sh bench
#   bash local-run-5p.sh stop

REPO_DIR="${REPO_DIR:-$PWD}"

MANAGERS="${MANAGERS:-127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668}"
MANAGER_P2PS="${MANAGER_P2PS:-127.0.0.1:3606,127.0.0.1:3607,127.0.0.1:3608}"
SERVERS="${SERVERS:-127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782,127.0.0.1:3783,127.0.0.1:3784,127.0.0.1:3785,127.0.0.1:3786,127.0.0.1:3787,127.0.0.1:3788,127.0.0.1:3789,127.0.0.1:3790,127.0.0.1:3791}"
SERVER_P2PS="${SERVER_P2PS:-127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711,127.0.0.1:3712,127.0.0.1:3713,127.0.0.1:3714,127.0.0.1:3715,127.0.0.1:3716,127.0.0.1:3717,127.0.0.1:3718,127.0.0.1:3719,127.0.0.1:3720,127.0.0.1:3721}"
BACKER_PREFIX="${BACKER_PREFIX:-./backer}"

STARTUP_WAIT_SEC="${STARTUP_WAIT_SEC:-5}"
READY_TIMEOUT_SEC="${READY_TIMEOUT_SEC:-180}"
BENCH_CLIENTS="${BENCH_CLIENTS:-10}"
LOG_DIR="${LOG_DIR:-/tmp/madkv-p3-local-5p}"

phase() {
  printf '[local-5p] %s\n' "$*" >&2
}

run_repo() {
  local cmd="$1"
  phase "$cmd"
  bash -lc "cd \"${REPO_DIR}\" && $cmd"
}

cleanup_host() {
  phase "cleanup local processes"
  run_repo "just p3::kill || true"
  pkill -f '[t]arget/release/service' >/dev/null 2>&1 || true
  pkill -f '[b]in/manager' >/dev/null 2>&1 || true
  pkill -f '[b]in/server' >/dev/null 2>&1 || true
  pkill -f '[b]in/client' >/dev/null 2>&1 || true
  pkill -f '[r]unner' >/dev/null 2>&1 || true
  rm -rf "$LOG_DIR"
  rm -rf ./backer.*
}

build_all() {
  run_repo "just p3::build && just utils::build"
}

spawn_service() {
  local node_id="$1"
  local log_file="$LOG_DIR/${node_id}.log"
  local pid_file="$LOG_DIR/${node_id}.pid"
  phase "start $node_id -> $log_file"
  mkdir -p "$LOG_DIR"
  bash -lc "cd \"${REPO_DIR}\" && nohup just p3::service \"$node_id\" \"$MANAGERS\" \"$MANAGER_P2PS\" \"3\" \"$SERVERS\" \"$SERVER_P2PS\" \"$BACKER_PREFIX\" >\"$log_file\" 2>&1 & echo \$! >\"$pid_file\""
}

wait_for_cluster_ready() {
  local waited=0
  phase "waiting for partition leaders"
  while [[ "$waited" -lt "$READY_TIMEOUT_SEC" ]]; do
    local ok=1
    local part
    for part in 0 1 2 3 4; do
      if ! grep -h "became leader" "$LOG_DIR"/s"${part}".0.log "$LOG_DIR"/s"${part}".1.log "$LOG_DIR"/s"${part}".2.log >/dev/null 2>&1; then
        ok=0
        break
      fi
    done
    if [[ "$ok" -eq 1 ]]; then
      phase "cluster ready"
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  phase "warning: cluster did not report leaders within ${READY_TIMEOUT_SEC}s"
}

start_cluster() {
  phase "starting cluster"
  cleanup_host
  build_all

  spawn_service "m.0"
  spawn_service "m.1"
  spawn_service "m.2"
  sleep "$STARTUP_WAIT_SEC"

  spawn_service "s0.0"
  spawn_service "s1.0"
  spawn_service "s2.0"
  spawn_service "s3.0"
  spawn_service "s4.0"

  spawn_service "s0.1"
  spawn_service "s1.1"
  spawn_service "s2.1"
  spawn_service "s3.1"
  spawn_service "s4.1"

  spawn_service "s0.2"
  spawn_service "s1.2"
  spawn_service "s2.2"
  spawn_service "s3.2"
  spawn_service "s4.2"

  sleep "$STARTUP_WAIT_SEC"
  wait_for_cluster_ready
}

run_bench_suite() {
  local wl
  phase "running benchmarks"
  for wl in a b c d e f; do
    phase "bench workload $wl"
    run_repo "just p3::bench $BENCH_CLIENTS $wl 3 '$MANAGERS'"
  done
}

usage() {
  cat <<USAGE
Usage: $0 <command>

Commands:
  stop   Clean up processes and remove local backer/log directories
  start  Clean, build, and start the localhost 5-partition cluster
  bench  Run benchmarks a..f against an already-running cluster
  all    Start the cluster, then run benchmarks a..f
USAGE
}

cmd="${1:-all}"
case "$cmd" in
  stop)
    cleanup_host
    ;;
  start)
    start_cluster
    ;;
  bench)
    run_bench_suite
    ;;
  all)
    start_cluster
    run_bench_suite
    ;;
  *)
    usage
    exit 1
    ;;
esac
