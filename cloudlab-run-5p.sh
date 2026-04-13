#!/usr/bin/env bash
set -euo pipefail

# CloudLab launcher for the 3-node / 5-partition topology.
#
# This script expects passwordless SSH access to the three CloudLab nodes.
# It will:
#   1. clean up old processes on all nodes
#   2. build the project on all nodes
#   3. start the 3 managers
#   4. start all 15 servers
#   5. run workloads a..f with `just p3::bench`
#
# Usage:
#   bash cloudlab-run-5p.sh all
#   bash cloudlab-run-5p.sh start
#   bash cloudlab-run-5p.sh bench
#   bash cloudlab-run-5p.sh stop

REPO_DIR="${REPO_DIR:-$HOME/KVStore}"
HOST0="${HOST0:-bhatnekk@clnode087.clemson.cloudlab.us}"
HOST1="${HOST1:-bhatnekk@clnode096.clemson.cloudlab.us}"
HOST2="${HOST2:-bhatnekk@clnode078.clemson.cloudlab.us}"

MANAGERS="${MANAGERS:-10.10.1.1:3666,10.10.1.2:3667,10.10.1.3:3668}"
MANAGER_P2PS="${MANAGER_P2PS:-10.10.1.1:3606,10.10.1.2:3607,10.10.1.3:3608}"
SERVERS="${SERVERS:-10.10.1.1:3777,10.10.1.2:3778,10.10.1.3:3779,10.10.1.1:3780,10.10.1.2:3781,10.10.1.3:3782,10.10.1.1:3783,10.10.1.2:3784,10.10.1.3:3785,10.10.1.1:3786,10.10.1.2:3787,10.10.1.3:3788,10.10.1.1:3789,10.10.1.2:3790,10.10.1.3:3791}"
SERVER_P2PS="${SERVER_P2PS:-10.10.1.1:3707,10.10.1.2:3708,10.10.1.3:3709,10.10.1.1:3710,10.10.1.2:3711,10.10.1.3:3712,10.10.1.1:3713,10.10.1.2:3714,10.10.1.3:3715,10.10.1.1:3716,10.10.1.2:3717,10.10.1.3:3718,10.10.1.1:3719,10.10.1.2:3720,10.10.1.3:3721}"
BACKER_PREFIX="${BACKER_PREFIX:-./backer}"
BRANCH="${BRANCH:-main}"

STARTUP_WAIT_SEC="${STARTUP_WAIT_SEC:-5}"
READY_TIMEOUT_SEC="${READY_TIMEOUT_SEC:-180}"
BENCH_CLIENTS="${BENCH_CLIENTS:-10}"
REMOTE_PATH_PREFIX="${REMOTE_PATH_PREFIX:-/usr/local/go/bin:/usr/bin:/bin:/usr/local/bin}"
LOG_DIR="${LOG_DIR:-/tmp/madkv-p3-cloudlab-5p}"

phase() {
  printf '[cloudlab] %s\n' "$*" >&2
}

remote() {
  local host="$1"
  local cmd="$2"
  phase "[$host] $cmd"
  ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$host" "export PATH=\"$REMOTE_PATH_PREFIX:\$HOME/go/bin:\$HOME/.cargo/bin:\$PATH\"; cd \"\$HOME/KVStore\" && $cmd"
}

remote_quiet() {
  local host="$1"
  local cmd="$2"
  ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$host" "export PATH=\"$REMOTE_PATH_PREFIX:\$HOME/go/bin:\$HOME/.cargo/bin:\$PATH\"; cd \"\$HOME/KVStore\" && $cmd"
}

remote_bg() {
  local host="$1"
  local label="$2"
  local cmd="$3"
  local log_file="$LOG_DIR/${label}.log"
  local pid_file="$LOG_DIR/${label}.pid"
  phase "[$host] start $label -> $log_file"
  ssh -n -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$host" "mkdir -p \"$LOG_DIR\" && export PATH=\"$REMOTE_PATH_PREFIX:\$HOME/go/bin:\$HOME/.cargo/bin:\$PATH\"; cd \"\$HOME/KVStore\" && nohup $cmd >\"$log_file\" 2>&1 & echo \$! >\"$pid_file\"" >/dev/null 2>&1 &
}

cleanup_host() {
  local host="$1"
  phase "cleanup $host"
  remote "$host" "just p3::kill || true"
  remote "$host" "pkill -f '[t]arget/release/service' >/dev/null 2>&1 || true"
  remote "$host" "pkill -f '[b]in/manager' >/dev/null 2>&1 || true"
  remote "$host" "pkill -f '[b]in/server' >/dev/null 2>&1 || true"
  remote "$host" "pkill -f '[b]in/client' >/dev/null 2>&1 || true"
  remote "$host" "pkill -f '[r]unner' >/dev/null 2>&1 || true"
  remote "$host" "rm -rf '$LOG_DIR'"
  remote "$host" "rm -rf ./backer.*"
}

ensure_branch() {
  local host="$1"
  phase "ensure branch $BRANCH on $host"
  remote "$host" "git fetch origin && git checkout '$BRANCH' && git pull --ff-only origin '$BRANCH'"
}

build_all() {
  ensure_branch "$HOST0"
  ensure_branch "$HOST1"
  ensure_branch "$HOST2"
  remote "$HOST0" "just p3::build && just utils::build"
  remote "$HOST1" "just p3::build && just utils::build"
  remote "$HOST2" "just p3::build && just utils::build"
}

spawn_service() {
  local host="$1"
  local node_id="$2"
  local cmd
  printf -v cmd "just p3::service %q %q %q %q %q %q %q" \
    "$node_id" "$MANAGERS" "$MANAGER_P2PS" "3" "$SERVERS" "$SERVER_P2PS" "$BACKER_PREFIX"
  remote_bg "$host" "$node_id" "$cmd"
}

partition_has_leader_on_host() {
  local host="$1"
  local part="$2"
  local log_path
  case "$host" in
    "$HOST0") log_path="$LOG_DIR/s${part}.0.log" ;;
    "$HOST1") log_path="$LOG_DIR/s${part}.1.log" ;;
    "$HOST2") log_path="$LOG_DIR/s${part}.2.log" ;;
    *) return 1 ;;
  esac
  remote_quiet "$host" "test -f '$log_path' && grep -q 'became leader' '$log_path'"
}

wait_for_cluster_ready() {
  local waited=0
  phase "waiting for partition leaders"
  while [[ "$waited" -lt "$READY_TIMEOUT_SEC" ]]; do
    local ok=1
    local part
    for part in 0 1 2 3 4; do
      if ! partition_has_leader_on_host "$HOST0" "$part" &&
         ! partition_has_leader_on_host "$HOST1" "$part" &&
         ! partition_has_leader_on_host "$HOST2" "$part"; then
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
  return 0
}

start_cluster() {
  phase "starting cluster"
  cleanup_host "$HOST0"
  cleanup_host "$HOST1"
  cleanup_host "$HOST2"
  build_all

  spawn_service "$HOST0" "m.0"
  spawn_service "$HOST1" "m.1"
  spawn_service "$HOST2" "m.2"
  sleep "$STARTUP_WAIT_SEC"

  spawn_service "$HOST0" "s0.0"
  spawn_service "$HOST0" "s1.0"
  spawn_service "$HOST0" "s2.0"
  spawn_service "$HOST0" "s3.0"
  spawn_service "$HOST0" "s4.0"

  spawn_service "$HOST1" "s0.1"
  spawn_service "$HOST1" "s1.1"
  spawn_service "$HOST1" "s2.1"
  spawn_service "$HOST1" "s3.1"
  spawn_service "$HOST1" "s4.1"

  spawn_service "$HOST2" "s0.2"
  spawn_service "$HOST2" "s1.2"
  spawn_service "$HOST2" "s2.2"
  spawn_service "$HOST2" "s3.2"
  spawn_service "$HOST2" "s4.2"

  sleep "$STARTUP_WAIT_SEC"

  wait_for_cluster_ready
}

run_bench_suite() {
  local wl
  phase "running benchmarks"
  for wl in a b c d e f; do
    phase "bench workload $wl"
    remote "$HOST0" "just p3::bench $BENCH_CLIENTS $wl 3 $MANAGERS"
  done
}

usage() {
  cat <<USAGE
Usage: $0 <command>

Commands:
  stop   Clean up processes on all CloudLab nodes
  start  Clean, build, and start the 3-manager / 15-server cluster
  bench  Run benchmarks a..f against the already-running cluster
  all    Start the cluster, then run benchmarks a..f

Environment:
  BRANCH=...  Git branch to fetch, checkout, and build before launching
  REMOTE_PATH_PREFIX=...  Extra PATH entries to prepend on CloudLab nodes
USAGE
}

cmd="${1:-all}"
case "$cmd" in
  stop)
    cleanup_host "$HOST0"
    cleanup_host "$HOST1"
    cleanup_host "$HOST2"
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
