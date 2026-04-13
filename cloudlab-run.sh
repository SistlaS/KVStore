#!/usr/bin/env bash
set -euo pipefail

# CloudLab orchestration helper aligned to the manual RF=3 workflow.
#
# Run from control node (typically node0). Assumes passwordless SSH.
#
# Examples:
#   bash justmod/cloudlab-run.sh fuzz-suite
#   bash justmod/cloudlab-run.sh bench-suite
#   bash justmod/cloudlab-run.sh stop

REPO_DIR="${REPO_DIR:-$HOME/KVStore}"
HOST0="${HOST0:-node0}"
HOST1="${HOST1:-node1}"
HOST2="${HOST2:-node2}"

MANAGERS="${MANAGERS:-10.10.1.1:3666,10.10.1.2:3667,10.10.1.3:3668}"
MANAGER_P2PS="${MANAGER_P2PS:-10.10.1.1:3606,10.10.1.2:3607,10.10.1.3:3608}"
SERVERS_RF3="${SERVERS_RF3:-10.10.1.1:3777,10.10.1.2:3778,10.10.1.3:3779}"
SERVER_P2PS_RF3="${SERVER_P2PS_RF3:-10.10.1.1:3707,10.10.1.2:3708,10.10.1.3:3709}"
BACKER_PREFIX="${BACKER_PREFIX:-./backer}"

STARTUP_WAIT_SEC="${STARTUP_WAIT_SEC:-5}"
CRASH_DELAY_SEC="${CRASH_DELAY_SEC:-8}"
CRASH_GAP_SEC="${CRASH_GAP_SEC:-4}"
FUZZ_YES_LOG="/tmp/madkv-p3/fuzz/fuzz-3-yes.log"

run_remote() {
  local host="$1"
  shift
  ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$host" "cd '$REPO_DIR' && $*"
}

cleanup_host() {
  local host="$1"
  run_remote "$host" "just p3::kill || true"
  run_remote "$host" "pkill -f 'target/release/service' >/dev/null 2>&1 || true"
  run_remote "$host" "pkill -f 'bin/manager' >/dev/null 2>&1 || true"
  run_remote "$host" "pkill -f 'bin/server' >/dev/null 2>&1 || true"
  run_remote "$host" "pkill -f 'bin/client' >/dev/null 2>&1 || true"
  run_remote "$host" "pkill -f 'runner' >/dev/null 2>&1 || true"
  run_remote "$host" "rm -rf ./backer.*"
}

stop_all() {
  cleanup_host "$HOST0"
  cleanup_host "$HOST1"
  cleanup_host "$HOST2"
}

build_all() {
  run_remote "$HOST0" "just p3::build && just utils::build"
  run_remote "$HOST1" "just p3::build && just utils::build"
  run_remote "$HOST2" "just p3::build && just utils::build"
}

spawn_service() {
  local host="$1"
  local node_id="$2"
  run_remote "$host" "nohup just p3::service '$node_id' '$MANAGERS' '$MANAGER_P2PS' '3' '$SERVERS_RF3' '$SERVER_P2PS_RF3' '$BACKER_PREFIX' >/tmp/madkv-p3-${node_id}.log 2>&1 &"
}

start_cluster_rf3() {
  stop_all
  build_all

  spawn_service "$HOST0" "m.0"
  spawn_service "$HOST1" "m.1"
  spawn_service "$HOST2" "m.2"
  sleep "$STARTUP_WAIT_SEC"

  spawn_service "$HOST0" "s0.0"
  spawn_service "$HOST1" "s0.1"
  spawn_service "$HOST2" "s0.2"
  sleep "$STARTUP_WAIT_SEC"
}

run_fuzz_no() {
  start_cluster_rf3
  run_remote "$HOST0" "just p3::fuzz 3 no '$MANAGERS'"
  stop_all
}

leader_replica_rf3() {
  local line
  line="$(
    {
      run_remote "$HOST0" "grep -h 'became leader' /tmp/madkv-p3-s0.0.log 2>/dev/null | tail -n 1" || true
      run_remote "$HOST1" "grep -h 'became leader' /tmp/madkv-p3-s0.1.log 2>/dev/null | tail -n 1" || true
      run_remote "$HOST2" "grep -h 'became leader' /tmp/madkv-p3-s0.2.log 2>/dev/null | tail -n 1" || true
    } | tail -n 1
  )"
  echo "$line" | sed -E 's/.*replica ([0-9]+) became leader.*/\1/'
}

kill_replica_rf3() {
  local rid="$1"
  case "$rid" in
    0) run_remote "$HOST0" "pkill -f -- '--partition_id 0 --replica_id 0' >/dev/null 2>&1 || true" ;;
    1) run_remote "$HOST1" "pkill -f -- '--partition_id 0 --replica_id 1' >/dev/null 2>&1 || true" ;;
    2) run_remote "$HOST2" "pkill -f -- '--partition_id 0 --replica_id 2' >/dev/null 2>&1 || true" ;;
    *) return 1 ;;
  esac
}

run_crash_sequence_rf3() {
  local delay="$1"
  local gap="$2"
  local timeout_sec="${FUZZ_START_TIMEOUT_SEC:-240}"
  local waited=0
  while [[ "$waited" -lt "$timeout_sec" ]]; do
    if run_remote "$HOST0" "test -f '$FUZZ_YES_LOG' && grep -q 'Fuzzing starts' '$FUZZ_YES_LOG'"; then
      echo "[cloudlab] detected fuzzer start in log"
      break
    fi
    sleep 1
    waited=$((waited + 1))
  done
  if [[ "$waited" -ge "$timeout_sec" ]]; then
    echo "[cloudlab] warning: timed out waiting for fuzzer start log (${timeout_sec}s); skipping crash injection"
    return 0
  fi

  sleep "$delay"

  local leader
  leader="$(leader_replica_rf3)"
  local first="1"
  if [[ "$leader" == "1" ]]; then
    first="0"
  fi
  echo "[cloudlab] killing follower replica ${first}"
  kill_replica_rf3 "$first" || true

  sleep "$gap"
  leader="$(leader_replica_rf3)"
  local second="$leader"
  if [[ ! "$second" =~ ^[0-2]$ || "$second" == "$first" ]]; then
    for candidate in 0 1 2; do
      if [[ "$candidate" != "$first" ]]; then
        second="$candidate"
        break
      fi
    done
  fi
  echo "[cloudlab] killing leader replica ${second} (best-effort)"
  kill_replica_rf3 "$second" || true
}

run_fuzz_yes() {
  start_cluster_rf3
  run_crash_sequence_rf3 "$CRASH_DELAY_SEC" "$CRASH_GAP_SEC" &
  local killer_pid=$!

  set +e
  run_remote "$HOST0" "just p3::fuzz 3 yes '$MANAGERS'"
  local fuzz_rc=$?
  set -e

  kill "$killer_pid" >/dev/null 2>&1 || true
  wait "$killer_pid" >/dev/null 2>&1 || true
  stop_all
  return "$fuzz_rc"
}

run_bench_suite() {
  local wl
  start_cluster_rf3
  for wl in a b c d e f; do
    run_remote "$HOST0" "just p3::bench 10 '$wl' 3 '$MANAGERS'"
  done
  stop_all
}

usage() {
  cat <<USAGE
Usage: $0 <command>

Commands:
  stop        Cleanup on all nodes and stop processes
  start       Cleanup, rebuild, start managers and servers (RF=3)
  fuzz-no     Full workflow + fuzz no-crash (RF=3)
  fuzz-yes    Full workflow + fuzz with timed two-replica crash (RF=3)
  fuzz-suite  Run fuzz-no then fuzz-yes
  bench-suite Full workflow + benchmark workloads a..f at 10 clients (RF=3)
USAGE
}

cmd="${1:-}"
case "$cmd" in
  stop)
    stop_all
    ;;
  start)
    start_cluster_rf3
    ;;
  fuzz-no)
    run_fuzz_no
    ;;
  fuzz-yes)
    run_fuzz_yes
    ;;
  fuzz-suite)
    run_fuzz_no
    run_fuzz_yes
    ;;
  bench-suite)
    run_bench_suite
    ;;
  *)
    usage
    exit 1
    ;;
esac
