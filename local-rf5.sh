#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$(pwd)}"
MANAGER="127.0.0.1:3666"
SERVER_ADDRS="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781"
P2P_ADDRS="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711"
LOG_DIR="tmp/madkv-p3/fuzz"
FUZZ_YES_LOG="${LOG_DIR}/fuzz-5-yes.log"

leader_replica_from_logs() {
  local leader
  leader="$(grep -h 'became leader' "${LOG_DIR}"/madkv-p3-s*.log 2>/dev/null | tail -n 1 | sed -E 's/.*replica ([0-9]+) became leader.*/\1/')"
  if [[ "$leader" =~ ^[0-4]$ ]]; then
    echo "$leader"
  fi
}

kill_replica_by_id() {
  local rid="$1"
  local pid_file="${LOG_DIR}/madkv-p3-s${rid}.pid"
  if [[ ! -f "$pid_file" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "$pid_file")"
  if kill -0 "$pid" >/dev/null 2>&1; then
    echo "[fuzz-yes] killing server replica ${rid} (pid ${pid})"
    kill "$pid" >/dev/null 2>&1 || true
    return 0
  fi
  return 1
}

kill_first_alive_excluding() {
  local exclude="${1:-}"
  local rid
  for rid in 0 1 2 3 4; do
    if [[ -n "$exclude" && "$rid" == "$exclude" ]]; then
      continue
    fi
    if kill_replica_by_id "$rid"; then
      echo "$rid"
      return 0
    fi
  done
  return 1
}

wait_for_fuzz_start_local() {
  local timeout_sec="${FUZZ_START_TIMEOUT_SEC:-180}"
  local waited=0
  while [[ "$waited" -lt "$timeout_sec" ]]; do
    if [[ -f "$FUZZ_YES_LOG" ]] && grep -q "Fuzzing starts" "$FUZZ_YES_LOG"; then
      echo "[fuzz-yes] detected fuzzer start in log"
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  echo "[fuzz-yes] warning: timed out waiting for fuzzer start log (${timeout_sec}s); skipping crash injection"
  return 1
}

up() {
  just p3::build

  mkdir -p "$LOG_DIR"

  cd kvstore

  ./bin/manager \
    --replica_id 0 \
    --man_listen 127.0.0.1:3666 \
    --p2p_listen 127.0.0.1:3606 \
    --peer_addrs none \
    --server_rf 5 \
    --server_addrs "$SERVER_ADDRS" \
    --backer_path ./backer.m.0 \
    > "${LOG_DIR}/madkv-p3-manager.log" 2>&1 &
  echo $! > "${LOG_DIR}/madkv-p3-manager.pid"

  ./bin/server \
    --partition_id 0 --replica_id 0 \
    --manager_addrs "$MANAGER" \
    --api_listen 127.0.0.1:3777 \
    --p2p_listen 127.0.0.1:3707 \
    --peer_addrs "127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711" \
    --backer_path ./backer.s0.0 \
    > "${LOG_DIR}/madkv-p3-s0.log" 2>&1 &
  echo $! > "${LOG_DIR}/madkv-p3-s0.pid"

  ./bin/server \
    --partition_id 0 --replica_id 1 \
    --manager_addrs "$MANAGER" \
    --api_listen 127.0.0.1:3778 \
    --p2p_listen 127.0.0.1:3708 \
    --peer_addrs "127.0.0.1:3707,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711" \
    --backer_path ./backer.s0.1 \
    > "${LOG_DIR}/madkv-p3-s1.log" 2>&1 &
  echo $! > "${LOG_DIR}/madkv-p3-s1.pid"

  ./bin/server \
    --partition_id 0 --replica_id 2 \
    --manager_addrs "$MANAGER" \
    --api_listen 127.0.0.1:3779 \
    --p2p_listen 127.0.0.1:3709 \
    --peer_addrs "127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3710,127.0.0.1:3711" \
    --backer_path ./backer.s0.2 \
    > "${LOG_DIR}/madkv-p3-s2.log" 2>&1 &
  echo $! > "${LOG_DIR}/madkv-p3-s2.pid"

  ./bin/server \
    --partition_id 0 --replica_id 3 \
    --manager_addrs "$MANAGER" \
    --api_listen 127.0.0.1:3780 \
    --p2p_listen 127.0.0.1:3710 \
    --peer_addrs "127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3711" \
    --backer_path ./backer.s0.3 \
    > "${LOG_DIR}/madkv-p3-s3.log" 2>&1 &
  echo $! > "${LOG_DIR}/madkv-p3-s3.pid"

  ./bin/server \
    --partition_id 0 --replica_id 4 \
    --manager_addrs "$MANAGER" \
    --api_listen 127.0.0.1:3781 \
    --p2p_listen 127.0.0.1:3711 \
    --peer_addrs "127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710" \
    --backer_path ./backer.s0.4 \
    > "${LOG_DIR}/madkv-p3-s4.log" 2>&1 &
  echo $! > "${LOG_DIR}/madkv-p3-s4.pid"

  cd "$ROOT"
  sleep 5
  echo "cluster is up"
}

down() {
  just p3::kill || true

  for f in \
    "${LOG_DIR}/madkv-p3-manager.pid" \
    "${LOG_DIR}/madkv-p3-s0.pid" \
    "${LOG_DIR}/madkv-p3-s1.pid" \
    "${LOG_DIR}/madkv-p3-s2.pid" \
    "${LOG_DIR}/madkv-p3-s3.pid" \
    "${LOG_DIR}/madkv-p3-s4.pid"
  do
    if [[ -f "$f" ]]; then
      kill "$(cat "$f")" >/dev/null 2>&1 || true
      rm -f "$f"
    fi
  done
}

clean() {
  down
  rm -rf kvstore/backer.*
}

restart() {
  clean
  up
}

fuzz_no() {
  clean
  up
  set +e
  just p3::fuzz 5 no "$MANAGER"
  local fuzz_rc=$?
  set -e
  down
  return "$fuzz_rc"
}

fuzz_yes() {
  clean
  up

  local crash_delay="${CRASH_DELAY_SEC:-8}"
  local crash_gap="${CRASH_GAP_SEC:-3}"
  local killer_pid=""

  (
    wait_for_fuzz_start_local || exit 0
    sleep "$crash_delay"
    local first=""
    first="$(leader_replica_from_logs || true)"
    if [[ -n "$first" ]]; then
      if ! kill_replica_by_id "$first"; then
        first="$(kill_first_alive_excluding "" || true)"
      fi
    else
      first="$(kill_first_alive_excluding "" || true)"
    fi

    sleep "$crash_gap"
    kill_first_alive_excluding "$first" >/dev/null || true
  ) &
  killer_pid=$!

  set +e
  just p3::fuzz 5 yes "$MANAGER"
  local fuzz_rc=$?
  set -e

  kill "$killer_pid" >/dev/null 2>&1 || true
  wait "$killer_pid" >/dev/null 2>&1 || true
  down
  return "$fuzz_rc"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  clean) clean ;;
  restart) restart ;;
  fuzz-no) fuzz_no ;;
  fuzz-yes) fuzz_yes ;;
  *)
    echo "usage: $0 {up|down|clean|restart|fuzz-no|fuzz-yes}"
    exit 1
    ;;
esac
