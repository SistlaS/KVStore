set working-directory := '..'

# list project 3 just recipes
default:
    @just --list p3 --unsorted

# install extra dependencies of your KV code
deps:
    #!/usr/bin/env bash
    set -euo pipefail
    cd kvstore
    export PATH="$(go env GOPATH)/bin:$PATH"
    if ! command -v protoc >/dev/null 2>&1; then
        echo "protoc not found; please install Protocol Buffers (protoc) first."
        exit 1
    fi
    if ! command -v protoc-gen-go >/dev/null 2>&1; then
        GOBIN="$(go env GOPATH)/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
    fi
    if ! command -v protoc-gen-go-grpc >/dev/null 2>&1; then
        GOBIN="$(go env GOPATH)/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
    fi
    export PATH="$(go env GOPATH)/bin:$PATH"
    mkdir -p gen
    protoc -I proto --go_out=. --go_opt=module=madkv/kvstore \
           --go-grpc_out=. --go-grpc_opt=module=madkv/kvstore \
           proto/kvstore.proto proto/manager.proto proto/wal.proto proto/raft.proto
    echo "*******Dependencies installed and protobuf code generated*******"

# build your executables in release mode
build:
    just p3::deps
    cd kvstore && mkdir -p bin
    cd kvstore && mkdir -p .gocache
    cd kvstore && GOCACHE="$(pwd)/.gocache" go build -o bin/manager ./manager
    cd kvstore && GOCACHE="$(pwd)/.gocache" go build -o bin/server ./server
    cd kvstore && GOCACHE="$(pwd)/.gocache" go build -o bin/client ./client
    @echo "*******Built manager, server, and client binaries*******"

# clean the build of your executables
clean:
    rm -rf kvstore/bin
    rm -rf kvstore/gen
    rm -rf kvstore/.gocache
    rm -rf tmp/madkv-p3
    @echo "*******Cleaned build artifacts*******"

# run tests for your KV code
test:
    just p3::deps
    cd kvstore && mkdir -p .gocache
    cd kvstore && GOCACHE="$(pwd)/.gocache" go test ./... -count=1
    just p3::build
    @echo "*******Project 3 unit tests passed*******"

# run a deterministic replicated smoke testcase
testcase managers="127.0.0.1:3666" \
         servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779" \
         server_p2ps="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709": (tmpdir "tests")
    #!/usr/bin/env bash
    set -euo pipefail
    cd kvstore
    run_id="$(date +%s)_$RANDOM"
    out_dir="../tmp/madkv-p3/tests"
    mkdir -p "$out_dir"
    log="$out_dir/testcase-${run_id}.log"
    mgr_log="$out_dir/manager-${run_id}.log"
    s0_log="$out_dir/server0-${run_id}.log"
    s1_log="$out_dir/server1-${run_id}.log"
    s2_log="$out_dir/server2-${run_id}.log"
    backer_base="$out_dir/backer-${run_id}"
    IFS=',' read -r manager0 <<< "{{managers}}"
    IFS=',' read -r s0 s1 s2 <<< "{{servers}}"
    IFS=',' read -r p0 p1 p2 <<< "{{server_p2ps}}"
    cleanup() {
        for pid in "${s2_pid:-}" "${s1_pid:-}" "${s0_pid:-}" "${mgr_pid:-}"; do
            if [ -n "${pid}" ] && kill -0 "${pid}" >/dev/null 2>&1; then
                kill "${pid}" >/dev/null 2>&1 || true
                wait "${pid}" >/dev/null 2>&1 || true
            fi
        done
    }
    trap cleanup EXIT
    ./bin/manager --replica_id 0 --man_listen "$manager0" --p2p_listen 127.0.0.1:3606 --peer_addrs none --server_rf 3 --server_addrs "{{servers}}" --backer_path "${backer_base}.m0" >"$mgr_log" 2>&1 &
    mgr_pid=$!
    ./bin/server --partition_id 0 --replica_id 0 --manager_addrs "{{managers}}" --api_listen "$s0" --p2p_listen "$p0" --peer_addrs "$p1,$p2" --backer_path "${backer_base}.s0" >"$s0_log" 2>&1 &
    s0_pid=$!
    ./bin/server --partition_id 0 --replica_id 1 --manager_addrs "{{managers}}" --api_listen "$s1" --p2p_listen "$p1" --peer_addrs "$p0,$p2" --backer_path "${backer_base}.s1" >"$s1_log" 2>&1 &
    s1_pid=$!
    ./bin/server --partition_id 0 --replica_id 2 --manager_addrs "{{managers}}" --api_listen "$s2" --p2p_listen "$p2" --peer_addrs "$p0,$p1" --backer_path "${backer_base}.s2" >"$s2_log" 2>&1 &
    s2_pid=$!
    sleep 4
    assert() {
        local needle="$1"
        if ! grep -Fq "$needle" "$log"; then
            echo "assert failed: $needle" >&2
            echo "---- testcase log ----" >&2
            cat "$log" >&2
            exit 1
        fi
    }
    {
        echo "=== initial write through leader redirects ==="
        ./bin/client --manager_addrs "{{managers}}" --op put --key smoke --value one
        ./bin/client --manager_addrs "{{managers}}" --op get --key smoke
        echo "=== kill a follower and continue ==="
        leader_replica="$(grep -h 'became leader' "$s0_log" "$s1_log" "$s2_log" | tail -n 1 | sed -E 's/.*replica ([0-9]+) became leader.*/\1/')"
        case "$leader_replica" in
            0) kill "$s1_pid"; wait "$s1_pid" 2>/dev/null || true; s1_pid="" ;;
            1) kill "$s2_pid"; wait "$s2_pid" 2>/dev/null || true; s2_pid="" ;;
            2) kill "$s0_pid"; wait "$s0_pid" 2>/dev/null || true; s0_pid="" ;;
            *) kill "$s1_pid"; wait "$s1_pid" 2>/dev/null || true; s1_pid="" ;;
        esac
        sleep 2
        ./bin/client --manager_addrs "{{managers}}" --op put --key smoke --value two
        ./bin/client --manager_addrs "{{managers}}" --op get --key smoke
    } | tee "$log"
    assert "PUT smoke one (found=false)"
    assert "GET smoke one"
    assert "PUT smoke two (found=true)"
    assert "GET smoke two"
    echo "replicated testcase passed: $log"

# run your KV store manager replica
manager rep_id="0" \
        man_port="3666" \
        p2p_port="3606" \
        peers="127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="3" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782"\
        backer_path="./backer.m.0":
    cd kvstore && ./bin/manager \
        --replica_id "{{rep_id}}" \
        --man_listen "0.0.0.0:{{man_port}}" \
        --p2p_listen "0.0.0.0:{{p2p_port}}" \
        --peer_addrs "{{peers}}" \
        --server_rf "{{server_rf}}" \
        --server_addrs "{{servers}}" \
        --backer_path "{{backer_path}}"

# run your KV store server replica
server part_id="0" \
       rep_id="0" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3777" \
       p2p_port="3707" \
       peers="127.0.0.1:3708,127.0.0.1:3709" \
       backer_path="./backer.s0.0":
    cd kvstore && ./bin/server \
        --partition_id "{{part_id}}" \
        --replica_id "{{rep_id}}" \
        --manager_addrs "{{managers}}" \
        --api_listen "0.0.0.0:{{api_port}}" \
        --p2p_listen "0.0.0.0:{{p2p_port}}" \
        --peer_addrs "{{peers}}" \
        --backer_path "{{backer_path}}"

# run you KV store client in stdin/out interface mode
client managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668":
    cd kvstore && ./bin/client --manager_addrs "{{managers}}"

# kill all processes of your KV store system
kill:
    -pkill -f 'kvstore/bin/manager' >/dev/null 2>&1
    -pkill -f 'kvstore/bin/server' >/dev/null 2>&1
    -pkill -f 'kvstore/bin/client' >/dev/null 2>&1

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p3"
cargo_home := if `pwd` != "" { "`pwd`/.cargo-home" } else { ".cargo-home" }

# launch the KV service components on node (see README for conventions)
service node="s0.0" \
        managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
        manager_p2ps="127.0.0.1:3606,127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="3" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782" \
        server_p2ps="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711,127.0.0.1:3712" \
        backer_prefix="./backer":
    just p3::build
    just utils::build
    mkdir -p "{{cargo_home}}"
    CARGO_HOME="{{cargo_home}}" cargo run -p runner -r --bin service -- \
        --server-just-args p3::server \
            $({{python_run}} justmod/xtract.py partid "{{node}}") \
            $({{python_run}} justmod/xtract.py repid "{{node}}") \
            "{{managers}}" \
            $({{python_run}} justmod/xtract.py portof "{{servers}}" "{{node}}" "{{server_rf}}") \
            $({{python_run}} justmod/xtract.py portof "{{server_p2ps}}" "{{node}}" "{{server_rf}}") \
            $({{python_run}} justmod/xtract.py peersof "{{server_p2ps}}" "{{node}}" "{{server_rf}}") \
            "{{backer_prefix}}.{{node}}" \
        --manager-just-args p3::manager \
            $({{python_run}} justmod/xtract.py repid "{{node}}") \
            $({{python_run}} justmod/xtract.py portof "{{managers}}" "{{node}}") \
            $({{python_run}} justmod/xtract.py portof "{{manager_p2ps}}" "{{node}}") \
            $({{python_run}} justmod/xtract.py peersof "{{manager_p2ps}}" "{{node}}") \
            "{{server_rf}}" \
            "{{servers}}" \
            "{{backer_prefix}}.{{node}}" \
        --node-id "{{node}}"

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a fuzz testing scenario
fuzz server_rf="5" crashing="no" \
     managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668": (tmpdir "fuzz")
    #!/usr/bin/env bash
    set -euo pipefail
    just p3::build
    just utils::build
    reachable=0
    IFS=',' read -r -a addrs <<< "{{managers}}"
    for addr in "${addrs[@]}"; do
        host="${addr%:*}"
        port="${addr##*:}"
        if nc -z "$host" "$port" >/dev/null 2>&1; then
            reachable=1
            break
        fi
    done
    if [ "$reachable" -eq 0 ]; then
        echo "no manager replicas are reachable at {{managers}}" >&2
        echo "start the cluster first with 'just p3::service ...' (or launch manager/server recipes manually), then rerun fuzz" >&2
        exit 1
    fi
    mkdir -p "{{cargo_home}}"
    CARGO_HOME="{{cargo_home}}" cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 \
        --conflict \
        --client-just-args p3::client "{{managers}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{server_rf}}-{{crashing}}.log"
    just p3::kill

# run a YCSB benchmark workload
bench nclis wload server_rf="3" \
      managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668": (tmpdir "bench")
    #!/usr/bin/env bash
    set -euo pipefail
    just p3::build
    just utils::build
    just utils::ycsb
    reachable=0
    IFS=',' read -r -a addrs <<< "{{managers}}"
    for addr in "${addrs[@]}"; do
        host="${addr%:*}"
        port="${addr##*:}"
        if nc -z "$host" "$port" >/dev/null 2>&1; then
            reachable=1
            break
        fi
    done
    if [ "$reachable" -eq 0 ]; then
        echo "no manager replicas are reachable at {{managers}}" >&2
        echo "start the cluster first with 'just p3::service ...' (or launch manager/server recipes manually), then rerun bench" >&2
        exit 1
    fi
    mkdir -p "{{cargo_home}}"
    CARGO_HOME="{{cargo_home}}" cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p3::client "{{managers}}" \
        | tee "{{tmpdir_prefix}}/bench/bench-{{nclis}}-{{wload}}-{{server_rf}}.log"
    # just p3::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj3.py
