set working-directory := '..'

# list project 2 just recipes
default:
    @just --list p2 --unsorted

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
        echo "protoc-gen-go not found in PATH; install it first."
        exit 1
    fi
    if ! command -v protoc-gen-go-grpc >/dev/null 2>&1; then
        echo "protoc-gen-go-grpc not found in PATH; install it first."
        exit 1
    fi
    if [ ! -f go.mod ]; then
        go mod init madkv/kvstore
    fi
    mkdir -p gen/kvpb
    protoc --go_out=. --go_opt=module=madkv/kvstore \
           --go-grpc_out=. --go-grpc_opt=module=madkv/kvstore \
           proto/kvstore.proto \
           proto/manager.proto \
           proto/wal.proto
    echo "*******Dependencies installed and protobuf code generated*******"

# build your executables in release mode
build:
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
    rm -rf tmp/madkv-p2
    @echo "*******Cleaned build artifacts*******"

# run tests for your KV code
test:
    just p2::deps
    cd kvstore && mkdir -p .gocache
    cd kvstore && GOCACHE="$(pwd)/.gocache" go test ./... -count=1
    just p2::build
    just p2::testcase
    @echo "*******All tests passed*******"

# run the required deterministic demo testcase
testcase manager="127.0.0.1:3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779": (tmpdir "tests")
    #!/usr/bin/env bash
    set -euo pipefail
    cd kvstore
    run_id="$(date +%s)_$RANDOM"
    out_dir="../tmp/madkv-p2/tests"
    mkdir -p "$out_dir"
    log="$out_dir/testcase-${run_id}.log"
    mgr_log="$out_dir/manager-${run_id}.log"
    s0_log="$out_dir/server0-${run_id}.log"
    s1_log="$out_dir/server1-${run_id}.log"
    s2_log="$out_dir/server2-${run_id}.log"
    backer_base="$out_dir/backer-${run_id}"
    IFS=',' read -r s0 s1 s2 <<< "{{servers}}"
    p0="${s0##*:}"
    p1="${s1##*:}"
    p2="${s2##*:}"
    run_with_timeout() {
        local secs="$1"
        shift
        if command -v timeout >/dev/null 2>&1; then
            timeout "${secs}s" "$@"
            return $?
        fi
        if command -v gtimeout >/dev/null 2>&1; then
            gtimeout "${secs}s" "$@"
            return $?
        fi
        python3 -c $'import subprocess,sys\nsecs=float(sys.argv[1])\ncmd=sys.argv[2:]\ntry:\n    p=subprocess.run(cmd, timeout=secs)\n    sys.exit(p.returncode)\nexcept subprocess.TimeoutExpired:\n    sys.exit(124)' "$secs" "$@"
    }
    cleanup() {
        for pid in "${s0_pid:-}" "${s1_pid:-}" "${s2_pid:-}" "${mgr_pid:-}"; do
            if [ -n "${pid}" ] && kill -0 "${pid}" >/dev/null 2>&1; then
                kill "${pid}" >/dev/null 2>&1 || true
                wait "${pid}" >/dev/null 2>&1 || true
            fi
        done
    }
    trap cleanup EXIT
    ./bin/manager --listen "{{manager}}" --servers "{{servers}}" >"$mgr_log" 2>&1 &
    mgr_pid=$!
    ./bin/server --id 0 --manager "{{manager}}" --listen "0.0.0.0:${p0}" --backer "${backer_base}.s0" >"$s0_log" 2>&1 &
    s0_pid=$!
    ./bin/server --id 1 --manager "{{manager}}" --listen "0.0.0.0:${p1}" --backer "${backer_base}.s1" >"$s1_log" 2>&1 &
    s1_pid=$!
    ./bin/server --id 2 --manager "{{manager}}" --listen "0.0.0.0:${p2}" --backer "${backer_base}.s2" >"$s2_log" 2>&1 &
    s2_pid=$!
    for _ in $(seq 1 60); do
        if run_with_timeout 2 ./bin/client --manager "{{manager}}" --op get --key k0 >/dev/null 2>&1; then
            break
        fi
        sleep 0.2
    done
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
        echo "=== init writes/reads ==="
        printf '%s\n' \
            "PUT k0 v0" \
            "PUT k1 v1" \
            "PUT k3 v3" \
            "PUT k7 v7" \
            "SWAP k0 v0b" \
            "SWAP k1 v1b" \
            "SWAP k3 v3b" \
            "GET k0" \
            "GET k1" \
            "GET k3" \
            "SCAN k0 k3" \
            "STOP" \
            | ./bin/client --manager "{{manager}}"
        echo "=== kill server 1 ==="
        kill "$s1_pid"
        wait "$s1_pid" 2>/dev/null || true
        s1_pid=""
        echo "=== unaffected get+scan (partition 0) ==="
        run_with_timeout 4 ./bin/client --manager "{{manager}}" --op get --key k1
        run_with_timeout 4 ./bin/client --manager "{{manager}}" --op scan --start k1 --end k1
        echo "=== affected get should timeout (partition 1) ==="
        if run_with_timeout 4 ./bin/client --manager "{{manager}}" --op get --key k0; then
            echo "expected timeout on failed partition but command succeeded"
            exit 1
        else
            rc=$?
            if [ "$rc" -ne 124 ]; then
                echo "expected timeout exit code 124, got $rc"
                exit 1
            fi
            echo "timeout observed as expected"
        fi
        echo "=== restart server 1 ==="
        ./bin/server --id 1 --manager "{{manager}}" --listen "0.0.0.0:${p1}" --backer "${backer_base}.s1" >"$s1_log" 2>&1 &
        s1_pid=$!
        for _ in $(seq 1 60); do
            if run_with_timeout 3 ./bin/client --manager "{{manager}}" --op get --key k0 >/dev/null 2>&1; then
                break
            fi
            sleep 0.2
        done
        echo "=== recovered partition check ==="
        run_with_timeout 4 ./bin/client --manager "{{manager}}" --op get --key k0
    } | tee "$log"
    assert "PUT k0 not_found"
    assert "PUT k1 not_found"
    assert "PUT k3 not_found"
    assert "SWAP k0 v0"
    assert "SWAP k1 v1"
    assert "SWAP k3 v3"
    assert "GET k1 v1b"
    assert "SCAN k1 k1"
    assert "timeout observed as expected"
    assert "GET k0 v0b"
    echo "deterministic testcase passed: $log"

# run your KV store manager
manager man_port="3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779":
    cd kvstore && ./bin/manager --listen "0.0.0.0:{{man_port}}" --servers "{{servers}}"

# run your KV store server
server id="0" manager="127.0.0.1:3666" api_port="3777" backer_path="./backer.s0":
    cd kvstore && ./bin/server \
        --id "{{id}}" \
        --manager "{{manager}}" \
        --listen "0.0.0.0:{{api_port}}" \
        --backer "{{backer_path}}"

# run you KV store client in stdin/out interface mode
client manager="127.0.0.1:3666":
    cd kvstore && ./bin/client --manager "{{manager}}"

# kill all processes of your KV store system
kill:
    -pkill -f 'kvstore/bin/manager' >/dev/null 2>&1
    -pkill -f 'kvstore/bin/server' >/dev/null 2>&1
    -pkill -f 'kvstore/bin/client' >/dev/null 2>&1

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p2"

# launch the KV service components on node (see README for conventions)
service node="s0" manager="127.0.0.1:3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779" backer_prefix="./backer":
    just p2::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p2::server \
            $({{python_run}} justmod/xtract.py partid "{{node}}") \
            "{{manager}}" \
            $({{python_run}} justmod/xtract.py portof "{{servers}}" "{{node}}") \
            "{{backer_prefix}}.{{node}}" \
        --manager-just-args p2::manager \
            $({{python_run}} justmod/xtract.py portof "{{manager}}" "{{node}}") \
            "{{servers}}" \
        --node-id "{{node}}"

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a fuzz testing scenario
fuzz nservers="1" crashing="no" manager="127.0.0.1:3666": (tmpdir "fuzz")
    just p2::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 \
        --conflict \
        --client-just-args p2::client "{{manager}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{nservers}}-{{crashing}}.log"
    just p2::kill

# run a YCSB benchmark workload
bench nclis wload nservers="1" manager="127.0.0.1:3666": (tmpdir "bench")
    just p2::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p2::client "{{manager}}" \
        | tee "{{tmpdir_prefix}}/bench/bench-{{nclis}}-{{wload}}-{{nservers}}.log"
    just p2::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj2.py
