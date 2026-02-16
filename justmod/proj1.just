set working-directory := '..'

# list project 1 just recipes
default:
    @just --list p1 --unsorted

deps:
    #!/usr/bin/env bash
    set -euo pipefail
    cd kvstore
    export PATH="$(go env GOPATH)/bin:$PATH"
    if ! command -v protoc >/dev/null 2>&1; then
        echo "protoc not found; please install Protocol Buffers (protoc) first."
        exit 1
    fi
    # Install protobuf plugins for Go
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
    # Initialize go module if not exists
    if [ ! -f go.mod ]; then
        go mod init madkv/kvstore
    fi
    # Get dependencies
    go get google.golang.org/grpc
    go get google.golang.org/protobuf
    go get github.com/google/btree
    # Generate protobuf code into gen/kvpb (module-aware output)
    mkdir -p gen/kvpb
    protoc --go_out=. --go_opt=module=madkv/kvstore \
           --go-grpc_out=. --go-grpc_opt=module=madkv/kvstore \
           proto/kvstore.proto
    # Tidy up dependencies
    go mod tidy
    echo "*******Dependencies installed and protobuf code generated*******"


build:
    cd kvstore && mkdir -p bin
    cd kvstore && go build -o bin/server ./server
    cd kvstore && go build -o bin/client ./client
    @echo "*******Built server and client binaries*******"

clean:
    rm -rf kvstore/bin
    rm -rf kvstore/gen
    @echo "*******Cleaned build artifacts*******"

# run your KV store server
server listen_addr="127.0.0.1:50051":
    cd kvstore && ./bin/server --listen {{listen_addr}}

# run you KV store client in stdin/out interface mode
client server="127.0.0.1:50051":
    cd kvstore && ./bin/client --server {{server}}

# run your KV store testcase 1 client
test1 server="127.0.0.1:50051":
    #!/usr/bin/env bash
    set -euo pipefail
    cd kvstore
    run_id="$(date +%s)_$RANDOM"
    k1="a_${run_id}"
    k2="b_${run_id}"
    zend="z_${run_id}"
    out="$(mktemp)"
    assert() {
        local needle="$1"
        if ! grep -Fq "$needle" "$out"; then
            echo "assert failed: $needle" >&2
            echo "---- output ----" >&2
            cat "$out" >&2
            exit 1
        fi
    }
    printf '%s\n' \
        "PUT $k1 1" \
        "PUT $k2 2" \
        "GET $k1" \
        "SWAP $k1 3" \
        "GET $k1" \
        "SCAN $k1 $zend" \
        "DELETE $k2" \
        "GET $k2" \
        "STOP" \
        | ./bin/client --server "{{server}}" >"$out" 2>&1
    assert "PUT $k1 not_found"
    assert "PUT $k2 not_found"
    assert "GET $k1 1"
    assert "SWAP $k1 1"
    assert "GET $k1 3"
    assert "SCAN $k1 $zend BEGIN"
    assert "  $k1 3"
    assert "  $k2 2"
    assert "SCAN END"
    assert "DELETE $k2 found"
    assert "GET $k2 null"
    cat "$out"
    echo "all asserts passed in test1!"

# run your KV store testcase 2 client
test2 server="127.0.0.1:50051":
    #!/usr/bin/env bash
    set -euo pipefail
    cd kvstore
    run_id="$(date +%s)_$RANDOM"
    kx="x_${run_id}"
    ky="y_${run_id}"
    kz="z_${run_id}"
    out="$(mktemp)"
    assert() {
        local needle="$1"
        if ! grep -Fq "$needle" "$out"; then
            echo "assert failed: $needle" >&2
            echo "---- output ----" >&2
            cat "$out" >&2
            exit 1
        fi
    }
    printf '%s\n' \
        "PUT $kx 9" \
        "GET $ky" \
        "SWAP $ky 1" \
        "DELETE $ky" \
        "SCAN $kz $kz" \
        "DELETE $kx" \
        "PÜT $kx 1" \
        "PUT k" \
        "GET" \
        "SCAN" \
        "STOP 55" \
        "STOP" \
        | ./bin/client --server "{{server}}" >"$out" 2>&1
    assert "PUT $kx not_found"
    assert "GET $ky null"
    assert "SWAP $ky null"
    assert "DELETE $ky found"
    assert "SCAN $kz $kz BEGIN"
    assert "SCAN END"
    assert "DELETE $kx found"
    assert "unknown command: PÜT"
    assert "PUT requires 2 arguments: key value"
    assert "GET requires 1 argument: key"
    assert "SCAN requires 2 arguments: start_key end_key"
    assert "STOP takes no arguments"
    cat "$out"
    echo "all asserts passed in test2!"

# run your KV store testcase 3 client
test3 server="127.0.0.1:50051":
    #!/usr/bin/env bash
    set -euo pipefail
    cd kvstore
    # Two clients on non-conflicting keys
    run_id="$(date +%s)_$RANDOM"
    k1="a_${run_id}"
    k2="b_${run_id}"
    out1="$(mktemp)"
    out2="$(mktemp)"
    assert_in() {
        local file="$1"
        local needle="$2"
        if ! grep -Fq "$needle" "$file"; then
            echo "assert failed: $needle" >&2
            echo "---- output ----" >&2
            cat "$file" >&2
            exit 1
        fi
    }
    (
        printf '%s\n' \
            "PUT $k1 1" \
            "GET $k1" \
            "SWAP $k1 2" \
            "GET $k1" \
            "DELETE $k1" \
            "STOP" \
            | ./bin/client --server "{{server}}" >"$out1" 2>&1
    ) &
    (
        printf '%s\n' \
            "PUT $k2 10" \
            "GET $k2" \
            "SWAP $k2 11" \
            "GET $k2" \
            "DELETE $k2" \
            "STOP" \
            | ./bin/client --server "{{server}}" >"$out2" 2>&1
    ) &
    wait
    assert_in "$out1" "PUT $k1 not_found"
    assert_in "$out1" "GET $k1 1"
    assert_in "$out1" "SWAP $k1 1"
    assert_in "$out1" "GET $k1 2"
    assert_in "$out1" "DELETE $k1 found"
    assert_in "$out2" "PUT $k2 not_found"
    assert_in "$out2" "GET $k2 10"
    assert_in "$out2" "SWAP $k2 10"
    assert_in "$out2" "GET $k2 11"
    assert_in "$out2" "DELETE $k2 found"
    echo "--- client 1 ---"
    cat "$out1"
    echo "--- client 2 ---"
    cat "$out2"
    echo "all asserts passed in test3!"

# run your KV store testcase 4 client(s)
test4 server="127.0.0.1:50051":
    #!/usr/bin/env bash
    set -euo pipefail
    cd kvstore
    # Two clients on overlapping keys; exercise PUT/GET/SWAP/DELETE/SCAN
    run_id="$(date +%s)_$RANDOM"
    k="k_${run_id}"
    m="m_${run_id}"
    zend="z_${run_id}"
    out1="$(mktemp)"
    out2="$(mktemp)"
    assert_in() {
        local file="$1"
        local needle="$2"
        if ! grep -Fq "$needle" "$file"; then
            echo "assert failed: $needle" >&2
            echo "---- output ----" >&2
            cat "$file" >&2
            exit 1
        fi
    }
    (
        printf '%s\n' \
            "PUT $k 1" \
            "PUT $m 7" \
            "GET $k" \
            "SWAP $k 2" \
            "GET $k" \
            "SCAN $k $m" \
            "DELETE $m" \
            "GET $m" \
            "STOP" \
            | ./bin/client --server "{{server}}" >"$out1" 2>&1
    ) &
    (
        sleep 0.2
        printf '%s\n' \
            "GET $k" \
            "PUT $k 9" \
            "GET $k" \
            "SCAN $k $zend" \
            "DELETE $k" \
            "GET $k" \
            "STOP" \
            | ./bin/client --server "{{server}}" >"$out2" 2>&1
    ) &
    wait
    assert_in "$out1" "PUT $k not_found"
    assert_in "$out1" "PUT $m not_found"
    assert_in "$out1" "SWAP $k "
    assert_in "$out1" "SCAN $k $m BEGIN"
    assert_in "$out1" "DELETE $m found"
    if ! awk -v key="$k" '$1=="GET" && $2==key && $3!="null" {found=1} END{exit !found}' "$out2"; then
        echo "assert failed: expected non-null GET $k" >&2
        echo "---- output ----" >&2
        cat "$out2" >&2
        exit 1
    fi
    assert_in "$out2" "PUT $k found"
    assert_in "$out2" "SCAN $k $zend BEGIN"
    echo "--- client 1 ---"
    cat "$out1"
    echo "--- client 2 ---"
    cat "$out2"
    echo "all asserts passed in test4!"

# run your KV store testcase 5 client(s)
test5 server="127.0.0.1:50051":
    #!/usr/bin/env bash
    set -euo pipefail
    cd kvstore
    # Overlapping keys with three clients; more interleaving and full op coverage
    run_id="$(date +%s)_$RANDOM"
    y="y_${run_id}"
    z="z_${run_id}"
    out1="$(mktemp)"
    out2="$(mktemp)"
    out3="$(mktemp)"
    assert_in() {
        local file="$1"
        local needle="$2"
        if ! grep -Fq "$needle" "$file"; then
            echo "assert failed: $needle" >&2
            echo "---- output ----" >&2
            cat "$file" >&2
            exit 1
        fi
    }
    (
        printf '%s\n' \
            "PUT $z 1" \
            "PUT $y 5" \
            "GET $z" \
            "SCAN $y $z" \
            "STOP" \
            | ./bin/client --server "{{server}}" >"$out1" 2>&1
    ) &
    (
        sleep 0.2
        printf '%s\n' \
            "SWAP $z 2" \
            "GET $z" \
            "DELETE $y" \
            "GET $y" \
            "SCAN x $z" \
            "STOP" \
            | ./bin/client --server "{{server}}" >"$out2" 2>&1
    ) &
    (
        sleep 0.4
        printf '%s\n' \
            "GET $z" \
            "PUT $z 3" \
            "GET $z" \
            "DELETE $z" \
            "GET $z" \
            "STOP" \
            | ./bin/client --server "{{server}}" >"$out3" 2>&1
    ) &
    wait
    assert_in "$out1" "PUT $z not_found"
    assert_in "$out1" "PUT $y not_found"
    assert_in "$out1" "SCAN $y $z BEGIN"
    assert_in "$out2" "SWAP $z 1"
    assert_in "$out2" "GET $z 2"
    assert_in "$out2" "DELETE $y found"
    assert_in "$out2" "GET $y null"
    assert_in "$out3" "GET $z null"
    echo "--- client 1 ---"
    cat "$out1"
    echo "--- client 2 ---"
    cat "$out2"
    echo "--- client 3 ---"
    cat "$out3"
    echo "all asserts passed in test5!"

# kill all processes of your KV store system
kill:
    -pkill -f "kvstore/bin/server" 2>/dev/null
    -pkill -f "kvstore/bin/client" 2>/dev/null
    -pkill -f "bin/server" 2>/dev/null
    -pkill -f "bin/client" 2>/dev/null

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p1"

# launch a long-running KV store service
service listen="0.0.0.0:50051":
    just p1::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p1::server "{{listen}}" \
        --manager-just-args none

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a student-provided testcase
testcase num server="127.0.0.1:50051": (tmpdir "tests")
    just p1::build
    just "p1::test{{num}}" "{{server}}" \
        | tee "{{tmpdir_prefix}}/tests/test{{num}}.log"
    just p1::kill

# run a fuzz testing scenario
fuzz nclis conflict="no" server="127.0.0.1:50051": (tmpdir "fuzz")
    just p1::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis "{{nclis}}" \
        {{ if conflict == "no" { "" } else { "--conflict" } }} \
        --client-just-args p1::client "{{server}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{nclis}}-{{conflict}}.log"
    just p1::kill

# run a YCSB benchmark workload
bench nclis wload server="127.0.0.1:50051": (tmpdir "bench")
    just p1::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p1::client "{{server}}" \
        | tee "/tmp/madkv-p1/bench/bench-{{nclis}}-{{wload}}.log"
    just p1::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj1.py
