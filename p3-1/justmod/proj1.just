set working-directory := '..'

# list project 1 just recipes
default:
    @just --list p1 --unsorted

# install extra dependencies of your KV code
deps:
    # DONE: your dependency installation commands here
    sudo apt-get update
    sudo apt-get install -y protobuf-compiler

# build your executables in release mode
build:
    # DONE: your build commands here (don't include clean)
    cargo build --release

# clean the build of your executables
clean:
    # DONE: your clean commands here
    cargo clean

# run your KV store server
server listen="0.0.0.0:3777":
    # DONE: your server run command here, listening on addr for new clients
    cargo run -p kvserver -r -- {{listen}}

# run you KV store client in stdin/out interface mode
client server="127.0.0.1:3777":
    # DONE: your client run command here, connecting to server at addr
    cargo run -p kvclient -r -- {{server}}

# run your KV store testcase 1 client
test1 server="127.0.0.1:3777":
    # DONE: your testcase 1 client run command here
    #        for testcases, feel free to reuse 'just p1::client' if you
    #        designed your clients this way, or invoke different executables
    cargo run -r --bin kvtester -- 1 {{server}}

# run your KV store testcase 2 client
test2 server="127.0.0.1:3777":
    # DONE: your testcase 2 client run command here
    cargo run -r --bin kvtester -- 2 {{server}}

# run your KV store testcase 3 client
test3 server="127.0.0.1:3777":
    # DONE: your testcase 3 client run command here
    cargo run -r --bin kvtester -- 3 {{server}}

# run your KV store testcase 4 client(s)
test4 server="127.0.0.1:3777":
    # DONE: your testcase 4 client(s) run commands here
    #        for multi-client testcases, this recipe should run multiple
    #        concurrent client processes; you may write an additional runner
    #        utility or a scripting layer to manage multiple processes
    cargo run -r --bin kvtester -- 4 {{server}}

# run your KV store testcase 5 client(s)
test5 server="127.0.0.1:3777":
    # DONE: your testcase 5 client(s) run commands here
    cargo run -r --bin kvtester -- 5 {{server}}

# kill all processes of your KV store system
kill:
    # DONE: your kill commands here
    #        make sure that it kills all server, clients, and any extra helper
    #        processes of your system; redirecting both out & err to /dev/null
    #        is recommended
    #   TIP: commands such as 'pkill' may return non-zero exit code on success,
    #        which would by default abort the 'just' recipe early; prepend the
    #        command with a '-' sign to ignore its exit code
    -pkill -9 -x "kvserver" 2>/dev/null || true
    -pkill -9 -x "kvclient" 2>/dev/null || true

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

# Custom recipe
unittest:
    cargo test -p kvstore
    cargo test -p kvserver

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p1"

# launch a long-running KV store service
service listen="0.0.0.0:3777":
    just p1::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p1::server "{{listen}}" \
        --manager-just-args none

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a student-provided testcase
testcase num server="127.0.0.1:3777": (tmpdir "tests")
    just p1::build
    just "p1::test{{num}}" "{{server}}" \
        | tee "{{tmpdir_prefix}}/tests/test{{num}}.log"
    just p1::kill

# run a fuzz testing scenario
fuzz nclis conflict="no" server="127.0.0.1:3777": (tmpdir "fuzz")
    just p1::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis "{{nclis}}" \
        {{ if conflict == "no" { "" } else { "--conflict" } }} \
        --client-just-args p1::client "{{server}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{nclis}}-{{conflict}}.log"
    just p1::kill

# run a YCSB benchmark workload
bench nclis wload server="127.0.0.1:3777": (tmpdir "bench")
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
