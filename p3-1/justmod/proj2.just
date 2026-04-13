set working-directory := '..'

# list project 2 just recipes
default:
    @just --list p2 --unsorted

# install extra dependencies of your KV code
deps:
    # DONE: your dependency installation commands here
    sudo apt update
    sudo apt-get install -y protobuf-compiler
    sudo apt install -y clang llvm-dev libclang-dev
    export LIBCLANG_PATH=$(llvm-config --libdir)
    export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-""}:$(llvm-config --libdir)
    echo $LIBCLANG_PATH
    ls $LIBCLANG_PATH

# build your executables in release mode
build:
    # DONE: your build commands here (don't include clean)
    cargo build --release
    cd kvserver && cargo build --release
    cd kvclient && cargo build --release
    cd kvstore && cargo build --release
    cd kvmanager && cargo build --release
    cd kvtester && cargo build --release

# clean the build of your executables
clean:
    # DONE: your clean commands here
    rm -rf ./backer.*
    cargo clean

# run your KV store manager
manager man_port="3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779":
    # DONE: your manager run command here, listening on 0.0.0.0:man_port for
    #        both server and client registration requests
    cargo run -p kvmanager --release -- --man_listen "0.0.0.0:{{man_port}}" --servers {{servers}}

# run your KV store server
server id="0" manager="127.0.0.1:3666" api_port="3777" backer_path="./backer.s0":
    # DONE: your server run command here. The server should first contact the
    #        manager to get its assignment of key range(s). It then listens on
    #        0.0.0.0:api_port for new clients, and stores all durable state
    #        under backer_path
    cargo run -p kvserver --release -- --manager_addr {{manager}} --api_listen "0.0.0.0:{{api_port}}" --server_id {{id}} --backer_path {{backer_path}}

# run you KV store client in stdin/out interface mode
client manager="127.0.0.1:3666":
    # DONE: your client run command here, querying manager at addr for server
    #        registration information
    cargo run -p kvclient --release -- --manager_addr {{manager}}

# kill all processes of your KV store system
kill:
    # DONE: your kill commands here
    #        make sure that it kills all server, clients, and any extra helper
    #        processes of your system; redirecting both out & err to /dev/null
    #        is recommended
    #   TIP: commands such as 'pkill' may return non-zero exit code on success,
    #        which would by default abort the 'just' recipe early; prepend the
    #        command with a '-' sign to ignore its exit code
    -pkill -9 -x "kvmanager" 2>/dev/null || true
    -sleep 1
    -pkill -9 -x "kvserver" 2>/dev/null || true
    -sleep 1
    -pkill -9 -x "kvclient" 2>/dev/null || true
    -sleep 1

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

mngr man_port="3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779":
    RUST_LOG=debug cargo run -p kvmanager -r -- --man_listen "0.0.0.0:{{man_port}}" --servers {{servers}} 2>&1 | tee mngr.log

s0 id="0" manager="127.0.0.1:3666" api_port="3777" backer_path="./backer.s0":
    RUST_LOG=debug cargo run -p kvserver -r -- --manager_addr {{manager}} --api_listen "0.0.0.0:{{api_port}}" --server_id {{id}} --backer_path {{backer_path}} 2>&1 | tee s0.log

s1 id="1" manager="127.0.0.1:3666" api_port="3778" backer_path="./backer.s1":
    RUST_LOG=debug cargo run -p kvserver -r -- --manager_addr {{manager}} --api_listen "0.0.0.0:{{api_port}}" --server_id {{id}} --backer_path {{backer_path}} 2>&1 | tee s1.log

s2 id="2" manager="127.0.0.1:3666" api_port="3779" backer_path="./backer.s2":
    RUST_LOG=debug cargo run -p kvserver -r -- --manager_addr {{manager}} --api_listen "0.0.0.0:{{api_port}}" --server_id {{id}} --backer_path {{backer_path}} 2>&1 | tee s2.log

s3 id="3" manager="127.0.0.1:3666" api_port="3780" backer_path="./backer.s3":
    RUST_LOG=debug cargo run -p kvserver -r -- --manager_addr {{manager}} --api_listen "0.0.0.0:{{api_port}}" --server_id {{id}} --backer_path {{backer_path}} 2>&1 | tee s2.log

s4 id="4" manager="127.0.0.1:3666" api_port="3781" backer_path="./backer.s4":
    RUST_LOG=debug cargo run -p kvserver -r -- --manager_addr {{manager}} --api_listen "0.0.0.0:{{api_port}}" --server_id {{id}} --backer_path {{backer_path}} 2>&1 | tee s2.log


c0 manager="127.0.0.1:3666":
    # DONE: your client run command here, querying manager at addr for server
    #        registration information
    RUST_LOG=debug cargo run -p kvclient --release -- --manager_addr {{manager}} 2>&1 | tee c.log

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

# Default IP addresses
default-manager-ip := "127.0.0.1"
default-server-ip := "127.0.0.1"

# Runs test1.sh, depends on build
test1 manager-ip server-ip: build
    @echo "Running test1.sh with manager IP '{{manager-ip}}' and server IP '{{server-ip}}'"
    bash test_scripts/test1.sh --manager-ip {{manager-ip}} --server-ip {{server-ip}}

test1-default: build
    just p2 test1 {{default-manager-ip}} {{default-server-ip}}


# Recipe to run test2.py with custom manager and server IPs
test2 manager-ip server-ip: build
    @echo "Running test2.py with manager IP '{{manager-ip}}' and server IP '{{server-ip}}'"
    python3 test_scripts/test2.py --manager-ip {{manager-ip}} --server-ip {{server-ip}}

# Recipe to run test2.py with default IPs
test2-default: build
    just p2 test2 {{default-manager-ip}} {{default-server-ip}}
