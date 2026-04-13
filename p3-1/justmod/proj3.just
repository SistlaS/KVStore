set working-directory := '..'

# list project 3 just recipes
default:
    @just --list p3 --unsorted

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
    cd raft && cargo build --release

# clean the build of your executables
clean:
    # DONE: your clean commands here    
    rm -rf ./backer.*
    cargo clean

# run your KV store manager replica
manager rep_id="0" \
        man_port="3666" \
        p2p_port="3606" \
        peers="127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="3" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782"\
        backer_path="./backer.m.0":
    # DONE: your manager run command here, listening on 0.0.0.0:p2p_port for
    #        peer replica connections and 0.0.0.0:man_port for both server and
    #        client registration requests
    # bash -cu "RUST_LOG=error cargo run -p kvmanager -- --replica_id {{rep_id}} --man_listen 0.0.0.0:{{man_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --server_rf {{server_rf}} --server_addrs {{servers}} --backer_path {{backer_path}} >& m0.log" 
    cargo run -p kvmanager -- \
        --replica_id {{rep_id}} --man_listen 0.0.0.0:{{man_port}} --p2p_listen 0.0.0.0:{{p2p_port}} \
        --peer_addrs {{peers}} --server_rf {{server_rf}} --server_addrs {{servers}} \
        --backer_path {{backer_path}} 


# run your KV store server replica
server part_id="0" \
       rep_id="0" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3777" \
       p2p_port="3707" \
       peers="127.0.0.1:3708,127.0.0.1:3709" \
       backer_path="./backer.s0.0":
    # FIXME: your server run command here. The server should first contact the
    #        replicated managers to get its assignment of key range(s). It then
    #        listens on 0.0.0.0:p2p_port for peer replica connections and
    #        0.0.0.0:api_port for new clients, and stores all durable state under
    #        backer_path
    cargo run -p kvserver -- \
        --partition_id {{part_id}} --replica_id {{rep_id}} \
        --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} \
        --peer_addrs {{peers}} --backer_path {{backer_path}} 


# run you KV store client in stdin/out interface mode
client managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668":
    # FIXME: your client run command here, querying the replicated managers
    #        for server registration information, and then connects to the
    #        servers ready to perform operations
    cargo run -p kvclient --release -- --manager_addrs {{managers}} --quiet
    # bash -cu "RUST_LOG=error cargo run -p kvclient --release -- --manager_addrs {{managers}} |& tee c0.log"


# kill all processes of your KV store system
kill:
    # FIXME: your kill commands here
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
    -pkill -9 -x "raft" 2>/dev/null || true
    -sleep 1

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p3"

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
    cargo run -p runner -r --bin service -- \
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
    just p3::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 \
        --conflict \
        --client-just-args p3::client "{{managers}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{server_rf}}-{{crashing}}.log"
    just p3::kill

# run a YCSB benchmark workload
bench nclis wload server_rf="3" \
      managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668": (tmpdir "bench")
    just p3::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p3::client "{{managers}}" \
        | tee "{{tmpdir_prefix}}/bench/bench-{{nclis}}-{{wload}}-{{server_rf}}.log"
    # just p3::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj3.py

# custom commands
rs0:
    bash -cu "RUST_LOG=error cargo run -p raft -- 3 0 |& tee s0.log" 
rs1:
    bash -cu "RUST_LOG=error cargo run -p raft -- 3 1 |& tee s1.log"
rs2:
    bash -cu "RUST_LOG=error cargo run -p raft -- 3 2 |& tee s2.log" 

kvs00 part_id="0" \
       rep_id="0" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3777" \
       p2p_port="3707" \
       peers="127.0.0.1:3708,127.0.0.1:3709" \
       backer_path="./backer.s0.0":
    bash -cu "RUST_LOG=error cargo run -p kvserver -- --partition_id {{part_id}} --replica_id {{rep_id}} --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --backer_path {{backer_path}} >& s00.log" 

kvs01 part_id="0" \
       rep_id="1" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3778" \
       p2p_port="3708" \
       peers="127.0.0.1:3707,127.0.0.1:3709" \
       backer_path="./backer.s0.1":
    bash -cu "RUST_LOG=error cargo run -p kvserver -- --partition_id {{part_id}} --replica_id {{rep_id}} --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --backer_path {{backer_path}} >& s01.log" 

kvs02 part_id="0" \
       rep_id="2" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3779" \
       p2p_port="3709" \
       peers="127.0.0.1:3707,127.0.0.1:3708" \
       backer_path="./backer.s0.2":
    bash -cu "RUST_LOG=error cargo run -p kvserver -- --partition_id {{part_id}} --replica_id {{rep_id}} --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --backer_path {{backer_path}} >& s02.log" 

kvs10 part_id="1" \
       rep_id="0" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3780" \
       p2p_port="3710" \
       peers="127.0.0.1:3711,127.0.0.1:3712" \
       backer_path="./backer.s1.0":
    bash -cu "RUST_LOG=error cargo run -p kvserver -- --partition_id {{part_id}} --replica_id {{rep_id}} --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --backer_path {{backer_path}} >& s10.log" 

kvs11 part_id="1" \
       rep_id="1" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3781" \
       p2p_port="3711" \
       peers="127.0.0.1:3710,127.0.0.1:3712" \
       backer_path="./backer.s1.1":
    bash -cu "RUST_LOG=error cargo run -p kvserver -- --partition_id {{part_id}} --replica_id {{rep_id}} --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --backer_path {{backer_path}} >& s11.log" 

kvs12 part_id="1" \
       rep_id="2" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3782" \
       p2p_port="3712" \
       peers="127.0.0.1:3710,127.0.0.1:3711" \
       backer_path="./backer.s1.2":
    bash -cu "RUST_LOG=error cargo run -p kvserver -- --partition_id {{part_id}} --replica_id {{rep_id}} --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --backer_path {{backer_path}} >& s12.log" 


m0 rep_id="0" \
        man_port="3666" \
        p2p_port="3606" \
        peers="127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="3" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782"\
        backer_path="./backer.m.0":
    bash -cu "RUST_LOG=error cargo run -p kvmanager -- --replica_id {{rep_id}} --man_listen 0.0.0.0:{{man_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --server_rf {{server_rf}} --server_addrs {{servers}} --backer_path {{backer_path}} |& tee m0.log" 


c0 manager="127.0.0.1:3666":
    # DONE: your client run command here, querying manager at addr for server
    #        registration information
    bash -cu "RUST_LOG=error cargo run -p kvclient --release -- --manager_addrs {{manager}}"

f0 server_rf="3" crashing="no" \
     managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668": (tmpdir "fuzz")
    just p3::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 2 \
        --conflict \
        --client-just-args p3::client "{{managers}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{server_rf}}-{{crashing}}.log"
    just p3::kill

r5p1m0 rep_id="0" \
        man_port="3666" \
        p2p_port="3606" \
        peers="127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="5" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781"\
        backer_path="./backer.m.0":
    bash -cu "RUST_LOG=error cargo run -p kvmanager -- --replica_id {{rep_id}} --man_listen 0.0.0.0:{{man_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --server_rf {{server_rf}} --server_addrs {{servers}} --backer_path {{backer_path}} |& tee m0.log" 


r5p1s00 part_id="0" \
       rep_id="0" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3777" \
       p2p_port="3707" \
       peers="127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711" \
       backer_path="./backer.s0.0":
    bash -cu "RUST_LOG=error cargo run -p kvserver -- --partition_id {{part_id}} --replica_id {{rep_id}} --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --backer_path {{backer_path}} >& s00.log" 

r5p1s01 part_id="0" \
       rep_id="1" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3778" \
       p2p_port="3708" \
       peers="127.0.0.1:3707,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711" \
       backer_path="./backer.s0.1":
    bash -cu "RUST_LOG=error cargo run -p kvserver -- --partition_id {{part_id}} --replica_id {{rep_id}} --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --backer_path {{backer_path}} >& s01.log" 

r5p1s02 part_id="0" \
       rep_id="2" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3779" \
       p2p_port="3709" \
       peers="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3710,127.0.0.1:3711" \
       backer_path="./backer.s0.2":
    bash -cu "RUST_LOG=error cargo run -p kvserver -- --partition_id {{part_id}} --replica_id {{rep_id}} --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --backer_path {{backer_path}} >& s02.log" 

r5p1s03 part_id="0" \
       rep_id="3" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3780" \
       p2p_port="3710" \
       peers="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3711" \
       backer_path="./backer.s0.3":
    bash -cu "RUST_LOG=error cargo run -p kvserver -- --partition_id {{part_id}} --replica_id {{rep_id}} --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --backer_path {{backer_path}} >& s03.log" 

r5p1s04 part_id="0" \
       rep_id="4" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3781" \
       p2p_port="3711" \
       peers="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710" \
       backer_path="./backer.s0.4":
    bash -cu "RUST_LOG=error cargo run -p kvserver -- --partition_id {{part_id}} --replica_id {{rep_id}} --manager_addrs {{managers}} --api_listen 0.0.0.0:{{api_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --backer_path {{backer_path}} >& s04.log"