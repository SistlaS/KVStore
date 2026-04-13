
#!/bin/bash
set -ex

# RUST_LOG=debug cargo run -p kvmanager -- --man_listen 0.0.0.0:3666 --servers 128.105.144.81:3777,128.105.144.81:3778,128.105.144.81:3779 |& tee mngr.log

# RUST_LOG=debug cargo run -p kvserver --  --manager_addr 128.105.144.81:3666 --api_listen 0.0.0.0:3778 --server_id 1 --backer_path ./backer.1 |& tee s1.log

# RUST_LOG=debug cargo run -p kvclient --  --manager_addr 128.105.144.81:3666 


# cargo test -- --test-threads=1


# RUST_LOG=debug cargo run -p kvmanager --release -- --man_listen 0.0.0.0:3666 --servers 128.105.144.81:3777 |& tee mngr.log

# RUST_LOG=debug cargo run -p kvserver --release --  --manager_addr 128.105.144.81:3666 --api_listen 0.0.0.0:3777 --server_id 1 --backer_path ./backer.1 |& tee s1.log

# RUST_LOG=debug cargo run -p kvclient --release --  --manager_addr 128.105.144.81:3666 

RUST_LOG=debug cargo run -p kvmanager -- --man_listen 0.0.0.0:3666 --servers 128.105.144.81:3777,128.105.144.81:3778,128.105.144.81:3779 |& tee mngr.log &
RUST_LOG=debug cargo run -p kvserver --  --manager_addr 128.105.144.81:3666 --api_listen 0.0.0.0:3777 --server_id 0 --backer_path ./backer.0 |& tee s0.log &
RUST_LOG=debug cargo run -p kvserver --  --manager_addr 128.105.144.81:3666 --api_listen 0.0.0.0:3778 --server_id 1 --backer_path ./backer.1 |& tee s1.log &
RUST_LOG=debug cargo run -p kvserver --  --manager_addr 128.105.144.81:3666 --api_listen 0.0.0.0:3779 --server_id 2 --backer_path ./backer.2 |& tee s2.log &


just p2 manager 3666 "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779"
just p2 server 0 "127.0.0.1:3666" "3777" "./backer.s0"
just p2 server 1 "127.0.0.1:3666" "3778" "./backer.s1"
just p2 server 2 "127.0.0.1:3666" "3779" "./backer.s2"
just p2::fuzz 3 yes


scp -r scripts/p2/* n1:~/cs739-madkv/scripts/p2/

```
[fuzz 3 servers healthy]     just p2::fuzz 3 no <manager_addr>
  [fuzz 3 servers crashing]  just p2::fuzz 3 yes <manager_addr>
  [fuzz 5 servers crashing]  just p2::fuzz 5 yes <manager_addr>
  [ycsb-a 1 parts 10 clis]   just p2::bench 10 a 1 <server_addr>
  [ycsb-a 3 parts 10 clis]   just p2::bench 10 a 3 <server_addr>
  [ycsb-a 5 parts 10 clis]   just p2::bench 10 a 5 <server_addr>
  [ycsb-b 1 parts 10 clis]   just p2::bench 10 b 1 <server_addr>
  [ycsb-b 3 parts 10 clis]   just p2::bench 10 b 3 <server_addr>
  [ycsb-b 5 parts 10 clis]   just p2::bench 10 b 5 <server_addr>
  [ycsb-c 1 parts 10 clis]   just p2::bench 10 c 1 <server_addr>
  [ycsb-c 3 parts 10 clis]   just p2::bench 10 c 3 <server_addr>
  [ycsb-c 5 parts 10 clis]   just p2::bench 10 c 5 <server_addr>
  [ycsb-d 1 parts 10 clis]   just p2::bench 10 d 1 <server_addr>
  [ycsb-d 3 parts 10 clis]   just p2::bench 10 d 3 <server_addr>
  [ycsb-d 5 parts 10 clis]   just p2::bench 10 d 5 <server_addr>
  [ycsb-e 1 parts 10 clis]   just p2::bench 10 e 1 <server_addr>
  [ycsb-e 3 parts 10 clis]   just p2::bench 10 e 3 <server_addr>
  [ycsb-e 5 parts 10 clis]   just p2::bench 10 e 5 <server_addr>
  [ycsb-f 1 parts 10 clis]   just p2::bench 10 f 1 <server_addr>
  [ycsb-f 3 parts 10 clis]   just p2::bench 10 f 3 <server_addr>
  [ycsb-f 5 parts 10 clis]   just p2::bench 10 f 5 <server_addr>
  [ycsb-a 1 parts 1 clis]    just p2::bench 1 a 1 <server_addr>
  [ycsb-a 1 parts 20 clis]   just p2::bench 20 a 1 <server_addr>
  [ycsb-a 1 parts 30 clis]   just p2::bench 30 a 1 <server_addr>
  [ycsb-a 5 parts 1 clis]    just p2::bench 1 a 5 <server_addr>
  [ycsb-a 5 parts 20 clis]   just p2::bench 20 a 5 <server_addr>
  [ycsb-a 5 parts 30 clis]   just p2::bench 30 a 5 <server_addr>
```