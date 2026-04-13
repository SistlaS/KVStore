RUSTFLAGS="-Awarnings" cargo test -- --nocapture

RUSTFLAGS="-Awarnings" cargo run -- --ip_addr 127.0.0.1 ./config.txt

RUST_LOG=debug cargo run -p raft -- 3 0 |& tee s0.log &
RUST_LOG=debug cargo run -p raft -- 3 1 |& tee s0.log &
RUST_LOG=debug cargo run -p raft -- 3 2 |& tee s0.log &