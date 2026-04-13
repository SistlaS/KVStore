fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    println!("Generated files will be in: {}", out_dir);
    tonic_build::compile_protos("../kvrpc/raft.proto").unwrap();
}
