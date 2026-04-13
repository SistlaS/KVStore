fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    println!("Generated files will be in: {}", out_dir);
    tonic_build::compile_protos("../kvrpc/kvrpc.proto").unwrap();
    // tonic_build::configure()
    //     .out_dir("../kvrpc") // Output directory
    //     .compile(&["../kvrpc/kvrpc.proto"], &["../kvrpc"])
    //     .expect("Failed to compile kvstore_rpc.proto");
}
