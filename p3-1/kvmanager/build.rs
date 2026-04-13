fn main() {
    tonic_build::compile_protos("../kvrpc/mngrpc.proto").unwrap();
}
