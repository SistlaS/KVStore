fn main() {
    tonic_build::compile_protos("proto/log.proto").unwrap();
}
