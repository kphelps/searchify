use protoc_grpcio;
use tower_grpc_build;

fn main() {
    let proto_root = "proto";
    let proto_file = format!("{}/internal.proto", proto_root);
    println!("cargo:rerun-if-changed={}", proto_file);
    protoc_grpcio::compile_grpc_protos(&[proto_file], &[proto_root], "src/proto/")
        .expect("Failed to compile gRPC definitions!");

    println!("cargo:rerun-if-changed={}", "proto/gossip.proto");
    tower_grpc_build::Config::new()
        .enable_server(true)
        .enable_client(true)
        .build(&["proto/gossip.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
}
