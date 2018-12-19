use protoc_grpcio;

fn main() {
    let proto_root = "proto";
    protoc_grpcio::compile_grpc_protos(
        &["proto/internal.proto"],
        &[proto_root],
        "src/proto/",
    ).expect("Failed to compile gRPC definitions!");
}
