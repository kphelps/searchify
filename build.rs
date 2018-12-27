use protoc_grpcio;

fn main() {
    let proto_root = "proto";
    let proto_file = format!("{}/internal.proto", proto_root);
    println!("cargo:rerun-if-changed={}", proto_file);
    protoc_grpcio::compile_grpc_protos(
        &[proto_file],
        &[proto_root],
        "src/proto/",
    ).expect("Failed to compile gRPC definitions!");
}
