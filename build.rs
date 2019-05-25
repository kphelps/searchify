use protoc_grpcio;

fn main() {
    let proto_root = "proto";
    let files = ["internal.proto", "gossip.proto"];
    let file_paths = files
        .iter()
        .map(|file| format!("{}/{}", proto_root, file))
        .collect::<Vec<String>>();
    file_paths
        .iter()
        .for_each(|path| println!("cargo:rerun-if-changed={}", path));
    protoc_grpcio::compile_grpc_protos(&file_paths, &[proto_root], "src/proto/", None)
        .expect("Failed to compile gRPC definitions!");
}
