//! tonic-build 编译 `proto/raft.proto` 生成 gRPC 代码。

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/raft.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/raft.proto");
    Ok(())
}
