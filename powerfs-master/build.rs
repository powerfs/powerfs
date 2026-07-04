fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/master.proto")?;
    tonic_build::configure()
        .out_dir("src/volume_proto")
        .compile(&["../powerfs-volume/proto/powerfs.proto"], &["../powerfs-volume/proto"])?;
    Ok(())
}
