fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/master.proto")?;

    // Volume proto generates to a subdirectory under OUT_DIR to avoid
    // filename collision with master.proto — both have package=powerfs,
    // so prost emits "powerfs.rs" for each. The subdirectory keeps them
    // separate while staying out of src/ (the standard OUT_DIR location
    // is not tracked by git).
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR not set");
    let volume_out_dir = format!("{}/volume_proto", out_dir);
    std::fs::create_dir_all(&volume_out_dir).expect("failed to create volume_proto out dir");
    tonic_build::configure().out_dir(&volume_out_dir).compile(
        &["../powerfs-volume/proto/powerfs.proto"],
        &["../powerfs-volume/proto"],
    )?;
    Ok(())
}
