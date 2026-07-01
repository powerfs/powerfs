use crate::volume_client::VolumeServerClient;
use clap::Args;

#[derive(Args)]
pub struct WriteArgs {
    #[arg(long, default_value = "localhost:8081")]
    volume_server: String,

    #[arg(short = 'i', long)]
    volume_id: u32,

    #[arg(long)]
    file_key: u64,

    #[arg(short, long)]
    file: String,
}

pub async fn write(args: WriteArgs) -> super::CommandResult {
    println!(
        "Writing file {} to volume {} with file_key {}",
        args.file, args.volume_id, args.file_key
    );

    let data = std::fs::read(&args.file).map_err(powerfs_common::error::PowerFsError::Io)?;

    println!("File size: {} bytes", data.len());

    let mut client = VolumeServerClient::new(&args.volume_server);
    client
        .write_needle(args.volume_id, args.file_key, &data)
        .await
        .map_err(|e| {
            powerfs_common::error::PowerFsError::Internal(format!("Write failed: {}", e))
        })?;

    println!("Successfully written!");
    Ok(())
}
