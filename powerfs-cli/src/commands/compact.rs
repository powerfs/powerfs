use crate::volume_client::VolumeServerClient;
use clap::Args;

#[derive(Args)]
pub struct CompactArgs {
    /// Volume server address (e.g., localhost:8081)
    #[arg(long, default_value = "localhost:8081")]
    volume_server: String,

    /// Volume ID to compact
    #[arg(short = 'i', long)]
    volume_id: u64,
}

pub async fn compact(args: CompactArgs) -> super::CommandResult {
    println!(
        "Compacting volume {} on volume server {}",
        args.volume_id, args.volume_server
    );

    let mut client = VolumeServerClient::new(&args.volume_server);
    let (reclaimed, moved) = client.compact_volume(args.volume_id).await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Compact failed: {}", e))
    })?;

    println!(
        "Compact succeeded: reclaimed={} bytes, moved={} needles",
        reclaimed, moved
    );
    Ok(())
}
