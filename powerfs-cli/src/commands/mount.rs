use clap::Args;
use log::info;
use std::process::Command;

use super::CommandResult;

#[derive(Args, Debug)]
pub struct MountArgs {
    /// Mount point path
    #[arg(long)]
    mount_point: String,

    /// Master server gRPC address
    #[arg(long, default_value = "localhost:9334")]
    master: String,

    /// Collection name
    #[arg(long, default_value = "default")]
    collection: String,

    /// Replication setting
    #[arg(long, default_value = "000")]
    replication: String,

    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,
}

pub fn mount(args: MountArgs) -> CommandResult {
    info!(
        "Mounting PowerFS at {} (master: {})",
        args.mount_point, args.master
    );

    // Try to find the powerfs-fuse binary
    let binary = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("powerfs-fuse")))
        .filter(|p| p.exists())
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "powerfs-fuse".to_string());

    info!("Using FUSE binary: {}", binary);

    let mut cmd = Command::new(&binary);
    cmd.arg("--master")
        .arg(&args.master)
        .arg("--mount-point")
        .arg(&args.mount_point)
        .arg("--collection")
        .arg(&args.collection)
        .arg("--replication")
        .arg(&args.replication);

    if args.verbose {
        cmd.arg("--verbose");
    }

    // Run in foreground
    let status = cmd.status().map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!(
            "failed to start powerfs-fuse: {}. Make sure the binary is built (cargo build -p powerfs-fuse).",
            e
        ))
    })?;

    if !status.success() {
        return Err(powerfs_common::error::PowerFsError::Internal(format!(
            "powerfs-fuse exited with status: {}",
            status
        )));
    }

    Ok(())
}
