use clap::Parser;
use log::{error, info};
use powerfs_common::config::PowerFsConfig;
use powerfs_filer::{
    shard_store::{FileType, InodeInfo, ShardStore},
    ShardId,
};

const POSIX_ROOT_INODE: u64 = 1;

/// PowerFS Initialization Tool
///
/// This tool initializes the Filer metadata store BEFORE starting the Filer service.
/// It directly writes to RocksDB to create the POSIX root inode (inode 1, directory "/").
///
/// Uses the SAME config file as the Filer service to ensure path consistency.
/// Usage: powerfs-init --config /path/to/filer-config.toml
#[derive(Parser)]
#[command(name = "powerfs-init")]
#[command(version = "0.1.0")]
#[command(about = "PowerFS Initialization Tool - Format POSIX root before service startup")]
struct Args {
    /// Path to the Filer config file (same config used by powerfs-filer)
    #[arg(short, long, required = true)]
    config: String,

    /// Overwrite existing data (WARNING: destroys existing metadata!)
    #[arg(long)]
    force: bool,
}

fn main() {
    let args = Args::parse();

    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    info!("PowerFS Initialization Tool v0.1.0");
    info!("====================================");

    // Load config - same config file used by powerfs-filer
    let config = match PowerFsConfig::load_or_error(&args.config) {
        Ok(cfg) => {
            info!("Successfully loaded configuration from: {}", args.config);
            cfg
        }
        Err(e) => {
            error!("ERROR: Failed to load configuration: {}", e);
            error!("You must provide a valid configuration file with all required ports and addresses.");
            std::process::exit(1);
        }
    };

    let data_dir = &config.filer.data_dir;
    let shard_count = config.filer.shard_count as u64;

    info!("Filer data directory: {}", data_dir);
    info!("Shard count: {}", shard_count);

    let shards_dir = format!("{}/shards", data_dir);

    // Check if shard data already exists (check first shard as indicator)
    let first_shard_path = format!("{}/shard_0_data", shards_dir);
    if std::path::Path::new(&first_shard_path).exists() {
        if !args.force {
            error!(
                "Shard data already exists at: {}. Use --force to overwrite.",
                data_dir
            );
            error!("WARNING: This will destroy ALL existing metadata!");
            std::process::exit(1);
        } else {
            info!("--force flag set, will overwrite existing data");
            // Remove existing shard data directories
            for shard_idx in 0..shard_count {
                let shard_path = format!("{}/shard_{}_data", shards_dir, shard_idx);
                if std::path::Path::new(&shard_path).exists() {
                    std::fs::remove_dir_all(&shard_path).unwrap_or_else(|e| {
                        error!("Failed to remove {}: {}", shard_path, e);
                    });
                }
            }
            info!("Removed existing shard data directories");
        }
    }

    // Create data directory and shards subdirectory (must match Filer's structure)
    std::fs::create_dir_all(data_dir).unwrap_or_else(|e| {
        error!("Failed to create data directory: {}", e);
        std::process::exit(1);
    });

    std::fs::create_dir_all(&shards_dir).unwrap_or_else(|e| {
        error!("Failed to create shards directory: {}", e);
        std::process::exit(1);
    });

    // Calculate which shard should hold the POSIX root (inode 1)
    // Must use the SAME shard routing algorithm as Filer's ShardStrategy:
    //   inode_per_shard = min(u64::MAX / shard_count, 1_000_000)
    //   shard_id = (inode / inode_per_shard) % shard_count
    let inode_per_shard = u64::MAX
        .checked_div(shard_count)
        .map(|v| v.min(1_000_000))
        .unwrap_or(1_000_000);
    let root_shard_id = (POSIX_ROOT_INODE / inode_per_shard) % shard_count;

    info!(
        "POSIX root inode (inode={}) will be stored in shard {}",
        POSIX_ROOT_INODE, root_shard_id
    );

    // Initialize each shard
    for shard_idx in 0..shard_count {
        let shard_id = ShardId(shard_idx);
        let shard_path = format!("{}/shard_{}_data", shards_dir, shard_idx);

        info!("Initializing shard {} at {}", shard_idx, shard_path);

        // Calculate inode range for this shard (must match Filer's ShardStrategy::get_shard_range):
        //   start = shard_id * inode_per_shard
        //   end = (shard_id + 1) * inode_per_shard (or u64::MAX for last shard)
        let start_inode = shard_idx * inode_per_shard;
        let end_inode = if shard_idx == shard_count - 1 {
            u64::MAX
        } else {
            (shard_idx + 1) * inode_per_shard
        };
        let inode_range = (start_inode, end_inode);

        // Create and open shard store
        let store = match ShardStore::new(shard_id, inode_range, &shard_path) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to create shard {}: {}", shard_idx, e);
                std::process::exit(1);
            }
        };

        // If this is the root shard, create the POSIX root inode
        if shard_idx == root_shard_id {
            info!("Creating POSIX root inode in shard {}...", shard_idx);

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let root_inode = InodeInfo {
                inode: POSIX_ROOT_INODE,
                name: "/".to_string(),
                parent_inode: 0,
                file_type: FileType::Directory,
                size: 4096,
                mtime: now,
                atime: now,
                ctime: now,
                mode: 0o40755, // Directory with 755 permissions (040755)
                uid: 0,
                gid: 0,
                blocks: 8,
                fid: None,
                volume_id: None,
                etag: None,
                chunks: vec![],
                inline_data: None,
                extended: std::collections::HashMap::new(),
                symlink_target: None,
                nlink: 2,
                version: 1,
                delete_time: 0,
                reliability: powerfs_layout::reliability::Reliability::default(),
                reliability_state: powerfs_layout::reliability::ReliabilityState::default(),
                compression_state: powerfs_layout::reliability::CompressionState::default(),
                replica_chunks: Vec::new(),
            };

            match store.create_inode_sync(root_inode.clone()) {
                Ok(()) => {
                    // Also add root directory entry for "."
                    if let Err(e) = store.add_dir_entry_sync(0, "/", POSIX_ROOT_INODE) {
                        error!("Warning: Failed to add dir entry for root: {}", e);
                    }

                    // Set root inode mapping with sync
                    store.set_root_inode_sync("/", POSIX_ROOT_INODE);

                    info!(
                        "Successfully created POSIX root inode: inode={}, name={}, mode={:o}",
                        root_inode.inode, root_inode.name, root_inode.mode
                    );
                }
                Err(e) => {
                    error!("Failed to create POSIX root inode: {}", e);
                    std::process::exit(1);
                }
            }

            // Verify the creation
            match store.get_inode(POSIX_ROOT_INODE) {
                Some(inode) => {
                    info!(
                        "Verification (memory): POSIX root inode exists - {}",
                        inode.name
                    );
                }
                None => {
                    error!("Verification failed: POSIX root inode not found in memory!");
                    std::process::exit(1);
                }
            }

            // Also verify directly in RocksDB
            if store.verify_inode_in_db(POSIX_ROOT_INODE) {
                info!("Verification (RocksDB): POSIX root inode exists in database");
            } else {
                error!("Verification failed: POSIX root inode NOT FOUND in RocksDB!");
                error!("Data was NOT persisted correctly!");
                std::process::exit(1);
            }
        }

        // Save root inodes mapping
        store.save_root_inodes();

        // Force flush data to disk to ensure persistence
        info!("Flushing shard {} data to disk...", shard_idx);
        if let Err(e) = store.flush() {
            error!("Failed to flush shard {}: {}", shard_idx, e);
            std::process::exit(1);
        }
        info!("Shard {} flushed successfully", shard_idx);

        // Verify data is still in RocksDB after flush
        if shard_idx == root_shard_id {
            if store.verify_inode_in_db(POSIX_ROOT_INODE) {
                info!("Post-flush verification: POSIX root inode still exists in database");
            } else {
                error!("Post-flush verification FAILED: POSIX root inode lost after flush!");
                std::process::exit(1);
            }
        }

        // Wait for filesystem to fully sync before closing the store
        std::thread::sleep(std::time::Duration::from_millis(500));

        info!("Shard {} initialized successfully", shard_idx);
    }

    info!("");
    info!("====================================");
    info!("PowerFS initialization completed!");
    info!("  Config file: {}", args.config);
    info!("  Data directory: {}", data_dir);
    info!("  Shard count: {}", shard_count);
    info!("  POSIX root inode: {} (/)", POSIX_ROOT_INODE);
    info!("");
    info!("Next steps:");
    info!("  1. Start the Filer service: powerfs-filer --config <same-config.toml>");
    info!("  2. Mount with FUSE: powerfs-fuse --config <fuse-config.toml>");
    info!("====================================");
}
