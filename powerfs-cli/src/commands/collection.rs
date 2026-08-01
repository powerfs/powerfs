use crate::client::MasterClient;
use clap::{Args, Subcommand};
use powerfs_master::proto::powerfs::{
    redundancy, volume_allocation, AutoAllocation, CollectionInfo, CreateCollectionRequest,
    ErasureCodingMode, HybridAllocation, ManualAllocation, Redundancy, ReplicationMode,
    StoragePolicy, UpdateCollectionRequest, VolumeAllocation,
};

#[derive(Args)]
pub struct CollectionArgs {
    #[command(subcommand)]
    command: CollectionCommand,
}

#[derive(Subcommand)]
enum CollectionCommand {
    /// List all collections
    List,
    /// Show details of a collection
    Info { name: String },
    /// Create a new collection with full P0 attributes
    Create {
        name: String,
        /// Redundancy mode: "replication" or "ec"
        #[arg(long, default_value = "replication")]
        redundancy: String,
        /// Number of replicas (for replication mode)
        #[arg(long, default_value_t = 1)]
        copies: u32,
        /// EC data shards (for ec mode)
        #[arg(long)]
        data_shards: Option<u32>,
        /// EC parity shards (for ec mode)
        #[arg(long)]
        parity_shards: Option<u32>,
        /// EC algorithm (for ec mode)
        #[arg(long, default_value = "reed_solomon")]
        algorithm: String,
        /// Disk type (hdd/ssd/nvme/mixed)
        #[arg(short, long, default_value = "hdd")]
        disk_type: String,
        /// Capacity quota in bytes (0 = unlimited)
        #[arg(long, default_value_t = 0)]
        capacity_quota: u64,
        /// Pre-allocated volume count
        #[arg(long, default_value_t = 0)]
        volume_count: u32,
        /// TTL in seconds (0 = never expire)
        #[arg(long, default_value_t = 0)]
        ttl: u32,
        /// Description
        #[arg(long, default_value = "")]
        description: String,
        /// Volume allocation mode: auto/manual/hybrid
        #[arg(long, default_value = "auto")]
        allocation: String,
        /// Auto allocation count (for auto/hybrid mode)
        #[arg(long, default_value_t = 0)]
        alloc_count: u32,
        /// Auto allocation volume size in bytes (for auto mode)
        #[arg(long, default_value_t = 0)]
        alloc_volume_size: u64,
        /// Manual volume IDs (for manual/hybrid mode, comma-separated)
        #[arg(long)]
        volume_ids: Option<String>,
        /// Excluded volume IDs (comma-separated)
        #[arg(long)]
        excluded_volume_ids: Option<String>,
    },
    /// Update a collection
    Update {
        name: String,
        #[arg(long)]
        status: Option<String>, // active/readonly/archived
        #[arg(long)]
        capacity_quota: Option<u64>,
        #[arg(long)]
        ttl: Option<u32>,
        #[arg(long)]
        description: Option<String>,
    },
    /// Delete a collection
    Delete { name: String },
    /// Show statistics for a collection
    Stats { name: String },
}

// Parse a comma-separated list of volume IDs.
fn parse_volume_ids(s: &Option<String>) -> Vec<u64> {
    s.as_ref()
        .map(|s| {
            s.split(',')
                .filter_map(|s| s.trim().parse::<u64>().ok())
                .collect()
        })
        .unwrap_or_default()
}

// Format a byte count into a human-readable string.
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    if bytes == 0 {
        return "0 B".to_string();
    }
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", bytes)
    } else {
        format!("{:.0} {}", size, UNITS[unit])
    }
}

// Render a capacity quota, using "∞" for unlimited (0).
fn format_quota(bytes: u64) -> String {
    if bytes == 0 {
        "∞".to_string()
    } else {
        format_bytes(bytes)
    }
}

// Convert a CollectionStatus value to a display string.
fn status_to_str(status: i32) -> &'static str {
    match status {
        1 => "Active",
        2 => "Readonly",
        3 => "Archived",
        4 => "Deleted",
        _ => "Unspecified",
    }
}

// Parse a status keyword into the proto enum value.
fn parse_status(s: &str) -> Result<i32, String> {
    match s.to_lowercase().as_str() {
        "active" => Ok(1),
        "readonly" => Ok(2),
        "archived" => Ok(3),
        "deleted" => Ok(4),
        "unspecified" => Ok(0),
        _ => Err(format!(
            "invalid status '{}', expected active/readonly/archived/deleted",
            s
        )),
    }
}

// Build a StoragePolicy from CLI redundancy arguments.
fn build_storage_policy(
    redundancy: &str,
    copies: u32,
    data_shards: Option<u32>,
    parity_shards: Option<u32>,
    algorithm: &str,
) -> Result<StoragePolicy, String> {
    let (mode, name, min_write_nodes) = match redundancy.to_lowercase().as_str() {
        "replication" => (
            redundancy::Mode::Replication(ReplicationMode { copies }),
            format!("replication-{}", copies),
            1u32,
        ),
        "ec" => {
            let data =
                data_shards.ok_or_else(|| "--data-shards is required for ec mode".to_string())?;
            let parity = parity_shards
                .ok_or_else(|| "--parity-shards is required for ec mode".to_string())?;
            (
                redundancy::Mode::ErasureCoding(ErasureCodingMode {
                    data_shards: data,
                    parity_shards: parity,
                    algorithm: algorithm.to_string(),
                }),
                format!("ec-{}+{}", data, parity),
                data,
            )
        }
        other => {
            return Err(format!(
                "invalid redundancy mode '{}', expected replication or ec",
                other
            ))
        }
    };
    Ok(StoragePolicy {
        name,
        redundancy: Some(Redundancy { mode: Some(mode) }),
        min_write_nodes,
    })
}

// Build a VolumeAllocation from CLI allocation arguments.
fn build_allocation(
    allocation: &str,
    alloc_count: u32,
    alloc_volume_size: u64,
    volume_ids: &Option<String>,
) -> Result<VolumeAllocation, String> {
    let mode = match allocation.to_lowercase().as_str() {
        "auto" => volume_allocation::Mode::Auto(AutoAllocation {
            count: alloc_count,
            volume_size: alloc_volume_size,
        }),
        "manual" => {
            let ids = parse_volume_ids(volume_ids);
            if ids.is_empty() {
                return Err("--volume-ids is required for manual allocation".to_string());
            }
            volume_allocation::Mode::Manual(ManualAllocation { volume_ids: ids })
        }
        "hybrid" => volume_allocation::Mode::Hybrid(HybridAllocation {
            fixed_volume_ids: parse_volume_ids(volume_ids),
            auto_count: alloc_count,
        }),
        other => {
            return Err(format!(
                "invalid allocation mode '{}', expected auto/manual/hybrid",
                other
            ))
        }
    };
    Ok(VolumeAllocation { mode: Some(mode) })
}

// Format the redundancy part of a storage policy for display.
fn format_redundancy(policy: Option<&StoragePolicy>) -> String {
    if let Some(p) = policy {
        if let Some(r) = &p.redundancy {
            if let Some(mode) = &r.mode {
                return match mode {
                    redundancy::Mode::Replication(rm) => format!("副本 x{}", rm.copies),
                    redundancy::Mode::ErasureCoding(ec) => {
                        format!("EC {}+{}", ec.data_shards, ec.parity_shards)
                    }
                };
            }
        }
    }
    "—".to_string()
}

// Format a VolumeAllocation for display.
fn format_allocation(alloc: Option<&VolumeAllocation>) -> String {
    if let Some(a) = alloc {
        if let Some(mode) = &a.mode {
            return match mode {
                volume_allocation::Mode::Auto(auto) => format!(
                    "auto(count={}, size={})",
                    auto.count,
                    format_bytes(auto.volume_size)
                ),
                volume_allocation::Mode::Manual(m) => format!("manual(ids={:?})", m.volume_ids),
                volume_allocation::Mode::Hybrid(h) => {
                    format!(
                        "hybrid(fixed={:?}, auto={})",
                        h.fixed_volume_ids, h.auto_count
                    )
                }
            };
        }
    }
    "—".to_string()
}

// Render the full details of a collection.
fn print_collection_info(c: &CollectionInfo) {
    println!("\n=== Collection: {} ===", c.name);
    println!("  Status:            {}", status_to_str(c.status));
    println!("  Disk type:         {}", c.disk_type);
    println!(
        "  Description:       {}",
        if c.description.is_empty() {
            "-"
        } else {
            &c.description
        }
    );

    if let Some(p) = &c.storage_policy {
        println!("  Storage policy:    {}", p.name);
        println!(
            "  Redundancy:        {}",
            format_redundancy(c.storage_policy.as_ref())
        );
        if let Some(r) = &p.redundancy {
            if let Some(mode) = &r.mode {
                match mode {
                    redundancy::Mode::Replication(rm) => {
                        println!("    Replicas:        {}", rm.copies);
                    }
                    redundancy::Mode::ErasureCoding(ec) => {
                        println!("    Data shards:     {}", ec.data_shards);
                        println!("    Parity shards:   {}", ec.parity_shards);
                        println!("    Algorithm:       {}", ec.algorithm);
                    }
                }
            }
        }
        println!("  Min write nodes:   {}", p.min_write_nodes);
    } else {
        println!("  Storage policy:    -");
    }

    println!(
        "  Capacity quota:    {}",
        format_quota(c.capacity_quota_bytes)
    );
    println!("  Volume count:      {}", c.volume_count);
    println!(
        "  TTL (seconds):     {}",
        if c.ttl_seconds == 0 {
            "never".to_string()
        } else {
            c.ttl_seconds.to_string()
        }
    );
    println!(
        "  Volume allocation: {}",
        format_allocation(c.volume_allocation.as_ref())
    );
    if c.excluded_volume_ids.is_empty() {
        println!("  Excluded volumes:  -");
    } else {
        println!("  Excluded volumes:  {:?}", c.excluded_volume_ids);
    }
    println!("  Created at:        {}", c.created_at);
    println!("  Updated at:        {}", c.updated_at);
}

pub async fn collection(mut client: MasterClient, args: CollectionArgs) -> super::CommandResult {
    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    match args.command {
        CollectionCommand::List => {
            let resp = service
                .list_collections(tonic::Request::new(
                    powerfs_master::proto::powerfs::ListCollectionsRequest {},
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();

            if !resp.error.is_empty() {
                println!("Error: {}", resp.error);
                return Ok(());
            }

            println!("\n=== Collections ({}) ===", resp.collections.len());
            for c in &resp.collections {
                println!(
                    "  {:<15} {:<9} {:<10} {:<5} {:<11} {} volumes",
                    c.name,
                    status_to_str(c.status),
                    format_redundancy(c.storage_policy.as_ref()),
                    c.disk_type,
                    format_quota(c.capacity_quota_bytes),
                    c.volume_count,
                );
            }
        }
        CollectionCommand::Info { name } => {
            let resp = service
                .get_collection(tonic::Request::new(
                    powerfs_master::proto::powerfs::GetCollectionRequest { name },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();

            if !resp.success {
                println!("Error: {}", resp.error);
                return Ok(());
            }
            match resp.collection {
                Some(c) => print_collection_info(&c),
                None => println!("Collection not found"),
            }
        }
        CollectionCommand::Create {
            name,
            redundancy,
            copies,
            data_shards,
            parity_shards,
            algorithm,
            disk_type,
            capacity_quota,
            volume_count,
            ttl,
            description,
            allocation,
            alloc_count,
            alloc_volume_size,
            volume_ids,
            excluded_volume_ids,
        } => {
            let storage_policy = Some(
                build_storage_policy(&redundancy, copies, data_shards, parity_shards, &algorithm)
                    .map_err(powerfs_common::error::PowerFsError::Internal)?,
            );
            let volume_allocation = Some(
                build_allocation(&allocation, alloc_count, alloc_volume_size, &volume_ids)
                    .map_err(powerfs_common::error::PowerFsError::Internal)?,
            );

            let req = CreateCollectionRequest {
                name: name.clone(),
                status: 1, // Active
                storage_policy,
                disk_type,
                capacity_quota_bytes: capacity_quota,
                volume_count,
                ttl_seconds: ttl,
                description,
                volume_allocation,
                excluded_volume_ids: parse_volume_ids(&excluded_volume_ids),
            };
            let resp = service
                .create_collection(tonic::Request::new(req))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();

            if resp.success {
                println!("Collection '{}' created successfully", name);
                if let Some(c) = resp.collection {
                    print_collection_info(&c);
                }
            } else {
                println!("Failed to create collection: {}", resp.error);
            }
        }
        CollectionCommand::Update {
            name,
            status,
            capacity_quota,
            ttl,
            description,
        } => {
            // Fetch current values to perform a partial update: the proto
            // request carries full scalar fields rather than field masks.
            let current = service
                .get_collection(tonic::Request::new(
                    powerfs_master::proto::powerfs::GetCollectionRequest { name: name.clone() },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();

            if !current.success {
                println!("Error: {}", current.error);
                return Ok(());
            }
            let cur = match current.collection {
                Some(c) => c,
                None => {
                    println!("Collection not found");
                    return Ok(());
                }
            };

            let new_status = match status.as_deref() {
                Some(s) => {
                    parse_status(s).map_err(powerfs_common::error::PowerFsError::Internal)?
                }
                None => cur.status,
            };
            let new_quota = capacity_quota.unwrap_or(cur.capacity_quota_bytes);
            let new_ttl = ttl.unwrap_or(cur.ttl_seconds);
            let new_desc = description.unwrap_or(cur.description);

            let req = UpdateCollectionRequest {
                name: name.clone(),
                status: new_status,
                storage_policy: cur.storage_policy,
                disk_type: cur.disk_type,
                capacity_quota_bytes: new_quota,
                ttl_seconds: new_ttl,
                description: new_desc,
                volume_allocation: cur.volume_allocation,
                excluded_volume_ids: cur.excluded_volume_ids,
            };
            let resp = service
                .update_collection(tonic::Request::new(req))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();

            if resp.success {
                println!("Collection '{}' updated successfully", name);
                if let Some(c) = resp.collection {
                    print_collection_info(&c);
                }
            } else {
                println!("Failed to update collection: {}", resp.error);
            }
        }
        CollectionCommand::Delete { name } => {
            let resp = service
                .delete_collection(tonic::Request::new(
                    powerfs_master::proto::powerfs::DeleteCollectionRequest { name },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();

            if resp.success {
                println!("Collection deleted successfully");
            } else {
                println!("Failed to delete collection: {}", resp.error);
            }
        }
        CollectionCommand::Stats { name } => {
            let resp = service
                .get_collection_stats(tonic::Request::new(
                    powerfs_master::proto::powerfs::GetCollectionStatsRequest {
                        name: name.clone(),
                    },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();

            if !resp.success {
                println!("Error: {}", resp.error);
                return Ok(());
            }
            match resp.stats {
                Some(s) => {
                    println!("\n=== Collection Stats: {} ===", name);
                    println!("  Used bytes:           {}", format_bytes(s.used_bytes));
                    println!("  File count:           {}", s.file_count);
                    println!("  Volume count:         {}", s.volume_count);
                    println!("  Writable volumes:     {}", s.writable_volume_count);
                    println!("  Read ops:             {}", s.read_ops);
                    println!("  Write ops:            {}", s.write_ops);
                    println!("  Read bytes:           {}", format_bytes(s.read_bytes));
                    println!("  Write bytes:          {}", format_bytes(s.write_bytes));
                }
                None => println!("No stats available for '{}'", name),
            }
        }
    }

    Ok(())
}
