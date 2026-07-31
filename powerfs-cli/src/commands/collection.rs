use crate::client::MasterClient;
use clap::{Args, Subcommand};

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
    /// Create a new collection
    Create {
        /// Collection name
        name: String,
        /// Replication placement (e.g. 001 = single replica)
        #[arg(short, long, default_value = "001")]
        replication: String,
        /// Disk type (e.g. hdd, ssd, nvme)
        #[arg(short, long, default_value = "hdd")]
        disk_type: String,
        /// TTL string (optional)
        #[arg(long)]
        ttl: Option<String>,
        /// Max volume count (0 = unlimited)
        #[arg(long, default_value = "0")]
        max_volume_count: u64,
    },
    /// Delete a collection
    Delete { name: String },
    /// Show statistics for a collection
    Stats { name: String },
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
            }

            println!("\n=== Collections ({}) ===", resp.collections.len());
            for c in resp.collections {
                println!(
                    "  {}  replication={} disk={} volumes={}/{} created={}",
                    c.name,
                    c.replication,
                    c.disk_type,
                    c.volume_count,
                    if c.max_volume_count == 0 {
                        "unlimited".to_string()
                    } else {
                        c.max_volume_count.to_string()
                    },
                    c.created_at
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
                Some(c) => {
                    println!("\n=== Collection: {} ===", c.name);
                    println!("  Replication:     {}", c.replication);
                    println!("  Disk type:       {}", c.disk_type);
                    println!("  TTL:             {}", c.ttl);
                    println!("  Volume count:    {}", c.volume_count);
                    println!(
                        "  Max volume:      {}",
                        if c.max_volume_count == 0 {
                            "unlimited".to_string()
                        } else {
                            c.max_volume_count.to_string()
                        }
                    );
                    println!("  Created at:      {}", c.created_at);
                    println!("  Modified at:     {}", c.modified_at);
                }
                None => println!("Collection not found"),
            }
        }
        CollectionCommand::Create {
            name,
            replication,
            disk_type,
            ttl,
            max_volume_count,
        } => {
            let req = powerfs_master::proto::powerfs::CreateCollectionRequest {
                name: name.clone(),
                replication,
                ttl: ttl.unwrap_or_default(),
                disk_type,
                max_volume_count,
            };
            let resp = service
                .create_collection(tonic::Request::new(req))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();

            if resp.success {
                println!("Collection '{}' created successfully", name);
                if let Some(c) = resp.collection {
                    println!(
                        "  replication={} disk={} max_volumes={}",
                        c.replication, c.disk_type, c.max_volume_count
                    );
                }
            } else {
                println!("Failed to create collection: {}", resp.error);
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
                .get_statistics(tonic::Request::new(
                    powerfs_master::proto::powerfs::StatisticsRequest {
                        collection: name.clone(),
                        data_center: String::new(),
                        rack: String::new(),
                    },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();

            if !resp.error.is_empty() {
                println!("Error: {}", resp.error);
                return Ok(());
            }

            let stats = resp
                .collection_stats
                .into_iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| powerfs_master::proto::powerfs::CollectionStats {
                    name: name.clone(),
                    volume_count: 0,
                    total_size: 0,
                    used_size: 0,
                });

            println!("\n=== Collection Stats: {} ===", stats.name);
            println!("  Volume count: {}", stats.volume_count);
            println!("  Total size:   {} bytes", stats.total_size);
            println!("  Used size:    {} bytes", stats.used_size);
        }
    }

    Ok(())
}
