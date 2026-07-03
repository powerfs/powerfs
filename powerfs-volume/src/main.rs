use clap::Parser;
use log::{info, warn};
use powerfs_common::types::NodeId;
use powerfs_core::storage::StorageManager;
use powerfs_volume::{master_client::MasterClient, server::VolumeServer};
use std::sync::Arc;
use tokio::time::Duration;

#[derive(Parser)]
#[command(name = "powerfs-volume")]
#[command(version = "0.1.0")]
#[command(about = "PowerFS Volume Server")]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:8081")]
    grpc_address: String,

    #[arg(long, default_value = "8080")]
    http_port: u32,

    #[arg(long, default_value = "volume-server-1")]
    node_id: String,

    #[arg(long, default_value = "default")]
    data_center: String,

    #[arg(long, default_value = "default")]
    rack: String,

    #[arg(long, default_value = "localhost:9333")]
    master_address: String,

    #[arg(long, default_value = "./data")]
    data_dir: String,

    #[arg(long, default_value = "1073741824")]
    volume_size: u64,

    #[arg(long, default_value = "true")]
    register_with_master: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args = Args::parse();

    info!("Starting PowerFS Volume Server");
    info!("  GRPC Address: {}", args.grpc_address);
    info!("  HTTP Port: {}", args.http_port);
    info!("  Node ID: {}", args.node_id);
    info!("  Data Center: {}", args.data_center);
    info!("  Rack: {}", args.rack);
    info!("  Master: {}", args.master_address);
    info!("  Data Dir: {}", args.data_dir);

    let node_id = NodeId(args.node_id.clone());
    let storage_manager = Arc::new(StorageManager::new(node_id.clone(), args.data_dir));

    let volume_server = VolumeServer::new(storage_manager.clone(), node_id.clone());

    let grpc_port = args
        .grpc_address
        .split(':')
        .next_back()
        .and_then(|p| p.parse().ok())
        .unwrap_or(args.http_port + 1);

    let mut master_client = MasterClient::new(
        &args.master_address,
        node_id,
        grpc_port,
        args.http_port,
        &args.data_center,
        &args.rack,
        &format!("http://127.0.0.1:{}", args.http_port),
    );

    if args.register_with_master {
        info!("Registering with master...");
        if let Err(e) = master_client.start_heartbeat().await {
            warn!("Failed to start heartbeat: {}", e);
        }

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let volumes = storage_manager.list_volumes();
            let proto_volumes: Vec<powerfs_master::proto::VolumeShortInfo> = volumes
                .into_iter()
                .map(|v| powerfs_master::proto::VolumeShortInfo {
                    volume_id: v.id.0,
                    size: v.size,
                    read_only: v.state == powerfs_common::types::VolumeState::ReadOnly,
                    collection: v.collection.0.clone(),
                    replica_placement: v.replica_count,
                    ttl: v.ttl.0 as u32,
                    disk_type: v.disk_type.0.clone(),
                })
                .collect();

            if let Err(e) = master_client.send_heartbeat(proto_volumes).await {
                warn!("Initial heartbeat failed: {}", e);
            }

            tokio::time::sleep(Duration::from_secs(1)).await;

            info!("Requesting initial volumes from master...");
            match master_client.grow("001", "default", 2).await {
                Ok(response) => {
                    if !response.new_volume_ids.is_empty() {
                        info!(
                            "Received {} new volume IDs from master",
                            response.new_volume_ids.len()
                        );
                        for &vid in &response.new_volume_ids {
                            if storage_manager
                                .create_volume(
                                    powerfs_common::types::VolumeId(vid),
                                    args.volume_size,
                                )
                                .is_ok()
                            {
                                info!("Created volume {}", vid);
                            } else {
                                warn!("Failed to create volume {}", vid);
                            }
                        }
                    } else {
                        warn!("No volume IDs received from master");
                    }
                }
                Err(e) => {
                    warn!("Failed to request volumes from master: {}", e);
                }
            }

            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let volumes = storage_manager.list_volumes();
                let proto_volumes: Vec<powerfs_master::proto::VolumeShortInfo> = volumes
                    .into_iter()
                    .map(|v| powerfs_master::proto::VolumeShortInfo {
                        volume_id: v.id.0,
                        size: v.size,
                        read_only: v.state == powerfs_common::types::VolumeState::ReadOnly,
                        collection: v.collection.0.clone(),
                        replica_placement: v.replica_count,
                        ttl: v.ttl.0 as u32,
                        disk_type: v.disk_type.0.clone(),
                    })
                    .collect();

                if let Err(e) = master_client.send_heartbeat(proto_volumes).await {
                    warn!("Failed to send heartbeat: {}", e);
                }
            }
        });
    }

    info!("Starting gRPC server on {}", args.grpc_address);
    volume_server.start(&args.grpc_address).await?;

    Ok(())
}
