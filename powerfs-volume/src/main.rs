use clap::Parser;
use log::{info, warn};
use powerfs_common::{config::PowerFsConfig, types::NodeId};
use powerfs_core::storage::StorageManager;
use powerfs_volume::{
    master_client::MasterClient, master_client::NewMasterClientParams, server::VolumeServer,
};
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

    #[arg(long, default_values = ["localhost:9333"])]
    master_address: Vec<String>,

    #[arg(long, default_value = "./data")]
    data_dir: String,

    #[arg(long)]
    volume_size: Option<u64>,

    #[arg(long)]
    initial_volume_count: Option<u32>,

    #[arg(long, default_value = "true")]
    register_with_master: bool,

    #[arg(long, short = 'c')]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let cfg = load_config(&args.config);
    let volume_cfg = cfg.volume;

    let log_level = cfg.global.log_level;
    env_logger::Builder::new()
        .filter_level(match log_level.as_str() {
            "debug" => log::LevelFilter::Debug,
            "warn" => log::LevelFilter::Warn,
            "error" => log::LevelFilter::Error,
            _ => log::LevelFilter::Info,
        })
        .init();

    powerfs_common::BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .log_startup();

    let grpc_address = if !args.grpc_address.is_empty() && args.grpc_address != "0.0.0.0:8081" {
        args.grpc_address.clone()
    } else {
        format!("{}:{}", "0.0.0.0", volume_cfg.grpc_port)
    };

    let http_port = if args.http_port != 8080 {
        args.http_port
    } else {
        volume_cfg.http_port as u32
    };

    let node_id = if !args.node_id.is_empty() && args.node_id != "volume-server-1" {
        args.node_id.clone()
    } else {
        volume_cfg.node_id.clone()
    };

    let master_address = if !args.master_address.is_empty()
        && args.master_address != vec!["localhost:9333".to_string()]
    {
        args.master_address.clone()
    } else {
        volume_cfg.master_addresses.clone()
    };

    let data_dir = if !args.data_dir.is_empty() && args.data_dir != "./data" {
        args.data_dir.clone()
    } else {
        volume_cfg.data_dir.clone()
    };

    let volume_size = args.volume_size.unwrap_or(volume_cfg.max_volume_size);

    let initial_volume_count = args
        .initial_volume_count
        .unwrap_or(volume_cfg.initial_volume_count);

    info!("Starting PowerFS Volume Server");
    info!("  GRPC Address: {}", grpc_address);
    info!("  HTTP Port: {}", http_port);
    info!("  Node ID: {}", node_id);
    info!("  Data Center: {}", args.data_center);
    info!("  Rack: {}", args.rack);
    info!("  Masters: {}", master_address.join(", "));
    info!("  Data Dir: {}", data_dir);
    info!("  Initial Volume Count: {}", initial_volume_count);

    let node_id = NodeId(node_id);
    let storage_manager = Arc::new(
        StorageManager::new(
            node_id.clone(),
            data_dir.clone(),
            volume_cfg.device_capacity,
        )
        .expect("Failed to create storage manager"),
    );

    let grpc_port = grpc_address
        .split(':')
        .next_back()
        .and_then(|p| p.parse().ok())
        .unwrap_or(http_port + 1);

    let ip = grpc_address
        .split(':')
        .next()
        .unwrap_or("127.0.0.1")
        .to_string();

    let volume_server = VolumeServer::new(
        storage_manager.clone(),
        node_id.clone(),
        &ip,
        grpc_port,
        http_port,
        &data_dir,
    );

    let master_addrs: Vec<&str> = master_address.iter().map(|s| s.as_str()).collect();
    let master_client = MasterClient::new(NewMasterClientParams {
        master_addresses: &master_addrs,
        node_id: node_id.clone(),
        grpc_port,
        http_port,
        data_center: &args.data_center,
        rack: &args.rack,
        public_url: &format!("http://{}:{}", ip, http_port),
        ip: &ip,
    });

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
                    used: v.used,
                })
                .collect();

            if master_client.send_heartbeat(proto_volumes).await.is_err() {
                warn!("Initial heartbeat failed, reconnecting...");
                if let Err(e) = master_client.start_heartbeat().await {
                    warn!("Failed to restart heartbeat: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_secs(20)).await;

            info!("Requesting initial volumes from master...");
            match master_client
                .grow("001", "default", initial_volume_count)
                .await
            {
                Ok(response) => {
                    info!(
                        "grow response: new_volume_ids={:?}, locations={}, error={}",
                        response.new_volume_ids,
                        response.locations.len(),
                        response.error
                    );
                    if !response.new_volume_ids.is_empty() {
                        info!(
                            "Received {} new volume IDs from master",
                            response.new_volume_ids.len()
                        );
                        for &vid in &response.new_volume_ids {
                            if storage_manager
                                .create_volume(powerfs_common::types::VolumeId(vid), volume_size)
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
                        used: v.used,
                    })
                    .collect();

                if master_client.send_heartbeat(proto_volumes).await.is_err() {
                    warn!("Failed to send heartbeat (no active connection)");
                }
            }
        });
    }

    info!("Starting gRPC server on {}", grpc_address);
    volume_server.start(&grpc_address).await?;

    Ok(())
}

fn load_config(config_path: &Option<String>) -> PowerFsConfig {
    match config_path {
        Some(path) => match PowerFsConfig::load_from_file(path) {
            Ok(cfg) => {
                if let Err(e) = cfg.validate() {
                    warn!("Config validation failed: {}, using defaults", e);
                    PowerFsConfig::default()
                } else {
                    info!("Loaded config from: {}", path);
                    cfg
                }
            }
            Err(e) => {
                warn!("Failed to load config file: {}, using defaults", e);
                PowerFsConfig::default()
            }
        },
        None => PowerFsConfig::default(),
    }
}
