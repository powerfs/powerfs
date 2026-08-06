use clap::Parser;
use log::{error, info, warn};
use powerfs_common::{
    config::PowerFsConfig,
    types::{NodeId, VolumeId},
};
use powerfs_core::storage::StorageManager;
use powerfs_net::PowerFsNetServer;
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
    /// 配置文件路径（必填，所有端口和地址必须在配置文件中设置）
    #[arg(short, long, required = true)]
    config: String,

    /// 可选：覆盖节点ID
    #[arg(long)]
    node_id: Option<String>,

    /// 可选：覆盖数据中心
    #[arg(long)]
    data_center: Option<String>,

    /// 可选：覆盖机架
    #[arg(long)]
    rack: Option<String>,

    /// 可选：覆盖数据目录
    #[arg(long)]
    data_dir: Option<String>,

    /// 可选：覆盖卷大小
    #[arg(long)]
    volume_size: Option<u64>,

    /// 可选：覆盖初始卷数量
    #[arg(long)]
    initial_volume_count: Option<u32>,

    /// 可选：是否注册到Master
    #[arg(long)]
    register_with_master: Option<bool>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let cfg = load_config(&args.config);
    let volume_cfg = cfg.volume.clone();

    let log_level = cfg.global.log_level.clone();
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

    // 从配置文件获取所有必需值 - 不再使用硬编码默认值
    let grpc_port = volume_cfg.grpc_port;
    let http_port = volume_cfg.http_port;
    let net_port = volume_cfg.net_port;

    let node_id = args
        .node_id
        .clone()
        .unwrap_or_else(|| volume_cfg.node_id.clone());

    let data_center = args
        .data_center
        .clone()
        .unwrap_or_else(|| "default".to_string());

    let rack = args.rack.clone().unwrap_or_else(|| "default".to_string());

    let master_address = volume_cfg.master_addresses.clone();

    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| volume_cfg.data_dir.clone());

    let volume_size = args.volume_size.unwrap_or(volume_cfg.max_volume_size);

    let initial_volume_count = args
        .initial_volume_count
        .unwrap_or(volume_cfg.initial_volume_count);

    // Volume server binds to all interfaces, but advertises a specific address for clients
    let bind_ip = "0.0.0.0".to_string();
    let grpc_address = format!("{}:{}", bind_ip, grpc_port);

    // Use advertise_addr from config for heartbeat registration
    // This is the IP that FUSE clients will use to connect to this Volume Server
    let ip = volume_cfg
        .advertise_addr
        .filter(|a| !a.is_empty() && a != "0.0.0.0")
        .unwrap_or_else(|| {
            eprintln!("ERROR: volume.advertise_addr must be set to a reachable IP (not 0.0.0.0)");
            std::process::exit(1);
        });

    info!("Starting PowerFS Volume Server");
    info!("  GRPC Address: {}", grpc_address);
    info!("  HTTP Port: {}", http_port);
    info!("  Net Port: {}", net_port);
    info!("  Node ID: {}", node_id);
    info!("  Data Center: {}", data_center);
    info!("  Rack: {}", rack);
    info!("  Masters: {}", master_address.join(", "));
    info!("  Data Dir: {}", data_dir);
    info!("  Initial Volume Count: {}", initial_volume_count);
    info!("  Volume Size: {}", volume_size);

    let node_id = NodeId(node_id);
    let storage_manager = Arc::new(
        StorageManager::new(
            node_id.clone(),
            data_dir.clone(),
            volume_cfg.device_capacity,
        )
        .expect("Failed to create storage manager"),
    );

    // Pre-create volumes at startup with UUID-based IDs
    // First check if volumes already exist on disk (for recovery after restart)
    let _pre_allocated_volumes = {
        let mut pre_allocated = Vec::new();
        let existing_volumes = storage_manager.list_volumes();

        info!(
            "Starting with {} existing volumes in memory",
            existing_volumes.len()
        );

        // Check if volumes exist on disk (for recovery)
        let disk_volumes = discover_volumes_on_disk(&data_dir);
        if !disk_volumes.is_empty() && existing_volumes.is_empty() {
            info!(
                "Recovering {} volumes from disk: {:?}",
                disk_volumes.len(),
                disk_volumes
            );
            for volume_id in &disk_volumes {
                match storage_manager.create_volume(*volume_id, volume_size) {
                    Ok(_) => {
                        info!("Recovered volume {} with size {}", volume_id.0, volume_size);
                        pre_allocated.push(*volume_id);
                    }
                    Err(e) => {
                        warn!("Failed to recover volume {}: {}", volume_id.0, e);
                    }
                }
            }
        } else {
            // Create new volumes with UUID-based IDs
            info!(
                "Creating {} new volumes with UUID-based IDs",
                initial_volume_count
            );
            for _i in 0..initial_volume_count {
                let volume_id = powerfs_common::types::VolumeId::generate();

                match storage_manager.create_volume(volume_id, volume_size) {
                    Ok(_) => {
                        info!(
                            "Created volume {} (UUID-based) with size {}",
                            volume_id.0, volume_size
                        );
                        pre_allocated.push(volume_id);
                    }
                    Err(e) => {
                        warn!("Failed to create volume {}: {}", volume_id.0, e);
                    }
                }
            }
        }

        info!("Total volumes ready: {}", pre_allocated.len());
        pre_allocated
    };

    let volume_server = VolumeServer::new(
        storage_manager.clone(),
        node_id.clone(),
        &ip,
        grpc_port as u32,
        http_port as u32,
        &data_dir,
    )
    .with_lease_enabled(cfg.volume.lease_enabled);

    // Start HTTP metrics & admin endpoints on http_port.
    // Exposes /metrics (Prometheus) and /admin/lease-stats (JSON) for the
    // lease subsystem. Failure to start is non-fatal: log and continue.
    {
        let metrics_addr_str = format!("{}:{}", ip, http_port);
        if let Ok(metrics_addr) = metrics_addr_str.parse() {
            if let Err(e) = powerfs_volume::metrics::start_metrics_server(
                metrics_addr,
                volume_server.range_lease_mgr.clone(),
            )
            .await
            {
                warn!("Failed to start volume metrics server: {}", e);
            }
        } else {
            warn!(
                "Failed to parse metrics bind address {}; metrics endpoint disabled",
                metrics_addr_str
            );
        }
    }

    // Start powerfs-net binary protocol server for Volume
    if net_port > 0 {
        let net_bind_addr = format!("{}:{}", ip, net_port);
        let net_handler = Arc::new(powerfs_volume::net_handler::VolumeNetHandler::new(
            Arc::new(volume_server.clone()),
        ));
        let net_handler: Arc<dyn powerfs_net::NetHandler> = net_handler;

        info!("Starting powerfs-net Volume server on {}", net_bind_addr);
        if let Ok(net_server) =
            PowerFsNetServer::bind_with_manager(&ip, net_port, net_handler).await
        {
            tokio::spawn(async move {
                if let Err(e) = net_server.serve().await {
                    error!("powerfs-net Volume server error: {:?}", e);
                }
            });
        }
    }

    let master_addrs: Vec<&str> = master_address.iter().map(|s| s.as_str()).collect();
    let master_client = MasterClient::new(NewMasterClientParams {
        master_addresses: &master_addrs,
        master_net_port: volume_cfg.master_net_port,
        node_id: node_id.clone(),
        http_port: http_port as u32,
        net_port: net_port as u32,
        ip: &ip,
    });

    let register = args.register_with_master.unwrap_or(true);
    if register {
        info!("Registering with master...");
        if let Err(e) = master_client.start_heartbeat().await {
            warn!("Failed to start heartbeat: {}", e);
        }

        // Spawn background task for heartbeat and volume reporting
        // Volumes are pre-created at startup, no need to request from master
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;

            // Send initial heartbeat with pre-created volumes
            let volumes = storage_manager.list_volumes();
            let proto_volumes: Vec<powerfs_master::proto::VolumeShortInfo> = volumes
                .into_iter()
                .map(|v| {
                    // 从 Volume 结构体获取真实统计 (used/needle_count)
                    let (used, _total, needle_count) = storage_manager
                        .get_volume(&v.id)
                        .map(|vol| vol.get_stats())
                        .unwrap_or((v.used, v.size, 0));
                    powerfs_master::proto::VolumeShortInfo {
                        volume_id: v.id.0,
                        size: v.size,
                        read_only: v.state == powerfs_common::types::VolumeState::ReadOnly,
                        collection: v.collection.0.clone(),
                        replica_placement: v.replica_count,
                        ttl: v.ttl.0 as u32,
                        disk_type: v.disk_type.0.clone(),
                        used,
                        file_count: needle_count,
                        compact_status: 0,
                        append_offset: 0,
                    }
                })
                .collect();

            info!(
                "Sending initial heartbeat with {} pre-created volumes: {:?}",
                proto_volumes.len(),
                proto_volumes
                    .iter()
                    .map(|v| v.volume_id)
                    .collect::<Vec<_>>()
            );

            if master_client.send_heartbeat(proto_volumes).await.is_err() {
                warn!("Initial heartbeat failed, reconnecting...");
                if let Err(e) = master_client.start_heartbeat().await {
                    warn!("Failed to restart heartbeat: {}", e);
                }
            }

            // Continuous heartbeat loop
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let volumes = storage_manager.list_volumes();
                let proto_volumes: Vec<powerfs_master::proto::VolumeShortInfo> = volumes
                    .into_iter()
                    .map(|v| {
                        let (used, _total, needle_count) = storage_manager
                            .get_volume(&v.id)
                            .map(|vol| vol.get_stats())
                            .unwrap_or((v.used, v.size, 0));
                        powerfs_master::proto::VolumeShortInfo {
                            volume_id: v.id.0,
                            size: v.size,
                            read_only: v.state == powerfs_common::types::VolumeState::ReadOnly,
                            collection: v.collection.0.clone(),
                            replica_placement: v.replica_count,
                            ttl: v.ttl.0 as u32,
                            disk_type: v.disk_type.0.clone(),
                            used,
                            file_count: needle_count,
                            compact_status: 0,
                            append_offset: 0,
                        }
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

fn load_config(config_path: &str) -> PowerFsConfig {
    match PowerFsConfig::load_or_error(config_path) {
        Ok(cfg) => {
            info!("Successfully loaded configuration from: {}", config_path);
            cfg
        }
        Err(e) => {
            eprintln!("ERROR: Failed to load configuration: {}", e);
            eprintln!("You must provide a valid configuration file with all required ports and addresses.");
            std::process::exit(1);
        }
    }
}

/// Discover volumes on disk by scanning for volume_* directories
fn discover_volumes_on_disk(data_dir: &str) -> Vec<VolumeId> {
    let mut volumes = Vec::new();
    let volumes_path = std::path::Path::new(data_dir);

    if !volumes_path.exists() {
        return volumes;
    }

    if let Ok(entries) = std::fs::read_dir(volumes_path) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("volume_") {
                    // Parse volume_id from directory name: volume_{id}
                    if let Some(id_str) = name.strip_prefix("volume_") {
                        if let Ok(id) = id_str.parse::<u64>() {
                            volumes.push(VolumeId(id));
                            info!("Found volume on disk: {} (id={})", name, id);
                        }
                    }
                }
            }
        }
    }

    volumes
}
