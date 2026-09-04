use clap::Parser;
use log::{error, info, warn};
use powerfs_common::{
    config::{PowerFsConfig, ServiceType},
    error::PowerFsError,
    system_metrics::collect_system_metrics,
    types::{NodeId, VolumeId},
};
use powerfs_core::storage::StorageManager;
use powerfs_master_net::{TlvMasterClient, TlvMasterClientConfig};
use powerfs_net::{MsgType, NetMessage, NotificationHandler, PowerFsNetServer};
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
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let cfg = load_config(&args.config);

    run_volume(cfg, args).await?;

    Ok(())
}

async fn run_volume(cfg: PowerFsConfig, args: Args) -> powerfs_common::error::Result<()> {
    let volume_cfg = cfg.volume.clone();

    // 使用 dynamic_log（支持运行时动态调整 + target 过滤 + 子系统开关）
    // master 通过 GetDebugConfig 下发配置，volume 每 2s 轮询并本地应用
    let log_level = cfg.global.log_level.clone();
    powerfs_common::dynamic_log::init(&log_level, None);

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

    // Create transport (tcp/rdma/auto) based on volume config.
    // Shared between net server and MasterClient heartbeats.
    let net_transport: Option<Arc<dyn powerfs_net::Transport>> = if net_port > 0 {
        let transport_cfg = powerfs_net::TransportConfig {
            transport: cfg
                .volume
                .transport
                .clone()
                .unwrap_or_else(|| "tcp".to_string()),
            rdma_device: cfg.volume.rdma_device.clone(),
            require_rdma: cfg.volume.require_rdma,
            ..Default::default()
        };
        match powerfs_net::create_transport(&transport_cfg) {
            Ok(t) => {
                info!("Net transport: {}", t.name());
                Some(t)
            }
            Err(e) => {
                error!(
                    "Failed to create transport '{}': {:?}",
                    transport_cfg.transport, e
                );
                return Err(PowerFsError::InvalidRequest(format!(
                    "transport '{}' init failed: {:?}",
                    transport_cfg.transport, e
                )));
            }
        }
    } else {
        None
    };

    // Master management transport: always TCP (management network).
    // Only the data path (net server for kernel client) uses the configured transport (RDMA).
    // Heartbeat and registration go over TCP to avoid RDMA connect-attempt overhead
    // and MR pool churn on every master reconnect.
    let master_transport: Arc<dyn powerfs_net::Transport> = {
        let mcfg = powerfs_net::TransportConfig {
            transport: "tcp".to_string(),
            ..Default::default()
        };
        match powerfs_net::create_transport(&mcfg) {
            Ok(t) => {
                info!("Master transport: {} (management network)", t.name());
                t
            }
            Err(e) => {
                error!("Failed to create master transport: {:?}", e);
                return Err(PowerFsError::InvalidRequest(format!(
                    "master transport init failed: {:?}",
                    e
                )));
            }
        }
    };

    // Start powerfs-net binary protocol server for Volume
    // (bind first so we can share its FlowController with the metrics server)
    let net_server = if net_port > 0 {
        let net_bind_addr = format!("{}:{}", ip, net_port);
        let net_handler = Arc::new(powerfs_volume::net_handler::VolumeNetHandler::new(
            Arc::new(volume_server.clone()),
        ));
        let net_handler: Arc<dyn powerfs_net::NetHandler> = net_handler;

        info!("Starting powerfs-net Volume server on {}", net_bind_addr);

        let transport = net_transport
            .clone()
            .expect("net_transport Some when net_port>0");

        PowerFsNetServer::bind_with_manager_and_transport(&ip, net_port, net_handler, transport)
            .await
            .ok()
    } else {
        None
    };

    // FlowController: from net_server if available, else standalone (empty stats)
    let flow_ctrl = net_server
        .as_ref()
        .map(|s| s.flow_ctrl().clone())
        .unwrap_or_else(|| Arc::new(powerfs_net::flow_control::FlowController::with_defaults()));

    // Enable AdaptiveConcurrencyPolicy so the server computes load_factor
    // from active_reqs/max_active ratio and stamps it on response frames.
    flow_ctrl.set_default_policy();
    info!("flow control: adaptive concurrency policy enabled");

    // Start HTTP metrics & admin endpoints on http_port.
    // Exposes /metrics (Prometheus), /admin/lease-stats, /admin/log-level,
    // and /admin/flow/* (flow control, Phase 1 S4).
    // Failure to start is non-fatal: log and continue.
    {
        let metrics_addr_str = format!("{}:{}", ip, http_port);
        if let Ok(metrics_addr) = metrics_addr_str.parse() {
            if let Err(e) = powerfs_volume::metrics::start_metrics_server(
                metrics_addr,
                volume_server.range_lease_mgr.clone(),
                flow_ctrl,
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

    // Spawn net server serve loop
    if let Some(net_server) = net_server {
        tokio::spawn(async move {
            if let Err(e) = net_server.serve().await {
                error!("powerfs-net Volume server error: {:?}", e);
            }
        });
    }

    let master_addrs: Vec<&str> = master_address.iter().map(|s| s.as_str()).collect();
    // Load client certificate PEM for production node authentication.
    let client_cert_pem = match &volume_cfg.client_crt {
        Some(path) if !path.is_empty() => match std::fs::read_to_string(path) {
            Ok(pem) => {
                info!("VOLUME: loaded client cert from {} ({}B)", path, pem.len());
                pem
            }
            Err(e) => {
                error!(
                    "VOLUME: failed to read client cert {}: {}. \
                       Master cert enforcement will reject this volume server.",
                    path, e
                );
                String::new()
            }
        },
        _ => String::new(),
    };

    // ── DebugConfig Push 客户端 (替代 legacy 2s poller) ──────────────────
    // 建立与 Master 的常驻 TLV 长连接, 接收 `DebugConfigChanged(0x008A)`
    // NOTIFY 即时应用, 启动时拉一次初值. 失败仅 warn, 不阻塞启动.
    {
        let push_node_id = node_id.0.clone();
        let push_master_net_port = volume_cfg.master_net_port;
        let push_master_net_addrs: Vec<(String, u16)> = master_address
            .iter()
            .map(|addr| {
                let ip = addr
                    .rfind(':')
                    .map(|i| &addr[..i])
                    .unwrap_or(addr)
                    .to_string();
                (ip, push_master_net_port)
            })
            .collect();
        let push_cert = if client_cert_pem.is_empty() {
            None
        } else {
            Some(client_cert_pem.clone())
        };
        let push_transport = Some(master_transport.clone());

        tokio::spawn(async move {
            struct DebugConfigPushHandler {
                node_id: String,
            }
            impl NotificationHandler for DebugConfigPushHandler {
                fn handle_notification(&self, msg: &NetMessage) {
                    if msg.msg_type() == Some(MsgType::DebugConfigChanged) {
                        match powerfs_net::serialize::decode_get_debug_config_resp(&msg.body) {
                            Ok(cfg) => {
                                powerfs_common::debug_config_poller::apply_config(
                                    &cfg,
                                    &self.node_id,
                                );
                            }
                            Err(e) => {
                                warn!("VOLUME_DEBUG_PUSH: DebugConfigChanged decode err: {}", e)
                            }
                        }
                    }
                }
            }

            let tlv_config = TlvMasterClientConfig {
                client_type: powerfs_net::ClientType::Volume,
                client_cert_pem: push_cert,
                transport: push_transport,
                ..Default::default()
            };
            let tlv_client = Arc::new(TlvMasterClient::new(
                push_master_net_addrs.clone(),
                tlv_config,
            ));

            let handler: Arc<dyn NotificationHandler + Send + Sync> =
                Arc::new(DebugConfigPushHandler {
                    node_id: push_node_id.clone(),
                });
            tlv_client.set_notification_handler(handler);

            match powerfs_net::serialize::encode_get_debug_config_req(&push_node_id) {
                Ok(body) => {
                    match tlv_client
                        .submit_request(MsgType::GetDebugConfig, &body)
                        .await
                    {
                        Ok(resp) => {
                            match powerfs_net::serialize::decode_get_debug_config_resp(&resp.body) {
                                Ok(cfg) => {
                                    powerfs_common::debug_config_poller::apply_config(
                                        &cfg,
                                        &push_node_id,
                                    );
                                }
                                Err(e) => warn!(
                                    "VOLUME_DEBUG_PUSH: initial GetDebugConfig decode err: {}",
                                    e
                                ),
                            }
                        }
                        Err(e) => warn!(
                            "VOLUME_DEBUG_PUSH: initial GetDebugConfig pull failed: {}",
                            e
                        ),
                    }
                }
                Err(e) => warn!("VOLUME_DEBUG_PUSH: encode GetDebugConfig req failed: {}", e),
            }

            info!(
                "VOLUME_DEBUG_PUSH: push client started (masters={:?}, node_id={})",
                push_master_net_addrs, push_node_id
            );
            std::future::pending::<()>().await;
        });
    }

    let master_client = MasterClient::new(NewMasterClientParams {
        master_addresses: &master_addrs,
        master_net_port: volume_cfg.master_net_port,
        node_id: node_id.clone(),
        http_port: http_port as u32,
        net_port: net_port as u32,
        ip: &ip,
        registration_token: volume_cfg.registration_token.as_deref(),
        client_cert_pem: &client_cert_pem,
        transport: Some(master_transport.clone()),
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

            // P5: sysinfo instance for node load metrics (cpu/memory).
            // Refreshed before each heartbeat; first refresh is a warm-up
            // (sysinfo needs two reads for accurate cpu delta).
            let mut sys = sysinfo::System::new_all();
            sys.refresh_all();

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

            if master_client
                .send_heartbeat(proto_volumes, 0.0, 0.0)
                .await
                .is_err()
            {
                warn!("Initial heartbeat failed, reconnecting...");
                if let Err(e) = master_client.start_heartbeat().await {
                    warn!("Failed to restart heartbeat: {}", e);
                }
            }

            // Continuous heartbeat loop
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;

                // P5: Collect node load metrics for this heartbeat.
                let metrics = collect_system_metrics(&mut sys, "");
                let cpu_usage = (metrics.cpu_usage / 100.0) as f32;
                let memory_usage = (metrics.mem_usage / 100.0) as f32;

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

                if master_client
                    .send_heartbeat(proto_volumes, cpu_usage, memory_usage)
                    .await
                    .is_err()
                {
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
    match PowerFsConfig::load_for_service(config_path, ServiceType::Volume) {
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
