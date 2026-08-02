use clap::Parser;
use log::{error, info, warn};
use std::sync::Arc;
use std::time::Duration;

use powerfs_common::build_info::BuildInfo;
use powerfs_common::config::PowerFsConfig;
use powerfs_common::error::PowerFsError;
use powerfs_common::traits::EventProvider;
use powerfs_common::{collect_system_metrics, Event, NodeStatusEvent, NullEventProvider};
use powerfs_master::s3::master_client::S3MasterClient;
use powerfs_master::s3::MasterApi;
use powerfs_master::volume_client::VolumeClientPool;

use powerfs_filer::{
    BucketManager, EntryManager, FilerMetaServiceImpl, FilerNetHandler, FilerServer,
    MetaShardManager, MetadataStore, RaftGroupManager, S3Handler, ShardId, ShardScheduler,
    ShardStrategy, VolumeRouter,
};
use powerfs_net::{ManagedNetHandler, PowerFsNetServer, ServerConnectionManager};

#[derive(Parser)]
#[command(name = "powerfs-filer")]
#[command(version = "0.1.0")]
#[command(about = "PowerFS Filer Node - Metadata & S3 API server with sharding")]
struct Args {
    /// 配置文件路径（必填，所有端口和地址必须在配置文件中设置）
    #[arg(short, long, required = true)]
    config: String,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let cfg = load_config(&args.config);

    let log_level = cfg.global.log_level.as_str();
    env_logger::Builder::new()
        .filter_level(match log_level {
            "debug" => log::LevelFilter::Debug,
            "warn" => log::LevelFilter::Warn,
            "error" => log::LevelFilter::Error,
            _ => log::LevelFilter::Info,
        })
        .init();

    BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).log_startup();

    run_filer(cfg).await?;

    Ok(())
}

async fn run_filer(cfg: PowerFsConfig) -> powerfs_common::error::Result<()> {
    let filer_cfg = cfg.filer.clone();

    info!("Starting PowerFS Filer with sharding");

    // 所有端口从配置文件获取 - 无硬编码默认值
    let port = filer_cfg.port;
    let grpc_port = filer_cfg.grpc_port;
    let net_port = filer_cfg.net_port;

    // 绑定地址：如果配置中有ip则使用，否则绑定所有接口
    let bind_ip = filer_cfg
        .ip
        .clone()
        .unwrap_or_else(|| "0.0.0.0".to_string());

    let s3_address = format!("{}:{}", bind_ip, port);
    let grpc_address = format!("{}:{}", bind_ip, grpc_port);
    let net_address = format!("{}:{}", bind_ip, net_port);
    // Raft communication uses gRPC port since Raft messages are sent via gRPC
    let raft_address = format!("{}:{}", bind_ip, grpc_port);

    info!("  S3 Address: {}", s3_address);
    info!("  gRPC Address: {}", grpc_address);
    info!("  Net Address: {}", net_address);
    info!("  Data Dir: {}", filer_cfg.data_dir);
    info!("  Shard Count: {}", filer_cfg.shard_count);
    info!("  Raft ID: {}", filer_cfg.raft_id);

    std::fs::create_dir_all(&filer_cfg.data_dir)
        .map_err(|e| PowerFsError::Internal(format!("failed to create data dir: {}", e)))?;

    // Redis 地址从全局配置获取
    let redis_url = cfg.global.redis_url.clone();
    let redis_client =
        redis::Client::open(redis_url).map_err(|e| PowerFsError::Internal(e.to_string()))?;

    let metadata_store = Arc::new(MetadataStore::new(redis_client));

    // Setup event provider for Redis-based node status publishing
    let event_provider: Arc<dyn EventProvider> = match std::env::var("REDIS_URL") {
        #[cfg(feature = "redis-event")]
        Ok(url) => {
            info!("Filer event provider enabled with Redis: {}", url);
            Arc::new(powerfs_common::event::RedisEventProvider::new(
                &url,
                "powerfs_events",
                "filer",
            ))
        }
        _ => {
            warn!("REDIS_URL not set, using null event provider");
            Arc::new(NullEventProvider)
        }
    };

    let node_id = format!("filer-{}", filer_cfg.raft_id);
    let grpc_port_for_event = grpc_port;
    let event_bind_ip = bind_ip.clone();
    let event_provider_clone = event_provider.clone();
    let data_dir_for_event = filer_cfg.data_dir.clone();
    tokio::spawn(async move {
        let mut sys = sysinfo::System::new_all();
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            sys.refresh_all();

            let metrics = collect_system_metrics(&mut sys, &data_dir_for_event);

            let event = Event::NodeStatus(NodeStatusEvent {
                node_id: node_id.clone(),
                node_type: "filer".to_string(),
                address: event_bind_ip.clone(),
                grpc_port: grpc_port_for_event as u32,
                http_port: grpc_port_for_event as u32,
                status: "healthy".to_string(),
                cpu_usage: metrics.cpu_usage,
                mem_usage: metrics.mem_usage,
                disk_usage: metrics.disk_usage,
                network_rx: metrics.network_rx,
                network_tx: metrics.network_tx,
                uptime: metrics.uptime,
                volume_count: 0,
                is_leader: false,
                raft_term: 0,
            });

            if let Err(e) = event_provider_clone.publish(event, &node_id).await {
                warn!("Failed to publish filer node_status event: {}", e);
            }
        }
    });

    // Master 地址列表从配置获取 - 必须非空
    let master_addresses = filer_cfg.master_addresses.clone();
    if master_addresses.is_empty() {
        return Err(PowerFsError::Internal(
            "filer.master_addresses must not be empty".to_string(),
        ));
    }

    info!("Filer master endpoints: {:?}", master_addresses);
    let master_client = Arc::new(S3MasterClient::new(master_addresses.clone())?);
    let master_api = Arc::new(MasterApi::Remote(master_client));

    let bucket_manager = Arc::new(BucketManager::new(metadata_store.clone(), master_api));
    let volume_router = Arc::new(VolumeRouter::new(metadata_store.clone()));
    let entry_manager = Arc::new(EntryManager::new(
        metadata_store.clone(),
        bucket_manager.clone(),
    ));
    let volume_client_pool = Arc::new(VolumeClientPool::new());

    let shard_strategy = Arc::new(ShardStrategy::new(filer_cfg.shard_count as u64));

    let raft_data_path = format!("{}/raft", filer_cfg.data_dir);
    std::fs::create_dir_all(&raft_data_path)
        .map_err(|e| PowerFsError::Internal(format!("failed to create raft dir: {}", e)))?;

    let raft_group_manager = Arc::new(RaftGroupManager::new(
        filer_cfg.raft_id,
        raft_address.clone(),
        raft_data_path,
    ));

    let shard_data_path = format!("{}/shards", filer_cfg.data_dir);
    std::fs::create_dir_all(&shard_data_path)
        .map_err(|e| PowerFsError::Internal(format!("failed to create shards dir: {}", e)))?;

    let meta_shard_manager = Arc::new(MetaShardManager::new(
        raft_group_manager.clone(),
        shard_strategy.clone(),
        shard_data_path,
        filer_cfg.raft_id,
    ));

    info!("Initializing {} metadata shards...", filer_cfg.shard_count);
    let peers: Vec<powerfs_filer::Peer> = if filer_cfg.raft_peers.is_empty() {
        vec![powerfs_filer::Peer {
            id: filer_cfg.raft_id,
            address: raft_address.clone(),
            net_address: net_address.clone(),
        }]
    } else {
        filer_cfg
            .raft_peers
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                // Convert gRPC address to net address
                let net_addr = if let Some(colon_pos) = addr.rfind(':') {
                    let ip_part = &addr[..colon_pos];
                    format!("{}:{}", ip_part, net_port)
                } else {
                    addr.clone()
                };
                powerfs_filer::Peer {
                    id: (i + 1) as u64,
                    address: addr.clone(),
                    net_address: net_addr,
                }
            })
            .collect()
    };

    for peer in &peers {
        raft_group_manager.register_peer(peer.clone()).await;
    }
    raft_group_manager.clone().start_message_transmitter().await;

    for i in 0..filer_cfg.shard_count {
        let shard_id = ShardId(i as u64);
        meta_shard_manager
            .create_shard(shard_id, peers.clone())
            .await
            .map_err(|e| PowerFsError::Internal(format!("failed to create shard {}: {}", i, e)))?;
        info!("Shard {} initialized", i);
    }

    // Load existing root inodes from shard stores (for persistence across restarts)
    meta_shard_manager.load_root_inodes_from_shards();

    let shard_scheduler = Arc::new(ShardScheduler::new(
        raft_group_manager.clone(),
        shard_strategy.clone(),
    ));

    for peer in &peers {
        shard_scheduler.register_node(&peer.id.to_string(), &peer.address);
    }

    tokio::spawn({
        let shard_scheduler = shard_scheduler.clone();
        async move {
            shard_scheduler.run().await;
        }
    });

    info!("ShardScheduler started with {} nodes", peers.len());

    // 启动后台 CRDT 维护任务：定期清理过期 Tombstone、压缩 Delta Log
    let crdt_maintenance_interval_secs = filer_cfg.crdt_maintenance_interval_secs.unwrap_or(60);
    let _crdt_handle = meta_shard_manager.spawn_crdt_maintenance(crdt_maintenance_interval_secs);
    info!(
        "CRDT maintenance task started (interval={}s)",
        crdt_maintenance_interval_secs
    );

    // Phase 3.5: 启动后台 GC 任务——定期扫描 tombstone 并物理删除超 grace_period 的条目
    // 物理删除元数据后异步回收 volume server 数据块（delete_needle）
    let gc_interval_secs = filer_cfg.gc_interval_secs.unwrap_or(300);
    let gc_grace_period_secs = filer_cfg.gc_grace_period_secs.unwrap_or(86400);
    let _gc_handle = meta_shard_manager.spawn_gc_task(
        gc_interval_secs,
        gc_grace_period_secs,
        volume_router.clone(),
        volume_client_pool.clone(),
    );
    info!(
        "GC task started (interval={}s, grace_period={}s, data_reclaim=enabled)",
        gc_interval_secs, gc_grace_period_secs
    );

    let s3_handler = Arc::new(
        S3Handler::new(
            bucket_manager.clone(),
            entry_manager.clone(),
            volume_router.clone(),
            volume_client_pool.clone(),
        )
        .with_meta_shard_manager(meta_shard_manager.clone()),
    );

    let addr: std::net::SocketAddr = s3_address.parse()?;
    let filer_server = FilerServer::new(
        addr,
        metadata_store.clone(),
        bucket_manager.clone(),
        entry_manager.clone(),
        volume_router.clone(),
        s3_handler.clone(),
        meta_shard_manager.clone(),
        shard_scheduler.clone(),
    );

    let grpc_service =
        FilerMetaServiceImpl::new(meta_shard_manager.clone(), shard_strategy.clone());

    let grpc_addr: std::net::SocketAddr = grpc_address.parse()?;
    info!("Starting gRPC meta service on {}", grpc_address);

    use powerfs_filer::powerfs::filer_meta_service_server::FilerMetaServiceServer;
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(FilerMetaServiceServer::new(grpc_service))
            .serve(grpc_addr)
            .await
        {
            error!("gRPC server error: {}", e);
        }
    });

    if net_port > 0 {
        // Phase 2: Create ServerConnectionManager and InodeNotifier first,
        // so the FilerNetHandler can push Invalidate notifications to clients
        // when directory metadata changes.
        let net_manager = Arc::new(ServerConnectionManager::new());
        let inode_notifier = Arc::new(powerfs_filer::inode_notifier::InodeNotifier::new(
            net_manager.clone(),
        ));

        let net_handler = Arc::new(FilerNetHandler::with_notifier(
            meta_shard_manager.clone(),
            shard_strategy.clone(),
            net_port,
            inode_notifier,
        ));

        // Wrap with ManagedNetHandler for session management + middleware
        let managed_handler = Arc::new(ManagedNetHandler::from_arc(
            net_manager.clone(),
            net_handler,
        ));

        if let Ok(net_server) =
            PowerFsNetServer::bind_with_manager(&bind_ip, net_port, managed_handler, net_manager)
                .await
        {
            tokio::spawn(async move {
                if let Err(e) = net_server.serve().await {
                    error!("powerfs-net server error: {:?}", e);
                }
            });
        } else {
            log::warn!("Failed to start powerfs-net server on {}", net_address);
        }
    }

    info!("Filer initialized");
    info!("S3 endpoint: {}", s3_address);
    info!("gRPC endpoint: {}", grpc_address);
    if net_port > 0 {
        info!("Net endpoint: {}", net_address);
    }
    info!("Connected to master(s): {:?}", master_addresses);

    filer_server.serve().await?;

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
