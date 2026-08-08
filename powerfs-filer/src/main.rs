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
use powerfs_net::{PowerFsNetServer, ServerConnectionManager};

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

    let log_level = cfg.global.log_level.clone();
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();
    let _ = powerfs_common::dynamic_log::set_log_level(&log_level);

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

    // Advertise IP: 必须是其他节点/客户端可到达的地址（不能是 0.0.0.0）
    // 优先级: filer.advertise_addr > filer.ip (非 0.0.0.0) > raft_peers[raft_id-1] 的 IP
    // raft_address 用 advertise_ip，因为 RaftGroupManager.node_address 会用于
    // REDIRECT 响应（check_leader 返回 REDIRECT 到自身时）
    let advertise_ip = filer_cfg
        .advertise_addr
        .as_deref()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            filer_cfg
                .ip
                .as_deref()
                .filter(|s| *s != "0.0.0.0" && *s != "::")
        })
        .or_else(|| {
            if filer_cfg.raft_id > 0 && filer_cfg.raft_id as usize <= filer_cfg.raft_peers.len() {
                let peer = &filer_cfg.raft_peers[(filer_cfg.raft_id - 1) as usize];
                peer.rfind(':').map(|pos| &peer[..pos])
            } else {
                None
            }
        })
        .unwrap_or("0.0.0.0")
        .to_string();

    let s3_address = format!("{}:{}", bind_ip, port);
    let grpc_address = format!("{}:{}", bind_ip, grpc_port);
    let net_address = format!("{}:{}", bind_ip, net_port);
    // Raft address uses advertise IP (must be reachable by peers and clients)
    let raft_address = format!("{}:{}", advertise_ip, grpc_port);

    info!("  S3 Address: {}", s3_address);
    info!("  gRPC Address: {}", grpc_address);
    info!("  Net Address: {}", net_address);
    info!(
        "  Raft Address: {} (advertise_ip={})",
        raft_address, advertise_ip
    );
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

    // Recover inode_generator by scanning existing inodes in RocksDB.
    // Prevents inode number reuse after restart (was causing -ENOSPC in kernel).
    meta_shard_manager.recover_inode_generator();

    // 初始化 POSIX root inode (inode=1, 目录 "/").
    // 首次启动或全新部署时必须创建, 否则 FUSE getattr(1) 返回 ENOENT,
    // 导致挂载点显示为 d????????? (无法访问).
    // format_posix_root 内部有幂等检查和 Raft leader 等待重试.
    {
        let mgr = meta_shard_manager.clone();
        tokio::spawn(async move {
            match mgr.format_posix_root().await {
                Ok(ino) => {
                    info!("POSIX root inode {} initialized", ino);
                }
                Err(e) => {
                    error!("Failed to initialize POSIX root inode: {}", e);
                }
            }
        });
    }

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
        // Phase 2: Create ConnRegistry + ServerConnectionManager and
        // InodeNotifier first, so the FilerNetHandler can push Invalidate
        // notifications to clients when directory metadata changes.
        // The registry is shared with the server via bind_with_registry.
        let net_registry = Arc::new(powerfs_net::ConnRegistry::new());
        let net_manager = Arc::new(ServerConnectionManager::new(net_registry.clone()));
        let inode_notifier = Arc::new(powerfs_filer::inode_notifier::InodeNotifier::new(
            net_manager.clone(),
        ));

        let net_handler = Arc::new(FilerNetHandler::with_notifier(
            meta_shard_manager.clone(),
            shard_strategy.clone(),
            net_port,
            inode_notifier,
        ));

        // P2.5: 启用 Inline 小文件优化 (config.inline_max_size, 默认 0 = 禁用).
        // 启用后 handle_create 对新文件返回 Placement::Inline, 数据直接存 Filer
        // 元数据 (Raft 复制), 绕过 Volume Server. 适合 IO500 mdtest 微小文件场景.
        if let Some(max_size) = filer_cfg.inline_max_size {
            if max_size > 0 {
                net_handler.set_inline_max_size(max_size);
            }
        }

        // === P2.3 + Phase A1: Zone 注册 + Filer 节点发现 (异步, 不阻塞 Filer 启动) ===
        // 向 Master 发送 RegisterFiler TLV 请求, 同时完成:
        //   1. Zone 分配 (获取 needle_id 空间 + 物理 volume 列表)
        //   2. Filer 节点发现注册 (供 kernel ListFilers 使用, 替代旧 gRPC RegisterFiler)
        //
        // 持续重试 + 周期性重注册 (60s 心跳):
        //   - 首次注册失败: 5 秒后重试 (Master Raft 选举可能需要几秒)
        //   - 注册成功: 60 秒后重新注册 (保持心跳, 应对 Master leader 切换后 filer_nodes 丢失)
        //   - Zone 的 physical_volumes 为空 (Volume 心跳未到达) 也视为失败并重试
        {
            let net_handler_for_zone = net_handler.clone();
            let master_addrs_for_zone = master_addresses.clone();
            let master_net_port = filer_cfg.master_net_port;
            let filer_raft_id = filer_cfg.raft_id;
            let shard_count = filer_cfg.shard_count as u64;
            let net_port_for_reg = net_port;
            // Filer 的可到达地址 (供 kernel 通过 ListFilers 发现本 Filer)
            let advertise_addr_for_reg = format!("{}:{}", advertise_ip, net_port);

            tokio::spawn(async move {
                let filer_id = format!("filer-{}", filer_raft_id);
                let shard_ids: Vec<u64> = (0..shard_count).collect();

                let registration = powerfs_filer::zone_client::FilerNodeRegistration {
                    filer_id: filer_id.clone(),
                    advertise_addr: advertise_addr_for_reg.clone(),
                    net_port: net_port_for_reg as u32,
                    shard_count,
                    shard_ids,
                };

                // 从 master_addresses ("ip:http_port") 提取 IP, 拼接 master_net_port
                let master_net_addrs: Vec<String> = master_addrs_for_zone
                    .iter()
                    .map(|addr| {
                        let ip = addr.rfind(':').map(|i| &addr[..i]).unwrap_or(addr);
                        format!("{}:{}", ip, master_net_port)
                    })
                    .collect();

                info!(
                    "FILER_ZONE: registering with Master (filer_id={}, net_addrs={:?}, advertise={})",
                    filer_id, master_net_addrs, advertise_addr_for_reg
                );

                const RETRY_INTERVAL_SECS: u64 = 5;
                const HEARTBEAT_INTERVAL_SECS: u64 = 60;
                let mut attempt: u64 = 0;
                let mut zones_recovered = false;

                loop {
                    attempt += 1;
                    let mut registered = false;

                    for master_addr in &master_net_addrs {
                        match powerfs_filer::zone_client::register_filer(master_addr, &registration)
                            .await
                        {
                            Ok(zones) => {
                                // 检查是否所有 Zone 都有物理 volume
                                let total_vols: usize =
                                    zones.iter().map(|z| z.physical_volumes.len()).sum();
                                if total_vols == 0 {
                                    warn!(
                                        "FILER_ZONE: registered but got 0 physical volumes (attempt={}), volume servers may not be ready, retrying...",
                                        attempt
                                    );
                                    break; // 跳出 master 循环, 进入重试
                                }

                                let zone_ids: Vec<u32> = zones.iter().map(|z| z.zone_id).collect();
                                info!(
                                    "FILER_ZONE: registered successfully (attempt={}), zones={:?}, total_volumes={}",
                                    attempt, zone_ids, total_vols
                                );
                                net_handler_for_zone.set_zones(zones);

                                // P2.5: 首次成功注册后, 从 chunk 映射恢复每个 Zone 的 counter
                                // (只在第一次注册成功时执行, 后续重注册不需要)
                                if !zones_recovered {
                                    let chunks =
                                        net_handler_for_zone.meta_shard_manager.list_all_chunks();
                                    let zone_ids = net_handler_for_zone.get_zones();
                                    for zone_id in zone_ids {
                                        let recovered = powerfs_filer::zone_client::recover_counter(
                                            zone_id, &chunks,
                                        );
                                        net_handler_for_zone.set_zone_counter(zone_id, recovered);
                                        info!(
                                            "FILER_ZONE: recovered zone_id={} counter={} (from {} chunks)",
                                            zone_id, recovered, chunks.len()
                                        );
                                    }
                                    zones_recovered = true;
                                }

                                registered = true;
                                break; // 注册成功, 跳出 master 循环
                            }
                            Err(e) => {
                                warn!(
                                    "FILER_ZONE: register_filer failed on {} (attempt={}): {}",
                                    master_addr, attempt, e
                                );
                            }
                        }
                    }

                    if registered {
                        // 注册成功: 60 秒后重新注册 (心跳, 应对 Master leader 切换)
                        attempt = 0; // 重置 attempt 计数
                        tokio::time::sleep(std::time::Duration::from_secs(HEARTBEAT_INTERVAL_SECS))
                            .await;
                    } else {
                        warn!(
                            "FILER_ZONE: all master attempts failed (attempt={}), retrying in {}s...",
                            attempt, RETRY_INTERVAL_SECS
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(RETRY_INTERVAL_SECS))
                            .await;
                    }
                }
            });
        }

        let net_handler: Arc<dyn powerfs_net::NetHandler> = net_handler;

        if let Ok(net_server) =
            PowerFsNetServer::bind_with_registry(&bind_ip, net_port, net_handler, net_registry)
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

    // Phase A1: Filer 节点发现注册已合并到上面的 TLV RegisterFiler 循环中
    // (Zone 注册 + 节点发现 + 60s 心跳重注册, 替代旧 gRPC ResilientMasterClient).

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
