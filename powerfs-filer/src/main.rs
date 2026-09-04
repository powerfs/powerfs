use clap::Parser;
use log::{error, info, warn};
use std::sync::Arc;
use std::time::Duration;

use powerfs_common::build_info::BuildInfo;
use powerfs_common::config::{PowerFsConfig, ServiceType};
use powerfs_common::error::PowerFsError;
use powerfs_common::traits::EventProvider;
use powerfs_common::{collect_system_metrics, Event, NodeStatusEvent, NullEventProvider};
use powerfs_master::s3::master_client::S3MasterClient;
use powerfs_master::s3::MasterApi;
use powerfs_master_net::{TlvMasterClient, TlvMasterClientConfig};

use powerfs_filer::raft_group_manager_v2::RaftGroupManagerV2;
use powerfs_filer::{
    BucketManager, EntryManager, FilerMetaServiceImpl, FilerNetHandler, FilerServer,
    MetaShardManager, MetadataStore, S3Handler, ShardId, ShardScheduler, ShardStrategy,
    TlvVolumeClient, VolumeRouter,
};
use powerfs_net::{
    ClientConnPool, ClientPoolConfig, MsgType, NetMessage, NotificationHandler, PowerFsNetServer,
    ServerConnectionManager,
};

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
    // 使用 dynamic_log（支持运行时动态调整 + target 过滤 + 子系统开关）
    powerfs_common::dynamic_log::init(&log_level, None);

    BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).log_startup();

    run_filer(cfg).await?;

    Ok(())
}

async fn run_filer(cfg: PowerFsConfig) -> powerfs_common::error::Result<()> {
    let filer_cfg = cfg.filer.clone();

    // Create transport (tcp/rdma/auto) based on filer config.
    // Shared across net server + inter-service clients (TlvMasterClient, ClientConnPool).
    let transport_cfg = powerfs_net::TransportConfig {
        transport: filer_cfg.transport.clone().unwrap_or_else(|| "tcp".to_string()),
        rdma_device: filer_cfg.rdma_device.clone(),
        require_rdma: filer_cfg.require_rdma,
        ..Default::default()
    };
    let net_transport: Arc<dyn powerfs_net::Transport> = match powerfs_net::create_transport(&transport_cfg) {
        Ok(t) => {
            info!("Net transport: {}", t.name());
            t
        }
        Err(e) => {
            error!("Failed to create transport '{}': {:?}", transport_cfg.transport, e);
            return Err(PowerFsError::InvalidRequest(format!(
                "transport '{}' init failed: {:?}",
                transport_cfg.transport, e
            )));
        }
    };

    // Master management transport: always TCP (management network).
    // Only the data path (net server + filer→volume) uses the configured transport (RDMA).
    // Heartbeat, registration, and shard notifications go over TCP to avoid
    // RDMA connect-attempt overhead and MR pool churn on every master reconnect.
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

    // ── DebugConfig Push 客户端 (替代 legacy 2s poller) ──────────────────
    // 建立与 Master 的常驻 TLV 长连接, 接收 `DebugConfigChanged(0x008A)`
    // NOTIFY 即时应用, 启动时拉一次初值. 失败仅 warn, 不阻塞 Filer 启动.
    {
        let push_node_id = node_id.clone();
        let push_master_net_port = filer_cfg.master_net_port;
        let push_transport = master_transport.clone();
        let push_master_net_addrs: Vec<(String, u16)> = filer_cfg
            .master_addresses
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
        let push_client_cert = match &filer_cfg.client_crt {
            Some(path) if !path.is_empty() => match std::fs::read_to_string(path) {
                Ok(pem) => Some(pem),
                Err(e) => {
                    warn!(
                        "FILER_DEBUG_PUSH: failed to read client cert {}: {}",
                        path, e
                    );
                    None
                }
            },
            _ => None,
        };

        tokio::spawn(async move {
            // Handler: 只处理 DebugConfigChanged NOTIFY, 解码后本地 apply.
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
                                warn!("FILER_DEBUG_PUSH: DebugConfigChanged decode err: {}", e)
                            }
                        }
                    }
                }
            }

            let tlv_config = TlvMasterClientConfig {
                client_type: powerfs_net::ClientType::Filer,
                client_cert_pem: push_client_cert,
                transport: Some(push_transport),
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

            // 尽力而为: 初始拉一次 DebugConfig, 不等 NOTIFY.
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
                                    "FILER_DEBUG_PUSH: initial GetDebugConfig decode err: {}",
                                    e
                                ),
                            }
                        }
                        Err(e) => warn!(
                            "FILER_DEBUG_PUSH: initial GetDebugConfig pull failed: {}",
                            e
                        ),
                    }
                }
                Err(e) => warn!("FILER_DEBUG_PUSH: encode GetDebugConfig req failed: {}", e),
            }

            info!(
                "FILER_DEBUG_PUSH: push client started (masters={:?}, node_id={})",
                push_master_net_addrs, push_node_id
            );
            // 保持 task 存活, 让 handler 引用和 TLV 长连接不被 drop.
            std::future::pending::<()>().await;
        });
    }

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
    // TLV volume client — 所有 Filer→Volume 业务通信走 powerfs-net TLV 协议 (非 gRPC),
    // 因为内核客户端没有 gRPC, 统一使用 TLV. 供 GC task, S3Handler, scrubber 共用.
    let volume_client_pool = Arc::new(TlvVolumeClient::new(Arc::new(
        ClientConnPool::new_with_transport(
            filer_cfg.raft_id,
            ClientPoolConfig::default(),
            None,
            Some(net_transport.clone()),
        ),
    )));

    let shard_strategy = Arc::new(ShardStrategy::new(filer_cfg.shard_count as u64));

    let raft_data_path = format!("{}/raft", filer_cfg.data_dir);
    std::fs::create_dir_all(&raft_data_path)
        .map_err(|e| PowerFsError::Internal(format!("failed to create raft dir: {}", e)))?;

    let raft_group_manager =
        RaftGroupManagerV2::new(filer_cfg.raft_id, raft_address.clone(), raft_data_path)
            .await
            .map_err(|e| {
                PowerFsError::Internal(format!("failed to create RaftGroupManagerV2: {}", e))
            })?;

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
    // Note: start_message_transmitter() is no longer needed — openraft uses
    // gRPC RaftService (MultiRaftServiceImpl) for inter-node communication,
    // started automatically inside RaftGroupManagerV2::new().

    for i in 0..filer_cfg.shard_count {
        let shard_id = ShardId(i as u64);
        meta_shard_manager
            .create_shard(shard_id, peers.clone())
            .await
            .map_err(|e| PowerFsError::Internal(format!("failed to create shard {}: {}", i, e)))?;
        info!("Shard {} initialized", i);
    }

    // 启动 Raft 状态健康监控: 只检测 Openraft running_state Fatal 错误
    // (存储损坏 / apply panic), 立即退出由 Docker restart policy 重启.
    // 假 Leader 检测已移交 Master 控制面 (见 filer_raft_monitor +
    // docs/raft_fault_tolerance_design.md), filer 不再因假 Leader 退出.
    raft_group_manager.spawn_health_monitor().await;

    // Spawn per-shard leader-change notifiers: when this filer gains/loses
    // leadership of a shard, notify the Master via ShardLeaderUpdate so it
    // maintains the shard_id → leader_addr table. This enables the
    // zero-redirect fast path: fuse clients fetch per-shard leaders from
    // the Master's GetTopology and route cap RPCs directly on first request.
    // Design principle: requests must not be forwarded between services;
    // a non-leader must reject — by advertising leaders upfront, the
    // client's first cap_open_grant lands on the true leader.
    {
        let master_net_port = filer_cfg.master_net_port;
        let first_master_net = filer_cfg
            .master_addresses
            .first()
            .map(|addr| {
                let ip = addr.rfind(':').map(|i| &addr[..i]).unwrap_or(addr);
                format!("{}:{}", ip, master_net_port)
            })
            .unwrap_or_default();
        if !first_master_net.is_empty() {
            let filer_id = format!("filer-{}", filer_cfg.raft_id);
            let advertise_addr_for_notifier = format!("{}:{}", advertise_ip, net_port);
            let notifier_transport = Some(master_transport.clone());
            let rgm = raft_group_manager.clone();
            for i in 0..filer_cfg.shard_count {
                let shard_id = ShardId(i as u64);
                rgm.spawn_shard_leader_notifier(
                    shard_id,
                    first_master_net.clone(),
                    filer_id.clone(),
                    advertise_addr_for_notifier.clone(),
                    notifier_transport.clone(),
                )
                .await;
            }
            info!(
                "Spawned {} shard_leader_notifier tasks (master={}, filer_id={}, advertise={})",
                filer_cfg.shard_count, first_master_net, filer_id, advertise_addr_for_notifier
            );
        } else {
            warn!("Cannot spawn shard_leader_notifier: no master addresses configured");
        }
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
    info!(
        "Starting shared gRPC server (RaftService + FilerMetaService) on {}",
        grpc_address
    );

    use powerfs_filer::powerfs::filer_meta_service_server::FilerMetaServiceServer;
    let raft_service = raft_group_manager.raft_service();
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(raft_service)
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

        let net_handler = {
            // Phase 5 §5.3: wire Raft-backed lease persistence so
            // active leases survive a Filer leader switch (closes the
            // "leader switch loses leases" correctness hole). The
            // backend routes LeasePut/Delete/SaveEpoch through the
            // filer's openraft state machine; `CF_LEASES` on shard 0
            // is the durable store. If shard 0 isn't available here
            // (single-node boot before Raft initialization), we fall
            // back to the in-memory lease manager — the cluster still
            // runs, just with the old "leader switch loses leases"
            // behavior, matching pre-phase-5 semantics.
            let base = FilerNetHandler::with_notifier(
                meta_shard_manager.clone(),
                shard_strategy.clone(),
                net_port,
                inode_notifier,
            );
            // §8.2/§8.3.1: shared client-health store for the three-layer
            // defense (scoring → throttle → quarantine/blacklist). Wired
            // into the lease manager's force-reclaim penalty hook so an
            // unresponsive holder (no RevokeAck within 2s) gets penalized;
            // repeated violations escalate to quarantine then blacklist.
            let client_health =
                Arc::new(powerfs_lock_health::ClientHealth::new(Default::default()));
            let base = base.with_client_health(client_health);
            // §13: wire ServerConnectionManager → NetCapRevoker into
            // CapManager. Without this, lock_arbiter enters GATHER state
            // but recall() callback is NoopCapRevoker → no TLV push,
            // 2s later force-reclaim timeout. MUST chain AFTER
            // with_client_health so cloned cap_mgr keeps penalty.
            let base = base.with_server_connection(net_manager.clone());
            if let Some(shard0_store) = meta_shard_manager.try_get_shard_store(ShardId(0)) {
                let persistence = powerfs_filer::RaftLeasePersistence::new(
                    raft_group_manager.clone(),
                    shard0_store,
                    ShardId(0),
                );
                base.with_lease_persistence(persistence)
            } else {
                base
            }
        };
        let net_handler = Arc::new(net_handler);

        // Phase 3 Lease Recall: wire the InodeNotifier + InodeLeaseManager
        // into the MetaShardManager so the GC loop can push Invalidate
        // notifications to lease holders when MetaCache memory pressure
        // exceeds the high watermark. Both components are owned by the
        // net_handler; we clone the Arcs before moving net_handler into
        // the server.
        if let Some(notifier) = &net_handler.inode_notifier {
            meta_shard_manager
                .set_recall_components(notifier.clone(), net_handler.inode_lease_mgr.clone());
        }

        // Phase 5 §5.3: recover any leases persisted to CF_LEASES on a
        // previous run / previous leader. Best-effort — failures log
        // a warning and leave the in-memory store empty, matching the
        // pre-persistence behavior. This is the no-leader-change
        // recovery path; the leader-takeover hook below catches the
        // case where this node is elected leader later.
        if let Err(e) = net_handler.recover_leases_from_persistence() {
            warn!("startup lease recovery failed (non-fatal): {}", e);
        }

        // P7 observability: start the Prometheus metrics server for the
        // inode lease manager + MetaCache. Exposes `/metrics` (Prometheus
        // text format) + `/admin/lease-stats` (JSON) +
        // `/admin/meta-cache-stats` (JSON) so operators can monitor active
        // leases, acquire/conflict counts, the Fencer SN high-water
        // (phase 4 §5.3), Early Grant waiter backpressure (phase 4 §5.2),
        // and MetaCache hit/miss/dirty/staging counters.
        //
        // The port MUST be explicitly provided via `filer.metrics_port`
        // (no `grpc_port + 1` derivation — see design rule: all service
        // ports declared statically in config.toml and unique).
        {
            let metrics_port = filer_cfg.metrics_port;
            let metrics_addr: std::net::SocketAddr =
                format!("{}:{}", bind_ip, metrics_port).parse()?;
            let lease_mgr = net_handler.inode_lease_mgr.clone();
            let meta_cache = meta_shard_manager.meta_cache();
            if let Err(e) =
                powerfs_filer::metrics::start_metrics_server(metrics_addr, lease_mgr, meta_cache)
                    .await
            {
                warn!("filer metrics server failed to start (non-fatal): {}", e);
            }
        }

        // §8.3.1: background force-reclaim sweep. Every 500ms, checks
        // for pending Early Revokes whose 2-second RevokeAck timeout
        // elapsed and force-reclaims the stuck holder's lease + grants
        // the next queued waiter + penalizes the holder's health score.
        // Bounds waiter stall under an unresponsive / crashed holder so
        // a stuck client can't block contended inodes indefinitely.
        {
            let sweep_handler = Arc::clone(&net_handler);
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(tokio::time::Duration::from_millis(500));
                // The first tick fires immediately on `tick` creation
                // (no pending revokes at startup) — skip it.
                tick.tick().await;
                loop {
                    tick.tick().await;
                    let reclaimed = sweep_handler.force_reclaim_expired_revokes();
                    if reclaimed > 0 {
                        info!(
                            "§8.3.1 force-reclaim sweep: reclaimed {} lease(s)",
                            reclaimed
                        );
                    }
                }
            });
        }

        // §13 Stage 4: cap 模型 sweep loop — 周期性调
        // `force_reclaim_expired_cap_recalls` 处理 GATHER 超时
        // force-reclaim + Loner 升级 (下发 CapUpgradeNotify).
        // 与 legacy lease sweep 同 500ms 间隔, 保证 cap 模型下
        // GATHER 超时 (recall_timeout 2s) 后 stuck holder 被强制回收,
        // 等待的 writer/xlock 请求者被唤醒.
        {
            let sweep_handler = Arc::clone(&net_handler);
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(tokio::time::Duration::from_millis(500));
                tick.tick().await; // skip immediate first tick
                loop {
                    tick.tick().await;
                    let promoted = sweep_handler.force_reclaim_expired_cap_recalls();
                    if promoted > 0 {
                        info!(
                            "§13 Stage 4 cap sweep: {} promote task(s) dispatched",
                            promoted
                        );
                    }
                }
            });
        }

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
            let force_register = filer_cfg.force_register;
            let registration_token_for_reg = filer_cfg.registration_token.clone();
            let transport_for_zone = Some(master_transport.clone());
            // Load client certificate PEM for production node authentication.
            // When the master has a CA configured, the filer MUST present this
            // cert during RegisterFiler; empty in dev mode (no cert configured).
            let client_cert_pem_for_reg = match &filer_cfg.client_crt {
                Some(path) if !path.is_empty() => match std::fs::read_to_string(path) {
                    Ok(pem) => {
                        info!("FILER: loaded client cert from {} ({}B)", path, pem.len());
                        pem
                    }
                    Err(e) => {
                        error!(
                            "FILER: failed to read client cert {}: {}. \
                                       Master cert enforcement will reject this filer.",
                            path, e
                        );
                        String::new()
                    }
                },
                _ => String::new(),
            };
            let net_port_for_reg = net_port;
            // S3 HTTP port (filer_cfg.port). The S3 server also serves the
            // /admin/shards endpoint, so the Master needs this port to
            // proxy shard-introspection in GetFilerStats.
            let http_port_for_reg = filer_cfg.port;
            // Filer Metrics HTTP port (explicitly configured — no derivation).
            let metrics_port_for_reg = filer_cfg.metrics_port;
            // Filer 的可到达地址 (供 kernel 通过 ListFilers 发现本 Filer)
            let advertise_addr_for_reg = format!("{}:{}", advertise_ip, net_port);

            tokio::spawn(async move {
                let filer_id = format!("filer-{}", filer_raft_id);
                let shard_ids: Vec<u64> = (0..shard_count).collect();

                let registration = powerfs_filer::zone_client::FilerNodeRegistration {
                    filer_id: filer_id.clone(),
                    advertise_addr: advertise_addr_for_reg.clone(),
                    net_port: net_port_for_reg as u32,
                    http_port: http_port_for_reg as u32,
                    metrics_port: metrics_port_for_reg as u32,
                    shard_count,
                    shard_ids,
                    force: force_register,
                    registration_token: registration_token_for_reg,
                    client_cert_pem: client_cert_pem_for_reg,
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
                        match powerfs_filer::zone_client::register_filer(
                            master_addr,
                            &registration,
                            transport_for_zone.clone(),
                        )
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
                                // BAD_REQUEST 表示 master 拒绝本 filer 加入集群
                                // （通常是 shard_count 与集群现有 filer 不一致）。
                                // 重试无意义——配置不变，结果不变。
                                //
                                // 启动门禁策略：
                                //   - 非 force 模式（force_register=false）：立即退出进程，
                                //     避免错误配置的节点进入集群导致 inode 路由错位。
                                //   - force 模式：理论上不应走到这里（已传 force=1，master
                                //     应放行）；若仍 BAD_REQUEST，说明 master 是旧版本不
                                //     识别 Force 字段——降级为 warn 并继续重试，让运维
                                //     有机会升级 master。
                                if e.contains("(BAD_REQUEST)") {
                                    if !force_register {
                                        log::error!(
                                            "FILER_ZONE: master rejected registration (shard_count \
                                             mismatch likely): {}. Exiting (set filer.force_register=true \
                                             to override).",
                                            e
                                        );
                                        std::process::exit(1);
                                    } else {
                                        log::warn!(
                                            "FILER_ZONE: master returned BAD_REQUEST despite \
                                             force_register=true (old master ignores Force field?): \
                                             {}. Will keep retrying.",
                                            e
                                        );
                                    }
                                } else {
                                    warn!(
                                        "FILER_ZONE: register_filer failed on {} (attempt={}): {}",
                                        master_addr, attempt, e
                                    );
                                }
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

        // P4: 启动 scrubber worker (后台副本复制 + P6 EC 转换)
        // 使用 powerfs-net TLV 协议 (非 gRPC) 与 Volume Server 通信,
        // 因为内核客户端没有 gRPC, 所有业务通信统一走 TLV.
        //
        // EC/scrubber 参数可通过环境变量覆盖 (便于测试调整 EC 配置):
        //   POWERFS_SCRUBBER_SCAN_INTERVAL  扫描间隔秒 (默认 30)
        //   POWERFS_SCRUBBER_MAX_INODES     每轮最大 inode 数 (默认 50)
        //   POWERFS_EC_DATA_SHARDS          EC 数据分片数 (默认 4)
        //   POWERFS_EC_PARITY_SHARDS        EC 校验分片数 (默认 2)
        //   POWERFS_EC_MIN_FILE_SIZE        EC 转换最小文件字节数 (默认 0=不限制)
        {
            let scrubber_config = powerfs_filer::scrubber::ScrubberConfig {
                scan_interval_secs: std::env::var("POWERFS_SCRUBBER_SCAN_INTERVAL")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(30),
                max_inodes_per_scan: std::env::var("POWERFS_SCRUBBER_MAX_INODES")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(50),
                replica_count: 2,
                ec_data_shards: std::env::var("POWERFS_EC_DATA_SHARDS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(4),
                ec_parity_shards: std::env::var("POWERFS_EC_PARITY_SHARDS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(2),
                ec_min_file_size: std::env::var("POWERFS_EC_MIN_FILE_SIZE")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0),
            };
            info!(
                "P4_SCRUBBER: config scan_interval={}s max_inodes={} ec={:?}+{:?} min_file_size={}",
                scrubber_config.scan_interval_secs,
                scrubber_config.max_inodes_per_scan,
                scrubber_config.ec_data_shards,
                scrubber_config.ec_parity_shards,
                scrubber_config.ec_min_file_size,
            );
            let scrubber = powerfs_filer::scrubber::ScrubberWorker::new(
                meta_shard_manager.clone(),
                volume_client_pool.clone(),
                net_handler.clone(),
                scrubber_config,
            );
            tokio::spawn(async move {
                scrubber.run().await;
            });
            info!("P4_SCRUBBER: scrubber worker started (TLV protocol)");
        }

        let net_handler: Arc<dyn powerfs_net::NetHandler> = net_handler;

        // net_transport was created early in run_filer() and is shared with
        // inter-service clients (TlvMasterClient, ClientConnPool).
        if let Ok(net_server) = PowerFsNetServer::bind_with_registry_and_transport(
            &bind_ip, net_port, net_handler, net_registry,
            powerfs_net::ServerConfig::default(), net_transport.clone(),
        )
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
    match PowerFsConfig::load_for_service(config_path, ServiceType::Filer) {
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
