use clap::{Parser, Subcommand};
use log::{info, warn};
use powerfs_common::{
    error::{PowerFsError, Result},
    types::NodeId,
    utils::generate_node_id,
};
use powerfs_core::config::Config;
use powerfs_core::storage::StorageManager;
use powerfs_core::storage_backend::factory::BackendFactory;
#[cfg(any(feature = "spdk", feature = "spdk-stub"))]
use powerfs_core::storage_backend::SpdkBackend;
use powerfs_fuse::FuserApp;
use powerfs_master::{
    master::MasterNode,
    s3::directory_tree_api::{DirectoryTreeApi, RemoteDirectoryTree},
    s3::master_client::S3MasterClient,
    s3::MasterApi,
    s3::S3Server,
    tracking_allocator::TrackingAllocator,
};
use powerfs_volume::{
    master_client::{MasterClient, NewMasterClientParams},
    server::VolumeServer,
};
use std::fs::{self, File};

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator {
    inner: tikv_jemallocator::Jemalloc,
};
use std::io::Write;
use std::sync::Arc;
use tokio::time::Duration;

#[derive(Parser)]
#[command(
    name = "powerfs",
    version = "0.1.0",
    about = "PowerFS - Zero-jitter unified parallel file system"
)]
struct Cli {
    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long)]
    log_file: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Master {
        #[arg(long, short = 'P', default_value = "9333")]
        port: u16,

        /// Data directory (meta, raft will be created inside)
        #[arg(long, short = 'D', default_value = "./data/master")]
        dir: String,

        /// Raft log directory (default: <dir>/raft)
        #[arg(long, short = 'r')]
        raft_dir: Option<String>,

        /// Meta storage directory (default: <dir>/meta)
        #[arg(long, short = 'm')]
        meta_dir: Option<String>,

        /// Bind IP address
        #[arg(long)]
        ip: Option<String>,

        /// Advertise address for Raft communication (default: same as bind address)
        #[arg(long)]
        advertise_addr: Option<String>,

        /// Raft node ID (default: 1)
        #[arg(long, short = 'i', default_value = "1")]
        raft_id: u64,

        /// Raft peer addresses (e.g., --peer=172.20.0.11:9333 --peer=172.20.0.12:9333)
        #[arg(long, short = 'p')]
        peer: Vec<String>,
    },

    Volume {
        #[arg(long, short = 'P', default_value = "8080")]
        port: u16,

        /// Data directory (meta, data will be created inside)
        #[arg(long, short = 'D', default_value = "./data/volume")]
        dir: String,

        /// Meta storage directory (default: <dir>/meta)
        #[arg(long, short = 'm')]
        meta_dir: Option<String>,

        /// Data storage directory (default: <dir>/data)
        #[arg(long, short = 'd')]
        data_dir: Option<String>,

        /// Master address
        #[arg(long, short = 'M')]
        master: String,

        /// Bind IP address
        #[arg(long)]
        ip: Option<String>,

        /// Max volume size in bytes
        #[arg(long, short = 's', default_value = "1073741824")]
        max_volume_size: u64,

        /// 配置文件路径 (YAML)。提供后将从配置加载后端 (LocalFile/Spdk) 和节点参数,
        /// 覆盖命令行默认值。示例: --config examples/config-spdk.yaml
        #[arg(long, short = 'c')]
        config: Option<String>,
    },

    Filer {
        #[arg(long, short, default_value = "8888")]
        port: u16,

        /// Master address
        #[arg(long, short)]
        master: String,

        /// Bind IP address
        #[arg(long)]
        ip: Option<String>,
    },

    Fuse {
        /// Mount directory
        #[arg(long, short)]
        dir: String,

        /// Master address
        #[arg(long, short)]
        master: Option<String>,

        /// Volume port
        #[arg(long, short, default_value = "8080")]
        volume_port: u16,
    },

    Mount {
        /// Mount directory
        #[arg(long, short)]
        dir: String,

        /// Master address
        #[arg(long, short)]
        master: Option<String>,
    },

    S3 {
        #[arg(long, short, default_value = "9000")]
        port: u16,

        /// Master address
        #[arg(long, short)]
        master: String,

        /// Bind IP address
        #[arg(long)]
        ip: Option<String>,

        /// Data directory for DirectoryTree
        #[arg(long, short, default_value = "./data/s3")]
        dir: String,

        /// S3 access key
        #[arg(long, default_value = "powerfs")]
        access_key: String,

        /// S3 secret key
        #[arg(long, default_value = "powerfs123")]
        secret_key: String,
    },
}

#[tokio::main]
#[allow(clippy::result_large_err)]
async fn main() -> Result<()> {
    let test_vec: Vec<u8> = vec![0u8; 1024];
    let snap = powerfs_master::tracking_allocator::ALLOC_STATS.snapshot();
    println!(
        "ALLOCATOR_TEST: alloc_bytes={}, alloc_count={}, live_bytes={}, live_cnt={}",
        snap.alloc_bytes,
        snap.alloc_count,
        snap.live_bytes(),
        snap.live_count()
    );

    drop(test_vec);
    let snap2 = powerfs_master::tracking_allocator::ALLOC_STATS.snapshot();
    println!(
        "ALLOCATOR_TEST_AFTER_DROP: alloc_bytes={}, alloc_count={}, live_bytes={}, live_cnt={}",
        snap2.alloc_bytes,
        snap2.alloc_count,
        snap2.live_bytes(),
        snap2.live_count()
    );

    let cli = Cli::parse();

    let mut builder = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(cli.log_level.as_str()),
    );

    builder.format(|buf, record| {
        writeln!(
            buf,
            "[{}] [{}] [{}] {}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            record.level(),
            record.target(),
            record.args()
        )
    });

    if let Some(log_file) = &cli.log_file {
        let log_path = std::path::Path::new(log_file);
        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent).unwrap_or_else(|e| {
                eprintln!("Failed to create log directory: {}", e);
            });
        }

        let file = File::create(log_file).unwrap_or_else(|e| {
            eprintln!("Failed to create log file: {}", e);
            std::process::exit(1);
        });

        builder.target(env_logger::Target::Pipe(Box::new(file)));
        eprintln!("Logging to file: {}", log_file);
    }

    builder.init();

    match cli.command {
        Commands::Master {
            port,
            dir,
            raft_dir,
            meta_dir,
            ip,
            advertise_addr,
            raft_id,
            peer,
        } => {
            run_master(RunMasterParams {
                port,
                dir: &dir,
                raft_dir,
                meta_dir,
                ip,
                advertise_addr,
                raft_id,
                peers: peer,
            })
            .await
        }

        Commands::Volume {
            port,
            dir,
            meta_dir,
            data_dir,
            master,
            ip,
            max_volume_size,
            config,
        } => {
            run_volume(
                port,
                &dir,
                meta_dir,
                data_dir,
                &master,
                ip,
                max_volume_size,
                config.as_deref(),
            )
            .await
        }

        Commands::Filer { port, master, ip } => run_filer(port, &master, ip).await,

        Commands::Fuse {
            dir,
            master,
            volume_port,
        } => run_fuse(&dir, master, volume_port).await,

        Commands::Mount { dir, master } => run_mount(&dir, master).await,

        Commands::S3 {
            port,
            master,
            ip,
            dir,
            access_key,
            secret_key,
        } => run_s3(port, &master, ip, &dir, &access_key, &secret_key).await,
    }
}

struct RunMasterParams<'a> {
    port: u16,
    dir: &'a str,
    raft_dir: Option<String>,
    meta_dir: Option<String>,
    ip: Option<String>,
    advertise_addr: Option<String>,
    raft_id: u64,
    peers: Vec<String>,
}

async fn run_master(params: RunMasterParams<'_>) -> Result<()> {
    info!("Starting PowerFS Master node");

    let raft_dir = params
        .raft_dir
        .unwrap_or_else(|| format!("{}/raft", params.dir));
    let meta_dir = params
        .meta_dir
        .unwrap_or_else(|| format!("{}/meta", params.dir));

    std::fs::create_dir_all(params.dir)?;
    std::fs::create_dir_all(&raft_dir)?;
    std::fs::create_dir_all(&meta_dir)?;

    let bind_address = match params.ip {
        Some(ip) => format!("{}:{}", ip, params.port),
        None => format!("0.0.0.0:{}", params.port),
    };

    let raft_address = params
        .advertise_addr
        .unwrap_or_else(|| bind_address.clone());

    let master = MasterNode::new(
        &bind_address,
        &raft_address,
        None,
        &raft_dir,
        params.raft_id,
        params.peers,
    )
    .await?;

    info!("Master node initialized: {:?}", master.id());
    info!("Listening on: {}", bind_address);
    info!("Data directory: {}", params.dir);
    info!("Raft directory: {}", raft_dir);
    info!("Meta directory: {}", meta_dir);

    Arc::new(master).start().await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_volume(
    port: u16,
    dir: &str,
    meta_dir: Option<String>,
    data_dir: Option<String>,
    master: &str,
    ip: Option<String>,
    _max_volume_size: u64,
    config_path: Option<&str>,
) -> Result<()> {
    info!("Starting PowerFS Volume node");

    // 加载配置文件 (如果提供)
    let config = if let Some(path) = config_path {
        let cfg = Config::load_from_file(path)
            .map_err(|e| PowerFsError::Internal(format!("Failed to load config: {}", e)))?;
        cfg.validate()
            .map_err(|e| PowerFsError::Internal(format!("Config validation failed: {}", e)))?;
        info!(
            "Loaded config from {} (node={}, backend={:?})",
            path, cfg.node.node_id, cfg.storage.backend.backend_type
        );
        Some(cfg)
    } else {
        None
    };

    // Calculate subdirectories
    let meta_dir = meta_dir.unwrap_or_else(|| format!("{}/meta", dir));
    let data_dir = data_dir.unwrap_or_else(|| format!("{}/data", dir));

    // Create directories
    std::fs::create_dir_all(dir)?;
    std::fs::create_dir_all(&meta_dir)?;
    std::fs::create_dir_all(&data_dir)?;

    let bind_ip = ip.clone().unwrap_or_else(|| "0.0.0.0".to_string());
    let grpc_port = port;
    let http_port = port;

    let address = format!("{}:{}", bind_ip, port);

    // 创建 storage_manager:
    // - 有配置文件: 用 BackendFactory 从配置创建 backend (LocalFile/Spdk),node_id 从配置读
    // - 无配置文件: 用默认 LocalFsBackend,node_id 自动生成
    let node_id = config
        .as_ref()
        .map(|c| NodeId(c.node.node_id.clone()))
        .unwrap_or_else(generate_node_id);

    let storage_manager = if let Some(cfg) = &config {
        use powerfs_core::storage_backend::BackendType;

        match cfg.storage.backend.backend_type {
            BackendType::Spdk => {
                #[cfg(any(feature = "spdk", feature = "spdk-stub"))]
                {
                    use powerfs_core::storage_backend::BackendConfigDetails;
                    // SPDK backend: 直接创建强类型 Arc<SpdkBackend>,保留引用用于后台 attach。
                    // 设备 attach 不在这里同步做 — SPDK subsystem 初始化是异步的,
                    // 需要等服务 ready 后通过 RPC 异步 attach (见下方 spawn 的任务)。
                    let spdk_details = match &cfg.storage.backend.config {
                        BackendConfigDetails::Spdk(d) => d.clone(),
                        _ => {
                            return Err(PowerFsError::Internal(
                                "config mismatch: backend_type=Spdk but config is not Spdk".into(),
                            ));
                        }
                    };

                    let rpc_path = spdk_details.rpc_socket_path.as_deref();
                    let spdk_backend = Arc::new(
                        SpdkBackend::new(&cfg.node.node_id, rpc_path)
                            .map_err(|e| PowerFsError::Storage(e.to_string()))?,
                    );
                    info!("Created SpdkBackend (node_id={}), devices will be attached async after SPDK ready",
                          cfg.node.node_id);

                    // spawn 后台任务:等服务 ready 后异步 attach 所有设备
                    // 不阻塞主流程,失败设备跳过并记录日志
                    let backend_for_attach = Arc::clone(&spdk_backend);
                    let node_id_for_log = node_id.clone();
                    tokio::spawn(async move {
                        info!(
                            "[{}] Background SPDK device attach task started",
                            node_id_for_log
                        );
                        let results = backend_for_attach
                            .attach_devices_from_config(
                                &spdk_details.devices,
                                spdk_details.rpc_socket_path.as_deref(),
                            )
                            .await;
                        // 结果已在方法内汇总日志,这里无需重复
                        let _ = results;
                    });

                    Arc::new(StorageManager::new_with_backend(
                        node_id.clone(),
                        data_dir.clone(),
                        spdk_backend,
                    ))
                }
                #[cfg(not(any(feature = "spdk", feature = "spdk-stub")))]
                {
                    return Err(PowerFsError::Internal(
                        "SPDK backend not compiled. Enable 'spdk' or 'spdk-stub' feature.".into(),
                    ));
                }
            }
            BackendType::LocalFile => {
                let backend = BackendFactory::create_from_config(cfg)
                    .map_err(|e| PowerFsError::Storage(e.to_string()))?;
                info!("Created LocalFile backend from config");
                Arc::new(StorageManager::new_with_backend(
                    node_id.clone(),
                    data_dir.clone(),
                    backend,
                ))
            }
        }
    } else {
        Arc::new(
            StorageManager::new(node_id.clone(), data_dir.clone())
                .expect("Failed to create storage manager"),
        )
    };

    storage_manager.load_volumes()?;

    info!("Volume node initialized: {:?}", node_id);
    info!("Listening on: {}", address);
    info!("Data directory: {}", dir);
    info!("Meta directory: {}", meta_dir);
    info!("Data storage: {}", data_dir);
    info!("Connected to master: {}", master);

    let volume_server = VolumeServer::new(
        storage_manager.clone(),
        node_id.clone(),
        &bind_ip,
        grpc_port as u32,
        http_port as u32,
        &data_dir,
    );

    tokio::spawn(async move {
        if let Err(e) = volume_server.start(&address).await {
            eprintln!("Volume gRPC server failed: {}", e);
        }
    });

    let master_client = MasterClient::new(NewMasterClientParams {
        master_addresses: &[master],
        node_id: node_id.clone(),
        grpc_port: grpc_port.into(),
        http_port: http_port.into(),
        data_center: "dc1",
        rack: "rack1",
        public_url: &format!("http://{}:{}", bind_ip, port),
        ip: &bind_ip,
    });

    info!("Registering with master at {}...", master);
    match master_client.start_heartbeat().await {
        Ok(_) => info!("Heartbeat started successfully"),
        Err(e) => warn!("Failed to start heartbeat: {}", e),
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
                        if let Err(e) = storage_manager
                            .create_volume(powerfs_common::types::VolumeId(vid), 1024 * 1024 * 1024)
                        {
                            warn!("Failed to create volume {}: {}", vid, e);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to request volumes from master: {}", e);
            }
        }

        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

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

            if let Err(e) = master_client.send_heartbeat(proto_volumes).await {
                warn!("Failed to send heartbeat: {}", e);
            }
        }
    });

    tokio::signal::ctrl_c().await?;

    Ok(())
}

async fn run_filer(port: u16, master: &str, ip: Option<String>) -> Result<()> {
    info!("Starting PowerFS Filer");

    let address = match ip {
        Some(ip) => format!("{}:{}", ip, port),
        None => format!("0.0.0.0:{}", port),
    };

    info!("Filer initialized");
    info!("Listening on: {}", address);
    info!("Connected to master: {}", master);

    tokio::signal::ctrl_c().await?;

    Ok(())
}

async fn run_fuse(dir: &str, master: Option<String>, _volume_port: u16) -> Result<()> {
    info!("Starting PowerFS FUSE client");

    let master_addr = master.as_deref().unwrap_or("localhost:9334");
    let fuse_app =
        FuserApp::new(&[master_addr.to_string()], dir, "default", "000", 8, false).await?;

    info!("Mounting PowerFS at: {}", dir);
    info!("Connected to master: {}", master_addr);

    fuse_app.run().await
}

async fn run_mount(dir: &str, master: Option<String>) -> Result<()> {
    info!("Mounting PowerFS at: {}", dir);

    let master_addr = master.as_deref().unwrap_or("localhost:9334");
    let fuse_app =
        FuserApp::new(&[master_addr.to_string()], dir, "default", "000", 8, false).await?;

    info!("Connected to master: {}", master_addr);

    fuse_app.run().await
}

async fn run_s3(
    port: u16,
    master: &str,
    ip: Option<String>,
    _dir: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<()> {
    info!("Starting PowerFS S3 Server (Backend)");

    let address = match ip {
        Some(ip) => format!("{}:{}", ip, port),
        None => format!("0.0.0.0:{}", port),
    };

    let s3_addr: std::net::SocketAddr = address.parse()?;

    let directory_tree: Arc<dyn DirectoryTreeApi> = Arc::new(RemoteDirectoryTree::new(master));

    let master_api = Arc::new(MasterApi::Remote(Arc::new(S3MasterClient::new(master))));

    let volume_client_pool = Arc::new(powerfs_master::volume_client::VolumeClientPool::new());

    let lock_manager = Arc::new(powerfs_master::lock_manager::LockManager::new());

    let auth_manager = Arc::new(
        powerfs_master::s3::auth::AuthManager::with_default_credentials(access_key, secret_key),
    );

    let s3_server = S3Server::new(
        s3_addr,
        directory_tree,
        master_api,
        volume_client_pool,
        lock_manager,
        auth_manager,
    );

    info!("S3 Server initialized (Backend)");
    info!("Listening on: {}", address);
    info!("Connected to master: {}", master);
    info!("Access key: {}", access_key);

    s3_server.serve().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("S3 server error: {}", e))
    })?;

    Ok(())
}
