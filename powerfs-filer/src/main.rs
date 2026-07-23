use clap::{Parser, Subcommand};
use log::{error, info, warn};
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::transport::Server;

use axum::{response::IntoResponse, routing::get, Router};
use serde_json::json;

use powerfs_filer::grpc_service::FilerMetaServiceImpl;
use powerfs_filer::meta_shard_manager::MetaShardManager;
use powerfs_filer::posix_service::PosixMetaServiceImpl;
use powerfs_filer::powerfs::filer_meta_service_server::FilerMetaServiceServer;
use powerfs_filer::powerfs::posix_meta_service_server::PosixMetaServiceServer;
use powerfs_filer::raft_group_manager::{Peer, RaftGroupManager, ShardId};
use powerfs_filer::shard_scheduler::ShardScheduler;
use powerfs_filer::shard_strategy::ShardStrategy;
use powerfs_master::proto::powerfs::{
    master_service_client::MasterServiceClient, RegisterFilerRequest,
};

#[derive(Parser)]
#[command(name = "powerfs-filer")]
#[command(version = "0.1.0")]
#[command(about = "PowerFS Filer Server - Metadata Sharding & Routing")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    #[arg(long, default_value = "0.0.0.0:8888")]
    s3_address: String,

    #[arg(long, default_value = "0.0.0.0:8889")]
    grpc_address: String,

    #[arg(long, default_value = "1")]
    node_id: u64,

    #[arg(long, default_value = "127.0.0.1:8889")]
    raft_address: String,

    #[arg(long, default_value = "./data/filer")]
    data_dir: String,

    #[arg(long, default_value = "4")]
    shard_count: u32,

    #[arg(long, default_value = "localhost:9333")]
    master_address: String,

    #[arg(long, default_value = "redis://localhost:6379/")]
    redis_address: String,

    #[arg(long, default_values = ["127.0.0.1:8889"])]
    peers: Vec<String>,

    #[arg(long, default_value = "default")]
    default_bucket: String,
}

#[derive(Subcommand)]
enum Command {
    /// Format/initialize metadata storage (like mkfs)
    Format {
        /// Bucket to create (default: default)
        #[arg(long, default_value = "default")]
        bucket: String,
    },
    /// Start the filer server (default)
    Start,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    powerfs_common::BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .log_startup();

    let args = Args::parse();

    info!("Starting PowerFS Filer Server");
    info!("  S3 Address: {}", args.s3_address);
    info!("  gRPC Address: {}", args.grpc_address);
    info!("  Node ID: {}", args.node_id);
    info!("  Raft Address: {}", args.raft_address);
    info!("  Data Dir: {}", args.data_dir);
    info!("  Shard Count: {}", args.shard_count);
    info!("  Master Address: {}", args.master_address);

    std::fs::create_dir_all(&args.data_dir)?;

    let shard_strategy = Arc::new(ShardStrategy::new(args.shard_count as u64));

    let raft_data_path = format!("{}/raft", args.data_dir);
    std::fs::create_dir_all(&raft_data_path)?;

    let raft_group_manager = Arc::new(RaftGroupManager::new(
        args.node_id,
        args.raft_address.clone(),
        raft_data_path,
    ));

    let shard_data_path = format!("{}/shards", args.data_dir);
    std::fs::create_dir_all(&shard_data_path)?;

    let meta_shard_manager = Arc::new(MetaShardManager::new(
        raft_group_manager.clone(),
        shard_strategy.clone(),
        shard_data_path,
    ));

    info!("Initializing {} shards...", args.shard_count);
    let peers: Vec<Peer> = args
        .peers
        .iter()
        .enumerate()
        .map(|(i, addr)| Peer {
            id: (i + 1) as u64,
            address: addr.clone(),
        })
        .collect();

    // Register peers first so Raft message transmitter can send to them
    for peer in &peers {
        raft_group_manager.register_peer(peer.clone()).await;
    }

    // Start Raft message transmitter BEFORE creating shards
    // so that broadcast channel has an active receiver when Raft starts sending messages
    raft_group_manager.clone().start_message_transmitter().await;

    for i in 0..args.shard_count {
        let shard_id = ShardId(i as u64);
        meta_shard_manager
            .create_shard(shard_id, peers.clone())
            .await?;
        info!("Shard {} initialized", i);
    }

    // Create both service implementations
    let grpc_service =
        FilerMetaServiceImpl::new(meta_shard_manager.clone(), shard_strategy.clone());
    let posix_service =
        PosixMetaServiceImpl::new(meta_shard_manager.clone(), shard_strategy.clone());

    // Start gRPC server FIRST so healthcheck can pass even if Raft is not ready
    info!(
        "Starting gRPC server on {} (before Raft initialization)",
        args.grpc_address
    );
    info!("  - FilerMetaService (S3 bucket compatibility)");
    info!("  - PosixMetaService (POSIX flat path)");

    let grpc_addr = args.grpc_address.parse()?;
    let grpc_server = Server::builder()
        .add_service(FilerMetaServiceServer::new(grpc_service))
        .add_service(PosixMetaServiceServer::new(posix_service))
        .serve(grpc_addr);

    tokio::spawn(async move {
        if let Err(e) = grpc_server.await {
            log::error!("gRPC server error: {}", e);
        }
    });

    // Bind HTTP listener
    let http_listener = TcpListener::bind(&args.s3_address).await?;
    let std_listener = http_listener.into_std()?;
    info!(
        "S3 endpoint ready on {} (before Raft initialization)",
        args.s3_address
    );

    // Start HTTP admin server for CRDT management
    let meta_shard_mgr = meta_shard_manager.clone();
    let admin_router = Router::new()
        .route(
            "/admin/status",
            get({
                let mgr = meta_shard_mgr.clone();
                move || {
                    let m = mgr.clone();
                    async move {
                        let status = m.get_filer_status().await;
                        axum::Json(serde_json::to_value(status).unwrap_or_default())
                    }
                }
            }),
        )
        .route(
            "/admin/shards",
            get({
                let mgr = meta_shard_mgr.clone();
                move || {
                    let m = mgr.clone();
                    async move {
                        let shards = m.list_shards_detail().await;
                        axum::Json(serde_json::to_value(shards).unwrap_or_default())
                    }
                }
            }),
        )
        .route(
            "/admin/crdt/overview",
            get({
                let mgr = meta_shard_mgr.clone();
                move || {
                    let m = mgr.clone();
                    async move {
                        let overview = m.get_crdt_overview();
                        axum::Json(serde_json::json!(overview))
                    }
                }
            }),
        )
        .route(
            "/admin/crdt/shards/:id",
            get({
                let mgr = meta_shard_mgr.clone();
                move |path: axum::extract::Path<u64>| {
                    let m = mgr.clone();
                    let id = path.0;
                    async move {
                        let states = m.get_shard_orset_states(ShardId(id));
                        axum::Json(serde_json::to_value(states).unwrap_or_default())
                    }
                }
            }),
        )
        .route(
            "/admin/crdt/shards/:id/dirs/:dir_ino",
            get({
                let mgr = meta_shard_mgr.clone();
                move |path: axum::extract::Path<(u64, u64)>| {
                    let m = mgr.clone();
                    let (id, dir_ino) = path.0;
                    async move {
                        match m.get_dir_orset_state(ShardId(id), dir_ino) {
                            Some(state) => axum::Json(json!(state)).into_response(),
                            None => {
                                (axum::http::StatusCode::NOT_FOUND, "Not found").into_response()
                            }
                        }
                    }
                }
            }),
        )
        .route(
            "/admin/crdt/cleanup",
            axum::routing::post({
                let mgr = meta_shard_mgr.clone();
                move |body: String| {
                    let m = mgr.clone();
                    async move {
                        let ttl_hours: u64 = body
                            .split('=')
                            .find(|s| s.starts_with("ttl"))
                            .and_then(|s| s.split('=').nth(1))
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(24);
                        let cleaned = m.cleanup_tombstones(ttl_hours);
                        axum::Json(json!({
                            "cleaned_count": cleaned,
                            "ttl_hours": ttl_hours
                        }))
                    }
                }
            }),
        );

    info!("Starting HTTP admin server on {}...", args.s3_address);
    tokio::spawn(async move {
        if let Err(e) = axum::Server::from_tcp(std_listener)
            .unwrap()
            .serve(admin_router.into_make_service())
            .await
        {
            error!("HTTP admin server error: {}", e);
        }
    });
    info!("HTTP admin server started on {}", args.s3_address);

    // Handle subcommand
    match &args.command {
        Some(Command::Format { bucket }) => {
            // Format mode: initialize metadata (like mkfs)
            // 1. Initialize POSIX root inode (inode 1)
            info!("Format mode: initializing POSIX root inode...");
            match meta_shard_manager.format_posix_root().await {
                Ok(root_inode) => {
                    info!("Successfully formatted POSIX root inode {}", root_inode);
                }
                Err(e) => {
                    error!("Failed to format POSIX root: {}", e);
                    return Err(e.into());
                }
            }

            // 2. Initialize S3 bucket root (if specified)
            info!("Format mode: initializing bucket '{}'", bucket);
            match meta_shard_manager.format_bucket_root(bucket).await {
                Ok(root_inode) => {
                    info!(
                        "Successfully formatted bucket '{}' with root inode {}",
                        bucket, root_inode
                    );
                    info!("Metadata initialization complete. You can now start the filer server.");
                    return Ok(());
                }
                Err(e) => {
                    error!("Failed to format bucket '{}': {}", bucket, e);
                    return Err(e.into());
                }
            }
        }
        Some(Command::Start) | None => {
            // Start mode: load existing metadata and serve
            info!("Start mode: loading existing metadata...");

            // Wait for Raft leader election to complete (with timeout)
            let shard_strategy_local = meta_shard_manager.get_shard_strategy();
            let posix_shard_id = shard_strategy_local.calculate_shard(1);
            info!(
                "Waiting for Raft leader election for shard {}...",
                posix_shard_id.0
            );
            let mut is_leader = false;
            let mut leader_found = false;
            for _ in 0..30 {
                if raft_group_manager.is_shard_leader(posix_shard_id).await {
                    is_leader = true;
                    leader_found = true;
                    info!("This node is the leader for shard {}", posix_shard_id.0);
                    break;
                }
                if let Some(leader_addr) = raft_group_manager.get_shard_leader(posix_shard_id).await
                {
                    leader_found = true;
                    info!(
                        "Leader elected for shard {}: {} (this node is follower)",
                        posix_shard_id.0, leader_addr
                    );
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }
            if !leader_found {
                warn!(
                    "Leader election timeout, continuing anyway (gRPC server is already running)"
                );
            }

            // Check and initialize POSIX root (only leader can do this)
            if !meta_shard_manager.has_posix_root() {
                if is_leader {
                    warn!("POSIX root inode not found. Initializing...");
                    match meta_shard_manager.format_posix_root().await {
                        Ok(root_inode) => {
                            info!("Initialized POSIX root inode {}", root_inode);
                        }
                        Err(e) => {
                            error!("Failed to initialize POSIX root: {}", e);
                            // Don't return error - gRPC server is already running
                            // The POSIX root will be initialized by the leader when it becomes available
                        }
                    }
                } else {
                    warn!("POSIX root inode not found, but this node is not leader. Waiting for leader to initialize...");
                    // Wait a bit for leader to initialize
                    for _ in 0..20 {
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                        if meta_shard_manager.has_posix_root() {
                            info!("POSIX root inode initialized by leader");
                            break;
                        }
                    }
                    if !meta_shard_manager.has_posix_root() {
                        warn!("POSIX root inode not initialized yet (will retry later)");
                    }
                }
            } else {
                info!("POSIX root inode already exists");
            }

            meta_shard_manager.load_root_inodes_from_shards();

            let buckets = meta_shard_manager.list_buckets();
            if buckets.is_empty() {
                warn!(
                    "No S3 buckets found. Run 'powerfs-filer format' first to initialize metadata."
                );
                warn!("Attempting to auto-initialize default bucket...");
                match meta_shard_manager
                    .format_bucket_root(&args.default_bucket)
                    .await
                {
                    Ok(root_inode) => {
                        info!(
                            "Auto-initialized default bucket '{}' with root inode {}",
                            args.default_bucket, root_inode
                        );
                    }
                    Err(e) => {
                        warn!("Failed to auto-initialize default bucket: {}", e);
                    }
                }
            } else {
                info!("Loaded {} S3 buckets: {:?}", buckets.len(), buckets);
            }
        }
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

    register_with_master(&args).await;

    info!("Filer server started successfully");

    tokio::signal::ctrl_c().await?;
    info!("Shutting down Filer server...");

    Ok(())
}

async fn register_with_master(args: &Args) {
    let address = args.raft_address.clone();
    let grpc_port: u32 = args
        .grpc_address
        .split(':')
        .next_back()
        .unwrap_or("8889")
        .parse()
        .unwrap_or(8889);
    let http_port: u32 = args
        .s3_address
        .split(':')
        .next_back()
        .unwrap_or("8888")
        .parse()
        .unwrap_or(8888);

    let shard_ids: Vec<u64> = (0..args.shard_count).map(|i| i as u64).collect();

    let request = RegisterFilerRequest {
        node_id: args.node_id.to_string(),
        address,
        grpc_port,
        http_port,
        shard_count: args.shard_count as u64,
        shard_ids,
    };

    let endpoint =
        match tonic::transport::Channel::from_shared(format!("http://{}", args.master_address)) {
            Ok(ep) => ep,
            Err(e) => {
                warn!("Failed to connect to master for registration: {}", e);
                return;
            }
        };

    let channel = match endpoint.connect().await {
        Ok(ch) => ch,
        Err(e) => {
            warn!("Failed to connect to master for registration: {}", e);
            return;
        }
    };

    let mut client = MasterServiceClient::new(channel);

    match client.register_filer(tonic::Request::new(request)).await {
        Ok(response) => {
            let resp = response.into_inner();
            if resp.success {
                info!("Successfully registered filer with master");
            } else {
                warn!("Filer registration failed: {}", resp.error);
            }
        }
        Err(e) => {
            warn!("Failed to register filer with master: {}", e);
        }
    }
}
