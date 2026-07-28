use clap::Parser;
use log::{error, info};
use std::sync::Arc;

use powerfs_common::build_info::BuildInfo;
use powerfs_common::error::PowerFsError;
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
    #[arg(long, default_value = "8888")]
    port: u16,

    #[arg(long, default_value = "8889")]
    grpc_port: u16,

    #[arg(long, default_value = "8890")]
    net_port: u16,

    #[arg(long)]
    ip: Option<String>,

    #[arg(long, short = 'D', default_value = "./data/filer")]
    data_dir: String,

    #[arg(long, default_value = "3")]
    shard_count: u32,

    #[arg(long, short = 'i', default_value = "1")]
    raft_id: u64,

    #[arg(long, short = 'm')]
    master: Vec<String>,

    #[arg(long)]
    raft_peer: Vec<String>,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).log_startup();

    let args = Args::parse();

    run_filer(args).await?;

    Ok(())
}

async fn run_filer(args: Args) -> powerfs_common::error::Result<()> {
    info!("Starting PowerFS Filer with sharding");

    let bind_ip = args.ip.as_deref().unwrap_or("0.0.0.0");
    let s3_address = format!("{}:{}", bind_ip, args.port);
    let grpc_address = format!("{}:{}", bind_ip, args.grpc_port);
    let net_address = format!("{}:{}", bind_ip, args.net_port);
    let raft_address = format!("{}:{}", bind_ip, args.grpc_port + 1);

    info!("  S3 Address: {}", s3_address);
    info!("  gRPC Address: {}", grpc_address);
    info!("  Net Address: {}", net_address);
    info!("  Data Dir: {}", args.data_dir);
    info!("  Shard Count: {}", args.shard_count);
    info!("  Raft ID: {}", args.raft_id);

    std::fs::create_dir_all(&args.data_dir)
        .map_err(|e| PowerFsError::Internal(format!("failed to create data dir: {}", e)))?;

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let redis_client =
        redis::Client::open(redis_url).map_err(|e| PowerFsError::Internal(e.to_string()))?;

    let metadata_store = Arc::new(MetadataStore::new(redis_client));

    let default_master = "127.0.0.1:9333".to_string();
    let master_addr = args.master.first().unwrap_or(&default_master);
    let master_client = Arc::new(S3MasterClient::new(master_addr));
    let master_api = Arc::new(MasterApi::Remote(master_client));

    let bucket_manager = Arc::new(BucketManager::new(metadata_store.clone(), master_api));
    let volume_router = Arc::new(VolumeRouter::new(metadata_store.clone()));
    let entry_manager = Arc::new(EntryManager::new(
        metadata_store.clone(),
        bucket_manager.clone(),
    ));
    let volume_client_pool = Arc::new(VolumeClientPool::new());

    let shard_strategy = Arc::new(ShardStrategy::new(args.shard_count as u64));

    let raft_data_path = format!("{}/raft", args.data_dir);
    std::fs::create_dir_all(&raft_data_path)
        .map_err(|e| PowerFsError::Internal(format!("failed to create raft dir: {}", e)))?;

    let raft_group_manager = Arc::new(RaftGroupManager::new(
        args.raft_id,
        raft_address.clone(),
        raft_data_path,
    ));

    let shard_data_path = format!("{}/shards", args.data_dir);
    std::fs::create_dir_all(&shard_data_path)
        .map_err(|e| PowerFsError::Internal(format!("failed to create shards dir: {}", e)))?;

    let meta_shard_manager = Arc::new(MetaShardManager::new(
        raft_group_manager.clone(),
        shard_strategy.clone(),
        shard_data_path,
    ));

    info!("Initializing {} metadata shards...", args.shard_count);
    let peers: Vec<powerfs_filer::Peer> = if args.raft_peer.is_empty() {
        vec![powerfs_filer::Peer {
            id: args.raft_id,
            address: raft_address.clone(),
        }]
    } else {
        args.raft_peer
            .iter()
            .enumerate()
            .map(|(i, addr)| powerfs_filer::Peer {
                id: (i + 1) as u64,
                address: addr.clone(),
            })
            .collect()
    };

    for peer in &peers {
        raft_group_manager.register_peer(peer.clone()).await;
    }
    raft_group_manager.clone().start_message_transmitter().await;

    for i in 0..args.shard_count {
        let shard_id = ShardId(i as u64);
        meta_shard_manager
            .create_shard(shard_id, peers.clone())
            .await
            .map_err(|e| PowerFsError::Internal(format!("failed to create shard {}: {}", i, e)))?;
        info!("Shard {} initialized", i);
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

    if args.net_port > 0 {
        let net_handler = Arc::new(FilerNetHandler::new(
            meta_shard_manager.clone(),
            shard_strategy.clone(),
        ));

        // Wrap with ManagedNetHandler for session management + middleware
        let net_manager = Arc::new(ServerConnectionManager::new());
        let managed_handler = Arc::new(ManagedNetHandler::from_arc(
            net_manager.clone(),
            net_handler,
        ));

        if let Ok(net_server) = PowerFsNetServer::bind_with_manager(
            bind_ip,
            args.net_port,
            managed_handler,
            net_manager,
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
    if args.net_port > 0 {
        info!("Net endpoint: {}", net_address);
    }
    info!("Connected to master(s): {:?}", args.master);

    filer_server.serve().await?;

    Ok(())
}
