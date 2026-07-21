use clap::Parser;
use log::{info, warn};
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::transport::Server;

use powerfs_filer::grpc_service::FilerMetaServiceImpl;
use powerfs_filer::meta_shard_manager::MetaShardManager;
use powerfs_filer::powerfs::filer_meta_service_server::FilerMetaServiceServer;
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

    for i in 0..args.shard_count {
        let shard_id = ShardId(i as u64);
        meta_shard_manager
            .create_shard(shard_id, peers.clone())
            .await?;
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

    register_with_master(&args).await;

    let grpc_service =
        FilerMetaServiceImpl::new(meta_shard_manager.clone(), shard_strategy.clone());

    info!("Starting gRPC server on {}", args.grpc_address);

    let grpc_addr = args.grpc_address.parse()?;
    let grpc_server = Server::builder()
        .add_service(FilerMetaServiceServer::new(grpc_service))
        .serve(grpc_addr);

    info!("Filer server started successfully");

    tokio::spawn(async move {
        if let Err(e) = grpc_server.await {
            log::error!("gRPC server error: {}", e);
        }
    });

    let _listener = TcpListener::bind(&args.s3_address).await?;
    info!("S3 endpoint ready on {}", args.s3_address);

    tokio::signal::ctrl_c().await?;
    info!("Shutting down Filer server...");

    Ok(())
}

async fn register_with_master(args: &Args) {
    let address = args.raft_address.clone();
    let grpc_port: u32 = args.grpc_address.split(':').last().unwrap_or("8889").parse().unwrap_or(8889);
    let http_port: u32 = args.s3_address.split(':').last().unwrap_or("8888").parse().unwrap_or(8888);

    let shard_ids: Vec<u64> = (0..args.shard_count).map(|i| i as u64).collect();

    let request = RegisterFilerRequest {
        node_id: args.node_id.to_string(),
        address,
        grpc_port,
        http_port,
        shard_count: args.shard_count as u64,
        shard_ids,
    };

    let endpoint = match tonic::transport::Channel::from_shared(format!("http://{}", args.master_address)) {
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
