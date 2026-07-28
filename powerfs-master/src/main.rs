use clap::Parser;
use log::info;
use std::sync::Arc;

use powerfs_common::build_info::BuildInfo;
use powerfs_common::types::ClusterConfig;
use powerfs_master::master::MasterNode;

#[derive(Parser)]
#[command(name = "powerfs-master")]
#[command(version = "0.1.0")]
#[command(about = "PowerFS Master Node - Cluster coordination & metadata management")]
struct Args {
    #[arg(long, short = 'P', default_value = "9333")]
    port: u16,

    #[arg(long, default_value = "9334")]
    net_port: u16,

    #[arg(long, short = 'D', default_value = "./data/master")]
    dir: String,

    #[arg(long, short = 'r')]
    raft_dir: Option<String>,

    #[arg(long, short = 'm')]
    meta_dir: Option<String>,

    #[arg(long)]
    ip: Option<String>,

    #[arg(long)]
    advertise_addr: Option<String>,

    #[arg(long, short = 'i', default_value = "1")]
    raft_id: u64,

    #[arg(long, short = 'p')]
    peer: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).log_startup();

    let args = Args::parse();

    let raft_dir = args
        .raft_dir
        .unwrap_or_else(|| format!("{}/raft", args.dir));
    let meta_dir = args
        .meta_dir
        .unwrap_or_else(|| format!("{}/meta", args.dir));

    std::fs::create_dir_all(&args.dir)?;
    std::fs::create_dir_all(&raft_dir)?;
    std::fs::create_dir_all(&meta_dir)?;

    let bind_address = match args.ip {
        Some(ref ip) => format!("{}:{}", ip, args.port),
        None => format!("0.0.0.0:{}", args.port),
    };

    let raft_address = args.advertise_addr.unwrap_or_else(|| bind_address.clone());

    let master = MasterNode::new(
        &bind_address,
        &raft_address,
        None::<ClusterConfig>,
        &raft_dir,
        args.raft_id,
        args.peer,
        args.net_port,
    )
    .await?;

    info!("Master node initialized: {:?}", master.id());
    info!("Listening on: {}", bind_address);
    info!("Data directory: {}", args.dir);

    Arc::new(master).start().await?;

    Ok(())
}
