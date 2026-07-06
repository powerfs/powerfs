use clap::{Parser, Subcommand};
use log::info;
use powerfs_common::{error::Result, utils::generate_node_id};
use powerfs_core::storage::StorageManager;
use powerfs_fuse::FuserApp;
use powerfs_master::{master::MasterNode, s3::S3Server, s3::MasterApi, s3::master_client::S3MasterClient};
use std::sync::Arc;

#[derive(Parser)]
#[command(
    name = "powerfs",
    version = "0.1.0",
    about = "PowerFS - Zero-jitter unified parallel file system"
)]
struct Cli {
    #[arg(long, default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Master {
        #[arg(long, short, default_value = "9333")]
        port: u16,

        /// Data directory (meta, raft will be created inside)
        #[arg(long, short, default_value = "./data/master")]
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
    },

    Volume {
        #[arg(long, short, default_value = "8080")]
        port: u16,

        /// Data directory (meta, data will be created inside)
        #[arg(long, short, default_value = "./data/volume")]
        dir: String,

        /// Meta storage directory (default: <dir>/meta)
        #[arg(long, short = 'm')]
        meta_dir: Option<String>,

        /// Data storage directory (default: <dir>/data)
        #[arg(long, short = 'd')]
        data_dir: Option<String>,

        /// Master address
        #[arg(long, short)]
        master: String,

        /// Bind IP address
        #[arg(long)]
        ip: Option<String>,

        /// Max volume size in bytes
        #[arg(long, short, default_value = "1073741824")]
        max_volume_size: u64,
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
    },
}

#[tokio::main]
#[allow(clippy::result_large_err)]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(cli.log_level.as_str()),
    )
    .init();

    match cli.command {
        Commands::Master {
            port,
            dir,
            raft_dir,
            meta_dir,
            ip,
        } => run_master(port, &dir, raft_dir, meta_dir, ip).await,

        Commands::Volume {
            port,
            dir,
            meta_dir,
            data_dir,
            master,
            ip,
            max_volume_size,
        } => run_volume(port, &dir, meta_dir, data_dir, &master, ip, max_volume_size).await,

        Commands::Filer { port, master, ip } => run_filer(port, &master, ip).await,

        Commands::Fuse {
            dir,
            master,
            volume_port,
        } => run_fuse(&dir, master, volume_port).await,

        Commands::Mount { dir, master } => run_mount(&dir, master).await,

        Commands::S3 { port, master, ip, dir } => run_s3(port, &master, ip, &dir).await,
    }
}

async fn run_master(
    port: u16,
    dir: &str,
    raft_dir: Option<String>,
    meta_dir: Option<String>,
    ip: Option<String>,
) -> Result<()> {
    info!("Starting PowerFS Master node");

    // Calculate subdirectories
    let raft_dir = raft_dir.unwrap_or_else(|| format!("{}/raft", dir));
    let meta_dir = meta_dir.unwrap_or_else(|| format!("{}/meta", dir));

    // Create directories
    std::fs::create_dir_all(dir)?;
    std::fs::create_dir_all(&raft_dir)?;
    std::fs::create_dir_all(&meta_dir)?;

    let address = match ip {
        Some(ip) => format!("{}:{}", ip, port),
        None => format!("0.0.0.0:{}", port),
    };

    let master = MasterNode::new(&address, None, &raft_dir).await?;

    info!("Master node initialized: {:?}", master.id());
    info!("Listening on: {}", address);
    info!("Data directory: {}", dir);
    info!("Raft directory: {}", raft_dir);
    info!("Meta directory: {}", meta_dir);

    Arc::new(master).start().await?;

    Ok(())
}

async fn run_volume(
    port: u16,
    dir: &str,
    meta_dir: Option<String>,
    data_dir: Option<String>,
    master: &str,
    ip: Option<String>,
    _max_volume_size: u64,
) -> Result<()> {
    info!("Starting PowerFS Volume node");

    // Calculate subdirectories
    let meta_dir = meta_dir.unwrap_or_else(|| format!("{}/meta", dir));
    let data_dir = data_dir.unwrap_or_else(|| format!("{}/data", dir));

    // Create directories
    std::fs::create_dir_all(dir)?;
    std::fs::create_dir_all(&meta_dir)?;
    std::fs::create_dir_all(&data_dir)?;

    let address = match ip {
        Some(ip) => format!("{}:{}", ip, port),
        None => format!("0.0.0.0:{}", port),
    };

    let node_id = generate_node_id();
    let storage_manager = Arc::new(StorageManager::new(node_id.clone(), data_dir.clone()));

    storage_manager.load_volumes()?;

    info!("Volume node initialized: {:?}", node_id);
    info!("Listening on: {}", address);
    info!("Data directory: {}", dir);
    info!("Meta directory: {}", meta_dir);
    info!("Data storage: {}", data_dir);
    info!("Connected to master: {}", master);

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
    let fuse_app = FuserApp::new(master_addr, dir, "default", "000", 8).await?;

    info!("Mounting PowerFS at: {}", dir);
    info!("Connected to master: {}", master_addr);

    fuse_app.run().await
}

async fn run_mount(dir: &str, master: Option<String>) -> Result<()> {
    info!("Mounting PowerFS at: {}", dir);

    let master_addr = master.as_deref().unwrap_or("localhost:9334");
    let fuse_app = FuserApp::new(master_addr, dir, "default", "000", 8).await?;

    info!("Connected to master: {}", master_addr);

    fuse_app.run().await
}

async fn run_s3(port: u16, master: &str, ip: Option<String>, dir: &str) -> Result<()> {
    info!("Starting PowerFS S3 Server");

    std::fs::create_dir_all(dir)?;

    let address = match ip {
        Some(ip) => format!("{}:{}", ip, port),
        None => format!("0.0.0.0:{}", port),
    };

    let s3_addr: std::net::SocketAddr = address.parse()?;

    let directory_tree = Arc::new(powerfs_master::directory_tree::DirectoryTree::new(std::path::Path::new(dir))
        .map_err(|e| powerfs_common::error::PowerFsError::Internal(format!("Failed to create directory tree: {}", e)))?);

    let master_api = Arc::new(MasterApi::Remote(Arc::new(S3MasterClient::new(master))));

    let volume_client_pool = Arc::new(powerfs_master::volume_client::VolumeClientPool::new());

    let lock_manager = Arc::new(powerfs_master::lock_manager::LockManager::new());

    let s3_server = S3Server::new(
        s3_addr,
        directory_tree,
        master_api,
        volume_client_pool,
        lock_manager,
    );

    info!("S3 Server initialized");
    info!("Listening on: {}", address);
    info!("Connected to master: {}", master);
    info!("Data directory: {}", dir);

    s3_server.serve().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("S3 server error: {}", e))
    })?;

    Ok(())
}
