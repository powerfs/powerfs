use powerfs_common::{
    types::{NodeId, ClusterConfig, VolumeId},
    utils::{generate_node_id, generate_volume_id},
    error::{PowerFsError, Result},
};
use powerfs_core::storage::StorageManager;
use powerfs_master::master::MasterNode;
use powerfs_fuse::fuse::FuseClient;
use tokio;
use clap::{Parser, Subcommand};
use log::{info, warn, error};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "powerfs", version = "0.1.0", about = "PowerFS - Zero-jitter unified parallel file system")]
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
        
        #[arg(long, short, default_value = "./data/master")]
        dir: String,
        
        #[arg(long)]
        ip: Option<String>,
    },
    
    Volume {
        #[arg(long, short, default_value = "8080")]
        port: u16,
        
        #[arg(long, short, default_value = "./data/volume")]
        dir: String,
        
        #[arg(long, short)]
        master: String,
        
        #[arg(long)]
        ip: Option<String>,
        
        #[arg(long, short, default_value = "1073741824")]
        max_volume_size: u64,
    },
    
    Filer {
        #[arg(long, short, default_value = "8888")]
        port: u16,
        
        #[arg(long, short)]
        master: String,
        
        #[arg(long)]
        ip: Option<String>,
    },
    
    Fuse {
        #[arg(long, short)]
        dir: String,
        
        #[arg(long, short)]
        master: Option<String>,
        
        #[arg(long, short, default_value = "8080")]
        volume_port: u16,
    },
    
    Mount {
        #[arg(long, short)]
        dir: String,
        
        #[arg(long, short)]
        master: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    env_logger::Builder::new()
        .filter_level(match cli.log_level.as_str() {
            "debug" => log::LevelFilter::Debug,
            "info" => log::LevelFilter::Info,
            "warn" => log::LevelFilter::Warn,
            "error" => log::LevelFilter::Error,
            _ => log::LevelFilter::Info,
        })
        .init();
    
    match cli.command {
        Commands::Master { port, dir, ip } => {
            run_master(port, &dir, ip).await
        }
        
        Commands::Volume { port, dir, master, ip, max_volume_size } => {
            run_volume(port, &dir, &master, ip, max_volume_size).await
        }
        
        Commands::Filer { port, master, ip } => {
            run_filer(port, &master, ip).await
        }
        
        Commands::Fuse { dir, master, volume_port } => {
            run_fuse(&dir, master, volume_port).await
        }
        
        Commands::Mount { dir, master } => {
            run_mount(&dir, master).await
        }
    }
}

async fn run_master(port: u16, dir: &str, ip: Option<String>) -> Result<()> {
    info!("Starting PowerFS Master node");
    
    std::fs::create_dir_all(dir)?;
    
    let address = match ip {
        Some(ip) => format!("{}:{}", ip, port),
        None => format!("0.0.0.0:{}", port),
    };
    
    let master = MasterNode::new(&address, None).await?;
    
    info!("Master node initialized: {:?}", master.id());
    info!("Listening on: {}", address);
    info!("Data directory: {}", dir);
    
    master.start().await?;
    
    Ok(())
}

async fn run_volume(port: u16, dir: &str, master: &str, ip: Option<String>, _max_volume_size: u64) -> Result<()> {
    info!("Starting PowerFS Volume node");
    
    let node_id = generate_node_id();
    std::fs::create_dir_all(dir)?;
    
    let address = match ip {
        Some(ip) => format!("{}:{}", ip, port),
        None => format!("0.0.0.0:{}", port),
    };
    
    let storage_manager = Arc::new(StorageManager::new(node_id.clone(), dir.to_string()));
    
    storage_manager.load_volumes()?;
    
    info!("Volume node initialized: {:?}", node_id);
    info!("Listening on: {}", address);
    info!("Data directory: {}", dir);
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

async fn run_fuse(dir: &str, master: Option<String>, volume_port: u16) -> Result<()> {
    info!("Starting PowerFS FUSE client");
    
    let node_id = generate_node_id();
    let data_dir = format!("./data/fuse_{}", volume_port);
    std::fs::create_dir_all(&data_dir)?;
    
    let storage_manager = Arc::new(StorageManager::new(node_id, data_dir));
    
    storage_manager.load_volumes()?;
    
    let fuse_client = FuseClient::new(storage_manager, dir);
    
    info!("Mounting PowerFS at: {}", dir);
    
    if let Some(m) = &master {
        info!("Connected to master: {}", m);
    }
    
    if let Err(e) = fuse_client.mount().await {
        error!("Failed to mount FUSE: {}", e);
        return Err(e);
    }
    
    tokio::signal::ctrl_c().await?;
    
    fuse_client.unmount().await?;
    
    Ok(())
}

async fn run_mount(dir: &str, master: Option<String>) -> Result<()> {
    info!("Mounting PowerFS at: {}", dir);
    
    if let Some(m) = &master {
        info!("Connected to master: {}", m);
    }
    
    let node_id = generate_node_id();
    let storage_manager = Arc::new(StorageManager::new(node_id, "./data/mount".to_string()));
    storage_manager.load_volumes()?;
    
    let fuse_client = FuseClient::new(storage_manager, dir);
    fuse_client.mount().await?;
    
    tokio::signal::ctrl_c().await?;
    
    fuse_client.unmount().await?;
    
    Ok(())
}
