use clap::Parser;
use log::{info, warn};
use std::sync::Arc;

use powerfs_common::build_info::BuildInfo;
use powerfs_common::{
    config::PowerFsConfig,
    error::{PowerFsError, Result},
};
use powerfs_master::{
    lock_manager::LockManager,
    s3::{
        auth::AuthManager,
        directory_tree_api::{DirectoryTreeApi, RemoteDirectoryTree},
        master_client::S3MasterClient,
        MasterApi, S3Server,
    },
    volume_client::VolumeClientPool,
};

#[derive(Parser)]
#[command(name = "powerfs-s3")]
#[command(version = "0.1.0")]
#[command(about = "PowerFS S3 Gateway - S3-compatible object storage API")]
struct Args {
    #[arg(long, short, default_value = "9000")]
    port: u16,

    #[arg(long, short)]
    master: Option<String>,

    #[arg(long)]
    ip: Option<String>,

    #[arg(long, short, default_value = "./data/s3")]
    dir: String,

    #[arg(long, default_value = "powerfs")]
    access_key: String,

    #[arg(long, default_value = "powerfs123")]
    secret_key: String,

    #[arg(long, short = 'c')]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).log_startup();

    let args = Args::parse();

    run_s3(args).await?;

    Ok(())
}

async fn run_s3(args: Args) -> Result<()> {
    info!("Starting PowerFS S3 Server");

    let cfg = args
        .config
        .as_ref()
        .and_then(|c| match PowerFsConfig::load_from_file(c) {
            Ok(cfg) => {
                if let Err(e) = cfg.validate() {
                    warn!("Config validation failed: {}, using defaults", e);
                    None
                } else {
                    info!("Loaded config from: {}", c);
                    Some(cfg)
                }
            }
            Err(e) => {
                warn!("Failed to load config file: {}, using defaults", e);
                None
            }
        });

    let s3_cfg = cfg.as_ref().map(|c| &c.s3);

    let port = s3_cfg.map(|s| s.port).unwrap_or(args.port);
    let master_addr = args
        .master
        .or_else(|| s3_cfg.map(|s| s.master_address.clone()))
        .unwrap_or_else(|| "127.0.0.1:9333".to_string());
    let ip = args
        .ip
        .or_else(|| cfg.as_ref().and_then(|c| c.s3.ip.clone()));
    let access_key = s3_cfg
        .map(|s| s.access_key.clone())
        .unwrap_or_else(|| args.access_key.clone());
    let secret_key = s3_cfg
        .map(|s| s.secret_key.clone())
        .unwrap_or_else(|| args.secret_key.clone());

    let address = match ip {
        Some(ref ip) => format!("{}:{}", ip, port),
        None => format!("0.0.0.0:{}", port),
    };

    let s3_addr: std::net::SocketAddr = address.parse()?;

    let directory_tree: Arc<dyn DirectoryTreeApi> =
        Arc::new(RemoteDirectoryTree::new(&master_addr));

    let master_api = Arc::new(MasterApi::Remote(Arc::new(S3MasterClient::new(
        &master_addr,
    ))));

    let volume_client_pool = Arc::new(VolumeClientPool::new());
    let lock_manager = Arc::new(LockManager::new());
    let auth_manager = Arc::new(AuthManager::with_default_credentials(
        &access_key,
        &secret_key,
    ));

    let s3_server = S3Server::new(
        s3_addr,
        directory_tree,
        master_api,
        volume_client_pool,
        lock_manager,
        auth_manager,
    );

    info!("S3 Server initialized");
    info!("Listening on: {}", address);
    info!("Connected to master: {}", master_addr);

    s3_server
        .serve()
        .await
        .map_err(|e| PowerFsError::Internal(format!("S3 server error: {}", e)))?;

    Ok(())
}
