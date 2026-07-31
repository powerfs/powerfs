use clap::Parser;
use log::info;
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
    /// 配置文件路径（必填，所有端口和地址必须在配置文件中设置）
    #[arg(short, long, required = true)]
    config: String,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).log_startup();

    let args = Args::parse();
    let cfg = load_config(&args.config);

    run_s3(cfg).await?;

    Ok(())
}

async fn run_s3(cfg: PowerFsConfig) -> Result<()> {
    info!("Starting PowerFS S3 Server");

    let s3_cfg = cfg.s3.clone();

    // 所有值从配置获取 - 无硬编码默认值
    let port = s3_cfg.port;
    let master_addr = s3_cfg.master_address.clone();
    let access_key = s3_cfg.access_key.clone();
    let secret_key = s3_cfg.secret_key.clone();

    // 绑定地址
    let bind_ip = s3_cfg.ip.clone().unwrap_or_else(|| "0.0.0.0".to_string());
    let address = format!("{}:{}", bind_ip, port);

    let s3_addr: std::net::SocketAddr = address.parse()?;

    if master_addr.is_empty() {
        return Err(PowerFsError::Internal(
            "s3.master_address must not be empty".to_string(),
        ));
    }

    // Build the list of master endpoints for resilient client (leader discovery
    // + failover).  Fall back to [master_address] when master_endpoints is not
    // configured so that single-master setups keep working.
    let master_endpoints: Vec<String> = if s3_cfg.master_endpoints.is_empty() {
        vec![master_addr.clone()]
    } else {
        s3_cfg.master_endpoints.clone()
    };
    info!("S3 master endpoints: {:?}", master_endpoints);

    let directory_tree: Arc<dyn DirectoryTreeApi> =
        Arc::new(RemoteDirectoryTree::new(master_endpoints.clone())?);

    let master_api = Arc::new(MasterApi::Remote(Arc::new(S3MasterClient::new(
        master_endpoints,
    )?)));

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
