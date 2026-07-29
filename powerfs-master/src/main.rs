use clap::Parser;
use log::info;
use std::sync::Arc;

use powerfs_common::build_info::BuildInfo;
use powerfs_common::config::PowerFsConfig;
use powerfs_common::types::ClusterConfig;
use powerfs_master::master::MasterNode;

#[derive(Parser)]
#[command(name = "powerfs-master")]
#[command(version = "0.1.0")]
#[command(about = "PowerFS Master Node - Cluster coordination & metadata management")]
struct Args {
    /// 配置文件路径（必填，所有端口和地址必须在配置文件中设置）
    #[arg(short, long, required = true)]
    config: String,

    /// 可选：覆盖节点ID
    #[arg(long)]
    raft_id: Option<u64>,

    /// 可选：覆盖监听IP
    #[arg(long)]
    ip: Option<String>,

    /// 可选：覆盖广播地址
    #[arg(long)]
    advertise_addr: Option<String>,

    /// 可选：覆盖peers
    #[arg(long)]
    peer: Vec<String>,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let cfg = load_config(&args.config);
    let master_cfg = cfg.master.clone();

    let log_level = cfg.global.log_level.as_str();
    env_logger::Builder::new()
        .filter_level(match log_level {
            "debug" => log::LevelFilter::Debug,
            "warn" => log::LevelFilter::Warn,
            "error" => log::LevelFilter::Error,
            _ => log::LevelFilter::Info,
        })
        .init();

    BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).log_startup();

    // 从配置文件获取所有必需值 - 无硬编码默认值
    let port = master_cfg.port;
    let net_port = master_cfg.net_port;
    let dir = master_cfg.dir;

    let raft_id = args.raft_id.unwrap_or(master_cfg.raft_id);
    let raft_dir = master_cfg
        .raft_dir
        .unwrap_or_else(|| format!("{}/raft", dir));
    let meta_dir = master_cfg
        .meta_dir
        .unwrap_or_else(|| format!("{}/meta", dir));

    let peers = if !args.peer.is_empty() {
        args.peer
    } else {
        master_cfg.peers
    };

    let ip = args.ip.unwrap_or_else(|| {
        master_cfg.ip.unwrap_or_else(|| {
            eprintln!("ERROR: master.ip must be set in config or via --ip (no default value allowed)");
            std::process::exit(1);
        })
    });

    std::fs::create_dir_all(&dir)?;
    std::fs::create_dir_all(&raft_dir)?;
    std::fs::create_dir_all(&meta_dir)?;

    let bind_address = format!("{}:{}", ip, port);

    let advertise_addr = args
        .advertise_addr
        .or(master_cfg.advertise_addr)
        .unwrap_or_else(|| {
            eprintln!("ERROR: master.advertise_addr must be set in config or via --advertise-addr (no default value allowed)");
            std::process::exit(1);
        });

    info!("Starting PowerFS Master Node");
    info!("  Bind Address: {}", bind_address);
    info!("  Raft Address: {}", advertise_addr);
    info!("  Net Port: {}", net_port);
    info!("  Raft ID: {}", raft_id);
    info!("  Data Dir: {}", dir);

    let master = MasterNode::new(
        &bind_address,
        &advertise_addr,
        None::<ClusterConfig>,
        &raft_dir,
        raft_id,
        peers,
        net_port,
    )
    .await?;

    info!("Master node initialized: {:?}", master.id());
    info!("Listening on: {}", bind_address);
    info!("Data directory: {}", dir);

    Arc::new(master).start().await?;

    Ok(())
}
