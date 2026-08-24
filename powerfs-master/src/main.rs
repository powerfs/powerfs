use clap::Parser;
use log::info;
use std::sync::Arc;

use powerfs_common::build_info::BuildInfo;
use powerfs_common::config::{PowerFsConfig, ServiceType};
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
    match PowerFsConfig::load_for_service(config_path, ServiceType::Master) {
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

    let log_level = cfg.global.log_level.clone();
    // 使用 dynamic_log（支持运行时动态调整 + target 过滤 + 子系统开关）
    powerfs_common::dynamic_log::init(&log_level, None);

    BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).log_startup();

    // 从配置文件获取所有必需值 - 无硬编码默认值
    let port = master_cfg.port;
    let raft_port = master_cfg.raft_port;
    let net_port = master_cfg.net_port;
    let metrics_port = master_cfg.metrics_port;
    let dir = master_cfg.dir;

    let raft_id = args.raft_id.unwrap_or(master_cfg.raft_id);
    let raft_dir = master_cfg
        .raft_dir
        .unwrap_or_else(|| format!("{}/raft", dir));
    let meta_dir = master_cfg
        .meta_dir
        .unwrap_or_else(|| format!("{}/meta", dir));
    let ca_dir = master_cfg.ca_dir.unwrap_or_else(|| format!("{}/ca", dir));
    let admin_token = master_cfg.admin_token.clone();
    let registration_token = master_cfg.registration_token.clone();

    let peers = if !args.peer.is_empty() {
        args.peer
    } else {
        master_cfg.raft_peers
    };

    let ip = args.ip.unwrap_or_else(|| {
        master_cfg.ip.unwrap_or_else(|| {
            eprintln!(
                "ERROR: master.ip must be set in config or via --ip (no default value allowed)"
            );
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

    // Raft inter-node gRPC address: advertise_ip:raft_port
    // (advertise_addr is ip:port for MasterService; raft_port is separate)
    let advertise_ip = advertise_addr.split(':').next().unwrap_or(&advertise_addr);
    let raft_address = format!("{}:{}", advertise_ip, raft_port);

    info!("Starting PowerFS Master Node");
    info!("  Bind Address: {}", bind_address);
    info!("  Raft Address: {}", raft_address);
    info!("  Master Port: {}", port);
    info!("  Raft Port: {}", raft_port);
    info!("  Net Port: {}", net_port);
    info!("  Metrics/Admin Port: {}", metrics_port);
    info!("  Raft ID: {}", raft_id);
    info!("  Data Dir: {}", dir);

    // 端口冲突自检：全部 4 个端口必须唯一（无端口加减推导, 配置文件显式配置）
    {
        let used = [
            ("port", port),
            ("raft_port", raft_port),
            ("net_port", net_port),
            ("metrics_port", metrics_port),
        ];
        for (i, (na, a)) in used.iter().enumerate() {
            for (nb, b) in &used[i + 1..] {
                if a == b {
                    eprintln!(
                        "ERROR: master.{} and master.{} both use port {} — must be distinct (all ports explicitly configured, no derivation)",
                        na, nb, a
                    );
                    std::process::exit(1);
                }
            }
        }
    }

    let master = MasterNode::new(
        &bind_address,
        &raft_address,
        None::<ClusterConfig>,
        &raft_dir,
        raft_id,
        peers,
        net_port,
        metrics_port,
        admin_token,
        Some(ca_dir),
        registration_token,
    )
    .await?;

    info!("Master node initialized: {:?}", master.id());
    info!("Listening on: {}", bind_address);
    info!("Data directory: {}", dir);

    Arc::new(master).start().await?;

    Ok(())
}
