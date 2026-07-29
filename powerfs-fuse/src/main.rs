use clap::Parser;
use log::{error, info, warn};
use std::ffi::CString;
use std::io::Write;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;

use powerfs_common::config::PowerFsConfig;
use powerfs_fuse::FuseApp;

static MOUNT_POINT_PATH: OnceLock<CString> = OnceLock::new();
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

#[derive(Parser, Debug)]
#[command(name = "powerfs-fuse")]
#[command(about = "PowerFS FUSE client - mount PowerFS as a filesystem via powerfs-net")]
struct Args {
    /// 配置文件路径（必填，所有端口和地址必须在配置文件中设置）
    #[arg(long, short = 'c', required = true)]
    config: String,

    /// 可选：覆盖Mount点路径
    #[arg(long)]
    mount_point: Option<String>,

    /// 可选：覆盖Collection名称
    #[arg(long)]
    collection: Option<String>,

    /// 可选：覆盖Replication设置
    #[arg(long)]
    replication: Option<String>,

    /// 启用详细日志
    #[arg(short, long)]
    verbose: bool,

    /// 容器模式：安装SIGTERM/SIGINT处理器
    #[arg(long)]
    container: bool,

    /// 日志文件路径
    #[arg(long)]
    log_file: Option<String>,

    /// 启用数据完整性验证
    #[arg(long)]
    verify_data: bool,
}

/// 异步信号安全的处理器：只调用write(2)和umount2(2)
extern "C" fn handle_signal(sig: i32) {
    let sig_name = match sig {
        libc::SIGTERM => "SIGTERM",
        libc::SIGINT => "SIGINT",
        libc::SIGHUP => "SIGHUP",
        _ => "unknown",
    };
    let msg = format!("powerfs-fuse: received {}, unmounting\n", sig_name);
    unsafe {
        libc::write(2, msg.as_ptr() as *const _, msg.len());
    }
    if let Some(c_path) = MOUNT_POINT_PATH.get() {
        unsafe {
            libc::umount2(c_path.as_ptr(), libc::MNT_FORCE);
        }
    }
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

fn install_signal_handlers(mount_point: &str) {
    let c_path = CString::new(mount_point).expect("invalid mount point path");
    let _ = MOUNT_POINT_PATH.set(c_path);

    for sig in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
        unsafe {
            libc::signal(sig, handle_signal as *const () as usize);
        }
    }
    info!("Container mode: signal handlers installed (SIGTERM/SIGINT/SIGHUP trigger graceful umount + exit)");
}

/// 从配置文件加载配置，失败时直接退出
fn load_config(config_path: &str) -> PowerFsConfig {
    match PowerFsConfig::load_or_error(config_path) {
        Ok(cfg) => {
            info!("Successfully loaded configuration from: {}", config_path);
            cfg
        }
        Err(e) => {
            eprintln!("ERROR: Failed to load configuration: {}", e);
            eprintln!("You must provide a valid configuration file with all required ports and addresses.");
            eprintln!("Use PowerFsConfig::generate_template() to create a template configuration.");
            process::exit(1);
        }
    }
}

fn main() {
    let args = Args::parse();

    // 强制加载配置文件，失败直接退出
    let cfg = load_config(&args.config);
    let fuse_cfg = cfg.fuse.clone();

    // 从配置获取所有必需的地址和端口
    let master_addrs = fuse_cfg.master_addresses.clone();
    let mount_point = args
        .mount_point
        .unwrap_or_else(|| fuse_cfg.mount_point.clone());
    let collection = args
        .collection
        .unwrap_or_else(|| fuse_cfg.collection.clone());
    let replication = args
        .replication
        .unwrap_or_else(|| fuse_cfg.replication.clone());

    let master_net_port = fuse_cfg.master_net_port;
    let volume_net_port = fuse_cfg.volume_net_port;
    let volume_addrs = fuse_cfg.volume_addresses.clone();

    // 从配置获取filer地址（取第一个）
    let (filer_addr, filer_net_port) = if let Some(first_filer) = fuse_cfg.filer_addresses.first() {
        // 解析 host:port 或 仅host
        let parts: Vec<&str> = first_filer.split(':').collect();
        let host = parts.first().unwrap_or(&"127.0.0.1").to_string();
        (host, fuse_cfg.filer_net_port)
    } else {
        // 这个分支不会执行，因为validate已经检查了filer_addresses非空
        eprintln!("ERROR: filer_addresses is empty in configuration");
        process::exit(1);
    };

    let verbose = args.verbose || fuse_cfg.verbose;
    let container = args.container || fuse_cfg.container;
    let log_level = if verbose { "debug" } else { "info" };
    let log_file = args.log_file.clone().or(fuse_cfg.log_file);

    // 验证配置完整性
    if master_addrs.is_empty() {
        eprintln!("ERROR: fuse.master_addresses must not be empty");
        process::exit(1);
    }
    if volume_addrs.is_empty() {
        eprintln!("ERROR: fuse.volume_addresses must not be empty");
        process::exit(1);
    }
    if master_net_port == 0 {
        eprintln!("ERROR: fuse.master_net_port must be set");
        process::exit(1);
    }
    if volume_net_port == 0 {
        eprintln!("ERROR: fuse.volume_net_port must be set");
        process::exit(1);
    }

    // 配置日志
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level));

    builder.format(|buf, record| {
        writeln!(
            buf,
            "[{}] [{}] [{}] {}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            record.level(),
            record.target(),
            record.args()
        )
    });

    if let Some(log_file_path) = &log_file {
        use std::fs::{self, File};
        use std::path::Path;

        let log_path = Path::new(log_file_path);
        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent).unwrap_or_else(|e| {
                eprintln!("Failed to create log directory: {}", e);
            });
        }

        let file = File::create(log_file_path).unwrap_or_else(|e| {
            eprintln!("Failed to create log file: {}", e);
            std::process::exit(1);
        });

        builder.target(env_logger::Target::Pipe(Box::new(file)));
        info!("Logging to file: {}", log_file_path);
    }

    builder.init();

    powerfs_common::BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .log_startup();

    info!("PowerFS FUSE Client starting (powerfs-net)...");
    info!("  Config file: {}", args.config);
    info!("  Masters: {}", master_addrs.join(", "));
    info!("  Master net port: {}", master_net_port);
    info!("  Filer: {}:{}", filer_addr, filer_net_port);
    info!("  Volume addresses: {:?}", volume_addrs);
    info!("  Volume net port: {}", volume_net_port);
    info!("  Mount point: {}", mount_point);
    info!("  Collection: {}", collection);
    info!("  Replication: {}", replication);
    info!("  Container mode: {}", container);
    info!("  Data verification: {}", args.verify_data);

    // 创建挂载点目录
    let mount_path = std::path::Path::new(&mount_point);
    if !mount_path.exists() {
        std::fs::create_dir_all(mount_path).expect("Failed to create mount point directory");
        info!("Created mount point: {}", mount_point);
    } else if mount_path.is_file() {
        panic!("Mount point path is a file, not a directory");
    }

    // 容器模式：SIGTERM触发umount
    if container {
        install_signal_handlers(&mount_point);
    }

    // 创建tokio runtime
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let runtime_arc = Arc::new(runtime);

    let result = runtime_arc.block_on(async {
        let fuse_client = FuseApp::new(
            &master_addrs,
            &mount_point,
            &collection,
            &replication,
            master_net_port,
            volume_net_port,
            volume_addrs,
            filer_addr,
            filer_net_port,
            runtime_arc.clone(),
        )
        .await
        .expect("Failed to create FUSE client");

        info!("Mounting PowerFS at: {}", mount_point);

        fuse_client.run().await
    });

    if let Err(e) = &result {
        error!("FUSE session error: {}", e);
    }

    if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
        info!("Shutdown requested by signal, cleaning up...");
    }

    // 最终清理：确保挂载点被卸载
    let c_path = CString::new(mount_point.as_str()).unwrap();
    let ret = unsafe { libc::umount2(c_path.as_ptr(), libc::MNT_FORCE) };
    if ret == 0 {
        info!("Mount point unmounted on exit");
    } else {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINVAL) {
            warn!(
                "umount2 on exit returned: {} ({})",
                err,
                err.raw_os_error().unwrap_or(0)
            );
        }
    }

    info!("PowerFS FUSE Client stopped");
}
