use clap::Parser;
use log::{error, info, warn};
use std::ffi::CString;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;

use powerfs_common::config::{PowerFsConfig, ServiceType};
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

    /// 可选：CA 证书文件路径（PEM）。用于验证 master 返回的服务端证书；
    /// 当前保留用于未来 9334 TLS 升级，TLV 层暂时不强制读取。
    #[arg(long)]
    ca_crt: Option<String>,

    /// 客户端证书文件路径（PEM）。生产模式下 master 有 CA 时必填。
    /// 内容读取后通过 TLV FieldId::ClientCert(0xD4) 嵌入 RegisterClient 请求。
    #[arg(long)]
    client_crt: Option<String>,

    /// 客户端证书私钥文件路径（PEM）。保留用于 9334 端口 TLS 升级；
    /// 当前 TLV 0xD4 只发送公钥证书，私钥保存在本地即可。
    #[arg(long)]
    client_key: Option<String>,
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
    match PowerFsConfig::load_for_service(config_path, ServiceType::Fuse) {
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

    // Lease mode config (always "cap" after validation)
    let lease_mode = fuse_cfg.lease.mode.clone();
    let lease_duration_ms = fuse_cfg.lease.lease_duration_ms;
    let lease_renew_interval_ms = fuse_cfg.lease.renew_interval_ms;
    info!(
        "Lease mode: {} (duration={}ms, renew_interval={}ms)",
        lease_mode, lease_duration_ms, lease_renew_interval_ms
    );

    // 从配置获取filer地址列表（取第一个作为主地址，全部用于轮换重试）
    //
    // filer_addresses 现在可选：为空时由 facade 从 master 拓扑发现 filer 列表。
    // 这里仅提取配置中的兜底地址；topology 就绪后会覆盖。
    let (filer_addr, filer_addrs, filer_net_port) =
        if let Some(first_filer) = fuse_cfg.filer_addresses.first() {
            // 解析 host:port 或 仅host（主地址取 host 部分）
            let parts: Vec<&str> = first_filer.split(':').collect();
            let host = parts.first().unwrap_or(&"127.0.0.1").to_string();
            // 所有 Filer 地址取 host 部分，端口统一用 filer_net_port
            let all_hosts: Vec<String> = fuse_cfg
                .filer_addresses
                .iter()
                .map(|addr| {
                    let p: Vec<&str> = addr.split(':').collect();
                    p.first().unwrap_or(&"127.0.0.1").to_string()
                })
                .collect();
            (host, all_hosts, fuse_cfg.filer_net_port)
        } else {
            // filer_addresses 为空：由 facade 从 master 拓扑发现 filer 列表。
            // 这里给一个空字符串作为占位（facade 检测到空会用 topology 列表）。
            info!("fuse.filer_addresses is empty — will discover filers from master topology");
            (String::new(), Vec::new(), fuse_cfg.filer_net_port)
        };
    let force_mount = fuse_cfg.force_mount;
    let request_timeout_secs = if fuse_cfg.request_timeout_secs == 0 {
        10
    } else {
        fuse_cfg.request_timeout_secs
    };
    info!("Request timeout: {}s", request_timeout_secs);
    let admin_port = fuse_cfg.admin_port;
    if admin_port > 0 {
        info!(
            "Legacy fuse.admin_port={} present in config, but the FUSE \
             client no longer binds any HTTP endpoint. Stats are collected \
             via the Master KeepConnected heartbeat; use \
             `powerfs-cli -m <master> fuse-stats`.",
            admin_port
        );
    }

    let verbose = args.verbose || fuse_cfg.verbose;
    let container = args.container || fuse_cfg.container;
    let log_level = if verbose { "debug" } else { "info" };
    let _log_file = args.log_file.clone().or(fuse_cfg.log_file);

    // 验证配置完整性
    if master_addrs.is_empty() {
        eprintln!("ERROR: fuse.master_addresses must not be empty");
        process::exit(1);
    }
    // volume_addrs 可选：为空时由 FuseClientFacade 从 master 拓扑动态发现
    // (master GetTopology 下发 volumes[].addr)。仅当 force_mount=true 且拓扑
    // 也为空时，才在 facade 层报错（无兜底地址）。
    if volume_addrs.is_empty() {
        info!("fuse.volume_addresses is empty — will discover volumes from master topology");
    }
    if master_net_port == 0 {
        eprintln!("ERROR: fuse.master_net_port must be set");
        process::exit(1);
    }
    if volume_net_port == 0 {
        eprintln!("ERROR: fuse.volume_net_port must be set");
        process::exit(1);
    }

    // 配置日志：使用 dynamic_log（支持运行时动态调整 + target 过滤 + 子系统开关）
    // master 通过 GetDebugConfig 下发配置，fuse 每 2s 轮询并本地应用
    powerfs_common::dynamic_log::init(log_level, None);

    powerfs_common::BuildInfo::current(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .log_startup();

    info!("PowerFS FUSE Client starting (powerfs-net)...");
    info!("  Config file: {}", args.config);
    info!("  Masters: {}", master_addrs.join(", "));
    info!("  Master net port: {}", master_net_port);
    info!("  Filer: {}:{}", filer_addr, filer_net_port);
    info!("  Filer addresses (rotation): {:?}", filer_addrs);
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
    //
    // NOTE: Runtime::new() defaults to worker_threads = num_cpus() (host value,
    // can be 16/32/64 on the host). However, cgroup-pids limits or ulimit
    // inside Docker often cap our threads at a much smaller number (~7-12).
    // When that happens:
    //   1. `main` thread permanently parks in runtime.block_on(FuseApp::new.await)
    //      → this keeps one OS thread busy for the entire process lifetime.
    //   2. FUSE callbacks (write/release/create/open/unlink...) are handled by
    //      6 fuse_backend_rs workers running on OS threads — not our tokio pool.
    //      Each callback uses SyncFuseClientFacade::block_on(), which spawns a
    //      task onto the tokio runtime and waits on an mpsc channel.
    //   3. If tokio has fewer worker threads than concurrent block_on() callers,
    //      all workers get parked on mpsc::recv() waiting for responses, while
    //      the async network futures (recv_loop, send_task, lockify sync tasks
    //      that produce those responses) never get scheduled → deadlock, all
    //      FUSE requests hang forever ("context canceled" inside tests).
    //
    // Fix: explicitly size the runtime. Use 8 workers (well above the 1 main
    // thread + 6 FUSE workers simultaneous-block-on worst case, leaving head
    // room for network tasks) + generous blocking pool budget. We also read
    // TOKIO_WORKER_THREADS so it can be overridden per-deploy.
    let default_workers: usize = 8;
    let worker_threads = std::env::var("TOKIO_WORKER_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default_workers)
        .clamp(2, 64);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .max_blocking_threads(32)
        .enable_all()
        .thread_name("powerfs-tokio")
        .build()
        .expect("Failed to create tokio runtime");
    let runtime_arc = Arc::new(runtime);

    // ---- 客户端证书 ----
    //
    // 优先级: CLI --client-crt > fuse.toml [fuse] client_crt > None
    // 生产模式 master 有 CA manager 时，必须携带经 `powerfs-cli cert sign-client`
    // 签发的证书，并且证书的 san_ips 必须包含当前节点 IP，mount_dirs 必须包含
    // 本 mount_point。读取失败直接 abort（不要默默降级到 dev-mode 行为）。
    let client_crt_path: Option<&str> = args
        .client_crt
        .as_deref()
        .or(fuse_cfg.client_crt.as_deref());
    let ca_crt_path: Option<&str> = args
        .ca_crt
        .as_deref()
        .or(fuse_cfg.ca_crt.as_deref());
    let client_key_path: Option<&str> = args
        .client_key
        .as_deref()
        .or(fuse_cfg.client_key.as_deref());

    let client_cert_pem = match client_crt_path {
        Some(path) => match std::fs::read_to_string(path) {
            Ok(pem) => {
                info!(
                    "Loaded client certificate from {} ({} bytes)",
                    path,
                    pem.len()
                );
                Some(pem)
            }
            Err(e) => {
                eprintln!("ERROR: failed to read client_crt {}: {}", path, e);
                process::exit(1);
            }
        },
        None => {
            warn!("client_crt not provided (checked CLI --client-crt and config) — \
                  if master has CA manager enabled, RegisterClient will be denied");
            None
        }
    };
    // ca_crt / client_key 保留用于未来 9334 端口 TLS 升级；这里只校验参数存在性。
    if let Some(path) = ca_crt_path {
        if !std::path::Path::new(path).exists() {
            eprintln!("ERROR: ca_crt file not found: {}", path);
            process::exit(1);
        }
    }
    if let Some(path) = client_key_path {
        if !std::path::Path::new(path).exists() {
            eprintln!("ERROR: client_key file not found: {}", path);
            process::exit(1);
        }
    }

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
            filer_addrs,
            filer_net_port,
            &lease_mode,
            lease_duration_ms,
            lease_renew_interval_ms,
            force_mount,
            request_timeout_secs,
            admin_port,
            client_cert_pem,
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
