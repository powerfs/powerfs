use clap::Parser;
use log::{error, info, warn};
use std::ffi::CString;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

static MOUNT_POINT_PATH: OnceLock<CString> = OnceLock::new();
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

#[derive(Parser, Debug)]
#[command(name = "powerfs-fuse")]
#[command(about = "PowerFS FUSE client - mount PowerFS as a filesystem")]
struct Args {
    /// Master server gRPC address (e.g. localhost:9334)
    #[arg(long, default_value = "localhost:9334")]
    master: String,

    /// Mount point path
    #[arg(long)]
    mount_point: String,

    /// Collection name
    #[arg(long, default_value = "default")]
    collection: String,

    /// Replication setting (e.g. "000" for no replicas)
    #[arg(long, default_value = "000")]
    replication: String,

    /// Number of FUSE worker threads
    #[arg(long, default_value = "8")]
    threads: usize,

    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Run as container PID 1: install SIGTERM/SIGINT handlers,
    /// unmount on exit so the kernel wakes up any blocked FUSE callers
    /// instead of leaving them in D (disk-sleep) state.
    #[arg(long)]
    container: bool,

    /// Log file path (if not specified, logs only to stdout)
    #[arg(long)]
    log_file: Option<String>,

    /// Max log file size in MB before rotation (default: 10MB)
    #[arg(long, default_value = "10")]
    log_max_size_mb: u64,

    /// Number of rotated log files to keep (default: 5)
    #[arg(long, default_value = "5")]
    log_max_files: usize,
}

/// Async-signal-safe handler: only calls write(2) and umount2(2).
/// Sets a flag so the main loop can exit gracefully after the FUSE session
/// unblocks (umount2 causes /dev/fuse reads to return ENODEV).
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

fn main() {
    let args = Args::parse();

    let log_level = if args.verbose { "debug" } else { "info" };

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

    if let Some(log_file) = &args.log_file {
        use std::fs::{self, File};
        use std::path::Path;

        let log_path = Path::new(log_file);
        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent).unwrap_or_else(|e| {
                eprintln!("Failed to create log directory: {}", e);
            });
        }

        let file = File::create(log_file).unwrap_or_else(|e| {
            eprintln!("Failed to create log file: {}", e);
            std::process::exit(1);
        });

        builder.target(env_logger::Target::Pipe(Box::new(file)));
        info!("Logging to file: {}", log_file);
    }

    builder.init();

    info!("PowerFS FUSE Client starting...");
    info!("  Master: {}", args.master);
    info!("  Mount point: {}", args.mount_point);
    info!("  Collection: {}", args.collection);
    info!("  Replication: {}", args.replication);
    info!("  Worker threads: {}", args.threads);
    info!("  Container mode: {}", args.container);

    // Create mount point directory if it doesn't exist
    let mount_path = std::path::Path::new(&args.mount_point);
    if !mount_path.exists() {
        std::fs::create_dir_all(mount_path).expect("Failed to create mount point directory");
        info!("Created mount point: {}", args.mount_point);
    } else if mount_path.is_file() {
        panic!("Mount point path is a file, not a directory");
    }

    // Container mode: SIGTERM (e.g. from `docker stop`) triggers umount so
    // processes blocked on FUSE reads/writes are woken instead of staying in
    // D state forever. The session's /dev/fuse read then returns ENODEV and
    // run() returns naturally — no signal-driven termination.
    if args.container {
        install_signal_handlers(&args.mount_point);
    }

    // Create tokio runtime
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    let result = runtime.block_on(async {
        let fuse_client = powerfs_fuse::FuserApp::new(
            &args.master,
            &args.mount_point,
            &args.collection,
            &args.replication,
            args.threads,
        )
        .await
        .expect("Failed to create FUSE client");

        info!("Mounting PowerFS at: {}", args.mount_point);

        fuse_client.run().await
    });

    if let Err(e) = &result {
        error!("FUSE session error: {}", e);
    }

    if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
        info!("Shutdown requested by signal, cleaning up...");
    }

    // Final cleanup: ensure the mount point is unmounted even if the session
    // exited abnormally. This is critical for container mode — without it,
    // blocked FUSE callers stay in D state.
    let c_path = CString::new(args.mount_point.as_str()).unwrap();
    let ret = unsafe { libc::umount2(c_path.as_ptr(), libc::MNT_FORCE) };
    if ret == 0 {
        info!("Mount point unmounted on exit");
    } else {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINVAL) {
            // EINVAL means "not mounted", which is fine
            warn!(
                "umount2 on exit returned: {} ({})",
                err,
                err.raw_os_error().unwrap_or(0)
            );
        }
    }

    info!("PowerFS FUSE Client stopped");
}
