use clap::Parser;
use log::info;

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
}

fn main() {
    let args = Args::parse();

    let log_level = if args.verbose { "debug" } else { "info" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();

    info!("PowerFS FUSE Client starting...");
    info!("  Master: {}", args.master);
    info!("  Mount point: {}", args.mount_point);
    info!("  Collection: {}", args.collection);
    info!("  Replication: {}", args.replication);
    info!("  Worker threads: {}", args.threads);

    // Create mount point directory if it doesn't exist
    let mount_path = std::path::Path::new(&args.mount_point);
    if !mount_path.exists() {
        std::fs::create_dir_all(mount_path).expect("Failed to create mount point directory");
        info!("Created mount point: {}", args.mount_point);
    } else if mount_path.is_file() {
        panic!("Mount point path is a file, not a directory");
    }

    // Create tokio runtime
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    runtime.block_on(async {
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

        fuse_client.run().await.expect("FUSE session error");
    });
}
