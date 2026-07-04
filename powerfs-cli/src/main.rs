use clap::{Parser, Subcommand};
use log::info;

mod client;
mod commands;
mod kv_client;
mod volume_client;

use commands::{
    AssignArgs, ClusterAddArgs, ClusterRemoveArgs, ClusterStatusArgs, ClusterTransferArgs,
    GrowArgs, HeartbeatArgs, KvArgs, LookupArgs, MountArgs, ReadArgs, StatusArgs, VolumeListArgs,
    WriteArgs,
};

/// PowerFS CLI tool for testing and administration
#[derive(Parser)]
#[command(name = "powerfs-cli")]
#[command(author = "PowerFS Team")]
#[command(version = "0.1.0")]
#[command(about = "CLI tool for PowerFS testing and administration", long_about = None)]
struct Cli {
    /// Master server address (e.g., localhost:9333)
    #[arg(short, long, global = true, default_value = "localhost:9333")]
    master: String,

    /// Verbosity level (-v, -vv, -vvv)
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show master status (leader, nodes, volumes)
    Status(StatusArgs),

    /// Assign a new file ID (FID)
    Assign(AssignArgs),

    /// Lookup volume location by volume ID or FID
    Lookup(LookupArgs),

    /// List all volumes and nodes
    VolumeList(VolumeListArgs),

    /// Send heartbeat to master (simulate volume server)
    Heartbeat(HeartbeatArgs),

    /// Request volume growth
    Grow(GrowArgs),

    /// Write data to volume server
    Write(WriteArgs),

    /// Read data from volume server
    Read(ReadArgs),

    /// Add node to cluster
    ClusterAdd(ClusterAddArgs),

    /// Remove node from cluster
    ClusterRemove(ClusterRemoveArgs),

    /// Show detailed cluster status
    ClusterStatus(ClusterStatusArgs),

    /// Transfer leadership to another node
    ClusterTransfer(ClusterTransferArgs),

    /// Mount PowerFS as a FUSE filesystem
    Mount(MountArgs),

    /// KV Cache operations (session/block/list/stats)
    Kv(KvArgs),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = match cli.verbose {
        0 => log::LevelFilter::Warn,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };
    env_logger::Builder::new().filter_level(log_level).init();

    info!("Connecting to master at: {}", cli.master);

    // Create client
    let client = client::MasterClient::new(&cli.master);

    // Execute command
    let result = match cli.command {
        Commands::Status(args) => commands::status(client, args).await,
        Commands::Assign(args) => commands::assign(client, args).await,
        Commands::Lookup(args) => commands::lookup(client, args).await,
        Commands::VolumeList(args) => commands::volume_list(client, args).await,
        Commands::Heartbeat(args) => commands::heartbeat(client, args).await,
        Commands::Grow(args) => commands::grow(client, args).await,
        Commands::Write(args) => commands::write(args).await,
        Commands::Read(args) => commands::read(args).await,
        Commands::ClusterAdd(args) => commands::cluster_add(client, args).await,
        Commands::ClusterRemove(args) => commands::cluster_remove(client, args).await,
        Commands::ClusterStatus(args) => commands::cluster_status(client, args).await,
        Commands::ClusterTransfer(args) => commands::cluster_transfer(client, args).await,
        Commands::Mount(args) => commands::mount(args),
        Commands::Kv(args) => {
            let kv_client = kv_client::KvCacheClient::new(&cli.master);
            commands::kv(kv_client, args).await
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
