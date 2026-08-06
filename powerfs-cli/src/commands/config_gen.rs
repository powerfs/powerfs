use clap::Args;
use powerfs_common::config::{
    FilerConfig, FuseConfig, GlobalConfig, MasterConfig, MonitorConfig, PowerFsConfig, S3Config,
    VolumeConfig,
};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::path::Path;

// ---------------------------------------------------------------------------
// Topology file structures (TOML)
// ---------------------------------------------------------------------------

/// Topology file root.
///
/// Minimal example:
/// ```toml
/// [masters]
/// hosts = ["10.0.0.11", "10.0.0.12", "10.0.0.13"]
///
/// [volumes]
/// hosts = ["10.0.0.21", "10.0.0.22"]
///
/// [filers]
/// hosts = ["10.0.0.31", "10.0.0.32", "10.0.0.33"]
///
/// [redis]
/// host = "10.0.0.11"
/// ```
///
/// Optional overrides (`[ports]`, `[storage]`, `[fuse]`, `[s3]`, `[misc]`)
/// are also accepted; CLI flags take precedence over file values.
#[derive(Debug, Deserialize, Default)]
struct TopologyConfig {
    masters: Option<HostList>,
    volumes: Option<HostList>,
    filers: Option<HostList>,
    redis: Option<RedisEntry>,
    ports: Option<PortsConfig>,
    storage: Option<StorageConfig>,
    fuse: Option<FuseSettings>,
    s3: Option<S3Settings>,
    misc: Option<MiscSettings>,
}

#[derive(Debug, Deserialize)]
struct HostList {
    hosts: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RedisEntry {
    host: String,
}

#[derive(Debug, Deserialize, Default)]
struct PortsConfig {
    master: Option<u16>,
    master_net: Option<u16>,
    volume_grpc: Option<u16>,
    volume_http: Option<u16>,
    volume_net: Option<u16>,
    filer: Option<u16>,
    filer_grpc: Option<u16>,
    filer_net: Option<u16>,
    s3: Option<u16>,
    monitor: Option<u16>,
}

#[derive(Debug, Deserialize, Default)]
struct StorageConfig {
    data_dir: Option<String>,
    max_volume_size: Option<u64>,
    initial_volume_count: Option<u32>,
    shard_count: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
struct FuseSettings {
    mount_point: Option<String>,
    threads: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct S3Settings {
    access_key: Option<String>,
    secret_key: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct MiscSettings {
    collection: Option<String>,
    replication: Option<String>,
    output: Option<String>,
}

// ---------------------------------------------------------------------------
// Built-in defaults
// ---------------------------------------------------------------------------

struct Defaults;

impl Defaults {
    const OUTPUT: &'static str = "./configs";
    const DATA_DIR: &'static str = "/data";
    const MOUNT_POINT: &'static str = "/mnt/powerfs";
    const COLLECTION: &'static str = "default";
    const REPLICATION: &'static str = "000";
    const S3_ACCESS_KEY: &'static str = "powerfs";
    const S3_SECRET_KEY: &'static str = "powerfs123";
    const MASTER_PORT: u16 = 9333;
    const MASTER_NET_PORT: u16 = 9334;
    const VOLUME_GRPC_PORT: u16 = 8080;
    const VOLUME_HTTP_PORT: u16 = 8091;
    const VOLUME_NET_PORT: u16 = 8901;
    const FILER_PORT: u16 = 8888;
    const FILER_GRPC_PORT: u16 = 8889;
    const FILER_NET_PORT: u16 = 9334;
    const S3_PORT: u16 = 9000;
    const MONITOR_PORT: u16 = 8081;
    const MAX_VOLUME_SIZE: u64 = 107_374_182_400; // 100 GB
    const INITIAL_VOLUME_COUNT: u32 = 4;
    const SHARD_COUNT: u32 = 2;
    const FUSE_THREADS: usize = 8;
}

// ---------------------------------------------------------------------------
// Resolved configuration (after merging topology file + CLI + defaults)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ResolvedConfig {
    masters: Vec<String>,
    volumes: Vec<String>,
    filers: Vec<String>,
    redis: String,
    output: String,
    data_dir: String,
    master_port: u16,
    master_net_port: u16,
    volume_grpc_port: u16,
    volume_http_port: u16,
    volume_net_port: u16,
    filer_port: u16,
    filer_grpc_port: u16,
    filer_net_port: u16,
    s3_port: u16,
    monitor_port: u16,
    s3_access_key: String,
    s3_secret_key: String,
    max_volume_size: u64,
    initial_volume_count: u32,
    shard_count: u32,
    mount_point: String,
    fuse_threads: usize,
    collection: String,
    replication: String,
}

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

/// Cluster topology specification for config generation.
///
/// Values can be provided via `--topology <file>` (TOML) or individual CLI
/// flags. CLI flags take precedence over topology file values, which in turn
/// take precedence over built-in defaults.
#[derive(Args, Debug)]
pub struct ConfigGenArgs {
    /// Path to a TOML topology file describing the cluster layout.
    /// Avoids repeating --masters/--volumes/--filers/--redis on every invocation.
    #[arg(long)]
    pub topology: Option<String>,

    /// Master node IPs, comma-separated (e.g., "10.0.0.11,10.0.0.12,10.0.0.13").
    /// Overrides [masters] in the topology file.
    #[arg(long, value_delimiter = ',')]
    pub masters: Option<Vec<String>>,

    /// Volume node IPs, comma-separated. Overrides [volumes] in the topology file.
    #[arg(long, value_delimiter = ',')]
    pub volumes: Option<Vec<String>>,

    /// Filer node IPs, comma-separated. Overrides [filers] in the topology file.
    #[arg(long, value_delimiter = ',')]
    pub filers: Option<Vec<String>>,

    /// Redis IP address. Overrides [redis] in the topology file.
    #[arg(long)]
    pub redis: Option<String>,

    /// Allow the same IP to appear in multiple roles (e.g., master + volume on
    /// the same host). By default this is rejected to catch misconfiguration.
    #[arg(long)]
    pub allow_collocated: bool,

    // --- Optional overrides (CLI > topology file > built-in default) -------
    /// Output directory for generated config files.
    #[arg(long)]
    pub output: Option<String>,

    /// Data directory prefix.
    #[arg(long)]
    pub data_dir: Option<String>,

    #[arg(long)]
    pub master_port: Option<u16>,
    #[arg(long)]
    pub master_net_port: Option<u16>,
    #[arg(long)]
    pub volume_grpc_port: Option<u16>,
    #[arg(long)]
    pub volume_http_port: Option<u16>,
    #[arg(long)]
    pub volume_net_port: Option<u16>,
    #[arg(long)]
    pub filer_port: Option<u16>,
    #[arg(long)]
    pub filer_grpc_port: Option<u16>,
    #[arg(long)]
    pub filer_net_port: Option<u16>,
    #[arg(long)]
    pub s3_port: Option<u16>,
    #[arg(long)]
    pub monitor_port: Option<u16>,

    #[arg(long)]
    pub s3_access_key: Option<String>,
    #[arg(long)]
    pub s3_secret_key: Option<String>,

    /// Max volume size in bytes.
    #[arg(long)]
    pub max_volume_size: Option<u64>,

    /// Initial volume count per volume server.
    #[arg(long)]
    pub initial_volume_count: Option<u32>,

    /// Filer shard count.
    #[arg(long)]
    pub shard_count: Option<u32>,

    /// FUSE mount point.
    #[arg(long)]
    pub mount_point: Option<String>,

    /// FUSE worker thread count.
    #[arg(long)]
    pub fuse_threads: Option<usize>,

    /// Collection name.
    #[arg(long)]
    pub collection: Option<String>,

    /// Replication placement (e.g., "000" for no replication).
    #[arg(long)]
    pub replication: Option<String>,
}

// ---------------------------------------------------------------------------
// Merge logic: CLI > topology file > built-in defaults
// ---------------------------------------------------------------------------

fn resolve(args: &ConfigGenArgs) -> Result<ResolvedConfig, String> {
    // Load topology file if specified.
    let topo: TopologyConfig = match &args.topology {
        Some(path) => {
            let content = std::fs::read_to_string(path)
                .map_err(|e| format!("Failed to read topology file '{}': {}", path, e))?;
            toml::from_str(&content)
                .map_err(|e| format!("Failed to parse topology file '{}': {}", path, e))?
        }
        None => TopologyConfig::default(),
    };

    // Helper: CLI > file > default
    fn opt_str(cli: &Option<String>, file: Option<String>, default: &str) -> String {
        cli.clone().or(file).unwrap_or_else(|| default.to_string())
    }
    fn opt_u16(cli: Option<u16>, file: Option<u16>, default: u16) -> u16 {
        cli.or(file).unwrap_or(default)
    }

    let masters = args
        .masters
        .clone()
        .or_else(|| topo.masters.as_ref().map(|m| m.hosts.clone()))
        .ok_or_else(|| {
            "masters is required: provide --masters or [masters] in --topology file".to_string()
        })?;
    let volumes = args
        .volumes
        .clone()
        .or_else(|| topo.volumes.as_ref().map(|v| v.hosts.clone()))
        .ok_or_else(|| {
            "volumes is required: provide --volumes or [volumes] in --topology file".to_string()
        })?;
    let filers = args
        .filers
        .clone()
        .or_else(|| topo.filers.as_ref().map(|f| f.hosts.clone()))
        .ok_or_else(|| {
            "filers is required: provide --filers or [filers] in --topology file".to_string()
        })?;
    let redis = args
        .redis
        .clone()
        .or_else(|| topo.redis.as_ref().map(|r| r.host.clone()))
        .ok_or_else(|| {
            "redis is required: provide --redis or [redis] in --topology file".to_string()
        })?;

    let ports = topo.ports.unwrap_or_default();
    let storage = topo.storage.unwrap_or_default();
    let fuse = topo.fuse.unwrap_or_default();
    let s3 = topo.s3.unwrap_or_default();
    let misc = topo.misc.unwrap_or_default();

    let resolved = ResolvedConfig {
        masters,
        volumes,
        filers,
        redis,
        output: opt_str(&args.output, misc.output, Defaults::OUTPUT),
        data_dir: opt_str(&args.data_dir, storage.data_dir, Defaults::DATA_DIR),
        master_port: opt_u16(args.master_port, ports.master, Defaults::MASTER_PORT),
        master_net_port: opt_u16(
            args.master_net_port,
            ports.master_net,
            Defaults::MASTER_NET_PORT,
        ),
        volume_grpc_port: opt_u16(
            args.volume_grpc_port,
            ports.volume_grpc,
            Defaults::VOLUME_GRPC_PORT,
        ),
        volume_http_port: opt_u16(
            args.volume_http_port,
            ports.volume_http,
            Defaults::VOLUME_HTTP_PORT,
        ),
        volume_net_port: opt_u16(
            args.volume_net_port,
            ports.volume_net,
            Defaults::VOLUME_NET_PORT,
        ),
        filer_port: opt_u16(args.filer_port, ports.filer, Defaults::FILER_PORT),
        filer_grpc_port: opt_u16(
            args.filer_grpc_port,
            ports.filer_grpc,
            Defaults::FILER_GRPC_PORT,
        ),
        filer_net_port: opt_u16(
            args.filer_net_port,
            ports.filer_net,
            Defaults::FILER_NET_PORT,
        ),
        s3_port: opt_u16(args.s3_port, ports.s3, Defaults::S3_PORT),
        monitor_port: opt_u16(args.monitor_port, ports.monitor, Defaults::MONITOR_PORT),
        s3_access_key: opt_str(&args.s3_access_key, s3.access_key, Defaults::S3_ACCESS_KEY),
        s3_secret_key: opt_str(&args.s3_secret_key, s3.secret_key, Defaults::S3_SECRET_KEY),
        max_volume_size: args
            .max_volume_size
            .or(storage.max_volume_size)
            .unwrap_or(Defaults::MAX_VOLUME_SIZE),
        initial_volume_count: args
            .initial_volume_count
            .or(storage.initial_volume_count)
            .unwrap_or(Defaults::INITIAL_VOLUME_COUNT),
        shard_count: args
            .shard_count
            .or(storage.shard_count)
            .unwrap_or(Defaults::SHARD_COUNT),
        mount_point: opt_str(&args.mount_point, fuse.mount_point, Defaults::MOUNT_POINT),
        fuse_threads: args
            .fuse_threads
            .or(fuse.threads)
            .unwrap_or(Defaults::FUSE_THREADS),
        collection: opt_str(&args.collection, misc.collection, Defaults::COLLECTION),
        replication: opt_str(&args.replication, misc.replication, Defaults::REPLICATION),
    };

    Ok(resolved)
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validate the resolved cluster topology.
///
/// Hard failures (return Err):
///   - masters < 3          (Raft requires 3+ nodes for production)
///   - volumes < 1
///   - filers < 1
///   - duplicate IP within a single role
///   - same IP across roles (unless --allow-collocated)
///   - invalid IPv4 address
///   - port out of range (1..=65535) or port collision between services on the same host
///   - redis host empty or invalid IP
///
/// Soft warnings (eprintln, non-fatal):
///   - even number of masters (Raft prefers odd counts)
fn validate(cfg: &ResolvedConfig, allow_collocated: bool) -> Result<(), String> {
    // 1. Node count requirements
    if cfg.masters.len() < 3 {
        return Err(format!(
            "masters must have at least 3 nodes for Raft production (got {}); \
             single-node mode is for development only",
            cfg.masters.len()
        ));
    }
    if cfg.volumes.is_empty() {
        return Err("volumes must have at least 1 node".to_string());
    }
    if cfg.filers.is_empty() {
        return Err("filers must have at least 1 node".to_string());
    }

    // 2. Soft warning: prefer odd master count
    if cfg.masters.len() % 2 == 0 {
        eprintln!(
            "WARNING: {} masters is even; Raft prefers odd counts (3, 5, 7) \
             to avoid split-brain ties. Consider adding or removing one master.",
            cfg.masters.len()
        );
    }

    // 3. IPv4 format validation
    for ip in &cfg.masters {
        validate_ip(ip, "masters")?;
    }
    for ip in &cfg.volumes {
        validate_ip(ip, "volumes")?;
    }
    for ip in &cfg.filers {
        validate_ip(ip, "filers")?;
    }
    validate_ip(&cfg.redis, "redis")?;

    // 4. Duplicate within each role
    check_intra_role_dup(&cfg.masters, "masters")?;
    check_intra_role_dup(&cfg.volumes, "volumes")?;
    check_intra_role_dup(&cfg.filers, "filers")?;

    // 5. Cross-role IP overlap
    if !allow_collocated {
        let mut seen: HashMap<&str, &str> = HashMap::new();
        for ip in &cfg.masters {
            if let Some(prev_role) = seen.get(ip.as_str()) {
                return Err(format!(
                    "IP '{}' appears in both '{}' and 'masters'; use --allow-collocated \
                     if this is intentional (e.g., master + volume co-located on the same host)",
                    ip, prev_role
                ));
            }
            seen.insert(ip.as_str(), "masters");
        }
        for ip in &cfg.volumes {
            if let Some(prev_role) = seen.get(ip.as_str()) {
                return Err(format!(
                    "IP '{}' appears in both '{}' and 'volumes'; use --allow-collocated \
                     if this is intentional",
                    ip, prev_role
                ));
            }
            seen.insert(ip.as_str(), "volumes");
        }
        for ip in &cfg.filers {
            if let Some(prev_role) = seen.get(ip.as_str()) {
                return Err(format!(
                    "IP '{}' appears in both '{}' and 'filers'; use --allow-collocated \
                     if this is intentional",
                    ip, prev_role
                ));
            }
            seen.insert(ip.as_str(), "filers");
        }
    } else {
        eprintln!("WARNING: --allow-collocated set; cross-role IP overlap is permitted.");
    }

    // 6. Port range validation
    let all_ports = [
        ("master_port", cfg.master_port),
        ("master_net_port", cfg.master_net_port),
        ("volume_grpc_port", cfg.volume_grpc_port),
        ("volume_http_port", cfg.volume_http_port),
        ("volume_net_port", cfg.volume_net_port),
        ("filer_port", cfg.filer_port),
        ("filer_grpc_port", cfg.filer_grpc_port),
        ("filer_net_port", cfg.filer_net_port),
        ("s3_port", cfg.s3_port),
        ("monitor_port", cfg.monitor_port),
    ];
    for (name, port) in &all_ports {
        if *port == 0 {
            return Err(format!("{} must be > 0", name));
        }
    }

    // 7. Port collision detection (same host, different services must use different ports)
    //    We check that ports meant for the *same node type* don't conflict.
    if cfg.master_port == cfg.master_net_port {
        return Err(format!(
            "master_port ({}) and master_net_port ({}) must differ",
            cfg.master_port, cfg.master_net_port
        ));
    }
    if cfg.volume_http_port == cfg.volume_net_port {
        return Err(format!(
            "volume_http_port ({}) and volume_net_port ({}) must differ",
            cfg.volume_http_port, cfg.volume_net_port
        ));
    }
    if cfg.filer_port == cfg.filer_net_port {
        return Err(format!(
            "filer_port ({}) and filer_net_port ({}) must differ",
            cfg.filer_port, cfg.filer_net_port
        ));
    }

    Ok(())
}

fn validate_ip(ip: &str, role: &str) -> Result<(), String> {
    ip.parse::<Ipv4Addr>().map_err(|_| {
        format!(
            "invalid IPv4 address '{}' in role '{}'; expected format like 10.0.0.11",
            ip, role
        )
    })?;
    Ok(())
}

fn check_intra_role_dup(hosts: &[String], role: &str) -> Result<(), String> {
    let mut seen = HashSet::new();
    for ip in hosts {
        if !seen.insert(ip.as_str()) {
            return Err(format!(
                "duplicate IP '{}' within role '{}'; each node must have a unique address",
                ip, role
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Config generation (unchanged logic, now driven by ResolvedConfig)
// ---------------------------------------------------------------------------

pub fn config_gen(args: &ConfigGenArgs) -> Result<(), String> {
    let cfg = resolve(args)?;
    validate(&cfg, args.allow_collocated)?;

    let output_dir = Path::new(&cfg.output);
    std::fs::create_dir_all(output_dir)
        .map_err(|e| format!("Failed to create output directory: {}", e))?;

    let master_peers: Vec<String> = cfg
        .masters
        .iter()
        .map(|ip| format!("{}:{}", ip, cfg.master_port))
        .collect();

    let filer_peers: Vec<String> = cfg
        .filers
        .iter()
        .map(|ip| format!("{}:{}", ip, cfg.filer_grpc_port))
        .collect();

    let master_addrs: Vec<String> = cfg
        .masters
        .iter()
        .map(|ip| format!("{}:{}", ip, cfg.master_port))
        .collect();

    // Generate master configs
    for (i, ip) in cfg.masters.iter().enumerate() {
        let config = build_config(
            &cfg,
            ip,
            (i + 1) as u64,
            &master_peers,
            &filer_peers,
            &master_addrs,
        );
        let filename = format!("master-{}.toml", i + 1);
        save_config(&config, output_dir, &filename)?;
        println!("Generated: {}/{}", cfg.output, filename);
    }

    // Generate volume configs
    for (i, ip) in cfg.volumes.iter().enumerate() {
        let config = build_volume_config(
            &cfg,
            ip,
            &format!("volume-server-{}", i + 1),
            &master_addrs,
            &filer_peers,
        );
        let filename = format!("volume-{}.toml", i + 1);
        save_config(&config, output_dir, &filename)?;
        println!("Generated: {}/{}", cfg.output, filename);
    }

    // Generate filer configs
    for (i, ip) in cfg.filers.iter().enumerate() {
        let config = build_filer_config(&cfg, ip, (i + 1) as u64, &master_addrs, &filer_peers);
        let filename = format!("filer-{}.toml", i + 1);
        save_config(&config, output_dir, &filename)?;
        println!("Generated: {}/{}", cfg.output, filename);
    }

    // Generate FUSE client config
    let fuse_config = build_fuse_config(&cfg, &master_addrs, &filer_peers);
    save_config(&fuse_config, output_dir, "fuse.toml")?;
    println!("Generated: {}/fuse.toml", cfg.output);

    // Generate monitor config
    let monitor_config = build_monitor_config(&cfg, &master_addrs);
    save_config(&monitor_config, output_dir, "monitor.toml")?;
    println!("Generated: {}/monitor.toml", cfg.output);

    println!(
        "\n{} config files generated in {}",
        cfg.masters.len() + cfg.volumes.len() + cfg.filers.len() + 2,
        cfg.output
    );
    println!("\nNext steps:");
    println!("  1. Copy config files to each node");
    println!("  2. Run powerfs-init --config filer-N.toml on each filer node");
    println!("  3. Start services: powerfs-master --config master-N.toml");
    Ok(())
}

fn build_config(
    cfg: &ResolvedConfig,
    ip: &str,
    raft_id: u64,
    master_peers: &[String],
    filer_peers: &[String],
    master_addrs: &[String],
) -> PowerFsConfig {
    PowerFsConfig {
        global: GlobalConfig {
            log_level: "info".to_string(),
            log_file: None,
            redis_url: format!("redis://{}:6379", cfg.redis),
        },
        master: MasterConfig {
            port: cfg.master_port,
            net_port: cfg.master_net_port,
            dir: format!("{}/master", cfg.data_dir),
            raft_dir: None,
            meta_dir: None,
            ip: Some("0.0.0.0".to_string()),
            advertise_addr: Some(format!("{}:{}", ip, cfg.master_port)),
            raft_id,
            peers: master_peers.to_vec(),
        },
        volume: VolumeConfig {
            grpc_port: cfg.volume_grpc_port,
            http_port: cfg.volume_http_port,
            net_port: cfg.volume_net_port,
            data_dir: format!("{}/volume", cfg.data_dir),
            master_addresses: master_addrs.to_vec(),
            master_net_port: cfg.master_net_port,
            node_id: "volume-server-1".to_string(),
            max_volume_size: cfg.max_volume_size,
            initial_volume_count: cfg.initial_volume_count,
            device_capacity: None,
            advertise_addr: Some(ip.to_string()),
            lease_enabled: true,
        },
        filer: FilerConfig {
            port: cfg.filer_port,
            grpc_port: cfg.filer_grpc_port,
            net_port: cfg.filer_net_port,
            master_addresses: master_addrs.to_vec(),
            master_net_port: cfg.master_net_port,
            ip: Some("0.0.0.0".to_string()),
            data_dir: format!("{}/filer", cfg.data_dir),
            shard_count: cfg.shard_count,
            raft_id,
            raft_peers: filer_peers.to_vec(),
            advertise_addr: Some(ip.to_string()),
            crdt_maintenance_interval_secs: None,
            gc_interval_secs: None,
            gc_grace_period_secs: None,
        },
        s3: S3Config {
            port: cfg.s3_port,
            master_address: master_addrs.first().cloned().unwrap_or_default(),
            master_endpoints: master_addrs.to_vec(),
            ip: Some("0.0.0.0".to_string()),
            dir: format!("{}/s3", cfg.data_dir),
            access_key: cfg.s3_access_key.clone(),
            secret_key: cfg.s3_secret_key.clone(),
        },
        fuse: FuseConfig {
            mount_point: cfg.mount_point.clone(),
            master_addresses: cfg.masters.clone(),
            filer_addresses: cfg.filers.clone(),
            volume_addresses: cfg.volumes.clone(),
            master_net_port: cfg.master_net_port,
            volume_net_port: cfg.volume_net_port,
            filer_net_port: cfg.filer_net_port,
            collection: cfg.collection.clone(),
            replication: cfg.replication.clone(),
            threads: cfg.fuse_threads,
            verbose: false,
            container: false,
            log_file: None,
            lease: powerfs_common::config::LeaseConfig::default(),
        },
        monitor: MonitorConfig {
            addr: format!("0.0.0.0:{}", cfg.monitor_port),
            redis_url: format!("redis://{}:6379", cfg.redis),
            s3_endpoint: format!(
                "http://{}:{}",
                cfg.filers.first().unwrap_or(&"127.0.0.1".to_string()),
                cfg.s3_port
            ),
            s3_backend_endpoint: format!(
                "http://{}:{}",
                cfg.filers.first().unwrap_or(&"127.0.0.1".to_string()),
                cfg.s3_port
            ),
            master_endpoint: format!(
                "http://{}",
                master_addrs
                    .first()
                    .unwrap_or(&"127.0.0.1:9333".to_string())
            ),
            master_endpoints: master_addrs
                .iter()
                .map(|a| format!("http://{}", a))
                .collect(),
        },
    }
}

fn build_volume_config(
    cfg: &ResolvedConfig,
    ip: &str,
    node_id: &str,
    master_addrs: &[String],
    filer_peers: &[String],
) -> PowerFsConfig {
    let mut config = build_config(cfg, ip, 1, master_addrs, filer_peers, master_addrs);
    config.volume.node_id = node_id.to_string();
    config.volume.advertise_addr = Some(ip.to_string());
    config.volume.data_dir = cfg.data_dir.clone();
    config
}

fn build_filer_config(
    cfg: &ResolvedConfig,
    ip: &str,
    raft_id: u64,
    master_addrs: &[String],
    filer_peers: &[String],
) -> PowerFsConfig {
    let mut config = build_config(cfg, ip, raft_id, master_addrs, filer_peers, master_addrs);
    config.filer.raft_id = raft_id;
    config.filer.raft_peers = filer_peers.to_vec();
    config.filer.ip = Some("0.0.0.0".to_string());
    config
}

fn build_fuse_config(
    cfg: &ResolvedConfig,
    master_addrs: &[String],
    filer_peers: &[String],
) -> PowerFsConfig {
    let dummy_ip = cfg.masters.first().unwrap();
    let mut config = build_config(cfg, dummy_ip, 1, master_addrs, filer_peers, master_addrs);
    config.fuse.container = false;
    config
}

fn build_monitor_config(cfg: &ResolvedConfig, master_addrs: &[String]) -> PowerFsConfig {
    let dummy_ip = cfg.masters.first().unwrap();
    let filer_peers: Vec<String> = cfg
        .filers
        .iter()
        .map(|ip| format!("{}:{}", ip, cfg.filer_grpc_port))
        .collect();
    let mut config = build_config(cfg, dummy_ip, 1, master_addrs, &filer_peers, master_addrs);
    config.monitor.addr = format!("0.0.0.0:{}", cfg.monitor_port);
    config
}

fn save_config(config: &PowerFsConfig, output_dir: &Path, filename: &str) -> Result<(), String> {
    let toml_content = config
        .to_toml()
        .map_err(|e| format!("Failed to serialize config: {}", e))?;
    let path = output_dir.join(filename);
    std::fs::write(&path, toml_content)
        .map_err(|e| format!("Failed to write {}: {}", path.display(), e))?;
    Ok(())
}
