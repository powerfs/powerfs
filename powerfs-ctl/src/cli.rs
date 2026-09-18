//! clap command surface for powerfs-ctl.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "powerfs-ctl",
    version,
    about = "PowerFS deployment and lifecycle control plane",
    long_about = "Declarative deployment for PowerFS. Edit cluster.toml, then \
                  render configs, issue certs, and reconcile the cluster."
)]
pub struct Cli {
    /// State directory (overrides POWERFS_HOME; default: ./.powerfs)
    #[arg(long, env = "POWERFS_HOME", global = true)]
    pub home: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// One-shot: init + render + cert + up + health gate
    Bootstrap {
        /// Deployment profile
        #[arg(long, value_enum, default_value = "ha")]
        profile: ProfileArg,
        /// Subnet CIDR
        #[arg(long, default_value = "172.30.0.0/16")]
        network: String,
        /// Number of master nodes (overrides profile default)
        #[arg(long)]
        nodes: Option<u8>,
    },

    /// Generate cluster.toml + .powerfs/ skeleton (declarative, no side effects)
    Init {
        /// Force overwrite existing cluster.toml
        #[arg(long)]
        force: bool,
    },

    /// Reconcile-start the cluster (first run auto-renders configs + issues certs)
    Up {
        /// Only start a single role or node, e.g. master, master-1
        #[arg(long)]
        role: Option<String>,
    },

    /// Stop the cluster
    Down {
        /// Also remove data volumes
        #[arg(long)]
        purge: bool,
        /// Only stop a single role or node
        #[arg(long)]
        role: Option<String>,
    },

    /// Rolling restart (per-node with health gate)
    Restart {
        #[arg(long)]
        role: Option<String>,
        /// Skip health gate between nodes
        #[arg(long)]
        force: bool,
    },

    /// Cluster health overview
    Status,

    /// Configuration rendering and validation
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },

    /// Certificate lifecycle: fetch CA, issue node/client certs, list registry.
    /// (renew/revoke are M4+ — pending master `POST /api/cert/revoke` endpoint.)
    Cert {
        #[command(subcommand)]
        action: CertAction,
    },

    /// Cluster node lifecycle
    Node {
        #[command(subcommand)]
        action: NodeAction,
    },

    /// Enroll a new client (kernel or fuse)
    Client {
        #[command(subcommand)]
        action: ClientAction,
    },

    /// Automated cluster diagnostics
    Doctor {
        /// Attempt to auto-fix low-severity issues
        #[arg(long)]
        fix: bool,
    },

    /// Aggregated logs for a role
    Logs {
        /// Role filter (master, volume, filer, ...)
        #[arg(long)]
        role: Option<String>,
        /// Follow output
        #[arg(short, long)]
        follow: bool,
    },
}

#[derive(Subcommand, Debug)]
pub enum ConfigAction {
    /// Render cluster.toml into docker-compose.yml + per-role TOML (writes to .powerfs/rendered/)
    Render,
    /// Validate cluster.toml and detect config drift
    Check,
    /// Print rendered output without writing files
    Show,
}

#[derive(Subcommand, Debug)]
pub enum CertAction {
    /// Initialize (or reuse) the cluster CA — fetch master's CA cert locally.
    InitCa,
    /// Issue a node or client certificate and persist it under .powerfs/certs/.
    Issue {
        /// Certificate name (e.g. filer-1, fuse-client-3, kernel-node7)
        name: String,
        /// SAN IP addresses (at least one required).
        #[arg(long = "san-ip", num_args = 0..)]
        san_ips: Vec<String>,
        /// Mount directories the client may access. Required for client certs
        /// (ignored when --node is set). Pass multiple times for multi-mount.
        #[arg(long = "mount-dir", num_args = 0..)]
        mount_dirs: Vec<String>,
        /// Issue a node certificate (vs client). Node certs have empty mount_dirs.
        #[arg(long)]
        node: bool,
    },
    /// List certificates with expiry (reads master-side client_registry.json).
    List,
}

#[derive(Subcommand, Debug)]
pub enum NodeAction {
    /// Add a new node (render + start + raft join + cert)
    Add {
        #[arg(long, value_enum)]
        role: NodeRole,
        #[arg(long)]
        ip: String,
    },
    /// Remove a node (drain → raft remove → stop)
    Remove { name: String },
    /// Toggle maintenance mode
    Maintenance {
        name: String,
        #[arg(long)]
        off: bool,
    },
}

#[derive(Subcommand, Debug)]
pub enum ClientAction {
    /// Enroll a new client: issue cert + emit client.toml
    Enroll {
        name: String,
        #[arg(long)]
        ip: String,
        /// Client type
        #[arg(long, value_enum, default_value = "fuse")]
        kind: ClientKindArg,
    },
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum ProfileArg {
    Simple,
    Ha,
    Rdma,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum NodeRole {
    Master,
    Volume,
    Filer,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum ClientKindArg {
    Kernel,
    Fuse,
}
