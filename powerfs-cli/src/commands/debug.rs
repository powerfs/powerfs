//! `powerfs-cli debug` — dynamic log level and debug config control via MasterService gRPC.
//!
//! Controls the Master's centralized debug configuration, which is polled by
//! all nodes (filer/fuse/volume) every 2 seconds via `GetDebugConfig`.
//!
//! Admin-control traffic goes exclusively through MasterService gRPC (the
//! same `-m` endpoint used for all other master APIs). Metrics port on the
//! other hand is reserved for Prometheus data-collection only (`/metrics`).
//!
//! # Subcommands
//!
//! ```sh
//! powerfs-cli debug get                          # list all node configs
//! powerfs-cli debug level debug                  # set global log level
//! powerfs-cli debug level debug --node fuse-1    # set per-node level
//! powerfs-cli debug level                        # show current effective level
//! powerfs-cli debug flag fuse_create_timing --on # enable a debug flag
//! powerfs-cli debug flag fuse_create_timing --off
//! powerfs-cli debug target "powerfs_fuse::fuse"  # set target filter
//! powerfs-cli debug clear --node fuse-1          # clear node config
//! ```

use clap::{Args, Subcommand};

use crate::client::MasterClient;
use powerfs_common::error::{PowerFsError, Result};
use powerfs_master::proto::powerfs::{
    ClearDebugConfigRequest, DebugConfigEntry, GetDebugConfigsRequest, GetLogLevelRequest,
    UpdateDebugConfigRequest,
};

#[derive(Args, Debug)]
pub struct DebugArgs {
    #[command(subcommand)]
    command: DebugSubcommand,
}

#[derive(Subcommand, Debug)]
enum DebugSubcommand {
    /// List all node debug configurations.
    Get,

    /// Get or set log level (off/error/warn/info/debug/trace).
    Level {
        /// Level to set. If omitted, shows current master log level.
        level: Option<String>,

        /// Target node ("all" for global default, or "fuse-1", "filer-2", etc.)
        #[arg(long, default_value = "all")]
        node: String,
    },

    /// Set or clear a debug flag.
    Flag {
        /// Flag name (e.g. "fuse_create_timing", "metacache_trace").
        name: String,

        /// Turn the flag on.
        #[arg(long)]
        on: bool,

        /// Turn the flag off.
        #[arg(long)]
        off: bool,

        /// Target node.
        #[arg(long, default_value = "all")]
        node: String,
    },

    /// Set target filter (e.g. "powerfs_fuse::fuse" to only show FUSE logs).
    Target {
        /// Target filter string. Empty string clears the filter.
        filter: String,

        /// Target node.
        #[arg(long, default_value = "all")]
        node: String,
    },

    /// Clear a node's debug configuration (revert to defaults).
    Clear {
        /// Node to clear. Use "all" to clear global default.
        #[arg(long)]
        node: String,
    },
}

pub async fn debug(mut client: MasterClient, args: DebugArgs) -> super::CommandResult {
    let mut svc = client
        .service()
        .await
        .map_err(|e| PowerFsError::Internal(format!("Failed to connect: {}", e)))?;

    match args.command {
        DebugSubcommand::Get => {
            let resp = svc
                .get_debug_configs(tonic::Request::new(GetDebugConfigsRequest {}))
                .await
                .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?;
            print_config_list(resp.into_inner().configs)?;
        }
        DebugSubcommand::Level { level, node } => {
            if let Some(lvl) = level {
                validate_level(&lvl)?;
                let req = UpdateDebugConfigRequest {
                    node: node.clone(),
                    has_level: true,
                    level: lvl.clone(),
                    has_target_filter: false,
                    target_filter: String::new(),
                    has_flag: false,
                    flag: String::new(),
                    on: false,
                };
                let resp = svc
                    .update_debug_config(tonic::Request::new(req))
                    .await
                    .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
                    .into_inner();
                if !resp.success {
                    return Err(PowerFsError::Internal(if resp.error.is_empty() {
                        "update_debug_config failed".into()
                    } else {
                        resp.error
                    }));
                }
                println!("Set log level '{}' for node '{}'", lvl, node);
                if let Some(updated) = resp.updated {
                    println!("  → {}", format_entry(&updated));
                }
            } else {
                // Show current master node's local log level (per-master, not cluster-wide)
                let resp = svc
                    .get_log_level(tonic::Request::new(GetLogLevelRequest {}))
                    .await
                    .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?;
                println!("Master log level: {}", resp.into_inner().level);
            }
        }
        DebugSubcommand::Flag {
            name,
            on,
            off,
            node,
        } => {
            let val = if on {
                true
            } else if off {
                false
            } else {
                return Err(PowerFsError::Internal("must specify --on or --off".into()));
            };
            let req = UpdateDebugConfigRequest {
                node: node.clone(),
                has_level: false,
                level: String::new(),
                has_target_filter: false,
                target_filter: String::new(),
                has_flag: true,
                flag: name.clone(),
                on: val,
            };
            let resp = svc
                .update_debug_config(tonic::Request::new(req))
                .await
                .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            if !resp.success {
                return Err(PowerFsError::Internal(if resp.error.is_empty() {
                    "update_debug_config failed".into()
                } else {
                    resp.error
                }));
            }
            println!("Flag '{}' = {} for node '{}'", name, val, node);
        }
        DebugSubcommand::Target { filter, node } => {
            let req = UpdateDebugConfigRequest {
                node: node.clone(),
                has_level: false,
                level: String::new(),
                has_target_filter: true,
                target_filter: filter.clone(),
                has_flag: false,
                flag: String::new(),
                on: false,
            };
            let resp = svc
                .update_debug_config(tonic::Request::new(req))
                .await
                .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            if !resp.success {
                return Err(PowerFsError::Internal(if resp.error.is_empty() {
                    "update_debug_config failed".into()
                } else {
                    resp.error
                }));
            }
            println!(
                "Target filter '{}' set for node '{}'",
                if filter.is_empty() {
                    "(cleared)"
                } else {
                    &filter
                },
                node
            );
        }
        DebugSubcommand::Clear { node } => {
            let req = ClearDebugConfigRequest { node: node.clone() };
            let resp = svc
                .clear_debug_config(tonic::Request::new(req))
                .await
                .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            if !resp.success {
                return Err(PowerFsError::Internal(if resp.error.is_empty() {
                    "clear_debug_config failed".into()
                } else {
                    resp.error
                }));
            }
            println!("Cleared debug config for node '{}'", node);
            println!("  → removed: {}", resp.removed);
        }
    }
    Ok(())
}

fn validate_level(level: &str) -> Result<()> {
    match level {
        "off" | "error" | "warn" | "info" | "debug" | "trace" => Ok(()),
        _ => Err(PowerFsError::Internal(format!(
            "invalid level '{}': must be off/error/warn/info/debug/trace",
            level
        ))),
    }
}

fn format_entry(e: &DebugConfigEntry) -> String {
    let flags = e
        .flags
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(", ");
    let level = if e.has_log_level { &e.log_level } else { "-" };
    let target = if e.has_target_filter {
        if e.target_filter.is_empty() {
            "(cleared)"
        } else {
            &e.target_filter
        }
    } else {
        "-"
    };
    format!(
        "node={}, level={}, target={}, flags={{{}}}",
        e.node, level, target, flags
    )
}

fn print_config_list(entries: Vec<DebugConfigEntry>) -> Result<()> {
    println!("═══════════════════════════════════════════════════════════");
    println!("  Debug Configurations (via MasterService gRPC)");
    println!("═══════════════════════════════════════════════════════════");
    println!("  Node              Level    Target Filter             Flags");
    println!("  {}", "-".repeat(70));

    if entries.is_empty() {
        println!("  (no configurations set — all nodes use defaults)");
        println!();
        return Ok(());
    }

    // Sort: "all" first, then alphabetical
    let mut sorted = entries;
    sorted.sort_by(|a, b| {
        if a.node == "all" {
            std::cmp::Ordering::Less
        } else if b.node == "all" {
            std::cmp::Ordering::Greater
        } else {
            a.node.cmp(&b.node)
        }
    });

    for entry in &sorted {
        let level = if entry.has_log_level {
            entry.log_level.as_str()
        } else {
            "-"
        };
        let filter = if entry.has_target_filter {
            if entry.target_filter.is_empty() {
                "(cleared)"
            } else {
                entry.target_filter.as_str()
            }
        } else {
            "-"
        };
        let flags = entry
            .flags
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(", ");
        println!("  {:<15} {:<8} {:<25} {}", entry.node, level, filter, flags);
    }
    println!();
    Ok(())
}
