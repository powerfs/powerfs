//! `powerfs-cli debug` — dynamic log level and debug config control.
//!
//! Controls the Master's centralized debug configuration, which is polled by
//! all nodes (filer/fuse/volume) every 2 seconds via `GetDebugConfig`.
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

use crate::http;
use powerfs_common::error::{PowerFsError, Result};

#[derive(Args, Debug)]
pub struct DebugArgs {
    /// Master metrics/admin server address (host:port).
    /// Uses the metrics port (default 9300), NOT the gRPC port (9333).
    #[arg(long, default_value = "localhost:9335", global = true)]
    admin: String,

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

pub async fn debug(_master: &str, args: DebugArgs) -> super::CommandResult {
    match args.command {
        DebugSubcommand::Get => {
            let body = http::http_get(&args.admin, "/admin/debug")?;
            print_config_list(&body)?;
        }
        DebugSubcommand::Level { level, node } => {
            if let Some(lvl) = level {
                validate_level(&lvl)?;
                let req = serde_json::json!({
                    "node": node,
                    "level": lvl,
                });
                let resp = http::http_put(&args.admin, "/admin/debug", &req.to_string())?;
                println!("Set log level '{}' for node '{}'", lvl, node);
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&resp) {
                    if let Some(updated) = v.get("updated") {
                        println!("  → {}", serde_json::to_string_pretty(updated)?);
                    }
                }
            } else {
                // Show current master log level (local to master process)
                let body = http::http_get(&args.admin, "/admin/log-level")?;
                let v: serde_json::Value = serde_json::from_str(&body)
                    .map_err(|e| PowerFsError::Internal(format!("parse JSON: {}", e)))?;
                println!(
                    "Master log level: {}",
                    v.get("level").and_then(|x| x.as_str()).unwrap_or("?")
                );
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
            let req = serde_json::json!({
                "node": node,
                "flag": name,
                "on": val,
            });
            http::http_put(&args.admin, "/admin/debug", &req.to_string())?;
            println!("Flag '{}' = {} for node '{}'", name, val, node);
        }
        DebugSubcommand::Target { filter, node } => {
            let req = serde_json::json!({
                "node": node,
                "target_filter": filter,
            });
            http::http_put(&args.admin, "/admin/debug", &req.to_string())?;
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
            let path = format!("/admin/debug?node={}", node);
            let resp = http::http_delete(&args.admin, &path)?;
            println!("Cleared debug config for node '{}'", node);
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&resp) {
                if let Some(removed) = v.get("removed") {
                    println!("  → removed: {}", removed);
                }
            }
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

fn print_config_list(body: &str) -> Result<()> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| PowerFsError::Internal(format!("parse JSON: {}", e)))?;

    // API returns {"configs": [["node", {...}], ...]} (Vec of tuples)
    let configs = v
        .get("configs")
        .ok_or_else(|| PowerFsError::Internal("missing 'configs' field".into()))?;

    let entries: Vec<(String, &serde_json::Value)> = match configs {
        serde_json::Value::Array(arr) => {
            // Each item is [node_name, config_obj]
            arr.iter()
                .filter_map(|item| {
                    if let serde_json::Value::Array(pair) = item {
                        if pair.len() == 2 {
                            let name = pair[0].as_str().unwrap_or("?").to_string();
                            return Some((name, &pair[1]));
                        }
                    }
                    None
                })
                .collect()
        }
        serde_json::Value::Object(map) => map.iter().map(|(k, v)| (k.clone(), v)).collect(),
        _ => {
            println!("No debug configurations set (all nodes use defaults).");
            return Ok(());
        }
    };

    if entries.is_empty() {
        println!("No debug configurations set (all nodes use defaults).");
        return Ok(());
    }

    println!("═══════════════════════════════════════════════════════════");
    println!("  Debug Configurations");
    println!("═══════════════════════════════════════════════════════════");
    println!("  Node              Level    Target Filter             Flags");
    println!("  {}", "-".repeat(70));

    for (node, cfg) in &entries {
        let level = cfg.get("log_level").and_then(|x| x.as_str()).unwrap_or("-");
        let filter = cfg
            .get("target_filter")
            .and_then(|x| x.as_str())
            .unwrap_or("-");
        let flags = cfg
            .get("flags")
            .and_then(|x| x.as_object())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| format!("{}={}", k, v.as_bool().unwrap_or(false)))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();

        println!("  {:<15} {:<8} {:<25} {}", node, level, filter, flags);
    }
    println!();
    Ok(())
}
