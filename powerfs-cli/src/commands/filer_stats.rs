//! `powerfs-cli filer-stats` — query Filer admin statistics.
//!
//! Connects to a Filer's HTTP admin server and displays:
//! - MetaCache stats (hit/miss/dirty/recall metrics)
//! - Lease stats (active leases, holders, revokes)
//! - Shard distribution (shard IDs, leaders, entry counts)
//!
//! # Usage
//!
//! ```sh
//! powerfs-cli filer-stats                          # default localhost:8888
//! powerfs-cli filer-stats --addr filer-1:8888
//! powerfs-cli filer-stats --addr filer-1:8888 --meta-cache
//! powerfs-cli filer-stats --addr filer-1:8888 --lease
//! powerfs-cli filer-stats --addr filer-1:8888 --shards
//! powerfs-cli filer-stats --json
//! ```

use clap::Args;
use powerfs_common::error::{PowerFsError, Result};

use crate::http;

#[derive(Args, Debug)]
pub struct FilerStatsArgs {
    /// Filer admin server address (host:port).
    /// Default is the metrics server port (grpc_port + 1).
    /// Use port 8888 for /admin/status and /admin/shards (S3 HTTP server).
    #[arg(long, default_value = "localhost:8890", global = true)]
    addr: String,

    /// Show only MetaCache stats.
    #[arg(long)]
    meta_cache: bool,

    /// Show only Lease stats.
    #[arg(long)]
    lease: bool,

    /// Show only Shard distribution.
    #[arg(long)]
    shards: bool,

    /// Output raw JSON.
    #[arg(long)]
    json: bool,
}

pub async fn filer_stats(_master: &str, args: FilerStatsArgs) -> super::CommandResult {
    let show_all = !args.meta_cache && !args.lease && !args.shards;

    if args.meta_cache || show_all {
        fetch_and_print(
            &args.addr,
            "/admin/meta-cache-stats",
            "MetaCache Statistics",
            args.json,
        )?;
    }
    if args.lease || show_all {
        fetch_and_print(
            &args.addr,
            "/admin/lease-stats",
            "Lease Statistics",
            args.json,
        )?;
    }
    if args.shards || show_all {
        fetch_and_print(&args.addr, "/admin/shards", "Shard Distribution", args.json)?;
    }
    Ok(())
}

fn fetch_and_print(addr: &str, path: &str, title: &str, json: bool) -> Result<()> {
    let body = http::http_get(addr, path)?;

    if json {
        println!("{}", body);
        return Ok(());
    }

    let v: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| PowerFsError::Internal(format!("parse JSON: {}", e)))?;

    println!("═══════════════════════════════════════════════════════════");
    println!("  {}  ({})", title, addr);
    println!("═══════════════════════════════════════════════════════════");

    print_json_flat(&v, 0);
    println!();
    Ok(())
}

/// Recursively print JSON as flat key=value lines (2-space indent per level).
fn print_json_flat(v: &serde_json::Value, indent: usize) {
    let pad = "  ".repeat(indent);
    match v {
        serde_json::Value::Object(map) => {
            for (k, val) in map.iter() {
                match val {
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        println!("{}{}:", pad, k);
                        print_json_flat(val, indent + 1);
                    }
                    _ => {
                        println!("{}{:<30} {}", pad, format!("{}:", k), val);
                    }
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, item) in arr.iter().enumerate() {
                println!("{}[{}]:", pad, i);
                print_json_flat(item, indent + 1);
            }
        }
        _ => {
            println!("{}{}", pad, v);
        }
    }
}
