//! `powerfs-cli topology` — view cluster topology from Monitor API.
//!
//! Fetches `/api/topology` from the Monitor service and displays:
//! - shard_count, filer nodes, volume nodes, master nodes
//! - Per-node health status (online/offline/healthy)
//!
//! # Usage
//!
//! ```sh
//! powerfs-cli topology                          # default localhost:8080
//! powerfs-cli topology --monitor 172.30.0.20:8080
//! powerfs-cli topology --json
//! ```

use clap::Args;
use powerfs_common::error::PowerFsError;

use crate::http;

#[derive(Args, Debug)]
pub struct TopologyArgs {
    /// Monitor server address (host:port).
    #[arg(long, default_value = "localhost:8080", global = true)]
    monitor: String,

    /// Output raw JSON.
    #[arg(long)]
    json: bool,
}

pub async fn topology(_master: &str, args: TopologyArgs) -> super::CommandResult {
    let body = http::http_get(&args.monitor, "/api/topology")?;

    if args.json {
        println!("{}", body);
        return Ok(());
    }

    let v: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| PowerFsError::Internal(format!("parse JSON: {}", e)))?;

    println!("═══════════════════════════════════════════════════════════");
    println!("  PowerFS Cluster Topology");
    println!("═══════════════════════════════════════════════════════════");

    // shard_count
    let shard_count = v.get("shard_count").and_then(|x| x.as_u64()).unwrap_or(0);
    println!("\n  Shard count: {}", shard_count);

    // Master nodes
    print_node_group(&v, "masters", "Master Nodes");
    print_node_group(&v, "filers", "Filer Nodes");
    print_node_group(&v, "volumes", "Volume Nodes");

    // Summary
    println!("\n── Summary ──");
    let masters = v.get("masters").and_then(|x| x.as_array());
    let filers = v.get("filers").and_then(|x| x.as_array());
    let volumes = v.get("volumes").and_then(|x| x.as_array());
    println!(
        "  Masters: {}  Filers: {}  Volumes: {}",
        masters.map(|a| a.len()).unwrap_or(0),
        filers.map(|a| a.len()).unwrap_or(0),
        volumes.map(|a| a.len()).unwrap_or(0),
    );

    println!();
    Ok(())
}

fn print_node_group(v: &serde_json::Value, key: &str, title: &str) {
    if let Some(arr) = v.get(key).and_then(|x| x.as_array()) {
        if arr.is_empty() {
            return;
        }
        println!("\n┌─ {} ─────────────────────────────────", title);
        println!(
            "  {:<20} {:<15} {:<10} {:<20}",
            "ID", "Address", "Status", "Role"
        );
        println!("  {}", "-".repeat(65));
        for node in arr {
            let id = node
                .get("id")
                .or_else(|| node.get("node_id"))
                .and_then(|x| x.as_str())
                .unwrap_or("?");
            let addr = node
                .get("address")
                .or_else(|| node.get("addr"))
                .and_then(|x| x.as_str())
                .unwrap_or("?");
            let status = node
                .get("status")
                .or_else(|| node.get("state"))
                .and_then(|x| x.as_str())
                .unwrap_or("?");
            let role = node.get("role").and_then(|x| x.as_str()).unwrap_or("-");
            println!("  {:<20} {:<15} {:<10} {:<20}", id, addr, status, role);
        }
        println!("└──────────────────────────────────────────");
    }
}
