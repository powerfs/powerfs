//! `powerfs-cli fuse-stats` — query FUSE client request statistics.
//!
//! Connects to the FUSE admin/debug HTTP server (started when `admin_port > 0`
//! in the FUSE config) and displays:
//! - Global counters (submitted/completed/errors/in-flight)
//! - Per-`MsgType` breakdown (count, errors, latency min/max/avg)
//! - In-flight requests sorted by age (oldest first — most likely stuck)
//!
//! # Usage
//!
//! ```sh
//! powerfs-cli fuse-stats                          # default localhost:9999
//! powerfs-cli fuse-stats --addr 172.30.0.10:9999  # query remote FUSE
//! powerfs-cli fuse-stats --json                   # raw JSON output
//! ```

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use clap::Args;

use powerfs_common::error::{PowerFsError, Result};

#[derive(Args, Debug)]
pub struct FuseStatsArgs {
    /// FUSE admin server address (host:port).
    #[arg(long, default_value = "localhost:9999")]
    addr: String,

    /// Output raw JSON (for scripting / piping to jq).
    #[arg(long)]
    json: bool,
}

pub fn fuse_stats(_master: &str, args: FuseStatsArgs) -> Result<()> {
    let json = fetch_stats(&args.addr)?;

    if args.json {
        println!("{}", json);
        return Ok(());
    }

    display_formatted(&json, &args.addr)
}

/// Fetch /stats from the FUSE admin server via raw TCP.
fn fetch_stats(addr: &str) -> Result<String> {
    // Resolve hostname (e.g. "localhost") to SocketAddr before connect_timeout.
    let socket_addr = addr
        .to_socket_addrs()
        .map_err(|e| PowerFsError::Internal(format!("invalid addr '{}': {}", addr, e)))?
        .next()
        .ok_or_else(|| PowerFsError::Internal(format!("no address resolved for '{}'", addr)))?;

    let mut stream = TcpStream::connect_timeout(&socket_addr, Duration::from_secs(5))
        .map_err(|e| PowerFsError::Internal(format!("connect {} failed: {}", addr, e)))?;

    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| PowerFsError::Internal(format!("set_read_timeout: {}", e)))?;

    let request = "GET /stats HTTP/1.0\r\nHost: fuse-stats\r\nConnection: close\r\n\r\n";
    stream
        .write_all(request.as_bytes())
        .map_err(|e| PowerFsError::Internal(format!("write request: {}", e)))?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|e| PowerFsError::Internal(format!("read response: {}", e)))?;

    // Parse HTTP response: skip headers, return body
    let response_str = String::from_utf8_lossy(&response);
    let body_start = response_str.find("\r\n\r\n").ok_or_else(|| {
        PowerFsError::Internal("malformed HTTP response (no header/body separator)".into())
    })?;
    let body = &response_str[body_start + 4..];

    // Check HTTP status
    let status_line = response_str.lines().next().unwrap_or("");
    if !status_line.contains("200") {
        return Err(PowerFsError::Internal(format!(
            "admin server returned: {}",
            status_line
        )));
    }

    Ok(body.trim().to_string())
}

/// Display formatted stats output.
fn display_formatted(json_str: &str, addr: &str) -> Result<()> {
    let v: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| PowerFsError::Internal(format!("parse JSON: {}", e)))?;

    println!("═══════════════════════════════════════════════════════════════");
    println!("  PowerFS FUSE Request Statistics  ({})", addr);
    println!("═══════════════════════════════════════════════════════════════");

    // Global summary
    let total_submitted = v
        .get("total_submitted")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    let total_completed = v
        .get("total_completed")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    let total_errors = v.get("total_errors").and_then(|x| x.as_u64()).unwrap_or(0);
    let in_flight = v
        .get("in_flight_count")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    let uptime = v.get("uptime_secs").and_then(|x| x.as_u64()).unwrap_or(0);

    println!("\n┌─ Global ────────────────────────────────────────────────────");
    println!("│ Submitted:  {:>10}", total_submitted);
    println!("│ Completed:  {:>10}", total_completed);
    println!("│ Errors:     {:>10}", total_errors);
    println!("│ In-flight:  {:>10}", in_flight);
    println!("│ Uptime:     {:>9}", format_duration(uptime));

    if total_submitted > 0 {
        let err_rate = (total_errors as f64 / total_submitted as f64) * 100.0;
        println!("│ Error rate: {:>9.2}%", err_rate);
    }
    println!("└──────────────────────────────────────────────────────────────");

    // Per-msg_type stats
    if let Some(per_msg) = v.get("per_msg_type").and_then(|x| x.as_object()) {
        if !per_msg.is_empty() {
            println!("\n┌─ Per Request Type ──────────────────────────────────────────");
            println!(
                "│ {:<18} {:>8} {:>8} {:>6} {:>6} {:>6} {:>8} {:>8} {:>8}",
                "Type", "Submit", "Done", "Err", "T/O", "QFull", "Min_us", "Max_us", "Avg_us"
            );
            println!("│ {}", "-".repeat(86));

            // Sort by submitted descending
            let mut entries: Vec<_> = per_msg.iter().collect();
            entries.sort_by(|a, b| {
                let a_sub = a.1.get("submitted").and_then(|x| x.as_u64()).unwrap_or(0);
                let b_sub = b.1.get("submitted").and_then(|x| x.as_u64()).unwrap_or(0);
                b_sub.cmp(&a_sub)
            });

            for (name, stats) in entries {
                let submitted = stats.get("submitted").and_then(|x| x.as_u64()).unwrap_or(0);
                if submitted == 0 {
                    continue;
                }
                let completed = stats.get("completed").and_then(|x| x.as_u64()).unwrap_or(0);
                let errors = stats.get("errors").and_then(|x| x.as_u64()).unwrap_or(0);
                let timeouts = stats.get("timeouts").and_then(|x| x.as_u64()).unwrap_or(0);
                let queue_fulls = stats
                    .get("queue_fulls")
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0);
                let min_us = stats.get("min_us").and_then(|x| x.as_u64()).unwrap_or(0);
                let max_us = stats.get("max_us").and_then(|x| x.as_u64()).unwrap_or(0);
                let total_us = stats.get("total_us").and_then(|x| x.as_u64()).unwrap_or(0);
                let avg_us = total_us.checked_div(completed).unwrap_or(0);

                println!(
                    "│ {:<18} {:>8} {:>8} {:>6} {:>6} {:>6} {:>8} {:>8} {:>8}",
                    name,
                    submitted,
                    completed,
                    errors,
                    timeouts,
                    queue_fulls,
                    min_us,
                    max_us,
                    avg_us
                );
            }
            println!("└──────────────────────────────────────────────────────────────");
        }
    }

    // In-flight requests (stuck detection)
    if let Some(in_flight_arr) = v.get("in_flight").and_then(|x| x.as_array()) {
        if !in_flight_arr.is_empty() {
            println!("\n┌─ In-Flight Requests (sorted by age, oldest first) ─────────");
            println!(
                "│ {:<3} {:<18} {:>10} {:>10}",
                "#", "Type", "Shard", "Age_ms"
            );
            println!("│ {}", "-".repeat(50));

            for (i, req) in in_flight_arr.iter().enumerate() {
                let msg_type_name = req
                    .get("msg_type_name")
                    .and_then(|x| x.as_str())
                    .unwrap_or("?");
                let shard_id = req.get("shard_id").and_then(|x| x.as_u64()).unwrap_or(0);
                let age_ms = req.get("age_ms").and_then(|x| x.as_u64()).unwrap_or(0);

                // Flag requests older than 1 second as potentially stuck
                let marker = if age_ms > 5000 {
                    " !!! STUCK"
                } else if age_ms > 1000 {
                    " ! slow"
                } else {
                    ""
                };

                println!(
                    "│ {:<3} {:<18} {:>10} {:>10}{}",
                    i + 1,
                    msg_type_name,
                    shard_id,
                    age_ms,
                    marker
                );
            }
            println!("└──────────────────────────────────────────────────────────────");
        }
    }

    println!();
    Ok(())
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    }
}
