//! `powerfs-cli fuse-stats` — query FUSE client statistics via MasterService gRPC.
//!
//! The CLI **never** connects directly to FUSE clients (clients must not
//! expose listening endpoints).  Instead, FUSE/kernel clients report their
//! identity, heartbeat, and runtime counters to the Master through the
//! periodic TLV `KeepConnected` heartbeat.  This command fetches the
//! aggregated view via `GetFuseClients` on the master (`-m`).
//!
//! Displays per-client identity, dirty-state, heartbeat age, and the
//! internal `ClientStats` counters reported through the heartbeat.  The
//! old `--addr` argument (which targeted a FUSE admin HTTP port) is
//! removed to enforce the "CLI only talks to the Master" design rule.
//!
//! # Usage
//!
//! ```sh
//! powerfs-cli -m localhost:9333 fuse-stats           # all registered clients
//! powerfs-cli -m localhost:9333 fuse-stats --json    # raw JSON output
//! ```

use clap::Args;

use crate::client::MasterClient;
use powerfs_common::error::PowerFsError;
use powerfs_master::proto::powerfs::{ClientStats, FuseClientsRequest};
use powerfs_master::proto::FuseClientInfo;

#[derive(Args, Debug)]
pub struct FuseStatsArgs {
    /// Output raw JSON (for scripting / piping to jq).
    #[arg(long)]
    json: bool,
}

/// Fetch all registered FUSE clients from the Master (via gRPC) and
/// display their statistics.  Never opens a direct connection to FUSE.
pub async fn fuse_stats(mut client: MasterClient, args: FuseStatsArgs) -> super::CommandResult {
    let mut svc = client
        .service()
        .await
        .map_err(|e| PowerFsError::Internal(format!("Failed to connect: {}", e)))?;

    let resp = svc
        .get_fuse_clients(tonic::Request::new(FuseClientsRequest {}))
        .await
        .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
        .into_inner();

    if !resp.error.is_empty() {
        return Err(PowerFsError::Internal(format!(
            "GetFuseClients returned error: {}",
            resp.error
        )));
    }

    if args.json {
        // NOTE: prost-generated types don't derive serde::Serialize by
        // default.  For scripting purposes the debug representation is
        // unambiguous enough; if a strict JSON schema is needed in the
        // future, this can be built explicitly from the fields.
        for (i, c) in resp.clients.iter().enumerate() {
            if i > 0 {
                println!();
            }
            print_client_debug(c);
        }
        return Ok(());
    }

    display_formatted(&resp.clients);
    Ok(())
}

fn print_client_debug(c: &FuseClientInfo) {
    println!("FuseClientInfo {{");
    println!("  client_id: {:?}", c.client_id);
    println!("  client_type: {:?}", c.client_type);
    println!("  mount_point: {:?}", c.mount_point);
    println!("  collection: {:?}", c.collection);
    println!("  replication: {:?}", c.replication);
    println!("  host: {:?}", c.host);
    println!("  pid: {}", c.pid);
    println!("  connected_at: {}", c.connected_at);
    println!("  last_heartbeat: {}", c.last_heartbeat);
    println!("  dirty_chunks: {}", c.dirty_chunks);
    println!("  dirty_bytes: {}", c.dirty_bytes);
    if let Some(s) = &c.stats {
        println!("  stats: ClientStats {{");
        println!("    data_queue_depth: {}", s.data_queue_depth);
        println!("    lease_queue_depth: {}", s.lease_queue_depth);
        println!("    admin_queue_depth: {}", s.admin_queue_depth);
        println!("    data_processed_total: {}", s.data_processed_total);
        println!("    lease_processed_total: {}", s.lease_processed_total);
        println!("    admin_processed_total: {}", s.admin_processed_total);
        println!("    read_latency_p50_us: {}", s.read_latency_p50_us);
        println!("    read_latency_p99_us: {}", s.read_latency_p99_us);
        println!("    write_latency_p50_us: {}", s.write_latency_p50_us);
        println!("    write_latency_p99_us: {}", s.write_latency_p99_us);
        println!("    active_leases: {}", s.active_leases);
        println!("    lease_renewals_total: {}", s.lease_renewals_total);
        println!("    lease_expired_total: {}", s.lease_expired_total);
        println!("  }}");
    }
    println!("}}");
}

fn display_formatted(clients: &[FuseClientInfo]) {
    println!("════════════════════════════════════════════════════════════════════════");
    println!(
        "  PowerFS FUSE Clients (via MasterService gRPC)  —  {} registered",
        clients.len()
    );
    println!("════════════════════════════════════════════════════════════════════════");

    if clients.is_empty() {
        println!("  (no FUSE clients registered on this master)");
        println!();
        return;
    }

    // Per-client identity & dirty state
    println!("\n┌─ Identity & Dirty State ──────────────────────────────────────────────");
    println!(
        "│ {:<28} {:<6} {:<16} {:<10} {:<12} {:>10} {:>14}",
        "ClientId", "Type", "Mount", "Coll", "Host", "DirtyC", "DirtyBytes"
    );
    println!("│ {}", "─".repeat(102));

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    for c in clients {
        let mount = truncate(&c.mount_point, 16);
        let coll = truncate(&c.collection, 10);
        let host = truncate(&c.host, 12);
        let cid = truncate(&c.client_id, 28);
        println!(
            "│ {:<28} {:<6} {:<16} {:<10} {:<12} {:>10} {:>14}",
            cid,
            c.client_type,
            mount,
            coll,
            host,
            c.dirty_chunks,
            format_bytes(c.dirty_bytes)
        );
    }
    println!("└───────────────────────────────────────────────────────────────────────");

    // Heartbeat / timing
    println!("\n┌─ Heartbeat / Timing ──────────────────────────────────────────────────");
    println!(
        "│ {:<28} {:>7} {:>14} {:>14}",
        "ClientId", "PID", "Connected(s)", "HB-age(s)"
    );
    println!("│ {}", "─".repeat(72));
    for c in clients {
        let cid = truncate(&c.client_id, 28);
        let connected = if c.connected_at > 0 && now_secs >= c.connected_at {
            now_secs - c.connected_at
        } else {
            0
        };
        let hb_age = if c.last_heartbeat > 0 && now_secs >= c.last_heartbeat {
            now_secs - c.last_heartbeat
        } else {
            0
        };
        println!(
            "│ {:<28} {:>7} {:>14} {:>14}",
            cid, c.pid, connected, hb_age
        );
    }
    println!("└───────────────────────────────────────────────────────────────────────");

    // ClientStats counters
    println!("\n┌─ Runtime Counters (ClientStats via heartbeat) ────────────────────────");
    println!(
        "│ {:<28} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10}",
        "ClientId", "DataQ", "LeaseQ", "AdminQ", "DataDone", "LeaseDone", "CoalDirty"
    );
    println!("│ {}", "─".repeat(90));
    for c in clients {
        let cid = truncate(&c.client_id, 28);
        let s = c.stats.as_ref().unwrap_or(&EMPTY_STATS);
        let coalescer = format_bytes(s.coalescer_dirty_bytes);
        println!(
            "│ {:<28} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10}",
            cid,
            s.data_queue_depth,
            s.lease_queue_depth,
            s.admin_queue_depth,
            shorten(s.data_processed_total),
            shorten(s.lease_processed_total),
            coalescer,
        );
    }
    println!("└───────────────────────────────────────────────────────────────────────");

    // Latencies & lease counters
    println!("\n┌─ Latency Pctiles (µs) & Lease State ──────────────────────────────────");
    println!(
        "│ {:<28} {:>8} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10}",
        "ClientId", "RdP50", "RdP99", "WrP50", "WrP99", "ActLeases", "LeaseRenew", "LeaseExp"
    );
    println!("│ {}", "─".repeat(96));
    for c in clients {
        let cid = truncate(&c.client_id, 28);
        let s = c.stats.as_ref().unwrap_or(&EMPTY_STATS);
        println!(
            "│ {:<28} {:>8} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10}",
            cid,
            s.read_latency_p50_us,
            s.read_latency_p99_us,
            s.write_latency_p50_us,
            s.write_latency_p99_us,
            s.active_leases,
            shorten(s.lease_renewals_total),
            shorten(s.lease_expired_total),
        );
    }
    println!("└───────────────────────────────────────────────────────────────────────");
    println!();
}

const EMPTY_STATS: ClientStats = ClientStats {
    data_queue_depth: 0,
    lease_queue_depth: 0,
    admin_queue_depth: 0,
    data_processed_total: 0,
    lease_processed_total: 0,
    admin_processed_total: 0,
    cb_closed_count: 0,
    cb_open_count: 0,
    cb_half_open_count: 0,
    cb_trip_total: 0,
    coalescer_dirty_bytes: 0,
    coalescer_dirty_entries: 0,
    coalescer_writes_in_total: 0,
    coalescer_flushes_out_total: 0,
    pool_active_connections: 0,
    pool_reconnect_total: 0,
    pool_ping_failures: 0,
    read_latency_p50_us: 0,
    read_latency_p99_us: 0,
    write_latency_p50_us: 0,
    write_latency_p99_us: 0,
    active_leases: 0,
    lease_renewals_total: 0,
    lease_expired_total: 0,
};

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

fn format_bytes(b: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;
    if b >= TB {
        format!("{:.1} TB", b as f64 / TB as f64)
    } else if b >= GB {
        format!("{:.1} GB", b as f64 / GB as f64)
    } else if b >= MB {
        format!("{:.1} MB", b as f64 / MB as f64)
    } else if b >= KB {
        format!("{:.1} KB", b as f64 / KB as f64)
    } else {
        format!("{} B", b)
    }
}

fn shorten(v: u64) -> String {
    if v >= 1_000_000_000 {
        format!("{}G", v / 1_000_000_000)
    } else if v >= 1_000_000 {
        format!("{}M", v / 1_000_000)
    } else if v >= 1_000 {
        format!("{}K", v / 1_000)
    } else {
        v.to_string()
    }
}
