//! `powerfs-cli filer-stats` — query Filer statistics via MasterService gRPC.
//!
//! The CLI **never** connects directly to Filer nodes.  Instead, the Master
//! holds a cached view of every registered Filer (identity, heartbeat,
//! shard counts, and admin endpoint ports) and proxies each Filer's HTTP
//! introspection endpoints (`/admin/meta-cache-stats`, `/admin/lease-stats`,
//! `/admin/shards`) on behalf of the CLI.  The aggregated responses are
//! returned to the CLI via the `GetFilerStats` gRPC call.
//!
//! The old `--addr` argument (which targeted a single Filer's HTTP admin
//! port at a hard-coded "grpc_port + 1" derived address) is removed to
//! enforce the "CLI only talks to the Master" and "no port arithmetic"
//! design rules.  If only one Filer is of interest, use `--node-id` to
//! filter.

use clap::Args;

use crate::client::MasterClient;
use powerfs_common::error::PowerFsError;
use powerfs_master::proto::powerfs::FilerStatsRequest;
use powerfs_master::proto::FilerNodeStats;

#[derive(Args, Debug)]
pub struct FilerStatsArgs {
    /// Only return stats for the given filer node id (e.g. "filer-1").
    /// When omitted, all registered filers are returned.
    #[arg(long)]
    node_id: Option<String>,

    /// Output raw, per-filer JSON payloads with minimal decoration (useful
    /// for scripting / piping to jq).  The top-level wrapper is still
    /// human-readable to preserve context; the three raw JSON blobs
    /// (`meta_cache_stats_json`, `lease_stats_json`, `shards_json`) coming
    /// back from each filer's admin endpoints are emitted verbatim.
    #[arg(long)]
    json: bool,
}

/// Fetch Filer statistics from the Master via the `GetFilerStats` gRPC
/// call and display them.  Never opens a direct connection to a Filer.
pub async fn filer_stats(mut client: MasterClient, args: FilerStatsArgs) -> super::CommandResult {
    let mut svc = client
        .service()
        .await
        .map_err(|e| PowerFsError::Internal(format!("Failed to connect: {}", e)))?;

    let req = FilerStatsRequest {
        node_id: args.node_id.clone().unwrap_or_default(),
    };

    let resp = svc
        .get_filer_stats(tonic::Request::new(req))
        .await
        .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
        .into_inner();

    if !resp.error.is_empty() {
        return Err(PowerFsError::Internal(format!(
            "GetFilerStats returned error: {}",
            resp.error
        )));
    }

    if resp.stats.is_empty() {
        match args.node_id {
            Some(id) => println!("No filers matched node_id={}", id),
            None => println!("No filers are currently registered with the Master"),
        }
        return Ok(());
    }

    if args.json {
        for (i, s) in resp.stats.iter().enumerate() {
            if i > 0 {
                println!();
            }
            print_stats_debug(s);
        }
        return Ok(());
    }

    display_formatted(&resp.stats);
    Ok(())
}

fn print_stats_debug(s: &FilerNodeStats) {
    println!("FilerNodeStats {{");
    println!("  node_id: {:?}", s.node_id);
    println!("  address: {:?}", s.address);
    println!("  is_healthy: {}", s.is_healthy);
    println!("  leader_count: {}", s.leader_count);
    println!("  total_shards: {}", s.total_shards);
    println!("  meta_cache_stats_json:");
    println!("{}", s.meta_cache_stats_json);
    println!("  lease_stats_json:");
    println!("{}", s.lease_stats_json);
    println!("  shards_json:");
    println!("{}", s.shards_json);
    println!("}}");
}

fn display_formatted(stats: &[FilerNodeStats]) {
    for s in stats {
        println!(
            "=== Filer: {} (addr={}, healthy={}) ===",
            s.node_id, s.address, s.is_healthy
        );
        println!(
            "  shards: total={}, leader={}",
            s.total_shards, s.leader_count
        );
        if !s.meta_cache_stats_json.is_empty() {
            println!("--- meta-cache-stats ---");
            println!("{}", s.meta_cache_stats_json);
        } else {
            println!("--- meta-cache-stats --- (not reported: metrics_port=0 or endpoint down)");
        }
        if !s.lease_stats_json.is_empty() {
            println!("--- lease-stats ---");
            println!("{}", s.lease_stats_json);
        } else {
            println!("--- lease-stats --- (not reported: metrics_port=0 or endpoint down)");
        }
        if !s.shards_json.is_empty() {
            println!("--- shards ---");
            println!("{}", s.shards_json);
        } else {
            println!("--- shards --- (not reported: http_port=0 or endpoint down)");
        }
        println!();
    }
}
