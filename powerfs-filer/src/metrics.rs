//! HTTP metrics & admin endpoints for the Filer's inode lease manager
//! AND MetaCache (lock-optimization plan §7.2 — P7 observability).
//!
//! Exposes:
//! - `GET /metrics`: Prometheus-format lease + Early Grant / SN + MetaCache
//!   hit/miss/dirty/staging counters.
//! - `GET /admin/lease-stats`: JSON snapshot of [`LeaseStats`].
//! - `GET /admin/meta-cache-stats`: JSON snapshot of [`MetaCacheStats`].
//!
//! Mirrors the Volume Server's `powerfs-volume/src/metrics.rs` layout so
//! dashboards can reuse the same panels with a `powerfs_filer_` prefix.
//!
//! # Why gauges (not counters)
//!
//! The source of truth lives in the lease manager's / MetaCache's atomic
//! counters and is read via `stats()` at scrape time. Prometheus is
//! pull-based, so we mirror the cumulative counters as gauges that always
//! reflect the current value — semantically equivalent to a counter for
//! scrape-based collection (this is the same convention `powerfs-volume`
//! uses).

use crate::inode_lease_manager::InodeLeaseManager;
use crate::meta_cache::{MetaCache, MetaCacheStats};
use axum::{routing::get, Json, Router, Server};
use log::{error, info};
use prometheus::{register_int_gauge, Encoder, IntGauge};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;

lazy_static::lazy_static! {
    // ===== Lease current state (gauges) =====
    static ref LEASE_ACTIVE_COUNT: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_active_count",
        "Currently active (non-expired) inode leases"
    ).unwrap();

    static ref LEASE_ACTIVE_HOLDERS: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_active_holders",
        "Currently active unique inode lease holders"
    ).unwrap();

    // ===== Lease cumulative counters (mirrored as gauges) =====
    static ref LEASE_ACQUIRE_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_acquire_total",
        "Total inode lease acquire calls (success + conflict) since startup"
    ).unwrap();

    static ref LEASE_ACQUIRE_CONFLICT_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_acquire_conflict_total",
        "Inode lease acquire calls that resulted in conflict since startup"
    ).unwrap();

    static ref LEASE_RENEW_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_renew_total",
        "Total successful inode lease renew calls since startup"
    ).unwrap();

    static ref LEASE_RELEASE_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_release_total",
        "Total successful inode lease release calls since startup"
    ).unwrap();

    static ref LEASE_EXPIRED_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_expired_total",
        "Total inode leases removed by cleanup_expired since startup"
    ).unwrap();

    static ref LEASE_DISCONNECTED_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_disconnected_total",
        "Total inode leases removed by disconnect_holder since startup"
    ).unwrap();

    // ===== Phase 4 §5.2/§5.3 — Early Grant + SN visibility =====
    static ref LEASE_CURRENT_SN: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_current_sn",
        "Current high-water sequence number (phase 4 §5.3 — orders IO across leader switches)"
    ).unwrap();

    static ref LEASE_HAS_QUEUED_WAITERS: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_has_queued_waiters",
        "1 if any inode has queued waiters (phase 4 §5.2 Early Grant backpressure), else 0"
    ).unwrap();

    // ===== MetaCache cumulative counters (mirrored as gauges) =====
    static ref MC_INODE_HIT_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_inode_hit_total",
        "MetaCache: total inode reads served from cache (Staging/Clean/Dirty)"
    ).unwrap();
    static ref MC_INODE_MISS_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_inode_miss_total",
        "MetaCache: total inode reads that missed cache (fell back to ShardStore)"
    ).unwrap();
    static ref MC_INODE_DELETED_SERVED_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_inode_deleted_served_total",
        "MetaCache: total reads that returned ENOENT from a pending Deleted tombstone"
    ).unwrap();
    static ref MC_DIRENTRY_HIT_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_direntry_hit_total",
        "MetaCache: total dir-entry reads served from cache"
    ).unwrap();
    static ref MC_DIRENTRY_MISS_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_direntry_miss_total",
        "MetaCache: total dir-entry reads that missed cache (fell back to ShardStore)"
    ).unwrap();
    static ref MC_DIRENTRY_DELETED_SERVED_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_direntry_deleted_served_total",
        "MetaCache: total dir-entry reads that returned ENOENT from a pending Deleted tombstone"
    ).unwrap();
    static ref MC_DIRTY_MARK_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_dirty_mark_total",
        "MetaCache: total SetAttr calls that marked an inode Dirty (pre-Raft)"
    ).unwrap();
    static ref MC_DIRTY_CONFIRM_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_dirty_confirm_total",
        "MetaCache: total SetAttr Raft apply callbacks that moved Dirty→Clean"
    ).unwrap();
    static ref MC_STAGE_DELETE_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_stage_delete_total",
        "MetaCache: total unlink/rmdir calls that staged an entry as Deleted"
    ).unwrap();
    static ref MC_BACKFILL_CLEAN_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_backfill_clean_total",
        "MetaCache: total ShardStore reads that populated a Clean cache entry"
    ).unwrap();
    static ref MC_INVALIDATE_ALL_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_invalidate_all_total",
        "MetaCache: total full invalidations (typically one per leader change)"
    ).unwrap();

    // ===== MetaCache current-state counts =====
    static ref MC_INODE_CLEAN: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_inode_clean",
        "MetaCache: count of inodes currently in Clean state"
    ).unwrap();
    static ref MC_INODE_STAGING: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_inode_staging",
        "MetaCache: count of inodes currently in Staging state (awaiting Raft apply)"
    ).unwrap();
    static ref MC_INODE_DIRTY: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_inode_dirty",
        "MetaCache: count of inodes currently in Dirty state (SetAttr in flight)"
    ).unwrap();
    static ref MC_INODE_DELETED: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_inode_deleted",
        "MetaCache: count of inode Deleted tombstones currently held"
    ).unwrap();
    static ref MC_DIRENTRY_CLEAN: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_direntry_clean",
        "MetaCache: count of directory entries currently in Clean state"
    ).unwrap();
    static ref MC_DIRENTRY_STAGING: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_direntry_staging",
        "MetaCache: count of directory entries currently in Staging state"
    ).unwrap();
    static ref MC_DIRENTRY_DELETED: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_direntry_deleted",
        "MetaCache: count of direntry Deleted tombstones currently held"
    ).unwrap();
    static ref MC_INODE_TRIMMING: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_inode_trimming",
        "MetaCache: count of inodes currently in transient Trimming state"
    ).unwrap();
    static ref MC_DIRENTRY_TRIMMING: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_direntry_trimming",
        "MetaCache: count of direntries currently in transient Trimming state"
    ).unwrap();
    static ref MC_DIRENTRY_DIRTY: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_direntry_dirty",
        "MetaCache: count of direntries currently in Dirty state (rare: only SetAttrData via dentry)"
    ).unwrap();

    // ===== MetaCache Phase-2 counters & memory gauges =====
    static ref MC_TRIM_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_trim_total",
        "MetaCache Phase 2: cumulative entries evicted by LRU trim passes"
    ).unwrap();
    static ref MC_STAGING_TIMEOUT_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_staging_timeout_total",
        "MetaCache Phase 2: entries dropped because Staging was never confirmed by Raft"
    ).unwrap();
    static ref MC_DELETED_TIMEOUT_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_deleted_timeout_total",
        "MetaCache Phase 2: Deleted tombstones swept by sweep_expired_deletions"
    ).unwrap();
    static ref MC_MEMORY_USAGE_BYTES: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_memory_usage_bytes",
        "MetaCache Phase 2: estimated current bytes used by cached inodes + direntries"
    ).unwrap();
    static ref MC_MEMORY_LIMIT_BYTES: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_memory_limit_bytes",
        "MetaCache Phase 2: configured hard memory ceiling (POWERFS_MC_MEMORY_LIMIT_BYTES default 2 GiB)"
    ).unwrap();
    static ref MC_MEMORY_HIGH_BYTES: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_memory_high_watermark_bytes",
        "MetaCache Phase 2: trim_pass starts when usage exceeds this many bytes"
    ).unwrap();
    static ref MC_MEMORY_LOW_BYTES: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_memory_low_watermark_bytes",
        "MetaCache Phase 2: trim_pass stops once usage drops below this many bytes"
    ).unwrap();

    // ===== MetaCache Phase-3 counters (Lease Recall) =====
    static ref MC_RECALL_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_recall_total",
        "MetaCache Phase 3: cumulative lease-recall Invalidate notifications pushed to clients"
    ).unwrap();
    static ref MC_RECALL_COOLDOWN_SKIPS: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_recall_cooldown_skips",
        "MetaCache Phase 3: recall candidates skipped because inode was in recall cooldown (5s)"
    ).unwrap();
    static ref MC_REFCOUNT_LEAK_FIXES: IntGauge = register_int_gauge!(
        "powerfs_filer_mc_refcount_leak_fixes",
        "MetaCache Phase 3: leaked refcounts (refcount > 0 but no active lease) reset to 0 by sweep"
    ).unwrap();
}

/// Shared state passed into the axum Router: lease manager + meta cache.
pub struct MetricsAppState {
    pub lease_mgr: Arc<InodeLeaseManager>,
    pub meta_cache: Arc<MetaCache>,
}

/// Refresh prometheus gauges from a lease-manager snapshot + MetaCache
/// snapshot. Called on every `/metrics` scrape.
pub fn refresh_prometheus(state: &MetricsAppState) {
    // --- Lease ---
    let s = state.lease_mgr.stats();
    LEASE_ACTIVE_COUNT.set(s.active_count as i64);
    LEASE_ACTIVE_HOLDERS.set(s.active_holders as i64);
    LEASE_ACQUIRE_TOTAL.set(s.acquire_total as i64);
    LEASE_ACQUIRE_CONFLICT_TOTAL.set(s.acquire_conflict_total as i64);
    LEASE_RENEW_TOTAL.set(s.renew_total as i64);
    LEASE_RELEASE_TOTAL.set(s.release_total as i64);
    LEASE_EXPIRED_TOTAL.set(s.expired_total as i64);
    LEASE_DISCONNECTED_TOTAL.set(s.disconnected_total as i64);
    LEASE_CURRENT_SN.set(state.lease_mgr.current_sn() as i64);
    LEASE_HAS_QUEUED_WAITERS.set(if state.lease_mgr.has_queued_waiters() {
        1
    } else {
        0
    });

    // --- MetaCache ---
    let m: MetaCacheStats = state.meta_cache.stats();
    MC_INODE_HIT_TOTAL.set(m.inode_hit_total as i64);
    MC_INODE_MISS_TOTAL.set(m.inode_miss_total as i64);
    MC_INODE_DELETED_SERVED_TOTAL.set(m.inode_deleted_served_total as i64);
    MC_DIRENTRY_HIT_TOTAL.set(m.direntry_hit_total as i64);
    MC_DIRENTRY_MISS_TOTAL.set(m.direntry_miss_total as i64);
    MC_DIRENTRY_DELETED_SERVED_TOTAL.set(m.direntry_deleted_served_total as i64);
    MC_DIRTY_MARK_TOTAL.set(m.dirty_mark_total as i64);
    MC_DIRTY_CONFIRM_TOTAL.set(m.dirty_confirm_total as i64);
    MC_STAGE_DELETE_TOTAL.set(m.stage_delete_total as i64);
    MC_BACKFILL_CLEAN_TOTAL.set(m.backfill_clean_total as i64);
    MC_INVALIDATE_ALL_TOTAL.set(m.invalidate_all_total as i64);
    MC_INODE_CLEAN.set(m.inode_clean_count as i64);
    MC_INODE_STAGING.set(m.inode_staging_count as i64);
    MC_INODE_DIRTY.set(m.inode_dirty_count as i64);
    MC_INODE_DELETED.set(m.inode_deleted_count as i64);
    MC_INODE_TRIMMING.set(m.inode_trimming_count as i64);
    MC_DIRENTRY_CLEAN.set(m.direntry_clean_count as i64);
    MC_DIRENTRY_STAGING.set(m.direntry_staging_count as i64);
    MC_DIRENTRY_DIRTY.set(m.direntry_dirty_count as i64);
    MC_DIRENTRY_DELETED.set(m.direntry_deleted_count as i64);
    MC_DIRENTRY_TRIMMING.set(m.direntry_trimming_count as i64);

    // --- Phase 2: trim counters + memory gauges ---
    MC_TRIM_TOTAL.set(m.trim_total as i64);
    MC_STAGING_TIMEOUT_TOTAL.set(m.staging_timeout_total as i64);
    MC_DELETED_TIMEOUT_TOTAL.set(m.deleted_timeout_total as i64);
    MC_MEMORY_USAGE_BYTES.set(m.memory_usage_bytes as i64);
    MC_MEMORY_LIMIT_BYTES.set(m.memory_limit_bytes as i64);
    MC_MEMORY_HIGH_BYTES.set(m.memory_high_watermark_bytes as i64);
    MC_MEMORY_LOW_BYTES.set(m.memory_low_watermark_bytes as i64);

    // --- Phase 3: lease recall counters ---
    MC_RECALL_TOTAL.set(m.recall_total as i64);
    MC_RECALL_COOLDOWN_SKIPS.set(m.recall_cooldown_skips as i64);
    MC_REFCOUNT_LEAK_FIXES.set(m.refcount_leak_fixes as i64);
}

/// Start the HTTP metrics server on the given address.
///
/// Spawns a background tokio task. Returns immediately. Exposes:
/// - `/metrics` (Prometheus text format)
/// - `/admin/lease-stats` (JSON)
/// - `/admin/meta-cache-stats` (JSON)
pub async fn start_metrics_server(
    addr: SocketAddr,
    lease_mgr: Arc<InodeLeaseManager>,
    meta_cache: Arc<MetaCache>,
) -> Result<(), String> {
    let state = Arc::new(MetricsAppState {
        lease_mgr,
        meta_cache,
    });
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/admin/lease-stats", get(lease_stats_handler))
        .route("/admin/meta-cache-stats", get(meta_cache_stats_handler))
        .with_state(state);

    info!(
        "Filer metrics server (lease + MetaCache) listening on http://{}",
        addr
    );

    tokio::spawn(async move {
        if let Err(e) = Server::bind(&addr).serve(app.into_make_service()).await {
            error!("Filer metrics server error: {}", e);
        }
    });

    Ok(())
}

async fn metrics_handler(
    axum::extract::State(state): axum::extract::State<Arc<MetricsAppState>>,
) -> String {
    refresh_prometheus(&state);

    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

async fn lease_stats_handler(
    axum::extract::State(state): axum::extract::State<Arc<MetricsAppState>>,
) -> Json<serde_json::Value> {
    let s = state.lease_mgr.stats();
    Json(json!({
        "active_count": s.active_count,
        "active_holders": s.active_holders,
        "acquire_total": s.acquire_total,
        "acquire_conflict_total": s.acquire_conflict_total,
        "renew_total": s.renew_total,
        "release_total": s.release_total,
        "expired_total": s.expired_total,
        "disconnected_total": s.disconnected_total,
        "current_sn": state.lease_mgr.current_sn(),
        "has_queued_waiters": state.lease_mgr.has_queued_waiters(),
    }))
}

async fn meta_cache_stats_handler(
    axum::extract::State(state): axum::extract::State<Arc<MetricsAppState>>,
) -> Json<serde_json::Value> {
    let m = state.meta_cache.stats();
    Json(json!({
        "inode_hit_total": m.inode_hit_total,
        "inode_miss_total": m.inode_miss_total,
        "inode_deleted_served_total": m.inode_deleted_served_total,
        "direntry_hit_total": m.direntry_hit_total,
        "direntry_miss_total": m.direntry_miss_total,
        "direntry_deleted_served_total": m.direntry_deleted_served_total,
        "dirty_mark_total": m.dirty_mark_total,
        "dirty_confirm_total": m.dirty_confirm_total,
        "stage_delete_total": m.stage_delete_total,
        "backfill_clean_total": m.backfill_clean_total,
        "invalidate_all_total": m.invalidate_all_total,
        "trim_total": m.trim_total,
        "staging_timeout_total": m.staging_timeout_total,
        "deleted_timeout_total": m.deleted_timeout_total,
        "recall_total": m.recall_total,
        "recall_cooldown_skips": m.recall_cooldown_skips,
        "refcount_leak_fixes": m.refcount_leak_fixes,
        "memory": {
            "usage_bytes": m.memory_usage_bytes,
            "limit_bytes": m.memory_limit_bytes,
            "high_watermark_bytes": m.memory_high_watermark_bytes,
            "low_watermark_bytes": m.memory_low_watermark_bytes,
        },
        "state": {
            "inode_clean": m.inode_clean_count,
            "inode_staging": m.inode_staging_count,
            "inode_dirty": m.inode_dirty_count,
            "inode_deleted": m.inode_deleted_count,
            "inode_trimming": m.inode_trimming_count,
            "direntry_clean": m.direntry_clean_count,
            "direntry_staging": m.direntry_staging_count,
            "direntry_dirty": m.direntry_dirty_count,
            "direntry_deleted": m.direntry_deleted_count,
            "direntry_trimming": m.direntry_trimming_count,
        }
    }))
}

/// Helper for tests / callers that want a `IntoResponse`-style status
/// string without spinning up the HTTP server.
#[allow(dead_code)]
pub fn render_status(state: &MetricsAppState) -> String {
    let s = state.lease_mgr.stats();
    let m = state.meta_cache.stats();
    format!(
        "lease: active={} holders={} acquire={} conflict={} renew={} release={} expired={} sn={} waiters={} | \
         mc: ihit={} imiss={} ddel={} dnhit={} dmiss={} dirty={}/{} del={} backfill={} inv={}",
        s.active_count,
        s.active_holders,
        s.acquire_total,
        s.acquire_conflict_total,
        s.renew_total,
        s.release_total,
        s.expired_total,
        state.lease_mgr.current_sn(),
        state.lease_mgr.has_queued_waiters(),
        m.inode_hit_total,
        m.inode_miss_total,
        m.inode_deleted_served_total,
        m.direntry_hit_total,
        m.direntry_miss_total,
        m.dirty_mark_total,
        m.dirty_confirm_total,
        m.stage_delete_total,
        m.backfill_clean_total,
        m.invalidate_all_total,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_refresh_prometheus_reflects_lease_stats() {
        // Use the real InodeLeaseManager so stats() reflects acquire side
        // effects. acquire_total / active_count should show up in gauges.
        let lease_mgr = Arc::new(InodeLeaseManager::new());
        lease_mgr.acquire(1, "client-A", 1_000).unwrap();
        // One conflict (different holder, same inode) bumps conflict_total.
        let _ = lease_mgr.acquire(1, "client-B", 1_000).unwrap_err();
        let meta_cache = Arc::new(MetaCache::new());
        let state = Arc::new(MetricsAppState {
            lease_mgr,
            meta_cache,
        });

        // IntGauges live in the SHARED default prometheus registry. Other
        // parallel tests can overwrite them with their own (possibly zero)
        // snapshot between our refresh_prometheus() and gauge.get().
        // Retry up to 5 times with a tiny sleep; as long as OUR refresh
        // wins the race at least once the test passes.
        let mut ok = false;
        for _ in 0..5u32 {
            refresh_prometheus(&state);
            let active = LEASE_ACTIVE_COUNT.get();
            let acq = LEASE_ACQUIRE_TOTAL.get();
            let cfl = LEASE_ACQUIRE_CONFLICT_TOTAL.get();
            let wait = LEASE_HAS_QUEUED_WAITERS.get();
            if active == 1 && acq == 2 && cfl >= 1 && wait == 0 {
                ok = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            ok,
            "lease prometheus gauges did not settle to expected values \
             (active=1, acquire=2, conflict>=1, waiters=0) after 5 retries — \
             shared registry race or propagation broken"
        );
    }

    #[test]
    fn test_metacache_counters_flow_to_gauges() {
        let lease_mgr = Arc::new(InodeLeaseManager::new());
        let mc = Arc::new(MetaCache::new());

        // Fake some activity: 2 clean backfills + 1 miss per inode/direntry
        // paths is best tested via MetaCache's own methods; here we just
        // touch two counter sides to ensure refresh_prometheus propagates
        // something non-zero.
        use crate::shard_store::InodeInfo;
        mc.cache_put_clean(InodeInfo::tombstone(1));
        mc.cache_put_clean_direntry(100, "foo", 1);
        let _ = mc.get_inode(1); // hit
        let _ = mc.get_direntry(100, "foo"); // hit
        let _ = mc.get_inode(9999); // miss (returns None)

        // 1) Verify MetaCache's own atomic counters first (no global race).
        let s = mc.stats();
        assert!(
            s.backfill_clean_total >= 2,
            "two backfills (inode + direntry), got={}",
            s.backfill_clean_total
        );
        assert_eq!(s.inode_hit_total, 1, "one inode hit");
        assert_eq!(s.direntry_hit_total, 1, "one direntry hit");
        assert_eq!(s.inode_miss_total, 1, "one inode miss");

        // 2) Verify refresh_prometheus propagates the snapshot to the
        //    shared IntGauges. IntGauges live in the default prometheus
        //    registry and are SHARED across parallel tests — other threads
        //    calling refresh_prometheus on their own (possibly empty)
        //    MetaCache instances can overwrite the gauges between our
        //    refresh and our get().
        //
        // We therefore retry refresh + probe up to 5 times with a small
        // sleep between attempts; as long as OUR refresh wins the race
        // at least once the test passes. This keeps the test meaningful
        // (it still validates the propagation path) while tolerating
        // parallel-test interference on the global registry.
        let state = Arc::new(MetricsAppState {
            lease_mgr,
            meta_cache: mc,
        });

        let mut ok = false;
        for _ in 0..5u32 {
            refresh_prometheus(&state);
            let bf = MC_BACKFILL_CLEAN_TOTAL.get();
            let ih = MC_INODE_HIT_TOTAL.get();
            let dh = MC_DIRENTRY_HIT_TOTAL.get();
            let im = MC_INODE_MISS_TOTAL.get();
            if bf >= s.backfill_clean_total as i64
                && ih >= s.inode_hit_total as i64
                && dh >= s.direntry_hit_total as i64
                && im >= s.inode_miss_total as i64
            {
                ok = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            ok,
            "prometheus gauges did not settle to >= MetaCache snapshot \
             (backfill>=2, inode_hit>=1, direntry_hit>=1, inode_miss>=1) \
             after 5 retries — shared registry race or propagation broken"
        );
    }
}
