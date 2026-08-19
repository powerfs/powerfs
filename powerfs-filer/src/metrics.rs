//! HTTP metrics & admin endpoints for the Filer's inode lease manager
//! (lock-optimization plan §7.2 — P7 可观测性).
//!
//! Exposes:
//! - `GET /metrics`: Prometheus-format lease + Early Grant / SN metrics.
//! - `GET /admin/lease-stats`: JSON snapshot of [`LeaseStats`].
//!
//! Mirrors the Volume Server's `powerfs-volume/src/metrics.rs` layout so
//! dashboards can reuse the same panels with a `powerfs_filer_` prefix.
//!
//! # Why gauges (not counters)
//!
//! The source of truth lives in the lease manager's atomic counters and is
//! read via `stats()` at scrape time. Prometheus is pull-based, so we mirror
//! the cumulative counters as gauges that always reflect the current value —
//! semantically equivalent to a counter for scrape-based collection (this is
//! the same convention `powerfs-volume` uses).

use crate::inode_lease_manager::InodeLeaseManager;
use axum::{routing::get, Json, Router, Server};
use log::{error, info};
use prometheus::{register_int_gauge, Encoder, IntGauge};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;

lazy_static::lazy_static! {
    // ===== Current state (gauges) =====
    static ref LEASE_ACTIVE_COUNT: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_active_count",
        "Currently active (non-expired) inode leases"
    ).unwrap();

    static ref LEASE_ACTIVE_HOLDERS: IntGauge = register_int_gauge!(
        "powerfs_filer_lease_active_holders",
        "Currently active unique inode lease holders"
    ).unwrap();

    // ===== Cumulative counters (mirrored as gauges) =====
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
}

/// Refresh prometheus gauges from an [`InodeLeaseManager`] snapshot.
///
/// Reads `stats()` + `current_sn()` + `has_queued_waiters()` and writes
/// them to the global gauges. Called on every `/metrics` scrape.
pub fn refresh_prometheus(lease_mgr: &InodeLeaseManager) {
    let s = lease_mgr.stats();
    LEASE_ACTIVE_COUNT.set(s.active_count as i64);
    LEASE_ACTIVE_HOLDERS.set(s.active_holders as i64);
    LEASE_ACQUIRE_TOTAL.set(s.acquire_total as i64);
    LEASE_ACQUIRE_CONFLICT_TOTAL.set(s.acquire_conflict_total as i64);
    LEASE_RENEW_TOTAL.set(s.renew_total as i64);
    LEASE_RELEASE_TOTAL.set(s.release_total as i64);
    LEASE_EXPIRED_TOTAL.set(s.expired_total as i64);
    LEASE_DISCONNECTED_TOTAL.set(s.disconnected_total as i64);
    LEASE_CURRENT_SN.set(lease_mgr.current_sn() as i64);
    LEASE_HAS_QUEUED_WAITERS.set(if lease_mgr.has_queued_waiters() { 1 } else { 0 });
}

/// Start the HTTP metrics server on the given address.
///
/// Spawns a background tokio task. Returns immediately. Exposes:
/// - `/metrics` (Prometheus text format)
/// - `/admin/lease-stats` (JSON)
pub async fn start_metrics_server(
    addr: SocketAddr,
    lease_mgr: Arc<InodeLeaseManager>,
) -> Result<(), String> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/admin/lease-stats", get(lease_stats_handler))
        .with_state(lease_mgr);

    info!("Filer lease metrics server listening on http://{}", addr);

    tokio::spawn(async move {
        if let Err(e) = Server::bind(&addr).serve(app.into_make_service()).await {
            error!("Filer lease metrics server error: {}", e);
        }
    });

    Ok(())
}

async fn metrics_handler(
    axum::extract::State(lease_mgr): axum::extract::State<Arc<InodeLeaseManager>>,
) -> String {
    refresh_prometheus(&lease_mgr);

    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

async fn lease_stats_handler(
    axum::extract::State(lease_mgr): axum::extract::State<Arc<InodeLeaseManager>>,
) -> Json<serde_json::Value> {
    let s = lease_mgr.stats();
    Json(json!({
        "active_count": s.active_count,
        "active_holders": s.active_holders,
        "acquire_total": s.acquire_total,
        "acquire_conflict_total": s.acquire_conflict_total,
        "renew_total": s.renew_total,
        "release_total": s.release_total,
        "expired_total": s.expired_total,
        "disconnected_total": s.disconnected_total,
        "current_sn": lease_mgr.current_sn(),
        "has_queued_waiters": lease_mgr.has_queued_waiters(),
    }))
}

/// Helper for tests / callers that want a `IntoResponse`-style status
/// string without spinning up the HTTP server.
#[allow(dead_code)]
pub fn render_status(lease_mgr: &InodeLeaseManager) -> String {
    let s = lease_mgr.stats();
    format!(
        "active={} holders={} acquire={} conflict={} renew={} release={} expired={} sn={} waiters={}",
        s.active_count,
        s.active_holders,
        s.acquire_total,
        s.acquire_conflict_total,
        s.renew_total,
        s.release_total,
        s.expired_total,
        lease_mgr.current_sn(),
        lease_mgr.has_queued_waiters(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_refresh_prometheus_reflects_stats() {
        // Use the real InodeLeaseManager so stats() reflects acquire side
        // effects. acquire_total / active_count should show up in gauges.
        let mgr = InodeLeaseManager::new();
        mgr.acquire(1, "client-A", 1_000).unwrap();
        // One conflict (different holder, same inode) bumps conflict_total.
        let _ = mgr.acquire(1, "client-B", 1_000).unwrap_err();

        refresh_prometheus(&mgr);

        assert_eq!(LEASE_ACTIVE_COUNT.get(), 1, "one active lease");
        assert_eq!(LEASE_ACQUIRE_TOTAL.get(), 2, "two acquire calls");
        assert!(LEASE_ACQUIRE_CONFLICT_TOTAL.get() >= 1, "one conflict");
        assert_eq!(LEASE_HAS_QUEUED_WAITERS.get(), 0, "no waiters queued");
    }

    #[test]
    fn test_render_status_contains_key_fields() {
        let mgr = InodeLeaseManager::new();
        mgr.acquire(42, "client-A", 5_000).unwrap();
        let status = render_status(&mgr);
        assert!(status.contains("active=1"), "status: {}", status);
        assert!(status.contains("acquire=1"), "status: {}", status);
        assert!(status.contains("sn="), "status: {}", status);
    }
}
