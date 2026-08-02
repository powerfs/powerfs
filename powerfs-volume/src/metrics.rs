//! HTTP metrics & admin endpoints for the Volume Server.
//!
//! Exposes:
//! - `GET /metrics`: Prometheus-format lease metrics.
//! - `GET /admin/lease-stats`: JSON snapshot of [`LeaseStats`].
//!
//! The HTTP listener is bound to the volume server's `http_port` (the same
//! port advertised to the master), so no extra config is required.
//!
//! All metrics are exposed as Prometheus gauges. The `*_total` fields from
//! [`LeaseStats`] are cumulative since store creation; we mirror them as
//! gauges (rather than counters) because the source of truth lives in the
//! store's atomic counters and Prometheus cannot push deltas — a gauge that
//! always reflects the current cumulative value is semantically equivalent
//! for scrape-based collection.

use crate::range_lease::RangeLeaseManager;
use axum::{routing::get, Json, Router, Server};
use log::{error, info};
use prometheus::{register_int_gauge, Encoder, IntGauge};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;

lazy_static::lazy_static! {
    // Current state
    static ref LEASE_ACTIVE_COUNT: IntGauge = register_int_gauge!(
        "powerfs_volume_lease_active_count",
        "Currently active (non-expired) leases"
    ).unwrap();

    static ref LEASE_ACTIVE_HOLDERS: IntGauge = register_int_gauge!(
        "powerfs_volume_lease_active_holders",
        "Currently active unique lease holders"
    ).unwrap();

    // Cumulative counters (mirrored as gauges)
    static ref LEASE_ACQUIRE_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_volume_lease_acquire_total",
        "Total lease acquire calls (success + conflict) since startup"
    ).unwrap();

    static ref LEASE_ACQUIRE_CONFLICT_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_volume_lease_acquire_conflict_total",
        "Lease acquire calls that resulted in conflict since startup"
    ).unwrap();

    static ref LEASE_RENEW_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_volume_lease_renew_total",
        "Total successful lease renew calls since startup"
    ).unwrap();

    static ref LEASE_RELEASE_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_volume_lease_release_total",
        "Total successful lease release calls since startup"
    ).unwrap();

    static ref LEASE_EXPIRED_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_volume_lease_expired_total",
        "Total leases removed by cleanup_expired since startup"
    ).unwrap();

    static ref LEASE_DISCONNECTED_TOTAL: IntGauge = register_int_gauge!(
        "powerfs_volume_lease_disconnected_total",
        "Total leases removed by disconnect_holder since startup"
    ).unwrap();
}

/// Refresh prometheus gauges from a [`LeaseStats`] snapshot.
fn refresh_prometheus(stats: &powerfs_lease::LeaseStats) {
    LEASE_ACTIVE_COUNT.set(stats.active_count as i64);
    LEASE_ACTIVE_HOLDERS.set(stats.active_holders as i64);
    LEASE_ACQUIRE_TOTAL.set(stats.acquire_total as i64);
    LEASE_ACQUIRE_CONFLICT_TOTAL.set(stats.acquire_conflict_total as i64);
    LEASE_RENEW_TOTAL.set(stats.renew_total as i64);
    LEASE_RELEASE_TOTAL.set(stats.release_total as i64);
    LEASE_EXPIRED_TOTAL.set(stats.expired_total as i64);
    LEASE_DISCONNECTED_TOTAL.set(stats.disconnected_total as i64);
}

/// Start the HTTP metrics server on the given address.
///
/// Spawns a background tokio task. Returns immediately.
pub async fn start_metrics_server(
    addr: SocketAddr,
    lease_mgr: Arc<RangeLeaseManager>,
) -> Result<(), String> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/admin/lease-stats", get(lease_stats_handler))
        .with_state(lease_mgr);

    info!("Volume metrics server listening on http://{}", addr);

    tokio::spawn(async move {
        if let Err(e) = Server::bind(&addr).serve(app.into_make_service()).await {
            error!("Volume metrics server error: {}", e);
        }
    });

    Ok(())
}

async fn metrics_handler(
    axum::extract::State(lease_mgr): axum::extract::State<Arc<RangeLeaseManager>>,
) -> String {
    let stats = lease_mgr.stats();
    refresh_prometheus(&stats);

    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

async fn lease_stats_handler(
    axum::extract::State(lease_mgr): axum::extract::State<Arc<RangeLeaseManager>>,
) -> Json<serde_json::Value> {
    let stats = lease_mgr.stats();
    Json(json!({
        "active_count": stats.active_count,
        "active_holders": stats.active_holders,
        "acquire_total": stats.acquire_total,
        "acquire_conflict_total": stats.acquire_conflict_total,
        "renew_total": stats.renew_total,
        "release_total": stats.release_total,
        "expired_total": stats.expired_total,
        "disconnected_total": stats.disconnected_total,
    }))
}
