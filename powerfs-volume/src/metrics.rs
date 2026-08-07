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
use axum::extract::Query;
use axum::response::IntoResponse;
use axum::{routing::get, Json, Router, Server};
use log::{error, info};
use powerfs_net::flow_control::FlowController;
use prometheus::{register_int_gauge, Encoder, IntGauge};
use serde_json::json;
use std::collections::HashMap;
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

    // ===== Flow control metrics (Phase 1 S5) =====
    // Cumulative counters mirrored as gauges (same pattern as lease metrics
    // above; semantically equivalent for scrape-based collection).

    // Current state (gauges)
    static ref FLOW_ACTIVE_REQS: IntGauge = register_int_gauge!(
        "powerfs_flow_active_reqs",
        "In-flight requests being processed"
    ).unwrap();

    static ref FLOW_ACTIVE_CONNS: IntGauge = register_int_gauge!(
        "powerfs_flow_active_conns",
        "Currently registered connections"
    ).unwrap();

    static ref FLOW_SLOW_CONNS: IntGauge = register_int_gauge!(
        "powerfs_flow_slow_conns",
        "Connections currently marked slow (EWMA latency above threshold)"
    ).unwrap();

    // Cumulative counters (mirrored as gauges)
    static ref FLOW_TOTAL_REQS: IntGauge = register_int_gauge!(
        "powerfs_flow_total_reqs",
        "Total requests completed since startup"
    ).unwrap();

    static ref FLOW_TOTAL_ERRS: IntGauge = register_int_gauge!(
        "powerfs_flow_total_errs",
        "Total requests completed with error since startup"
    ).unwrap();

    static ref FLOW_TOTAL_BYTES_SENT: IntGauge = register_int_gauge!(
        "powerfs_flow_total_bytes_sent",
        "Total response bytes sent since startup"
    ).unwrap();

    static ref FLOW_TOTAL_BYTES_RECV: IntGauge = register_int_gauge!(
        "powerfs_flow_total_bytes_recv",
        "Total request bytes received since startup"
    ).unwrap();

    // Latency histogram buckets (cumulative counts, mirrored as gauges)
    static ref FLOW_LAT_BUCKET_1MS: IntGauge = register_int_gauge!(
        "powerfs_flow_latency_bucket_1ms",
        "Requests with latency <= 1ms (cumulative)"
    ).unwrap();

    static ref FLOW_LAT_BUCKET_10MS: IntGauge = register_int_gauge!(
        "powerfs_flow_latency_bucket_10ms",
        "Requests with latency <= 10ms (cumulative)"
    ).unwrap();

    static ref FLOW_LAT_BUCKET_100MS: IntGauge = register_int_gauge!(
        "powerfs_flow_latency_bucket_100ms",
        "Requests with latency <= 100ms (cumulative)"
    ).unwrap();

    static ref FLOW_LAT_BUCKET_1S: IntGauge = register_int_gauge!(
        "powerfs_flow_latency_bucket_1s",
        "Requests with latency <= 1s (cumulative)"
    ).unwrap();

    static ref FLOW_LAT_BUCKET_10S: IntGauge = register_int_gauge!(
        "powerfs_flow_latency_bucket_10s",
        "Requests with latency <= 10s (cumulative)"
    ).unwrap();

    static ref FLOW_LAT_BUCKET_INF: IntGauge = register_int_gauge!(
        "powerfs_flow_latency_bucket_inf",
        "Requests with latency > 10s (cumulative)"
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

/// Refresh flow control prometheus gauges from a FlowController snapshot.
///
/// Called periodically by the collector task (every 5s). All values are set
/// as absolute (cumulative counters mirrored as gauges, same pattern as
/// lease metrics).
fn refresh_flow_prometheus(fc: &FlowController) {
    let snap = fc.snapshot_global();
    FLOW_ACTIVE_REQS.set(snap.active_reqs as i64);
    FLOW_ACTIVE_CONNS.set(snap.active_conns as i64);
    FLOW_SLOW_CONNS.set(snap.slow_conns as i64);
    FLOW_TOTAL_REQS.set(snap.total_reqs as i64);
    FLOW_TOTAL_ERRS.set(snap.total_errs as i64);
    FLOW_TOTAL_BYTES_SENT.set(snap.total_bytes_sent as i64);
    FLOW_TOTAL_BYTES_RECV.set(snap.total_bytes_recv as i64);
    FLOW_LAT_BUCKET_1MS.set(snap.lat_buckets[0] as i64);
    FLOW_LAT_BUCKET_10MS.set(snap.lat_buckets[1] as i64);
    FLOW_LAT_BUCKET_100MS.set(snap.lat_buckets[2] as i64);
    FLOW_LAT_BUCKET_1S.set(snap.lat_buckets[3] as i64);
    FLOW_LAT_BUCKET_10S.set(snap.lat_buckets[4] as i64);
    FLOW_LAT_BUCKET_INF.set(snap.lat_buckets[5] as i64);
}

/// Start the HTTP metrics server on the given address.
///
/// Spawns a background tokio task. Returns immediately.
///
/// Exposes:
/// - `/metrics` (Prometheus), `/admin/lease-stats`, `/admin/log-level` (lease State)
/// - `/admin/flow/*` (flow control State, Phase 1 S4)
pub async fn start_metrics_server(
    addr: SocketAddr,
    lease_mgr: Arc<RangeLeaseManager>,
    flow_ctrl: Arc<FlowController>,
) -> Result<(), String> {
    // Lease & log-level routes (State = Arc<RangeLeaseManager>)
    let lease_app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/admin/lease-stats", get(lease_stats_handler))
        .route(
            "/admin/log-level",
            get(get_log_level_handler).put(set_log_level_handler),
        )
        .with_state(lease_mgr);

    // Flow control routes (State = Arc<FlowController>)
    let flow_app = Router::new()
        .route("/admin/flow/overview", get(flow_overview_handler))
        .route("/admin/flow/connections", get(flow_connections_handler))
        .route("/admin/flow/global", get(flow_global_handler))
        .route("/admin/flow/slow", get(flow_slow_handler))
        .route(
            "/admin/flow/policy",
            get(flow_policy_handler).put(flow_policy_set_handler),
        )
        .with_state(flow_ctrl.clone());

    // Merge: 两个 Router<()> (State 已填充) 可直接 merge
    let app = lease_app.merge(flow_app);

    info!("Volume metrics server listening on http://{}", addr);

    tokio::spawn(async move {
        if let Err(e) = Server::bind(&addr).serve(app.into_make_service()).await {
            error!("Volume metrics server error: {}", e);
        }
    });

    // Spawn flow control metrics collector (refresh every 5s)
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            refresh_flow_prometheus(&flow_ctrl);
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

// ===== Dynamic log level control =====
//
// GET  /admin/log-level           → {"level":"info"}
// PUT  /admin/log-level?level=debug → {"level":"debug","prev":"info"}
// PUT  /admin/log-level?level=warn  → {"level":"warn","prev":"debug"}

async fn get_log_level_handler() -> Json<serde_json::Value> {
    Json(json!({ "level": powerfs_common::dynamic_log::get_log_level() }))
}

async fn set_log_level_handler(
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let level = match params.get("level") {
        Some(l) => l.as_str(),
        None => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(json!({ "error": "missing 'level' query parameter" })),
            )
        }
    };
    let prev = powerfs_common::dynamic_log::get_log_level().to_string();
    match powerfs_common::dynamic_log::set_log_level(level) {
        Ok(()) => {
            info!(
                "log level changed via HTTP: {} -> {}",
                prev, level
            );
            (
                axum::http::StatusCode::OK,
                Json(json!({ "level": level, "prev": prev })),
            )
        }
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": e })),
        ),
    }
}

// ===== Flow control admin (Phase 1 S4) =====
//
// GET /admin/flow/overview    → 总览 (policy + global + counts), 适合 dashboard 首页
// GET /admin/flow/connections → 所有连接统计 (按 conn_id 排序)
// GET /admin/flow/global      → 全局统计 (含 6 桶延迟直方图 + slow_conns)
// GET /admin/flow/slow        → 慢连接列表 (slow=true)
// GET /admin/flow/policy      → 策略信息 (name + load_factor)
//
// Handler 通过 powerfs_net::flow_admin 纯函数生成 JSON, 保持本层薄.

async fn flow_overview_handler(
    axum::extract::State(fc): axum::extract::State<Arc<FlowController>>,
) -> Json<serde_json::Value> {
    Json(powerfs_net::flow_admin::overview_json(&fc))
}

async fn flow_connections_handler(
    axum::extract::State(fc): axum::extract::State<Arc<FlowController>>,
) -> Json<serde_json::Value> {
    Json(powerfs_net::flow_admin::connections_json(&fc))
}

async fn flow_global_handler(
    axum::extract::State(fc): axum::extract::State<Arc<FlowController>>,
) -> Json<serde_json::Value> {
    Json(powerfs_net::flow_admin::global_json(&fc))
}

async fn flow_slow_handler(
    axum::extract::State(fc): axum::extract::State<Arc<FlowController>>,
) -> Json<serde_json::Value> {
    Json(powerfs_net::flow_admin::slow_connections_json(&fc))
}

async fn flow_policy_handler(
    axum::extract::State(fc): axum::extract::State<Arc<FlowController>>,
) -> Json<serde_json::Value> {
    Json(powerfs_net::flow_admin::policy_json(&fc))
}

/// PUT /admin/flow/policy?max_active_global=N&max_active_per_conn=M
///
/// 运行时调整 AdaptiveConcurrencyPolicy 参数, 便于测试 load_factor 联动.
async fn flow_policy_set_handler(
    axum::extract::State(fc): axum::extract::State<Arc<FlowController>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let mut changed = Vec::new();

    if let Some(v) = params.get("max_active_global") {
        match v.parse::<u32>() {
            Ok(n) if n > 0 => {
                if fc.set_max_active_global(n) {
                    changed.push(format!("max_active_global={}", n));
                } else {
                    return (
                        axum::http::StatusCode::BAD_REQUEST,
                        Json(json!({"error": "adaptive-concurrency policy not installed"})),
                    );
                }
            }
            _ => {
                return (
                    axum::http::StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid max_active_global"})),
                );
            }
        }
    }

    if let Some(v) = params.get("max_active_per_conn") {
        match v.parse::<u32>() {
            Ok(n) if n > 0 => {
                if fc.set_max_active_per_conn(n) {
                    changed.push(format!("max_active_per_conn={}", n));
                }
            }
            _ => {
                return (
                    axum::http::StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid max_active_per_conn"})),
                );
            }
        }
    }

    info!("flow policy adjusted via HTTP: {}", changed.join(", "));
    (
        axum::http::StatusCode::OK,
        Json(json!({
            "changed": changed,
            "load_factor": fc.current_load_factor(),
        })),
    )
}
