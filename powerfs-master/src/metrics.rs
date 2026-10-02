use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{routing::get, Router, Server};
use log::{error, info};
use powerfs_core::kv_cache::KVCacheEngine;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Gauge as ProtoGauge, LabelPair, Metric, MetricFamily, MetricType};
use prometheus::{
    register, register_counter, register_gauge, Counter, Encoder, Gauge, TextEncoder,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::admin_api::{admin_router, AdminOps, AdminState};
use crate::ca_manager::{cert_router, CaManager};

/// Global flag set by the raft health monitor when the node is a
/// fake-Leader (lease expired but still Leader). The /healthz endpoint
/// reads this to return 503 so Docker auto-restarts the container (#58).
static RAFT_UNAVAILABLE: AtomicBool = AtomicBool::new(false);

/// Set the global raft-unavailable flag (called from health monitor).
pub fn set_raft_unavailable(v: bool) {
    RAFT_UNAVAILABLE.store(v, Ordering::Relaxed);
}

lazy_static::lazy_static! {
    pub static ref RAFT_TERM: Gauge = register_gauge!(
        "powerfs_raft_term",
        "Current Raft term"
    ).unwrap();

    pub static ref IS_LEADER: Gauge = register_gauge!(
        "powerfs_is_leader",
        "1 if this node is leader, 0 otherwise"
    ).unwrap();

    // Raft progress gauges — consumed by powerfs-ctl's health gate to detect
    // zombie leaders (asymmetric split-brain where heartbeats round-trip but
    // AppendEntries can't commit, so commit_index never advances even though
    // scheme C's ensure_linearizable probe returns OK).
    pub static ref RAFT_COMMIT_INDEX: Gauge = register_gauge!(
        "powerfs_raft_commit_index",
        "Raft commit index (local node view)"
    ).unwrap();

    pub static ref RAFT_LAST_APPLIED: Gauge = register_gauge!(
        "powerfs_raft_last_applied",
        "Last applied log index (local node view)"
    ).unwrap();

    pub static ref VOLUME_COUNT: Gauge = register_gauge!(
        "powerfs_volume_count",
        "Total number of volumes in the cluster"
    ).unwrap();

    pub static ref NODE_COUNT: Gauge = register_gauge!(
        "powerfs_node_count",
        "Total number of nodes in the cluster"
    ).unwrap();

    pub static ref COLLECTION_COUNT: Gauge = register_gauge!(
        "powerfs_collection_count",
        "Total number of collections"
    ).unwrap();

    pub static ref REQUEST_COUNT: Counter = register_counter!(
        "powerfs_request_count",
        "Total number of requests handled"
    ).unwrap();

    pub static ref ASSIGN_REQUEST_COUNT: Counter = register_counter!(
        "powerfs_assign_request_count",
        "Number of volume assign requests"
    ).unwrap();

    pub static ref LOOKUP_REQUEST_COUNT: Counter = register_counter!(
        "powerfs_lookup_request_count",
        "Number of volume lookup requests"
    ).unwrap();
}

/// Dynamic Prometheus collector for KV read heat (Phase C). On every scrape it
/// reads a fresh engine snapshot and emits session/block read gauges, so there
/// are no stale series after sessions or blocks are deleted and no permanent
/// high-cardinality registry state.
pub struct KvHeatCollector {
    engine: Arc<KVCacheEngine>,
    session_desc: Desc,
    block_desc: Desc,
}

impl KvHeatCollector {
    pub fn new(engine: Arc<KVCacheEngine>) -> Self {
        Self {
            session_desc: Desc::new(
                "powerfs_kv_session_reads".to_string(),
                "Total successful reads per KV session".to_string(),
                vec!["session_id".to_string(), "namespace_id".to_string()],
                std::collections::HashMap::new(),
            )
            .unwrap(),
            block_desc: Desc::new(
                "powerfs_kv_block_reads".to_string(),
                "Total successful reads per KV block".to_string(),
                vec![
                    "block_id".to_string(),
                    "session_id".to_string(),
                    "namespace_id".to_string(),
                ],
                std::collections::HashMap::new(),
            )
            .unwrap(),
            engine,
        }
    }
}

fn label_pair(name: &str, value: &str) -> LabelPair {
    let mut p = LabelPair::new();
    p.set_name(name.to_string());
    p.set_value(value.to_string());
    p
}

impl Collector for KvHeatCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.session_desc, &self.block_desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let (sessions, blocks) = self.engine.read_heat(0);

        let mut smf = MetricFamily::new();
        smf.set_name("powerfs_kv_session_reads".to_string());
        smf.set_help("Total successful reads per KV session".to_string());
        smf.set_field_type(MetricType::GAUGE);
        for s in &sessions {
            let mut m = Metric::new();
            m.mut_label().push(label_pair("session_id", &s.session_id));
            m.mut_label()
                .push(label_pair("namespace_id", &s.namespace_id));
            let mut g = ProtoGauge::new();
            g.set_value(s.read_count as f64);
            m.set_gauge(g);
            smf.mut_metric().push(m);
        }

        let mut bmf = MetricFamily::new();
        bmf.set_name("powerfs_kv_block_reads".to_string());
        bmf.set_help("Total successful reads per KV block".to_string());
        bmf.set_field_type(MetricType::GAUGE);
        for b in &blocks {
            let mut m = Metric::new();
            m.mut_label()
                .push(label_pair("block_id", &b.block_id.to_string()));
            m.mut_label().push(label_pair("session_id", &b.session_id));
            m.mut_label()
                .push(label_pair("namespace_id", &b.namespace_id));
            let mut g = ProtoGauge::new();
            g.set_value(b.read_count as f64);
            m.set_gauge(g);
            bmf.mut_metric().push(m);
        }

        vec![smf, bmf]
    }
}

pub async fn start_metrics_server(
    addr: &str,
    ca_manager: Arc<CaManager>,
    admin: Arc<AdminState>,
    engine: Arc<KVCacheEngine>,
) -> Result<(), String> {
    register(Box::new(KvHeatCollector::new(engine)))
        .map_err(|e| format!("failed to register KV heat collector: {}", e))?;

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        // Health endpoint for Docker healthcheck. Returns 503 when the
        // master is a fake-Leader (raft_unavailable=true) so Docker
        // restarts the container automatically (#58).
        .route("/healthz", get(healthz_handler))
        // Certificate Authority HTTP API (master acts as cluster CA),
        // including the M5 registry admin routes (list/renew/revoke).
        .nest("/api/cert", cert_router().with_state(ca_manager))
        // M5 master administration API (raft membership, node lifecycle).
        .nest(
            "/api/admin",
            admin_router().with_state(admin as Arc<dyn AdminOps>),
        );

    let addr = addr
        .parse()
        .map_err(|e| format!("Invalid metrics address: {}", e))?;

    info!("Metrics + cert API server listening on http://{}", addr);

    tokio::spawn(async move {
        if let Err(e) = Server::bind(&addr).serve(app.into_make_service()).await {
            error!("Metrics/cert API server error: {}", e);
        }
    });

    Ok(())
}

async fn metrics_handler() -> String {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metrics = prometheus::gather();
    encoder.encode(&metrics, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Docker healthcheck endpoint. Returns 200 OK when raft is available,
/// 503 when the master is a fake-Leader (raft_unavailable=true).
/// This lets Docker auto-restart wedged masters (#58).
async fn healthz_handler() -> axum::response::Response {
    if RAFT_UNAVAILABLE.load(Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "raft unavailable (fake-Leader)\n",
        )
            .into_response();
    }
    (StatusCode::OK, "ok\n").into_response()
}
