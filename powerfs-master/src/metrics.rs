use axum::{routing::get, routing::post, Router, Server};
use log::{error, info};
use prometheus::{register_counter, register_gauge, Counter, Encoder, Gauge, TextEncoder};
use std::sync::Arc;

use crate::ca_manager::{get_ca_cert, sign_client, sign_server, CaManager};

lazy_static::lazy_static! {
    pub static ref RAFT_TERM: Gauge = register_gauge!(
        "powerfs_raft_term",
        "Current Raft term"
    ).unwrap();

    pub static ref IS_LEADER: Gauge = register_gauge!(
        "powerfs_is_leader",
        "1 if this node is leader, 0 otherwise"
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

pub async fn start_metrics_server(addr: &str, ca_manager: Arc<CaManager>) -> Result<(), String> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        // Certificate Authority HTTP API (master acts as cluster CA).
        .route("/api/cert/ca", get(get_ca_cert))
        .route("/api/cert/sign-client", post(sign_client))
        .route("/api/cert/sign-server", post(sign_server))
        .with_state(ca_manager);

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
