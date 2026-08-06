use axum::extract::Query;
use axum::response::IntoResponse;
use axum::{routing::get, Json, Router, Server};
use log::{error, info};
use prometheus::{register_counter, register_gauge, Counter, Encoder, Gauge, TextEncoder};
use serde_json::json;
use std::collections::HashMap;

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

pub async fn start_metrics_server(addr: &str) -> Result<(), String> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route(
            "/admin/log-level",
            get(get_log_level_handler).put(set_log_level_handler),
        );

    let addr = addr
        .parse()
        .map_err(|e| format!("Invalid metrics address: {}", e))?;

    info!("Metrics server listening on http://{}", addr);

    tokio::spawn(async move {
        if let Err(e) = Server::bind(&addr).serve(app.into_make_service()).await {
            error!("Metrics server error: {}", e);
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

// ===== Dynamic log level control =====

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
            info!("log level changed via HTTP: {} -> {}", prev, level);
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
