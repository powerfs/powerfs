use std::sync::Arc;

use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Json, Path, State},
    response::IntoResponse,
    routing::{get, post, put},
    Router, Server,
};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use serde::Serialize;
use tokio::sync::{Mutex, RwLock};
use tower_http::cors::CorsLayer;

use powerfs_monitor::alert_engine::AlertEngine;
use powerfs_monitor::event::{AlertInfo, AlertRule, ClusterMetrics, Event, EventEnvelope, KVMetrics};
use powerfs_monitor::event_bus::{EventBus, EventStream};
use powerfs_monitor::metric_store::{KVSessionInfo, MetricStore, NodeInfo, VolumeInfo};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8081")]
    addr: String,

    #[arg(long, default_value = "redis://localhost:6379")]
    redis_url: String,

    #[arg(long, default_value = "powerfs:events")]
    stream_key: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
enum WsMessage {
    #[serde(rename = "cluster_metrics")]
    ClusterMetrics(ClusterMetrics),
    #[serde(rename = "node_status")]
    NodeStatus(NodeInfo),
    #[serde(rename = "volume_status")]
    VolumeStatus(VolumeInfo),
    #[serde(rename = "kv_metrics")]
    KVMetrics(KVMetrics),
    #[serde(rename = "alert")]
    Alert(String),
}

struct AppState {
    metric_store: Arc<MetricStore>,
    alert_engine: Arc<AlertEngine>,
    ws_clients: Arc<Mutex<Vec<tokio::sync::mpsc::Sender<WsMessage>>>>,
}

#[derive(Debug, Serialize)]
struct ApiResponse<T> {
    code: i32,
    message: String,
    data: Option<T>,
}

impl<T> ApiResponse<T> {
    fn success(data: T) -> Self {
        Self {
            code: 200,
            message: "success".to_string(),
            data: Some(data),
        }
    }
    fn error(message: &str) -> Self {
        Self {
            code: 500,
            message: message.to_string(),
            data: None,
        }
    }
}

async fn get_cluster_metrics(State(state): State<Arc<AppState>>) -> Json<ApiResponse<ClusterMetrics>> {
    let metrics = state.metric_store.get_cluster_metrics().await;
    Json(ApiResponse::success(metrics))
}

async fn get_nodes(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<NodeInfo>>> {
    let nodes = state.metric_store.get_nodes().await;
    Json(ApiResponse::success(nodes))
}

async fn get_node(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Json<ApiResponse<NodeInfo>> {
    match state.metric_store.get_node(&id).await {
        Some(node) => Json(ApiResponse::success(node)),
        None => Json(ApiResponse::error("Node not found")),
    }
}

async fn get_volumes(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<VolumeInfo>>> {
    let volumes = state.metric_store.get_volumes().await;
    Json(ApiResponse::success(volumes))
}

async fn get_volume(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Json<ApiResponse<VolumeInfo>> {
    match id.parse::<u32>() {
        Ok(id) => match state.metric_store.get_volume(id).await {
            Some(volume) => Json(ApiResponse::success(volume)),
            None => Json(ApiResponse::error("Volume not found")),
        },
        Err(_) => Json(ApiResponse::error("Invalid volume id")),
    }
}

async fn get_kv_metrics(State(state): State<Arc<AppState>>) -> Json<ApiResponse<KVMetrics>> {
    let metrics = state.metric_store.get_kv_metrics().await;
    Json(ApiResponse::success(metrics))
}

async fn get_kv_sessions(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<KVSessionInfo>>> {
    let sessions = state.metric_store.get_kv_sessions().await;
    Json(ApiResponse::success(sessions))
}

async fn get_kv_session(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Json<ApiResponse<KVSessionInfo>> {
    match state.metric_store.get_kv_session(&id).await {
        Some(session) => Json(ApiResponse::success(session)),
        None => Json(ApiResponse::error("Session not found")),
    }
}

async fn get_alerts(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<AlertInfo>>> {
    let alerts = state.alert_engine.get_alerts().await;
    Json(ApiResponse::success(alerts))
}

async fn get_alert(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Json<ApiResponse<AlertInfo>> {
    match state.alert_engine.get_alert(&id).await {
        Some(alert) => Json(ApiResponse::success(alert)),
        None => Json(ApiResponse::error("Alert not found")),
    }
}

async fn acknowledge_alert(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Json<ApiResponse<()>> {
    state.alert_engine.acknowledge_alert(&id).await;
    Json(ApiResponse::success(()))
}

async fn get_alert_rules(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<AlertRule>>> {
    let rules = state.alert_engine.get_rules().await;
    Json(ApiResponse::success(rules))
}

async fn add_alert_rule(State(state): State<Arc<AppState>>, Json(rule): Json<AlertRule>) -> Json<ApiResponse<()>> {
    state.alert_engine.add_rule(rule).await;
    Json(ApiResponse::success(()))
}

async fn update_alert_rule(State(state): State<Arc<AppState>>, Path(_id): Path<String>, Json(rule): Json<AlertRule>) -> Json<ApiResponse<()>> {
    state.alert_engine.update_rule(rule).await;
    Json(ApiResponse::success(()))
}

async fn delete_alert_rule(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Json<ApiResponse<()>> {
    state.alert_engine.remove_rule(&id).await;
    Json(ApiResponse::success(()))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(
    socket: WebSocket,
    state: Arc<AppState>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    
    state.ws_clients.lock().await.push(tx);
    
    let (mut sender, mut receiver) = socket.split();

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let json = serde_json::to_string(&msg).unwrap();
            if sender.send(Message::Text(json)).await.is_err() {
                break;
            }
        }
    });

    while let Some(_msg) = receiver.next().await {}
}

async fn broadcast_message(state: Arc<AppState>, message: WsMessage) {
    let mut clients = state.ws_clients.lock().await;
    let mut i = 0;
    while i < clients.len() {
        if clients[i].send(message.clone()).await.is_err() {
            clients.remove(i);
        } else {
            i += 1;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args = Args::parse();
    info!("Starting PowerFS Monitor Service...");
    info!("Listening on: {}", args.addr);
    info!("Redis URL: {}", args.redis_url);

    let metric_store = Arc::new(MetricStore::new());
    let alert_engine = Arc::new(AlertEngine::new(metric_store.clone()));
    alert_engine.load_default_rules().await;

    let ws_clients = Arc::new(Mutex::new(Vec::new()));

    let app_state = Arc::new(AppState {
        metric_store: metric_store.clone(),
        alert_engine: alert_engine.clone(),
        ws_clients,
    });

    let event_bus = EventBus::new(&args.redis_url, &args.stream_key);

    tokio::spawn(start_event_processor(
        event_bus,
        metric_store.clone(),
        alert_engine.clone(),
        app_state.clone(),
    ));

    tokio::spawn(start_alert_evaluator(alert_engine.clone(), app_state.clone()));

    tokio::spawn(start_metric_broadcaster(metric_store.clone(), app_state.clone()));

    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .route("/api/metrics/cluster", get(get_cluster_metrics))
        .route("/api/metrics/nodes", get(get_nodes))
        .route("/api/metrics/nodes/:id", get(get_node))
        .route("/api/metrics/volumes", get(get_volumes))
        .route("/api/metrics/volumes/:id", get(get_volume))
        .route("/api/metrics/kv", get(get_kv_metrics))
        .route("/api/metrics/kv/sessions", get(get_kv_sessions))
        .route("/api/metrics/kv/sessions/:id", get(get_kv_session))
        .route("/api/alerts", get(get_alerts))
        .route("/api/alerts/:id", get(get_alert))
        .route("/api/alerts/:id/acknowledge", post(acknowledge_alert))
        .route("/api/alert-rules", get(get_alert_rules))
        .route("/api/alert-rules", post(add_alert_rule))
        .route("/api/alert-rules/:id", put(update_alert_rule))
        .route("/api/alert-rules/:id/delete", post(delete_alert_rule))
        .route("/ws/metrics", get(ws_handler))
        .with_state(app_state)
        .layer(cors);

    Server::bind(&args.addr.parse()?)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn start_event_processor(
    event_bus: EventBus,
    metric_store: Arc<MetricStore>,
    alert_engine: Arc<AlertEngine>,
    app_state: Arc<AppState>,
) {
    let stream = event_bus.subscribe().await;

    info!("Event processor started");

    loop {
        match stream.read().await {
            Ok(events) => {
                for event in events {
                    match &event.event {
                        Event::NodeStatus(e) => {
                            metric_store.update_node(e.clone()).await;
                            let node_info = metric_store.get_node(&e.node_id).await;
                            if let Some(node) = node_info {
                                broadcast_message(app_state.clone(), WsMessage::NodeStatus(node)).await;
                            }
                        }
                        Event::VolumeStatus(e) => {
                            metric_store.update_volume(e.clone()).await;
                            let volume_info = metric_store.get_volume(e.volume_id).await;
                            if let Some(volume) = volume_info {
                                broadcast_message(app_state.clone(), WsMessage::VolumeStatus(volume)).await;
                            }
                        }
                        Event::KVSession(e) => {
                            metric_store.update_kv_session(e.clone()).await;
                            let kv_metrics = metric_store.get_kv_metrics().await;
                            broadcast_message(app_state.clone(), WsMessage::KVMetrics(kv_metrics)).await;
                        }
                        Event::KVBlock(e) => {
                            if e.event_type == "write" {
                                metric_store.increment_kv_put().await;
                            } else if e.event_type == "read" {
                                metric_store.increment_kv_get().await;
                            }
                        }
                        Event::MetricUpdate(e) => {
                            info!("Metric update: {} = {}", e.metric_name, e.value);
                        }
                        Event::AlertTrigger(e) => {
                            broadcast_message(app_state.clone(), WsMessage::Alert(e.message.clone())).await;
                        }
                    }
                    let _ = stream.ack(&event.event_id).await;
                }
            }
            Err(e) => {
                warn!("Error reading events: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn start_alert_evaluator(
    alert_engine: Arc<AlertEngine>,
    app_state: Arc<AppState>,
) {
    info!("Alert evaluator started");

    loop {
        let alerts = alert_engine.evaluate_rules().await;
        for alert in alerts {
            info!("Alert triggered: {}", alert.name);
            broadcast_message(app_state.clone(), WsMessage::Alert(serde_json::to_string(&alert).unwrap())).await;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    }
}

async fn start_metric_broadcaster(
    metric_store: Arc<MetricStore>,
    app_state: Arc<AppState>,
) {
    info!("Metric broadcaster started");

    loop {
        let cluster_metrics = metric_store.get_cluster_metrics().await;
        broadcast_message(app_state.clone(), WsMessage::ClusterMetrics(cluster_metrics)).await;

        let kv_metrics = metric_store.get_kv_metrics().await;
        broadcast_message(app_state.clone(), WsMessage::KVMetrics(kv_metrics)).await;

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}