use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Json, Path, Query, State,
    },
    response::IntoResponse,
    routing::{delete, get, post, put},
    Router, Server,
};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

use powerfs_monitor::alert_engine::AlertEngine;
use powerfs_monitor::event::{AlertInfo, AlertRule, ClusterMetrics, Event, KVMetrics};
use powerfs_monitor::event_bus::EventBus;
use powerfs_monitor::metric_store::{KVSessionInfo, MetricStore, NodeInfo, VolumeInfo};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8081")]
    addr: String,

    #[arg(long, default_value = "redis://localhost:6379")]
    redis_url: String,

    #[arg(long, default_value = "powerfs_events")]
    stream_key: String,

    #[arg(long, default_value = "http://localhost:9000")]
    s3_endpoint: String,

    #[arg(long, default_value = "http://localhost:9002")]
    s3_backend_endpoint: String,
}

#[derive(Debug, Clone, Serialize)]
struct WsMetricUpdate {
    #[serde(rename = "type")]
    message_type: String,
    source: String,
    payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
struct WsAlertUpdate {
    #[serde(rename = "type")]
    message_type: String,
    payload: serde_json::Value,
}

struct AppState {
    metric_store: Arc<MetricStore>,
    alert_engine: Arc<AlertEngine>,
    ws_clients: Arc<Mutex<Vec<tokio::sync::mpsc::Sender<serde_json::Value>>>>,
    s3_endpoint: String,
    s3_backend_endpoint: String,
    fuse_mounts: Arc<Mutex<Vec<FuseMount>>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct S3Metrics {
    bucket_count: u64,
    object_count: u64,
    total_size: u64,
    active_multipart_uploads: u64,
    put_requests: u64,
    get_requests: u64,
    delete_requests: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct BucketInfo {
    name: String,
    creation_date: String,
    object_count: u64,
    total_size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ObjectInfo {
    key: String,
    etag: String,
    size: u64,
    last_modified: String,
    storage_class: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct MultipartUploadInfo {
    bucket: String,
    key: String,
    upload_id: String,
    initiator: String,
    creation_date: String,
    part_count: u64,
    status: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct FuseMount {
    id: String,
    mount_point: String,
    collection: String,
    replication: String,
    master: String,
    threads: usize,
    status: String,
    mounted_at: String,
    pid: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct CreateFuseMountRequest {
    mount_point: String,
    collection: String,
    replication: String,
    master: String,
    threads: usize,
}

#[derive(Debug, Deserialize)]
struct CreateBucketRequest {
    name: String,
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

async fn get_cluster_metrics(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<ClusterMetrics>> {
    let metrics = state.metric_store.get_cluster_metrics().await;
    Json(ApiResponse::success(metrics))
}

async fn get_nodes(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<NodeInfo>>> {
    let nodes = state.metric_store.get_nodes().await;
    Json(ApiResponse::success(nodes))
}

async fn get_node(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<ApiResponse<NodeInfo>> {
    match state.metric_store.get_node(&id).await {
        Some(node) => Json(ApiResponse::success(node)),
        None => Json(ApiResponse::error("Node not found")),
    }
}

async fn get_volumes(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<VolumeInfo>>> {
    let volumes = state.metric_store.get_volumes().await;
    Json(ApiResponse::success(volumes))
}

async fn get_volume(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<ApiResponse<VolumeInfo>> {
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

async fn get_kv_sessions(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<KVSessionInfo>>> {
    let sessions = state.metric_store.get_kv_sessions().await;
    Json(ApiResponse::success(sessions))
}

async fn get_kv_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<ApiResponse<KVSessionInfo>> {
    match state.metric_store.get_kv_session(&id).await {
        Some(session) => Json(ApiResponse::success(session)),
        None => Json(ApiResponse::error("Session not found")),
    }
}

#[derive(Debug, Serialize)]
struct TimeSeriesPoint {
    time: String,
    value: f64,
}

async fn get_s3_metrics(State(state): State<Arc<AppState>>) -> Json<ApiResponse<S3Metrics>> {
    let client = reqwest::Client::new();
    let url = format!("{}/", state.s3_endpoint);

    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                if let Ok(body) = response.text().await {
                    let bucket_count = body.matches("<Bucket>").count() as u64;
                    Json(ApiResponse::success(S3Metrics {
                        bucket_count,
                        object_count: 0,
                        total_size: 0,
                        active_multipart_uploads: 0,
                        put_requests: 0,
                        get_requests: 0,
                        delete_requests: 0,
                    }))
                } else {
                    Json(ApiResponse::error("Failed to parse S3 response"))
                }
            } else {
                Json(ApiResponse::error("Failed to get S3 metrics"))
            }
        }
        Err(e) => {
            warn!("S3 connection error: {}", e);
            Json(ApiResponse::success(S3Metrics {
                bucket_count: 0,
                object_count: 0,
                total_size: 0,
                active_multipart_uploads: 0,
                put_requests: 0,
                get_requests: 0,
                delete_requests: 0,
            }))
        }
    }
}

async fn get_buckets(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<BucketInfo>>> {
    let client = reqwest::Client::new();
    let url = format!("{}/", state.s3_endpoint);

    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                if let Ok(body) = response.text().await {
                    let buckets = parse_list_buckets_xml(&body);
                    Json(ApiResponse::success(buckets))
                } else {
                    Json(ApiResponse::error("Failed to parse S3 response"))
                }
            } else {
                Json(ApiResponse::error("Failed to get buckets"))
            }
        }
        Err(e) => {
            warn!("S3 connection error: {}", e);
            Json(ApiResponse::success(Vec::new()))
        }
    }
}

async fn get_bucket(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<ApiResponse<BucketInfo>> {
    let client = reqwest::Client::new();
    let url = format!("{}/{}", state.s3_endpoint, name);

    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                if let Ok(body) = response.text().await {
                    let objects = parse_list_objects_xml(&body);
                    let total_size: u64 = objects.iter().map(|o| o.size).sum();
                    Json(ApiResponse::success(BucketInfo {
                        name,
                        creation_date: chrono::Utc::now().to_rfc3339(),
                        object_count: objects.len() as u64,
                        total_size,
                    }))
                } else {
                    Json(ApiResponse::error("Failed to parse S3 response"))
                }
            } else {
                Json(ApiResponse::error("Bucket not found"))
            }
        }
        Err(e) => {
            warn!("S3 connection error: {}", e);
            Json(ApiResponse::error("S3 connection error"))
        }
    }
}

async fn create_bucket(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateBucketRequest>,
) -> Json<ApiResponse<()>> {
    let client = reqwest::Client::new();
    let url = format!("{}/{}", state.s3_endpoint, req.name);

    match client.put(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                Json(ApiResponse::success(()))
            } else {
                Json(ApiResponse::error("Failed to create bucket"))
            }
        }
        Err(e) => {
            warn!("S3 connection error: {}", e);
            Json(ApiResponse::error("S3 connection error"))
        }
    }
}

async fn delete_bucket(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<ApiResponse<()>> {
    let client = reqwest::Client::new();
    let url = format!("{}/{}", state.s3_endpoint, name);

    match client.delete(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                Json(ApiResponse::success(()))
            } else {
                Json(ApiResponse::error("Failed to delete bucket"))
            }
        }
        Err(e) => {
            warn!("S3 connection error: {}", e);
            Json(ApiResponse::error("S3 connection error"))
        }
    }
}

async fn get_objects(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> Json<ApiResponse<Vec<ObjectInfo>>> {
    let client = reqwest::Client::new();
    let url = format!("{}/{}", state.s3_endpoint, bucket);

    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                if let Ok(body) = response.text().await {
                    let objects = parse_list_objects_xml(&body);
                    Json(ApiResponse::success(objects))
                } else {
                    Json(ApiResponse::error("Failed to parse S3 response"))
                }
            } else {
                Json(ApiResponse::error("Bucket not found"))
            }
        }
        Err(e) => {
            warn!("S3 connection error: {}", e);
            Json(ApiResponse::success(Vec::new()))
        }
    }
}

async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<ApiResponse<()>> {
    if let Some(upload_id) = params.get("uploadId") {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/_admin/multipart-uploads/{}",
            state.s3_endpoint, upload_id
        );

        match client.delete(&url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    Json(ApiResponse::success(()))
                } else {
                    Json(ApiResponse::error("Failed to abort multipart upload"))
                }
            }
            Err(e) => {
                warn!("S3 connection error: {}", e);
                Json(ApiResponse::error("S3 connection error"))
            }
        }
    } else {
        let client = reqwest::Client::new();
        let url = format!("{}/{}/{}", state.s3_endpoint, bucket, key);

        match client.delete(&url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    Json(ApiResponse::success(()))
                } else {
                    Json(ApiResponse::error("Failed to delete object"))
                }
            }
            Err(e) => {
                warn!("S3 connection error: {}", e);
                Json(ApiResponse::error("S3 connection error"))
            }
        }
    }
}

async fn get_multipart_uploads(
    State(state): State<Arc<AppState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<ApiResponse<Vec<MultipartUploadInfo>>> {
    let client = reqwest::Client::new();
    let url = format!("{}/_admin/multipart-uploads", state.s3_endpoint);

    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                if let Ok(mut json) = response.json::<Vec<MultipartUploadInfo>>().await {
                    if let Some(bucket) = params.get("bucket") {
                        json.retain(|u| u.bucket == *bucket);
                    }
                    Json(ApiResponse::success(json))
                } else {
                    Json(ApiResponse::error("Failed to parse multipart uploads"))
                }
            } else {
                Json(ApiResponse::error("Failed to get multipart uploads"))
            }
        }
        Err(e) => {
            warn!("S3 connection error: {}", e);
            Json(ApiResponse::success(Vec::new()))
        }
    }
}

async fn upload_object(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    mut req: axum::extract::Multipart,
) -> Json<ApiResponse<()>> {
    info!("Upload object request received for bucket: {}", bucket);

    let mut key: Option<String> = None;
    let mut file_data: Option<axum::body::Bytes> = None;

    while let Some(field) = req.next_field().await.unwrap() {
        let name = field.name().unwrap_or("").to_string();
        info!("Found field: {}", name);
        if name == "key" {
            key = Some(field.text().await.unwrap());
            info!("Got key: {:?}", key);
        } else if name == "file" {
            file_data = Some(field.bytes().await.unwrap());
            info!(
                "Got file data: {} bytes",
                file_data.as_ref().map(|b| b.len()).unwrap_or(0)
            );
        }
    }

    let key = match key {
        Some(k) => k,
        None => return Json(ApiResponse::error("Missing key")),
    };

    let data = match file_data {
        Some(d) => d,
        None => return Json(ApiResponse::error("Missing file")),
    };

    let client = reqwest::Client::new();
    let url = format!("{}/{}/{}", state.s3_endpoint, bucket, key);
    info!("Sending request to S3: PUT {}", url);

    match client.put(&url).body(data).send().await {
        Ok(response) => {
            info!("S3 response status: {}", response.status());
            if response.status().is_success() {
                Json(ApiResponse::success(()))
            } else {
                let body = response.text().await.unwrap_or_default();
                warn!("S3 upload failed: {}", body);
                Json(ApiResponse::error("Failed to upload object"))
            }
        }
        Err(e) => {
            warn!("S3 connection error: {}", e);
            Json(ApiResponse::error("S3 connection error"))
        }
    }
}

async fn download_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let client = reqwest::Client::new();
    let url = format!("{}/{}/{}", state.s3_endpoint, bucket, key);

    match client.get(&url).send().await {
        Ok(response) => {
            let status = response.status();
            let content_type = response
                .headers()
                .get("Content-Type")
                .map(|h| h.to_str().unwrap_or("application/octet-stream"))
                .unwrap_or("application/octet-stream")
                .to_string();
            let etag = response
                .headers()
                .get("ETag")
                .map(|h| h.to_str().unwrap_or(""))
                .unwrap_or("")
                .to_string();

            if status.is_success() {
                let body = response.bytes().await.unwrap();

                let mut resp: axum::http::Response<axum::body::Body> =
                    axum::response::Response::new(axum::body::Body::from(body));
                resp.headers_mut().insert(
                    axum::http::header::CONTENT_TYPE,
                    content_type.parse().unwrap(),
                );
                resp.headers_mut().insert(
                    axum::http::header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", key).parse().unwrap(),
                );
                if !etag.is_empty() {
                    resp.headers_mut()
                        .insert(axum::http::header::ETAG, etag.parse().unwrap());
                }
                resp
            } else {
                let body = response.text().await.unwrap_or_default();
                let mut resp: axum::http::Response<axum::body::Body> =
                    axum::response::Response::new(axum::body::Body::from(body));
                *resp.status_mut() = axum::http::StatusCode::NOT_FOUND;
                resp
            }
        }
        Err(e) => {
            warn!("S3 connection error: {}", e);
            let mut resp: axum::http::Response<axum::body::Body> =
                axum::response::Response::new(axum::body::Body::from("S3 connection error"));
            *resp.status_mut() = axum::http::StatusCode::INTERNAL_SERVER_ERROR;
            resp
        }
    }
}

// ===== S3 Access Key Management =====

async fn get_s3_access_keys(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<serde_json::Value>>> {
    let client = reqwest::Client::new();
    let url = format!("{}/_admin/keys", state.s3_backend_endpoint);

    match client.get(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<Vec<serde_json::Value>>().await {
                    Ok(keys) => Json(ApiResponse::success(keys)),
                    Err(_) => Json(ApiResponse::error("Failed to parse access keys")),
                }
            } else {
                Json(ApiResponse::error("Failed to get access keys"))
            }
        }
        Err(e) => {
            warn!("S3 backend connection error: {}", e);
            Json(ApiResponse::error("S3 backend connection error"))
        }
    }
}

async fn create_s3_access_key(
    State(state): State<Arc<AppState>>,
    Json(req): Json<serde_json::Value>,
) -> Json<ApiResponse<serde_json::Value>> {
    let client = reqwest::Client::new();
    let url = format!("{}/_admin/keys", state.s3_backend_endpoint);

    match client.post(&url).json(&req).send().await {
        Ok(response) => {
            if response.status().is_success() || response.status().as_u16() == 201 {
                match response.json::<serde_json::Value>().await {
                    Ok(key) => Json(ApiResponse::success(key)),
                    Err(_) => Json(ApiResponse::error("Failed to parse response")),
                }
            } else {
                Json(ApiResponse::error("Failed to create access key"))
            }
        }
        Err(e) => {
            warn!("S3 backend connection error: {}", e);
            Json(ApiResponse::error("S3 backend connection error"))
        }
    }
}

async fn delete_s3_access_key(
    State(state): State<Arc<AppState>>,
    Path(access_key): Path<String>,
) -> Json<ApiResponse<()>> {
    let client = reqwest::Client::new();
    let url = format!("{}/_admin/keys/{}", state.s3_backend_endpoint, access_key);

    match client.delete(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                Json(ApiResponse::success(()))
            } else {
                Json(ApiResponse::error("Failed to delete access key"))
            }
        }
        Err(e) => {
            warn!("S3 backend connection error: {}", e);
            Json(ApiResponse::error("S3 backend connection error"))
        }
    }
}

async fn get_fuse_mounts(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<FuseMount>>> {
    let mounts = state.fuse_mounts.lock().await;
    let mut result = mounts.clone();

    for mount in result.iter_mut() {
        if let Some(pid) = mount.pid {
            match tokio::process::Command::new("kill")
                .arg("-0")
                .arg(pid.to_string())
                .spawn()
            {
                Ok(mut child) => {
                    if let Ok(status) = child.wait().await {
                        if !status.success() {
                            mount.status = "unmounted".to_string();
                        }
                    } else {
                        mount.status = "unmounted".to_string();
                    }
                }
                Err(_) => {
                    mount.status = "unmounted".to_string();
                }
            }
        }
    }

    Json(ApiResponse::success(result))
}

async fn create_fuse_mount(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateFuseMountRequest>,
) -> Json<ApiResponse<FuseMount>> {
    let mount_id = uuid::Uuid::new_v4().to_string();

    let mount_path = std::path::Path::new(&req.mount_point);
    if !mount_path.exists() {
        if let Err(e) = std::fs::create_dir_all(mount_path) {
            return Json(ApiResponse::error(&format!(
                "Failed to create mount point: {}",
                e
            )));
        }
    }

    let cmd = tokio::process::Command::new("/app/powerfs-fuse")
        .arg("--master")
        .arg(&req.master)
        .arg("--mount-point")
        .arg(&req.mount_point)
        .arg("--collection")
        .arg(&req.collection)
        .arg("--replication")
        .arg(&req.replication)
        .arg("--threads")
        .arg(req.threads.to_string())
        .spawn();

    match cmd {
        Ok(mut child) => {
            let pid = child.id();

            let mount = FuseMount {
                id: mount_id,
                mount_point: req.mount_point,
                collection: req.collection,
                replication: req.replication,
                master: req.master,
                threads: req.threads,
                status: "mounted".to_string(),
                mounted_at: chrono::Utc::now().to_rfc3339(),
                pid,
            };

            state.fuse_mounts.lock().await.push(mount.clone());

            tokio::spawn(async move {
                let _ = child.wait().await;
            });

            Json(ApiResponse::success(mount))
        }
        Err(e) => Json(ApiResponse::error(&format!(
            "Failed to start FUSE mount: {}",
            e
        ))),
    }
}

async fn delete_fuse_mount(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<ApiResponse<()>> {
    let mut mounts = state.fuse_mounts.lock().await;

    if let Some(index) = mounts.iter().position(|m| m.id == id) {
        let mount = mounts.remove(index);

        if let Some(pid) = mount.pid {
            if let Ok(mut child) = tokio::process::Command::new("umount")
                .arg(&mount.mount_point)
                .spawn()
            {
                let _ = child.wait().await;
            }

            if let Ok(mut child) = tokio::process::Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .spawn()
            {
                let _ = child.wait().await;
            }
        }

        Json(ApiResponse::success(()))
    } else {
        Json(ApiResponse::error("Mount not found"))
    }
}

fn parse_list_buckets_xml(xml: &str) -> Vec<BucketInfo> {
    let mut buckets = Vec::new();
    let re = regex::Regex::new(
        r"<Bucket>\s*<Name>([^<]+)</Name>\s*<CreationDate>([^<]+)</CreationDate>\s*</Bucket>",
    )
    .unwrap();

    for cap in re.captures_iter(xml) {
        buckets.push(BucketInfo {
            name: cap[1].to_string(),
            creation_date: cap[2].to_string(),
            object_count: 0,
            total_size: 0,
        });
    }
    buckets
}

fn parse_list_objects_xml(xml: &str) -> Vec<ObjectInfo> {
    let mut objects = Vec::new();
    let re = regex::Regex::new(r"<Contents>\s*<Key>([^<]+)</Key>\s*<Size>([^<]+)</Size>\s*<LastModified>([^<]+)</LastModified>\s*</Contents>").unwrap();

    for cap in re.captures_iter(xml) {
        let size: u64 = cap[2].parse().unwrap_or(0);
        objects.push(ObjectInfo {
            key: cap[1].to_string(),
            etag: "".to_string(),
            size,
            last_modified: cap[3].to_string(),
            storage_class: "STANDARD".to_string(),
        });
    }
    objects
}

async fn get_metric_history(
    State(_state): State<Arc<AppState>>,
    Path(metric): Path<String>,
) -> Json<ApiResponse<Vec<TimeSeriesPoint>>> {
    let mut data = Vec::new();
    let now = chrono::Utc::now();
    for i in (0..24).rev() {
        let time = now - chrono::Duration::hours(i);
        let base_value = match metric.as_str() {
            "powerfs_node_disk_usage" => 65.0,
            "powerfs_node_cpu_usage" => 45.0,
            "powerfs_kv_hit_ratio" => 90.0,
            "powerfs_kv_memory_used" => 50.0,
            _ => 50.0,
        };
        let value = base_value + (rand::random::<f64>() - 0.5) * 20.0;
        data.push(TimeSeriesPoint {
            time: time.to_rfc3339(),
            value,
        });
    }
    Json(ApiResponse::success(data))
}

async fn get_alerts(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<AlertInfo>>> {
    let alerts = state.alert_engine.get_alerts().await;
    Json(ApiResponse::success(alerts))
}

async fn get_alert(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<ApiResponse<AlertInfo>> {
    match state.alert_engine.get_alert(&id).await {
        Some(alert) => Json(ApiResponse::success(alert)),
        None => Json(ApiResponse::error("Alert not found")),
    }
}

async fn acknowledge_alert(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<ApiResponse<()>> {
    state.alert_engine.acknowledge_alert(&id).await;
    Json(ApiResponse::success(()))
}

async fn get_alert_rules(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<AlertRule>>> {
    let rules = state.alert_engine.get_rules().await;
    Json(ApiResponse::success(rules))
}

async fn add_alert_rule(
    State(state): State<Arc<AppState>>,
    Json(rule): Json<AlertRule>,
) -> Json<ApiResponse<()>> {
    state.alert_engine.add_rule(rule).await;
    Json(ApiResponse::success(()))
}

async fn update_alert_rule(
    State(state): State<Arc<AppState>>,
    Path(_id): Path<String>,
    Json(rule): Json<AlertRule>,
) -> Json<ApiResponse<()>> {
    state.alert_engine.update_rule(rule).await;
    Json(ApiResponse::success(()))
}

async fn delete_alert_rule(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<ApiResponse<()>> {
    state.alert_engine.remove_rule(&id).await;
    Json(ApiResponse::success(()))
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
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

async fn broadcast_message(state: Arc<AppState>, message: serde_json::Value) {
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
        s3_endpoint: args.s3_endpoint,
        s3_backend_endpoint: args.s3_backend_endpoint,
        fuse_mounts: Arc::new(Mutex::new(Vec::new())),
    });

    let event_bus = EventBus::new(&args.redis_url, &args.stream_key);

    tokio::spawn(start_event_processor(
        event_bus,
        metric_store.clone(),
        alert_engine.clone(),
        app_state.clone(),
    ));

    tokio::spawn(start_alert_evaluator(
        alert_engine.clone(),
        app_state.clone(),
    ));

    tokio::spawn(start_metric_broadcaster(
        metric_store.clone(),
        app_state.clone(),
    ));

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
        .route("/api/metrics/history/:metric", get(get_metric_history))
        .route("/api/metrics/s3", get(get_s3_metrics))
        .route("/api/s3/buckets", get(get_buckets))
        .route("/api/s3/buckets/:name", get(get_bucket))
        .route("/api/s3/buckets", post(create_bucket))
        .route("/api/s3/buckets/:name", delete(delete_bucket))
        .route("/api/s3/buckets/:bucket/objects", get(get_objects))
        .route("/api/s3/buckets/:bucket/objects", post(upload_object))
        .route(
            "/api/s3/buckets/:bucket/objects/:key",
            delete(delete_object),
        )
        .route(
            "/api/s3/buckets/:bucket/objects/:key/download",
            get(download_object),
        )
        .route("/api/s3/multipart-uploads", get(get_multipart_uploads))
        .route("/api/s3/keys", get(get_s3_access_keys))
        .route("/api/s3/keys", post(create_s3_access_key))
        .route("/api/s3/keys/:access_key", delete(delete_s3_access_key))
        .route("/api/fuse/mounts", get(get_fuse_mounts))
        .route("/api/fuse/mounts", post(create_fuse_mount))
        .route("/api/fuse/mounts/:id", delete(delete_fuse_mount))
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
    _alert_engine: Arc<AlertEngine>,
    app_state: Arc<AppState>,
) {
    info!("Event processor started");

    match event_bus.read_history().await {
        Ok(events) => {
            info!("Loaded {} historical events", events.len());
            for event in events {
                match &event.event {
                    Event::NodeStatus(e) => {
                        metric_store.update_node(e.clone()).await;
                    }
                    Event::VolumeStatus(e) => {
                        metric_store.update_volume(e.clone()).await;
                    }
                    Event::KVSession(e) => {
                        metric_store.update_kv_session(e.clone()).await;
                    }
                    _ => {}
                }
            }
        }
        Err(e) => {
            warn!("Failed to load historical events: {}", e);
        }
    }

    let mut stream = event_bus.subscribe().await;

    loop {
        match stream.read().await {
            Ok(events) => {
                for event in events {
                    match &event.event {
                        Event::NodeStatus(e) => {
                            metric_store.update_node(e.clone()).await;
                            let node_info = metric_store.get_node(&e.node_id).await;
                            if let Some(node) = node_info {
                                let msg = WsMetricUpdate {
                                    message_type: "metric_update".to_string(),
                                    source: "nodes".to_string(),
                                    payload: serde_json::to_value(node).unwrap(),
                                };
                                broadcast_message(
                                    app_state.clone(),
                                    serde_json::to_value(msg).unwrap(),
                                )
                                .await;
                            }
                        }
                        Event::VolumeStatus(e) => {
                            metric_store.update_volume(e.clone()).await;
                            let volume_info = metric_store.get_volume(e.volume_id).await;
                            if let Some(volume) = volume_info {
                                let msg = WsMetricUpdate {
                                    message_type: "metric_update".to_string(),
                                    source: "volumes".to_string(),
                                    payload: serde_json::to_value(volume).unwrap(),
                                };
                                broadcast_message(
                                    app_state.clone(),
                                    serde_json::to_value(msg).unwrap(),
                                )
                                .await;
                            }
                        }
                        Event::KVSession(e) => {
                            metric_store.update_kv_session(e.clone()).await;
                            let kv_metrics = metric_store.get_kv_metrics().await;
                            let msg = WsMetricUpdate {
                                message_type: "metric_update".to_string(),
                                source: "kv".to_string(),
                                payload: serde_json::to_value(kv_metrics).unwrap(),
                            };
                            broadcast_message(
                                app_state.clone(),
                                serde_json::to_value(msg).unwrap(),
                            )
                            .await;
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
                            let msg = WsAlertUpdate {
                                message_type: "alert_trigger".to_string(),
                                payload: serde_json::to_value(e).unwrap(),
                            };
                            broadcast_message(
                                app_state.clone(),
                                serde_json::to_value(msg).unwrap(),
                            )
                            .await;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Error reading events: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn start_alert_evaluator(alert_engine: Arc<AlertEngine>, app_state: Arc<AppState>) {
    info!("Alert evaluator started");

    loop {
        let alerts = alert_engine.evaluate_rules().await;
        for alert in alerts {
            info!("Alert triggered: {}", alert.name);
            let msg = WsAlertUpdate {
                message_type: "alert_trigger".to_string(),
                payload: serde_json::to_value(alert).unwrap(),
            };
            broadcast_message(app_state.clone(), serde_json::to_value(msg).unwrap()).await;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    }
}

async fn start_metric_broadcaster(metric_store: Arc<MetricStore>, app_state: Arc<AppState>) {
    info!("Metric broadcaster started");

    loop {
        let cluster_metrics = metric_store.get_cluster_metrics().await;
        let cluster_msg = WsMetricUpdate {
            message_type: "metric_update".to_string(),
            source: "cluster".to_string(),
            payload: serde_json::to_value(cluster_metrics).unwrap(),
        };
        broadcast_message(
            app_state.clone(),
            serde_json::to_value(cluster_msg).unwrap(),
        )
        .await;

        let kv_metrics = metric_store.get_kv_metrics().await;
        let kv_msg = WsMetricUpdate {
            message_type: "metric_update".to_string(),
            source: "kv".to_string(),
            payload: serde_json::to_value(kv_metrics).unwrap(),
        };
        broadcast_message(app_state.clone(), serde_json::to_value(kv_msg).unwrap()).await;

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
