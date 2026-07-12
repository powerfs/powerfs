use std::io::Write;
use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Extension, Json, Path, Query, State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Router, Server,
};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

use powerfs_kv_client::KvCacheClient;
use powerfs_monitor::alert_engine::AlertEngine;
use powerfs_monitor::auth::{
    auth_middleware, generate_access_key, generate_secret_key, hash_secret_key, AuthState,
    CurrentUser, JwtValidator, RateLimiter, ResourceOwner, ResourceOwnerStore, ResourceType, Role,
    RoleStore, S3AccessKey, S3AccessKeyInfo, S3AccessKeyStore, UserRole, UserStatus, UserStore,
};
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

    #[arg(long, default_value = "powerfs")]
    s3_access_key: String,

    #[arg(long, default_value = "powerfs123")]
    s3_secret_key: String,

    #[arg(long, default_value = "/data/master/auth.db")]
    auth_db_path: String,

    #[arg(long, default_value = "powerfs-secret-key-change-in-production")]
    jwt_secret: String,

    #[arg(long, default_value = "powerfs-hmac-secret-change-in-production")]
    hmac_secret: String,

    #[arg(long, default_value = "admin")]
    admin_username: String,

    #[arg(long, default_value = "admin123")]
    admin_password: String,

    #[arg(long, default_value = "localhost:9333")]
    master_endpoint: String,

    #[arg(long)]
    log_file: Option<String>,

    #[arg(long, default_value = "10")]
    log_max_size_mb: u64,

    #[arg(long, default_value = "5")]
    log_max_files: usize,

    #[arg(long, default_value = "info")]
    log_level: String,
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
    #[allow(dead_code)]
    s3_backend_endpoint: String,
    s3_access_key: String,
    s3_secret_key: String,
    fuse_mounts: Arc<Mutex<Vec<FuseMount>>>,
    auth: Arc<AuthState>,
    /// 资源归属存储（与 UserStore 共享 auth.db）
    resource_owners: Arc<ResourceOwnerStore>,
    /// 角色存储（与 UserStore 共享 auth.db）
    roles: Arc<RoleStore>,
    /// S3 AccessKey 存储（与 UserStore 共享 auth.db）
    s3_keys: Arc<S3AccessKeyStore>,
    /// 用于 HMAC-SHA256 哈希 secret_key 的密钥
    hmac_secret: String,
    /// 登录速率限制器
    rate_limiter: Arc<RateLimiter>,
    /// KV Cache 客户端
    kv_client: Arc<Mutex<KvCacheClient>>,
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

fn s3_auth_headers(access_key: &str, secret_key: &str) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "Authorization",
        format!("AWS {}:{}", access_key, secret_key)
            .parse()
            .unwrap(),
    );
    headers
}

async fn get_s3_metrics(State(state): State<Arc<AppState>>) -> Json<ApiResponse<S3Metrics>> {
    let client = reqwest::Client::new();
    let url = format!("{}/", state.s3_endpoint);

    match client
        .get(&url)
        .headers(s3_auth_headers(&state.s3_access_key, &state.s3_secret_key))
        .send()
        .await
    {
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

async fn get_buckets(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
) -> Json<ApiResponse<Vec<BucketInfo>>> {
    let client = reqwest::Client::new();
    let url = format!("{}/", state.s3_endpoint);

    match client
        .get(&url)
        .headers(s3_auth_headers(&state.s3_access_key, &state.s3_secret_key))
        .send()
        .await
    {
        Ok(response) => {
            if response.status().is_success() {
                if let Ok(body) = response.text().await {
                    let mut buckets = parse_list_buckets_xml(&body);
                    // 非 admin 用户仅可见自己拥有的 bucket
                    if !user.is_admin() {
                        let owned = state
                            .resource_owners
                            .list_user_resources(&user.id, Some(&ResourceType::S3Bucket))
                            .unwrap_or_default();
                        let owned_ids: std::collections::HashSet<String> =
                            owned.into_iter().map(|o| o.resource_id).collect();
                        buckets.retain(|b| owned_ids.contains(&b.name));
                    }
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

    match client
        .get(&url)
        .headers(s3_auth_headers(&state.s3_access_key, &state.s3_secret_key))
        .send()
        .await
    {
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
    Extension(user): Extension<CurrentUser>,
    Json(req): Json<CreateBucketRequest>,
) -> Json<ApiResponse<()>> {
    let client = reqwest::Client::new();
    let url = format!("{}/{}", state.s3_endpoint, req.name);

    match client
        .put(&url)
        .headers(s3_auth_headers(&state.s3_access_key, &state.s3_secret_key))
        .send()
        .await
    {
        Ok(response) => {
            if response.status().is_success() {
                // 记录 bucket 归属权
                let owner = ResourceOwner::new(
                    &user.id,
                    ResourceType::S3Bucket,
                    &req.name,
                    vec![
                        "read".to_string(),
                        "write".to_string(),
                        "delete".to_string(),
                    ],
                );
                if let Err(e) = state.resource_owners.set_owner(&owner) {
                    warn!("Failed to record bucket owner: {}", e);
                }
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
    Extension(user): Extension<CurrentUser>,
    Path(name): Path<String>,
) -> Response {
    // 非 admin 用户只能删除自己的 bucket
    if !user.is_admin() {
        match state
            .resource_owners
            .is_owner(&user.id, &ResourceType::S3Bucket, &name)
        {
            Ok(true) => {}
            _ => {
                return (
                    StatusCode::FORBIDDEN,
                    Json::<ApiResponse<()>>(ApiResponse::error("Not bucket owner")),
                )
                    .into_response();
            }
        }
    }

    let client = reqwest::Client::new();
    let url = format!("{}/{}", state.s3_endpoint, name);

    match client
        .delete(&url)
        .headers(s3_auth_headers(&state.s3_access_key, &state.s3_secret_key))
        .send()
        .await
    {
        Ok(response) => {
            if response.status().is_success() {
                // 删除归属记录（忽略错误，bucket 已删）
                let _ = state
                    .resource_owners
                    .delete_owner(&ResourceType::S3Bucket, &name);
                Json::<ApiResponse<()>>(ApiResponse::success(())).into_response()
            } else {
                Json::<ApiResponse<()>>(ApiResponse::error("Failed to delete bucket"))
                    .into_response()
            }
        }
        Err(e) => {
            warn!("S3 connection error: {}", e);
            Json::<ApiResponse<()>>(ApiResponse::error("S3 connection error")).into_response()
        }
    }
}

async fn get_objects(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> Json<ApiResponse<Vec<ObjectInfo>>> {
    let client = reqwest::Client::new();
    let url = format!("{}/{}", state.s3_endpoint, bucket);

    match client
        .get(&url)
        .headers(s3_auth_headers(&state.s3_access_key, &state.s3_secret_key))
        .send()
        .await
    {
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

        match client
            .delete(&url)
            .headers(s3_auth_headers(&state.s3_access_key, &state.s3_secret_key))
            .send()
            .await
        {
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

        match client
            .delete(&url)
            .headers(s3_auth_headers(&state.s3_access_key, &state.s3_secret_key))
            .send()
            .await
        {
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

    match client
        .put(&url)
        .headers(s3_auth_headers(&state.s3_access_key, &state.s3_secret_key))
        .body(data)
        .send()
        .await
    {
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

    match client
        .get(&url)
        .headers(s3_auth_headers(&state.s3_access_key, &state.s3_secret_key))
        .send()
        .await
    {
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

#[derive(Debug, Serialize)]
struct CreatedKeyInfo {
    #[serde(flatten)]
    info: S3AccessKeyInfo,
    /// 创建时返回明文 secret_key（仅此一次）
    secret_key: String,
}

async fn get_s3_access_keys(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
) -> Json<ApiResponse<Vec<S3AccessKeyInfo>>> {
    // 普通用户只能查看自己的 AccessKey
    match state.s3_keys.list_user_keys(&user.id) {
        Ok(keys) => {
            let infos: Vec<S3AccessKeyInfo> = keys.iter().map(S3AccessKeyInfo::from).collect();
            Json(ApiResponse::success(infos))
        }
        Err(e) => Json(ApiResponse::error(&e)),
    }
}

async fn create_s3_access_key(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
) -> impl IntoResponse {
    // 生成新的 AccessKey/SecretKey 对
    let access_key = generate_access_key();
    let secret_key = generate_secret_key();
    let secret_hash = hash_secret_key(&secret_key, &state.hmac_secret);

    let key = S3AccessKey::new(&user.id, &access_key, &secret_hash);
    match state.s3_keys.create_key(&key) {
        Ok(()) => {
            let info = CreatedKeyInfo {
                info: S3AccessKeyInfo::from(&key),
                secret_key,
            };
            Json(ApiResponse::success(info)).into_response()
        }
        Err(e) => Json::<ApiResponse<CreatedKeyInfo>>(ApiResponse::error(&e)).into_response(),
    }
}

async fn delete_s3_access_key(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    // 检查归属：用户只能删除自己的 key，admin 可删除任意
    let key = match state.s3_keys.get_key_by_id(&id) {
        Ok(Some(k)) => k,
        Ok(None) => {
            return Json::<ApiResponse<()>>(ApiResponse::error("AccessKey not found"))
                .into_response();
        }
        Err(e) => return Json::<ApiResponse<()>>(ApiResponse::error(&e)).into_response(),
    };

    if !user.is_admin() && key.user_id != user.id {
        return (
            StatusCode::FORBIDDEN,
            Json::<ApiResponse<()>>(ApiResponse::error("Cannot delete other user's key")),
        )
            .into_response();
    }

    match state.s3_keys.delete_key(&id) {
        Ok(true) => Json(ApiResponse::success(())).into_response(),
        Ok(false) => {
            Json::<ApiResponse<()>>(ApiResponse::error("AccessKey not found")).into_response()
        }
        Err(e) => Json::<ApiResponse<()>>(ApiResponse::error(&e)).into_response(),
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

async fn get_alerts(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
) -> Json<ApiResponse<Vec<AlertInfo>>> {
    let mut alerts = state.alert_engine.get_alerts().await;
    // 非 admin 用户仅可见归属自己的告警；系统级告警（owner_id=None）仅 admin 可见
    if !user.is_admin() {
        alerts.retain(|a| a.owner_id.as_deref() == Some(&user.id));
    }
    Json(ApiResponse::success(alerts))
}

async fn get_alert(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Json<ApiResponse<AlertInfo>> {
    match state.alert_engine.get_alert(&id).await {
        Some(alert) => {
            // 非 admin 用户只能查看归属自己的告警
            if !user.is_admin() && alert.owner_id.as_deref() != Some(&user.id) {
                return Json(ApiResponse::error("Forbidden"));
            }
            Json(ApiResponse::success(alert))
        }
        None => Json(ApiResponse::error("Alert not found")),
    }
}

async fn acknowledge_alert(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Response {
    // 非 admin 用户只能确认归属自己的告警
    if !user.is_admin() {
        match state.alert_engine.get_alert(&id).await {
            Some(alert) => {
                if alert.owner_id.as_deref() != Some(&user.id) {
                    return (
                        StatusCode::FORBIDDEN,
                        Json::<ApiResponse<()>>(ApiResponse::error("Forbidden")),
                    )
                        .into_response();
                }
            }
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json::<ApiResponse<()>>(ApiResponse::error("Alert not found")),
                )
                    .into_response();
            }
        }
    }
    state.alert_engine.acknowledge_alert(&id).await;
    Json::<ApiResponse<()>>(ApiResponse::success(())).into_response()
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

// ===== Auth API =====

#[derive(Debug, Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Debug, Serialize)]
struct LoginResponse {
    token: String,
    refresh_token: String,
    expires_in: u64,
    user: UserInfo,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserInfo {
    id: String,
    username: String,
    role: String,
    status: String,
    email: Option<String>,
    phone: Option<String>,
    created_at: String,
}

impl From<&powerfs_monitor::auth::User> for UserInfo {
    fn from(u: &powerfs_monitor::auth::User) -> Self {
        Self {
            id: u.id.clone(),
            username: u.username.clone(),
            role: u.role.to_string(),
            status: format!("{:?}", u.status).to_lowercase(),
            email: u.email.clone(),
            phone: u.phone.clone(),
            created_at: u.created_at.to_rfc3339(),
        }
    }
}

async fn login(
    State(state): State<Arc<AppState>>,
    Json(login_req): Json<LoginRequest>,
) -> impl IntoResponse {
    let client_ip = "127.0.0.1";

    if !state
        .rate_limiter
        .check_login(client_ip, &login_req.username)
        .await
        .unwrap_or(false)
    {
        return Json(ApiResponse::<LoginResponse>::error(
            "Too many login attempts, please try again later",
        ));
    }

    let auth_state = &state.auth;
    let user = match auth_state
        .user_store
        .get_user_by_username(&login_req.username)
    {
        Ok(Some(u)) => u,
        _ => {
            return Json(ApiResponse::<LoginResponse>::error(
                "Invalid username or password",
            ));
        }
    };

    if !user.is_active() {
        return Json(ApiResponse::<LoginResponse>::error(
            "Account is disabled or locked",
        ));
    }

    if !auth_state
        .user_store
        .verify_password(&user, &login_req.password)
    {
        return Json(ApiResponse::<LoginResponse>::error(
            "Invalid username or password",
        ));
    }

    let tokens =
        auth_state
            .validator
            .generate_token_pair(&user.id, &user.username, &user.role.to_string());

    Json(ApiResponse::success(LoginResponse {
        token: tokens.access_token,
        refresh_token: tokens.refresh_token,
        expires_in: tokens.expires_in,
        user: UserInfo::from(&user),
    }))
}

#[derive(Debug, Deserialize)]
struct RefreshRequest {
    refresh_token: String,
}

async fn refresh_token(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RefreshRequest>,
) -> impl IntoResponse {
    let auth_state = &state.auth;
    match auth_state
        .validator
        .refresh_access_token(&req.refresh_token)
    {
        Ok(tokens) => {
            // Get latest user info
            let claims = auth_state
                .validator
                .validate_refresh_token(&req.refresh_token)
                .ok();
            let user = if let Some(c) = &claims {
                auth_state.user_store.get_user_by_id(&c.sub).ok().flatten()
            } else {
                None
            };

            if let Some(u) = user {
                Json(ApiResponse::success(LoginResponse {
                    token: tokens.access_token,
                    refresh_token: tokens.refresh_token,
                    expires_in: tokens.expires_in,
                    user: UserInfo::from(&u),
                }))
            } else {
                Json(ApiResponse::<LoginResponse>::error("User not found"))
            }
        }
        Err(e) => Json(ApiResponse::<LoginResponse>::error(&e)),
    }
}

async fn get_current_user(
    Extension(user): Extension<CurrentUser>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let auth_state = &state.auth;
    match auth_state.user_store.get_user_by_id(&user.id) {
        Ok(Some(u)) => Json(ApiResponse::success(UserInfo::from(&u))),
        _ => Json(ApiResponse::<UserInfo>::error("User not found")),
    }
}

#[derive(Debug, Deserialize)]
struct CreateUserRequest {
    username: String,
    password: String,
    role: Option<String>,
    email: Option<String>,
    phone: Option<String>,
}

async fn list_users(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
) -> impl IntoResponse {
    if !user.is_admin() {
        return (
            StatusCode::FORBIDDEN,
            Json::<ApiResponse<Vec<UserInfo>>>(ApiResponse::error("Forbidden")),
        )
            .into_response();
    }
    let auth_state = &state.auth;
    match auth_state.user_store.list_users() {
        Ok(users) => {
            let users: Vec<UserInfo> = users.iter().map(UserInfo::from).collect();
            Json(ApiResponse::success(users)).into_response()
        }
        Err(e) => Json::<ApiResponse<Vec<UserInfo>>>(ApiResponse::error(&e)).into_response(),
    }
}

async fn create_user(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
    Json(req): Json<CreateUserRequest>,
) -> impl IntoResponse {
    if !user.is_admin() {
        return (
            StatusCode::FORBIDDEN,
            Json::<ApiResponse<UserInfo>>(ApiResponse::error("Forbidden")),
        )
            .into_response();
    }

    let role = req
        .role
        .as_deref()
        .map(|r| r.parse::<UserRole>().unwrap_or(UserRole::User))
        .unwrap_or(UserRole::User);

    let auth_state = &state.auth;
    match auth_state
        .user_store
        .create_user(&req.username, &req.password, role)
    {
        Ok(mut u) => {
            if req.email.is_some() || req.phone.is_some() {
                u = auth_state
                    .user_store
                    .update_user(&u.id, req.email.clone(), req.phone.clone(), None, None)
                    .unwrap_or(u);
            }
            Json(ApiResponse::success(UserInfo::from(&u))).into_response()
        }
        Err(e) => Json::<ApiResponse<UserInfo>>(ApiResponse::error(&e)).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct UpdateUserRequest {
    email: Option<String>,
    phone: Option<String>,
    status: Option<String>,
    role: Option<String>,
    password: Option<String>,
}

async fn update_user(
    State(state): State<Arc<AppState>>,
    Extension(current): Extension<CurrentUser>,
    Path(id): Path<String>,
    Json(req): Json<UpdateUserRequest>,
) -> impl IntoResponse {
    // Admin can update anyone; users can only update themselves
    if !current.is_admin() && current.id != id {
        return (
            StatusCode::FORBIDDEN,
            Json::<ApiResponse<UserInfo>>(ApiResponse::error("Forbidden")),
        )
            .into_response();
    }

    // 仅管理员可修改角色和状态
    let status = if current.is_admin() {
        req.status.as_deref().and_then(|s| match s {
            "active" => Some(UserStatus::Active),
            "inactive" => Some(UserStatus::Inactive),
            "locked" => Some(UserStatus::Locked),
            _ => None,
        })
    } else {
        None
    };

    let role = if current.is_admin() {
        req.role.as_deref().and_then(|r| match r {
            "admin" => Some(UserRole::Admin),
            "user" => Some(UserRole::User),
            _ => None,
        })
    } else {
        None
    };

    let auth_state = &state.auth;

    if let Some(pwd) = req.password {
        if let Err(e) = auth_state.user_store.update_password(&id, &pwd) {
            return Json::<ApiResponse<UserInfo>>(ApiResponse::error(&e)).into_response();
        }
    }

    match auth_state
        .user_store
        .update_user(&id, req.email, req.phone, status, role)
    {
        Ok(u) => Json(ApiResponse::success(UserInfo::from(&u))).into_response(),
        Err(e) => Json::<ApiResponse<UserInfo>>(ApiResponse::error(&e)).into_response(),
    }
}

async fn delete_user(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if !user.is_admin() {
        return (
            StatusCode::FORBIDDEN,
            Json::<ApiResponse<()>>(ApiResponse::error("Forbidden")),
        )
            .into_response();
    }
    if user.id == id {
        return (
            StatusCode::BAD_REQUEST,
            Json::<ApiResponse<()>>(ApiResponse::error("Cannot delete yourself")),
        )
            .into_response();
    }

    let auth_state = &state.auth;
    match auth_state.user_store.delete_user(&id) {
        Ok(true) => {
            let _ = state.s3_keys.clear_user_keys(&id);
            let _ = state.resource_owners.clear_user_resources(&id);
            Json(ApiResponse::success(())).into_response()
        }
        Ok(false) => Json::<ApiResponse<()>>(ApiResponse::error("User not found")).into_response(),
        Err(e) => Json::<ApiResponse<()>>(ApiResponse::error(&e)).into_response(),
    }
}

// ===== 角色管理 API =====

#[derive(Debug, Serialize)]
struct RoleInfo {
    id: String,
    name: String,
    description: String,
    permissions: Vec<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<&Role> for RoleInfo {
    fn from(r: &Role) -> Self {
        Self {
            id: r.id.clone(),
            name: r.name.clone(),
            description: r.description.clone(),
            permissions: r.permissions.clone(),
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateRoleRequest {
    name: String,
    description: Option<String>,
    permissions: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct UpdateRoleRequest {
    name: Option<String>,
    description: Option<String>,
    permissions: Option<Vec<String>>,
}

async fn list_roles(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
) -> impl IntoResponse {
    if !user.is_admin() {
        return (
            StatusCode::FORBIDDEN,
            Json::<ApiResponse<Vec<RoleInfo>>>(ApiResponse::error("Forbidden")),
        )
            .into_response();
    }
    match state.roles.list_roles() {
        Ok(roles) => Json(ApiResponse::success(
            roles.iter().map(RoleInfo::from).collect::<Vec<_>>(),
        ))
        .into_response(),
        Err(e) => Json::<ApiResponse<Vec<RoleInfo>>>(ApiResponse::error(&e)).into_response(),
    }
}

async fn get_role(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if !user.is_admin() {
        return (
            StatusCode::FORBIDDEN,
            Json::<ApiResponse<RoleInfo>>(ApiResponse::error("Forbidden")),
        )
            .into_response();
    }
    match state.roles.get_role_by_id(&id) {
        Ok(Some(role)) => Json(ApiResponse::success(RoleInfo::from(&role))).into_response(),
        Ok(None) => {
            Json::<ApiResponse<RoleInfo>>(ApiResponse::error("Role not found")).into_response()
        }
        Err(e) => Json::<ApiResponse<RoleInfo>>(ApiResponse::error(&e)).into_response(),
    }
}

async fn create_role(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
    Json(req): Json<CreateRoleRequest>,
) -> impl IntoResponse {
    if !user.is_admin() {
        return (
            StatusCode::FORBIDDEN,
            Json::<ApiResponse<RoleInfo>>(ApiResponse::error("Forbidden")),
        )
            .into_response();
    }
    match state.roles.create_role(
        &req.name,
        req.description.as_deref().unwrap_or(""),
        req.permissions,
    ) {
        Ok(role) => Json(ApiResponse::success(RoleInfo::from(&role))).into_response(),
        Err(e) => Json::<ApiResponse<RoleInfo>>(ApiResponse::error(&e)).into_response(),
    }
}

async fn update_role(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
    Json(req): Json<UpdateRoleRequest>,
) -> impl IntoResponse {
    if !user.is_admin() {
        return (
            StatusCode::FORBIDDEN,
            Json::<ApiResponse<RoleInfo>>(ApiResponse::error("Forbidden")),
        )
            .into_response();
    }
    match state
        .roles
        .update_role(&id, req.name, req.description, req.permissions)
    {
        Ok(role) => Json(ApiResponse::success(RoleInfo::from(&role))).into_response(),
        Err(e) => Json::<ApiResponse<RoleInfo>>(ApiResponse::error(&e)).into_response(),
    }
}

async fn delete_role(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if !user.is_admin() {
        return (
            StatusCode::FORBIDDEN,
            Json::<ApiResponse<()>>(ApiResponse::error("Forbidden")),
        )
            .into_response();
    }
    match state.roles.delete_role(&id) {
        Ok(true) => Json(ApiResponse::success(())).into_response(),
        Ok(false) => Json::<ApiResponse<()>>(ApiResponse::error("Role not found")).into_response(),
        Err(e) => Json::<ApiResponse<()>>(ApiResponse::error(&e)).into_response(),
    }
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

#[derive(Debug, Deserialize)]
struct CreateKVNamespaceRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
struct CreateKVKeyRequest {
    key: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct KVNamespace {
    id: String,
    name: String,
    owner_id: String,
    created_at: u64,
    updated_at: u64,
}

async fn list_kv_namespaces(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut client = state.kv_client.lock().await;
    match client.list_namespaces().await {
        Ok(namespaces) => {
            let converted: Vec<KVNamespace> = namespaces
                .into_iter()
                .map(|ns| KVNamespace {
                    id: ns.id,
                    name: ns.name,
                    owner_id: ns.owner_id,
                    created_at: ns.created_at,
                    updated_at: ns.updated_at,
                })
                .collect();
            Json(ApiResponse::success(converted))
        }
        Err(e) => {
            warn!("Failed to list KV namespaces: {}", e);
            Json(ApiResponse::error("Failed to list namespaces"))
        }
    }
}

async fn create_kv_namespace(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateKVNamespaceRequest>,
) -> impl IntoResponse {
    let mut client = state.kv_client.lock().await;
    match client.create_namespace(&req.name).await {
        Ok(_) => Json(ApiResponse::success(())),
        Err(e) => {
            warn!("Failed to create KV namespace: {}", e);
            Json(ApiResponse::error("Failed to create namespace"))
        }
    }
}

async fn delete_kv_namespace(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut client = state.kv_client.lock().await;
    match client.delete_namespace(&name).await {
        Ok(_) => Json(ApiResponse::success(())),
        Err(e) => {
            warn!("Failed to delete KV namespace: {}", e);
            Json(ApiResponse::error("Failed to delete namespace"))
        }
    }
}

async fn list_kv_keys(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut client = state.kv_client.lock().await;
    match client.list_keys(&name).await {
        Ok(keys) => Json(ApiResponse::success(keys)),
        Err(e) => {
            warn!("Failed to list KV keys: {}", e);
            Json(ApiResponse::error("Failed to list keys"))
        }
    }
}

async fn create_kv_key(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(req): Json<CreateKVKeyRequest>,
) -> impl IntoResponse {
    let mut client = state.kv_client.lock().await;
    match client.put_key(&name, &req.key, &req.value).await {
        Ok(_) => Json(ApiResponse::success(())),
        Err(e) => {
            warn!("Failed to create KV key: {}", e);
            Json(ApiResponse::error("Failed to create key"))
        }
    }
}

async fn get_kv_key(
    State(state): State<Arc<AppState>>,
    Path((name, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let mut client = state.kv_client.lock().await;
    match client.get_key(&name, &key).await {
        Ok(Some(value)) => Json(ApiResponse::success(value)),
        Ok(None) => Json(ApiResponse::error("Key not found")),
        Err(e) => {
            warn!("Failed to get KV key: {}", e);
            Json(ApiResponse::error("Failed to get key"))
        }
    }
}

async fn delete_kv_key(
    State(state): State<Arc<AppState>>,
    Path((name, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let mut client = state.kv_client.lock().await;
    match client.delete_key(&name, &key).await {
        Ok(_) => Json(ApiResponse::success(())),
        Err(e) => {
            warn!("Failed to delete KV key: {}", e);
            Json(ApiResponse::error("Failed to delete key"))
        }
    }
}

#[derive(Debug, Serialize)]
struct KVAccessKeyInfo {
    id: String,
    user_id: String,
    access_key: String,
    status: String,
    created_at: String,
    last_used_at: Option<String>,
}

async fn list_kv_access_keys(
    State(_state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
) -> impl IntoResponse {
    let _ = user;
    let keys = vec![KVAccessKeyInfo {
        id: "key-1".to_string(),
        user_id: "user-1".to_string(),
        access_key: "mock-kv-access-key".to_string(),
        status: "active".to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        last_used_at: None,
    }];
    Json(ApiResponse::success(keys))
}

async fn create_kv_access_key(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
) -> impl IntoResponse {
    let _ = user;
    let _ = state;
    let key_id = uuid::Uuid::new_v4().to_string();
    let access_key = format!("kv_{}", key_id.split('-').next().unwrap_or(""));
    let secret_key = uuid::Uuid::new_v4().to_string().replace('-', "");
    Json(ApiResponse::success(serde_json::json!({
        "id": key_id,
        "access_key": access_key,
        "secret_key": secret_key,
        "created_at": chrono::Utc::now().to_rfc3339(),
    })))
}

async fn delete_kv_access_key(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let _ = user;
    let _ = state;
    let _ = id;
    Json(ApiResponse::success(()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut builder = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(&args.log_level),
    );

    builder.format(|buf, record| {
        writeln!(
            buf,
            "[{}] [{}] [{}] {}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            record.level(),
            record.target(),
            record.args()
        )
    });

    if let Some(log_file) = &args.log_file {
        use std::fs::{self, File};
        use std::path::Path;

        let log_path = Path::new(log_file);
        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent).unwrap_or_else(|e| {
                eprintln!("Failed to create log directory: {}", e);
            });
        }

        let file = File::create(log_file).unwrap_or_else(|e| {
            eprintln!("Failed to create log file: {}", e);
            std::process::exit(1);
        });

        builder.target(env_logger::Target::Pipe(Box::new(file)));
        eprintln!("Logging to file: {}", log_file);
    }

    builder.init();

    info!("Starting PowerFS Monitor Service...");
    info!("Listening on: {}", args.addr);
    info!("Redis URL: {}", args.redis_url);

    // Initialize auth store
    let user_store = Arc::new(UserStore::new(&args.auth_db_path)?);
    user_store.ensure_admin_exists(&args.admin_username, &args.admin_password)?;
    let resource_owners = Arc::new(ResourceOwnerStore::from_user_store(&user_store));
    let roles = Arc::new(RoleStore::from_user_store(&user_store));
    roles.ensure_default_roles()?;
    let s3_keys = Arc::new(S3AccessKeyStore::from_user_store(&user_store));
    let jwt_validator = JwtValidator::new(&args.jwt_secret);

    let auth_state = Arc::new(AuthState {
        validator: jwt_validator,
        user_store: user_store.clone(),
    });

    let metric_store = Arc::new(MetricStore::new());
    let alert_engine = Arc::new(AlertEngine::new(metric_store.clone()));
    alert_engine.load_default_rules().await;

    let ws_clients = Arc::new(Mutex::new(Vec::new()));

    let kv_client = Arc::new(Mutex::new(
        KvCacheClient::connect(&args.master_endpoint).await?,
    ));

    let app_state = Arc::new(AppState {
        metric_store: metric_store.clone(),
        alert_engine: alert_engine.clone(),
        ws_clients,
        s3_endpoint: args.s3_endpoint,
        s3_backend_endpoint: args.s3_backend_endpoint,
        s3_access_key: args.s3_access_key,
        s3_secret_key: args.s3_secret_key,
        fuse_mounts: Arc::new(Mutex::new(Vec::new())),
        auth: auth_state.clone(),
        resource_owners: resource_owners.clone(),
        roles: roles.clone(),
        s3_keys: s3_keys.clone(),
        hmac_secret: args.hmac_secret.clone(),
        rate_limiter: Arc::new(RateLimiter::new(Arc::new(
            redis::Client::open(args.redis_url.clone())
                .expect("Failed to create Redis client for rate limiter"),
        ))),
        kv_client,
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

    // Public routes (no auth required)
    let public_routes = Router::new()
        .route("/api/auth/login", post(login))
        .route("/api/auth/refresh", post(refresh_token));

    // Authenticated routes - all routes under this layer require JWT
    let protected_routes = Router::new()
        .route("/api/auth/me", get(get_current_user))
        .route("/api/users", get(list_users))
        .route("/api/users", post(create_user))
        .route("/api/users/:id", put(update_user))
        .route("/api/users/:id", delete(delete_user))
        .route("/api/roles", get(list_roles))
        .route("/api/roles/:id", get(get_role))
        .route("/api/roles", post(create_role))
        .route("/api/roles/:id", put(update_role))
        .route("/api/roles/:id", delete(delete_role))
        .route("/api/metrics/cluster", get(get_cluster_metrics))
        .route("/api/metrics/nodes", get(get_nodes))
        .route("/api/metrics/nodes/:id", get(get_node))
        .route("/api/metrics/volumes", get(get_volumes))
        .route("/api/metrics/volumes/:id", get(get_volume))
        .route("/api/metrics/kv", get(get_kv_metrics))
        .route("/api/metrics/kv/sessions", get(get_kv_sessions))
        .route("/api/metrics/kv/sessions/:id", get(get_kv_session))
        .route("/api/kv/namespaces", get(list_kv_namespaces))
        .route("/api/kv/namespaces", post(create_kv_namespace))
        .route("/api/kv/namespaces/:name", delete(delete_kv_namespace))
        .route("/api/kv/namespaces/:name/keys", get(list_kv_keys))
        .route("/api/kv/namespaces/:name/keys", post(create_kv_key))
        .route("/api/kv/namespaces/:name/keys/:key", get(get_kv_key))
        .route("/api/kv/namespaces/:name/keys/:key", delete(delete_kv_key))
        .route("/api/kv/keys", get(list_kv_access_keys))
        .route("/api/kv/keys", post(create_kv_access_key))
        .route("/api/kv/keys/:id", delete(delete_kv_access_key))
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
        .route_layer(axum::middleware::from_fn_with_state(
            auth_state.clone(),
            auth_middleware,
        ));

    let app = Router::new()
        .merge(public_routes)
        .merge(protected_routes)
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
