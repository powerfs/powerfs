use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Json},
    routing::{delete, get, head, post, put},
    Router, Server,
};

/// S3 ListObjects (v1) / ListObjectsV2 query parameters.
/// Implements the subset of S3 semantics needed by PowerFS clients:
/// prefix filtering, max-keys truncation, delimiter-based common prefixes,
/// and v2 continuation-token pagination.
#[derive(Debug, Default, serde::Deserialize)]
pub struct ListObjectsParams {
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub delimiter: Option<String>,
    /// Echoed back; actual cap is [0, 1000].
    #[serde(default, rename = "max-keys")]
    pub max_keys: Option<i64>,
    /// v2: opaque token returned as NextContinuationToken.
    #[serde(default, rename = "continuation-token")]
    pub continuation_token: Option<String>,
    /// v2: exclusive lower bound on key (skips entries <= start_after).
    #[serde(default, rename = "start-after")]
    pub start_after: Option<String>,
    /// v2: "url" (only value S3 supports); controls key encoding in response.
    #[serde(default, rename = "encoding-type")]
    pub encoding_type: Option<String>,
    /// v2: presence with value "2" selects ListObjectsV2 response shape.
    #[serde(default, rename = "list-type")]
    pub list_type: Option<u8>,
}
use log::info;
use powerfs_common::error::PowerFsError;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use crate::bucket_manager::BucketManager;
use crate::entry_manager::EntryManager;
use crate::meta_shard_manager::{
    CrdtOverview, FilerStatus, MetaShardManager, OrsetStateDetail, ShardDetail,
};
use crate::metadata_store::MetadataStore;
use crate::raft_group_manager::ShardId;
use crate::s3_handler::S3Handler;
use crate::shard_scheduler::{SchedulerConfig, SchedulerStatus};
use crate::volume_router::VolumeRouter;

use crate::shard_scheduler::ShardScheduler;

pub struct FilerServer {
    s3_handler: Arc<S3Handler>,
    meta_shard_manager: Arc<MetaShardManager>,
    shard_scheduler: Arc<ShardScheduler>,
    addr: std::net::SocketAddr,
}

pub struct FilerState {
    s3_handler: Arc<S3Handler>,
    meta_shard_manager: Arc<MetaShardManager>,
    shard_scheduler: Arc<ShardScheduler>,
}

impl FilerServer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        addr: std::net::SocketAddr,
        _metadata_store: Arc<MetadataStore>,
        _bucket_manager: Arc<BucketManager>,
        _entry_manager: Arc<EntryManager>,
        _volume_router: Arc<VolumeRouter>,
        s3_handler: Arc<S3Handler>,
        meta_shard_manager: Arc<MetaShardManager>,
        shard_scheduler: Arc<ShardScheduler>,
    ) -> Self {
        Self {
            s3_handler,
            meta_shard_manager,
            shard_scheduler,
            addr,
        }
    }

    pub async fn serve(self) -> Result<(), PowerFsError> {
        let state = Arc::new(FilerState {
            s3_handler: self.s3_handler,
            meta_shard_manager: self.meta_shard_manager,
            shard_scheduler: self.shard_scheduler,
        });

        let router = Router::new()
            // Admin routes are declared as flat routes (not nested) so that
            // the more specific `/admin/...` paths win over the `/:bucket`
            // wildcard below. With `nest("/admin", ...)` axum 0.6 may match
            // `/admin/status` as `/:bucket` = "admin" and dispatch to
            // `bucket_handler`, which deadlocks on Raft propose.
            .route("/admin/status", get(admin_status))
            .route("/admin/shards", get(admin_list_shards))
            .route("/admin/shards/:id", get(admin_get_shard))
            .route("/admin/init-root", post(admin_init_root))
            // CRDT management routes
            .route("/admin/crdt/overview", get(admin_crdt_overview))
            .route("/admin/crdt/shards/:id", get(admin_crdt_shard_states))
            .route(
                "/admin/crdt/shards/:id/dirs/:dir_ino",
                get(admin_crdt_dir_state),
            )
            .route("/admin/crdt/cleanup", post(admin_crdt_cleanup))
            // Balancer routes
            .route("/admin/balancer/status", get(admin_balancer_status))
            .route("/admin/balancer/start", post(admin_balancer_start))
            .route("/admin/balancer/stop", post(admin_balancer_stop))
            .route("/admin/balancer/trigger", post(admin_balancer_trigger))
            .route("/admin/balancer/config", get(admin_balancer_get_config))
            .route("/admin/balancer/config", put(admin_balancer_set_config))
            // Admin bucket management (JSON API, proxied via Monitor — docs/filer-redesign-plan.md 决策 2)
            .route("/admin/buckets", get(admin_list_buckets))
            .route("/admin/buckets", post(admin_create_bucket))
            .route("/admin/buckets/:name", delete(admin_delete_bucket))
            .route("/admin/buckets/:name/quota", put(admin_set_bucket_quota))
            .route("/", get(list_buckets))
            .route("/:bucket", put(create_bucket))
            .route("/:bucket", delete(delete_bucket))
            .route("/:bucket", get(bucket_handler))
            .route("/:bucket", head(head_bucket))
            .route("/:bucket/*key", put(object_put_handler))
            .route("/:bucket/*key", get(object_get_handler))
            .route("/:bucket/*key", delete(object_delete_handler))
            .with_state(state);

        info!("Filer server starting on {}", self.addr);

        Server::bind(&self.addr)
            .serve(router.into_make_service())
            .await
            .map_err(|e| PowerFsError::Internal(e.to_string()))?;
        Ok(())
    }
}

async fn list_buckets(State(state): State<Arc<FilerState>>) -> axum::response::Response {
    state.s3_handler.list_buckets().await
}

async fn create_bucket(
    State(state): State<Arc<FilerState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> axum::response::Response {
    // Optional collection selection via the `x-powerfs-collection` header.
    // Missing/empty header falls back to the "default" collection.
    let collection = headers
        .get("x-powerfs-collection")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    state.s3_handler.create_bucket(&bucket, &collection).await
}

async fn delete_bucket(
    State(state): State<Arc<FilerState>>,
    Path(bucket): Path<String>,
) -> axum::response::Response {
    state.s3_handler.delete_bucket(&bucket).await
}

async fn head_bucket(
    State(state): State<Arc<FilerState>>,
    Path(bucket): Path<String>,
) -> axum::response::Response {
    state.s3_handler.head_bucket(&bucket).await
}

async fn bucket_handler(
    State(state): State<Arc<FilerState>>,
    Path(bucket): Path<String>,
    Query(params): Query<ListObjectsParams>,
) -> axum::response::Response {
    state.s3_handler.list_objects(&bucket, &params).await
}

async fn object_put_handler(
    State(state): State<Arc<FilerState>>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> axum::response::Response {
    state
        .s3_handler
        .put_object(&bucket, &key, body.as_ref())
        .await
}

async fn object_get_handler(
    State(state): State<Arc<FilerState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> axum::response::Response {
    state.s3_handler.get_object(&bucket, &key).await
}

async fn object_delete_handler(
    State(state): State<Arc<FilerState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> axum::response::Response {
    state.s3_handler.delete_object(&bucket, &key).await
}

async fn admin_status(State(state): State<Arc<FilerState>>) -> Json<FilerStatus> {
    let status = state.meta_shard_manager.get_filer_status().await;
    Json(status)
}

async fn admin_init_root(State(state): State<Arc<FilerState>>) -> axum::response::Response {
    match state.meta_shard_manager.format_posix_root().await {
        Ok(inode) => Json(serde_json::json!({
            "success": true,
            "inode": inode,
            "message": format!("POSIX root inode {} initialized", inode)
        }))
        .into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

async fn admin_list_shards(State(state): State<Arc<FilerState>>) -> Json<Vec<ShardDetail>> {
    let shards = state.meta_shard_manager.list_shards_detail().await;
    Json(shards)
}

async fn admin_get_shard(
    State(state): State<Arc<FilerState>>,
    Path(id): Path<u64>,
) -> axum::response::Response {
    match state.meta_shard_manager.get_shard_detail(ShardId(id)).await {
        Some(detail) => Json(detail).into_response(),
        None => (axum::http::StatusCode::NOT_FOUND, "Shard not found").into_response(),
    }
}

async fn admin_balancer_status(State(state): State<Arc<FilerState>>) -> Json<SchedulerStatus> {
    let status = state.shard_scheduler.get_status().await;
    Json(status)
}

async fn admin_balancer_start(State(state): State<Arc<FilerState>>) -> axum::response::Response {
    tokio::spawn({
        let scheduler = state.shard_scheduler.clone();
        async move {
            scheduler.run().await;
        }
    });
    (axum::http::StatusCode::OK, "Balancer started").into_response()
}

async fn admin_balancer_stop(State(state): State<Arc<FilerState>>) -> axum::response::Response {
    state.shard_scheduler.stop().await;
    (axum::http::StatusCode::OK, "Balancer stopped").into_response()
}

async fn admin_balancer_trigger(State(state): State<Arc<FilerState>>) -> axum::response::Response {
    tokio::spawn({
        let scheduler = state.shard_scheduler.clone();
        async move {
            scheduler.trigger_balance().await;
        }
    });
    (axum::http::StatusCode::OK, "Balance triggered").into_response()
}

async fn admin_balancer_get_config(State(state): State<Arc<FilerState>>) -> Json<SchedulerConfig> {
    let config = state.shard_scheduler.config.read().unwrap().clone();
    Json(config)
}

async fn admin_balancer_set_config(
    State(state): State<Arc<FilerState>>,
    Json(config): Json<SchedulerConfig>,
) -> axum::response::Response {
    state.shard_scheduler.set_config(config);
    (axum::http::StatusCode::OK, "Config updated").into_response()
}

// ========================================================================
// Admin bucket management (JSON API, proxied via Monitor bridge)
// 参见 docs/filer-redesign-plan.md 决策 2
// ========================================================================

async fn admin_list_buckets(State(state): State<Arc<FilerState>>) -> axum::response::Response {
    state.s3_handler.admin_list_buckets().await
}

#[derive(Debug, Deserialize)]
struct AdminCreateBucketBody {
    name: String,
    #[serde(default)]
    collection: Option<String>,
    #[serde(default)]
    size_limit: Option<u64>,
}

async fn admin_create_bucket(
    State(state): State<Arc<FilerState>>,
    Json(body): Json<AdminCreateBucketBody>,
) -> axum::response::Response {
    use crate::s3_handler::AdminCreateBucketRequest;
    state
        .s3_handler
        .admin_create_bucket(AdminCreateBucketRequest {
            name: body.name,
            collection: body.collection,
            size_limit: body.size_limit,
        })
        .await
}

async fn admin_delete_bucket(
    State(state): State<Arc<FilerState>>,
    Path(name): Path<String>,
) -> axum::response::Response {
    state.s3_handler.admin_delete_bucket(&name).await
}

#[derive(Debug, Deserialize)]
struct AdminSetQuotaBody {
    size_limit: u64,
}

async fn admin_set_bucket_quota(
    State(state): State<Arc<FilerState>>,
    Path(name): Path<String>,
    Json(body): Json<AdminSetQuotaBody>,
) -> axum::response::Response {
    use crate::s3_handler::AdminSetQuotaRequest;
    state
        .s3_handler
        .admin_set_quota(
            &name,
            AdminSetQuotaRequest {
                size_limit: body.size_limit,
            },
        )
        .await
}

// ========================================================================
// CRDT 管理接口
// ========================================================================

async fn admin_crdt_overview(State(state): State<Arc<FilerState>>) -> Json<CrdtOverview> {
    let overview = state.meta_shard_manager.get_crdt_overview();
    Json(overview)
}

async fn admin_crdt_shard_states(
    State(state): State<Arc<FilerState>>,
    Path(id): Path<u64>,
) -> Json<Vec<OrsetStateDetail>> {
    let states = state.meta_shard_manager.get_shard_orset_states(ShardId(id));
    Json(states)
}

async fn admin_crdt_dir_state(
    State(state): State<Arc<FilerState>>,
    Path((id, dir_ino)): Path<(u64, u64)>,
) -> axum::response::Response {
    match state
        .meta_shard_manager
        .get_dir_orset_state(ShardId(id), dir_ino)
    {
        Some(state) => Json(state).into_response(),
        None => (
            axum::http::StatusCode::NOT_FOUND,
            "Directory OR-Set state not found",
        )
            .into_response(),
    }
}

async fn admin_crdt_cleanup(
    State(state): State<Arc<FilerState>>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> axum::response::Response {
    let ttl_hours = params
        .get("ttl")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(24);

    let cleaned = state.meta_shard_manager.cleanup_tombstones(ttl_hours);
    Json(serde_json::json!({
        "cleaned_count": cleaned,
        "ttl_hours": ttl_hours
    }))
    .into_response()
}
