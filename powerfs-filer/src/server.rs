use axum::{
    body::Bytes,
    extract::{Path, State},
    response::IntoResponse,
    routing::{delete, get, head, post, put},
    Json, Router, Server,
};
use log::info;
use powerfs_common::error::PowerFsError;
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
) -> axum::response::Response {
    state.s3_handler.create_bucket(&bucket).await
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
) -> axum::response::Response {
    state.s3_handler.list_objects(&bucket).await
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
