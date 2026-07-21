use axum::{
    body::Bytes,
    extract::{Path, State},
    response::IntoResponse,
    routing::{delete, get, head, put},
    Json, Router, Server,
};
use log::info;
use powerfs_common::error::PowerFsError;
use std::sync::Arc;

use crate::bucket_manager::BucketManager;
use crate::entry_manager::EntryManager;
use crate::meta_shard_manager::{FilerStatus, MetaShardManager, ShardDetail};
use crate::metadata_store::MetadataStore;
use crate::raft_group_manager::ShardId;
use crate::s3_handler::S3Handler;
use crate::volume_router::VolumeRouter;

pub struct FilerServer {
    s3_handler: Arc<S3Handler>,
    meta_shard_manager: Arc<MetaShardManager>,
    addr: std::net::SocketAddr,
}

pub struct FilerState {
    s3_handler: Arc<S3Handler>,
    meta_shard_manager: Arc<MetaShardManager>,
}

impl FilerServer {
    pub fn new(
        addr: std::net::SocketAddr,
        _metadata_store: Arc<MetadataStore>,
        _bucket_manager: Arc<BucketManager>,
        _entry_manager: Arc<EntryManager>,
        _volume_router: Arc<VolumeRouter>,
        s3_handler: Arc<S3Handler>,
        meta_shard_manager: Arc<MetaShardManager>,
    ) -> Self {
        Self {
            s3_handler,
            meta_shard_manager,
            addr,
        }
    }

    pub async fn serve(self) -> Result<(), PowerFsError> {
        let state = Arc::new(FilerState {
            s3_handler: self.s3_handler,
            meta_shard_manager: self.meta_shard_manager,
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
