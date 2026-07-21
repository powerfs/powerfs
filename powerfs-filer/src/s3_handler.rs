use axum::{http::StatusCode, response::IntoResponse};
use hex;
use powerfs_master::volume_client::VolumeClientPool;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::bucket_manager::BucketManager;
use crate::entry_manager::EntryManager;
use crate::meta_shard_manager::MetaShardManager;
use crate::volume_router::VolumeRouter;

pub struct S3Handler {
    bucket_manager: Arc<BucketManager>,
    entry_manager: Arc<EntryManager>,
    volume_router: Arc<VolumeRouter>,
    volume_client_pool: Arc<VolumeClientPool>,
    // Optional sharded metadata backend (方案A: 客户端直连MetaNode).
    // When present, S3 object metadata is served from Raft+RocksDB shards
    // instead of the Redis-backed EntryManager.
    meta_shard_manager: Option<Arc<MetaShardManager>>,
}

impl S3Handler {
    pub fn new(
        bucket_manager: Arc<BucketManager>,
        entry_manager: Arc<EntryManager>,
        volume_router: Arc<VolumeRouter>,
        volume_client_pool: Arc<VolumeClientPool>,
    ) -> Self {
        Self {
            bucket_manager,
            entry_manager,
            volume_router,
            volume_client_pool,
            meta_shard_manager: None,
        }
    }

    pub fn with_meta_shard_manager(mut self, manager: Arc<MetaShardManager>) -> Self {
        self.meta_shard_manager = Some(manager);
        self
    }

    pub async fn create_bucket(&self, bucket: &str) -> axum::response::Response {
        match self.bucket_manager.create_bucket(bucket, "001").await {
            Ok(_) => {
                // Ensure a root directory inode exists in the shard backend for this bucket.
                if let Some(mgr) = &self.meta_shard_manager {
                    if let Err(e) = mgr.ensure_bucket_root(bucket).await {
                        eprintln!("Failed to ensure bucket root in shards: {}", e);
                    }
                }
                (StatusCode::CREATED, "").into_response()
            }
            Err(e) => {
                eprintln!("Failed to create bucket: {}", e);
                if e.to_string().contains("already exists") {
                    (StatusCode::CONFLICT, "Bucket already exists".to_string()).into_response()
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, "").into_response()
                }
            }
        }
    }

    pub async fn delete_bucket(&self, bucket: &str) -> axum::response::Response {
        match self.bucket_manager.delete_bucket(bucket).await {
            Ok(_) => (StatusCode::NO_CONTENT, "").into_response(),
            Err(e) => {
                eprintln!("Failed to delete bucket: {}", e);
                if e.to_string().contains("not exist") {
                    s3_error(
                        StatusCode::NOT_FOUND,
                        "NoSuchBucket",
                        "The specified bucket does not exist",
                    )
                    .into_response()
                } else if e.to_string().contains("not empty") {
                    s3_error(
                        StatusCode::CONFLICT,
                        "BucketNotEmpty",
                        "The bucket you tried to delete is not empty",
                    )
                    .into_response()
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, "").into_response()
                }
            }
        }
    }

    pub async fn head_bucket(&self, bucket: &str) -> axum::response::Response {
        if self.bucket_manager.get_bucket(bucket).await.is_some() {
            (StatusCode::OK, "").into_response()
        } else {
            (StatusCode::NOT_FOUND, "").into_response()
        }
    }

    pub async fn list_buckets(&self) -> axum::response::Response {
        let buckets = self.bucket_manager.list_buckets().await;
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<ListAllMyBucketsResult>
  <Owner>
    <ID>powerfs</ID>
    <DisplayName>PowerFS</DisplayName>
  </Owner>
  <Buckets>
{}
  </Buckets>
</ListAllMyBucketsResult>",
            buckets
                .into_iter()
                .map(|b| format!(
                    "    <Bucket>
      <Name>{}</Name>
      <CreationDate>{}</CreationDate>
    </Bucket>",
                    b.name,
                    b.creation_time.to_rfc3339()
                ))
                .collect::<Vec<String>>()
                .join("\n")
        );
        (StatusCode::OK, body).into_response()
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
    ) -> axum::response::Response {
        let bucket_info = match self.bucket_manager.get_bucket(bucket).await {
            Some(b) => b,
            None => return (StatusCode::NOT_FOUND, "Bucket not found".to_string()).into_response(),
        };

        let volume_id = bucket_info.volume_ids[0];
        let server_addr = match self.volume_router.get_server_addr(volume_id).await {
            Some(a) => a,
            None => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Volume route not found".to_string(),
                )
                    .into_response()
            }
        };

        let cookie = rand::random::<u64>();
        let file_key = rand::random::<u64>();
        let fid_str = format!("{},{},{}", volume_id, cookie, file_key);

        if let Err(e) = self
            .volume_client_pool
            .write_needle(&server_addr, volume_id, file_key, data)
            .await
        {
            eprintln!("Failed to write needle: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to write data".to_string(),
            )
                .into_response();
        }

        let size = data.len() as u64;
        let mut hasher = Sha256::new();
        hasher.update(data);
        let etag = hex::encode(hasher.finalize());

        // Prefer the sharded metadata backend when available.
        if let Some(mgr) = &self.meta_shard_manager {
            let root_inode = match mgr.ensure_bucket_root(bucket).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Failed to ensure bucket root: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to resolve bucket root".to_string(),
                    )
                        .into_response();
                }
            };
            if let Err(e) = mgr
                .put_object_entry(root_inode, key, size, &fid_str, volume_id, &etag)
                .await
            {
                eprintln!("Failed to put object entry in shards: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to store object metadata".to_string(),
                )
                    .into_response();
            }
            let mut resp = (StatusCode::OK, "").into_response();
            resp.headers_mut().insert("ETag", etag.parse().unwrap());
            return resp;
        }

        // Fallback: Redis-backed EntryManager.
        match self
            .entry_manager
            .put_entry(bucket, key, data, &fid_str, volume_id)
            .await
        {
            Ok(_) => {
                let mut resp = (StatusCode::OK, "").into_response();
                resp.headers_mut().insert("ETag", etag.parse().unwrap());
                resp
            }
            Err(e) => {
                eprintln!("Failed to put entry: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, "").into_response()
            }
        }
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> axum::response::Response {
        // Resolve object metadata (fid, volume_id, etag, size).
        let (fid, volume_id, etag, size): (String, u32, String, u64);

        if let Some(mgr) = &self.meta_shard_manager {
            let root_inode = match mgr.ensure_bucket_root(bucket).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Failed to ensure bucket root: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to resolve bucket root".to_string(),
                    )
                        .into_response();
                }
            };
            let info = match mgr.get_object_entry(root_inode, key) {
                Some(i) => i,
                None => {
                    return (StatusCode::NOT_FOUND, "Object not found".to_string()).into_response()
                }
            };
            fid = info.fid.unwrap_or_default();
            volume_id = info.volume_id.unwrap_or(0);
            etag = info.etag.unwrap_or_default();
            size = info.size;
        } else {
            let entry_info = match self.entry_manager.get_entry(bucket, key).await {
                Some(e) => e,
                None => {
                    return (StatusCode::NOT_FOUND, "Object not found".to_string()).into_response()
                }
            };
            fid = entry_info.fid;
            volume_id = entry_info.volume_id;
            etag = entry_info.etag;
            size = entry_info.size;
        }

        let server_addr = match self.volume_router.get_server_addr(volume_id).await {
            Some(a) => a,
            None => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Volume route not found".to_string(),
                )
                    .into_response()
            }
        };

        let fid_parts: Vec<&str> = fid.split(',').collect();
        if fid_parts.len() < 3 {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Invalid FID format".to_string(),
            )
                .into_response();
        }

        let vid: u32 = match fid_parts[0].parse() {
            Ok(v) => v,
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Invalid volume ID".to_string(),
                )
                    .into_response()
            }
        };

        let file_key: u64 = match fid_parts[2].parse() {
            Ok(f) => f,
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Invalid file key".to_string(),
                )
                    .into_response()
            }
        };

        let data = match self
            .volume_client_pool
            .read_needle(&server_addr, vid, file_key)
            .await
        {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Failed to read needle: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to read data".to_string(),
                )
                    .into_response();
            }
        };

        axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("ETag", &etag)
            .header("Content-Length", size.to_string())
            .body(axum::body::boxed(axum::body::Body::from(data)))
            .unwrap()
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> axum::response::Response {
        if let Some(mgr) = &self.meta_shard_manager {
            let root_inode = match mgr.ensure_bucket_root(bucket).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Failed to ensure bucket root: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to resolve bucket root".to_string(),
                    )
                        .into_response();
                }
            };
            match mgr.delete_object_entry(root_inode, key).await {
                Ok(_) => return (StatusCode::NO_CONTENT, "").into_response(),
                Err(e) => {
                    eprintln!("Failed to delete object entry: {}", e);
                    if e.contains("not found") {
                        return s3_error(
                            StatusCode::NOT_FOUND,
                            "NoSuchKey",
                            "The specified key does not exist",
                        )
                        .into_response();
                    }
                    return (StatusCode::INTERNAL_SERVER_ERROR, "").into_response();
                }
            }
        }

        match self.entry_manager.delete_entry(bucket, key).await {
            Ok(_) => (StatusCode::NO_CONTENT, "").into_response(),
            Err(e) => {
                eprintln!("Failed to delete object: {}", e);
                if e.to_string().contains("not found") {
                    s3_error(
                        StatusCode::NOT_FOUND,
                        "NoSuchKey",
                        "The specified key does not exist",
                    )
                    .into_response()
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, "").into_response()
                }
            }
        }
    }

    pub async fn list_objects(&self, bucket: &str) -> axum::response::Response {
        struct ObjectSummary {
            key: String,
            mtime_rfc3339: String,
            etag: String,
            size: u64,
        }

        let entries: Vec<ObjectSummary> = if let Some(mgr) = &self.meta_shard_manager {
            let root_inode = match mgr.ensure_bucket_root(bucket).await {
                Ok(v) => v,
                Err(_) => {
                    return s3_error(
                        StatusCode::NOT_FOUND,
                        "NoSuchBucket",
                        "The specified bucket does not exist",
                    )
                    .into_response();
                }
            };
            mgr.list_object_entries(root_inode)
                .into_iter()
                .map(|info| ObjectSummary {
                    key: info.name,
                    mtime_rfc3339: chrono::DateTime::from_timestamp(info.mtime as i64, 0)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_default(),
                    etag: info.etag.unwrap_or_default(),
                    size: info.size,
                })
                .collect()
        } else {
            match self.entry_manager.list_entries(bucket).await {
                Ok(e) => e
                    .into_iter()
                    .map(|info| ObjectSummary {
                        key: info.key,
                        mtime_rfc3339: info.mtime.to_rfc3339(),
                        etag: info.etag,
                        size: info.size,
                    })
                    .collect(),
                Err(_) => {
                    return s3_error(
                        StatusCode::NOT_FOUND,
                        "NoSuchBucket",
                        "The specified bucket does not exist",
                    )
                    .into_response();
                }
            }
        };

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<ListBucketResult>
  <Name>{}</Name>
  <Prefix></Prefix>
  <Marker></Marker>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
{}
</ListBucketResult>",
            bucket,
            entries
                .into_iter()
                .map(|e| format!(
                    "  <Contents>
    <Key>{}</Key>
    <LastModified>{}</LastModified>
    <ETag>{}</ETag>
    <Size>{}</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>",
                    e.key, e.mtime_rfc3339, e.etag, e.size
                ))
                .collect::<Vec<String>>()
                .join("\n")
        );
        (StatusCode::OK, body).into_response()
    }
}

fn s3_error(status_code: StatusCode, code: &str, message: &str) -> (StatusCode, String) {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{}</Code>
  <Message>{}</Message>
  <RequestId>test-request-id</RequestId>
  <HostId>test-host-id</HostId>
</Error>"#,
        code, message
    );
    (status_code, xml)
}
