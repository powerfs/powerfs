use axum::{http::StatusCode, response::IntoResponse, Json};
use hex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::metadata_store::BucketInfo;

use crate::bucket_manager::BucketManager;
use crate::entry_manager::EntryManager;
use crate::meta_shard_manager::MetaShardManager;
use crate::tlv_volume_client::TlvVolumeClient;
use crate::volume_router::VolumeRouter;

/// Admin bucket create request body (JSON, proxied via Monitor).
#[derive(Debug, Deserialize)]
pub struct AdminCreateBucketRequest {
    pub name: String,
    #[serde(default)]
    pub collection: Option<String>,
    #[serde(default)]
    pub size_limit: Option<u64>,
}

/// Admin bucket quota update request body.
#[derive(Debug, Deserialize)]
pub struct AdminSetQuotaRequest {
    pub size_limit: u64,
}

/// Admin bucket list response (JSON, not S3 XML).
#[derive(Debug, Serialize)]
pub struct AdminBucketListResponse {
    pub buckets: Vec<BucketInfo>,
    pub total: usize,
}

pub struct S3Handler {
    bucket_manager: Arc<BucketManager>,
    entry_manager: Arc<EntryManager>,
    volume_router: Arc<VolumeRouter>,
    volume_client_pool: Arc<TlvVolumeClient>,
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
        volume_client_pool: Arc<TlvVolumeClient>,
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

    pub async fn create_bucket(&self, bucket: &str, collection: &str) -> axum::response::Response {
        match self
            .bucket_manager
            .create_bucket(bucket, "001", collection)
            .await
        {
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
        // Verify the bucket exists, then dynamically assign a volume from the
        // bucket's collection pool (Step 4 unified Collection design).
        if self.bucket_manager.get_bucket(bucket).await.is_none() {
            return (StatusCode::NOT_FOUND, "Bucket not found".to_string()).into_response();
        }

        let (volume_id, server_addr, fid_str) =
            match self.bucket_manager.assign_volume_for_object(bucket).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Failed to assign volume for object: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to assign volume".to_string(),
                    )
                        .into_response();
                }
            };

        // fid_str = "volume_id,cookie,file_key"; extract file_key for write_needle.
        let file_key: u64 = fid_str
            .split(',')
            .nth(2)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

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
        let (fid, volume_id, etag, size): (String, u64, String, u64);

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

        let vid: u64 = match fid_parts[0].parse() {
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

    pub async fn list_objects(
        &self,
        bucket: &str,
        params: &crate::server::ListObjectsParams,
    ) -> axum::response::Response {
        struct ObjectSummary {
            key: String,
            mtime_rfc3339: String,
            etag: String,
            size: u64,
        }

        // XML-escape helper for S3 keys (keys may contain & < > and other chars).
        fn xml_escape(s: &str) -> String {
            s.replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('"', "&quot;")
                .replace('\'', "&apos;")
        }

        // Hex-encode continuation token (opaque to client; round-trips the last key).
        fn encode_token(key: &str) -> String {
            hex::encode(key.as_bytes())
        }
        fn decode_token(tok: &str) -> Option<String> {
            let bytes = hex::decode(tok).ok()?;
            String::from_utf8(bytes).ok()
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

        // Stable order is required for deterministic pagination.
        let mut entries = entries;
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        // Continuation token (v2) takes precedence over start-after (v2) which
        // takes precedence over marker (v1, not supported here — we treat v1 as
        // a one-shot list of everything).
        let is_v2 = matches!(params.list_type, Some(2));
        if is_v2 {
            if let Some(ct) = &params.continuation_token {
                if let Some(last_key) = decode_token(ct) {
                    entries.retain(|e| e.key.as_str() > last_key.as_str());
                }
            } else if let Some(sa) = &params.start_after {
                entries.retain(|e| e.key.as_str() > sa.as_str());
            }
        }

        // Prefix filtering (applied before delimiter grouping).
        let prefix = params.prefix.clone().unwrap_or_default();
        if !prefix.is_empty() {
            entries.retain(|e| e.key.starts_with(&prefix));
        }

        // Delimiter → roll matching entries into <CommonPrefixes>.
        let mut common_prefixes: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        if let Some(delim) = &params.delimiter {
            if !delim.is_empty() {
                let mut kept = Vec::with_capacity(entries.len());
                for e in entries.drain(..) {
                    if let Some(rest) = e.key.strip_prefix(&prefix) {
                        if let Some(idx) = rest.find(delim) {
                            let cp = format!("{}{}", &prefix, &rest[..idx + delim.len()]);
                            common_prefixes.insert(cp);
                            continue;
                        }
                    }
                    kept.push(e);
                }
                entries = kept;
            }
        }

        // max-keys: S3 default 1000, server caps to [0, 1000].
        let max_keys = params.max_keys.unwrap_or(1000).clamp(0, 1000) as usize;
        let total_after_filter = entries.len();
        let truncated = total_after_filter > max_keys;
        if truncated {
            entries.truncate(max_keys);
        }
        let key_count = entries.len();

        // NextContinuationToken: opaque hex of the last key returned, only when truncated.
        let next_ct = if truncated {
            entries.last().map(|e| encode_token(&e.key))
        } else {
            None
        };

        let contents_xml = entries
            .iter()
            .map(|e| {
                format!(
                    "  <Contents>\n    <Key>{}</Key>\n    <LastModified>{}</LastModified>\n    <ETag>{}</ETag>\n    <Size>{}</Size>\n    <StorageClass>STANDARD</StorageClass>\n  </Contents>",
                    xml_escape(&e.key),
                    e.mtime_rfc3339,
                    xml_escape(&e.etag),
                    e.size
                )
            })
            .collect::<Vec<String>>()
            .join("\n");

        let cp_xml = common_prefixes
            .iter()
            .map(|cp| format!("  <CommonPrefixes>\n    <Prefix>{}</Prefix>\n  </CommonPrefixes>", xml_escape(cp)))
            .collect::<Vec<String>>()
            .join("\n");

        // Build response. Include v2-only fields (KeyCount, ContinuationToken,
        // NextContinuationToken, StartAfter) only when list-type=2 was requested.
        let v2_fields = if is_v2 {
            let mut s = format!("\n  <KeyCount>{}</KeyCount>", key_count);
            if let Some(ct) = &params.continuation_token {
                s.push_str(&format!("\n  <ContinuationToken>{}</ContinuationToken>", xml_escape(ct)));
            }
            if let Some(sa) = &params.start_after {
                s.push_str(&format!("\n  <StartAfter>{}</StartAfter>", xml_escape(sa)));
            }
            if let Some(nct) = &next_ct {
                s.push_str(&format!("\n  <NextContinuationToken>{}</NextContinuationToken>", xml_escape(nct)));
            }
            s
        } else {
            String::new()
        };

        let delim_xml = if params.delimiter.is_some() {
            format!("\n  <Delimiter>{}</Delimiter>", xml_escape(params.delimiter.as_deref().unwrap_or("")))
        } else {
            String::new()
        };

        let enc_xml = if params.encoding_type.as_deref() == Some("url") {
            "\n  <EncodingType>url</EncodingType>".to_string()
        } else {
            String::new()
        };

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult>\n  <Name>{}</Name>\n  <Prefix>{}</Prefix>\n  <MaxKeys>{}</MaxKeys>\n  <IsTruncated>{}</IsTruncated>{}{}{}\n{}\n{}\n</ListBucketResult>",
            xml_escape(bucket),
            xml_escape(&prefix),
            max_keys,
            truncated,
            v2_fields,
            delim_xml,
            enc_xml,
            contents_xml,
            cp_xml,
        );
        (StatusCode::OK, body).into_response()
    }

    // ── Admin bucket management (JSON API, proxied via Monitor) ──

    /// Admin: list all buckets as JSON (not S3 XML).
    pub async fn admin_list_buckets(&self) -> axum::response::Response {
        let buckets = self.bucket_manager.list_buckets().await;
        let total = buckets.len();
        Json(AdminBucketListResponse { buckets, total }).into_response()
    }

    /// Admin: create a bucket via JSON body.
    pub async fn admin_create_bucket(
        &self,
        req: AdminCreateBucketRequest,
    ) -> axum::response::Response {
        let collection = req.collection.as_deref().unwrap_or("default");
        match self
            .bucket_manager
            .create_bucket(&req.name, "001", collection)
            .await
        {
            Ok(mut info) => {
                if let Some(limit) = req.size_limit {
                    if let Ok(updated) =
                        self.bucket_manager.set_bucket_quota(&req.name, limit).await
                    {
                        info = updated;
                    }
                }
                (StatusCode::CREATED, Json(info)).into_response()
            }
            Err(e) => {
                let msg = e.to_string();
                let code = if msg.contains("exist") || msg.contains("FileExists") {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                (code, Json(serde_json::json!({ "error": msg }))).into_response()
            }
        }
    }

    /// Admin: delete a bucket by name.
    pub async fn admin_delete_bucket(&self, bucket: &str) -> axum::response::Response {
        match self.bucket_manager.delete_bucket(bucket).await {
            Ok(true) => (StatusCode::NO_CONTENT, "").into_response(),
            Ok(false) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "bucket not found" })),
            )
                .into_response(),
            Err(e) => {
                let msg = e.to_string();
                let code = if msg.contains("not empty") {
                    StatusCode::CONFLICT
                } else if msg.contains("not exist") || msg.contains("DirectoryNotFound") {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                (code, Json(serde_json::json!({ "error": msg }))).into_response()
            }
        }
    }

    /// Admin: set bucket quota (size_limit = 0 means unlimited).
    pub async fn admin_set_quota(
        &self,
        bucket: &str,
        req: AdminSetQuotaRequest,
    ) -> axum::response::Response {
        match self
            .bucket_manager
            .set_bucket_quota(bucket, req.size_limit)
            .await
        {
            Ok(info) => (StatusCode::OK, Json(info)).into_response(),
            Err(e) => {
                let msg = e.to_string();
                let code = if msg.contains("not exist") || msg.contains("DirectoryNotFound") {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                (code, Json(serde_json::json!({ "error": msg }))).into_response()
            }
        }
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
