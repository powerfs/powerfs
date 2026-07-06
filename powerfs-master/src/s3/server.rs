use axum::{
    body::Bytes,
    extract::{Query, State},
    http::HeaderMap,
    response::Response,
    routing::{delete, get, head, post, put},
    Router, Server,
};
use std::sync::Arc;

use crate::directory_tree::DirectoryTree;
use crate::lock_manager::{LockLevel, LockManager};
use crate::s3::master_api::MasterApi;
use crate::volume_client::VolumeClientPool;

pub struct S3Server {
    directory_tree: Arc<DirectoryTree>,
    master: Arc<MasterApi>,
    volume_client_pool: Arc<VolumeClientPool>,
    lock_manager: Arc<LockManager>,
    addr: std::net::SocketAddr,
}

#[derive(Clone)]
pub struct PartInfo {
    pub part_number: i32,
    pub etag: String,
    pub size: u64,
    pub fid: String,
}

pub struct MultipartSession {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub parts: Vec<PartInfo>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub volume_id: u32,
}

pub struct S3State {
    pub directory_tree: Arc<DirectoryTree>,
    pub master: Arc<MasterApi>,
    pub volume_client_pool: Arc<VolumeClientPool>,
    pub lock_manager: Arc<LockManager>,
    pub multipart_sessions:
        tokio::sync::RwLock<std::collections::HashMap<String, MultipartSession>>,
}

impl S3Server {
    pub fn new(
        addr: std::net::SocketAddr,
        directory_tree: Arc<DirectoryTree>,
        master: Arc<MasterApi>,
        volume_client_pool: Arc<VolumeClientPool>,
        lock_manager: Arc<LockManager>,
    ) -> Self {
        S3Server {
            directory_tree,
            master,
            volume_client_pool,
            lock_manager,
            addr,
        }
    }

    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error>> {
        let state = Arc::new(S3State {
            directory_tree: self.directory_tree,
            master: self.master,
            volume_client_pool: self.volume_client_pool,
            lock_manager: self.lock_manager,
            multipart_sessions: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        });

        let router = Router::new()
            .route("/", get(handlers::list_buckets))
            .route("/_admin/multipart-uploads", get(handlers::list_multipart_uploads))
            .route("/_admin/multipart-uploads/:upload_id", delete(handlers::admin_abort_multipart_upload))
            .route("/:bucket", put(handlers::create_bucket))
            .route("/:bucket", delete(handlers::delete_bucket))
            .route("/:bucket", get(handlers::list_objects))
            .route("/:bucket", head(handlers::head_bucket))
            .route("/:bucket/*key", put(handlers::object_put_handler))
            .route("/:bucket/*key", get(handlers::object_get_handler))
            .route("/:bucket/*key", delete(handlers::object_delete_handler))
            .route("/:bucket/*key", head(handlers::head_object))
            .route("/:bucket/*key", post(handlers::object_post_handler))
            .with_state(state);

        Server::bind(&self.addr)
            .serve(router.into_make_service())
            .await?;
        Ok(())
    }
}

pub mod handlers {
    use super::*;
    use crate::proto::{Entry, FileChunk, FuseAttributes};
    use axum::{extract::Path, http::StatusCode, response::IntoResponse};
    use hex;
    use powerfs_common::types::VolumeId;
    use sha2::{Digest, Sha256};

    fn build_error_response(status: StatusCode, message: &str) -> Response {
        (status, message.to_string()).into_response()
    }

    pub async fn object_put_handler(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
        query: Option<Query<std::collections::HashMap<String, String>>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let upload_id = query.as_ref().and_then(|q| q.get("uploadId"));
        let part_number = query.as_ref().and_then(|q| q.get("partNumber"));

        if let (Some(upload_id), Some(part_number)) = (upload_id, part_number) {
            upload_part(
                State(state),
                Path((bucket, key)),
                upload_id.clone(),
                part_number.clone(),
                headers,
                body,
            )
            .await
            .into_response()
        } else {
            put_object(State(state), Path((bucket, key)), headers, body)
                .await
                .into_response()
        }
    }

    pub async fn object_get_handler(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
        query: Option<Query<std::collections::HashMap<String, String>>>,
    ) -> Response {
        let upload_id = query.as_ref().and_then(|q| q.get("uploadId"));

        if let Some(upload_id) = upload_id {
            list_parts(State(state), Path((bucket, key)), upload_id.clone())
                .await
                .into_response()
        } else {
            get_object(State(state), Path((bucket, key)))
                .await
                .into_response()
        }
    }

    pub async fn object_delete_handler(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
        query: Option<Query<std::collections::HashMap<String, String>>>,
    ) -> Response {
        let upload_id = query.as_ref().and_then(|q| q.get("uploadId"));

        if let Some(upload_id) = upload_id {
            abort_multipart_upload(State(state), Path((bucket, key)), upload_id.clone())
                .await
                .into_response()
        } else {
            delete_object(State(state), Path((bucket, key)))
                .await
                .into_response()
        }
    }

    pub async fn object_post_handler(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
        query: Option<Query<std::collections::HashMap<String, String>>>,
        body: Bytes,
    ) -> Response {
        let upload_id = query.as_ref().and_then(|q| q.get("uploadId"));

        if let Some(upload_id) = upload_id {
            complete_multipart_upload(State(state), Path((bucket, key)), upload_id.clone(), body)
                .await
                .into_response()
        } else {
            create_multipart_upload(State(state), Path((bucket, key)))
                .await
                .into_response()
        }
    }

    pub async fn list_buckets(State(state): State<Arc<S3State>>) -> impl IntoResponse {
        let entries = state.directory_tree.list_entries("/", 1000, "");

        let bucket_names: Vec<String> = entries
            .into_iter()
            .filter(|e| {
                e.attributes
                    .as_ref()
                    .map(|a| a.mode == 0o40755)
                    .unwrap_or(false)
            })
            .map(|e| e.name)
            .collect();

        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Buckets>
    {}
  </Buckets>
</ListAllMyBucketsResult>"#,
            bucket_names
                .into_iter()
                .map(|name| format!(
                    "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
                    name,
                    chrono::Utc::now().to_rfc3339()
                ))
                .collect::<Vec<String>>()
                .join("\n")
        );

        (StatusCode::OK, body)
    }

    pub async fn create_bucket(
        State(state): State<Arc<S3State>>,
        Path(bucket): Path<String>,
    ) -> impl IntoResponse {
        let bucket_path = format!("/{}", bucket);
        let lock_key = format!("bucket:{}", bucket);

        let _lock = state
            .lock_manager
            .acquire(&lock_key, LockLevel::RaftLease)
            .await;

        if state.directory_tree.get_entry(&bucket_path).is_some() {
            return (StatusCode::CONFLICT, "Bucket already exists".to_string());
        }

        match state.directory_tree.create_directory(&bucket_path) {
            Ok(_) => (StatusCode::CREATED, "".to_string()),
            Err(e) => {
                eprintln!("Failed to create bucket: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, "".to_string())
            }
        }
    }

    pub async fn delete_bucket(
        State(state): State<Arc<S3State>>,
        Path(bucket): Path<String>,
    ) -> impl IntoResponse {
        let bucket_path = format!("/{}", bucket);
        let lock_key = format!("bucket:{}", bucket);

        let _lock = state
            .lock_manager
            .acquire(&lock_key, LockLevel::RaftLease)
            .await;

        if state.directory_tree.get_entry(&bucket_path).is_none() {
            return (StatusCode::NOT_FOUND, "Bucket not found".to_string());
        }

        let entries = state.directory_tree.list_entries(&bucket_path, 1, "");
        if !entries.is_empty() {
            return (StatusCode::CONFLICT, "Bucket is not empty".to_string());
        }

        match state.directory_tree.delete_entry(&bucket_path) {
            Ok(true) => (StatusCode::NO_CONTENT, "".to_string()),
            Ok(false) => (StatusCode::NOT_FOUND, "Bucket not found".to_string()),
            Err(e) => {
                eprintln!("Failed to delete bucket: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, "".to_string())
            }
        }
    }

    pub async fn head_bucket(
        State(state): State<Arc<S3State>>,
        Path(bucket): Path<String>,
    ) -> impl IntoResponse {
        let bucket_path = format!("/{}", bucket);

        if state.directory_tree.get_entry(&bucket_path).is_some() {
            (StatusCode::OK, "")
        } else {
            (StatusCode::NOT_FOUND, "")
        }
    }

    pub async fn list_objects(
        State(state): State<Arc<S3State>>,
        Path(bucket): Path<String>,
    ) -> impl IntoResponse {
        let bucket_path = format!("/{}", bucket);

        if state.directory_tree.get_entry(&bucket_path).is_none() {
            return (StatusCode::NOT_FOUND, "Bucket not found".to_string());
        }

        let entries = state.directory_tree.list_entries(&bucket_path, 1000, "");

        let object_list: Vec<String> = entries
            .into_iter()
            .map(|e| {
                let size = e.content_size;
                let mtime_secs = if e.attributes.as_ref().map(|a| a.mtime).unwrap_or(0) > i64::MAX as u64 {
                    i64::MAX
                } else {
                    (e.attributes.as_ref().map(|a| a.mtime).unwrap_or(0) / 1_000_000_000) as i64
                };
                let last_modified = chrono::DateTime::from_timestamp(mtime_secs, 0)
                    .unwrap_or_default();

                format!(
                    "<Contents><Key>{}</Key><Size>{}</Size><LastModified>{}</LastModified></Contents>",
                    e.name,
                    size,
                    last_modified.format("%Y-%m-%dT%H:%M:%S.000Z")
                )
            })
            .collect();

        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{}</Name>
  {}
</ListBucketResult>"#,
            bucket,
            object_list.join("\n")
        );

        (StatusCode::OK, body)
    }

    pub async fn put_object(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
        _headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        let bucket_path = format!("/{}", bucket);
        let lock_key = format!("object:{}/{}", bucket, key);

        let _lock = state
            .lock_manager
            .acquire(&lock_key, LockLevel::RaftLease)
            .await;

        if state.directory_tree.get_entry(&bucket_path).is_none() {
            return build_error_response(StatusCode::NOT_FOUND, "Bucket not found");
        }

        let data = body.as_ref();
        let size = data.len() as u64;

        let (fid, nodes) = match state.master.assign_volume("001", "default").await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Failed to assign volume: {}", e);
                return build_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to assign volume",
                );
            }
        };

        if nodes.is_empty() {
            return build_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "No volume nodes available",
            );
        }

        let node = &nodes[0];
        let node_address = format!("{}:{}", node.address, node.grpc_port);

        if let Err(e) = state
            .volume_client_pool
            .write_needle(&node_address, fid.volume_id.0, fid.file_key, data)
            .await
        {
            eprintln!("Failed to write needle: {}", e);
            return build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "Failed to write data");
        }

        let mut hasher = Sha256::new();
        hasher.update(data);
        let etag = hex::encode(hasher.finalize());

        let chunks = vec![FileChunk {
            offset: 0,
            size,
            mtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            fid: format!("{},{},{}", fid.volume_id.0, fid.cookie, fid.file_key),
            cookie: fid.cookie as u32,
            crc32: 0,
        }];

        let mut extended = std::collections::HashMap::new();
        extended.insert("etag".to_string(), etag.clone().into_bytes());

        let entry = Entry {
            name: key.clone(),
            directory: bucket_path,
            attributes: Some(FuseAttributes {
                ino: 0,
                mode: 0o100644,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                size,
                blksize: 4096,
                blocks: size.div_ceil(4096),
                atime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                mtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                ctime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                crtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                perm: 0o644,
            }),
            chunks,
            hard_link_id: "".to_string(),
            hard_link_counter: 0,
            extended,
            content_size: size,
            disk_size: size,
            ttl: "".to_string(),
            symlink_target: "".to_string(),
        };

        match state.directory_tree.create_entry(entry) {
            Ok(_) => {
                let mut response = (StatusCode::OK, "").into_response();
                response
                    .headers_mut()
                    .insert("ETag", format!("\"{}\"", etag).parse().unwrap());
                response
                    .headers_mut()
                    .insert("Content-Length", size.to_string().parse().unwrap());
                response
            }
            Err(e) => {
                eprintln!("Failed to put object: {}", e);
                build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "")
            }
        }
    }

    pub async fn get_object(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
    ) -> impl IntoResponse {
        let bucket_path = format!("/{}", bucket);
        let object_path = format!("/{}/{}", bucket, key);

        if state.directory_tree.get_entry(&bucket_path).is_none() {
            return build_error_response(StatusCode::NOT_FOUND, "Bucket not found");
        }

        let entry = match state.directory_tree.get_entry(&object_path) {
            Some(e) => e,
            None => return build_error_response(StatusCode::NOT_FOUND, "Object not found"),
        };

        if entry.chunks.is_empty() {
            return (StatusCode::OK, "").into_response();
        }

        let chunk = &entry.chunks[0];
        let fid_parts: Vec<&str> = chunk.fid.split(',').collect();
        if fid_parts.len() < 3 {
            return build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "Invalid FID format");
        }

        let volume_id: u32 = match fid_parts[0].parse() {
            Ok(v) => v,
            Err(_) => {
                return build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "Invalid volume ID")
            }
        };

        let file_key: u64 = match fid_parts[2].parse() {
            Ok(f) => f,
            Err(_) => {
                return build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "Invalid file key")
            }
        };

        let volume_info = match state.master.get_volume_info(&VolumeId(volume_id)).await {
            Some(v) => v,
            None => {
                return build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "Volume not found")
            }
        };

        let node = match state.master.get_node_info(&volume_info.node_id).await {
            Some(n) => n,
            None => {
                return build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "Node not found")
            }
        };

        let node_address = format!("{}:{}", node.address, node.grpc_port);

        let data = match state
            .volume_client_pool
            .read_needle(&node_address, volume_id, file_key)
            .await
        {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Failed to read needle: {}", e);
                return build_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to read data",
                );
            }
        };

        let etag = entry
            .extended
            .get("etag")
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_default();

        let mut response = (StatusCode::OK, Bytes::from(data)).into_response();
        response.headers_mut().insert(
            "Content-Length",
            entry.content_size.to_string().parse().unwrap(),
        );
        response
            .headers_mut()
            .insert("Content-Type", "application/octet-stream".parse().unwrap());
        if !etag.is_empty() {
            response
                .headers_mut()
                .insert("ETag", format!("\"{}\"", etag).parse().unwrap());
        }
        response
    }

    pub async fn head_object(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
    ) -> impl IntoResponse {
        let bucket_path = format!("/{}", bucket);
        let object_path = format!("/{}/{}", bucket, key);

        if state.directory_tree.get_entry(&bucket_path).is_none() {
            return build_error_response(StatusCode::NOT_FOUND, "Bucket not found");
        }

        let entry = match state.directory_tree.get_entry(&object_path) {
            Some(e) => e,
            None => return build_error_response(StatusCode::NOT_FOUND, "Object not found"),
        };

        let etag = entry
            .extended
            .get("etag")
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_default();

        let mut response = (StatusCode::OK, "").into_response();
        response.headers_mut().insert(
            "Content-Length",
            entry.content_size.to_string().parse().unwrap(),
        );
        if !etag.is_empty() {
            response
                .headers_mut()
                .insert("ETag", format!("\"{}\"", etag).parse().unwrap());
        }
        response
    }

    pub async fn delete_object(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
    ) -> impl IntoResponse {
        let bucket_path = format!("/{}", bucket);
        let object_path = format!("/{}/{}", bucket, key);
        let lock_key = format!("object:{}/{}", bucket, key);

        let _lock = state
            .lock_manager
            .acquire(&lock_key, LockLevel::RaftLease)
            .await;

        if state.directory_tree.get_entry(&bucket_path).is_none() {
            return build_error_response(StatusCode::NOT_FOUND, "Bucket not found");
        }

        let entry = match state.directory_tree.get_entry(&object_path) {
            Some(e) => e,
            None => return build_error_response(StatusCode::NOT_FOUND, "Object not found"),
        };

        for chunk in &entry.chunks {
            let fid_parts: Vec<&str> = chunk.fid.split(',').collect();
            if fid_parts.len() >= 3 {
                if let (Ok(volume_id), Ok(file_key)) =
                    (fid_parts[0].parse::<u32>(), fid_parts[2].parse::<u64>())
                {
                    if let Some(volume_info) = state.master.get_volume_info(&VolumeId(volume_id)).await {
                        if let Some(node) = state.master.get_node_info(&volume_info.node_id).await {
                            let node_address = format!("{}:{}", node.address, node.grpc_port);
                            let _ = state
                                .volume_client_pool
                                .delete_needle(&node_address, volume_id, file_key)
                                .await;
                        }
                    }
                }
            }
        }

        match state.directory_tree.delete_entry(&object_path) {
            Ok(true) => (StatusCode::NO_CONTENT, "").into_response(),
            Ok(false) => build_error_response(StatusCode::NOT_FOUND, "Object not found"),
            Err(e) => {
                eprintln!("Failed to delete object: {}", e);
                build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "")
            }
        }
    }

    pub async fn create_multipart_upload(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
    ) -> impl IntoResponse {
        let bucket_path = format!("/{}", bucket);
        let lock_key = format!("object:{}/{}", bucket, key);

        let _lock = state
            .lock_manager
            .acquire(&lock_key, LockLevel::RaftLease)
            .await;

        if state.directory_tree.get_entry(&bucket_path).is_none() {
            return build_error_response(StatusCode::NOT_FOUND, "Bucket not found");
        }

        let (fid, nodes) = match state.master.assign_volume("001", "default").await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Failed to assign volume: {}", e);
                return build_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to assign volume",
                );
            }
        };

        if nodes.is_empty() {
            return build_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "No volume nodes available",
            );
        }

        let upload_id = uuid::Uuid::new_v4().to_string();

        let session = MultipartSession {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            parts: Vec::new(),
            created_at: chrono::Utc::now(),
            volume_id: fid.volume_id.0,
        };

        state
            .multipart_sessions
            .write()
            .await
            .insert(upload_id.clone(), session);

        let response_body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <UploadId>{}</UploadId>
</InitiateMultipartUploadResult>"#,
            bucket, key, upload_id
        );

        let mut response = (StatusCode::OK, response_body).into_response();
        response
            .headers_mut()
            .insert("Content-Type", "application/xml".parse().unwrap());
        response
    }

    pub async fn upload_part(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
        upload_id: String,
        part_number_str: String,
        _headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        let part_number: i32 = match part_number_str.parse() {
            Ok(n) => n,
            Err(_) => return build_error_response(StatusCode::BAD_REQUEST, "Invalid part-number"),
        };

        let mut sessions = state.multipart_sessions.write().await;
        let session = match sessions.get_mut(&upload_id) {
            Some(s) => s,
            None => return build_error_response(StatusCode::NOT_FOUND, "Upload ID not found"),
        };

        if session.bucket != bucket || session.key != key {
            return build_error_response(StatusCode::BAD_REQUEST, "Bucket/Key mismatch");
        }

        let data = body.as_ref();
        let size = data.len() as u64;

        let volume_info = match state.master.get_volume_info(&VolumeId(session.volume_id)).await {
            Some(v) => v,
            None => {
                return build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "Volume not found")
            }
        };

        let node = match state.master.get_node_info(&volume_info.node_id).await {
            Some(n) => n,
            None => {
                return build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "Node not found")
            }
        };

        let node_address = format!("{}:{}", node.address, node.grpc_port);

        let file_key = (session.parts.len() + 1) as u64;

        if let Err(e) = state
            .volume_client_pool
            .write_needle(&node_address, session.volume_id, file_key, data)
            .await
        {
            eprintln!("Failed to write needle: {}", e);
            return build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "Failed to write data");
        }

        let mut hasher = Sha256::new();
        hasher.update(data);
        let etag = hex::encode(hasher.finalize());

        let fid = format!("{},0,{}", session.volume_id, file_key);

        session.parts.push(PartInfo {
            part_number,
            etag: etag.clone(),
            size,
            fid,
        });

        let mut response = (StatusCode::OK, "").into_response();
        response
            .headers_mut()
            .insert("ETag", format!("\"{}\"", etag).parse().unwrap());
        response
    }

    pub async fn list_parts(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
        upload_id: String,
    ) -> impl IntoResponse {
        let sessions = state.multipart_sessions.read().await;
        let session = match sessions.get(&upload_id) {
            Some(s) => s,
            None => return build_error_response(StatusCode::NOT_FOUND, "Upload ID not found"),
        };

        if session.bucket != bucket || session.key != key {
            return build_error_response(StatusCode::BAD_REQUEST, "Bucket/Key mismatch");
        }

        let parts_xml: Vec<String> = session
            .parts
            .iter()
            .map(|p| {
                format!(
                    "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag><Size>{}</Size></Part>",
                    p.part_number, p.etag, p.size
                )
            })
            .collect();

        let response_body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <UploadId>{}</UploadId>
  <PartMarker></PartMarker>
  <IsTruncated>false</IsTruncated>
  {}
</ListPartsResult>"#,
            bucket,
            key,
            upload_id,
            parts_xml.join("\n")
        );

        let mut response = (StatusCode::OK, response_body).into_response();
        response
            .headers_mut()
            .insert("Content-Type", "application/xml".parse().unwrap());
        response
    }

    pub async fn complete_multipart_upload(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
        upload_id: String,
        _body: Bytes,
    ) -> impl IntoResponse {
        let lock_key = format!("object:{}/{}", bucket, key);
        let _lock = state
            .lock_manager
            .acquire(&lock_key, LockLevel::RaftLease)
            .await;

        let mut sessions = state.multipart_sessions.write().await;
        let session = match sessions.remove(&upload_id) {
            Some(s) => s,
            None => return build_error_response(StatusCode::NOT_FOUND, "Upload ID not found"),
        };

        if session.bucket != bucket || session.key != key {
            return build_error_response(StatusCode::BAD_REQUEST, "Bucket/Key mismatch");
        }

        if session.parts.is_empty() {
            return build_error_response(StatusCode::BAD_REQUEST, "No parts uploaded");
        }

        let mut parts_sorted = session.parts.clone();
        parts_sorted.sort_by_key(|a| a.part_number);

        let etags: Vec<String> = parts_sorted.iter().map(|p| p.etag.clone()).collect();
        let mut hasher = Sha256::new();
        hasher.update(etags.join(", "));
        let final_etag = hex::encode(hasher.finalize());

        let mut chunks = Vec::new();
        let mut offset: u64 = 0;
        let mut total_size: u64 = 0;

        for part in &parts_sorted {
            chunks.push(FileChunk {
                offset,
                size: part.size,
                mtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                fid: part.fid.clone(),
                cookie: 0,
                crc32: 0,
            });
            offset += part.size;
            total_size += part.size;
        }

        let mut extended = std::collections::HashMap::new();
        extended.insert("etag".to_string(), final_etag.clone().into_bytes());

        let entry = Entry {
            name: key.clone(),
            directory: format!("/{}", bucket),
            attributes: Some(FuseAttributes {
                ino: 0,
                mode: 0o100644,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                size: total_size,
                blksize: 4096,
                blocks: total_size.div_ceil(4096),
                atime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                mtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                ctime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                crtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                perm: 0o644,
            }),
            chunks,
            hard_link_id: "".to_string(),
            hard_link_counter: 0,
            extended,
            content_size: total_size,
            disk_size: total_size,
            ttl: "".to_string(),
            symlink_target: "".to_string(),
        };

        match state.directory_tree.create_entry(entry) {
            Ok(_) => {
                let response_body = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>/{}/{}</Location>
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <ETag>"{}</ETag>
</CompleteMultipartUploadResult>"#,
                    bucket, key, bucket, key, final_etag
                );

                let mut response = (StatusCode::OK, response_body).into_response();
                response
                    .headers_mut()
                    .insert("Content-Type", "application/xml".parse().unwrap());
                response
                    .headers_mut()
                    .insert("ETag", format!("\"{}\"", final_etag).parse().unwrap());
                response
            }
            Err(e) => {
                eprintln!("Failed to complete multipart upload: {}", e);
                build_error_response(StatusCode::INTERNAL_SERVER_ERROR, "")
            }
        }
    }

    pub async fn abort_multipart_upload(
        State(state): State<Arc<S3State>>,
        Path((bucket, key)): Path<(String, String)>,
        upload_id: String,
    ) -> impl IntoResponse {
        let lock_key = format!("object:{}/{}", bucket, key);
        let _lock = state
            .lock_manager
            .acquire(&lock_key, LockLevel::RaftLease)
            .await;

        let mut sessions = state.multipart_sessions.write().await;
        let session = match sessions.remove(&upload_id) {
            Some(s) => s,
            None => return build_error_response(StatusCode::NOT_FOUND, "Upload ID not found"),
        };

        if session.bucket != bucket || session.key != key {
            return build_error_response(StatusCode::BAD_REQUEST, "Bucket/Key mismatch");
        }

        for part in &session.parts {
            let fid_parts: Vec<&str> = part.fid.split(',').collect();
            if fid_parts.len() >= 3 {
                if let (Ok(volume_id), Ok(file_key)) =
                    (fid_parts[0].parse::<u32>(), fid_parts[2].parse::<u64>())
                {
                    if let Some(volume_info) = state.master.get_volume_info(&VolumeId(volume_id)).await {
                        if let Some(node) = state.master.get_node_info(&volume_info.node_id).await {
                            let node_address = format!("{}:{}", node.address, node.grpc_port);
                            let _ = state
                                .volume_client_pool
                                .delete_needle(&node_address, volume_id, file_key)
                                .await;
                        }
                    }
                }
            }
        }

        (StatusCode::NO_CONTENT, "").into_response()
    }

    pub async fn list_multipart_uploads(State(state): State<Arc<S3State>>) -> impl IntoResponse {
        let sessions = state.multipart_sessions.read().await;
        let uploads: Vec<serde_json::Value> = sessions
            .values()
            .map(|s| {
                serde_json::json!({
                    "bucket": s.bucket,
                    "key": s.key,
                    "upload_id": s.upload_id,
                    "initiator": "admin",
                    "creation_date": s.created_at.to_rfc3339(),
                    "part_count": s.parts.len() as u64,
                    "status": "in_progress",
                })
            })
            .collect();
        let json = serde_json::to_string(&uploads).unwrap_or("[]".to_string());
        let mut response = (StatusCode::OK, json).into_response();
        response
            .headers_mut()
            .insert("Content-Type", "application/json".parse().unwrap());
        response
    }

    pub async fn admin_abort_multipart_upload(
        State(state): State<Arc<S3State>>,
        Path(upload_id): Path<String>,
    ) -> impl IntoResponse {
        let mut sessions = state.multipart_sessions.write().await;
        let session = match sessions.remove(&upload_id) {
            Some(s) => s,
            None => return build_error_response(StatusCode::NOT_FOUND, "Upload ID not found"),
        };

        for part in &session.parts {
            let fid_parts: Vec<&str> = part.fid.split(',').collect();
            if fid_parts.len() >= 3 {
                if let (Ok(volume_id), Ok(file_key)) =
                    (fid_parts[0].parse::<u32>(), fid_parts[2].parse::<u64>())
                {
                    if let Some(volume_info) = state.master.get_volume_info(&VolumeId(volume_id)).await {
                        if let Some(node) = state.master.get_node_info(&volume_info.node_id).await {
                            let node_address = format!("{}:{}", node.address, node.grpc_port);
                            let _ = state
                                .volume_client_pool
                                .delete_needle(&node_address, volume_id, file_key)
                                .await;
                        }
                    }
                }
            }
        }

        let json = serde_json::json!({"message": "Aborted successfully"});
        let mut response = (StatusCode::OK, serde_json::to_string(&json).unwrap()).into_response();
        response
            .headers_mut()
            .insert("Content-Type", "application/json".parse().unwrap());
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::HttpBody;
    use axum::http::{Method, Request, StatusCode};
    use tempfile::tempdir;
    use tower::ServiceExt;
    use Body;

    async fn create_test_state() -> Arc<S3State> {
        let dir = tempdir().unwrap();
        let dt = Arc::new(DirectoryTree::new(dir.path()).unwrap());
        let master = Arc::new(
            crate::master::MasterNode::new(
                "127.0.0.1:50051",
                None,
                dir.path().join("raft").to_str().unwrap(),
            )
            .await
            .unwrap(),
        );
        Arc::new(S3State {
            directory_tree: dt,
            master,
            volume_client_pool: Arc::new(VolumeClientPool::new()),
            lock_manager: Arc::new(LockManager::new()),
            multipart_sessions: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        })
    }

    #[tokio::test]
    async fn test_list_buckets_empty() {
        let state = create_test_state().await;
        let router = Router::new()
            .route("/", get(handlers::list_buckets))
            .with_state(state);

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_create_bucket() {
        let state = create_test_state().await;
        let router = Router::new()
            .route("/:bucket", put(handlers::create_bucket))
            .with_state(state);

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_create_bucket_already_exists() {
        let state = create_test_state().await;

        let router1 = Router::new()
            .route("/:bucket", put(handlers::create_bucket))
            .with_state(state.clone());

        let _ = router1
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;

        let router2 = Router::new()
            .route("/:bucket", put(handlers::create_bucket))
            .with_state(state);

        let response = router2
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_delete_bucket() {
        let state = create_test_state().await;

        let router1 = Router::new()
            .route("/:bucket", put(handlers::create_bucket))
            .with_state(state.clone());

        let _ = router1
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;

        let router2 = Router::new()
            .route("/:bucket", delete(handlers::delete_bucket))
            .with_state(state);

        let response = router2
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_bucket_crud_full() {
        let state = create_test_state().await;
        let router = Router::new()
            .route("/:bucket", put(handlers::create_bucket))
            .route("/:bucket", delete(handlers::delete_bucket))
            .route("/:bucket", head(handlers::head_bucket))
            .with_state(state);

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/my-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/my-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/my-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/my-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_list_objects() {
        let state = create_test_state().await;
        let router = Router::new()
            .route("/:bucket", put(handlers::create_bucket))
            .route("/:bucket", get(handlers::list_objects))
            .with_state(state);

        let _ = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_head_object_not_found() {
        let state = create_test_state().await;
        let router = Router::new()
            .route("/:bucket", put(handlers::create_bucket))
            .route("/:bucket/*key", head(handlers::head_object))
            .with_state(state);

        let _ = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/test-bucket/test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    #[ignore = "requires Volume nodes to be available"]
    async fn test_multipart_upload_lifecycle() {
        let state = create_test_state().await;
        let router = Router::new()
            .route("/:bucket", put(handlers::create_bucket))
            .route("/:bucket/*key", post(handlers::object_post_handler))
            .route("/:bucket/*key", put(handlers::object_put_handler))
            .route("/:bucket/*key", get(handlers::object_get_handler))
            .route("/:bucket/*key", delete(handlers::object_delete_handler))
            .with_state(state);

        let _ = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;

        let init_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/test-bucket/test-object")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(init_response.status(), StatusCode::OK);

        let mut body = init_response.into_body();
        let mut init_body = Vec::new();
        while let Some(chunk) = body.data().await {
            init_body.extend_from_slice(&chunk.unwrap());
        }
        let init_body_str = String::from_utf8_lossy(&init_body);
        let upload_id_start = init_body_str.find("<UploadId>").unwrap() + 10;
        let upload_id_end = init_body_str.find("</UploadId>").unwrap();
        let upload_id = &init_body_str[upload_id_start..upload_id_end];

        let part1_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!(
                        "/test-bucket/test-object?uploadId={}&partNumber=1",
                        upload_id
                    ))
                    .body(Body::from("part1 data"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(part1_response.status(), StatusCode::OK);

        let part2_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!(
                        "/test-bucket/test-object?uploadId={}&partNumber=2",
                        upload_id
                    ))
                    .body(Body::from("part2 data"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(part2_response.status(), StatusCode::OK);

        let list_parts_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/test-bucket/test-object?uploadId={}", upload_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_parts_response.status(), StatusCode::OK);

        let complete_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/test-bucket/test-object?uploadId={}", upload_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(complete_response.status(), StatusCode::OK);

        let delete_response = router
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/test-bucket/test-object")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    #[ignore = "requires Volume nodes to be available"]
    async fn test_abort_multipart_upload() {
        let state = create_test_state().await;
        let router = Router::new()
            .route("/:bucket", put(handlers::create_bucket))
            .route("/:bucket/*key", post(handlers::object_post_handler))
            .route("/:bucket/*key", delete(handlers::object_delete_handler))
            .with_state(state);

        let _ = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;

        let init_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/test-bucket/test-object")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(init_response.status(), StatusCode::OK);

        let mut body = init_response.into_body();
        let mut init_body = Vec::new();
        while let Some(chunk) = body.data().await {
            init_body.extend_from_slice(&chunk.unwrap());
        }
        let init_body_str = String::from_utf8_lossy(&init_body);
        let upload_id_start = init_body_str.find("<UploadId>").unwrap() + 10;
        let upload_id_end = init_body_str.find("</UploadId>").unwrap();
        let upload_id = &init_body_str[upload_id_start..upload_id_end];

        let abort_response = router
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!("/test-bucket/test-object?uploadId={}", upload_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(abort_response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_invalid_upload_id() {
        let state = create_test_state().await;
        let router = Router::new()
            .route("/:bucket", put(handlers::create_bucket))
            .route("/:bucket/*key", get(handlers::object_get_handler))
            .route("/:bucket/*key", delete(handlers::object_delete_handler))
            .with_state(state);

        let _ = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;

        let list_parts_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/test-bucket/test-object?uploadId=invalid-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_parts_response.status(), StatusCode::NOT_FOUND);

        let abort_response = router
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/test-bucket/test-object?uploadId=invalid-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(abort_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_nonexistent_bucket() {
        let state = create_test_state().await;
        let router = Router::new()
            .route("/:bucket/*key", put(handlers::object_put_handler))
            .route("/:bucket/*key", get(handlers::object_get_handler))
            .route("/:bucket/*key", head(handlers::head_object))
            .with_state(state);

        let put_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/nonexistent/test-key")
                    .body(Body::from("test data"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_response.status(), StatusCode::NOT_FOUND);

        let get_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/nonexistent/test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::NOT_FOUND);

        let head_response = router
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/nonexistent/test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_response.status(), StatusCode::NOT_FOUND);
    }
}
