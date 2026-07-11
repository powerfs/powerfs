use crate::master::MasterNode;
use crate::proto::powerfs::kv_cache_service_server::KvCacheService;
use crate::proto::powerfs::*;
use crate::proto::Location;
use crate::volume_client::VolumeClientPool;
use powerfs_common::types::{DataNodeInfo, Fid, VolumeId};
use powerfs_core::kv_cache::{KVBlockMeta, KVCacheEngine, KVDtype};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct KvCacheServiceImpl {
    pub engine: Arc<KVCacheEngine>,
    pub volume_client_pool: Arc<VolumeClientPool>,
    pub master: Arc<MasterNode>,
}

impl KvCacheServiceImpl {
    fn get_volume_nodes(&self, volume_id: VolumeId) -> Vec<DataNodeInfo> {
        if let Some(vol_info) = self.master.get_volume_info(&volume_id) {
            if let Some(node) = self.master.get_node_info(&vol_info.node_id) {
                return vec![node];
            }
        }
        Vec::new()
    }

    fn get_volume_address(&self, volume_id: VolumeId) -> Option<String> {
        let nodes = self.get_volume_nodes(volume_id);
        nodes
            .first()
            .map(|n| format!("{}:{}", n.address, n.grpc_port))
    }

    fn get_fid_locations(&self, fid_str: &str) -> Vec<Location> {
        let mut locations = Vec::new();
        if let Ok(fid) = Fid::from_string(fid_str) {
            let nodes = self.get_volume_nodes(fid.volume_id);
            for node in nodes {
                locations.push(Location {
                    url: format!("{}:{}", node.address, node.grpc_port),
                    public_url: node.public_url.clone(),
                    grpc_port: node.grpc_port,
                    data_center: node.data_center_id.0.clone(),
                });
            }
        }
        locations
    }
}

#[tonic::async_trait]
impl KvCacheService for KvCacheServiceImpl {
    async fn create_session(
        &self,
        request: Request<CreateSessionRequest>,
    ) -> Result<Response<CreateSessionResponse>, Status> {
        let req = request.into_inner();
        let dtype = KVDtype::parse(&req.dtype).unwrap_or(KVDtype::FP16);

        let result = self.engine.create_session(
            &req.session_id,
            &req.namespace_id,
            &req.owner_id,
            &req.model_name,
            req.num_layers,
            req.num_heads,
            req.head_dim,
            dtype,
            req.ttl_seconds,
        );

        match result {
            Ok(()) => {
                let meta = powerfs_core::kv_cache_persist::SessionMeta {
                    session_id: req.session_id.clone(),
                    model_name: req.model_name.clone(),
                    num_layers: req.num_layers,
                    num_heads: req.num_heads,
                    head_dim: req.head_dim,
                    dtype: dtype.as_str().to_string(),
                    block_ids: Vec::new(),
                    ttl_seconds: req.ttl_seconds,
                };
                let _ = self.master.kv_persist.save_session(&req.session_id, &meta);

                let kv_dir = format!("/kv/{}", req.session_id);
                let _ = self.master.directory_tree.create_directory(&kv_dir);

                Ok(Response::new(CreateSessionResponse {
                    success: true,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(CreateSessionResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn delete_session(
        &self,
        request: Request<DeleteSessionRequest>,
    ) -> Result<Response<DeleteSessionResponse>, Status> {
        let req = request.into_inner();
        let result = self.engine.delete_session(&req.session_id);

        match result {
            Ok(()) => {
                let _ = self.master.kv_persist.delete_session(&req.session_id);
                Ok(Response::new(DeleteSessionResponse {
                    success: true,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(DeleteSessionResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn get_session(
        &self,
        request: Request<GetSessionRequest>,
    ) -> Result<Response<GetSessionResponse>, Status> {
        let req = request.into_inner();
        let session = self.engine.get_session(&req.session_id);

        match session {
            Some(sess) => {
                let blocks = self.engine.get_session_blocks(&req.session_id);
                let total_tokens: u64 = blocks.iter().map(|b| b.num_tokens as u64).sum();
                let used_bytes: u64 = blocks.iter().map(|b| b.size_bytes).sum();

                Ok(Response::new(GetSessionResponse {
                    exists: true,
                    session_id: sess.session_id,
                    model_name: sess.model_name,
                    num_layers: sess.num_layers,
                    num_blocks: sess.block_ids.len() as u64,
                    total_tokens,
                    used_bytes,
                }))
            }
            None => Ok(Response::new(GetSessionResponse {
                exists: false,
                session_id: req.session_id,
                model_name: String::new(),
                num_layers: 0,
                num_blocks: 0,
                total_tokens: 0,
                used_bytes: 0,
            })),
        }
    }

    async fn put_block(
        &self,
        request: Request<PutBlockRequest>,
    ) -> Result<Response<PutBlockResponse>, Status> {
        let req = request.into_inner();

        if self.engine.get_session(&req.session_id).is_none() {
            return Ok(Response::new(PutBlockResponse {
                success: false,
                block_id: 0,
                error: "session not found".to_string(),
                fid: String::new(),
            }));
        }

        let (fid, nodes) = match self.master.assign_volume("001", "default").await {
            Ok(r) => r,
            Err(e) => {
                return Ok(Response::new(PutBlockResponse {
                    success: false,
                    block_id: 0,
                    error: format!("failed to assign volume: {}", e),
                    fid: String::new(),
                }));
            }
        };

        let fid_str = fid.to_string();

        let result = self.engine.put_block(
            &req.session_id,
            req.layer_id,
            req.num_tokens,
            &req.data,
            &fid_str,
            0,
            powerfs_core::kv_cache::PinMode::None,
        );

        match result {
            Ok(block_id) => {
                let volume_address = match self.get_volume_address(fid.volume_id) {
                    Some(a) => a,
                    None => {
                        return Ok(Response::new(PutBlockResponse {
                            success: false,
                            block_id,
                            error: "volume not found in topology".to_string(),
                            fid: fid_str,
                        }));
                    }
                };

                match self
                    .volume_client_pool
                    .write_needle(&volume_address, fid.volume_id.0, fid.file_key, &req.data)
                    .await
                {
                    Ok(_) => {
                        let _ = self.master.kv_persist.save_block_fid(block_id, &fid_str);

                        let layer_dir = format!("/kv/{}/layer_{}", req.session_id, req.layer_id);
                        let _ = self.master.directory_tree.create_directory(&layer_dir);

                        let block_file = format!("block_{}.data", block_id);
                        let file_entry = crate::proto::Entry {
                            name: block_file,
                            directory: layer_dir,
                            attributes: Some(crate::proto::FuseAttributes {
                                ino: 0,
                                mode: 0o100644,
                                nlink: 1,
                                uid: 0,
                                gid: 0,
                                rdev: 0,
                                size: req.data.len() as u64,
                                blksize: 4096,
                                blocks: req.data.len().div_ceil(4096) as u64,
                                atime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                                mtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                                ctime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                                crtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                                    as u64,
                                perm: 0o644,
                            }),
                            chunks: vec![crate::proto::FileChunk {
                                offset: 0,
                                size: req.data.len() as u64,
                                mtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                                fid: fid_str.clone(),
                                cookie: 0,
                                crc32: 0,
                            }],
                            hard_link_id: "".to_string(),
                            hard_link_counter: 0,
                            extended: std::collections::HashMap::new(),
                            content_size: req.data.len() as u64,
                            disk_size: req.data.len() as u64,
                            ttl: "".to_string(),
                            symlink_target: "".to_string(),
                            owner: String::new(),
                            generation: 0,
                        };
                        let _ = self.master.directory_tree.create_entry(file_entry, "");

                        let mut locations = Vec::new();
                        for node in nodes {
                            locations.push(Location {
                                url: format!("{}:{}", node.address, node.grpc_port),
                                public_url: node.public_url.clone(),
                                grpc_port: node.grpc_port,
                                data_center: node.data_center_id.0.clone(),
                            });
                        }

                        Ok(Response::new(PutBlockResponse {
                            success: true,
                            block_id,
                            error: String::new(),
                            fid: fid_str,
                        }))
                    }
                    Err(e) => Ok(Response::new(PutBlockResponse {
                        success: false,
                        block_id,
                        error: format!("failed to write to volume: {}", e),
                        fid: fid_str,
                    })),
                }
            }
            Err(e) => Ok(Response::new(PutBlockResponse {
                success: false,
                block_id: 0,
                error: e,
                fid: String::new(),
            })),
        }
    }

    async fn get_block(
        &self,
        request: Request<GetBlockRequest>,
    ) -> Result<Response<GetBlockResponse>, Status> {
        let req = request.into_inner();

        if let Some((meta, data)) = self.engine.get_block_data(req.block_id) {
            let locations = self.get_fid_locations(&meta.fid);
            Ok(Response::new(GetBlockResponse {
                found: true,
                block_id: meta.block_id,
                layer_id: meta.layer_id,
                num_tokens: meta.num_tokens,
                data,
                error: String::new(),
                fid: meta.fid,
                volume_locations: locations,
            }))
        } else {
            let fid = self.engine.get_fid_by_block_id(req.block_id);
            if let Some(fid_str) = fid {
                if let Ok(f) = Fid::from_string(&fid_str) {
                    let volume_address = match self.get_volume_address(f.volume_id) {
                        Some(a) => a,
                        None => {
                            return Ok(Response::new(GetBlockResponse {
                                found: false,
                                block_id: req.block_id,
                                layer_id: 0,
                                num_tokens: 0,
                                data: Vec::new(),
                                error: "volume not found in topology".to_string(),
                                fid: fid_str,
                                volume_locations: Vec::new(),
                            }));
                        }
                    };

                    match self
                        .volume_client_pool
                        .read_needle(&volume_address, f.volume_id.0, f.file_key)
                        .await
                    {
                        Ok(data) => {
                            let session = self.engine.get_session_by_block_id(req.block_id);
                            if let Some(sess) = session {
                                let meta = KVBlockMeta {
                                    block_id: req.block_id,
                                    session_id: sess.session_id,
                                    layer_id: 0,
                                    num_tokens: (data.len()
                                        / (sess.head_dim as usize * sess.num_heads as usize * 2))
                                        .try_into()
                                        .unwrap_or(0),
                                    dtype: sess.dtype,
                                    head_dim: sess.head_dim,
                                    num_heads: sess.num_heads,
                                    size_bytes: data.len() as u64,
                                    created_at: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs(),
                                    last_accessed: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs(),
                                    ttl: sess.ttl,
                                    fid: fid_str.clone(),
                                    namespace_id: sess.namespace_id,
                                    owner_id: sess.owner_id,
                                    block_index: 0,
                                    pin_mode: powerfs_core::kv_cache::PinMode::None,
                                };
                                let locations = self.get_fid_locations(&fid_str);
                                Ok(Response::new(GetBlockResponse {
                                    found: true,
                                    block_id: meta.block_id,
                                    layer_id: meta.layer_id,
                                    num_tokens: meta.num_tokens,
                                    data,
                                    error: String::new(),
                                    fid: meta.fid,
                                    volume_locations: locations,
                                }))
                            } else {
                                Ok(Response::new(GetBlockResponse {
                                    found: false,
                                    block_id: req.block_id,
                                    layer_id: 0,
                                    num_tokens: 0,
                                    data: Vec::new(),
                                    error: "session not found for block".to_string(),
                                    fid: fid_str,
                                    volume_locations: Vec::new(),
                                }))
                            }
                        }
                        Err(e) => Ok(Response::new(GetBlockResponse {
                            found: false,
                            block_id: req.block_id,
                            layer_id: 0,
                            num_tokens: 0,
                            data: Vec::new(),
                            error: format!("failed to read from volume: {}", e),
                            fid: fid_str,
                            volume_locations: Vec::new(),
                        })),
                    }
                } else {
                    Ok(Response::new(GetBlockResponse {
                        found: false,
                        block_id: req.block_id,
                        layer_id: 0,
                        num_tokens: 0,
                        data: Vec::new(),
                        error: "invalid fid format".to_string(),
                        fid: fid_str,
                        volume_locations: Vec::new(),
                    }))
                }
            } else {
                Ok(Response::new(GetBlockResponse {
                    found: false,
                    block_id: req.block_id,
                    layer_id: 0,
                    num_tokens: 0,
                    data: Vec::new(),
                    error: "block not found".to_string(),
                    fid: String::new(),
                    volume_locations: Vec::new(),
                }))
            }
        }
    }

    async fn batch_put(
        &self,
        request: Request<BatchPutRequest>,
    ) -> Result<Response<BatchPutResponse>, Status> {
        let req = request.into_inner();
        let requests: Vec<powerfs_core::kv_cache::BatchPutRequest> = req
            .blocks
            .into_iter()
            .enumerate()
            .map(|(i, b)| {
                (
                    b.session_id,
                    b.layer_id,
                    b.num_tokens,
                    b.data,
                    "".to_string(),
                    i as u32,
                )
            })
            .collect();

        let results = self.engine.batch_put(&requests);
        let responses: Vec<PutBlockResponse> = results
            .into_iter()
            .map(|r| match r {
                Ok(block_id) => {
                    let fid = self
                        .engine
                        .get_fid_by_block_id(block_id)
                        .unwrap_or_default();
                    PutBlockResponse {
                        success: true,
                        block_id,
                        error: String::new(),
                        fid,
                    }
                }
                Err(e) => PutBlockResponse {
                    success: false,
                    block_id: 0,
                    error: e,
                    fid: String::new(),
                },
            })
            .collect();

        Ok(Response::new(BatchPutResponse { results: responses }))
    }

    async fn batch_get(
        &self,
        request: Request<BatchGetRequest>,
    ) -> Result<Response<BatchGetResponse>, Status> {
        let req = request.into_inner();
        let results = self.engine.batch_get(&req.block_ids);
        let responses: Vec<GetBlockResponse> = results
            .into_iter()
            .map(|r| match r {
                Some((meta, data)) => GetBlockResponse {
                    found: true,
                    block_id: meta.block_id,
                    layer_id: meta.layer_id,
                    num_tokens: meta.num_tokens,
                    data,
                    error: String::new(),
                    fid: meta.fid,
                    volume_locations: Vec::new(),
                },
                None => GetBlockResponse {
                    found: false,
                    block_id: 0,
                    layer_id: 0,
                    num_tokens: 0,
                    data: Vec::new(),
                    error: "block not found".to_string(),
                    fid: String::new(),
                    volume_locations: Vec::new(),
                },
            })
            .collect();

        Ok(Response::new(BatchGetResponse { blocks: responses }))
    }

    async fn list_sessions(
        &self,
        request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 { 100 } else { req.limit };
        let (ids, total) = self.engine.list_sessions(limit, &req.prefix);

        Ok(Response::new(ListSessionsResponse {
            session_ids: ids,
            total,
        }))
    }

    async fn get_stats(
        &self,
        _request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        let stats = self.engine.stats();

        Ok(Response::new(GetStatsResponse {
            total_blocks: stats.total_blocks,
            total_sessions: stats.total_sessions,
            used_memory_bytes: stats.used_memory_bytes,
            max_memory_bytes: self.engine.max_memory_bytes(),
            cache_hits: stats.hits,
            cache_misses: stats.misses,
            evictions: stats.evictions,
        }))
    }

    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .engine
            .create_namespace(&req.namespace_id, &req.name, &req.owner_id);

        match result {
            Ok(()) => Ok(Response::new(CreateNamespaceResponse {
                success: true,
                error: String::new(),
                namespace_id: req.namespace_id,
            })),
            Err(e) => Ok(Response::new(CreateNamespaceResponse {
                success: false,
                error: e,
                namespace_id: String::new(),
            })),
        }
    }

    async fn list_namespaces(
        &self,
        request: Request<ListNamespacesRequest>,
    ) -> Result<Response<ListNamespacesResponse>, Status> {
        let req = request.into_inner();
        let namespaces = self.engine.list_namespaces(&req.owner_id);

        let proto_namespaces: Vec<KvNamespace> = namespaces
            .into_iter()
            .map(|ns| KvNamespace {
                id: ns.id,
                name: ns.name,
                owner_id: ns.owner_id,
                created_at: ns.created_at,
                updated_at: ns.updated_at,
            })
            .collect();

        Ok(Response::new(ListNamespacesResponse {
            namespaces: proto_namespaces,
            error: String::new(),
        }))
    }

    async fn get_namespace(
        &self,
        request: Request<GetNamespaceRequest>,
    ) -> Result<Response<GetNamespaceResponse>, Status> {
        let req = request.into_inner();
        let namespace = self.engine.get_namespace(&req.namespace_id);

        match namespace {
            Some(ns) => Ok(Response::new(GetNamespaceResponse {
                found: true,
                namespace: Some(KvNamespace {
                    id: ns.id,
                    name: ns.name,
                    owner_id: ns.owner_id,
                    created_at: ns.created_at,
                    updated_at: ns.updated_at,
                }),
                error: String::new(),
            })),
            None => Ok(Response::new(GetNamespaceResponse {
                found: false,
                namespace: None,
                error: "namespace not found".to_string(),
            })),
        }
    }

    async fn delete_namespace(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .engine
            .delete_namespace(&req.namespace_id, &req.owner_id);

        match result {
            Ok(()) => Ok(Response::new(DeleteNamespaceResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(DeleteNamespaceResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn kv_put(&self, request: Request<KvPutRequest>) -> Result<Response<KvResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .engine
            .kv_put(&req.namespace_id, &req.key, &req.value, &req.owner_id);

        match result {
            Ok(()) => Ok(Response::new(KvResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(KvResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn kv_get(
        &self,
        request: Request<KvGetRequest>,
    ) -> Result<Response<KvGetResponse>, Status> {
        let req = request.into_inner();
        let result = self.engine.kv_get(&req.namespace_id, &req.key);

        match result {
            Ok(Some(value)) => Ok(Response::new(KvGetResponse {
                success: true,
                error: String::new(),
                value: value.data,
                found: true,
            })),
            Ok(None) => Ok(Response::new(KvGetResponse {
                success: true,
                error: String::new(),
                value: Vec::new(),
                found: false,
            })),
            Err(e) => Ok(Response::new(KvGetResponse {
                success: false,
                error: e,
                value: Vec::new(),
                found: false,
            })),
        }
    }

    async fn kv_delete(
        &self,
        request: Request<KvDeleteRequest>,
    ) -> Result<Response<KvResponse>, Status> {
        let req = request.into_inner();
        let result = self.engine.kv_delete(&req.namespace_id, &req.key);

        match result {
            Ok(_) => Ok(Response::new(KvResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(KvResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn kv_exists(
        &self,
        request: Request<KvExistsRequest>,
    ) -> Result<Response<KvExistsResponse>, Status> {
        let req = request.into_inner();
        let result = self.engine.kv_exists(&req.namespace_id, &req.key);

        match result {
            Ok(exists) => Ok(Response::new(KvExistsResponse {
                exists,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(KvExistsResponse {
                exists: false,
                error: e,
            })),
        }
    }

    async fn kv_list(
        &self,
        request: Request<KvListRequest>,
    ) -> Result<Response<KvListResponse>, Status> {
        let req = request.into_inner();
        let prefix = if req.prefix.is_empty() {
            None
        } else {
            Some(req.prefix.as_str())
        };
        let result = self.engine.kv_list(&req.namespace_id, prefix);

        match result {
            Ok(keys) => Ok(Response::new(KvListResponse {
                keys,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(KvListResponse {
                keys: Vec::new(),
                error: e,
            })),
        }
    }

    async fn kv_remove_by_regex(
        &self,
        request: Request<KvRemoveByRegexRequest>,
    ) -> Result<Response<KvResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .engine
            .kv_remove_by_regex(&req.namespace_id, &req.pattern);

        match result {
            Ok(_) => Ok(Response::new(KvResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(KvResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn kv_remove_all(
        &self,
        request: Request<KvRemoveAllRequest>,
    ) -> Result<Response<KvResponse>, Status> {
        let req = request.into_inner();
        let result = self.engine.kv_remove_all(&req.namespace_id);

        match result {
            Ok(_) => Ok(Response::new(KvResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(KvResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn kv_batch_put(
        &self,
        request: Request<KvBatchPutRequest>,
    ) -> Result<Response<KvBatchResponse>, Status> {
        let req = request.into_inner();
        let mut successes = Vec::new();

        for (key, value) in req.keys.iter().zip(req.values.iter()) {
            let result = self
                .engine
                .kv_put(&req.namespace_id, key, value, &req.owner_id);
            successes.push(result.is_ok());
        }

        Ok(Response::new(KvBatchResponse {
            successes,
            error: String::new(),
        }))
    }

    async fn kv_batch_get(
        &self,
        request: Request<KvBatchGetRequest>,
    ) -> Result<Response<KvBatchGetResponse>, Status> {
        let req = request.into_inner();
        let mut values = Vec::new();
        let mut found = Vec::new();

        for key in &req.keys {
            match self.engine.kv_get(&req.namespace_id, key) {
                Ok(Some(v)) => {
                    values.push(v.data);
                    found.push(true);
                }
                Ok(None) => {
                    values.push(Vec::new());
                    found.push(false);
                }
                Err(_) => {
                    values.push(Vec::new());
                    found.push(false);
                }
            }
        }

        Ok(Response::new(KvBatchGetResponse {
            values,
            found,
            error: String::new(),
        }))
    }
}
