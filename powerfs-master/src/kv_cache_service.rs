use crate::master::MasterNode;
use crate::proto::powerfs::kv_cache_service_server::KvCacheService;
use crate::proto::powerfs::*;
use crate::proto::Location;
use crate::raft_v2::{KvPayload, RaftCommand, KV_INLINE_LIMIT};
use crate::volume_client::VolumeClientPool;
use powerfs_common::types::{DataNodeInfo, Fid, VolumeId};
use powerfs_core::kv_cache::{KVCacheEngine, KVDtype, KVExternalRef};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Current wall time in milliseconds (version clock).
pub(crate) fn now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Build a fencing token for a replicated write: the current raft term in the
/// high 64 bits and wall-clock milliseconds in the low 64 bits. The term
/// strictly increases across leaders, so a new leader's writes always order
/// after every prior term even if its wall clock is behind; within a term the
/// (monotonic) wall clock orders writes. This replaces a raw wall-clock
/// version, which could silently drop a new leader's write (and still return
/// success) after a failover with clock skew.
pub(crate) fn fencing_version(term: u64) -> u128 {
    ((term as u128) << 64) | (now_millis() & 0xFFFF_FFFF_FFFF_FFFF)
}

pub struct KvCacheServiceImpl {
    pub engine: Arc<KVCacheEngine>,
    pub volume_client_pool: Arc<VolumeClientPool>,
    pub master: Arc<MasterNode>,
}

impl KvCacheServiceImpl {
    /// Map a raft proposal result into the generic KV success/error shape.
    async fn propose_kv(&self, cmd: RaftCommand) -> KvResponse {
        match self.master.propose_command(cmd).await {
            Ok(_) => KvResponse {
                success: true,
                error: String::new(),
            },
            Err(e) => KvResponse {
                success: false,
                error: format!("{}", e),
            },
        }
    }

    /// Resolve a generic KV value: inline bytes or, for an external marker,
    /// the bytes stored in the referenced volume needle.
    async fn resolve_value(&self, namespace_id: &str, key: &str) -> Option<Vec<u8>> {
        let raw = self.engine.get_raw_slot(namespace_id, key)?;
        if let Some(marker) = KVExternalRef::parse_marker(&raw) {
            let fid = Fid::from_string(&marker.fid).ok()?;
            let addr = self.get_volume_address(fid.volume_id)?;
            self.volume_client_pool
                .read_needle(&addr, fid.volume_id.0, fid.file_key)
                .await
                .ok()
        } else {
            serde_json::from_slice::<powerfs_core::kv_cache::KVStoredValue>(&raw)
                .ok()
                .map(|v| v.data)
        }
    }

    /// Persist a large value into a volume needle and return the External
    /// payload that replicates only its fid. Bytes are durably stored before
    /// any marker is replicated.
    async fn write_external(
        &self,
        data: &[u8],
        collection: &str,
        _namespace_id: &str,
    ) -> Result<KvPayload, String> {
        let collection = if collection.is_empty() {
            "default"
        } else {
            collection
        };
        let (fid, _nodes) = self
            .master
            .assign_volume("001", collection)
            .await
            .map_err(|e| format!("failed to assign volume: {}", e))?;
        let addr = self
            .get_volume_address(fid.volume_id)
            .ok_or_else(|| "volume not found in topology".to_string())?;
        self.volume_client_pool
            .write_needle(&addr, fid.volume_id.0, fid.file_key, data)
            .await
            .map_err(|e| format!("failed to write to volume: {}", e))?;
        Ok(KvPayload::External {
            fid: fid.to_string(),
            size: data.len() as u64,
        })
    }

    /// Fetch one block: local in-memory copy if present, otherwise the bytes
    /// from the volume needle referenced by its replicated mapping.
    async fn fetch_block(&self, block_id: u64) -> GetBlockResponse {
        if let Some((meta, data)) = self.engine.get_block_data(block_id) {
            let locations = self.get_fid_locations(&meta.fid);
            return GetBlockResponse {
                found: true,
                block_id: meta.block_id,
                layer_id: meta.layer_id,
                num_tokens: meta.num_tokens,
                data,
                error: String::new(),
                fid: meta.fid,
                volume_locations: locations,
            };
        }

        let fid_str = match self.engine.get_fid_by_block_id(block_id) {
            Some(f) => f,
            None => {
                return GetBlockResponse {
                    found: false,
                    block_id,
                    layer_id: 0,
                    num_tokens: 0,
                    data: Vec::new(),
                    error: "block not found".to_string(),
                    fid: String::new(),
                    volume_locations: Vec::new(),
                };
            }
        };

        let f = match Fid::from_string(&fid_str) {
            Ok(f) => f,
            Err(_) => {
                return GetBlockResponse {
                    found: false,
                    block_id,
                    layer_id: 0,
                    num_tokens: 0,
                    data: Vec::new(),
                    error: "invalid fid format".to_string(),
                    fid: fid_str,
                    volume_locations: Vec::new(),
                };
            }
        };

        let addr = match self.get_volume_address(f.volume_id) {
            Some(a) => a,
            None => {
                return GetBlockResponse {
                    found: false,
                    block_id,
                    layer_id: 0,
                    num_tokens: 0,
                    data: Vec::new(),
                    error: "volume not found in topology".to_string(),
                    fid: fid_str,
                    volume_locations: Vec::new(),
                };
            }
        };

        match self
            .volume_client_pool
            .read_needle(&addr, f.volume_id.0, f.file_key)
            .await
        {
            Ok(data) => {
                let locations = self.get_fid_locations(&fid_str);
                // Derive layer/token info from the session when available.
                let (layer_id, num_tokens) = self
                    .engine
                    .get_session_by_block_id(block_id)
                    .map(|s| {
                        // Use checked division to guard against zero dims
                        // (proto defaults), which would otherwise panic.
                        let per_token = s.head_dim as usize * s.num_heads as usize * 2;
                        let tokens = data.len().checked_div(per_token).unwrap_or(0) as u32;
                        (0u32, tokens)
                    })
                    .unwrap_or((0, 0));
                GetBlockResponse {
                    found: true,
                    block_id,
                    layer_id,
                    num_tokens,
                    data,
                    error: String::new(),
                    fid: fid_str,
                    volume_locations: locations,
                }
            }
            Err(e) => GetBlockResponse {
                found: false,
                block_id,
                layer_id: 0,
                num_tokens: 0,
                data: Vec::new(),
                error: format!("failed to read from volume: {}", e),
                fid: fid_str,
                volume_locations: Vec::new(),
            },
        }
    }

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
        nodes.first().map(|n| {
            // grpc_port is reused as the powerfs-net data port (890x) in
            // data-plane deployments; the admin gRPC server (WriteNeedle)
            // uses admin_grpc_port (8080). Fall back for older nodes.
            let port = if n.admin_grpc_port > 0 {
                n.admin_grpc_port
            } else {
                n.grpc_port
            };
            format!("{}:{}", n.address, port)
        })
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
        let namespace_id = if req.namespace_id.is_empty() {
            "default".to_string()
        } else {
            req.namespace_id.clone()
        };
        let collection = if req.collection.is_empty() {
            "default".to_string()
        } else {
            req.collection.clone()
        };

        let cmd = RaftCommand::KvCreateSession {
            session_id: req.session_id.clone(),
            namespace_id,
            owner_id: req.owner_id.clone(),
            model_name: req.model_name.clone(),
            num_layers: req.num_layers,
            num_heads: req.num_heads,
            head_dim: req.head_dim,
            dtype: dtype.as_str().to_string(),
            ttl_seconds: req.ttl_seconds,
            collection,
        };

        match self.master.propose_command(cmd).await {
            Ok(_) => Ok(Response::new(CreateSessionResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(CreateSessionResponse {
                success: false,
                error: format!("{}", e),
            })),
        }
    }

    async fn delete_session(
        &self,
        request: Request<DeleteSessionRequest>,
    ) -> Result<Response<DeleteSessionResponse>, Status> {
        let req = request.into_inner();
        let cmd = RaftCommand::KvDeleteSession {
            session_id: req.session_id.clone(),
        };

        match self.master.propose_command(cmd).await {
            Ok(_) => Ok(Response::new(DeleteSessionResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(DeleteSessionResponse {
                success: false,
                error: format!("{}", e),
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

        let session = match self.engine.get_session(&req.session_id) {
            Some(s) => s,
            None => {
                return Ok(Response::new(PutBlockResponse {
                    success: false,
                    block_id: 0,
                    error: "session not found".to_string(),
                    fid: String::new(),
                }));
            }
        };

        // Use the session's collection so KV blocks land in the same volume
        // pool as FUSE/S3 data for that collection.
        let collection = if session.collection.is_empty() {
            "default".to_string()
        } else {
            session.collection
        };

        let (fid, _nodes) = match self.master.assign_volume("001", &collection).await {
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

        let volume_address = match self.get_volume_address(fid.volume_id) {
            Some(a) => a,
            None => {
                return Ok(Response::new(PutBlockResponse {
                    success: false,
                    block_id: 0,
                    error: "volume not found in topology".to_string(),
                    fid: fid_str,
                }));
            }
        };

        // Reserve an id, then persist bytes to volume BEFORE replicating the
        // mapping, so no node can ever reference a needle that doesn't exist.
        let block_id = self.engine.alloc_block_id();
        if let Err(e) = self
            .volume_client_pool
            .write_needle(&volume_address, fid.volume_id.0, fid.file_key, &req.data)
            .await
        {
            return Ok(Response::new(PutBlockResponse {
                success: false,
                block_id,
                error: format!("failed to write to volume: {}", e),
                fid: fid_str,
            }));
        }

        // Cache the bytes in the leader's in-memory block cache (fast reads +
        // stats). The bytes are already durable in the volume, so even if this
        // fails the block remains readable on demand; the raft entry below is
        // what propagates the id->fid mapping to followers.
        if let Err(e) = self.engine.store_leader_block(
            block_id,
            &req.session_id,
            req.layer_id,
            req.num_tokens,
            &req.data,
            &fid_str,
            0,
            powerfs_core::kv_cache::PinMode::None,
        ) {
            eprintln!(
                "[warn] block {} not cached in leader memory: {}",
                block_id, e
            );
        }

        let cmd = RaftCommand::KvSaveBlocks {
            blocks: vec![crate::raft_v2::KvBlockMeta {
                block_id,
                session_id: req.session_id.clone(),
                layer_id: req.layer_id,
                num_tokens: req.num_tokens,
                fid: fid_str.clone(),
            }],
        };

        match self.master.propose_command(cmd).await {
            Ok(_) => Ok(Response::new(PutBlockResponse {
                success: true,
                block_id,
                error: String::new(),
                fid: fid_str,
            })),
            Err(e) => Ok(Response::new(PutBlockResponse {
                success: false,
                block_id,
                error: format!("{}", e),
                fid: fid_str,
            })),
        }
    }

    async fn get_block(
        &self,
        request: Request<GetBlockRequest>,
    ) -> Result<Response<GetBlockResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(self.fetch_block(req.block_id).await))
    }

    async fn batch_put(
        &self,
        request: Request<BatchPutRequest>,
    ) -> Result<Response<BatchPutResponse>, Status> {
        let req = request.into_inner();

        // Per-block: validate session, assign a fid, reserve an id, write the
        // needle. Successful blocks are collected and replicated in ONE
        // KvSaveBlocks proposal; failed blocks report independently.
        let mut replicated: Vec<crate::raft_v2::KvBlockMeta> = Vec::new();
        let mut responses: Vec<PutBlockResponse> = Vec::with_capacity(req.blocks.len());

        for b in req.blocks {
            let session = match self.engine.get_session(&b.session_id) {
                Some(s) => s,
                None => {
                    responses.push(PutBlockResponse {
                        success: false,
                        block_id: 0,
                        error: "session not found".to_string(),
                        fid: String::new(),
                    });
                    continue;
                }
            };
            let collection = if session.collection.is_empty() {
                "default".to_string()
            } else {
                session.collection
            };

            let (fid, _nodes) = match self.master.assign_volume("001", &collection).await {
                Ok(r) => r,
                Err(e) => {
                    responses.push(PutBlockResponse {
                        success: false,
                        block_id: 0,
                        error: format!("failed to assign volume: {}", e),
                        fid: String::new(),
                    });
                    continue;
                }
            };
            let fid_str = fid.to_string();

            let addr = match self.get_volume_address(fid.volume_id) {
                Some(a) => a,
                None => {
                    responses.push(PutBlockResponse {
                        success: false,
                        block_id: 0,
                        error: "volume not found in topology".to_string(),
                        fid: fid_str,
                    });
                    continue;
                }
            };

            let block_id = self.engine.alloc_block_id();
            if let Err(e) = self
                .volume_client_pool
                .write_needle(&addr, fid.volume_id.0, fid.file_key, &b.data)
                .await
            {
                responses.push(PutBlockResponse {
                    success: false,
                    block_id,
                    error: format!("failed to write to volume: {}", e),
                    fid: fid_str,
                });
                continue;
            }

            // Cache bytes in the leader's memory cache (fast reads + stats).
            if let Err(e) = self.engine.store_leader_block(
                block_id,
                &b.session_id,
                b.layer_id,
                b.num_tokens,
                &b.data,
                &fid_str,
                0,
                powerfs_core::kv_cache::PinMode::None,
            ) {
                eprintln!(
                    "[warn] block {} not cached in leader memory: {}",
                    block_id, e
                );
            }

            replicated.push(crate::raft_v2::KvBlockMeta {
                block_id,
                session_id: b.session_id,
                layer_id: b.layer_id,
                num_tokens: b.num_tokens,
                fid: fid_str.clone(),
            });
            responses.push(PutBlockResponse {
                success: true,
                block_id,
                error: String::new(),
                fid: fid_str,
            });
        }

        if !replicated.is_empty() {
            let cmd = RaftCommand::KvSaveBlocks { blocks: replicated };
            if let Err(e) = self.master.propose_command(cmd).await {
                // Mark all previously-successful results as failed so the
                // client doesn't assume durability.
                let msg = format!("{}", e);
                for r in responses.iter_mut() {
                    if r.success {
                        r.success = false;
                        r.error = msg.clone();
                    }
                }
            }
        }

        Ok(Response::new(BatchPutResponse { results: responses }))
    }

    async fn batch_get(
        &self,
        request: Request<BatchGetRequest>,
    ) -> Result<Response<BatchGetResponse>, Status> {
        let req = request.into_inner();

        let futs = req.block_ids.iter().map(|id| self.fetch_block(*id));
        let blocks = futures::future::join_all(futs).await;

        Ok(Response::new(BatchGetResponse { blocks }))
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
        let version = fencing_version(self.master.current_term());
        let cmd = RaftCommand::KvCreateNamespace {
            namespace_id: req.namespace_id.clone(),
            name: req.name.clone(),
            owner_id: req.owner_id.clone(),
            version,
        };

        match self.master.propose_command(cmd).await {
            Ok(_) => Ok(Response::new(CreateNamespaceResponse {
                success: true,
                error: String::new(),
                namespace_id: req.namespace_id,
            })),
            Err(e) => Ok(Response::new(CreateNamespaceResponse {
                success: false,
                error: format!("{}", e),
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
        let cmd = RaftCommand::KvDeleteNamespace {
            namespace_id: req.namespace_id.clone(),
            owner_id: req.owner_id.clone(),
        };

        match self.master.propose_command(cmd).await {
            Ok(_) => Ok(Response::new(DeleteNamespaceResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(DeleteNamespaceResponse {
                success: false,
                error: format!("{}", e),
            })),
        }
    }

    async fn kv_put(&self, request: Request<KvPutRequest>) -> Result<Response<KvResponse>, Status> {
        let req = request.into_inner();
        let version = fencing_version(self.master.current_term());

        let payload = if req.value.len() <= KV_INLINE_LIMIT {
            KvPayload::Inline(req.value.clone())
        } else {
            match self.write_external(&req.value, "", &req.namespace_id).await {
                Ok(p) => p,
                Err(e) => {
                    return Ok(Response::new(KvResponse {
                        success: false,
                        error: e,
                    }));
                }
            }
        };

        let cmd = RaftCommand::KvPut {
            namespace_id: req.namespace_id,
            key: req.key,
            owner_id: req.owner_id,
            payload,
            version,
        };
        Ok(Response::new(self.propose_kv(cmd).await))
    }

    async fn kv_get(
        &self,
        request: Request<KvGetRequest>,
    ) -> Result<Response<KvGetResponse>, Status> {
        let req = request.into_inner();

        // Missing namespace -> surface as an error, consistent with engine.
        if self.engine.get_namespace(&req.namespace_id).is_none() {
            return Ok(Response::new(KvGetResponse {
                success: false,
                error: format!("namespace {} not found", req.namespace_id),
                value: Vec::new(),
                found: false,
            }));
        }

        match self.resolve_value(&req.namespace_id, &req.key).await {
            Some(value) => Ok(Response::new(KvGetResponse {
                success: true,
                error: String::new(),
                value,
                found: true,
            })),
            None => Ok(Response::new(KvGetResponse {
                success: true,
                error: String::new(),
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
        let cmd = RaftCommand::KvDelete {
            namespace_id: req.namespace_id,
            keys: vec![req.key],
        };
        Ok(Response::new(self.propose_kv(cmd).await))
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

        let re = match regex::Regex::new(&req.pattern) {
            Ok(r) => r,
            Err(e) => {
                return Ok(Response::new(KvResponse {
                    success: false,
                    error: format!("invalid regex: {}", e),
                }));
            }
        };

        // Enumerate on the leader at proposal time; replicate the exact key
        // set so followers needn't interpret the regex (and replicas which
        // lack a key simply no-op).
        let keys: Vec<String> = self
            .engine
            .enumerate_namespace_keys(&req.namespace_id)
            .into_iter()
            .filter(|k| re.is_match(k))
            .collect();

        let cmd = RaftCommand::KvDelete {
            namespace_id: req.namespace_id,
            keys,
        };
        Ok(Response::new(self.propose_kv(cmd).await))
    }

    async fn kv_remove_all(
        &self,
        request: Request<KvRemoveAllRequest>,
    ) -> Result<Response<KvResponse>, Status> {
        let req = request.into_inner();
        let keys = self.engine.enumerate_namespace_keys(&req.namespace_id);

        let cmd = RaftCommand::KvDelete {
            namespace_id: req.namespace_id,
            keys,
        };
        Ok(Response::new(self.propose_kv(cmd).await))
    }

    async fn kv_batch_put(
        &self,
        request: Request<KvBatchPutRequest>,
    ) -> Result<Response<KvBatchResponse>, Status> {
        let req = request.into_inner();
        let mut successes = Vec::with_capacity(req.keys.len());
        let mut first_error = String::new();

        let term = self.master.current_term();
        for (key, value) in req.keys.iter().zip(req.values.iter()) {
            let version = fencing_version(term);
            let payload = if value.len() <= KV_INLINE_LIMIT {
                KvPayload::Inline(value.clone())
            } else {
                match self.write_external(value, "", &req.namespace_id).await {
                    Ok(p) => p,
                    Err(e) => {
                        successes.push(false);
                        if first_error.is_empty() {
                            first_error = e;
                        }
                        continue;
                    }
                }
            };

            let cmd = RaftCommand::KvPut {
                namespace_id: req.namespace_id.clone(),
                key: key.clone(),
                owner_id: req.owner_id.clone(),
                payload,
                version,
            };
            let resp = self.propose_kv(cmd).await;
            if !resp.success && first_error.is_empty() {
                first_error = resp.error;
            }
            successes.push(resp.success);
        }

        Ok(Response::new(KvBatchResponse {
            successes,
            error: first_error,
        }))
    }

    async fn kv_batch_get(
        &self,
        request: Request<KvBatchGetRequest>,
    ) -> Result<Response<KvBatchGetResponse>, Status> {
        let req = request.into_inner();

        // Resolve each key (inline or external needle) concurrently.
        let futs = req
            .keys
            .iter()
            .map(|k| self.resolve_value(&req.namespace_id, k));
        let resolved = futures::future::join_all(futs).await;

        let mut values = Vec::with_capacity(resolved.len());
        let mut found = Vec::with_capacity(resolved.len());
        for v in resolved {
            match v {
                Some(data) => {
                    values.push(data);
                    found.push(true);
                }
                None => {
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
