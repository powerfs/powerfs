use crate::proto::powerfs::kv_cache_service_server::KvCacheService;
use crate::proto::powerfs::*;
use powerfs_core::kv_cache::{KVCacheEngine, KVDtype};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct KvCacheServiceImpl {
    pub engine: Arc<KVCacheEngine>,
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
            &req.model_name,
            req.num_layers,
            req.num_heads,
            req.head_dim,
            dtype,
            req.ttl_seconds,
        );

        match result {
            Ok(()) => Ok(Response::new(CreateSessionResponse {
                success: true,
                error: String::new(),
            })),
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
            Ok(()) => Ok(Response::new(DeleteSessionResponse {
                success: true,
                error: String::new(),
            })),
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
        let result =
            self.engine
                .put_block(&req.session_id, req.layer_id, req.num_tokens, &req.data);

        match result {
            Ok(block_id) => Ok(Response::new(PutBlockResponse {
                success: true,
                block_id,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(PutBlockResponse {
                success: false,
                block_id: 0,
                error: e,
            })),
        }
    }

    async fn get_block(
        &self,
        request: Request<GetBlockRequest>,
    ) -> Result<Response<GetBlockResponse>, Status> {
        let req = request.into_inner();
        let result = self.engine.get_block_data(req.block_id);

        match result {
            Some((meta, data)) => Ok(Response::new(GetBlockResponse {
                found: true,
                block_id: meta.block_id,
                layer_id: meta.layer_id,
                num_tokens: meta.num_tokens,
                data,
                error: String::new(),
            })),
            None => Ok(Response::new(GetBlockResponse {
                found: false,
                block_id: req.block_id,
                layer_id: 0,
                num_tokens: 0,
                data: Vec::new(),
                error: "block not found".to_string(),
            })),
        }
    }

    async fn batch_put(
        &self,
        request: Request<BatchPutRequest>,
    ) -> Result<Response<BatchPutResponse>, Status> {
        let req = request.into_inner();
        let requests: Vec<(String, u32, u32, Vec<u8>)> = req
            .blocks
            .into_iter()
            .map(|b| (b.session_id, b.layer_id, b.num_tokens, b.data))
            .collect();

        let results = self.engine.batch_put(&requests);
        let responses: Vec<PutBlockResponse> = results
            .into_iter()
            .map(|r| match r {
                Ok(block_id) => PutBlockResponse {
                    success: true,
                    block_id,
                    error: String::new(),
                },
                Err(e) => PutBlockResponse {
                    success: false,
                    block_id: 0,
                    error: e,
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
                },
                None => GetBlockResponse {
                    found: false,
                    block_id: 0,
                    layer_id: 0,
                    num_tokens: 0,
                    data: Vec::new(),
                    error: "block not found".to_string(),
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
}
