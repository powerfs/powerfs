use powerfs_master::proto::powerfs::kv_cache_service_client::KvCacheServiceClient;
use powerfs_master::proto::powerfs::*;
use tonic::transport::Channel;

#[derive(Debug, thiserror::Error)]
pub enum KvCacheClientError {
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("server error: {0}")]
    Server(String),
}

pub struct KvCacheClient {
    client: KvCacheServiceClient<Channel>,
}

impl KvCacheClient {
    pub async fn connect(addr: &str) -> Result<Self, KvCacheClientError> {
        let addr = if !addr.starts_with("http://") {
            format!("http://{}", addr)
        } else {
            addr.to_string()
        };

        let client = KvCacheServiceClient::connect(addr).await?;
        Ok(Self { client })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_session(
        &mut self,
        session_id: &str,
        model_name: &str,
        num_layers: u32,
        num_heads: u32,
        head_dim: u32,
        dtype: &str,
        ttl_seconds: u64,
    ) -> Result<(), KvCacheClientError> {
        let req = CreateSessionRequest {
            session_id: session_id.to_string(),
            model_name: model_name.to_string(),
            num_layers,
            num_heads,
            head_dim,
            dtype: dtype.to_string(),
            ttl_seconds,
        };

        let resp = self.client.create_session(req).await?.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(KvCacheClientError::Server(resp.error))
        }
    }

    pub async fn delete_session(&mut self, session_id: &str) -> Result<(), KvCacheClientError> {
        let req = DeleteSessionRequest {
            session_id: session_id.to_string(),
        };

        let resp = self.client.delete_session(req).await?.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(KvCacheClientError::Server(resp.error))
        }
    }

    pub async fn get_session(
        &mut self,
        session_id: &str,
    ) -> Result<Option<GetSessionResponse>, KvCacheClientError> {
        let req = GetSessionRequest {
            session_id: session_id.to_string(),
        };

        let resp = self.client.get_session(req).await?.into_inner();
        if resp.exists {
            Ok(Some(resp))
        } else {
            Ok(None)
        }
    }

    pub async fn put_block(
        &mut self,
        session_id: &str,
        layer_id: u32,
        num_tokens: u32,
        data: Vec<u8>,
    ) -> Result<u64, KvCacheClientError> {
        let req = PutBlockRequest {
            session_id: session_id.to_string(),
            layer_id,
            num_tokens,
            data,
        };

        let resp = self.client.put_block(req).await?.into_inner();
        if resp.success {
            Ok(resp.block_id)
        } else {
            Err(KvCacheClientError::Server(resp.error))
        }
    }

    pub async fn get_block(
        &mut self,
        block_id: u64,
    ) -> Result<Option<GetBlockResponse>, KvCacheClientError> {
        let req = GetBlockRequest { block_id };

        let resp = self.client.get_block(req).await?.into_inner();
        if resp.found {
            Ok(Some(resp))
        } else {
            Ok(None)
        }
    }

    pub async fn batch_put(
        &mut self,
        blocks: Vec<PutBlockRequest>,
    ) -> Result<Vec<PutBlockResponse>, KvCacheClientError> {
        let req = BatchPutRequest { blocks };
        let resp = self.client.batch_put(req).await?.into_inner();
        Ok(resp.results)
    }

    pub async fn batch_get(
        &mut self,
        block_ids: Vec<u64>,
    ) -> Result<Vec<GetBlockResponse>, KvCacheClientError> {
        let req = BatchGetRequest { block_ids };
        let resp = self.client.batch_get(req).await?.into_inner();
        Ok(resp.blocks)
    }

    pub async fn list_sessions(
        &mut self,
        limit: u32,
        prefix: &str,
    ) -> Result<(Vec<String>, u64), KvCacheClientError> {
        let req = ListSessionsRequest {
            limit,
            prefix: prefix.to_string(),
        };

        let resp = self.client.list_sessions(req).await?.into_inner();
        Ok((resp.session_ids, resp.total))
    }

    pub async fn get_stats(&mut self) -> Result<GetStatsResponse, KvCacheClientError> {
        let req = GetStatsRequest {};
        let resp = self.client.get_stats(req).await?.into_inner();
        Ok(resp)
    }
}
