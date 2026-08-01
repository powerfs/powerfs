//! 测试 mock：实现 DeltaSyncChannel / MetadataCacheInvalidator / CacheCoherence 的空实现，
//! 供 crate 内部测试与 fuse 端单测使用。

use std::sync::Arc;

use crate::{
    AllocInodeBatchRequest, AllocInodeBatchResponse, CacheCoherence, CacheKind, DeltaSyncChannel,
    DeltaWire, MetadataCacheInvalidator, PullDeltaRequest, PullDeltaResponse, PushDeltaRequest,
    PushDeltaResponse, UpdateInodeSizeChunksRequest, UpdateInodeSizeChunksResponse,
    ValidationResult, WriteOp,
};

/// 空的 DeltaSyncChannel mock（所有调用返回空成功）
#[derive(Debug, Default, Clone)]
pub struct MockDeltaSyncChannel;

#[async_trait::async_trait]
impl DeltaSyncChannel for MockDeltaSyncChannel {
    async fn push_delta(&self, _req: &PushDeltaRequest) -> Result<PushDeltaResponse, String> {
        Ok(PushDeltaResponse {
            success: true,
            error: String::new(),
            server_vclock: Default::default(),
        })
    }
    async fn pull_delta(&self, _req: &PullDeltaRequest) -> Result<PullDeltaResponse, String> {
        Ok(PullDeltaResponse {
            deltas: vec![],
            server_vclock: Default::default(),
        })
    }
    async fn alloc_inode_batch(
        &self,
        _req: &AllocInodeBatchRequest,
    ) -> Result<AllocInodeBatchResponse, String> {
        Ok(AllocInodeBatchResponse {
            success: true,
            error: String::new(),
            start_inode: 1,
            end_inode: 1024,
        })
    }
    async fn update_inode_size_chunks(
        &self,
        _req: &UpdateInodeSizeChunksRequest,
    ) -> Result<UpdateInodeSizeChunksResponse, String> {
        Ok(UpdateInodeSizeChunksResponse {
            success: true,
            error: String::new(),
        })
    }
}

/// 空的 MetadataCacheInvalidator mock
#[derive(Debug, Default, Clone)]
pub struct MockInvalidator;

impl MetadataCacheInvalidator for MockInvalidator {
    fn invalidate_inode(&self, _inode: u64) {}
    fn invalidate_dir(&self, _parent_inode: u64) {}
}

/// 空的 CacheCoherence mock（不做任何操作）
pub struct MockCacheCoherence;

#[async_trait::async_trait]
impl CacheCoherence for MockCacheCoherence {
    fn on_local_write(&self, _parent_ino: u64, _op: &WriteOp) {}

    fn validate_cache(&self, _kind: CacheKind) -> ValidationResult {
        ValidationResult::Valid
    }

    async fn on_remote_delta(&self, _parent_ino: u64, _delta: DeltaWire) {}

    fn record_version(&self, _kind: CacheKind, _version: u64) {}
}

/// 构造一个 Arc<MockDeltaSyncChannel> 便于测试
pub fn mock_channel() -> Arc<dyn DeltaSyncChannel> {
    Arc::new(MockDeltaSyncChannel)
}

/// 构造一个 Arc<MockInvalidator> 便于测试
pub fn mock_invalidator() -> Arc<dyn MetadataCacheInvalidator> {
    Arc::new(MockInvalidator)
}
