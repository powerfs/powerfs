//! powerfs-coherence: 通用元数据同步接口（强一致路径使用）。
//!
//! 对外 trait：
//! - [`DeltaSyncChannel`]（fuse 客户端侧：alloc_inode_batch / update_inode_size_chunks /
//!   open_count_inc / open_count_dec）

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// 对外 trait
// ---------------------------------------------------------------------------

/// 元数据同步通道（fuse 端实现，封装 meta_shard_client 的 RPC 调用）。
///
/// powerfs-coherence 不依赖 powerfs-fuse-core（避免循环依赖），
/// fuse 端在 meta_shard_client.rs 中实现此 trait。
#[async_trait::async_trait]
pub trait DeltaSyncChannel: Send + Sync {
    async fn alloc_inode_batch(
        &self,
        req: &AllocInodeBatchRequest,
    ) -> Result<AllocInodeBatchResponse, String>;
    async fn update_inode_size_chunks(
        &self,
        req: &UpdateInodeSizeChunksRequest,
    ) -> Result<UpdateInodeSizeChunksResponse, String>;
    async fn open_count_inc(&self, req: &OpenCountRequest) -> Result<OpenCountResponse, String>;
    async fn open_count_dec(&self, req: &OpenCountRequest) -> Result<OpenCountResponse, String>;
}

// ---------------------------------------------------------------------------
// 公共传输类型（wire format，JSON 序列化）
// ---------------------------------------------------------------------------

/// 中性 Chunk（wire 格式）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkWire {
    pub offset: u64,
    pub size: u64,
    pub mtime: u64,
    pub fid: String,
    pub cookie: u32,
    pub crc32: u32,
}

/// alloc_inode_batch 请求体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AllocInodeBatchRequest {
    pub shard_id: u64,
    pub count: u32,
    pub client_id: String,
}

/// alloc_inode_batch 响应体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AllocInodeBatchResponse {
    pub success: bool,
    pub error: String,
    pub start_inode: u64,
    pub end_inode: u64,
}

/// update_inode_size_chunks 请求体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdateInodeSizeChunksRequest {
    pub shard_id: u64,
    pub inode: u64,
    pub size: u64,
    pub chunks: Vec<ChunkWire>,
    pub client_id: String,
}

/// update_inode_size_chunks 响应体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdateInodeSizeChunksResponse {
    pub success: bool,
    pub error: String,
}

/// open_count 增减请求体（Phase 3.5.3: GC 第三条件 open_count==0 追踪）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenCountRequest {
    pub shard_id: u64,
    pub inode: u64,
}

/// open_count 增减响应体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenCountResponse {
    pub success: bool,
    pub open_count: u32,
    pub error: String,
}
