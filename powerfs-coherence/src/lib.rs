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
///
/// 注意: `powerfs_layout::ChunkRef` 是此类型的演进版本 (二进制 TLV 编码)。
/// P2 集成后, 新客户端使用 `ChunkRef` + `ChunkEncoding`, 旧客户端仍使用
/// `ChunkWire` + JSON. 通过 `From`/`Into` 可在两者间无损转换.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkWire {
    pub offset: u64,
    pub size: u64,
    /// Chunk-level storage key (needle_id on volume server).
    pub needle_id: u64,
    /// Volume this chunk resides on (per-chunk to support stripe mode).
    pub volume_id: u64,
    pub crc32: u32,
    pub mtime: u64,
}

// ---- ChunkWire <-> ChunkRef 转换桥接 (P2 集成准备) ----

impl From<ChunkWire> for powerfs_layout::ChunkRef {
    fn from(w: ChunkWire) -> Self {
        Self {
            offset: w.offset,
            size: w.size,
            needle_id: w.needle_id,
            volume_id: w.volume_id,
            crc32: w.crc32,
            mtime: w.mtime,
        }
    }
}

impl From<powerfs_layout::ChunkRef> for ChunkWire {
    fn from(c: powerfs_layout::ChunkRef) -> Self {
        Self {
            offset: c.offset,
            size: c.size,
            needle_id: c.needle_id,
            volume_id: c.volume_id,
            crc32: c.crc32,
            mtime: c.mtime,
        }
    }
}

impl From<&ChunkWire> for powerfs_layout::ChunkRef {
    fn from(w: &ChunkWire) -> Self {
        w.clone().into()
    }
}

/// 将 `Vec<ChunkWire>` 转换为 `ChunkEncoding::PerChunk`, 便于 P2 二进制编码.
/// (不能用 `From` trait — orphan rule 禁止为外部类型 `ChunkEncoding` 实现
/// `From<Vec<ChunkWire>>`, 因为 `Vec` 是外部类型且 `ChunkWire` 被其覆盖.)
pub fn chunks_wire_to_encoding(chunks: Vec<ChunkWire>) -> powerfs_layout::ChunkEncoding {
    powerfs_layout::ChunkEncoding::PerChunk {
        chunks: chunks.into_iter().map(Into::into).collect(),
    }
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
    /// P2.5: Inline 小文件数据 (≤ 8KB). Some 时表示 Inline 模式,
    /// 数据直接存 Filer 元数据, chunks 应为空. None 时走原 Flat 路径.
    #[serde(default)]
    pub inline_data: Option<Vec<u8>>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_wire_to_chunk_ref() {
        let wire = ChunkWire {
            offset: 1024,
            size: 4096,
            needle_id: 42,
            volume_id: 10,
            crc32: 0xdeadbeef,
            mtime: 1234567890,
        };
        let chunk_ref: powerfs_layout::ChunkRef = wire.into();
        assert_eq!(chunk_ref.offset, 1024);
        assert_eq!(chunk_ref.size, 4096);
        assert_eq!(chunk_ref.needle_id, 42);
        assert_eq!(chunk_ref.volume_id, 10);
        assert_eq!(chunk_ref.crc32, 0xdeadbeef);
        assert_eq!(chunk_ref.mtime, 1234567890);
    }

    #[test]
    fn chunk_ref_to_chunk_wire() {
        let chunk_ref = powerfs_layout::ChunkRef {
            offset: 0,
            size: 2048,
            needle_id: 99,
            volume_id: 7,
            crc32: 0,
            mtime: 0,
        };
        let wire: ChunkWire = chunk_ref.into();
        assert_eq!(wire.offset, 0);
        assert_eq!(wire.size, 2048);
        assert_eq!(wire.needle_id, 99);
        assert_eq!(wire.volume_id, 7);
    }

    #[test]
    fn chunk_wire_ref_to_chunk_ref() {
        let wire = ChunkWire {
            offset: 0,
            size: 100,
            needle_id: 1,
            volume_id: 1,
            crc32: 0,
            mtime: 0,
        };
        let chunk_ref: powerfs_layout::ChunkRef = (&wire).into();
        assert_eq!(chunk_ref.needle_id, 1);
        // 原 wire 仍然可用 (引用转换不 consume)
        assert_eq!(wire.needle_id, 1);
    }

    #[test]
    fn vec_chunk_wire_to_chunk_encoding() {
        let wires = vec![
            ChunkWire {
                offset: 0,
                size: 1024,
                needle_id: 1,
                volume_id: 10,
                crc32: 0,
                mtime: 0,
            },
            ChunkWire {
                offset: 1024,
                size: 2048,
                needle_id: 2,
                volume_id: 20,
                crc32: 0,
                mtime: 0,
            },
        ];
        let encoding = chunks_wire_to_encoding(wires);
        match encoding {
            powerfs_layout::ChunkEncoding::PerChunk { chunks } => {
                assert_eq!(chunks.len(), 2);
                assert_eq!(chunks[0].needle_id, 1);
                assert_eq!(chunks[1].needle_id, 2);
            }
            _ => panic!("expected PerChunk"),
        }
    }

    #[test]
    fn roundtrip_chunk_wire_chunk_ref() {
        let original = ChunkWire {
            offset: 4096,
            size: 8192,
            needle_id: 777,
            volume_id: 42,
            crc32: 0xcafebabe,
            mtime: 999999,
        };
        let chunk_ref: powerfs_layout::ChunkRef = original.clone().into();
        let back: ChunkWire = chunk_ref.into();
        assert_eq!(original, back);
    }
}
