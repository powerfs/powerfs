//! 维度 3: ChunkEncoding — 元数据如何序列化
//!
//! 设计文档 S6:
//! - `ChunkEncoding::InlineData`: 数据直接存元数据 (<= 8KB, 与 Placement::Inline 绑定)
//! - `ChunkEncoding::PerChunk`: per-chunk 列表 (随机写、小文件)
//! - `ChunkEncoding::StripeDescriptor`: 几何描述符 (顺序写, 1GB 文件 100KB JSON -> 80B 二进制)
//! - `ChunkEncoding::Paginated`: 分页 (超大文件, chunk 数 > 阈值时分批返回)

use crate::error::LayoutError;

/// 单 chunk 引用
///
/// 从 `powerfs_coherence::ChunkWire` 演进, 替代其作为 chunk wire 格式.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChunkRef {
    /// 文件内偏移
    pub offset: u64,
    /// chunk 大小
    pub size: u64,
    /// volume server 上的 needle id
    pub needle_id: u64,
    /// 所在 volume
    pub volume_id: u64,
    /// CRC32 校验
    pub crc32: u32,
    /// 修改时间 (Unix epoch)
    pub mtime: u64,
}

/// Chunk 编码方式
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChunkEncoding {
    /// Inline 数据直接存元数据 (<= 8KB, 与 Placement::Inline 绑定)
    InlineData {
        /// 内联数据
        data: Vec<u8>,
    },

    /// Per-chunk 列表 (随机写、小文件)
    PerChunk {
        /// 完整 chunk 列表
        chunks: Vec<ChunkRef>,
    },

    /// Stripe 描述符 (顺序写, 几何压缩)
    ///
    /// 适用于 needle_id 连续递增的顺序写场景:
    /// 只需存 start_needle_id + chunk_size + count, 无需 per-chunk 列表.
    /// 1GB 文件: 100KB JSON -> 80B 二进制
    StripeDescriptor {
        /// 首 needle_id
        start_needle_id: u64,
        /// 固定 chunk 大小 (默认 2MB)
        chunk_size: u32,
        /// chunk 总数
        chunk_count: u32,
        /// stripe 涉及的 volume 列表
        volume_ids: Vec<u64>,
        /// 起始 volume 索引
        start_volume_idx: u32,
    },

    /// 分页 (超大文件, chunk 数 > 阈值时分批返回)
    Paginated {
        /// 当前页 chunk 列表
        chunks: Vec<ChunkRef>,
        /// 总 chunk 数
        total_count: u32,
        /// 是否还有更多页
        has_more: bool,
        /// 下次 LIST_CHUNKS 起始 offset
        next_offset: u64,
    },
}

impl ChunkEncoding {
    /// 文件总大小
    pub fn total_size(&self) -> u64 {
        match self {
            ChunkEncoding::InlineData { data } => data.len() as u64,
            ChunkEncoding::PerChunk { chunks } => {
                chunks.last().map(|c| c.offset + c.size).unwrap_or(0)
            }
            ChunkEncoding::StripeDescriptor {
                chunk_size,
                chunk_count,
                ..
            } => *chunk_size as u64 * *chunk_count as u64,
            ChunkEncoding::Paginated { chunks, .. } => {
                chunks.last().map(|c| c.offset + c.size).unwrap_or(0)
            }
        }
    }

    /// chunk 数量
    pub fn chunk_count(&self) -> usize {
        match self {
            ChunkEncoding::InlineData { .. } => 0,
            ChunkEncoding::PerChunk { chunks } => chunks.len(),
            ChunkEncoding::StripeDescriptor { chunk_count, .. } => *chunk_count as usize,
            ChunkEncoding::Paginated {
                chunks,
                total_count,
                ..
            } => {
                if chunks.len() < *total_count as usize {
                    *total_count as usize
                } else {
                    chunks.len()
                }
            }
        }
    }

    /// 读取范围选择: 给定 [offset, offset+length), 返回涉及的 chunk 列表
    ///
    /// TODO: P2 实现时完善, 支持 StripeDescriptor 模式的几何展开
    pub fn select_range(&self, _offset: u64, _length: u64) -> Vec<&ChunkRef> {
        todo!("select_range: 按 offset 范围过滤 chunks, StripeDescriptor 模式需展开")
    }

    /// StripeDescriptor 模式: 展开为 PerChunk (调试/兼容用)
    ///
    /// TODO: P2 实现时完善
    pub fn expand_to_perchunk(&self) -> Result<ChunkEncoding, LayoutError> {
        match self {
            ChunkEncoding::StripeDescriptor {
                start_needle_id,
                chunk_size,
                chunk_count,
                volume_ids,
                start_volume_idx,
            } => {
                if volume_ids.is_empty() {
                    return Err(LayoutError::InvalidEncoding(
                        "StripeDescriptor volume_ids is empty".into(),
                    ));
                }
                let mut chunks = Vec::with_capacity(*chunk_count as usize);
                for i in 0..*chunk_count as u64 {
                    let vol_rank = (i % volume_ids.len() as u64) as u32;
                    let vol_idx =
                        (*start_volume_idx + vol_rank) as usize % volume_ids.len();
                    chunks.push(ChunkRef {
                        offset: i * *chunk_size as u64,
                        size: *chunk_size as u64,
                        needle_id: start_needle_id + i,
                        volume_id: volume_ids[vol_idx],
                        crc32: 0,
                        mtime: 0,
                    });
                }
                Ok(ChunkEncoding::PerChunk { chunks })
            }
            other => Ok(other.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_data_total_size() {
        let e = ChunkEncoding::InlineData {
            data: vec![1, 2, 3, 4],
        };
        assert_eq!(e.total_size(), 4);
        assert_eq!(e.chunk_count(), 0);
    }

    #[test]
    fn perchunk_total_size() {
        let e = ChunkEncoding::PerChunk {
            chunks: vec![
                ChunkRef {
                    offset: 0,
                    size: 1024,
                    needle_id: 1,
                    volume_id: 10,
                    crc32: 0,
                    mtime: 0,
                },
                ChunkRef {
                    offset: 1024,
                    size: 2048,
                    needle_id: 2,
                    volume_id: 10,
                    crc32: 0,
                    mtime: 0,
                },
            ],
        };
        assert_eq!(e.total_size(), 3072);
        assert_eq!(e.chunk_count(), 2);
    }

    #[test]
    fn stripe_descriptor_total_size() {
        let e = ChunkEncoding::StripeDescriptor {
            start_needle_id: 100,
            chunk_size: 2 * 1024 * 1024,
            chunk_count: 512,
            volume_ids: vec![1, 2, 3, 4],
            start_volume_idx: 0,
        };
        assert_eq!(e.total_size(), 512 * 2 * 1024 * 1024);
        assert_eq!(e.chunk_count(), 512);
    }

    #[test]
    fn stripe_descriptor_expand() {
        let e = ChunkEncoding::StripeDescriptor {
            start_needle_id: 100,
            chunk_size: 1024,
            chunk_count: 4,
            volume_ids: vec![10, 20],
            start_volume_idx: 0,
        };
        let expanded = e.expand_to_perchunk().unwrap();
        match expanded {
            ChunkEncoding::PerChunk { chunks } => {
                assert_eq!(chunks.len(), 4);
                assert_eq!(chunks[0].needle_id, 100);
                assert_eq!(chunks[0].volume_id, 10);
                assert_eq!(chunks[1].needle_id, 101);
                assert_eq!(chunks[1].volume_id, 20);
                assert_eq!(chunks[2].needle_id, 102);
                assert_eq!(chunks[2].volume_id, 10);
                assert_eq!(chunks[3].needle_id, 103);
                assert_eq!(chunks[3].volume_id, 20);
            }
            _ => panic!("expected PerChunk"),
        }
    }
}
