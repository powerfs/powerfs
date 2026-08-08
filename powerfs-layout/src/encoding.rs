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
    /// - `PerChunk`/`Paginated`: 过滤已存在的 chunks, 返回重叠部分的克隆
    /// - `StripeDescriptor`: 几何计算生成涉及范围的 chunks (owned)
    /// - `InlineData`: 返回空 (数据内联, 无 chunk)
    ///
    /// 返回 owned `ChunkRef` 而非引用, 因为 StripeDescriptor 的 chunks 是
    /// 按需生成的, 不存在预分配的实体可引用.
    pub fn select_range(&self, offset: u64, length: u64) -> Vec<ChunkRef> {
        let range_end = offset.saturating_add(length);
        if length == 0 {
            return Vec::new();
        }

        match self {
            ChunkEncoding::InlineData { .. } => Vec::new(),

            ChunkEncoding::PerChunk { chunks } | ChunkEncoding::Paginated { chunks, .. } => {
                chunks
                    .iter()
                    .filter(|c| {
                        let chunk_end = c.offset.saturating_add(c.size);
                        // 区间重叠: chunk [c.offset, chunk_end) ∩ [offset, range_end) ≠ ∅
                        c.offset < range_end && chunk_end > offset
                    })
                    .cloned()
                    .collect()
            }

            ChunkEncoding::StripeDescriptor {
                start_needle_id,
                chunk_size,
                chunk_count,
                volume_ids,
                start_volume_idx,
            } => {
                if volume_ids.is_empty() || *chunk_count == 0 || *chunk_size == 0 {
                    return Vec::new();
                }
                let cs = *chunk_size as u64;
                let total = *chunk_count as u64;

                // 计算涉及的 chunk 索引范围 [first_idx, last_idx]
                let first_idx = offset / cs;
                let last_idx = (range_end.saturating_sub(1)) / cs;

                // 限制在有效范围内
                let first_idx = first_idx.min(total - 1);
                let last_idx = last_idx.min(total - 1);

                let vol_count = volume_ids.len() as u64;
                (first_idx..=last_idx)
                    .map(|i| {
                        let vol_idx = ((*start_volume_idx as u64 + i) % vol_count) as usize;
                        ChunkRef {
                            offset: i * cs,
                            size: cs,
                            needle_id: start_needle_id + i,
                            volume_id: volume_ids[vol_idx],
                            crc32: 0,
                            mtime: 0,
                        }
                    })
                    .collect()
            }
        }
    }

    /// StripeDescriptor 模式: 展开为 PerChunk (调试/兼容用)
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

    // ===== select_range 单元测试 =====

    fn make_chunk(offset: u64, size: u64, needle_id: u64, vol_id: u64) -> ChunkRef {
        ChunkRef {
            offset,
            size,
            needle_id,
            volume_id: vol_id,
            crc32: 0,
            mtime: 0,
        }
    }

    #[test]
    fn select_range_inline_returns_empty() {
        let e = ChunkEncoding::InlineData { data: vec![1, 2, 3] };
        assert!(e.select_range(0, 3).is_empty());
    }

    #[test]
    fn select_range_perchunk_full_overlap() {
        let e = ChunkEncoding::PerChunk {
            chunks: vec![
                make_chunk(0, 1024, 1, 10),
                make_chunk(1024, 1024, 2, 20),
                make_chunk(2048, 1024, 3, 30),
            ],
        };
        let selected = e.select_range(0, 3072);
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn select_range_perchunk_partial() {
        let e = ChunkEncoding::PerChunk {
            chunks: vec![
                make_chunk(0, 1024, 1, 10),
                make_chunk(1024, 1024, 2, 20),
                make_chunk(2048, 1024, 3, 30),
            ],
        };
        // [512, 1536) → 涉及 chunk 0 和 chunk 1
        let selected = e.select_range(512, 1024);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].needle_id, 1);
        assert_eq!(selected[1].needle_id, 2);
    }

    #[test]
    fn select_range_perchunk_boundary() {
        let e = ChunkEncoding::PerChunk {
            chunks: vec![
                make_chunk(0, 1024, 1, 10),
                make_chunk(1024, 1024, 2, 20),
            ],
        };
        // 恰好 [0, 1024) → 只有 chunk 0
        let selected = e.select_range(0, 1024);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].needle_id, 1);
        // 恰好 [1024, 2048) → 只有 chunk 1
        let selected = e.select_range(1024, 1024);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].needle_id, 2);
    }

    #[test]
    fn select_range_perchunk_no_overlap() {
        let e = ChunkEncoding::PerChunk {
            chunks: vec![make_chunk(0, 1024, 1, 10)],
        };
        // [2048, 3072) → 不涉及任何 chunk
        let selected = e.select_range(2048, 1024);
        assert!(selected.is_empty());
    }

    #[test]
    fn select_range_zero_length() {
        let e = ChunkEncoding::PerChunk {
            chunks: vec![make_chunk(0, 1024, 1, 10)],
        };
        assert!(e.select_range(0, 0).is_empty());
    }

    #[test]
    fn select_range_stripe_descriptor() {
        let e = ChunkEncoding::StripeDescriptor {
            start_needle_id: 100,
            chunk_size: 1024,
            chunk_count: 10,
            volume_ids: vec![10, 20],
            start_volume_idx: 0,
        };
        // [1024, 3072) → 涉及 chunk 1, 2 (索引 1~2)
        let selected = e.select_range(1024, 2048);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].needle_id, 101);
        assert_eq!(selected[0].volume_id, 20);
        assert_eq!(selected[1].needle_id, 102);
        assert_eq!(selected[1].volume_id, 10);
    }

    #[test]
    fn select_range_stripe_descriptor_full() {
        let e = ChunkEncoding::StripeDescriptor {
            start_needle_id: 100,
            chunk_size: 1024,
            chunk_count: 4,
            volume_ids: vec![10, 20, 30, 40],
            start_volume_idx: 0,
        };
        let selected = e.select_range(0, 4096);
        assert_eq!(selected.len(), 4);
        assert_eq!(selected[0].needle_id, 100);
        assert_eq!(selected[3].needle_id, 103);
    }

    #[test]
    fn select_range_stripe_descriptor_beyond_end() {
        let e = ChunkEncoding::StripeDescriptor {
            start_needle_id: 100,
            chunk_size: 1024,
            chunk_count: 4,
            volume_ids: vec![10, 20],
            start_volume_idx: 0,
        };
        // 请求范围超出文件末尾 → 限制到 chunk_count
        let selected = e.select_range(0, 100_000);
        assert_eq!(selected.len(), 4);
    }

    #[test]
    fn select_range_stripe_descriptor_start_volume_idx() {
        let e = ChunkEncoding::StripeDescriptor {
            start_needle_id: 100,
            chunk_size: 1024,
            chunk_count: 4,
            volume_ids: vec![10, 20, 30],
            start_volume_idx: 1, // 从 vol 20 开始
        };
        let selected = e.select_range(0, 4096);
        assert_eq!(selected.len(), 4);
        assert_eq!(selected[0].volume_id, 20); // (1+0) % 3 = 1 → vol_ids[1] = 20
        assert_eq!(selected[1].volume_id, 30); // (1+1) % 3 = 2 → vol_ids[2] = 30
        assert_eq!(selected[2].volume_id, 10); // (1+2) % 3 = 0 → vol_ids[0] = 10
        assert_eq!(selected[3].volume_id, 20); // (1+3) % 3 = 1 → vol_ids[1] = 20
    }

    #[test]
    fn select_range_paginated() {
        let e = ChunkEncoding::Paginated {
            chunks: vec![
                make_chunk(0, 1024, 1, 10),
                make_chunk(1024, 1024, 2, 20),
            ],
            total_count: 10,
            has_more: true,
            next_offset: 2048,
        };
        let selected = e.select_range(512, 1024);
        assert_eq!(selected.len(), 2);
    }
}
