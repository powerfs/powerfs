//! P7: FUSE 客户端侧的 chunk 压缩算法缓存。
//!
//! 读路径只拿到 `(volume_id, needle_id, offset, size)`，而压缩算法记录在
//! `FileLayout` 的 `ChunkRef` 里。为避免把压缩标记一路塞进 `FileChunk`
//! （内核/多处构造点都要改），这里维护一个进程级有界 LRU：
//! - 解码 `FileLayout`（`provider_adapter` / `meta_shard_client`）时批量写入；
//! - `FacadeStorageProvider::read_blob` 读前查询，命中压缩则读整条 needle
//!   再解压切片。
//!
//! 键 `(volume_id, needle_id)` 全局唯一（needle_id 由 Master 分配），多挂载共享安全。
//! 容量由 `POWERFS_CHUNK_COMPRESSION_CACHE_CAPACITY` 控制（默认 65536）。

use std::num::NonZeroUsize;
use std::sync::{Mutex, OnceLock};

use lru::LruCache;

use powerfs_layout::encoding::CompressionAlgo;

/// 单个 chunk 的压缩信息（读路径解压所需）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkCompressionInfo {
    /// 压缩算法；缓存内只会记录 `is_compressed() == true` 的项。
    pub algo: CompressionAlgo,
    /// chunk 逻辑字节数（解压后长度必须等于它）。
    pub logical_size: u64,
}

const DEFAULT_CAPACITY: usize = 65_536;

fn capacity_from_env() -> usize {
    std::env::var("POWERFS_CHUNK_COMPRESSION_CACHE_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_CAPACITY)
}

fn cache() -> &'static Mutex<LruCache<(u64, u64), ChunkCompressionInfo>> {
    static CACHE: OnceLock<Mutex<LruCache<(u64, u64), ChunkCompressionInfo>>> = OnceLock::new();
    CACHE.get_or_init(|| {
        let cap = NonZeroUsize::new(capacity_from_env()).unwrap_or(NonZeroUsize::MIN);
        Mutex::new(LruCache::new(cap))
    })
}

/// 记录一个 chunk 的压缩信息。
///
/// - 压缩 chunk：写入（LRU 超容量自动淘汰）；
/// - 未压缩 chunk：主动移除同键项，防止 needle 复用/覆盖后残留旧压缩标记。
pub fn record(volume_id: u64, needle_id: u64, algo: CompressionAlgo, logical_size: u64) {
    let mut guard = match cache().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let key = (volume_id, needle_id);
    if algo.is_compressed() {
        guard.put(key, ChunkCompressionInfo { algo, logical_size });
    } else {
        guard.pop(&key);
    }
}

/// 查询 chunk 压缩信息；未命中返回 `None`（调用方保持原未压缩读语义）。
pub fn lookup(volume_id: u64, needle_id: u64) -> Option<ChunkCompressionInfo> {
    let mut guard = match cache().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.get(&(volume_id, needle_id)).copied()
}

/// 便捷判断：该 chunk 是否压缩。
pub fn is_compressed(volume_id: u64, needle_id: u64) -> bool {
    lookup(volume_id, needle_id).is_some()
}

/// 从解码后的 `FileLayout` 批量记录 chunk 压缩信息。
///
/// 仅 `PerChunk`/`Paginated` 带显式 chunk 列表；`InlineData`/`StripeDescriptor`
/// 无压缩语义，直接跳过。
pub fn record_layout(layout: &powerfs_layout::layout::FileLayout) {
    use powerfs_layout::encoding::ChunkEncoding;
    match &layout.encoding {
        ChunkEncoding::PerChunk { chunks } | ChunkEncoding::Paginated { chunks, .. } => {
            for c in chunks {
                record(c.volume_id, c.needle_id, c.compression, c.size);
            }
        }
        _ => {}
    }
}

/// 从 `ChunkRef` 列表批量记录（副本 chunk 列表用）。
pub fn record_chunk_refs(chunks: &[powerfs_layout::encoding::ChunkRef]) {
    for c in chunks {
        record(c.volume_id, c.needle_id, c.compression, c.size);
    }
}

/// 清空缓存（仅测试用）。
pub fn clear() {
    let mut guard = match cache().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_lookup_compressed_chunk() {
        record(1001, 0x1001, CompressionAlgo::Zstd, 4096);
        let info = lookup(1001, 0x1001).expect("压缩 chunk 应命中");
        assert_eq!(info.algo, CompressionAlgo::Zstd);
        assert_eq!(info.logical_size, 4096);
        assert!(is_compressed(1001, 0x1001));
    }

    #[test]
    fn recording_none_clears_stale_entry() {
        record(1002, 0x2002, CompressionAlgo::Zstd, 8192);
        assert!(is_compressed(1002, 0x2002));
        // needle 被覆盖为未压缩：必须移除旧标记，避免误解压。
        record(1002, 0x2002, CompressionAlgo::None, 8192);
        assert!(!is_compressed(1002, 0x2002));
    }

    #[test]
    fn lookup_miss_returns_none() {
        assert!(lookup(1003, 0x9999).is_none());
    }
}
