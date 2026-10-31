//! P7 副本压缩：zstd 编解码与算法枚举。
//!
//! 设计约束：
//! - 只实现 zstd；算法枚举预留扩展位（lz4/snappy 后续按需加）。
//! - `CompressionAlgo::None` 是默认且零开销：编解码原样返回，不产生拷贝以外的行为。
//! - 解压失败必须返回带上下文的错误，**绝不**把压缩字节当数据返回
//!   （文件系统承载用户数据，宁可 5xx 也不能静默错数据）。

use serde::{Deserialize, Serialize};

use crate::error::{PowerFsError, Result};

/// 每 chunk 压缩算法。
///
/// wire/磁盘 tag：`0=None, 1=Zstd`；新增算法顺延 tag，旧 tag 语义永不改变。
/// `#[serde(default)]` 保证旧元数据缺字段时反序列化为 `None`。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionAlgo {
    /// 未压缩
    #[default]
    None = 0,
    /// zstd 单帧格式（`zstd::encode_all`/`decode_all`）
    Zstd = 1,
}

impl CompressionAlgo {
    /// 序列化到 wire/磁盘的 tag。
    pub fn to_tag(self) -> u8 {
        match self {
            CompressionAlgo::None => 0,
            CompressionAlgo::Zstd => 1,
        }
    }

    /// 从 tag 还原算法；未知 tag 返回 `None`（解码侧据此做兼容降级，不报错：
    /// trailing 区段可能被旧版本截断，宁可按未压缩处理也不能硬失败阻塞读）。
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(CompressionAlgo::None),
            1 => Some(CompressionAlgo::Zstd),
            _ => None,
        }
    }

    pub fn is_compressed(self) -> bool {
        !matches!(self, CompressionAlgo::None)
    }
}

/// 默认最小压缩比：原始/压缩 ≥ 1.05 才值得换存储（避免随机数据压完反而变大）。
pub const DEFAULT_MIN_RATIO: f64 = 1.05;

/// 用指定算法压缩数据。`None` 算法原样返回（浅拷贝）。
pub fn compress(algo: CompressionAlgo, data: &[u8]) -> Result<Vec<u8>> {
    match algo {
        CompressionAlgo::None => Ok(data.to_vec()),
        CompressionAlgo::Zstd => zstd::encode_all(data, 0).map_err(|e| {
            PowerFsError::Internal(format!("zstd compress failed (len={}): {}", data.len(), e))
        }),
    }
}

/// 用指定算法解压数据。`None` 算法原样返回（浅拷贝）。
///
/// 解压失败返回 `InvalidRequest`，调用方（FUSE/S3 读路径）应映射为 EIO/5xx，
/// 不允许回退为"原样返回压缩字节"。
pub fn decompress(algo: CompressionAlgo, data: &[u8]) -> Result<Vec<u8>> {
    match algo {
        CompressionAlgo::None => Ok(data.to_vec()),
        CompressionAlgo::Zstd => zstd::decode_all(data).map_err(|e| {
            PowerFsError::InvalidRequest(format!(
                "zstd decompress failed (compressed len={}): {}",
                data.len(),
                e
            ))
        }),
    }
}

/// 判定压缩是否划算：`original / compressed >= min_ratio`。
/// 压缩后为空（不可能）或膨胀时一律拒绝。
pub fn should_compress(original_len: usize, compressed_len: usize, min_ratio: f64) -> bool {
    if original_len == 0 || compressed_len == 0 || compressed_len >= original_len {
        return false;
    }
    (original_len as f64 / compressed_len as f64) >= min_ratio
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 可压缩数据：重复模式（模拟文本/日志）。
    fn compressible() -> Vec<u8> {
        let line = b"powerfs compression test line with repetitive content abcdefghij\n";
        line.iter().cycle().take(256 * 1024).copied().collect()
    }

    /// 不可压缩数据：固定种子的伪随机字节。
    fn incompressible() -> Vec<u8> {
        let mut x: u64 = 0x9E3779B97F4A7C15;
        (0..64 * 1024)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 32) as u8
            })
            .collect()
    }

    #[test]
    fn round_trip_compressible() {
        let data = compressible();
        let c = compress(CompressionAlgo::Zstd, &data).unwrap();
        assert!(should_compress(data.len(), c.len(), DEFAULT_MIN_RATIO));
        let d = decompress(CompressionAlgo::Zstd, &c).unwrap();
        assert_eq!(d, data);
    }

    #[test]
    fn none_algo_passthrough() {
        let data = compressible();
        let c = compress(CompressionAlgo::None, &data).unwrap();
        assert_eq!(c, data);
        let d = decompress(CompressionAlgo::None, &c).unwrap();
        assert_eq!(d, data);
    }

    #[test]
    fn random_data_rejected_by_ratio() {
        let data = incompressible();
        let c = compress(CompressionAlgo::Zstd, &data).unwrap();
        assert!(
            !should_compress(data.len(), c.len(), DEFAULT_MIN_RATIO),
            "随机数据压缩比不足，必须被拒绝"
        );
    }

    #[test]
    fn corrupted_stream_errors_not_passthrough() {
        let data = compressible();
        let mut c = compress(CompressionAlgo::Zstd, &data).unwrap();
        // 破坏帧头后的数据
        let n = c.len();
        c[n / 2] ^= 0xFF;
        c.truncate(n / 2 + 8);
        assert!(decompress(CompressionAlgo::Zstd, &c).is_err());
        // 完全非法的输入也必须报错
        assert!(decompress(CompressionAlgo::Zstd, b"not a zstd frame at all").is_err());
    }

    #[test]
    fn empty_data() {
        let c = compress(CompressionAlgo::Zstd, &[]).unwrap();
        let d = decompress(CompressionAlgo::Zstd, &c).unwrap();
        assert!(d.is_empty());
        assert!(!should_compress(0, 0, DEFAULT_MIN_RATIO));
    }

    #[test]
    fn tag_round_trip() {
        assert_eq!(CompressionAlgo::from_tag(0), Some(CompressionAlgo::None));
        assert_eq!(CompressionAlgo::from_tag(1), Some(CompressionAlgo::Zstd));
        assert_eq!(CompressionAlgo::from_tag(255), None);
        assert!(!CompressionAlgo::None.is_compressed());
        assert!(CompressionAlgo::Zstd.is_compressed());
    }
}
