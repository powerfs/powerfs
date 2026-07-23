//! 文件布局（FileLayout）与条带化（Stripe）支持
//!
//! 设计目标：
//! - 小文件使用 Flat 模式（单 volume 顺序写入），无额外元数据开销
//! - 大文件自动提升为 Stripe 模式，数据条带化分布到多个 volume，实现并行 I/O
//! - Layout 信息存储在 Entry.extended["file_layout"] 中，不修改 proto
//!
//! Stripe 模型（RAID0）：
//! ```text
//! 文件 offset 0 ──────── stripe_size ──────── stripe_size*2 ────── ...
//!                    ↓          ↓                  ↓
//!   volume[0]:    [0, s)     [s*2, s*3)       [s*4, s*5)
//!   volume[1]:    [s, s*2)   [s*3, s*4)       [s*5, s*6)
//!   volume[2]:    ...
//!   volume[3]:    ...
//! ```
//! 每个 volume 连续写入 stripe_size 字节后轮转到下一个 volume。
//! 不同文件的起始 volume 通过 round-robin 错开，避免热点。

use std::collections::HashMap;

/// 默认条带大小：64MB
pub const DEFAULT_STRIPE_SIZE: u64 = 64 * 1024 * 1024;
/// 默认条带宽度：4 个 volume
pub const DEFAULT_STRIPE_COUNT: u32 = 4;
/// 文件大小超过此阈值时，从 Flat 提升为 Stripe
pub const PROMOTE_THRESHOLD: u64 = DEFAULT_STRIPE_SIZE;

/// 布局类型
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayoutType {
    /// 平铺模式：所有 chunk 写入同一个 volume（小文件）
    Flat = 0,
    /// 条带模式：数据按 stripe_size 轮流分布到多个 volume（大文件）
    Stripe = 1,
}

/// 文件布局描述
#[derive(Clone, Debug)]
pub struct FileLayout {
    /// 布局类型
    pub layout_type: LayoutType,
    /// 单条带大小（字节），Stripe 模式下每个 volume 连续写入的字节数
    pub stripe_size: u64,
    /// 条带宽度（volume 数量）
    pub stripe_count: u32,
    /// 起始 volume 索引（round-robin 错开不同文件）
    pub start_volume_idx: u32,
    /// 分配的 volume ID 列表
    pub volume_ids: Vec<u64>,
}

impl FileLayout {
    /// 创建 Flat 布局（小文件默认）
    pub fn flat() -> Self {
        FileLayout {
            layout_type: LayoutType::Flat,
            stripe_size: 0,
            stripe_count: 0,
            start_volume_idx: 0,
            volume_ids: vec![],
        }
    }

    /// 创建 Stripe 布局
    pub fn stripe(
        stripe_size: u64,
        stripe_count: u32,
        volume_ids: Vec<u64>,
        start_volume_idx: u32,
    ) -> Self {
        FileLayout {
            layout_type: LayoutType::Stripe,
            stripe_size,
            stripe_count,
            start_volume_idx,
            volume_ids,
        }
    }

    /// 是否为 Stripe 模式
    pub fn is_stripe(&self) -> bool {
        self.layout_type == LayoutType::Stripe && !self.volume_ids.is_empty()
    }

    /// 根据文件偏移定位目标 volume 和 volume 内偏移
    ///
    /// 返回 (volume_ids 中的索引, volume 内偏移)
    pub fn locate(&self, file_offset: u64) -> (usize, u64) {
        match self.layout_type {
            LayoutType::Flat => (0, file_offset),
            LayoutType::Stripe => {
                let stripe_size = self.stripe_size.max(1);
                let stripe_idx = file_offset / stripe_size;
                let vol_rank = (stripe_idx % self.stripe_count as u64) as u32;
                let vol_array_idx =
                    ((self.start_volume_idx + vol_rank) as usize) % self.volume_ids.len();
                let vol_offset = (stripe_idx / self.stripe_count as u64) * stripe_size
                    + (file_offset % stripe_size);
                (vol_array_idx, vol_offset)
            }
        }
    }

    /// 获取文件偏移对应的 volume_id
    pub fn volume_id_for_offset(&self, file_offset: u64) -> Option<u64> {
        if self.volume_ids.is_empty() {
            return None;
        }
        let (idx, _) = self.locate(file_offset);
        self.volume_ids.get(idx).copied()
    }

    /// 计算一个写入区间 [offset, offset+size) 跨越哪些 volume
    ///
    /// 返回 Vec<(volume_array_idx, vol_offset_start, vol_offset_end, file_offset_start)>
    /// 用于并行写入多个 volume
    pub fn locate_range(&self, file_offset: u64, size: u64) -> Vec<(usize, u64, u64, u64)> {
        if self.layout_type == LayoutType::Flat || self.volume_ids.is_empty() {
            return vec![(0, file_offset, file_offset + size, file_offset)];
        }

        let stripe_size = self.stripe_size.max(1);
        let mut result = Vec::new();
        let mut remaining = size;
        let mut current_file_off = file_offset;

        while remaining > 0 {
            let (vol_idx, vol_off) = self.locate(current_file_off);
            // 当前 stripe 内剩余空间
            let stripe_remaining = stripe_size - (current_file_off % stripe_size);
            let chunk_size = remaining.min(stripe_remaining);

            result.push((vol_idx, vol_off, vol_off + chunk_size, current_file_off));

            remaining -= chunk_size;
            current_file_off += chunk_size;
        }

        result
    }

    /// 序列化为字节（存储到 Entry.extended["file_layout"]）
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 8 + 4 + 4 + 4 + self.volume_ids.len() * 8);
        buf.push(self.layout_type as u8);
        buf.extend_from_slice(&self.stripe_size.to_le_bytes());
        buf.extend_from_slice(&self.stripe_count.to_le_bytes());
        buf.extend_from_slice(&self.start_volume_idx.to_le_bytes());
        buf.extend_from_slice(&(self.volume_ids.len() as u32).to_le_bytes());
        for vid in &self.volume_ids {
            buf.extend_from_slice(&vid.to_le_bytes());
        }
        buf
    }

    /// 从字节反序列化
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 1 + 8 + 4 + 4 + 4 {
            return None;
        }
        let mut pos = 0;
        let layout_type = match data[pos] {
            0 => LayoutType::Flat,
            1 => LayoutType::Stripe,
            _ => return None,
        };
        pos += 1;
        let stripe_size = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let stripe_count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let start_volume_idx = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let num_volumes = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if data.len() < pos + num_volumes * 8 {
            return None;
        }
        let mut volume_ids = Vec::with_capacity(num_volumes);
        for _ in 0..num_volumes {
            volume_ids.push(u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?));
            pos += 8;
        }
        Some(FileLayout {
            layout_type,
            stripe_size,
            stripe_count,
            start_volume_idx,
            volume_ids,
        })
    }

    /// 从 Entry.extended map 中提取 FileLayout
    pub fn from_extended(extended: &HashMap<String, Vec<u8>>) -> Option<Self> {
        extended
            .get("file_layout")
            .and_then(|data| Self::from_bytes(data))
    }

    /// 将 FileLayout 存入 Entry.extended map
    pub fn to_extended(&self, extended: &mut HashMap<String, Vec<u8>>) {
        extended.insert("file_layout".to_string(), self.to_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stripe_locate() {
        let layout = FileLayout::stripe(
            64 * 1024 * 1024, // 64MB
            4,                // 4 volumes
            vec![1, 2, 3, 4],
            0, // start at volume 0
        );

        // offset 0 → volume[0], vol_offset 0
        let (idx, off) = layout.locate(0);
        assert_eq!(idx, 0);
        assert_eq!(off, 0);

        // offset 64MB → volume[1], vol_offset 0
        let (idx, off) = layout.locate(64 * 1024 * 1024);
        assert_eq!(idx, 1);
        assert_eq!(off, 0);

        // offset 128MB → volume[2], vol_offset 0
        let (idx, off) = layout.locate(128 * 1024 * 1024);
        assert_eq!(idx, 2);
        assert_eq!(off, 0);

        // offset 256MB → volume[0] (second cycle), vol_offset 64MB
        let (idx, off) = layout.locate(256 * 1024 * 1024);
        assert_eq!(idx, 0);
        assert_eq!(off, 64 * 1024 * 1024);

        // offset 32MB → volume[0], vol_offset 32MB (within first stripe)
        let (idx, off) = layout.locate(32 * 1024 * 1024);
        assert_eq!(idx, 0);
        assert_eq!(off, 32 * 1024 * 1024);
    }

    #[test]
    fn test_stripe_round_robin_start() {
        let layout = FileLayout::stripe(
            64 * 1024 * 1024,
            4,
            vec![1, 2, 3, 4],
            2, // start at volume index 2
        );

        // offset 0 → volume[2] (index 2)
        let (idx, _) = layout.locate(0);
        assert_eq!(idx, 2);

        // offset 64MB → volume[3] (index 3)
        let (idx, _) = layout.locate(64 * 1024 * 1024);
        assert_eq!(idx, 3);

        // offset 128MB → volume[0] (wraps around)
        let (idx, _) = layout.locate(128 * 1024 * 1024);
        assert_eq!(idx, 0);
    }

    #[test]
    fn test_locate_range_single_stripe() {
        let layout = FileLayout::stripe(64 * 1024 * 1024, 4, vec![1, 2, 3, 4], 0);

        // Write within a single stripe: offset=10MB, size=20MB
        let ranges = layout.locate_range(10 * 1024 * 1024, 20 * 1024 * 1024);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].0, 0); // volume index 0
    }

    #[test]
    fn test_locate_range_cross_stripe() {
        let layout = FileLayout::stripe(64 * 1024 * 1024, 4, vec![1, 2, 3, 4], 0);

        // Write across stripe boundary: offset=50MB, size=30MB
        // 50MB→64MB on volume[0] (14MB), 64MB→80MB on volume[1] (16MB)
        let ranges = layout.locate_range(50 * 1024 * 1024, 30 * 1024 * 1024);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].0, 0); // first part on volume[0]
        assert_eq!(ranges[1].0, 1); // second part on volume[1]
    }

    #[test]
    fn test_serialize_deserialize() {
        let layout = FileLayout::stripe(64 * 1024 * 1024, 4, vec![1, 2, 3, 4], 2);

        let bytes = layout.to_bytes();
        let restored = FileLayout::from_bytes(&bytes).unwrap();

        assert_eq!(restored.layout_type, LayoutType::Stripe);
        assert_eq!(restored.stripe_size, 64 * 1024 * 1024);
        assert_eq!(restored.stripe_count, 4);
        assert_eq!(restored.start_volume_idx, 2);
        assert_eq!(restored.volume_ids, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_flat_layout() {
        let layout = FileLayout::flat();

        assert!(!layout.is_stripe());

        let (idx, off) = layout.locate(12345);
        assert_eq!(idx, 0);
        assert_eq!(off, 12345);
    }
}
