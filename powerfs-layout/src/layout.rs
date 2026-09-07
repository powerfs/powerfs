//! FileLayout: 三维正交组合
//!
//! 设计文档 S3:
//! ```text
//! FileLayout = Placement x Reliability x ChunkEncoding
//! ```
//!
//! 三个维度可独立演进, 任意组合.
//! 唯一不严格正交的组合: Placement::Inline 时,
//! Reliability 隐式为 Raft 复制, ChunkEncoding 必为 InlineData.

use crate::encoding::ChunkEncoding;
use crate::placement::{Placement, PlacementSpec};
use crate::policy::PlacementPolicy;
use crate::predictor::{LayoutPredictor, PredictContext};
use crate::reliability::{CompressionState, Reliability, ReliabilityState};

/// 文件布局 (三维正交)
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FileLayout {
    /// 维度 1: 数据分布
    pub placement: Placement,

    /// 维度 2: 可靠性策略
    pub reliability: Reliability,

    /// 维度 2: 可靠性状态 (scrubber 异步转换)
    pub reliability_state: ReliabilityState,

    /// 维度 2: 压缩状态
    pub compression: CompressionState,

    /// 维度 3: 元数据编码
    pub encoding: ChunkEncoding,
}

impl FileLayout {
    /// 新文件布局决策 (综合文件大小 + 目录属性 + 策略)
    ///
    /// 决策逻辑 (设计文档 S4.2 + S4.4):
    /// 1. 若父目录有 powerfs.inline xattr 且 > 0: 先尝试 Inline
    /// 2. 若父目录有 powerfs.placement xattr: 超 inline 阈值后按此 placement
    /// 3. 否则: 按全局默认 auto_promote
    ///
    /// 注意: 此方法用于已知文件大小的场景.
    /// 对于创建时空文件 (大小未知), 应使用 [`FileLayout::for_empty_file`]
    /// 获得 Empty 布局, 然后通过 [`FileLayout::resolve_on_first_write`]
    /// 或 [`FileLayout::resolve_with_predictor`] 在第一次写入时决定实际布局.
    pub fn for_new_file(
        file_size: u64,
        dir_placement: Option<&PlacementSpec>,
        dir_inline_threshold: Option<u32>,
        policy: &PlacementPolicy,
    ) -> Self {
        // Step 1: 若 inline 阈值 > 0 且文件足够小, 直接 Inline
        if let Some(threshold) = dir_inline_threshold {
            if threshold > 0 && file_size < threshold as u64 {
                return Self {
                    placement: Placement::Inline {
                        max_size: threshold,
                    },
                    reliability: Reliability::Replicated { count: 1 },
                    reliability_state: ReliabilityState::default(),
                    compression: CompressionState::default(),
                    encoding: ChunkEncoding::InlineData { data: Vec::new() },
                };
            }
        }

        // Step 2: 非 inline 路径. 若 inline 被显式禁用 (Some(0)),
        //         用修改后的策略 (inline_max_size=0) 调用 auto_promote
        let effective_policy = if matches!(dir_inline_threshold, Some(0)) {
            let mut p = policy.clone();
            p.inline_max_size = 0;
            p
        } else {
            policy.clone()
        };

        let placement = if let Some(spec) = dir_placement {
            spec_to_placement(spec, &effective_policy)
        } else {
            crate::placement::auto_promote(file_size, &effective_policy)
        };

        let (reliability, encoding) = if placement.is_inline() {
            (
                Reliability::Replicated { count: 1 },
                ChunkEncoding::InlineData { data: Vec::new() },
            )
        } else {
            (
                Reliability::SingleReplica,
                ChunkEncoding::PerChunk { chunks: Vec::new() },
            )
        };

        Self {
            placement,
            reliability,
            reliability_state: ReliabilityState::default(),
            compression: CompressionState::default(),
            encoding,
        }
    }

    /// 创建空文件布局 (StorageMode::Empty).
    ///
    /// 设计文档 §3.1: 新文件创建时返回 Empty 布局, 不预分配任何实际布局.
    /// 第一次 write 时由 `resolve_on_first_write()` 或 `resolve_with_predictor()`
    /// 决定实际布局 (Inline/Flat/Stripe), 避免初始布局误判导致的 Inline→Flat 迁移.
    ///
    /// Empty 状态语义:
    /// - `content_size = 0`, `inline_data = []`, `chunks = []`, `fid = None`
    /// - 读操作返回空数据 (0 bytes)
    /// - placement 为 Flat (占位, 不影响 IO, 因为 content_size=0)
    pub fn for_empty_file() -> Self {
        Self {
            placement: Placement::Flat,
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::default(),
            compression: CompressionState::default(),
            encoding: ChunkEncoding::PerChunk { chunks: Vec::new() },
        }
    }

    /// 第一次写入时通过预测器解析布局 (设计文档 §3.2)
    ///
    /// 调用时机: Empty 状态文件的第一次 write
    /// 返回: 解析后的实际布局 (Inline/Flat/Stripe) 或 None (预测低置信度, 调用方应回退 auto_promote)
    pub fn resolve_with_predictor(
        predictor: &dyn LayoutPredictor,
        ctx: &PredictContext,
        _policy: &PlacementPolicy,
        min_confidence: f32,
    ) -> Option<Self> {
        let result = predictor.predict(ctx);
        if !result.is_confident(min_confidence) {
            log::debug!(
                "LAYOUT_PREDICT: low confidence {} < {}, rule={}, deferring",
                result.confidence,
                min_confidence,
                result.rule_name
            );
            return None;
        }

        log::info!(
            "LAYOUT_PREDICT: file={} rule={} placement={:?} confidence={}",
            ctx.filename,
            result.rule_name,
            result.placement,
            result.confidence
        );

        let (reliability, encoding) = if result.placement.is_inline() {
            (
                Reliability::Replicated { count: 1 },
                ChunkEncoding::InlineData { data: Vec::new() },
            )
        } else {
            (
                Reliability::SingleReplica,
                ChunkEncoding::PerChunk { chunks: Vec::new() },
            )
        };

        Some(Self {
            placement: result.placement,
            reliability,
            reliability_state: ReliabilityState::default(),
            compression: CompressionState::default(),
            encoding,
        })
    }

    /// 第一次写入时通过大小回退解析布局 (auto_promote 策略)
    ///
    /// 当预测器未命中或置信度不足时, 回退到基于写入大小的 auto_promote.
    pub fn resolve_on_first_write(
        write_size: u64,
        dir_placement: Option<&PlacementSpec>,
        dir_inline_threshold: Option<u32>,
        policy: &PlacementPolicy,
    ) -> Self {
        Self::for_new_file(write_size, dir_placement, dir_inline_threshold, policy)
    }

    /// 统一定位入口: file_offset -> (volume_id, volume_offset)
    ///
    /// 内部按 placement 分发:
    /// - Inline/Flat: 返回 None (定位由 encoding 决定)
    /// - Stripe/WideStripe: 调用 placement.locate() + volume_id_at()
    pub fn locate(&self, file_offset: u64) -> Option<(u64, u64)> {
        let (idx, vol_offset) = self.placement.locate(file_offset)?;
        let vol_id = self.placement.volume_id_at(idx)?;
        Some((vol_id, vol_offset))
    }

    /// 是否为 Inline 模式
    pub fn is_inline(&self) -> bool {
        self.placement.is_inline()
    }
}

/// PlacementSpec (xattr 解析结果) -> Placement (带默认参数)
fn spec_to_placement(spec: &PlacementSpec, _policy: &PlacementPolicy) -> Placement {
    match spec {
        PlacementSpec::Flat => Placement::Flat,
        PlacementSpec::Stripe { count, stripe_size } => Placement::Stripe {
            stripe_size: *stripe_size,
            stripe_count: *count,
            start_volume_idx: 0,
            volume_ids: Vec::new(), // 由 Master 分配时填充
        },
        PlacementSpec::WideStripe { count, stripe_size } => Placement::WideStripe {
            stripe_size: *stripe_size,
            stripe_count: *count,
            start_volume_idx: 0,
            volume_ids: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_file_inline_default() {
        let layout = FileLayout::for_new_file(100, None, None, &PlacementPolicy::default());
        assert!(layout.is_inline());
        assert!(matches!(layout.encoding, ChunkEncoding::InlineData { .. }));
    }

    #[test]
    fn new_file_flat_default() {
        let layout = FileLayout::for_new_file(4096, None, None, &PlacementPolicy::default());
        assert!(!layout.is_inline());
        assert!(matches!(layout.placement, Placement::Flat));
        assert!(matches!(layout.encoding, ChunkEncoding::PerChunk { .. }));
    }

    #[test]
    fn new_file_with_dir_inline_threshold() {
        let layout = FileLayout::for_new_file(100, None, Some(8192), &PlacementPolicy::default());
        assert!(layout.is_inline());
        match &layout.placement {
            Placement::Inline { max_size } => assert_eq!(*max_size, 8192),
            _ => panic!("expected Inline"),
        }
    }

    #[test]
    fn new_file_dir_inline_disabled() {
        // inline=0 禁用 inline, 即使文件很小也走 auto_promote
        let layout = FileLayout::for_new_file(100, None, Some(0), &PlacementPolicy::default());
        assert!(!layout.is_inline());
        assert!(matches!(layout.placement, Placement::Flat));
    }

    #[test]
    fn locate_stripe() {
        let layout = FileLayout {
            placement: Placement::Stripe {
                stripe_size: 1024,
                stripe_count: 4,
                start_volume_idx: 0,
                volume_ids: vec![10, 20, 30, 40],
            },
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::default(),
            compression: CompressionState::default(),
            encoding: ChunkEncoding::PerChunk { chunks: Vec::new() },
        };
        // offset 0 -> vol 10, vol_offset 0
        assert_eq!(layout.locate(0), Some((10, 0)));
        // offset 1024 -> vol 20, vol_offset 0
        assert_eq!(layout.locate(1024), Some((20, 0)));
    }

    #[test]
    fn locate_flat_returns_none() {
        let layout = FileLayout {
            placement: Placement::Flat,
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::default(),
            compression: CompressionState::default(),
            encoding: ChunkEncoding::PerChunk { chunks: Vec::new() },
        };
        assert_eq!(layout.locate(0), None);
    }
}
