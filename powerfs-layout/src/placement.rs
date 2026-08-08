//! 维度 1: Placement — 数据如何分布到 volume
//!
//! 四态枚举 (设计文档 S4.1):
//! - `Placement::Inline`: 数据直接存 Filer 元数据 (微小文件 < 4KB)
//! - `Placement::Flat`: 单 volume (小文件默认)
//! - `Placement::Stripe`: 中等并行 (4-16 volume)
//! - `Placement::WideStripe`: 全集群并行 (128-256 volume)
//!
//! 核心算法 [`Placement::locate()`]: 根据 file_offset 计算 (volume 数组下标, volume 内偏移)

use crate::error::LayoutError;
use crate::policy::PlacementPolicy;

/// Placement 四态枚举
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Placement {
    /// 数据直接存 Filer 元数据 (微小文件, < 4KB 默认).
    /// 通过 Raft 隐式复制, 无 Volume Server 参与.
    /// 与 ChunkEncoding::InlineData 绑定.
    Inline {
        /// 阈值, 默认 4KB, 可配到 8KB
        max_size: u32,
    },

    /// 单 volume, 小文件默认
    Flat,

    /// 中等并行 (4-16 volume)
    Stripe {
        /// 默认 64MB
        stripe_size: u64,
        /// 默认 4
        stripe_count: u32,
        /// round-robin 错开, 避免热点
        start_volume_idx: u32,
        /// 显式卷列表
        volume_ids: Vec<u64>,
    },

    /// 全集群并行 (128-256 volume)
    WideStripe {
        /// 默认 4MB (小 stripe 高并发)
        stripe_size: u64,
        /// 128 或 256
        stripe_count: u32,
        start_volume_idx: u32,
        /// 范围压缩编码
        volume_ids: Vec<u64>,
    },
}

/// xattr 解析结果 (不含 Inline, Inline 由独立 powerfs.inline 控制)
///
/// 从 `powerfs.placement` xattr 解析得到, 用于创建文件时继承父目录策略.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlacementSpec {
    /// `flat`
    Flat,
    /// `stripe:<count>:<size>`
    Stripe { count: u32, stripe_size: u64 },
    /// `wide_stripe:<count>:<size>`
    WideStripe { count: u32, stripe_size: u64 },
}

impl Placement {
    /// 根据 file_offset 计算 (volume_ids 数组下标, volume 内偏移).
    ///
    /// 算法 (设计文档 S4.3):
    /// ```text
    /// stripe_idx = file_offset / stripe_size
    /// vol_rank   = stripe_idx % stripe_count
    /// vol_idx    = (start_volume_idx + vol_rank) % volume_ids.len()
    /// vol_offset = (stripe_idx / stripe_count) * stripe_size + (file_offset % stripe_size)
    /// ```
    ///
    /// Inline/Flat 返回 None (不走 stripe 定位):
    /// - Inline: 数据在元数据中, 无 volume 概念
    /// - Flat: 单 volume, 定位由 ChunkEncoding 的 volume_id 决定
    pub fn locate(&self, file_offset: u64) -> Option<(usize, u64)> {
        match self {
            Placement::Inline { .. } | Placement::Flat => None,
            Placement::Stripe {
                stripe_size,
                stripe_count,
                start_volume_idx,
                volume_ids,
            }
            | Placement::WideStripe {
                stripe_size,
                stripe_count,
                start_volume_idx,
                volume_ids,
            } => {
                if volume_ids.is_empty() || *stripe_count == 0 {
                    return None;
                }
                let stripe_size = (*stripe_size).max(1);
                let stripe_idx = file_offset / stripe_size;
                let vol_rank = (stripe_idx % *stripe_count as u64) as u32;
                let vol_array_idx =
                    ((*start_volume_idx + vol_rank) as usize) % volume_ids.len();
                let vol_offset = (stripe_idx / *stripe_count as u64) * stripe_size
                    + (file_offset % stripe_size);
                Some((vol_array_idx, vol_offset))
            }
        }
    }

    /// 是否为 Inline 模式
    pub fn is_inline(&self) -> bool {
        matches!(self, Placement::Inline { .. })
    }

    /// 涉及的 volume 数量
    pub fn volume_count(&self) -> usize {
        match self {
            Placement::Inline { .. } => 0,
            Placement::Flat => 1,
            Placement::Stripe { volume_ids, .. }
            | Placement::WideStripe { volume_ids, .. } => volume_ids.len(),
        }
    }

    /// 获取指定下标的 volume_id
    pub fn volume_id_at(&self, idx: usize) -> Option<u64> {
        match self {
            Placement::Inline { .. } => None,
            Placement::Flat => None,
            Placement::Stripe { volume_ids, .. }
            | Placement::WideStripe { volume_ids, .. } => volume_ids.get(idx).copied(),
        }
    }

    /// 校验参数合法性
    pub fn validate(&self) -> Result<(), LayoutError> {
        match self {
            Placement::Inline { max_size } => {
                if *max_size == 0 {
                    return Err(LayoutError::InvalidPlacement(
                        "Inline max_size must be > 0".into(),
                    ));
                }
                if *max_size as usize > 8 * 1024 {
                    return Err(LayoutError::InlineOversize {
                        actual: *max_size as usize,
                        max: 8 * 1024,
                    });
                }
            }
            Placement::Flat => {}
            Placement::Stripe {
                stripe_count,
                volume_ids,
                ..
            }
            | Placement::WideStripe {
                stripe_count,
                volume_ids,
                ..
            } => {
                if *stripe_count == 0 {
                    return Err(LayoutError::InvalidStripeParams(
                        "stripe_count must be > 0".into(),
                    ));
                }
                if volume_ids.is_empty() {
                    return Err(LayoutError::InvalidStripeParams(
                        "volume_ids must not be empty".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// 根据文件大小自动选择 Placement (设计文档 S4.2 阈值表).
///
/// 默认阈值:
/// - < 4KB      -> Inline(4096)
/// - < 64MB     -> Flat
/// - < 1GB      -> Stripe(4, 64MB)
/// - < 100GB    -> Stripe(16, 64MB)
/// - >= 100GB   -> WideStripe(256, 4MB)  (仅显式启用, 否则降级 Stripe(16))
///
/// 注意: WideStripe 默认仅显式启用, 此函数返回的 WideStripe 仅在
/// `policy.allow_auto_widestripe = true` 时生效.
pub fn auto_promote(file_size: u64, policy: &PlacementPolicy) -> Placement {
    if file_size < policy.inline_max_size as u64 {
        Placement::Inline {
            max_size: policy.inline_max_size,
        }
    } else if file_size < policy.flat_max_size {
        Placement::Flat
    } else if file_size < policy.stripe4_max_size {
        Placement::Stripe {
            stripe_size: policy.default_stripe_size,
            stripe_count: policy.default_stripe_count,
            start_volume_idx: 0,
            volume_ids: Vec::new(), // 由 Master 分配时填充
        }
    } else if file_size < policy.stripe16_max_size {
        Placement::Stripe {
            stripe_size: policy.default_stripe_size,
            stripe_count: 16,
            start_volume_idx: 0,
            volume_ids: Vec::new(),
        }
    } else if policy.allow_auto_widestripe {
        Placement::WideStripe {
            stripe_size: policy.default_wide_stripe_size,
            stripe_count: policy.default_wide_stripe_count,
            start_volume_idx: 0,
            volume_ids: Vec::new(),
        }
    } else {
        // WideStripe 未启用, 降级到 Stripe(16)
        Placement::Stripe {
            stripe_size: policy.default_stripe_size,
            stripe_count: 16,
            start_volume_idx: 0,
            volume_ids: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stripe_placement(stripe_size: u64, count: u32, n_vols: usize) -> Placement {
        Placement::Stripe {
            stripe_size,
            stripe_count: count,
            start_volume_idx: 0,
            volume_ids: (1..=n_vols as u64).collect(),
        }
    }

    #[test]
    fn locate_inline_returns_none() {
        let p = Placement::Inline { max_size: 4096 };
        assert_eq!(p.locate(0), None);
        assert_eq!(p.locate(100), None);
    }

    #[test]
    fn locate_flat_returns_none() {
        let p = Placement::Flat;
        assert_eq!(p.locate(0), None);
    }

    #[test]
    fn locate_stripe_basic() {
        // 4 volumes, stripe_size=64MB, stripe_count=4
        let p = stripe_placement(64 * 1024 * 1024, 4, 4);

        // offset 0 -> vol 0, vol_offset 0
        assert_eq!(p.locate(0), Some((0, 0)));

        // offset 64MB -> vol 1, vol_offset 0
        assert_eq!(p.locate(64 * 1024 * 1024), Some((1, 0)));

        // offset 128MB -> vol 2, vol_offset 0
        assert_eq!(p.locate(128 * 1024 * 1024), Some((2, 0)));

        // offset 256MB -> wraps to vol 0, vol_offset 64MB
        assert_eq!(
            p.locate(256 * 1024 * 1024),
            Some((0, 64 * 1024 * 1024))
        );
    }

    #[test]
    fn locate_stripe_with_start_offset() {
        let p = Placement::Stripe {
            stripe_size: 1024,
            stripe_count: 4,
            start_volume_idx: 2, // 错开起始
            volume_ids: vec![10, 20, 30, 40],
        };
        // offset 0 -> vol_rank=0, idx=(2+0)%4=2 -> vol 30
        assert_eq!(p.locate(0), Some((2, 0)));
        // offset 1024 -> vol_rank=1, idx=(2+1)%4=3 -> vol 40
        assert_eq!(p.locate(1024), Some((3, 0)));
        // offset 2048 -> vol_rank=2, idx=(2+2)%4=0 -> vol 10
        assert_eq!(p.locate(2048), Some((0, 0)));
    }

    #[test]
    fn locate_stripe_partial_offset() {
        let p = stripe_placement(1024, 2, 2);
        // offset 500 within first stripe -> vol 0, vol_offset 500
        assert_eq!(p.locate(500), Some((0, 500)));
        // offset 1024+500 -> vol 1, vol_offset 500
        assert_eq!(p.locate(1024 + 500), Some((1, 500)));
        // offset 2048+500 -> wraps vol 0, vol_offset 1024+500
        assert_eq!(p.locate(2048 + 500), Some((0, 1024 + 500)));
    }

    #[test]
    fn validate_rejects_bad_params() {
        let p = Placement::Inline { max_size: 0 };
        assert!(p.validate().is_err());

        let p = Placement::Inline { max_size: 16384 };
        assert!(p.validate().is_err()); // > 8KB

        let p = Placement::Stripe {
            stripe_size: 64,
            stripe_count: 0,
            start_volume_idx: 0,
            volume_ids: vec![1, 2],
        };
        assert!(p.validate().is_err());

        let p = Placement::Stripe {
            stripe_size: 64,
            stripe_count: 4,
            start_volume_idx: 0,
            volume_ids: vec![],
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn auto_promote_thresholds() {
        let policy = PlacementPolicy::default();

        // < 4KB -> Inline
        assert!(matches!(
            auto_promote(100, &policy),
            Placement::Inline { .. }
        ));

        // < 64MB -> Flat
        assert_eq!(auto_promote(4096, &policy), Placement::Flat);
        assert_eq!(auto_promote(63 * 1024 * 1024, &policy), Placement::Flat);

        // < 1GB -> Stripe(4)
        let p = auto_promote(64 * 1024 * 1024, &policy);
        assert!(matches!(p, Placement::Stripe { stripe_count: 4, .. }));

        // < 100GB -> Stripe(16)
        let p = auto_promote(2 * 1024 * 1024 * 1024, &policy);
        assert!(matches!(p, Placement::Stripe { stripe_count: 16, .. }));

        // >= 100GB, no auto widestripe -> Stripe(16)
        let p = auto_promote(200 * 1024 * 1024 * 1024, &policy);
        assert!(matches!(p, Placement::Stripe { stripe_count: 16, .. }));
    }

    #[test]
    fn volume_count() {
        assert_eq!(Placement::Inline { max_size: 4096 }.volume_count(), 0);
        assert_eq!(Placement::Flat.volume_count(), 1);
        assert_eq!(
            stripe_placement(64, 4, 6).volume_count(),
            6
        );
    }
}
