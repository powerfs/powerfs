//! Placement 策略配置 (可配置阈值)
//!
//! 默认值来自设计文档 S4.2 阈值表.
//! 可通过 Filer 配置文件覆盖, 也可通过目录 xattr 精细控制.

/// Placement 自动提升策略参数
#[derive(Clone, Debug)]
pub struct PlacementPolicy {
    /// Inline 阈值 (默认 4KB, 覆盖 IO500 mdtest-hard 3901B).
    /// 文件 < 此值 -> Placement::Inline
    pub inline_max_size: u32,

    /// Flat 上限 (默认 64MB).
    /// 文件 < 此值 -> Placement::Flat
    pub flat_max_size: u64,

    /// Stripe(4) 上限 (默认 1GB).
    /// 文件 < 此值 -> Placement::Stripe(count=4)
    pub stripe4_max_size: u64,

    /// Stripe(16) 上限 (默认 100GB).
    /// 文件 < 此值 -> Placement::Stripe(count=16)
    pub stripe16_max_size: u64,

    /// 是否允许自动提升到 WideStripe (默认 false, 仅显式启用).
    /// WideStripe 默认仅通过目录 xattr 或 API 参数显式启用, 避免误用.
    pub allow_auto_widestripe: bool,

    /// Stripe 默认 stripe_size (默认 64MB)
    pub default_stripe_size: u64,

    /// Stripe 默认 stripe_count (默认 4)
    pub default_stripe_count: u32,

    /// WideStripe 默认 stripe_size (默认 4MB, 小 stripe 高并发)
    pub default_wide_stripe_size: u64,

    /// WideStripe 默认 stripe_count (默认 256, 全集群并行)
    pub default_wide_stripe_count: u32,
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        Self {
            inline_max_size: 4096,
            flat_max_size: 64 * 1024 * 1024,
            stripe4_max_size: 1024 * 1024 * 1024,
            stripe16_max_size: 100 * 1024 * 1024 * 1024,
            allow_auto_widestripe: false,
            default_stripe_size: 64 * 1024 * 1024,
            default_stripe_count: 4,
            default_wide_stripe_size: 4 * 1024 * 1024,
            default_wide_stripe_count: 256,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values() {
        let p = PlacementPolicy::default();
        assert_eq!(p.inline_max_size, 4096);
        assert_eq!(p.flat_max_size, 64 * 1024 * 1024);
        assert_eq!(p.stripe4_max_size, 1024 * 1024 * 1024);
        assert_eq!(p.stripe16_max_size, 100 * 1024 * 1024 * 1024);
        assert!(!p.allow_auto_widestripe);
        assert_eq!(p.default_stripe_size, 64 * 1024 * 1024);
        assert_eq!(p.default_stripe_count, 4);
        assert_eq!(p.default_wide_stripe_size, 4 * 1024 * 1024);
        assert_eq!(p.default_wide_stripe_count, 256);
    }
}
