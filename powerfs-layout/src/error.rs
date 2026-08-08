//! 文件布局错误类型
//!
//! 所有 layout 操作的统一错误枚举, 覆盖:
//! - 类型构造/解析错误 (Placement/Reliability/ChunkEncoding)
//! - xattr 解析错误
//! - anti-affinity 约束不满足
//! - TLV 编解码错误
//! - Inline 数据超限

use thiserror::Error;

/// 文件布局操作错误
#[derive(Debug, Error)]
pub enum LayoutError {
    /// 无效的 Placement 值 (未知 tag / 非法参数)
    #[error("invalid placement: {0}")]
    InvalidPlacement(String),

    /// 无效的 Reliability 值 (未知 tag / 非法副本数)
    #[error("invalid reliability: {0}")]
    InvalidReliability(String),

    /// 无效的 ChunkEncoding 值 (未知 tag / 数据损坏)
    #[error("invalid chunk encoding: {0}")]
    InvalidEncoding(String),

    /// xattr 值解析失败
    #[error("invalid xattr value '{value}' for attr '{attr}'")]
    InvalidXattr {
        /// xattr 名称 (powerfs.placement / powerfs.inline)
        attr: String,
        /// 原始值
        value: String,
    },

    /// anti-affinity 约束不满足: 需要的节点数 > 可用节点数
    #[error("insufficient nodes for anti-affinity: need {need}, have {have}")]
    InsufficientNodes {
        /// 需要的不同节点数
        need: usize,
        /// 实际可用的不同节点数
        have: usize,
    },

    /// TLV 解码错误 (字段缺失 / 长度不符 / 格式错误)
    #[error("TLV decode error: {0}")]
    TlvDecode(String),

    /// Inline 数据超过 max_size 限制
    #[error("inline data size {actual} exceeds max {max}")]
    InlineOversize {
        /// 实际数据大小
        actual: usize,
        /// 最大允许大小
        max: usize,
    },

    /// Stripe 参数非法 (stripe_count == 0 / volume_ids 为空)
    #[error("invalid stripe params: {0}")]
    InvalidStripeParams(String),
}

/// layout crate 统一 Result 别名
pub type LayoutResult<T> = Result<T, LayoutError>;

/// 从 NetError 转换 (TLV 编解码错误)
impl From<powerfs_net::NetError> for LayoutError {
    fn from(e: powerfs_net::NetError) -> Self {
        LayoutError::TlvDecode(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let e = LayoutError::InvalidPlacement("unknown tag 0xFF".into());
        assert!(e.to_string().contains("invalid placement"));

        let e = LayoutError::InsufficientNodes { need: 4, have: 2 };
        assert!(e.to_string().contains("need 4"));
        assert!(e.to_string().contains("have 2"));

        let e = LayoutError::InlineOversize { actual: 8192, max: 4096 };
        assert!(e.to_string().contains("8192"));
        assert!(e.to_string().contains("4096"));
    }
}
