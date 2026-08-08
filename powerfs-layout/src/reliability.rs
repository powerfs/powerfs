//! 维度 2: Reliability — 数据如何保护 + 状态机
//!
//! 设计文档 S5:
//! - `Reliability`: 可靠性策略 (SingleReplica/Replicated/EC)
//! - `ReliabilityState`: 可靠性状态机 (scrubber 异步转换)
//! - `CompressionState`: 压缩状态

/// 可靠性策略
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum Reliability {
    /// 单副本 (临时态, 写入不等可靠性时用)
    #[default]
    SingleReplica,

    /// N 副本
    Replicated {
        /// 副本数 (含原始副本)
        count: u32,
    },

    /// EC(N+M) 纠删码
    EC {
        /// 数据块数
        data: u32,
        /// 校验块数
        parity: u32,
    },
}

/// 可靠性状态机 (scrubber 异步转换用)
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum ReliabilityState {
    /// 刚写入, 等待后台转换为 Replicated
    #[default]
    PendingReplicated,

    /// 已完成副本复制
    Replicated,

    /// 副本已就绪, 等待 EC 转换
    PendingEC,

    /// EC 编码完成
    EC,

    /// EC 降级 (部分块丢失, 可读但需修复)
    Degraded,
}

/// 压缩状态 (设计文档 S5)
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum CompressionState {
    /// 未压缩
    #[default]
    None,
    /// 等待后台压缩
    Pending,
    /// 已压缩
    Compressed,
}

impl Reliability {
    /// 空间开销因子 (1.0 = 无冗余, 3.0 = 3副本, 1.5 = EC(4+2))
    pub fn overhead_factor(&self) -> f64 {
        match self {
            Reliability::SingleReplica => 1.0,
            Reliability::Replicated { count } => *count as f64,
            Reliability::EC { data, parity } => {
                if *data == 0 {
                    return 1.0;
                }
                (*data + *parity) as f64 / *data as f64
            }
        }
    }

    /// 可容忍故障数 (在不丢数据的前提下)
    pub fn min_survivable_failures(&self) -> u32 {
        match self {
            Reliability::SingleReplica => 0,
            Reliability::Replicated { count } => count.saturating_sub(1),
            Reliability::EC { parity, .. } => *parity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overhead_factor() {
        assert_eq!(Reliability::SingleReplica.overhead_factor(), 1.0);
        assert_eq!(
            Reliability::Replicated { count: 3 }.overhead_factor(),
            3.0
        );
        assert_eq!(
            Reliability::EC { data: 4, parity: 2 }.overhead_factor(),
            1.5
        );
        assert_eq!(
            Reliability::EC { data: 8, parity: 4 }.overhead_factor(),
            1.5
        );
    }

    #[test]
    fn survivable_failures() {
        assert_eq!(Reliability::SingleReplica.min_survivable_failures(), 0);
        assert_eq!(
            Reliability::Replicated { count: 3 }.min_survivable_failures(),
            2
        );
        assert_eq!(
            Reliability::EC { data: 4, parity: 2 }.min_survivable_failures(),
            2
        );
    }
}
