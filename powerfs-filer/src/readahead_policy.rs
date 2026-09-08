//! readahead_policy.rs — Phase A-1.3/A-1.5 filer 端 readahead 策略引擎
//!
//! 详见 docs/ml-prefetch-kernel-rdma-plan.md §4.6 (缺省规则) + §4.8 (A-1.3/A-1.5).
//!
//! Phase A-1.5: 规则引擎 (无 ML), 按文件大小决定 per-file readahead,
//!   写入 xattr user.powerfs.readahead_policy = "RULE:<mb>".
//!   kernel 端解析时跳过 "RULE:" 前缀只看数字 (见 powerfs_readahead.c parse_xattr).
//!
//! Phase A-1.3 (本文件): NN 二分类 (random vs sequential), 纯 Rust 实现
//!   MLP 5→8→1 (SGD + backprop), 训练后写 "NN:<mb>" 覆盖规则值.
//!   A-1.6 安全回退: 置信度 < 阈值 → 保持 "RULE:" 不覆盖.
//!
//! 规则 (§4.6.2):
//!   size <  32KB  →  0 MB  (random-ish 小文件, 关闭预取省 RDMA 带宽)
//!   size >= 1MB   → 16 MB  (大文件, 顺序读概率高, 16MB 对齐 RDMA 2MB 帧)
//!   32KB~1MB      →  4 MB  (中等, 保守)
//!
//! 触发点: update_inode_size_chunks_atomic 成功后 (文件大小变化时).
//! 只在策略变化时写 xattr (避免冗余 Raft 写入).

use crate::meta_shard_manager::MetaShardManager;
use crate::raft_group_manager_v2::ShardId;
use crate::readahead_trace::{IoTraceAggregator, TraceFeatures};

/// xattr 名 — 与 kernel 端 powerfs_readahead.h 保持一致
pub const READAHEAD_XATTR_NAME: &str = "user.powerfs.readahead_policy";

/// 规则引擎阈值 (字节)
const THRESHOLD_SMALL: u64 = 32 * 1024; // < 32KB → random
const THRESHOLD_LARGE: u64 = 1024 * 1024; // >= 1MB → sequential

/// 规则引擎返回的 readahead MB 值
const POLICY_RANDOM: u32 = 0; // 关闭预取
const POLICY_CONSERVATIVE: u32 = 4;
const POLICY_SEQUENTIAL: u32 = 16;

/// 按文件大小决定 readahead 策略 (MB).
///
/// 纯函数, 无副作用, 便于单元测试.
pub fn decide_policy_by_size(size: u64) -> u32 {
    if size < THRESHOLD_SMALL {
        POLICY_RANDOM
    } else if size >= THRESHOLD_LARGE {
        POLICY_SEQUENTIAL
    } else {
        POLICY_CONSERVATIVE
    }
}

/// 构造 RULE: 前缀的 xattr value
fn rule_value(mb: u32) -> Vec<u8> {
    format!("RULE:{}", mb).into_bytes()
}

/// 解析 xattr value 中的 MB 数 (兼容 "RULE:N" / "NN:N" / 纯数字).
/// 返回 None 表示解析失败或 xattr 不存在.
fn parse_mb_from_xattr(value: &[u8]) -> Option<u32> {
    let s = std::str::from_utf8(value).ok()?;
    // 跳过前缀
    let num_str = if let Some(rest) = s.strip_prefix("RULE:") {
        rest
    } else if let Some(rest) = s.strip_prefix("NN:") {
        rest
    } else {
        s
    };
    num_str.parse::<u32>().ok()
}

/// 检查 inode 当前 readahead xattr 是否等于 desired_mb.
/// 返回 true = 需要更新 (当前值不同或不存在).
fn needs_update(meta_mgr: &MetaShardManager, inode: u64, desired_mb: u32) -> bool {
    match meta_mgr.get_inode(inode) {
        Some(info) => match info.extended.get(READAHEAD_XATTR_NAME) {
            Some(cur) => parse_mb_from_xattr(cur) != Some(desired_mb),
            None => true, // xattr 不存在
        },
        None => true, // inode 找不到, 尝试设置 (set_xattr 会处理)
    }
}

/// 根据文件大小应用 readahead 策略 (仅在变化时写 xattr).
///
/// 调用上下文: net_handler update_size_chunks 成功后 (异步, 不阻塞响应).
/// 失败不影响主流程 (best-effort): readahead 策略是优化, 不是正确性要求.
///
/// 返回 true = 实际写入了 xattr (策略变化), false = 无需更新或失败.
pub async fn apply_size_based_policy(
    meta_mgr: &MetaShardManager,
    shard_id: ShardId,
    inode: u64,
    size: u64,
) -> bool {
    let desired_mb = decide_policy_by_size(size);

    if !needs_update(meta_mgr, inode, desired_mb) {
        return false;
    }

    let value = rule_value(desired_mb);

    match meta_mgr
        .set_xattr(inode, shard_id, READAHEAD_XATTR_NAME, value)
        .await
    {
        Ok(_) => {
            log::info!(
                "READAHEAD_POLICY: inode={} size={} → RULE:{} (updated)",
                inode,
                size,
                desired_mb
            );
            true
        }
        Err(e) => {
            log::warn!(
                "READAHEAD_POLICY: inode={} size={} set_xattr failed: {}",
                inode,
                size,
                e
            );
            false
        }
    }
}

// =====================================================================
// A-1.3: Lightweight NN (MLP 5→8→1) — pure Rust, no ML crate
// =====================================================================

/// NN 输入特征数 (iops, offset_cma, delta_mean, file_size, current_readahead_mb)
const NN_INPUT_SIZE: usize = 5;
/// 隐藏层神经元数
const NN_HIDDEN_SIZE: usize = 16;
/// 安全回退置信度阈值 (§4.4: 低于此值不干预, 保持 RULE:)
const NN_CONFIDENCE_THRESHOLD: f64 = 0.7;
/// 训练所需最少样本数
const NN_MIN_TRAINING_SAMPLES: usize = 4;
/// 训练轮次
const NN_EPOCHS: usize = 1000;
/// 学习率
const NN_LEARNING_RATE: f64 = 0.05;

/// 轻量二分类 MLP: 5→16→1 (sigmoid 输出, 概率 0=random, 1=sequential)
pub struct ReadaheadNN {
    /// 输入→隐藏层权重 [hidden][input]
    w1: [[f64; NN_INPUT_SIZE]; NN_HIDDEN_SIZE],
    /// 隐藏层偏置
    b1: [f64; NN_HIDDEN_SIZE],
    /// 隐藏→输出权重 [hidden]
    w2: [f64; NN_HIDDEN_SIZE],
    /// 输出偏置
    b2: f64,
}

/// 特征归一化 (log1p 对大值, 线性对小值)
fn normalize_features(f: &TraceFeatures) -> [f64; NN_INPUT_SIZE] {
    [
        // IOPS: log1p 归一化 (0..∞ → 0..~10)
        (f.iops + 1.0).ln(),
        // offset CMA (4K pages): log1p
        (f.offset_cma + 1.0).ln(),
        // delta_mean (64KB 粒度): log1p — **最重要特征**
        (f.offset_delta_mean + 1.0).ln(),
        // file_size: log1p (bytes → 0..~20)
        (f.file_size as f64 + 1.0).ln(),
        // current_readahead_mb: 0/4/16 → 0.0/0.25/1.0
        f.current_readahead_mb as f64 / 16.0,
    ]
}

impl ReadaheadNN {
    /// 创建新模型 (Xavier 初始化)
    pub fn new() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let limit1 = (6.0 / (NN_INPUT_SIZE + NN_HIDDEN_SIZE) as f64).sqrt();
        let limit2 = (6.0 / (NN_HIDDEN_SIZE + 1) as f64).sqrt();

        let mut w1 = [[0.0; NN_INPUT_SIZE]; NN_HIDDEN_SIZE];
        for j in 0..NN_HIDDEN_SIZE {
            for i in 0..NN_INPUT_SIZE {
                w1[j][i] = rng.gen_range(-limit1..limit1);
            }
        }
        let mut b1 = [0.0; NN_HIDDEN_SIZE];
        for j in 0..NN_HIDDEN_SIZE {
            b1[j] = rng.gen_range(-limit1..limit1);
        }
        let mut w2 = [0.0; NN_HIDDEN_SIZE];
        for j in 0..NN_HIDDEN_SIZE {
            w2[j] = rng.gen_range(-limit2..limit2);
        }
        let b2 = rng.gen_range(-limit2..limit2);

        Self { w1, b1, w2, b2 }
    }

    /// 前向传播: 返回 (隐藏层激活, 输出概率)
    fn forward(&self, input: &[f64; NN_INPUT_SIZE]) -> ([f64; NN_HIDDEN_SIZE], f64) {
        // 输入→隐藏 (tanh — 避免死神经元, 零中心化)
        let mut hidden = [0.0; NN_HIDDEN_SIZE];
        for j in 0..NN_HIDDEN_SIZE {
            let mut sum = self.b1[j];
            for i in 0..NN_INPUT_SIZE {
                sum += self.w1[j][i] * input[i];
            }
            hidden[j] = sum.tanh();
        }

        // 隐藏→输出 (numerically stable sigmoid)
        let mut sum = self.b2;
        for j in 0..NN_HIDDEN_SIZE {
            sum += self.w2[j] * hidden[j];
        }
        let output = if sum >= 0.0 {
            1.0 / (1.0 + (-sum).exp())
        } else {
            let e = sum.exp();
            e / (1.0 + e)
        };

        (hidden, output)
    }

    /// 推理: 返回 sequential 概率 (0.0=random, 1.0=sequential)
    pub fn predict(&self, features: &TraceFeatures) -> f64 {
        let input = normalize_features(features);
        let (_, output) = self.forward(&input);
        if output.is_nan() {
            0.5
        } else {
            output
        }
    }

    /// 训练: SGD + 反向传播 (带梯度裁剪)
    /// data: [(特征, 标签)] 标签 1.0=sequential, 0.0=random
    pub fn train(&mut self, data: &[([f64; NN_INPUT_SIZE], f64)]) {
        use rand::seq::SliceRandom;

        const GRADIENT_CLIP: f64 = 2.0;
        const WEIGHT_CLIP: f64 = 10.0;

        for _epoch in 0..NN_EPOCHS {
            let mut indices: Vec<usize> = (0..data.len()).collect();
            indices.shuffle(&mut rand::thread_rng());

            for &idx in &indices {
                let (input, label) = &data[idx];
                let (hidden, output) = self.forward(input);

                // dL/d_output = output - label (BCE 对 sigmoid 的导数)
                let d_output = (output - label).clamp(-GRADIENT_CLIP, GRADIENT_CLIP);

                // 更新 w2, b2
                for j in 0..NN_HIDDEN_SIZE {
                    let grad = d_output * hidden[j];
                    self.w2[j] =
                        (self.w2[j] - NN_LEARNING_RATE * grad).clamp(-WEIGHT_CLIP, WEIGHT_CLIP);
                }
                self.b2 = (self.b2 - NN_LEARNING_RATE * d_output).clamp(-WEIGHT_CLIP, WEIGHT_CLIP);

                // 更新 w1, b1 (tanh 导数 = 1 - tanh²)
                for j in 0..NN_HIDDEN_SIZE {
                    let tanh_grad = 1.0 - hidden[j] * hidden[j];
                    let grad =
                        (d_output * self.w2[j] * tanh_grad).clamp(-GRADIENT_CLIP, GRADIENT_CLIP);
                    for i in 0..NN_INPUT_SIZE {
                        self.w1[j][i] = (self.w1[j][i] - NN_LEARNING_RATE * grad * input[i])
                            .clamp(-WEIGHT_CLIP, WEIGHT_CLIP);
                    }
                    self.b1[j] =
                        (self.b1[j] - NN_LEARNING_RATE * grad).clamp(-WEIGHT_CLIP, WEIGHT_CLIP);
                }
            }
        }
    }
}

impl Default for ReadaheadNN {
    fn default() -> Self {
        Self::new()
    }
}

/// 从聚合器训练全局 NN 模型, 对每个 inode 预测并写 xattr.
///
/// A-1.3: NN 训练 + 推理 + 下发
/// A-1.6: 置信度 < 阈值 → 保持 RULE: 不覆盖
///
/// 返回 (训练样本数, 预测 sequential 数, 预测 random 数, 低置信度跳过数)
pub async fn apply_ml_policy(
    aggregator: &IoTraceAggregator,
    meta_mgr: &MetaShardManager,
    shard_id: ShardId,
) -> (usize, usize, usize, usize) {
    // 收集所有 inode 的特征 + 标签
    let all = aggregator.list_all_features();
    if all.len() < NN_MIN_TRAINING_SAMPLES {
        log::debug!(
            "READAHEAD_ML: skipping training, only {} samples (need {})",
            all.len(),
            NN_MIN_TRAINING_SAMPLES
        );
        return (all.len(), 0, 0, 0);
    }

    // 构造训练数据: 标签 = if seq_run > rand_run { 1.0 } else { 0.0 }
    let train_data: Vec<([f64; NN_INPUT_SIZE], f64)> = all
        .iter()
        .map(|(_, features, seq_run, rand_run)| {
            let input = normalize_features(features);
            let label = if seq_run > rand_run { 1.0 } else { 0.0 };
            (input, label)
        })
        .collect();

    let seq_count = train_data.iter().filter(|(_, l)| *l > 0.5).count();
    let rand_count = train_data.len() - seq_count;

    // 训练
    let mut nn = ReadaheadNN::new();
    nn.train(&train_data);

    // 推理 + 下发
    let mut predicted_seq = 0;
    let mut predicted_rand = 0;
    let mut low_confidence = 0;

    for (ino, features, _, _) in &all {
        let prob = nn.predict(features); // 0=random, 1=sequential
        let confidence = prob.max(1.0 - prob); // 取较大值作为置信度

        if confidence < NN_CONFIDENCE_THRESHOLD {
            low_confidence += 1;
            log::debug!(
                "READAHEAD_ML: ino={} prob={:.3} confidence={:.3} < {} → skip (keep RULE)",
                ino,
                prob,
                confidence,
                NN_CONFIDENCE_THRESHOLD
            );
            continue;
        }

        // 预测结果 → readahead MB
        let desired_mb = if prob > 0.5 {
            POLICY_SEQUENTIAL // 16
        } else {
            POLICY_RANDOM // 0
        };

        // 检查当前 xattr 是否需要更新 (RULE: → NN: 覆盖, 或 NN: 值变化)
        let current_mb = match meta_mgr.get_inode(*ino) {
            Some(info) => match info.extended.get(READAHEAD_XATTR_NAME) {
                Some(cur) => parse_mb_from_xattr(cur),
                None => None,
            },
            None => None,
        };

        if current_mb == Some(desired_mb) {
            continue; // 已有正确值, 跳过
        }

        let xattr_value = nn_value(desired_mb);
        match meta_mgr
            .set_xattr(*ino, shard_id, READAHEAD_XATTR_NAME, xattr_value)
            .await
        {
            Ok(_) => {
                if desired_mb > 0 {
                    predicted_seq += 1;
                } else {
                    predicted_rand += 1;
                }
                log::info!(
                    "READAHEAD_ML: ino={} prob={:.3} confidence={:.3} → NN:{} (updated from {:?})",
                    ino,
                    prob,
                    confidence,
                    desired_mb,
                    current_mb
                );
            }
            Err(e) => {
                log::warn!("READAHEAD_ML: ino={} set_xattr failed: {}", ino, e);
            }
        }
    }

    log::info!(
        "READAHEAD_ML: trained on {} samples ({} seq, {} rand), predicted: {}→NN:16, {}→NN:0, {} low-confidence skipped",
        train_data.len(),
        seq_count,
        rand_count,
        predicted_seq,
        predicted_rand,
        low_confidence
    );

    (
        train_data.len(),
        predicted_seq,
        predicted_rand,
        low_confidence,
    )
}

/// 构造 NN: 前缀的 xattr value
fn nn_value(mb: u32) -> Vec<u8> {
    format!("NN:{}", mb).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decide_policy_by_size() {
        assert_eq!(decide_policy_by_size(0), POLICY_RANDOM);
        assert_eq!(decide_policy_by_size(1024), POLICY_RANDOM);
        assert_eq!(decide_policy_by_size(32 * 1024 - 1), POLICY_RANDOM);
        assert_eq!(decide_policy_by_size(32 * 1024), POLICY_CONSERVATIVE);
        assert_eq!(decide_policy_by_size(512 * 1024), POLICY_CONSERVATIVE);
        assert_eq!(decide_policy_by_size(1024 * 1024 - 1), POLICY_CONSERVATIVE);
        assert_eq!(decide_policy_by_size(1024 * 1024), POLICY_SEQUENTIAL);
        assert_eq!(decide_policy_by_size(100 * 1024 * 1024), POLICY_SEQUENTIAL);
    }

    #[test]
    fn test_parse_mb_from_xattr() {
        assert_eq!(parse_mb_from_xattr(b"RULE:0"), Some(0));
        assert_eq!(parse_mb_from_xattr(b"RULE:16"), Some(16));
        assert_eq!(parse_mb_from_xattr(b"NN:4"), Some(4));
        assert_eq!(parse_mb_from_xattr(b"8"), Some(8));
        assert_eq!(parse_mb_from_xattr(b""), None);
        assert_eq!(parse_mb_from_xattr(b"RULE:"), None);
        assert_eq!(parse_mb_from_xattr(b"abc"), None);
    }

    #[test]
    fn test_rule_value() {
        assert_eq!(rule_value(0), b"RULE:0");
        assert_eq!(rule_value(16), b"RULE:16");
    }

    #[test]
    fn test_nn_value() {
        assert_eq!(nn_value(0), b"NN:0");
        assert_eq!(nn_value(16), b"NN:16");
    }

    #[test]
    fn test_nn_classifies_sequential_vs_random() {
        // 构造模拟特征: 顺序访问 delta_mean 小, 随机访问 delta_mean 大
        let seq_features = TraceFeatures {
            iops: 100.0,
            offset_cma: 500.0,
            offset_delta_mean: 1.0, // 顺序: delta 小
            file_size: 8 * 1024 * 1024,
            current_readahead_mb: 16,
        };
        let rand_features = TraceFeatures {
            iops: 100.0,
            offset_cma: 500.0,
            offset_delta_mean: 50.0, // 随机: delta 大
            file_size: 8 * 1024 * 1024,
            current_readahead_mb: 16,
        };

        // 构造训练数据 (多样本以便 NN 收敛)
        let mut train_data = Vec::new();
        for i in 0..20 {
            let seq_input = normalize_features(&TraceFeatures {
                iops: 50.0 + i as f64 * 10.0,
                offset_cma: 200.0 + i as f64 * 30.0,
                offset_delta_mean: 1.0 + i as f64 * 0.2,
                file_size: 4 * 1024 * 1024 + i as u64 * 4096,
                current_readahead_mb: 16,
            });
            let rand_input = normalize_features(&TraceFeatures {
                iops: 50.0 + i as f64 * 10.0,
                offset_cma: 200.0 + i as f64 * 30.0,
                offset_delta_mean: 30.0 + i as f64 * 5.0,
                file_size: 4 * 1024 * 1024 + i as u64 * 4096,
                current_readahead_mb: 16,
            });
            train_data.push((seq_input, 1.0)); // sequential
            train_data.push((rand_input, 0.0)); // random
        }

        let mut nn = ReadaheadNN::new();
        nn.train(&train_data);

        // 验证分类
        let seq_prob = nn.predict(&seq_features);
        let rand_prob = nn.predict(&rand_features);

        assert!(
            seq_prob > 0.5,
            "sequential should predict > 0.5, got {}",
            seq_prob
        );
        assert!(
            rand_prob < 0.5,
            "random should predict < 0.5, got {}",
            rand_prob
        );
        assert!(
            seq_prob > rand_prob,
            "seq_prob ({}) should > rand_prob ({})",
            seq_prob,
            rand_prob
        );
    }
}
