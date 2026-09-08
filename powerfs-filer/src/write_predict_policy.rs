//! write_predict_policy.rs — Phase C-1.3 filer 端写预测 NN + 策略引擎
//!
//! 详见 docs/write-prediction-dedup-design.md §3.1.
//!
//! Phase C-1.3: NN 二分类 (写重复概率), 纯 Rust 实现
//!   MLP 7→16→1 (SGD + backprop), 训练后写 "NN:<threshold>" 到 xattr.
//!   xattr: user.powerfs.write_predict_policy = "NN:0.3" | "RULE:0.5" | "off"
//!
//! Phase C-1.4: 通过 xattr 下发策略到客户端
//! Phase C-1.5: 客户端读取 xattr, 决定是否计算 fingerprint
//!
//! 规则引擎 (冷启动兜底):
//!   同一 inode 第 2+ 次覆盖写 → 高重复概率 (threshold 0.5)
//!   文件 < 4KB → 中重复概率 (threshold 0.3)
//!   首次写 → 低重复概率 (threshold 0.0, 不算指纹)
//!   追加写 → 低重复概率 (threshold 0.1)

use crate::meta_shard_manager::MetaShardManager;
use crate::raft_group_manager_v2::ShardId;
use crate::readahead_trace::IoTraceAggregator;

/// xattr 名 — 写预测策略
pub const WRITE_PREDICT_XATTR_NAME: &str = "user.powerfs.write_predict_policy";

/// NN 输入维度 (7 写特征)
const NN_INPUT_SIZE: usize = 7;
/// NN 隐藏层大小
const NN_HIDDEN_SIZE: usize = 16;
/// 训练 epochs
const NN_EPOCHS: usize = 1000;
/// 学习率
const NN_LEARNING_RATE: f64 = 0.05;
/// 默认预测阈值 (概率 > 此值才计算 fingerprint)
const DEFAULT_THRESHOLD: f64 = 0.3;
/// 置信度阈值 (NN 概率 > 此值才覆盖规则)
const CONFIDENCE_THRESHOLD: f64 = 0.7;

/// 轻量二分类 MLP: 7→16→1 (sigmoid 输出, P(重复写))
pub struct WritePredictNN {
    w1: [[f64; NN_INPUT_SIZE]; NN_HIDDEN_SIZE],
    b1: [f64; NN_HIDDEN_SIZE],
    w2: [f64; NN_HIDDEN_SIZE],
    b2: f64,
}

impl WritePredictNN {
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
        let mut hidden = [0.0; NN_HIDDEN_SIZE];
        for j in 0..NN_HIDDEN_SIZE {
            let mut sum = self.b1[j];
            for i in 0..NN_INPUT_SIZE {
                sum += self.w1[j][i] * input[i];
            }
            hidden[j] = sum.tanh();
        }

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

    /// 预测: 返回 P(重复写) ∈ [0, 1]
    pub fn predict(&self, input: &[f64; NN_INPUT_SIZE]) -> f64 {
        let (_, output) = self.forward(input);
        output
    }

    /// 训练: SGD + 梯度裁剪
    /// labels: 1.0 = 写重复 (fingerprint hit), 0.0 = 不重复
    pub fn train(&mut self, samples: &[([f64; NN_INPUT_SIZE], f64)]) {
        for _epoch in 0..NN_EPOCHS {
            for (input, label) in samples {
                let (hidden, output) = self.forward(input);

                // 反向传播
                let d_output = output - label; // dL/dz (BCE → sigmoid → linear)

                // 梯度裁剪
                let d_output = d_output.clamp(-5.0, 5.0);

                // 隐藏层梯度
                let mut d_hidden = [0.0; NN_HIDDEN_SIZE];
                for j in 0..NN_HIDDEN_SIZE {
                    d_hidden[j] = d_output * self.w2[j] * (1.0 - hidden[j] * hidden[j]);
                }

                // 更新 w2, b2
                for j in 0..NN_HIDDEN_SIZE {
                    self.w2[j] -= NN_LEARNING_RATE * d_output * hidden[j];
                }
                self.b2 -= NN_LEARNING_RATE * d_output;

                // 更新 w1, b1
                for j in 0..NN_HIDDEN_SIZE {
                    for i in 0..NN_INPUT_SIZE {
                        self.w1[j][i] -= NN_LEARNING_RATE * d_hidden[j] * input[i];
                    }
                    self.b1[j] -= NN_LEARNING_RATE * d_hidden[j];
                }
            }
        }
    }
}

impl Default for WritePredictNN {
    fn default() -> Self {
        Self::new()
    }
}

/// 规则引擎: 根据写特征决定 fingerprint 阈值 (0.0 = 不算, 1.0 = 总是算)
fn rule_threshold(features: &crate::readahead_trace::WriteTraceFeatures) -> f64 {
    // 高覆写比例 + 多次写 → 高重复概率
    if features.overwrite_ratio > 0.5 && features.write_count > 1 {
        return 0.5;
    }
    // 小文件 → 中重复概率
    if features.file_size < 4 * 1024 {
        return 0.3;
    }
    // 追加写 (seq_write_ratio 高 但 overwrite_ratio 低) → 低重复
    if features.seq_write_ratio > 0.7 && features.overwrite_ratio < 0.3 {
        return 0.1;
    }
    // 默认: 中等阈值
    0.3
}

/// 应用 ML 写预测策略: 从 trace 聚合器提取写特征,
/// 训练 NN, 对每个 inode 预测重复概率, 下发 xattr 策略.
///
/// 返回 (total_samples, high_prob_count, low_confidence_count).
pub async fn apply_ml_write_policy(
    aggregator: &IoTraceAggregator,
    meta_mgr: &MetaShardManager,
    shard_id: ShardId,
) -> (usize, usize, usize) {
    // 收集所有有写 trace 的 inode 及其特征
    let all_features: Vec<(u64, crate::readahead_trace::WriteTraceFeatures)> = aggregator
        .traces_snapshot()
        .into_iter()
        .filter_map(|(ino, _)| aggregator.extract_write_features(ino).map(|f| (ino, f)))
        .collect();

    if all_features.is_empty() {
        return (0, 0, 0);
    }

    // C-1.3: 训练数据 = 规则引擎标签 (冷启动)
    // 正式环境需要 fingerprint hit/miss 反馈作为真实标签
    let training_samples: Vec<([f64; NN_INPUT_SIZE], f64)> = all_features
        .iter()
        .map(|(_, f)| {
            let label = if f.overwrite_ratio > 0.5 && f.write_count > 1 {
                1.0
            } else {
                0.0
            };
            (f.to_nn_input(), label)
        })
        .collect();

    if training_samples.len() < 2 {
        // 不足训练, 只用规则引擎
        for (ino, f) in &all_features {
            let threshold = rule_threshold(f);
            let _ = update_write_policy_xattr(meta_mgr, *ino, shard_id, threshold, "RULE").await;
        }
        return (all_features.len(), 0, all_features.len());
    }

    // 训练 NN
    let mut nn = WritePredictNN::new();
    nn.train(&training_samples);

    // 对每个 inode 预测并下发策略
    let mut total = 0usize;
    let mut high_prob = 0usize;
    let mut low_conf = 0usize;

    for (ino, f) in &all_features {
        let input = f.to_nn_input();
        let prob = nn.predict(&input);
        let rule_thresh = rule_threshold(f);

        // 置信度判断: NN 概率远离 0.5 → 高置信
        let (threshold, prefix) = if (prob - 0.5).abs() > (CONFIDENCE_THRESHOLD - 0.5) {
            // 高置信: 用 NN 阈值
            let thresh = if prob > CONFIDENCE_THRESHOLD {
                DEFAULT_THRESHOLD
            } else {
                0.1
            };
            if thresh >= DEFAULT_THRESHOLD {
                high_prob += 1;
            }
            (thresh, "NN")
        } else {
            // 低置信: 用规则引擎兜底
            low_conf += 1;
            (rule_thresh, "RULE")
        };

        let _ = update_write_policy_xattr(meta_mgr, *ino, shard_id, threshold, prefix).await;
        total += 1;
    }

    (total, high_prob, low_conf)
}

/// 更新 inode 的 write_predict_policy xattr.
/// 只在策略值变化时写入 (避免冗余 Raft 写).
async fn update_write_policy_xattr(
    meta_mgr: &MetaShardManager,
    inode: u64,
    shard_id: ShardId,
    threshold: f64,
    prefix: &str,
) -> bool {
    let desired = format!("{}:{}", prefix, threshold);
    let desired_bytes = desired.as_bytes();

    // 检查当前值
    if let Some(info) = meta_mgr.get_inode(inode) {
        if let Some(cur) = info.extended.get(WRITE_PREDICT_XATTR_NAME) {
            if cur == desired_bytes {
                return false; // 无变化
            }
        }
    }

    // 写入 xattr (通过 Raft propose)
    let _ = meta_mgr
        .set_xattr(
            inode,
            shard_id,
            WRITE_PREDICT_XATTR_NAME,
            desired_bytes.to_vec(),
        )
        .await;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nn_predict_range() {
        let nn = WritePredictNN::new();
        let input = [0.5; NN_INPUT_SIZE];
        let prob = nn.predict(&input);
        assert!(prob >= 0.0 && prob <= 1.0);
    }

    #[test]
    fn test_nn_train_converges() {
        let mut nn = WritePredictNN::new();
        // 高覆写特征 → label 1.0
        let high_overwrite = [0.5, 0.1, 0.9, 0.5, 0.2, 0.0, 0.3];
        // 首次写特征 → label 0.0
        let first_write = [0.0, 0.5, 0.0, 0.5, 0.1, 0.0, 0.5];

        let samples = vec![
            (high_overwrite, 1.0),
            (first_write, 0.0),
            (high_overwrite, 1.0),
            (first_write, 0.0),
        ];

        nn.train(&samples);

        // 训练后, 高覆写特征应有更高概率
        let p_high = nn.predict(&high_overwrite);
        let p_low = nn.predict(&first_write);
        assert!(
            p_high > p_low,
            "high_overwrite should predict higher: {} > {}",
            p_high,
            p_low
        );
    }

    #[test]
    fn test_rule_threshold() {
        // 高覆写 + 多次写 → 0.5
        let f1 = crate::readahead_trace::WriteTraceFeatures {
            write_count: 5,
            write_offset_delta: 1.0,
            overwrite_ratio: 0.8,
            file_size: 1024 * 1024,
            write_interval: 0.1,
            write_size_var: 0.0,
            seq_write_ratio: 0.5,
        };
        assert!((rule_threshold(&f1) - 0.5).abs() < 0.01);

        // 小文件 → 0.3
        let f2 = crate::readahead_trace::WriteTraceFeatures {
            write_count: 1,
            write_offset_delta: 0.0,
            overwrite_ratio: 0.0,
            file_size: 2048,
            write_interval: 1.0,
            write_size_var: 0.0,
            seq_write_ratio: 0.0,
        };
        assert!((rule_threshold(&f2) - 0.3).abs() < 0.01);

        // 追加写 → 0.1
        let f3 = crate::readahead_trace::WriteTraceFeatures {
            write_count: 3,
            write_offset_delta: 5.0,
            overwrite_ratio: 0.1,
            file_size: 10 * 1024 * 1024,
            write_interval: 0.5,
            write_size_var: 0.0,
            seq_write_ratio: 0.9,
        };
        assert!((rule_threshold(&f3) - 0.1).abs() < 0.01);
    }

    #[test]
    fn test_write_features_to_nn_input() {
        let f = crate::readahead_trace::WriteTraceFeatures {
            write_count: 10,
            write_offset_delta: 2.0,
            overwrite_ratio: 0.7,
            file_size: 4096,
            write_interval: 0.05,
            write_size_var: 0.0,
            seq_write_ratio: 0.3,
        };
        let input = f.to_nn_input();
        assert_eq!(input.len(), NN_INPUT_SIZE);
        // 所有输入应在合理范围
        for &v in &input {
            assert!(v >= 0.0, "input should be non-negative: {}", v);
        }
    }
}
