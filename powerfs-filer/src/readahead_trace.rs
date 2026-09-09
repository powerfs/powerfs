//! readahead_trace.rs — Phase A-1.2 filer 端 IO trace 聚合 + 5 特征提取
//!
//! 详见 docs/ml-prefetch-kernel-rdma-plan.md §4.7.4 + §4.1.1.
//!
//! 接收 kernel PushIoTrace 批量上报, per-inode 聚合 trace 数据,
//! 提取 KML 5 特征供 A-1.3 NN 二分类训练.
//!
//! === 5 特征 (KML 论文, §4.1.1) ===
//!
//! | # | 特征                | KML | PowerFS                          |
//! |---|---------------------|-----|----------------------------------|
//! | 1 | 每秒事务数 (IOPS)   | ✓   | 采样数×100 / 时间窗口            |
//! | 2 | page offset CMA     | ✓   | 环形 buffer offset 累积移动均值  |
//! | 3 | offset 差值均值     | ✓   | 连续 offset 差值绝对值均值 (最重要) |
//! | 4 | inode               | ✓   | ino (per-file 分类标识)          |
//! | 5 | 当前 readahead      | ✓   | xattr 中的 readahead MB 值       |
//!
//! === trace entry 格式 (kernel powerfs_readahead.c, 69 bytes packed) ===
//!
//! ino:u64 + placement:u8 + file_size:u64 + offsets[16]:u16 + kinds[16]:u8
//!   + seq_run:u16 + rand_run:u16

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// kernel 采样率 (1/100), 用于 IOPS 估算
const KERNEL_SAMPLE_RATE: u64 = 100;

/// 每个 inode 的 trace 聚合状态
#[derive(Debug, Clone)]
pub struct InodeTraceState {
    pub ino: u64,
    pub placement: u8,
    pub file_size: u64,
    /// 所有采样 offset (64KB 粒度, 来自 kernel offset>>16)
    pub offsets: Vec<u16>,
    /// 所有采样 kind (0=read, 1=write)
    pub kinds: Vec<u8>,
    /// 最近一次 kernel 上报的 seq_run
    pub last_seq_run: u16,
    /// 最近一次 kernel 上报的 rand_run
    pub last_rand_run: u16,
    /// 总采样数 (跨多次 flush batch 累计)
    pub total_samples: u64,
    /// 首次 trace 时间
    pub first_ts: Instant,
    /// 最近 trace 时间
    pub last_ts: Instant,
    /// 当前 readahead MB (从 xattr 查询, 0=无策略)
    pub current_readahead_mb: u32,
}

/// 提取出的 5 特征向量 (KML §4.1.1)
#[derive(Debug, Clone, PartialEq)]
pub struct TraceFeatures {
    /// 1. 估算 IOPS (采样数×100 / 秒)
    pub iops: f64,
    /// 2. page offset 累积移动均值 (4K page 粒度)
    pub offset_cma: f64,
    /// 3. 连续 offset 差值绝对值均值 (64KB 粒度, **最重要特征**)
    pub offset_delta_mean: f64,
    /// 4. 文件大小 (bytes) — 代替 KML 的 inode 作为数值特征
    pub file_size: u64,
    /// 5. 当前 readahead MB
    pub current_readahead_mb: u32,
}

/// per-inode trace 聚合器
pub struct IoTraceAggregator {
    /// ino → 聚合状态
    traces: Mutex<HashMap<u64, InodeTraceState>>,
}

/// 单条 kernel trace entry (解析后的结构)
#[derive(Debug, Clone)]
pub struct TraceEntry {
    pub ino: u64,
    pub placement: u8,
    pub file_size: u64,
    pub offsets: [u16; 16],
    pub kinds: [u8; 16],
    pub seq_run: u16,
    pub rand_run: u16,
}

/// TraceEntry 大小 (bytes)
pub const TRACE_ENTRY_SIZE: usize = 69;

/// 从 raw bytes 解析一条 TraceEntry (对齐 kernel 序列化)
///
/// body 布局 (69 bytes):
///   ino(8) + placement(1) + file_size(8) + offsets[16](32) + kinds[16](16)
///   + seq_run(2) + rand_run(2)
pub fn parse_trace_entry(buf: &[u8]) -> Option<TraceEntry> {
    if buf.len() < TRACE_ENTRY_SIZE {
        return None;
    }

    let ino = u64::from_le_bytes(buf[0..8].try_into().ok()?);
    let placement = buf[8];
    let file_size = u64::from_le_bytes(buf[9..17].try_into().ok()?);

    let mut offsets = [0u16; 16];
    for i in 0..16 {
        offsets[i] = u16::from_le_bytes(buf[17 + i * 2..19 + i * 2].try_into().ok()?);
    }

    let mut kinds = [0u8; 16];
    kinds.copy_from_slice(&buf[49..65]);

    let seq_run = u16::from_le_bytes(buf[65..67].try_into().ok()?);
    let rand_run = u16::from_le_bytes(buf[67..69].try_into().ok()?);

    Some(TraceEntry {
        ino,
        placement,
        file_size,
        offsets,
        kinds,
        seq_run,
        rand_run,
    })
}

impl IoTraceAggregator {
    pub fn new() -> Self {
        Self {
            traces: Mutex::new(HashMap::new()),
        }
    }

    /// 批量接收 kernel trace entries, 聚合到 per-inode 状态.
    ///
    /// `current_readahead_mb`: 调用方从 xattr 查询后传入 (避免聚合器直接依赖 meta_cache).
    /// 返回本次聚合涉及的 inode 列表 (用于 A-1.3 触发训练).
    pub fn ingest(
        &self,
        entries: &[TraceEntry],
        current_readahead_mb: impl Fn(u64) -> u32,
    ) -> Vec<u64> {
        let now = Instant::now();
        let mut touched_inodes = Vec::new();
        let mut traces = self.traces.lock().unwrap();

        for entry in entries {
            let state = traces.entry(entry.ino).or_insert_with(|| InodeTraceState {
                ino: entry.ino,
                placement: entry.placement,
                file_size: entry.file_size,
                offsets: Vec::new(),
                kinds: Vec::new(),
                last_seq_run: 0,
                last_rand_run: 0,
                total_samples: 0,
                first_ts: now,
                last_ts: now,
                current_readahead_mb: 0,
            });

            // 更新元数据
            state.placement = entry.placement;
            state.file_size = entry.file_size;
            state.last_seq_run = entry.seq_run;
            state.last_rand_run = entry.rand_run;
            state.last_ts = now;
            state.current_readahead_mb = current_readahead_mb(entry.ino);

            // 追加有效 offset/kind (ring buffer 中可能有空槽, idx 后的条目可能为 0)
            // kernel ring buffer 是环形覆盖写入, 我们取全量 16 条
            for i in 0..16 {
                // 跳过 kind=0 且 offset=0 的未使用槽位 (只在首次填充时)
                if entry.kinds[i] == 0 && entry.offsets[i] == 0 && state.total_samples == 0 {
                    continue;
                }
                state.offsets.push(entry.offsets[i]);
                state.kinds.push(entry.kinds[i]);
            }

            state.total_samples += 1;
            touched_inodes.push(entry.ino);
        }

        touched_inodes
    }

    /// 从聚合状态提取 5 特征.
    pub fn extract_features(&self, ino: u64) -> Option<TraceFeatures> {
        let traces = self.traces.lock().unwrap();
        let state = traces.get(&ino)?;

        // 1. IOPS = total_samples * KERNEL_SAMPLE_RATE / elapsed_seconds
        let elapsed = state.last_ts.duration_since(state.first_ts).as_secs_f64();
        let iops = if elapsed > 0.0 {
            (state.total_samples * KERNEL_SAMPLE_RATE) as f64 / elapsed
        } else {
            0.0
        };

        // 2. page offset CMA (4K page 粒度: offset_64k * 16)
        let offset_cma = if state.offsets.is_empty() {
            0.0
        } else {
            let sum: u64 = state.offsets.iter().map(|&o| o as u64).sum();
            (sum as f64 / state.offsets.len() as f64) * 16.0
        };

        // 3. 连续 offset 差值绝对值均值 (64KB 粒度, 最重要特征)
        let offset_delta_mean = if state.offsets.len() < 2 {
            0.0
        } else {
            let mut sum_delta: u64 = 0;
            let mut count: u64 = 0;
            for i in 1..state.offsets.len() {
                let delta = (state.offsets[i] as i32 - state.offsets[i - 1] as i32).unsigned_abs();
                sum_delta += delta as u64;
                count += 1;
            }
            if count > 0 {
                sum_delta as f64 / count as f64
            } else {
                0.0
            }
        };

        Some(TraceFeatures {
            iops,
            offset_cma,
            offset_delta_mean,
            file_size: state.file_size,
            current_readahead_mb: state.current_readahead_mb,
        })
    }

    /// 列出所有有 trace 数据的 inode 及其特征 (用于批量训练)
    pub fn list_all_features(&self) -> Vec<(u64, TraceFeatures, u16, u16)> {
        let traces = self.traces.lock().unwrap();
        traces
            .iter()
            .filter_map(|(&ino, state)| {
                let elapsed = state.last_ts.duration_since(state.first_ts).as_secs_f64();
                let iops = if elapsed > 0.0 {
                    (state.total_samples * KERNEL_SAMPLE_RATE) as f64 / elapsed
                } else {
                    0.0
                };

                let offset_cma = if state.offsets.is_empty() {
                    0.0
                } else {
                    let sum: u64 = state.offsets.iter().map(|&o| o as u64).sum();
                    (sum as f64 / state.offsets.len() as f64) * 16.0
                };

                let offset_delta_mean = if state.offsets.len() < 2 {
                    0.0
                } else {
                    let mut sum_delta: u64 = 0;
                    let mut count: u64 = 0;
                    for i in 1..state.offsets.len() {
                        let delta =
                            (state.offsets[i] as i32 - state.offsets[i - 1] as i32).unsigned_abs();
                        sum_delta += delta as u64;
                        count += 1;
                    }
                    if count > 0 {
                        sum_delta as f64 / count as f64
                    } else {
                        0.0
                    }
                };

                Some((
                    ino,
                    TraceFeatures {
                        iops,
                        offset_cma,
                        offset_delta_mean,
                        file_size: state.file_size,
                        current_readahead_mb: state.current_readahead_mb,
                    },
                    state.last_seq_run,
                    state.last_rand_run,
                ))
            })
            .collect()
    }

    /// 获取 inode 的聚合状态 (用于调试/日志)
    pub fn get_state(&self, ino: u64) -> Option<InodeTraceState> {
        self.traces.lock().unwrap().get(&ino).cloned()
    }

    /// C-1.2: 返回所有 trace inode 的快照 (ino → InodeTraceState clone)
    pub fn traces_snapshot(&self) -> Vec<(u64, InodeTraceState)> {
        self.traces
            .lock()
            .unwrap()
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect()
    }

    /// 清空所有聚合数据 (训练后调用)
    pub fn clear(&self) {
        self.traces.lock().unwrap().clear();
    }

    /// C-1.2: 从聚合状态提取写专用特征 (7 特征, 供 WritePredictNN).
    ///
    /// 特征见 docs/write-prediction-dedup-design.md §3.1.1:
    ///   1. write_count        — 写采样次数
    ///   2. write_offset_delta — 写 offset 差值均值 (覆盖写检测)
    ///   3. overwrite_ratio   — 写 offset < file_size 的比例
    ///   4. file_size          — 当前文件大小 (log1p 归一化)
    ///   5. write_interval    — 写 flush 间隔 (秒)
    ///   6. write_size_var    — 写大小方差 (固定大小=低方差=高重复概率)
    ///   7. seq_write_ratio   — 顺序写占比 (seq_run / (seq_run + rand_run))
    pub fn extract_write_features(&self, ino: u64) -> Option<WriteTraceFeatures> {
        let traces = self.traces.lock().unwrap();
        let state = traces.get(&ino)?;

        // 分离写采样
        let write_kinds: Vec<&u8> = state.kinds.iter().filter(|&&k| k == 1).collect();
        let write_count = write_kinds.len() as u64;

        if write_count == 0 {
            return None;
        }

        // 写 offset 差值均值
        let write_offsets: Vec<u16> = state
            .kinds
            .iter()
            .zip(state.offsets.iter())
            .filter(|(&k, _)| k == 1)
            .map(|(_, &o)| o)
            .collect();

        let write_offset_delta = if write_offsets.len() < 2 {
            0.0
        } else {
            let mut sum = 0u64;
            let mut cnt = 0u64;
            for i in 1..write_offsets.len() {
                let delta = (write_offsets[i] as i32 - write_offsets[i - 1] as i32).unsigned_abs();
                sum += delta as u64;
                cnt += 1;
            }
            if cnt > 0 {
                sum as f64 / cnt as f64
            } else {
                0.0
            }
        };

        // 覆写比例: offset < file_size (64KB 粒度 → bytes)
        let file_size_64k = (state.file_size / 65536) as u16;
        let overwrite_count = write_offsets.iter().filter(|&&o| o < file_size_64k).count();
        let overwrite_ratio = if write_count > 0 {
            overwrite_count as f64 / write_count as f64
        } else {
            0.0
        };

        // 写间隔 (秒)
        let elapsed = state.last_ts.duration_since(state.first_ts).as_secs_f64();
        let write_interval = if write_count > 0 && elapsed > 0.0 {
            elapsed / write_count as f64
        } else {
            0.0
        };

        // 写大小方差 (file_size 变化作为代理指标)
        // 多次写后 file_size 相同 → 覆盖写 → 高重复概率
        let write_size_var = 0.0; // C-1.2 简化: 需要多次 flush 的 file_size 历史

        // 顺序写占比
        let total_runs = state.last_seq_run + state.last_rand_run;
        let seq_write_ratio = if total_runs > 0 {
            state.last_seq_run as f64 / total_runs as f64
        } else {
            0.0
        };

        Some(WriteTraceFeatures {
            write_count,
            write_offset_delta,
            overwrite_ratio,
            file_size: state.file_size,
            write_interval,
            write_size_var,
            seq_write_ratio,
        })
    }
}

/// C-1.2: 写专用特征 (7 特征, 供 WritePredictNN)
#[derive(Debug, Clone, PartialEq)]
pub struct WriteTraceFeatures {
    /// 1. 写采样次数 (采样率 1/100, 实际写次数 = write_count * 100)
    pub write_count: u64,
    /// 2. 写 offset 差值均值 (64KB 粒度)
    pub write_offset_delta: f64,
    /// 3. 覆写比例 (0.0~1.0)
    pub overwrite_ratio: f64,
    /// 4. 文件大小 (bytes)
    pub file_size: u64,
    /// 5. 写间隔 (秒)
    pub write_interval: f64,
    /// 6. 写大小方差 (0.0=固定大小)
    pub write_size_var: f64,
    /// 7. 顺序写占比 (0.0~1.0)
    pub seq_write_ratio: f64,
}

impl WriteTraceFeatures {
    /// 转为 NN 输入向量 (7 维, log1p + 线性归一化)
    pub fn to_nn_input(&self) -> [f64; 7] {
        [
            (self.write_count as f64).ln_1p() / 10.0, // 0~~7
            self.write_offset_delta.ln_1p() / 10.0,   // 0~~10
            self.overwrite_ratio,                     // 0~1
            (self.file_size as f64).ln_1p() / 20.0,   // 0~~3
            self.write_interval.ln_1p() / 5.0,        // 0~~5
            self.write_size_var.ln_1p() / 10.0,       // 0~~10
            self.seq_write_ratio,                     // 0~1
        ]
    }
}

impl Default for IoTraceAggregator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(ino: u64, offsets: [u16; 16], seq_run: u16, rand_run: u16) -> TraceEntry {
        TraceEntry {
            ino,
            placement: 1,
            file_size: 8 * 1024 * 1024,
            offsets,
            kinds: [0; 16],
            seq_run,
            rand_run,
        }
    }

    #[test]
    fn test_parse_trace_entry() {
        // 构造 69 byte buffer
        let mut buf = vec![0u8; 69];
        // ino = 42
        buf[0..8].copy_from_slice(&42u64.to_le_bytes());
        // placement = 1
        buf[8] = 1;
        // file_size = 1MB
        buf[9..17].copy_from_slice(&(1024 * 1024u64).to_le_bytes());
        // offsets[0] = 10, offsets[1] = 12
        buf[17..19].copy_from_slice(&10u16.to_le_bytes());
        buf[19..21].copy_from_slice(&12u16.to_le_bytes());
        // kinds[0] = 0 (read)
        buf[49] = 0;
        buf[50] = 0;
        // seq_run = 5
        buf[65..67].copy_from_slice(&5u16.to_le_bytes());
        // rand_run = 2
        buf[67..69].copy_from_slice(&2u16.to_le_bytes());

        let entry = parse_trace_entry(&buf).unwrap();
        assert_eq!(entry.ino, 42);
        assert_eq!(entry.placement, 1);
        assert_eq!(entry.file_size, 1024 * 1024);
        assert_eq!(entry.offsets[0], 10);
        assert_eq!(entry.offsets[1], 12);
        assert_eq!(entry.seq_run, 5);
        assert_eq!(entry.rand_run, 2);
    }

    #[test]
    fn test_ingest_and_extract_sequential() {
        let agg = IoTraceAggregator::new();

        // 模拟顺序访问: offset 单调递增
        let offsets = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let entry = make_entry(100, offsets, 15, 0);

        agg.ingest(&[entry], |_| 16);

        let features = agg.extract_features(100).unwrap();

        // 顺序访问: delta_mean 应该接近 1 (64KB 粒度)
        assert!(
            (features.offset_delta_mean - 1.0).abs() < 0.1,
            "expected delta_mean≈1.0, got {}",
            features.offset_delta_mean
        );
        assert_eq!(features.file_size, 8 * 1024 * 1024);
        assert_eq!(features.current_readahead_mb, 16);
    }

    #[test]
    fn test_ingest_and_extract_random() {
        let agg = IoTraceAggregator::new();

        // 模拟随机访问: offset 无序
        let offsets = [
            100, 5, 200, 3, 150, 8, 300, 1, 250, 7, 180, 2, 90, 6, 120, 4,
        ];
        let entry = make_entry(200, offsets, 0, 15);

        agg.ingest(&[entry], |_| 0);

        let features = agg.extract_features(200).unwrap();

        // 随机访问: delta_mean 应该远大于 1
        assert!(
            features.offset_delta_mean > 10.0,
            "expected delta_mean>10 for random, got {}",
            features.offset_delta_mean
        );
        assert_eq!(features.current_readahead_mb, 0);
    }

    #[test]
    fn test_multiple_ingest_accumulates() {
        let agg = IoTraceAggregator::new();

        let offsets1 = [0, 1, 2, 3, 4, 5, 6, 7, 0, 0, 0, 0, 0, 0, 0, 0];
        let offsets2 = [8, 9, 10, 11, 12, 13, 14, 15, 0, 0, 0, 0, 0, 0, 0, 0];

        let e1 = make_entry(300, offsets1, 7, 0);
        let e2 = make_entry(300, offsets2, 15, 0);

        agg.ingest(&[e1], |_| 4);
        std::thread::sleep(std::time::Duration::from_millis(10));
        agg.ingest(&[e2], |_| 4);

        let state = agg.get_state(300).unwrap();
        assert!(
            state.total_samples >= 2,
            "total_samples={}",
            state.total_samples
        );

        let features = agg.extract_features(300).unwrap();
        assert!(features.iops > 0.0);
    }
}
