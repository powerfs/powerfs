//! S6 确定性故障模拟器（方案 §15.1 / 附录 B S6）。
//!
//! VOPR 式种子回放：每个种子生成一条确定性随机操作序列（write 覆写 /
//! delete / flush 屏障，async 与 strict 混合），在每个操作边界注入一次
//! crash——把盘面重建为「durable 前缀 + 可选撕裂尾」（等价于 page cache
//! 丢失 / fsync 进行中断电），随后重启引擎重放，断言：
//!
//! - **I1**：已 durable-ack 的操作效果全部可见（shadow model 含之）；
//! - **I2**：恢复后状态 == 按 LSN 顺序应用全部 durable 记录的终态；
//! - **I4（重放侧）**：统计与索引内容严格一致；
//! - **恢复稳定性**：恢复后续写单调，再次重启状态不变。
//!
//! 确定性来源：组提交配置 async_interval = 1 小时（工作负载期间无自动
//! fsync），durable 水位仅由 strict 写 / flush 屏障推进；同一 (seed,
//! crash_point, torn_len) 组合的盘面、重放与断言完全可复现。

use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write as IoWrite};
use std::path::Path;

use powerfs_core::wal::commit::CommitMode;
use powerfs_core::wal::engine::{EngineError, WalEngine, WalEngineConfig};
use powerfs_core::wal::frame::FRAME_HEADER_SIZE;
use powerfs_core::wal::segment::{parse_seg_id, seg_file_name};

/// SplitMix64：无外部依赖的确定性 RNG。
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_25ce_4d95);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// 随机操作。needle id 取自 1..=POOL 小池，强制产生覆写 / 删除 / 复活。
#[derive(Debug, Clone)]
enum Op {
    Write {
        needle: u64,
        data: Vec<u8>,
        strict: bool,
    },
    Delete {
        needle: u64,
        strict: bool,
    },
    FlushAll,
}

fn gen_ops(rng: &mut SplitMix64, count: usize) -> Vec<Op> {
    const POOL: u64 = 8;
    let mut ops = Vec::with_capacity(count);
    for i in 0..count {
        let roll = rng.below(100);
        let needle = 1 + rng.below(POOL);
        let len = 1 + rng.below(64) as usize;
        let data: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
        let strict = rng.below(100) < 30;
        if roll < 70 {
            ops.push(Op::Write {
                needle,
                data,
                strict,
            });
        } else if roll < 90 {
            // 前 2 个操作不生成 delete（池内 needle 尚未写入，只会命中
            // NotFound 空转）。
            if i < 2 {
                ops.push(Op::Write {
                    needle,
                    data,
                    strict,
                });
            } else {
                ops.push(Op::Delete { needle, strict });
            }
        } else {
            ops.push(Op::FlushAll);
        }
    }
    ops
}

/// 执行日志：每个操作落成的记录物理位置（失败操作记 None）与执行后的
/// durable 水位。
struct ExecLog {
    /// (lsn, seg_id, 帧结束偏移)。
    records: Vec<Option<(u64, u64, u64)>>,
    /// 执行完第 i 个操作后的 durable 水位（crash 注入点语义）。
    durable_after: Vec<u64>,
    /// 操作的 lsn（失败操作 None）。
    lsn_of: Vec<Option<u64>>,
}

fn no_autosync() -> WalEngineConfig {
    WalEngineConfig {
        // 小段（1KiB）：工作负载内自然触发多次换段，且避免大文件
        // fallocate/fsync 拖慢数千场景的回放。
        seg_size: 1024,
        preallocate: false,
        commit: powerfs_core::wal::commit::CommitConfig {
            async_interval: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
        // 停机不写 checkpoint：保持「crash = page cache 丢失」盘面语义，
        // 否则停机 ckpt 会把非 durable 操作带回恢复态（I1 违约假阳性）。
        ckpt_on_close: false,
        ..Default::default()
    }
}

fn run_workload(dir: &Path, ops: &[Op]) -> ExecLog {
    let eng = WalEngine::open(dir, no_autosync()).expect("engine open");
    let mut log = ExecLog {
        records: Vec::with_capacity(ops.len()),
        durable_after: Vec::with_capacity(ops.len()),
        lsn_of: Vec::with_capacity(ops.len()),
    };
    for op in ops {
        let rec = match op {
            Op::Write {
                needle,
                data,
                strict,
            } => {
                let mode = if *strict {
                    CommitMode::Strict
                } else {
                    CommitMode::Async
                };
                match eng.write(*needle, data, mode) {
                    Ok(r) => {
                        let end = r.placement.offset
                            + FRAME_HEADER_SIZE as u64
                            + r.placement.payload_len as u64;
                        Some((r.lsn, r.placement.seg_id, end))
                    }
                    Err(_) => None,
                }
            }
            Op::Delete { needle, strict } => {
                let mode = if *strict {
                    CommitMode::Strict
                } else {
                    CommitMode::Async
                };
                match eng.delete(*needle, mode) {
                    Ok(r) => {
                        let end = r.placement.offset
                            + FRAME_HEADER_SIZE as u64
                            + r.placement.payload_len as u64;
                        Some((r.lsn, r.placement.seg_id, end))
                    }
                    Err(_) => None,
                }
            }
            Op::FlushAll => {
                eng.flush_all().expect("flush_all");
                None
            }
        };
        log.lsn_of.push(rec.map(|(l, _, _)| l));
        log.records.push(rec);
        log.durable_after.push(eng.durable_lsn());
    }
    // 优雅关闭：最终 fsync（crash 注入在重开临时目录后进行，盘面随即
    // 被截断重建，这里无需关心非 durable 内容）。
    drop(eng);
    log
}

/// shadow model：按 LSN 顺序应用 lsn <= watermark 的操作终态。
/// delete 的存在性前提与引擎一致（目标在执行时点可见），因 LSN 单调，
/// 水位过滤不会出现 delete 先于其目标写生效的次序颠倒。
fn shadow_state(ops: &[Op], log: &ExecLog, watermark: u64) -> HashMap<u64, Vec<u8>> {
    let mut state = HashMap::new();
    for (i, op) in ops.iter().enumerate() {
        let Some(lsn) = log.lsn_of[i] else { continue };
        if lsn > watermark {
            continue;
        }
        match op {
            Op::Write { needle, data, .. } => {
                state.insert(*needle, data.clone());
            }
            Op::Delete { needle, .. } => {
                // 执行时引擎要求 needle 可见（active 或 tombstone）；shadow
                // 以 active 集为准，删除不存在者本就不会落记录（记 None）。
                state.remove(needle);
            }
            Op::FlushAll => {}
        }
    }
    state
}

/// crash 注入：把盘面重建为「lsn <= watermark 的记录 + 撕裂尾」。
/// 后续段删除、覆盖段截断到最后一 durable 帧尾，再追加 torn_len 个
/// 非零字节（全零会被识别为预分配 slack）。等价于崩溃时 page cache
/// 中未 fsync 内容全部丢失 + 正在写入的帧中途断电。
fn simulate_crash(dir: &Path, last: Option<(u64, u64)>, torn_len: usize, fill: u8) {
    let mut segs: Vec<u64> = std::fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|e| e.ok())
        .filter_map(|e| parse_seg_id(&e.file_name().to_string_lossy()))
        .collect();
    segs.sort_unstable();

    match last {
        None => {
            for s in segs {
                std::fs::remove_file(dir.join(seg_file_name(s))).expect("remove seg");
            }
        }
        Some((last_seg, end)) => {
            for s in segs {
                let path = dir.join(seg_file_name(s));
                if s > last_seg {
                    std::fs::remove_file(path).expect("remove future seg");
                } else if s == last_seg {
                    let mut f = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&path)
                        .expect("open seg");
                    f.set_len(end).expect("truncate to durable prefix");
                    if torn_len > 0 {
                        f.seek(SeekFrom::Start(end)).expect("seek");
                        f.write_all(&[fill; 32][..torn_len]).expect("torn tail");
                    }
                    // 逻辑 crash：随后由同进程重开重放，page cache 一致，
                    // 无需物理 fsync。
                }
            }
        }
    }
}

/// 重放断言：恢复态 == shadow(watermark)，统计一致，恢复后续写跨重启稳定。
fn assert_recovery(dir: &Path, ops: &[Op], log: &ExecLog, watermark: u64, tag: &str) {
    let shadow = shadow_state(ops, log, watermark);
    let eng = WalEngine::open(dir, no_autosync()).expect("reopen after crash");

    // I2 / I1：逐 needle 比对 shadow 终态。
    for (needle, data) in &shadow {
        let got = eng
            .read(*needle)
            .unwrap_or_else(|e| panic!("{tag}: needle {needle} expected visible: {e}"));
        assert_eq!(&got, data, "{tag}: needle {needle} data mismatch");
    }
    // 水位之外的 id 不得可见（池固定 1..=8）。
    for needle in 1..=8u64 {
        if !shadow.contains_key(&needle) {
            assert!(
                matches!(eng.read(needle), Err(EngineError::NotFound(_))),
                "{tag}: needle {needle} must be absent after crash"
            );
        }
    }

    // I4（重放侧）：统计与索引严格一致。
    let st = eng.stats();
    assert_eq!(
        st.index.active_count,
        shadow.len() as u64,
        "{tag}: active_count mismatch"
    );
    assert_eq!(
        st.index.used_bytes,
        shadow.values().map(|d| d.len() as u64).sum::<u64>(),
        "{tag}: used_bytes mismatch"
    );

    // LSN 接续：重放终态的 last_lsn == watermark。
    assert_eq!(eng.flushed_lsn(), watermark, "{tag}: flushed_lsn");

    // 恢复稳定性：续写 strict → 重启 → 可见。
    eng.write(9, b"post-crash-write", CommitMode::Strict)
        .expect("post-crash write");
    drop(eng);
    let eng2 = WalEngine::open(dir, no_autosync()).expect("second reopen");
    assert_eq!(eng2.read(9).unwrap(), b"post-crash-write", "{tag}");
    assert_eq!(eng2.read(9).unwrap(), b"post-crash-write", "{tag} twice");
}

/// 单种子完整回放：全部 crash 注入点 × 撕裂尾变体。
fn replay_seed(seed: u64, op_count: usize) -> usize {
    let mut rng = SplitMix64(seed);
    let ops = gen_ops(&mut rng, op_count);
    let mut scenarios = 0usize;

    for crash_point in 0..=ops.len() {
        for torn in [0usize, 7, 16] {
            let dir = tempfile::tempdir().expect("tempdir");
            let log = run_workload(dir.path(), &ops);

            // crash 注入点 = 执行完第 crash_point 个操作后的 durable 水位。
            let watermark = if crash_point == 0 {
                0
            } else {
                log.durable_after[crash_point - 1]
            };
            // 最后一 durable 帧位置：lsn <= watermark 的最大记录。
            let last = log
                .records
                .iter()
                .filter_map(|r| r.as_ref())
                .filter(|(l, _, _)| *l <= watermark)
                .max_by_key(|(l, _, _)| *l)
                .map(|(_, s, e)| (*s, *e));

            simulate_crash(dir.path(), last, torn, 0x5a);
            assert_recovery(
                dir.path(),
                &ops,
                &log,
                watermark,
                &format!("seed={seed} crash_at={crash_point} torn={torn}"),
            );
            scenarios += 1;
        }
    }
    scenarios
}

#[test]
fn deterministic_crash_matrix_i1_i2() {
    let seeds = 40u64;
    let op_count = 24usize;
    let workers = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(1);
    let seed_list: Vec<u64> = (0..seeds).collect();
    let chunks: Vec<Vec<u64>> = if workers <= 1 {
        vec![seed_list]
    } else {
        let per = seeds as usize / workers + 1;
        seed_list.chunks(per).map(<[u64]>::to_vec).collect()
    };
    let mut total = 0usize;
    // 种子间相互独立，确定性按种子成立（与调度无关），可并行回放。
    std::thread::scope(|s| {
        let handles: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                s.spawn(move || {
                    chunk
                        .iter()
                        .map(|&sd| replay_seed(sd, op_count))
                        .sum::<usize>()
                })
            })
            .collect();
        for h in handles {
            total += h.join().unwrap();
        }
    });
    // 验收线：数千 crash 注入组合全部通过 I1/I2 断言。
    assert!(
        total >= 2000,
        "expected >=2000 crash scenarios, ran {total}"
    );
}

/// 种子回放可复现：同一种子两次运行的 crash 矩阵结果一致（确定性）。
#[test]
fn seed_replay_is_deterministic() {
    assert_eq!(replay_seed(777, 12), replay_seed(777, 12));
}
