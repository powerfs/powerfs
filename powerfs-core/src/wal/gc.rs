//! 段 GC 与 tombstone purge（方案 §8 / 附录 C T6）。
//!
//! GC 周期扫描 sealed 段，按段级空间分桶（`per_seg_bytes`：live/staging/
//! dead）分类处置：
//!
//! - **case A 整段删除**：段内无活跃 needle、无未过期 tombstone（live==0
//!   且 staging==0）。段内记录全部失效（死副本/过期 tombstone/无主搬移
//!   记录/空 slack），unlink 段文件 + fsync 目录项 + 清单移除 + 死副本
//!   账本同步清除（内存统计与恢复重放终态一致，I4）。
//! - **case B 搬移**：dead/live > `ratio` 且 dead ≥ `min_bytes`。把段内
//!   存活 needle 逐一以 GC_MIGRATE 记录重写入活跃段（走组提交，不停写），
//!   限速 `max_bytes_per_sec`；搬移记录 durable（flush 屏障）后原段变全
//!   死 → case A。并发覆写安全：搬移记录自包含 `expect_version`，条目
//!   已被并发更新时条件应用跳过（用户更新胜出，见
//!   [`crate::wal::frame::GcMigratePayload`]）。
//! - **case C tombstone 过期 purge**：每个过期 tombstone 先向活跃段写一条
//!   持久化 TOMB_PURGE 标记（durable），再把物理副本迁入死副本账本
//!   （staging → garbage）。标记防止「DELETE 所在段先于旧数据段回收」后
//!   重放复活已删 needle（重放时压制同 needle 更早的 DATA/GC_MIGRATE
//!   孤儿），并经 checkpoint 持久化；所有更老段回收后由 GC 修剪。
//!
//! 回收语义：删除段即物理移除其全部记录。P2 无快照（无版本钉住），段内
//! 所有被索引引用的条目为空即可安全删除；删除后重放不再见到这些记录，
//! 终态与在线一致（记录序列终态的自洽性，I1/I2 不受影响）。
//!
//! 并发与锁序：GC 与写路径/ckpt 调度并发。锁序遵循全局约定
//! 「manifest → index」（嵌套获取不得逆序）；搬移的读数据（IO）在锁外
//! 执行，条件写入经 [`EngineCore::migrate_write`]（组提交 + 在线索引）。

use std::io::{Read as _, Seek, SeekFrom};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use super::engine::{fsync_dir, EngineCore, EngineError};
use crate::wal::checkpoint::per_seg_bytes;
use crate::wal::frame::{compute_frame_crc, RT_DATA, RT_GC_MIGRATE};
use crate::wal::index::{NeedleEntry, NEEDLE_FLAG_MIGRATED};
use crate::wal::manifest::SegmentState;
use crate::wal::segment::seg_file_name;

/// GC 配置（§8 参数）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GcConfig {
    /// 后台扫描周期；None 关闭后台 GC（仅 `WalEngine::gc` 手动触发）。
    pub interval: Option<Duration>,
    /// 搬移阈值：dead/live > ratio（§8 默认 0.3）。
    pub ratio: f64,
    /// 搬移阈值：dead ≥ min_bytes（§8 默认 32MiB）。
    pub min_bytes: u64,
    /// 搬移限速（字节/秒，§8 默认 100MiB/s；0 = 不限速）。
    pub max_bytes_per_sec: u64,
}

impl Default for GcConfig {
    fn default() -> Self {
        GcConfig {
            interval: Some(Duration::from_secs(30)),
            ratio: 0.3,
            min_bytes: 32 << 20,
            max_bytes_per_sec: 100 << 20,
        }
    }
}

/// 一轮 GC 的统计。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcOutcome {
    /// case C：purge 的过期 tombstone 数。
    pub purged: usize,
    /// case A：整段删除的段数。
    pub segments_deleted: usize,
    /// case B：搬移的 needle 数。
    pub migrated_needles: usize,
    /// case B：搬移的数据字节。
    pub migrated_bytes: u64,
    /// case A：整段回收释放的物理字节（死副本账本清账量）。
    pub reclaimed_bytes: u64,
}

/// 执行一轮 GC（case C → 候选扫描 → case B 搬移 → case A 回收）。
pub(super) fn run_gc_cycle(core: &EngineCore, cfg: &GcConfig) -> Result<GcOutcome, EngineError> {
    let mut out = GcOutcome::default();
    let now = chrono::Utc::now().timestamp();

    // ---- 清理失效 purge 标记：最老存活段 base_lsn 之后的标记不再需要
    // （任何可能被它压制的孤儿记录所在段都已回收）。----
    prune_purge_markers(core);

    // ---- case C：过期 tombstone purge。每个 purge 先持久化 TOMB_PURGE
    // 标记（durable）再搬账本（staging → garbage），保证 DELETE 物理副本
    // 随段回收后，重放仍能压制孤儿 DATA/GC_MIGRATE，needle 不复活。----
    let expired: Vec<(u64, u64)> = {
        let idx = core.index.read().unwrap();
        idx.expired_tombstones(now)
            .map(|tb| (tb.needle_id, tb.version_lsn))
            .collect()
    };
    for (needle_id, delete_lsn) in expired {
        core.purge_write(needle_id, delete_lsn)?;
        out.purged += 1;
    }

    // ---- 候选扫描（锁序 manifest → index）----
    let candidates: Vec<u64> = {
        let manifest = core.sink.manifest.lock().unwrap();
        let idx = core.index.read().unwrap();
        let buckets = per_seg_bytes(&idx);
        let mut cands = Vec::new();
        for e in manifest.segments() {
            if e.state == SegmentState::Active {
                continue; // 活跃段不回收（搬移目标，写入中）
            }
            let (live, staging, dead) = buckets.get(&e.seg_id).copied().unwrap_or((0, 0, 0));
            // case A：无活跃、无未过期 tombstone（dead 可为任意值）；
            // case B：搬移阈值满足（live > 0，未过期 tombstone 不阻塞
            // 搬移——搬移只处理活跃 needle）。
            if (live == 0 && staging == 0)
                || (live > 0 && dead as f64 / live as f64 > cfg.ratio && dead >= cfg.min_bytes)
            {
                cands.push(e.seg_id);
            }
        }
        cands
    };

    for seg_id in candidates {
        // 搬移（case B）：处理完成或条件已失效后统一尝试整段回收。
        migrate_segment(core, seg_id, cfg, &mut out)?;
        delete_if_dead(core, seg_id, &mut out)?;
    }

    Ok(out)
}

/// 清理失效 purge 标记（锁序 manifest → index）。
///
/// 标记 `purge_lsn = P` 用于压制存活段中 lsn ≤ P 的同 needle 孤儿记录。
/// 段按 lsn 区间单调排列：当现存最老段的 base_lsn > P 时，物理上不再可
/// 能存在 lsn ≤ P 的记录，标记失去作用，从内存与后续 checkpoint 移除。
fn prune_purge_markers(core: &EngineCore) {
    let min_base_lsn = {
        let manifest = core.sink.manifest.lock().unwrap();
        manifest.segments().iter().map(|e| e.base_lsn).min()
    };
    if let Some(min_base_lsn) = min_base_lsn {
        core.index
            .write()
            .unwrap()
            .prune_purge_markers(min_base_lsn);
    }
}

/// 搬移一个 sealed 段的全部存活 needle（case B）。
///
/// 每条搬移前按决策时条目（version_lsn）重写入活跃段；条件应用保证并发
/// 覆写下不丢用户更新。全部搬移记录经 flush 屏障 durable 后，原段变全死。
fn migrate_segment(
    core: &EngineCore,
    seg_id: u64,
    cfg: &GcConfig,
    out: &mut GcOutcome,
) -> Result<(), EngineError> {
    // 段内存活 needle 决策快照（needle_id 升序）。条目可能指向先前搬移
    // 写入的 GC_MIGRATE 帧（flags 标记），读数据时按对应帧形态解析；
    // version_lsn 作为条件应用的 expect_version。空段直接返回。
    let needles: Vec<NeedleEntry> = {
        let idx = core.index.read().unwrap();
        let mut v: Vec<_> = idx
            .needles()
            .filter(|n| n.seg_id == seg_id)
            .cloned()
            .collect();
        v.sort_unstable_by_key(|n| n.needle_id);
        v
    };
    if needles.is_empty() {
        return Ok(());
    }

    let mut max_lsn = 0u64;
    for entry in needles {
        // 读原段数据（锁外 IO）：按决策时条目直接 pread + 帧 CRC 校验。
        // 读失败（段被并发处置/损坏）中止本轮搬移，已搬移部分有效。
        let data = read_data_at(core, &entry)?;

        // 条件写入：版本已被并发覆写/删除时 apply 返回 false（记录成为
        // 无主副本），原段该 needle 已无活跃引用，不影响回收判定。
        let (receipt, applied) = core.migrate_write(entry.needle_id, entry.version_lsn, &data)?;
        max_lsn = max_lsn.max(receipt.lsn);

        if applied {
            out.migrated_needles += 1;
            out.migrated_bytes += data.len() as u64;
        }

        if cfg.max_bytes_per_sec > 0 {
            let dur = Duration::from_secs_f64(data.len() as f64 / cfg.max_bytes_per_sec as f64);
            if !dur.is_zero() {
                std::thread::sleep(dur);
            }
        }
    }

    // 屏障：搬移记录全部 durable 后原段方可安全删除（删除由调用方在
    // delete_if_dead 中执行，其判定基于 durable 后的索引状态）。
    core.commit.flush_barrier(max_lsn)?;
    Ok(())
}

/// case A：段全死（无活跃/无未过期 tombstone）时整段回收。
fn delete_if_dead(core: &EngineCore, seg_id: u64, out: &mut GcOutcome) -> Result<(), EngineError> {
    // 判定（锁序 manifest → index）：段仍存在且 sealed、无活跃、无未过期
    // tombstone 才回收。
    let deletable = {
        let manifest = core.sink.manifest.lock().unwrap();
        let idx = core.index.read().unwrap();
        match manifest.segments().iter().find(|e| e.seg_id == seg_id) {
            Some(e) if e.state != SegmentState::Active => {
                let (live, staging, _) = per_seg_bytes(&idx)
                    .get(&seg_id)
                    .copied()
                    .unwrap_or((0, 0, 0));
                live == 0 && staging == 0
            }
            _ => false,
        }
    };
    if !deletable {
        return Ok(());
    }

    // 回收：unlink → fsync 目录项 → 清单移除 + 死副本清账（锁序
    // manifest → index）。unlink 成功后清单移除必然跟随；中间崩溃时
    // 重启扫描会重新发现该段（内容全死、重放无害），下轮 GC 再回收。
    let path = core.dir.join(seg_file_name(seg_id));
    std::fs::remove_file(&path).map_err(EngineError::Io)?;
    fsync_dir(&core.dir)?;

    let reclaimed = {
        let mut manifest = core.sink.manifest.lock().unwrap();
        let mut idx = core.index.write().unwrap();
        let reclaimed = idx.remove_seg_garbage(seg_id);
        manifest.remove(seg_id);
        reclaimed
    };
    out.segments_deleted += 1;
    out.reclaimed_bytes += reclaimed;
    Ok(())
}

/// 按决策时条目直接 pread 原段数据 + 帧 CRC 校验（与读路径同校验强度）。
///
/// 条目 `flags` 携带 `NEEDLE_FLAG_MIGRATED` 时按 GC_MIGRATE 帧形态解析
/// （payload = `needle_id|expect_version|data_len|data`，20B 头，CRC 按
/// RT_GC_MIGRATE 重算），否则按 DATA 帧形态（12B 头）。
fn read_data_at(core: &EngineCore, entry: &NeedleEntry) -> Result<Vec<u8>, EngineError> {
    let path = core.dir.join(seg_file_name(entry.seg_id));
    let mut f = std::fs::File::open(&path).map_err(EngineError::Io)?;
    f.seek(SeekFrom::Start(entry.offset))
        .map_err(EngineError::Io)?;

    let migrated = entry.flags & NEEDLE_FLAG_MIGRATED != 0;
    let header_len = if migrated { 20usize } else { 12usize };
    let rt = if migrated { RT_GC_MIGRATE } else { RT_DATA };

    let mut payload = vec![0u8; header_len + entry.data_len as usize];
    f.read_exact(&mut payload).map_err(EngineError::Io)?;

    if core.config.verify_on_read {
        let got = compute_frame_crc(rt, 0, entry.version_lsn, &payload);
        if got != entry.crc {
            return Err(EngineError::ReadCorrupt {
                needle_id: entry.needle_id,
            });
        }
    }
    Ok(payload[header_len..].to_vec())
}

/// GC 后台线程（§8）：按 `interval` 轮询执行 [`run_gc_cycle`]。失败仅
/// 告警，不影响在线路径；停止标志与 ckpt 调度共用 `core.stop`。
pub(super) fn spawn_gc_worker(core: Arc<EngineCore>) -> std::thread::JoinHandle<()> {
    let interval = core.config.gc.interval.unwrap_or(Duration::from_secs(30));
    let poll = interval
        .min(Duration::from_secs(1))
        .max(Duration::from_millis(10));
    std::thread::Builder::new()
        .name("wal-gc".into())
        .spawn(move || {
            while !core.stop.load(Ordering::SeqCst) {
                std::thread::sleep(poll);
                if core.stop.load(Ordering::SeqCst) {
                    return;
                }
                if let Err(e) = run_gc_cycle(&core, &core.config.gc) {
                    log::warn!("wal: background gc cycle failed: {e}");
                }
            }
        })
        .expect("spawn wal gc worker")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::commit::CommitMode;
    use crate::wal::engine::{WalEngine, WalEngineConfig};
    use crate::wal::manifest::SegManifest;
    use std::path::Path;

    /// 小段 + 关闭后台调度 + 短 tombstone 保留期：GC 全部手动触发。
    fn gc_cfg(seg_size: u64) -> WalEngineConfig {
        WalEngineConfig {
            seg_size,
            preallocate: false,
            ckpt_interval: None,
            ckpt_on_close: false,
            gc: GcConfig {
                interval: None,
                ratio: 0.3,
                min_bytes: 0, // case B 阈值放开：任何 dead > live*0.3 即搬移
                max_bytes_per_sec: 0,
            },
            tombstone_retention_secs: 0, // tombstone 立即可 purge
            ..Default::default()
        }
    }

    fn seg_ids(dir: &Path) -> Vec<u64> {
        let m = SegManifest::load(dir, 1 << 20).unwrap();
        m.segments().iter().map(|s| s.seg_id).collect()
    }

    /// case A：覆写推进新段后旧段全死 → GC 整段回收、清单收缩、
    /// 读与统计一致、重开恢复一致。
    #[test]
    fn case_a_empty_segment_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 512u64;
        let eng = WalEngine::open(dir.path(), gc_cfg(seg_size)).unwrap();

        // 写满两个段（5 × 100B → seg1: w1..w3，seg2: w4..w5）。
        for i in 1..=5u64 {
            eng.write(i, &[0xa0 + i as u8; 100], CommitMode::Strict)
                .unwrap();
        }

        // 全部覆写到新段（seg3: w1'..w3'，seg4: w4'..w5'），旧段全死。
        for i in 1..=5u64 {
            eng.write(i, &[0xf0; 100], CommitMode::Strict).unwrap();
        }
        let segs_before = seg_ids(dir.path());
        assert_eq!(segs_before, vec![1, 2, 3, 4], "{segs_before:?}");

        let out = eng.gc().unwrap();
        // 布局：seg1=w1,w2,w3（全死）；seg2=w4,w5（死）+w1'（活，覆写
        // 时 seg2 尚有余量落入）→ case B 搬移 w1' 后整段回收；seg3/seg4
        // 全为最新版本不回收。
        assert_eq!(out.segments_deleted, 2, "{out:?}");
        assert_eq!(out.migrated_needles, 1, "{out:?}");
        assert_eq!(out.migrated_bytes, 100, "{out:?}");
        assert!(out.reclaimed_bytes > 0);

        // 段清单收缩（旧段消失，新段保留）。
        let segs_after = seg_ids(dir.path());
        assert_eq!(segs_after, vec![3, 4], "{segs_after:?}");

        // 读一致：数据是最新版本。
        for i in 1..=5u64 {
            assert_eq!(eng.read(i).unwrap(), vec![0xf0; 100], "needle {i}");
        }
        eng.assert_index_consistent();

        // 重开恢复一致：旧 DATA 记录随段物理删除，最新版本段可独立重放
        // 建立全部条目（缺席应用语义）。
        drop(eng);
        let eng2 = WalEngine::open(dir.path(), gc_cfg(seg_size)).unwrap();
        for i in 1..=5u64 {
            assert_eq!(eng2.read(i).unwrap(), vec![0xf0; 100]);
        }
        eng2.assert_index_consistent();
        drop(eng2);
    }

    /// case B：dead/live 超阈值 → 搬移 → 原段删除，数据不变。
    #[test]
    fn case_b_migrates_live_needles_and_reclaims() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 512u64;
        let eng = WalEngine::open(dir.path(), gc_cfg(seg_size)).unwrap();

        for i in 1..=5u64 {
            eng.write(i, &[0x11 + i as u8; 100], CommitMode::Strict)
                .unwrap();
        }
        let segs_before = seg_ids(dir.path());
        assert!(segs_before.len() >= 2);

        // 仅覆写 4、5（新段）；1/2/3 留在旧段（live），4/5 旧副本成死副本。
        eng.write(4, &[0x99; 100], CommitMode::Strict).unwrap();
        eng.write(5, &[0x99; 100], CommitMode::Strict).unwrap();

        // 旧段布局：seg2 live=100（w4'）、dead=200（w4/w5 原始副本）
        // → dead/live=2 > 0.3 → 搬移 needle4 后整段回收；seg1 全 live 不动。
        let out = eng.gc().unwrap();
        assert_eq!(out.migrated_needles, 1, "{out:?}");
        assert_eq!(out.migrated_bytes, 100);
        assert_eq!(out.segments_deleted, 1, "{out:?}");

        // 数据不变：三个版本读回原值。
        assert_eq!(eng.read(1).unwrap(), vec![0x12; 100]);
        assert_eq!(eng.read(2).unwrap(), vec![0x13; 100]);
        assert_eq!(eng.read(3).unwrap(), vec![0x14; 100]);
        assert_eq!(eng.read(4).unwrap(), vec![0x99; 100]);
        assert_eq!(eng.read(5).unwrap(), vec![0x99; 100]);

        drop(eng);
        let eng2 = WalEngine::open(dir.path(), gc_cfg(seg_size)).unwrap();
        assert_eq!(eng2.read(1).unwrap(), vec![0x12; 100]);
        assert_eq!(eng2.read(3).unwrap(), vec![0x14; 100]);
        assert_eq!(eng2.read(4).unwrap(), vec![0x99; 100]);
        drop(eng2);
    }

    /// case B 边界：dead/live ≤ ratio 时不动。
    #[test]
    fn case_b_threshold_boundary_respected() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = gc_cfg(512);
        cfg.gc.ratio = 0.99; // 阈值极高
        let eng = WalEngine::open(dir.path(), cfg).unwrap();

        for i in 1..=5u64 {
            eng.write(i, &[0x21; 100], CommitMode::Strict).unwrap();
        }
        eng.write(1, &[0x22; 100], CommitMode::Strict).unwrap();

        let out = eng.gc().unwrap();
        assert_eq!(out.migrated_needles, 0, "{out:?}");
        assert_eq!(out.segments_deleted, 0, "{out:?}");
        // 旧段未被搬移/回收，数据原地可读。
        assert_eq!(eng.read(1).unwrap(), vec![0x22; 100]);
        assert_eq!(eng.read(2).unwrap(), vec![0x21; 100]);
    }

    /// case C：过期 tombstone purge（保留期 0 → 立即过期），所在段
    /// 全死后整段回收；保留期内（长保留期）不被 purge。
    #[test]
    fn case_c_purge_expired_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 512u64;
        let eng = WalEngine::open(dir.path(), gc_cfg(seg_size)).unwrap();
        for i in 1..=3u64 {
            eng.write(i, &[0x31; 100], CommitMode::Strict).unwrap();
        }
        eng.delete(1, CommitMode::Strict).unwrap();
        eng.delete(2, CommitMode::Strict).unwrap();
        let st_mid = eng.stats().index;
        assert_eq!(st_mid.deleted_count, 2);

        let out = eng.gc().unwrap();
        assert_eq!(out.purged, 2, "{out:?}");

        // purge 后 needle 1/2 不可恢复（tombstone 已清除 → NotFound）。
        assert!(matches!(
            eng.read(1),
            Err(crate::wal::engine::EngineError::NotFound(1))
        ));
        assert!(matches!(
            eng.read(2),
            Err(crate::wal::engine::EngineError::NotFound(2))
        ));
        assert_eq!(eng.read(3).unwrap(), vec![0x31; 100]);
        assert_eq!(eng.stats().index.deleted_count, 0);
        assert_eq!(eng.stats().index.staging_bytes, 0);

        // tombstone 物理副本所在的段（若 sealed）被整段回收。
        drop(eng);
        let eng2 = WalEngine::open(dir.path(), gc_cfg(seg_size)).unwrap();
        assert!(matches!(
            eng2.read(1),
            Err(crate::wal::engine::EngineError::NotFound(1))
        ));
        assert_eq!(eng2.read(3).unwrap(), vec![0x31; 100]);
        drop(eng2);
    }

    /// purge 标记经 checkpoint 持久化：purge + 段回收 + checkpoint 后重开，
    /// 已删 needle 仍为 NotFound（孤儿副本不复活），存活 needle 可读。
    #[test]
    fn purge_marker_survives_checkpoint_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 512u64;
        let eng = WalEngine::open(dir.path(), gc_cfg(seg_size)).unwrap();
        for i in 1..=4u64 {
            eng.write(i, &[0x71; 100], CommitMode::Strict).unwrap();
        }
        eng.delete(1, CommitMode::Strict).unwrap();
        eng.delete(2, CommitMode::Strict).unwrap();
        assert_eq!(eng.gc().unwrap().purged, 2);
        // 再滚多轮写入制造新段后再 GC，使 purge 标记所在记录早于新活动段。
        for i in 10..=16u64 {
            eng.write(i, &[0x72; 100], CommitMode::Strict).unwrap();
        }
        eng.gc().unwrap();
        eng.checkpoint().unwrap();
        eng.assert_index_consistent();
        drop(eng);

        let eng2 = WalEngine::open(dir.path(), gc_cfg(seg_size)).unwrap();
        assert!(matches!(
            eng2.read(1),
            Err(crate::wal::engine::EngineError::NotFound(1))
        ));
        assert!(matches!(
            eng2.read(2),
            Err(crate::wal::engine::EngineError::NotFound(2))
        ));
        assert_eq!(eng2.read(3).unwrap(), vec![0x71; 100]);
        assert_eq!(eng2.read(14).unwrap(), vec![0x72; 100]);
        eng2.assert_index_consistent();
        drop(eng2);
    }

    /// 保留期内 tombstone 不被 purge，restore 语义保持。
    #[test]
    fn tombstone_within_retention_survives_gc() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = gc_cfg(512);
        cfg.tombstone_retention_secs = 7 * 24 * 3600;
        let eng = WalEngine::open(dir.path(), cfg).unwrap();

        eng.write(1, &[0x41; 100], CommitMode::Strict).unwrap();
        eng.delete(1, CommitMode::Strict).unwrap();

        let out = eng.gc().unwrap();
        assert_eq!(out.purged, 0);
        assert_eq!(eng.stats().index.staging_bytes, 100);
        drop(eng);
    }

    /// 搬移限速生效：限速 2KiB/s 搬移 100B 数据耗时 ≥40ms。
    #[test]
    fn migration_rate_limit_throttles() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = gc_cfg(512);
        cfg.gc.max_bytes_per_sec = 2048;
        let eng = WalEngine::open(dir.path(), cfg).unwrap();

        for i in 1..=8u64 {
            eng.write(i, &[0x51; 100], CommitMode::Strict).unwrap();
        }
        for i in 1..=5u64 {
            eng.write(i, &[0x52; 100], CommitMode::Strict).unwrap();
        }

        let start = std::time::Instant::now();
        let out = eng.gc().unwrap();
        let elapsed = start.elapsed();
        // 布局：seg2（w4/w5 dead + w6 live）触发搬移 1 条 needle6。
        assert_eq!(out.migrated_needles, 1, "{out:?}");
        assert!(
            elapsed >= Duration::from_millis(40),
            "限速未生效: {elapsed:?}"
        );
    }

    /// GC 后统计自洽（I4）：索引统计与死副本账本逐字节核对。
    #[test]
    fn index_consistent_after_gc() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 512u64;
        let eng = WalEngine::open(dir.path(), gc_cfg(seg_size)).unwrap();

        for i in 1..=6u64 {
            eng.write(i, &[0x61; 100], CommitMode::Strict).unwrap();
        }
        eng.write(1, &[0x62; 60], CommitMode::Strict).unwrap();
        eng.write(2, &[0x62; 80], CommitMode::Strict).unwrap();
        eng.delete(3, CommitMode::Strict).unwrap();
        eng.gc().unwrap();
        eng.assert_index_consistent();
        drop(eng);
    }
}
