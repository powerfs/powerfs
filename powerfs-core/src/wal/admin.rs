//! WAL 卷本地面管理命令（方案 §19.2 / §19.3）。
//!
//! 只读命令（stats/verify/list-needles）不取 flock、只读打开：扫描段
//! 清单 + 全量重放（与运行中的 server 并发安全）。读到的可能是略旧的
//! 一致快照；活跃段尾部正被追加时按 tolerate_tail 语义截断撕裂尾
//! （`truncated` 报告截断位置）。
//!
//! 写命令（gc/checkpoint/resize）正常取引擎 flock：server 运行中取锁
//! 失败 → [`AdminError::Busy`]，调用方按 EBUSY 引导走远程面（CLI 经
//! Master）或停服后重试。
//!
//! 命令映射：
//! - `stats`  → [`inspect`]：段清单 + 索引统计 + LSN 概览；
//! - `verify` → [`verify`]：全量重放（帧 CRC + 哈希链逐帧校验）并报告
//!   活跃 needle / tombstone / 死副本计数。deep 语义即重放本身——每条
//!   记录的 payload 都被读取并校验。
//! - `list-needles` → [`list_needles`]：活跃 needle 索引枚举。
//! - `gc`/`checkpoint`/`resize` → 同名函数：离线写操作（取锁）。

use std::path::{Path, PathBuf};

use crate::wal::engine::{EngineError, WalEngine, WalEngineConfig};
use crate::wal::gc::GcConfig;
use crate::wal::manifest::SegManifest;
use crate::wal::replay::replay_all;

/// 本地面写命令错误：Busy = flock 被运行中的 server 持有（EBUSY 语义）。
#[derive(Debug)]
pub enum AdminError {
    /// volume 正被运行中的 server 独占打开。
    Busy(PathBuf),
    /// 命令执行失败（打开/重放/写入错误）。
    Failed(String),
}

impl std::fmt::Display for AdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdminError::Busy(p) => write!(
                f,
                "volume busy (EBUSY): {} is held by a running volume server; \
                 use the remote face (powerfs-cli via master) or stop the server first",
                p.display()
            ),
            AdminError::Failed(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for AdminError {}

impl From<EngineError> for AdminError {
    fn from(e: EngineError) -> Self {
        match e {
            EngineError::Locked(p) => AdminError::Busy(p),
            other => AdminError::Failed(other.to_string()),
        }
    }
}

/// 离线管理用引擎配置：关闭一切后台调度（动作只由显式命令触发）、
/// 停机不额外 checkpoint、不 fallocate（维护窗口最小写放大）。
fn admin_engine_config(seg_size: u64, tombstone_retention_secs: Option<i64>) -> WalEngineConfig {
    WalEngineConfig {
        seg_size,
        preallocate: false,
        ckpt_interval: None,
        ckpt_on_close: false,
        tombstone_retention_secs: tombstone_retention_secs
            .unwrap_or(crate::wal::engine::DEFAULT_TOMBSTONE_RETENTION_SECS),
        gc: GcConfig {
            interval: None,
            ..GcConfig::default()
        },
        ..Default::default()
    }
}

/// inspect/verify 的统一报告。
#[derive(Debug, Clone)]
pub struct WalInspectReport {
    pub volume_id: u64,
    pub segments: Vec<(u64, String, u64)>, // (seg_id, state, base_lsn)
    pub active_seg_id: Option<u64>,
    pub active_count: u64,
    pub deleted_count: u64,
    pub used_bytes: u64,
    pub staging_bytes: u64,
    pub garbage_bytes: u64,
    pub pinned_bytes: u64,
    /// 卷逻辑容量（0 = 未限定）。
    pub volume_size: u64,
    /// 逻辑可用（未限定时为 u64::MAX）。
    pub free_bytes: u64,
    pub dead_copies: usize,
    pub last_lsn: u64,
    pub frames_replayed: u64,
    /// (seg_id, 截断到的偏移)——非空表示活跃段尾部存在撕裂帧。
    pub truncated: Vec<(u64, u64)>,
}

/// list-needles 的单行（活跃 needle；保留期内 tombstone 不列出）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalNeedleRow {
    pub needle_id: u64,
    pub seg_id: u64,
    pub offset: u64,
    pub data_len: u32,
    pub version_lsn: u64,
    pub crc: u32,
}

fn inspect_impl(dir: &Path, seg_size: u64) -> Result<WalInspectReport, String> {
    let manifest = SegManifest::load(dir, seg_size).map_err(|e| e.to_string())?;
    // 只读检查全量重放（不装载 checkpoint：与 server 并发时 ckpt 可能
    // 正被写出；全量重放只依赖不可变的 sealed 段 + TolerateTail）。
    let replay = replay_all(
        dir,
        &manifest,
        crate::wal::index::WalIndex::new(),
        crate::wal::replay::RecoveryMode::TolerateTail,
        0,
    )
    .map_err(|e| e.to_string())?;

    let segments = manifest
        .segments()
        .iter()
        .map(|s| (s.seg_id, format!("{:?}", s.state), s.base_lsn))
        .collect();
    let truncated: Vec<(u64, u64)> = replay.truncated.iter().map(|(k, v)| (*k, *v)).collect();
    let st = replay.index.stats();
    let volume_size = replay.index.volume_size();
    let free_bytes = match volume_size {
        0 => u64::MAX,
        cap => cap.saturating_sub(st.used_bytes + st.staging_bytes + st.pinned_bytes),
    };

    Ok(WalInspectReport {
        volume_id: manifest
            .segments()
            .first()
            .map(|s| s.volume_id)
            .unwrap_or(0),
        segments,
        active_seg_id: manifest.active().map(|s| s.seg_id),
        active_count: st.active_count,
        deleted_count: st.deleted_count,
        used_bytes: st.used_bytes,
        staging_bytes: st.staging_bytes,
        garbage_bytes: st.garbage_bytes,
        pinned_bytes: st.pinned_bytes,
        volume_size,
        free_bytes,
        dead_copies: replay.index.dead_copies().len(),
        last_lsn: replay.index.last_lsn(),
        frames_replayed: replay.frames_replayed,
        truncated,
    })
}

/// 只读统计：段清单 + 索引统计 + LSN 概览。
pub fn inspect(dir: &Path, seg_size: u64) -> Result<WalInspectReport, String> {
    inspect_impl(dir, seg_size)
}

/// 只读校验：全量重放（逐帧 CRC + 哈希链），重放成功即校验通过。
/// 返回与 inspect 相同的报告（counts 即校验结果的量化）。
pub fn verify(dir: &Path, seg_size: u64) -> Result<WalInspectReport, String> {
    inspect_impl(dir, seg_size)
}

/// 只读枚举活跃 needle（不取锁；全量重放构建索引快照）。
///
/// - `min_id`：仅返回 needle_id ≥ min_id 的行（`--prefix N`，默认 0）；
/// - `limit`：最多返回行数（None = 不限）。
///
/// 结果按 needle_id 升序。
pub fn list_needles(
    dir: &Path,
    seg_size: u64,
    min_id: u64,
    limit: Option<usize>,
) -> Result<Vec<WalNeedleRow>, String> {
    let manifest = SegManifest::load(dir, seg_size).map_err(|e| e.to_string())?;
    let replay = replay_all(
        dir,
        &manifest,
        crate::wal::index::WalIndex::new(),
        crate::wal::replay::RecoveryMode::TolerateTail,
        0,
    )
    .map_err(|e| e.to_string())?;
    let mut rows: Vec<WalNeedleRow> = replay
        .index
        .needles()
        .filter(|e| e.needle_id >= min_id)
        .map(|e| WalNeedleRow {
            needle_id: e.needle_id,
            seg_id: e.seg_id,
            offset: e.offset,
            data_len: e.data_len,
            version_lsn: e.version_lsn,
            crc: e.crc,
        })
        .collect();
    rows.sort_unstable_by_key(|r| r.needle_id);
    if let Some(n) = limit {
        rows.truncate(n);
    }
    Ok(rows)
}

/// 离线触发一轮 GC（§8；取 flock；server 运行中返回 [`AdminError::Busy`]）。
///
/// `tombstone_retention_secs = None` 使用默认保留期；维护窗口可传
/// `Some(0)` 立即 purge 全部过期 tombstone。
pub fn run_gc(
    dir: &Path,
    seg_size: u64,
    tombstone_retention_secs: Option<i64>,
) -> Result<crate::wal::gc::GcOutcome, AdminError> {
    let eng = WalEngine::open(dir, admin_engine_config(seg_size, tombstone_retention_secs))?;
    Ok(eng.gc()?)
}

/// 离线触发一次 checkpoint（§7；取 flock）。
pub fn run_checkpoint(
    dir: &Path,
    seg_size: u64,
) -> Result<crate::wal::engine::CkptOutcome, AdminError> {
    let eng = WalEngine::open(dir, admin_engine_config(seg_size, None))?;
    Ok(eng.checkpoint()?)
}

/// 离线容量伸缩（§11.1；取 flock；VOLUME_META strict 落盘并轮换
/// superblock 后返回）。shrink 低于占用返回 Failed（ResizeTooSmall）。
pub fn run_resize(dir: &Path, seg_size: u64, new_size: u64) -> Result<u64, AdminError> {
    let eng = WalEngine::open(dir, admin_engine_config(seg_size, None))?;
    let receipt = eng.resize(new_size)?;
    Ok(receipt.lsn)
}

/// list-needles 渲染为人类可读文本。
pub fn format_needles(rows: &[WalNeedleRow]) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{:<20} {:<20} {:>12} {:>10} {:>12}\n",
        "needle_id", "segment", "offset", "data_len", "version_lsn"
    ));
    for r in rows {
        out.push_str(&format!(
            "{:<20} seg_{:016x} {:>12} {:>10} {:>12}\n",
            r.needle_id, r.seg_id, r.offset, r.data_len, r.version_lsn
        ));
    }
    out.push_str(&format!("{} needle(s)\n", rows.len()));
    out
}

/// 渲染报告为人类可读文本（admin 命令 stdout）。
pub fn format_report(r: &WalInspectReport) -> String {
    let mut out = String::new();
    out.push_str(&format!("volume_id:      {}\n", r.volume_id));
    out.push_str(&format!("segments:       {}\n", r.segments.len()));
    for (seg_id, state, base_lsn) in &r.segments {
        out.push_str(&format!(
            "  seg_{:016x}  {}  base_lsn={}\n",
            seg_id, state, base_lsn
        ));
    }
    out.push_str(&format!(
        "active_seg:     {}\n",
        r.active_seg_id
            .map(|s| format!("seg_{:016x}", s))
            .unwrap_or_else(|| "-".into())
    ));
    out.push_str(&format!("active_needles: {}\n", r.active_count));
    out.push_str(&format!("tombstones:     {}\n", r.deleted_count));
    out.push_str(&format!("used_bytes:     {}\n", r.used_bytes));
    out.push_str(&format!("staging_bytes:  {}\n", r.staging_bytes));
    out.push_str(&format!("garbage_bytes:  {}\n", r.garbage_bytes));
    out.push_str(&format!("pinned_bytes:   {}\n", r.pinned_bytes));
    if r.volume_size == 0 {
        out.push_str("volume_size:    unlimited (0)\n");
        out.push_str("free_bytes:     unlimited\n");
    } else {
        out.push_str(&format!("volume_size:    {}\n", r.volume_size));
        out.push_str(&format!("free_bytes:     {}\n", r.free_bytes));
    }
    out.push_str(&format!("dead_copies:    {}\n", r.dead_copies));
    out.push_str(&format!("last_lsn:       {}\n", r.last_lsn));
    out.push_str(&format!("frames_replayed:{}\n", r.frames_replayed));
    if r.truncated.is_empty() {
        out.push_str("tail:           clean\n");
    } else {
        for (seg_id, off) in &r.truncated {
            out.push_str(&format!(
                "tail:           torn frame truncated in seg_{:016x} at offset {}\n",
                seg_id, off
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::commit::CommitMode;
    use crate::wal::engine::{WalEngine, WalEngineConfig};

    #[test]
    fn inspect_and_verify_report_replayed_state() {
        let dir = tempfile::tempdir().unwrap();
        {
            let eng = WalEngine::open(dir.path(), WalEngineConfig::default()).unwrap();
            eng.write(1, b"alpha", CommitMode::Strict).unwrap();
            eng.write(2, b"beta-longer", CommitMode::Strict).unwrap();
            eng.write(3, b"gamma", CommitMode::Strict).unwrap();
            eng.delete(2, CommitMode::Strict).unwrap();
            // drop：排空组提交 + 释放 flock
        }

        let r = inspect(dir.path(), WalEngineConfig::default().seg_size).unwrap();
        assert_eq!(r.volume_id, 0); // 测试未注入 volume_id
        assert_eq!(r.active_count, 2);
        assert_eq!(r.deleted_count, 1);
        assert_eq!(r.used_bytes, 5 + 5);
        assert_eq!(r.staging_bytes, 11); // "beta-longer" tombstone 保留期物理字节
        assert_eq!(r.garbage_bytes, 0);
        // last_lsn 含停机 CKPT_ANCHOR（3 write + 1 delete = lsn 4，anchor
        // 为 lsn 5）；帧数同。
        assert_eq!(r.last_lsn, 5);
        assert_eq!(r.frames_replayed, 5);
        assert_eq!(r.segments.len(), 1);
        assert!(r.active_seg_id.is_some());
        assert!(r.truncated.is_empty());

        let v = verify(dir.path(), WalEngineConfig::default().seg_size).unwrap();
        assert_eq!(v.active_count, r.active_count);
        let text = format_report(&v);
        assert!(text.contains("active_needles: 2"), "report:\n{text}");
        assert!(text.contains("tail:           clean"));
    }

    #[test]
    fn list_needles_enumerates_active_sorted_with_prefix() {
        let dir = tempfile::tempdir().unwrap();
        {
            let eng = WalEngine::open(dir.path(), WalEngineConfig::default()).unwrap();
            for i in [3u64, 1, 2] {
                eng.write(i, b"x", CommitMode::Strict).unwrap();
            }
            eng.delete(2, CommitMode::Strict).unwrap();
        }
        let rows = list_needles(dir.path(), WalEngineConfig::default().seg_size, 0, None).unwrap();
        // 仅活跃 needle，按 id 升序（id=2 已删）。
        assert_eq!(
            rows.iter().map(|r| r.needle_id).collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert!(rows.iter().all(|r| r.data_len == 1));

        let prefixed =
            list_needles(dir.path(), WalEngineConfig::default().seg_size, 3, None).unwrap();
        assert_eq!(prefixed.len(), 1);
        assert_eq!(prefixed[0].needle_id, 3);
        assert!(format_needles(&rows).contains("2 needle(s)"));
    }

    #[test]
    fn offline_write_commands_and_busy_guard() {
        let dir = tempfile::tempdir().unwrap();
        let seg = 4096;
        {
            let eng = WalEngine::open(
                dir.path(),
                WalEngineConfig {
                    seg_size: seg,
                    preallocate: false,
                    ckpt_interval: None,
                    ckpt_on_close: false,
                    tombstone_retention_secs: 0,
                    gc: GcConfig {
                        interval: None,
                        ..GcConfig::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap();
            eng.write(1, b"a", CommitMode::Strict).unwrap();
            eng.delete(1, CommitMode::Strict).unwrap();

            // server 持有 flock 期间：写命令 EBUSY。
            let busy = run_checkpoint(dir.path(), seg);
            assert!(matches!(busy, Err(AdminError::Busy(_))));
            assert!(
                run_gc(dir.path(), seg, Some(0)).is_err_and(|e| matches!(e, AdminError::Busy(_)))
            );
            assert!(matches!(
                run_resize(dir.path(), seg, 1234),
                Err(AdminError::Busy(_))
            ));
            // 只读命令不取锁，照常工作。
            assert!(inspect(dir.path(), seg).is_ok());
        }

        // server 退出后离线写命令成功。
        let lsn = run_resize(dir.path(), seg, 4096).unwrap();
        assert!(lsn >= 1);
        let out = run_checkpoint(dir.path(), seg).unwrap();
        assert!(out.ckpt_seq >= 1);
        let gc = run_gc(dir.path(), seg, Some(0)).unwrap();
        assert!(gc.purged >= 1);
        let r = inspect(dir.path(), seg).unwrap();
        assert_eq!(r.volume_size, 4096);
    }
}
