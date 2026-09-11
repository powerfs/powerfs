//! WAL 卷本地面只读检查（管理工具 P1 最小集，方案 §19.2 / §19.3）。
//!
//! 只读命令不取 flock、只读打开：扫描段清单 + 重放（与运行中的 server
//! 并发安全）。读到的可能是略旧的一致快照；活跃段尾部正被追加时按
//! tolerate_tail 语义截断撕裂尾（`truncated` 报告截断位置）。
//!
//! 命令映射：
//! - `stats`  → [`inspect`]：段清单 + 索引统计 + LSN 概览；
//! - `verify` → [`verify`]：全量重放（帧 CRC + 哈希链逐帧校验）并报告
//!   活跃 needle / tombstone / 死副本计数。deep 语义即重放本身——每条
//!   记录的 payload 都被读取并校验。

use std::path::Path;

use crate::wal::manifest::SegManifest;
use crate::wal::replay::replay_all;

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
    pub dead_copies: usize,
    pub last_lsn: u64,
    pub frames_replayed: u64,
    /// (seg_id, 截断到的偏移)——非空表示活跃段尾部存在撕裂帧。
    pub truncated: Vec<(u64, u64)>,
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
}
