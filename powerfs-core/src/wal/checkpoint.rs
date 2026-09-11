//! checkpoint 文件编解码（方案 §4.4）。
//!
//! checkpoint 是重放起点的物化：全量索引（活跃 needle / tombstone / 死
//! 副本账本 / 快照表留位 / 分配统计）+ 段清单（per-seg live/staging/dead
//! 字节分桶，GC 回收依据）。写完成后 rename 生效，文件不可变；同文件
//! 系统内 hardlink 即近零成本克隆（P3 克隆快照的基础）。
//!
//! 落盘流程（原子性）：写 `ckpt_<seq>.tmp` → fsync → rename 为
//! `ckpt_<seq>.bin` → fsync 目录项。任何一步失败只留下孤立 tmp，不破坏
//! 已存在的上一代 checkpoint。
//!
//! 文件布局（小端，无对齐填充）：
//!
//! ```text
//! [header 128B] magic|ver|header_len|ckpt_seq|applied_lsn|volume_id
//!               |active_seg_id|next_seg_id|next_needle_id|next_snapshot_id
//!               |created_ts|counts(seg/needle/tomb/dead/snap/purged)|reserved
//!               |header_crc(crc32c over [0..124))
//! [segments]    per seg 49B: seg_id|state|live_bytes|staging_bytes
//!               |dead_bytes|base_lsn|last_lsn
//! [needles]     per entry 61B: needle_id|seg_id|offset|data_len|checksum
//!               |checksum_algo|version_lsn|refcnt|created_at|flags
//! [tombstones]  per entry 56B: needle_id|seg_id|offset|data_len|crc
//!               |version_lsn|deleted_at|retention_until
//! [dead]        per entry 32B: seg_id|offset|data_len|crc|version_lsn
//! [snapshots]   per entry 26+nB: snapshot_id|root_lsn|created_ts|name_len
//!               |name（P2 空表）
//! [purged]      per entry 16B: needle_id|purge_lsn（§8 case C 防复活标记）
//! [alloc_stats] 56B: used|staging|garbage|pinned|active_count|deleted_count|volume_size
//! [footer 4B]   crc32c(whole file up to footer)
//! ```

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::wal::index::{DeadCopy, NeedleEntry, TombstoneEntry, WalIndex};

/// checkpoint 头部大小。
pub const CKPT_HEADER_SIZE: usize = 128;
/// 段头 magic（8 字节，与段文件 magic 风格一致）。
pub const CKPT_MAGIC: [u8; 8] = *b"PFWLCKPT";
/// 格式版本。
pub const CKPT_FORMAT_VER: u16 = 1;
/// header CRC 偏移（CRC 覆盖 [0..CKPT_HEADER_CRC_OFF)）。
const CKPT_HEADER_CRC_OFF: usize = CKPT_HEADER_SIZE - 4;

/// checksum 算法标识：帧 CRC32C 的 u64 扩展（P2 唯一取值）。
pub const CHECKSUM_ALGO_CRC32C_FRAME: u8 = 0;

/// 段状态编码（§4.4：sealed/active/gc_pending；gc_pending 为 GC 预留位）。
pub const SEG_STATE_SEALED: u8 = 0;
pub const SEG_STATE_ACTIVE: u8 = 1;
pub const SEG_STATE_GC_PENDING: u8 = 2;

/// 段清单条目（per-seg 字节分桶，GC 与统计的持久化形态）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CkptSegEntry {
    pub seg_id: u64,
    pub state: u8,
    /// 该段内活跃 needle 数据字节。
    pub live_bytes: u64,
    /// 该段内 tombstone 保留期字节。
    pub staging_bytes: u64,
    /// 该段内死副本字节。
    pub dead_bytes: u64,
    pub base_lsn: u64,
    /// 该段最后一条记录 lsn（重放边界判定）。
    pub last_lsn: u64,
}

/// 快照表条目（P2 空表，格式先行固定）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CkptSnapshotEntry {
    pub snapshot_id: u64,
    pub root_lsn: u64,
    pub created_ts: i64,
    pub name: String,
}

/// checkpoint 头部。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CkptHeader {
    pub ckpt_seq: u64,
    /// 本 checkpoint 覆盖的最后一条记录 lsn（重放起点判定）。
    pub applied_lsn: u64,
    pub volume_id: u64,
    pub active_seg_id: u64,
    pub next_seg_id: u64,
    pub next_needle_id: u64,
    pub next_snapshot_id: u64,
    pub created_ts: i64,
}

/// 空间分配统计（§9.3 四项 + 容量）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CkptAllocStats {
    pub used: u64,
    pub staging: u64,
    pub garbage: u64,
    pub pinned: u64,
    pub active_count: u64,
    pub deleted_count: u64,
    /// 卷逻辑容量（§11 VOLUME_META 推进值；0 = 未设置）。
    pub volume_size: u64,
}

impl CkptAllocStats {
    /// 与 [`crate::wal::index::IndexStats`] 的字段一一对应。
    pub fn to_index_stats(self) -> crate::wal::index::IndexStats {
        crate::wal::index::IndexStats {
            used_bytes: self.used,
            staging_bytes: self.staging,
            garbage_bytes: self.garbage,
            pinned_bytes: self.pinned,
            active_count: self.active_count,
            deleted_count: self.deleted_count,
        }
    }
}

/// checkpoint 全量数据。
#[derive(Debug, Clone, Default)]
pub struct CkptData {
    pub header: Option<CkptHeader>,
    pub segments: Vec<CkptSegEntry>,
    pub needles: Vec<NeedleEntry>,
    pub tombstones: Vec<TombstoneEntry>,
    pub dead: Vec<DeadCopy>,
    pub snapshots: Vec<CkptSnapshotEntry>,
    /// tombstone purge 标记（needle_id → purge_lsn，§8 case C）。
    pub purged: Vec<(u64, u64)>,
    pub alloc_stats: CkptAllocStats,
}

/// checkpoint 错误。
#[derive(Debug, thiserror::Error)]
pub enum CkptError {
    #[error("io error on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("bad magic in {path}")]
    BadMagic { path: PathBuf },
    #[error("unsupported format ver {got} in {path}")]
    Version { path: PathBuf, got: u16 },
    #[error("truncated file {path}: need {need} bytes, got {got}")]
    Truncated {
        path: PathBuf,
        need: usize,
        got: usize,
    },
    #[error("header crc mismatch in {path}: expected {expected:#010x}, got {got:#010x}")]
    HeaderCrc {
        path: PathBuf,
        expected: u32,
        got: u32,
    },
    #[error("file crc mismatch in {path}: expected {expected:#010x}, got {got:#010x}")]
    FileCrc {
        path: PathBuf,
        expected: u32,
        got: u32,
    },
    #[error("entry counts or sizes mismatch in {path}")]
    CountsMismatch { path: PathBuf },
}

fn io_err(path: &Path, e: std::io::Error) -> CkptError {
    CkptError::Io {
        path: path.to_path_buf(),
        source: e,
    }
}

/// `ckpt_<seq:016x>.bin`。
pub fn ckpt_file_name(seq: u64) -> String {
    format!("ckpt_{seq:016x}.bin")
}

/// 扫描目录内全部 checkpoint 序号（升序去重）。
pub fn list_ckpts(dir: &Path) -> Vec<u64> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(seq) = name
                .strip_prefix("ckpt_")
                .and_then(|s| s.strip_suffix(".bin"))
                .and_then(|s| u64::from_str_radix(s, 16).ok())
            {
                out.push(seq);
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// 最新 checkpoint 序号。
pub fn latest_seq(dir: &Path) -> Option<u64> {
    list_ckpts(dir).pop()
}

/// 删除 applied_lsn > `max_lsn` 的 checkpoint 文件（中部断链恢复点回退用：
/// 恢复点之前的 ckpt 锚点引用了已弃置段，必须失效，否则下次启动会装载
/// 超前状态）。返回被删除的 ckpt 序号（升序）。损坏/不可读的 ckpt 不动。
pub fn remove_future_ckpts(dir: &Path, max_lsn: u64) -> Vec<u64> {
    let mut removed = Vec::new();
    for seq in list_ckpts(dir) {
        match load(dir, seq) {
            Ok(data) if data.header.map(|h| h.applied_lsn).unwrap_or(0) > max_lsn => {
                let path = dir.join(ckpt_file_name(seq));
                if std::fs::remove_file(&path).is_ok() {
                    removed.push(seq);
                }
            }
            _ => {}
        }
    }
    removed
}

/// 从索引与段清单构造 CkptData（engine 侧调用）。
pub fn build(header: CkptHeader, seg_entries: Vec<CkptSegEntry>, index: &WalIndex) -> CkptData {
    let st = index.stats();
    CkptData {
        header: Some(header),
        segments: seg_entries,
        needles: index.needles().cloned().collect(),
        tombstones: index.tombstones().cloned().collect(),
        dead: index.dead_copies().to_vec(),
        snapshots: Vec::new(),
        purged: index
            .purged_markers()
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect(),
        alloc_stats: CkptAllocStats {
            used: st.used_bytes,
            staging: st.staging_bytes,
            garbage: st.garbage_bytes,
            pinned: st.pinned_bytes,
            active_count: st.active_count,
            deleted_count: st.deleted_count,
            volume_size: index.volume_size(),
        },
    }
}

/// 原子写入 checkpoint：tmp → fsync → rename → fsync 目录项。
pub fn write(dir: &Path, data: &CkptData) -> Result<PathBuf, CkptError> {
    let hdr = data.header.ok_or(CkptError::CountsMismatch {
        path: dir.to_path_buf(),
    })?;
    let body = encode(data);

    let final_path = dir.join(ckpt_file_name(hdr.ckpt_seq));
    let tmp_path = dir.join(format!("{}.tmp", ckpt_file_name(hdr.ckpt_seq)));
    {
        let mut f = std::fs::File::create(&tmp_path).map_err(|e| io_err(&tmp_path, e))?;
        f.write_all(&body).map_err(|e| io_err(&tmp_path, e))?;
        f.sync_all().map_err(|e| io_err(&tmp_path, e))?;
    }
    std::fs::rename(&tmp_path, &final_path).map_err(|e| io_err(&final_path, e))?;
    // 目录项持久化：崩溃后 rename 结果可见。
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_err(dir, e))?;
    Ok(final_path)
}

/// 编码完整文件体（含 footer CRC）。
pub fn encode(data: &CkptData) -> Vec<u8> {
    let hdr = data.header.expect("checkpoint header");
    let mut buf =
        Vec::with_capacity(CKPT_HEADER_SIZE + data.segments.len() * 49 + data.needles.len() * 61);

    // ---- header [0..124) + crc [124..128) ----
    buf.extend_from_slice(&CKPT_MAGIC);
    buf.extend_from_slice(&CKPT_FORMAT_VER.to_le_bytes());
    buf.extend_from_slice(&(CKPT_HEADER_SIZE as u16).to_le_bytes());
    buf.extend_from_slice(&hdr.ckpt_seq.to_le_bytes());
    buf.extend_from_slice(&hdr.applied_lsn.to_le_bytes());
    buf.extend_from_slice(&hdr.volume_id.to_le_bytes());
    buf.extend_from_slice(&hdr.active_seg_id.to_le_bytes());
    buf.extend_from_slice(&hdr.next_seg_id.to_le_bytes());
    buf.extend_from_slice(&hdr.next_needle_id.to_le_bytes());
    buf.extend_from_slice(&hdr.next_snapshot_id.to_le_bytes());
    buf.extend_from_slice(&hdr.created_ts.to_le_bytes());
    buf.extend_from_slice(&(data.segments.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(data.needles.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(data.tombstones.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(data.dead.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(data.snapshots.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(data.purged.len() as u64).to_le_bytes());
    buf.resize(CKPT_HEADER_CRC_OFF, 0); // 保留区零填充
    let hcrc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&hcrc.to_le_bytes());
    debug_assert_eq!(buf.len(), CKPT_HEADER_SIZE);

    // ---- segments（49B each）----
    for s in &data.segments {
        buf.extend_from_slice(&s.seg_id.to_le_bytes());
        buf.push(s.state);
        buf.extend_from_slice(&s.live_bytes.to_le_bytes());
        buf.extend_from_slice(&s.staging_bytes.to_le_bytes());
        buf.extend_from_slice(&s.dead_bytes.to_le_bytes());
        buf.extend_from_slice(&s.base_lsn.to_le_bytes());
        buf.extend_from_slice(&s.last_lsn.to_le_bytes());
    }

    // ---- needles（61B each）----
    for n in &data.needles {
        buf.extend_from_slice(&n.needle_id.to_le_bytes());
        buf.extend_from_slice(&n.seg_id.to_le_bytes());
        buf.extend_from_slice(&n.offset.to_le_bytes());
        buf.extend_from_slice(&n.data_len.to_le_bytes());
        buf.extend_from_slice(&(n.crc as u64).to_le_bytes());
        buf.push(CHECKSUM_ALGO_CRC32C_FRAME);
        buf.extend_from_slice(&n.version_lsn.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // refcnt：P3 快照引用
        buf.extend_from_slice(&n.created_at.to_le_bytes());
        buf.extend_from_slice(&n.flags.to_le_bytes());
    }

    // ---- tombstones（56B each）----
    for t in &data.tombstones {
        buf.extend_from_slice(&t.needle_id.to_le_bytes());
        buf.extend_from_slice(&t.seg_id.to_le_bytes());
        buf.extend_from_slice(&t.offset.to_le_bytes());
        buf.extend_from_slice(&t.data_len.to_le_bytes());
        buf.extend_from_slice(&t.crc.to_le_bytes());
        buf.extend_from_slice(&t.version_lsn.to_le_bytes());
        buf.extend_from_slice(&t.deleted_at.to_le_bytes());
        buf.extend_from_slice(&t.retention_until.to_le_bytes());
    }

    // ---- dead ledger（32B each）----
    for d in &data.dead {
        buf.extend_from_slice(&d.seg_id.to_le_bytes());
        buf.extend_from_slice(&d.offset.to_le_bytes());
        buf.extend_from_slice(&d.data_len.to_le_bytes());
        buf.extend_from_slice(&d.crc.to_le_bytes());
        buf.extend_from_slice(&d.version_lsn.to_le_bytes());
    }

    // ---- snapshots（26+nB each，P2 空表）----
    for s in &data.snapshots {
        buf.extend_from_slice(&s.snapshot_id.to_le_bytes());
        buf.extend_from_slice(&s.root_lsn.to_le_bytes());
        buf.extend_from_slice(&s.created_ts.to_le_bytes());
        buf.extend_from_slice(&(s.name.len() as u16).to_le_bytes());
        buf.extend_from_slice(s.name.as_bytes());
    }

    // ---- purged markers（16B each）----
    for (needle_id, purge_lsn) in &data.purged {
        buf.extend_from_slice(&needle_id.to_le_bytes());
        buf.extend_from_slice(&purge_lsn.to_le_bytes());
    }

    // ---- alloc_stats（48B）----
    let a = &data.alloc_stats;
    buf.extend_from_slice(&a.used.to_le_bytes());
    buf.extend_from_slice(&a.staging.to_le_bytes());
    buf.extend_from_slice(&a.garbage.to_le_bytes());
    buf.extend_from_slice(&a.pinned.to_le_bytes());
    buf.extend_from_slice(&a.active_count.to_le_bytes());
    buf.extend_from_slice(&a.deleted_count.to_le_bytes());
    buf.extend_from_slice(&a.volume_size.to_le_bytes());

    // ---- footer: whole-file crc ----
    let fcrc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&fcrc.to_le_bytes());
    buf
}

/// 装载指定 seq 的 checkpoint（全文件 CRC + header CRC 校验）。
pub fn load(dir: &Path, seq: u64) -> Result<CkptData, CkptError> {
    let path = dir.join(ckpt_file_name(seq));
    let raw = std::fs::read(&path).map_err(|e| io_err(&path, e))?;
    decode(&path, &raw)
}

/// 解码（供损坏注入测试直接调用）。
pub fn decode(path: &Path, raw: &[u8]) -> Result<CkptData, CkptError> {
    if raw.len() < CKPT_HEADER_SIZE + 4 {
        return Err(CkptError::Truncated {
            path: path.to_path_buf(),
            need: CKPT_HEADER_SIZE + 4,
            got: raw.len(),
        });
    }
    // footer CRC 覆盖 [0..len-4)。
    let expect = u32::from_le_bytes(raw[raw.len() - 4..].try_into().unwrap());
    let got = crc32c::crc32c(&raw[..raw.len() - 4]);
    if expect != got {
        return Err(CkptError::FileCrc {
            path: path.to_path_buf(),
            expected: expect,
            got,
        });
    }

    if raw[0..8] != CKPT_MAGIC {
        return Err(CkptError::BadMagic {
            path: path.to_path_buf(),
        });
    }
    let ver = u16::from_le_bytes(raw[8..10].try_into().unwrap());
    if ver != CKPT_FORMAT_VER {
        return Err(CkptError::Version {
            path: path.to_path_buf(),
            got: ver,
        });
    }
    let header_len = u16::from_le_bytes(raw[10..12].try_into().unwrap()) as usize;
    if header_len != CKPT_HEADER_SIZE {
        return Err(CkptError::Version {
            path: path.to_path_buf(),
            got: header_len as u16,
        });
    }
    let expect_hcrc = u32::from_le_bytes(
        raw[CKPT_HEADER_CRC_OFF..CKPT_HEADER_SIZE]
            .try_into()
            .unwrap(),
    );
    let got_hcrc = crc32c::crc32c(&raw[..CKPT_HEADER_CRC_OFF]);
    if expect_hcrc != got_hcrc {
        return Err(CkptError::HeaderCrc {
            path: path.to_path_buf(),
            expected: expect_hcrc,
            got: got_hcrc,
        });
    }

    let u64at = |off: usize| u64::from_le_bytes(raw[off..off + 8].try_into().unwrap());
    let header = CkptHeader {
        ckpt_seq: u64at(12),
        applied_lsn: u64at(20),
        volume_id: u64at(28),
        active_seg_id: u64at(36),
        next_seg_id: u64at(44),
        next_needle_id: u64at(52),
        next_snapshot_id: u64at(60),
        created_ts: i64::from_le_bytes(raw[68..76].try_into().unwrap()),
    };
    let seg_count = u64at(76) as usize;
    let needle_count = u64at(84) as usize;
    let tomb_count = u64at(92) as usize;
    let dead_count = u64at(100) as usize;
    let snap_count = u64at(108) as usize;
    let purged_count = u64at(116) as usize;

    let mut off = CKPT_HEADER_SIZE;
    let mut need = |n: usize| -> Result<&[u8], CkptError> {
        if off + n > raw.len() - 4 {
            return Err(CkptError::Truncated {
                path: path.to_path_buf(),
                need: off + n,
                got: raw.len(),
            });
        }
        let s = &raw[off..off + n];
        off += n;
        Ok(s)
    };
    let g64 = |s: &[u8], o: usize| u64::from_le_bytes(s[o..o + 8].try_into().unwrap());
    let g32 = |s: &[u8], o: usize| u32::from_le_bytes(s[o..o + 4].try_into().unwrap());
    let i64at = |s: &[u8], o: usize| i64::from_le_bytes(s[o..o + 8].try_into().unwrap());

    // ---- segments ----
    let mut segments = Vec::with_capacity(seg_count);
    for _ in 0..seg_count {
        let s = need(49)?;
        segments.push(CkptSegEntry {
            seg_id: g64(s, 0),
            state: s[8],
            live_bytes: g64(s, 9),
            staging_bytes: g64(s, 17),
            dead_bytes: g64(s, 25),
            base_lsn: g64(s, 33),
            last_lsn: g64(s, 41),
        });
    }

    // ---- needles ----
    let mut needles = Vec::with_capacity(needle_count);
    for _ in 0..needle_count {
        let s = need(61)?;
        needles.push(NeedleEntry {
            needle_id: g64(s, 0),
            seg_id: g64(s, 8),
            offset: g64(s, 16),
            data_len: g32(s, 24),
            crc: g32(s, 28),
            version_lsn: g64(s, 37),
            created_at: i64at(s, 49),
            flags: g32(s, 57),
        });
    }

    // ---- tombstones ----
    let mut tombstones = Vec::with_capacity(tomb_count);
    for _ in 0..tomb_count {
        let s = need(56)?;
        tombstones.push(TombstoneEntry {
            needle_id: g64(s, 0),
            seg_id: g64(s, 8),
            offset: g64(s, 16),
            data_len: g32(s, 24),
            crc: g32(s, 28),
            version_lsn: g64(s, 32),
            deleted_at: i64at(s, 40),
            retention_until: i64at(s, 48),
        });
    }

    // ---- dead ledger ----
    let mut dead = Vec::with_capacity(dead_count);
    for _ in 0..dead_count {
        let s = need(32)?;
        dead.push(DeadCopy {
            seg_id: g64(s, 0),
            offset: g64(s, 8),
            data_len: g32(s, 16),
            crc: g32(s, 20),
            version_lsn: g64(s, 24),
        });
    }

    // ---- snapshots ----
    let mut snapshots = Vec::with_capacity(snap_count);
    for _ in 0..snap_count {
        let head = need(26)?;
        let name_len = u16::from_le_bytes(head[24..26].try_into().unwrap()) as usize;
        let name = need(name_len)?;
        snapshots.push(CkptSnapshotEntry {
            snapshot_id: g64(head, 0),
            root_lsn: g64(head, 8),
            created_ts: i64at(head, 16),
            name: String::from_utf8_lossy(name).into_owned(),
        });
    }

    // ---- purged markers ----
    let mut purged = Vec::with_capacity(purged_count);
    for _ in 0..purged_count {
        let s = need(16)?;
        purged.push((g64(s, 0), g64(s, 8)));
    }

    // ---- alloc_stats（56B）----
    let st = need(56)?;
    let alloc_stats = CkptAllocStats {
        used: g64(st, 0),
        staging: g64(st, 8),
        garbage: g64(st, 16),
        pinned: g64(st, 24),
        active_count: g64(st, 32),
        deleted_count: g64(st, 40),
        volume_size: g64(st, 48),
    };

    if off != raw.len() - 4 {
        return Err(CkptError::CountsMismatch {
            path: path.to_path_buf(),
        });
    }

    Ok(CkptData {
        header: Some(header),
        segments,
        needles,
        tombstones,
        dead,
        snapshots,
        purged,
        alloc_stats,
    })
}

/// 从 checkpoint 数据重建内存索引（恢复路径装配）。
///
/// 统计从条目重建（不信任 alloc_stats 字段本身），重建结果应与
/// alloc_stats 一致——不一致说明 checkpoint 与索引失配（引擎侧断言）。
pub fn into_index(data: &CkptData) -> WalIndex {
    let mut idx = WalIndex::new();
    idx.load_from(
        data.needles.iter().cloned(),
        data.tombstones.iter().cloned(),
        data.dead.iter().cloned(),
        data.purged.iter().copied(),
    );
    idx.set_volume_size(data.alloc_stats.volume_size);
    idx
}

/// per-seg 字节分桶（GC 扫描与 checkpoint 段清单共用）。
///
/// 返回 `seg_id → (live, staging, dead)`：live = 活跃 needle 字节，
/// staging = tombstone 字节，dead = 死副本字节。
pub fn per_seg_bytes(index: &WalIndex) -> HashMap<u64, (u64, u64, u64)> {
    let mut map: HashMap<u64, (u64, u64, u64)> = HashMap::new();
    for n in index.needles() {
        map.entry(n.seg_id).or_insert((0, 0, 0)).0 += n.data_len as u64;
    }
    for t in index.tombstones() {
        map.entry(t.seg_id).or_insert((0, 0, 0)).1 += t.data_len as u64;
    }
    for d in index.dead_copies() {
        map.entry(d.seg_id).or_insert((0, 0, 0)).2 += d.data_len as u64;
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_index() -> WalIndex {
        let mut idx = WalIndex::new();
        // 段 1：needle 1 (100B) 覆写 150B → 死副本 100 进账本
        idx.apply_data(1, 64, 0x11, 1, &make_data(1, 100), 100)
            .unwrap();
        idx.apply_data(1, 300, 0x22, 2, &make_data(1, 150), 101)
            .unwrap();
        // 段 2：needle 2 (60B) + tombstone (60B)
        idx.apply_data(2, 64, 0x33, 3, &make_data(2, 60), 102)
            .unwrap();
        idx.apply_delete(4, &make_delete(9)).unwrap(); // 无活跃条目 → 忽略
        idx.apply_delete(5, &make_delete(2)).unwrap();
        idx
    }

    fn make_data(needle_id: u64, len: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(12 + len);
        v.extend_from_slice(&needle_id.to_le_bytes());
        v.extend_from_slice(&(len as u32).to_le_bytes());
        v.extend_from_slice(&vec![0u8; len]);
        v
    }

    fn make_delete(needle_id: u64) -> Vec<u8> {
        let mut v = Vec::with_capacity(24);
        v.extend_from_slice(&needle_id.to_le_bytes());
        v.extend_from_slice(&1000i64.to_le_bytes());
        v.extend_from_slice(&8000i64.to_le_bytes());
        v
    }

    fn sample_data() -> CkptData {
        let header = CkptHeader {
            ckpt_seq: 7,
            applied_lsn: 5,
            volume_id: 42,
            active_seg_id: 2,
            next_seg_id: 3,
            next_needle_id: 3,
            next_snapshot_id: 1,
            created_ts: 1725900000,
        };
        let segments = vec![
            CkptSegEntry {
                seg_id: 1,
                state: SEG_STATE_SEALED,
                live_bytes: 150,
                staging_bytes: 0,
                dead_bytes: 100,
                base_lsn: 1,
                last_lsn: 2,
            },
            CkptSegEntry {
                seg_id: 2,
                state: SEG_STATE_ACTIVE,
                live_bytes: 0,
                staging_bytes: 60,
                dead_bytes: 0,
                base_lsn: 3,
                last_lsn: 5,
            },
        ];
        build(header, segments, &sample_index())
    }

    #[test]
    fn roundtrip_preserves_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample_data();
        write(dir.path(), &data).unwrap();

        let loaded = load(dir.path(), 7).unwrap();
        assert_eq!(loaded.header, data.header);
        assert_eq!(loaded.segments, data.segments);
        // needles/tombstones 按 HashMap 迭代序写入，排序后逐项比对。
        let mut a = loaded.needles.clone();
        let mut b = data.needles.clone();
        a.sort_by_key(|n| n.needle_id);
        b.sort_by_key(|n| n.needle_id);
        assert_eq!(a, b);
        let mut a = loaded.tombstones.clone();
        let mut b = data.tombstones.clone();
        a.sort_by_key(|t| t.needle_id);
        b.sort_by_key(|t| t.needle_id);
        assert_eq!(a, b);
        assert_eq!(loaded.alloc_stats, data.alloc_stats);
        // 死副本账本顺序敏感（追加序），直接整体比对。
        assert_eq!(loaded.dead, data.dead);
        assert!(loaded.snapshots.is_empty());
    }

    #[test]
    fn tmp_file_cleaned_and_final_named() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), &sample_data()).unwrap();
        assert!(dir.path().join(ckpt_file_name(7)).exists());
        assert!(!dir
            .path()
            .join(format!("{}.tmp", ckpt_file_name(7)))
            .exists());
    }

    #[test]
    fn corrupted_payload_byte_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), &sample_data()).unwrap();

        let mut raw = std::fs::read(&path).unwrap();
        let mid = raw.len() / 2;
        raw[mid] ^= 0x01;
        let err = decode(&path, &raw).unwrap_err();
        assert!(matches!(err, CkptError::FileCrc { .. }));
    }

    #[test]
    fn corrupted_header_detected_before_file_crc() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), &sample_data()).unwrap();
        let mut raw = std::fs::read(&path).unwrap();
        // 破坏 header 区并重算 file crc：仅 header crc 失败。
        raw[30] ^= 0x01;
        let n = raw.len();
        let fcrc = crc32c::crc32c(&raw[..n - 4]);
        raw[n - 4..].copy_from_slice(&fcrc.to_le_bytes());
        let err = decode(&path, &raw).unwrap_err();
        assert!(matches!(err, CkptError::HeaderCrc { .. }));
    }

    #[test]
    fn truncated_file_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), &sample_data()).unwrap();
        let raw = std::fs::read(&path).unwrap();
        // 尾部截断：footer CRC 先行检出（等价撕裂写场景）。
        let err = decode(&path, &raw[..raw.len() - 30]).unwrap_err();
        assert!(matches!(err, CkptError::FileCrc { .. }));
        // 严重截断（不足 header+footer）：Truncated 检出。
        let err2 = decode(&path, &raw[..40]).unwrap_err();
        assert!(matches!(err2, CkptError::Truncated { .. }));
    }

    #[test]
    fn empty_tables_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let header = CkptHeader {
            ckpt_seq: 1,
            applied_lsn: 0,
            volume_id: 1,
            active_seg_id: 0,
            next_seg_id: 1,
            next_needle_id: 1,
            next_snapshot_id: 1,
            created_ts: 0,
        };
        let data = build(header, Vec::new(), &WalIndex::new());
        write(dir.path(), &data).unwrap();
        let loaded = load(dir.path(), 1).unwrap();
        assert_eq!(loaded.header.unwrap(), header);
        assert!(loaded.needles.is_empty() && loaded.tombstones.is_empty());
        assert_eq!(loaded.alloc_stats, CkptAllocStats::default());
        let idx = into_index(&loaded);
        idx.assert_consistent();
    }

    #[test]
    fn snapshot_table_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut data = sample_data();
        data.snapshots.push(CkptSnapshotEntry {
            snapshot_id: 1,
            root_lsn: 3,
            created_ts: 111,
            name: "snap-alpha".into(),
        });
        data.header.as_mut().unwrap().next_snapshot_id = 2;
        write(dir.path(), &data).unwrap();
        let loaded = load(dir.path(), 7).unwrap();
        assert_eq!(loaded.snapshots, data.snapshots);
    }

    #[test]
    fn list_and_latest_seq() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(latest_seq(dir.path()), None);
        for seq in [3u64, 1, 2] {
            let mut data = sample_data();
            data.header.as_mut().unwrap().ckpt_seq = seq;
            write(dir.path(), &data).unwrap();
        }
        assert_eq!(list_ckpts(dir.path()), vec![1, 2, 3]);
        assert_eq!(latest_seq(dir.path()), Some(3));
    }

    #[test]
    fn per_seg_bucketing_matches_index() {
        let idx = sample_index();
        let map = per_seg_bytes(&idx);
        // 段 1：needle1 覆写 → live=150, dead=100
        assert_eq!(map.get(&1), Some(&(150, 0, 100)));
        // 段 2：needle2 已删除 → tombstone 60 进 staging
        assert_eq!(map.get(&2), Some(&(0, 60, 0)));
        let st = idx.stats();
        assert_eq!(st.used_bytes, 150);
        assert_eq!(st.staging_bytes, 60);
        assert_eq!(st.garbage_bytes, 100);
        idx.assert_consistent();
    }

    #[test]
    fn into_index_restores_replay_state() {
        let data = sample_data();
        let idx = into_index(&data);
        assert_eq!(idx.needle_count(), 1);
        assert_eq!(idx.tombstone_count(), 1);
        assert_eq!(idx.stats(), data.alloc_stats.to_index_stats());
        idx.assert_consistent();
    }
}
