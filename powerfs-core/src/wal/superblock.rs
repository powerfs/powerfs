//! superblock 双副本轮换（方案 §4.5）。
//!
//! 两个固定文件 `superblock.a` / `superblock.b`（各 128B）轮换写入：
//! 每次更新写 seq 较旧的那份副本（barrier write：tmp → fsync → rename →
//! fsync 目录项），崩溃时至少一份完整副本存活。挂载时读两份取 seq 更大
//! 且 CRC 合法的一份；两份都损坏 → 拒绝挂载（recovery_mode=absolute
//! 语义的物理前提）。
//!
//! ```text
//! offset  size  field
//! 0       8     magic        "PFWLSB\0\0"
//! 8       2     format_ver   u16 = 1
//! 10      8     seq          u64   # 单调递增，高者胜出
//! 18      8     volume_id    u64
//! 26      8     latest_ckpt_seq u64
//! 34      8     active_seg_id   u64
//! 42      8     active_seg_size u64
//! 50      8     volume_size  u64
//! 58      1     state        u8
//! 59      8     created_ts   i64
//! 67      8     last_mount_ts i64
//! 75      8     min_live_snapshot_lsn u64  # P3 快照接入，暂 0
//! 83      41    reserved     零填充
//! 124     4     crc32c       over [0..124)
//! ```

use std::io::Write;
use std::path::{Path, PathBuf};

/// superblock 大小（两副本一致）。
pub const SB_SIZE: usize = 128;
/// magic。
pub const SB_MAGIC: &[u8; 8] = b"PFWLSB\0\0";
/// 格式版本。
pub const SB_FORMAT_VER: u16 = 1;
const SB_CRC_OFF: usize = SB_SIZE - 4;

const COPY_A: &str = "superblock.a";
const COPY_B: &str = "superblock.b";

/// superblock 内容。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Superblock {
    pub seq: u64,
    pub volume_id: u64,
    pub latest_ckpt_seq: u64,
    pub active_seg_id: u64,
    pub active_seg_size: u64,
    pub volume_size: u64,
    pub state: u8,
    pub created_ts: i64,
    pub last_mount_ts: i64,
    /// 活跃快照中最小的 root_lsn（P3；0 表示无快照）。
    pub min_live_snapshot_lsn: u64,
}

/// superblock 错误。
#[derive(Debug, thiserror::Error)]
pub enum SbError {
    #[error("no superblock in {dir} (fresh volume)")]
    NotFound { dir: PathBuf },
    #[error("both superblock copies unusable in {dir}: {reason}")]
    BothCorrupt { dir: PathBuf, reason: String },
    #[error("io error on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

fn io_err(path: &Path, e: std::io::Error) -> SbError {
    SbError::Io {
        path: path.to_path_buf(),
        source: e,
    }
}

/// 编码 128B（末 4B 为 CRC）。
pub fn encode(sb: &Superblock) -> Vec<u8> {
    let mut buf = Vec::with_capacity(SB_SIZE);
    buf.extend_from_slice(SB_MAGIC);
    buf.extend_from_slice(&SB_FORMAT_VER.to_le_bytes());
    buf.extend_from_slice(&sb.seq.to_le_bytes());
    buf.extend_from_slice(&sb.volume_id.to_le_bytes());
    buf.extend_from_slice(&sb.latest_ckpt_seq.to_le_bytes());
    buf.extend_from_slice(&sb.active_seg_id.to_le_bytes());
    buf.extend_from_slice(&sb.active_seg_size.to_le_bytes());
    buf.extend_from_slice(&sb.volume_size.to_le_bytes());
    buf.push(sb.state);
    buf.extend_from_slice(&sb.created_ts.to_le_bytes());
    buf.extend_from_slice(&sb.last_mount_ts.to_le_bytes());
    buf.extend_from_slice(&sb.min_live_snapshot_lsn.to_le_bytes());
    buf.resize(SB_CRC_OFF, 0);
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// 解码（magic/CRC 校验）。
pub fn decode(raw: &[u8]) -> Result<Superblock, &'static str> {
    if raw.len() < SB_SIZE {
        return Err("truncated");
    }
    if &raw[0..8] != SB_MAGIC {
        return Err("bad magic");
    }
    let ver = u16::from_le_bytes(raw[8..10].try_into().unwrap());
    if ver != SB_FORMAT_VER {
        return Err("unsupported version");
    }
    let expect = u32::from_le_bytes(raw[SB_CRC_OFF..].try_into().unwrap());
    let got = crc32c::crc32c(&raw[..SB_CRC_OFF]);
    if expect != got {
        return Err("crc mismatch");
    }
    Ok(Superblock {
        seq: u64::from_le_bytes(raw[10..18].try_into().unwrap()),
        volume_id: u64::from_le_bytes(raw[18..26].try_into().unwrap()),
        latest_ckpt_seq: u64::from_le_bytes(raw[26..34].try_into().unwrap()),
        active_seg_id: u64::from_le_bytes(raw[34..42].try_into().unwrap()),
        active_seg_size: u64::from_le_bytes(raw[42..50].try_into().unwrap()),
        volume_size: u64::from_le_bytes(raw[50..58].try_into().unwrap()),
        state: raw[58],
        created_ts: i64::from_le_bytes(raw[59..67].try_into().unwrap()),
        last_mount_ts: i64::from_le_bytes(raw[67..75].try_into().unwrap()),
        min_live_snapshot_lsn: u64::from_le_bytes(raw[75..83].try_into().unwrap()),
    })
}

/// 读单副本（不存在 → Ok(None)；损坏 → Ok(None)，交由上层裁决）。
fn read_copy(dir: &Path, name: &str) -> Result<Option<Superblock>, SbError> {
    let path = dir.join(name);
    match std::fs::read(&path) {
        Ok(raw) => Ok(decode(&raw).ok()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err(&path, e)),
    }
}

/// 加载：读两份副本取合法最大 seq；两份都不可用 → 拒绝（BothCorrupt）。
pub fn load(dir: &Path) -> Result<Superblock, SbError> {
    let a = read_copy(dir, COPY_A)?;
    let b = read_copy(dir, COPY_B)?;
    match (a, b) {
        (Some(x), Some(y)) => Ok(if x.seq >= y.seq { x } else { y }),
        (Some(x), None) => Ok(x),
        (None, Some(y)) => Ok(y),
        (None, None) => {
            // 区分"从未写过"与"写过但全坏"：任一副本文件存在即视为损坏。
            if dir.join(COPY_A).exists() || dir.join(COPY_B).exists() {
                Err(SbError::BothCorrupt {
                    dir: dir.to_path_buf(),
                    reason: "copies exist but none decodable".into(),
                })
            } else {
                Err(SbError::NotFound {
                    dir: dir.to_path_buf(),
                })
            }
        }
    }
}

/// barrier 写入：目标副本 = 较旧的一份（缺哪份补哪份，保证至少一份
/// 完好副本存活）；tmp → fsync → rename → fsync 目录项。调用方负责 seq
/// 单调递增（单写者 flock 保证无并发写）。
pub fn store(dir: &Path, sb: &Superblock) -> Result<(), SbError> {
    let cur_a = read_copy(dir, COPY_A)?;
    let cur_b = read_copy(dir, COPY_B)?;
    let target = match (&cur_a, &cur_b) {
        (Some(a), Some(b)) => {
            if a.seq > b.seq {
                COPY_B
            } else {
                COPY_A
            }
        }
        (Some(_), None) => COPY_B,
        (None, Some(_)) => COPY_A,
        (None, None) => COPY_A,
    };
    let raw = encode(sb);
    let final_path = dir.join(target);
    let tmp_path = dir.join(format!("{target}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp_path).map_err(|e| io_err(&tmp_path, e))?;
        f.write_all(&raw).map_err(|e| io_err(&tmp_path, e))?;
        f.sync_all().map_err(|e| io_err(&tmp_path, e))?;
    }
    std::fs::rename(&tmp_path, &final_path).map_err(|e| io_err(&final_path, e))?;
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_err(dir, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(seq: u64) -> Superblock {
        Superblock {
            seq,
            volume_id: 42,
            latest_ckpt_seq: 7,
            active_seg_id: 3,
            active_seg_size: 64 << 20,
            volume_size: 10 << 30,
            state: 0,
            created_ts: 1725900000,
            last_mount_ts: 1725900100,
            min_live_snapshot_lsn: 0,
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let sb = sample(5);
        let decoded = decode(&encode(&sb)).unwrap();
        assert_eq!(decoded, sb);
    }

    #[test]
    fn fresh_volume_reports_not_found() {
        let dir = tempfile::tempdir().unwrap();
        match load(dir.path()) {
            Err(SbError::NotFound { .. }) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn store_load_roundtrip_and_rotation() {
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), &sample(1)).unwrap();
        assert!(dir.path().join(COPY_A).exists());
        store(dir.path(), &sample(2)).unwrap();
        assert!(dir.path().join(COPY_B).exists());
        // 第三次覆盖较旧的 .a。
        store(dir.path(), &sample(3)).unwrap();
        assert_eq!(load(dir.path()).unwrap().seq, 3);

        // 覆盖后两份内容不同：.a 是 seq=3，.b 是 seq=2。
        let a = read_copy(dir.path(), COPY_A).unwrap().unwrap();
        let b = read_copy(dir.path(), COPY_B).unwrap().unwrap();
        assert_eq!((a.seq, b.seq), (3, 2));
    }

    #[test]
    fn single_corrupt_copy_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), &sample(1)).unwrap();
        store(dir.path(), &sample(2)).unwrap();
        // 破坏较新的 .b：load 回退到 .a（seq=1），数据仍可用但读到旧值。
        let path_b = dir.path().join(COPY_B);
        let mut raw = std::fs::read(&path_b).unwrap();
        raw[30] ^= 0x01;
        std::fs::write(&path_b, &raw).unwrap();
        assert_eq!(load(dir.path()).unwrap().seq, 1);
    }

    #[test]
    fn both_corrupt_rejected() {
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), &sample(1)).unwrap();
        store(dir.path(), &sample(2)).unwrap();
        for name in [COPY_A, COPY_B] {
            let p = dir.path().join(name);
            let mut raw = std::fs::read(&p).unwrap();
            raw[30] ^= 0x01;
            std::fs::write(&p, &raw).unwrap();
        }
        match load(dir.path()) {
            Err(SbError::BothCorrupt { .. }) => {}
            other => panic!("expected BothCorrupt, got {other:?}"),
        }
    }

    #[test]
    fn torn_copy_treated_as_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), &sample(1)).unwrap();
        // 撕裂：只剩半份。
        let path_b = dir.path().join(COPY_B);
        std::fs::write(&path_b, vec![0u8; 40]).unwrap();
        assert_eq!(load(dir.path()).unwrap().seq, 1);

        // 两份都撕裂 → 拒绝。
        let path_a = dir.path().join(COPY_A);
        std::fs::write(&path_a, vec![0xab_u8; 60]).unwrap();
        assert!(matches!(load(dir.path()), Err(SbError::BothCorrupt { .. })));
    }

    #[test]
    fn tmp_file_never_left_behind() {
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), &sample(1)).unwrap();
        store(dir.path(), &sample(2)).unwrap();
        assert!(!dir.path().join("superblock.a.tmp").exists());
        assert!(!dir.path().join("superblock.b.tmp").exists());
    }
}
