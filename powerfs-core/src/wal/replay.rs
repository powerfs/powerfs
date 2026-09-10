//! WAL 重放器（方案 §5 / 附录 B S3）。
//!
//! 按段序（seg_id 升序）扫描全部段，将记录应用到 [`WalIndex`]。恢复
//! 策略（P1，无 checkpoint）：
//! - 最后一个段的尾部撕裂（torn tail）→ tolerate_tail：截断到最后一个
//!   完整帧，恢复结果等于"已完整写入的记录序列"的终态（I1/I2）；
//! - 非最后段撕裂 / 任意段中间校验失败 → 拒绝（返回 [`ReplayError`]）；
//! - LSN 必须跨段单调递增（检出段重复/错位）。

use std::collections::HashMap;
use std::path::Path;

use crate::wal::frame::RecordType;
use crate::wal::index::WalIndex;
use crate::wal::manifest::SegManifest;
use crate::wal::segment::{SegReader, SegmentError};

/// 重放错误。
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("segment error on seg {seg_id}: {source}")]
    Segment {
        seg_id: u64,
        #[source]
        source: SegmentError,
    },
    #[error("corrupt frame in seg {seg_id} at offset {offset}: {source}")]
    Corrupt {
        seg_id: u64,
        offset: u64,
        #[source]
        source: crate::wal::frame::FrameError,
    },
    #[error("torn tail in non-tail segment seg {seg_id} (offset {offset})")]
    TornNonTail { seg_id: u64, offset: u64 },
    #[error("lsn regression in seg {seg_id}: previous {prev}, got {got}")]
    LsnRegression { seg_id: u64, prev: u64, got: u64 },
    #[error("malformed payload in seg {seg_id} at lsn {lsn} (rtype {rtype:#04x})")]
    MalformedPayload { seg_id: u64, lsn: u64, rtype: u8 },
}

/// 重放结果。
#[derive(Debug)]
pub struct ReplayResult {
    pub index: WalIndex,
    /// 重放帧总数（含 PAD）。
    pub frames_replayed: u64,
    /// tolerate_tail 截断的段（seg_id → 截断位置）。
    pub truncated: HashMap<u64, u64>,
    /// 重放覆盖的段数。
    pub segments_scanned: usize,
}

/// 对 `dir` 内 manifest 列出的全部段执行重放。
///
/// `tolerate_tail` 生效时，最后一段的撕裂尾被物理截断到最后一个完整帧
/// （fsync 截断结果），重放可安全重复执行（幂等）。
pub fn replay_all(
    dir: &Path,
    manifest: &SegManifest,
    tolerate_tail: bool,
) -> Result<ReplayResult, ReplayError> {
    let mut index = WalIndex::new();
    let mut frames_replayed = 0u64;
    let mut truncated: HashMap<u64, u64> = HashMap::new();
    let segments = manifest.segments();
    let last_idx = segments.len().saturating_sub(1);

    for (i, entry) in segments.iter().enumerate() {
        let seg_id = entry.seg_id;
        let path = dir.join(crate::wal::segment::seg_file_name(seg_id));
        let mut reader =
            SegReader::open(&path).map_err(|source| ReplayError::Segment { seg_id, source })?;

        loop {
            match reader.next_frame() {
                Ok(Some(frame)) => {
                    // LSN 跨段单调递增（PAD 不消耗 lsn 语义，仅跳过）。
                    if frame.meta.rtype != RecordType::Pad {
                        let lsn = frame.meta.lsn;
                        let prev = index.last_lsn();
                        if lsn <= prev {
                            return Err(ReplayError::LsnRegression {
                                seg_id,
                                prev,
                                got: lsn,
                            });
                        }
                    }
                    apply_frame(&mut index, seg_id, &frame.meta, &frame.payload)?;
                    frames_replayed += 1;
                }
                Ok(None) => break,
                Err(crate::wal::segment::ScanError::TornTail { .. }) => {
                    let is_tail = i == last_idx;
                    if !is_tail || !tolerate_tail {
                        return Err(ReplayError::TornNonTail {
                            seg_id,
                            offset: reader.valid_end(),
                        });
                    }
                    // tolerate_tail：截断到最后一个完整帧并 fsync。
                    let valid_end = reader.valid_end();
                    truncate_segment(&path, valid_end).map_err(|source| ReplayError::Segment {
                        seg_id,
                        source: SegmentError::Io {
                            path: path.clone(),
                            source,
                        },
                    })?;
                    truncated.insert(seg_id, valid_end);
                    break;
                }
                Err(crate::wal::segment::ScanError::Corrupt(e)) => {
                    return Err(ReplayError::Corrupt {
                        seg_id,
                        offset: reader.valid_end(),
                        source: e,
                    });
                }
                Err(crate::wal::segment::ScanError::Io(e)) => {
                    return Err(ReplayError::Segment {
                        seg_id,
                        source: SegmentError::Io {
                            path: path.clone(),
                            source: e,
                        },
                    });
                }
            }
        }
    }

    Ok(ReplayResult {
        index,
        frames_replayed,
        truncated,
        segments_scanned: segments.len(),
    })
}

/// 应用单帧到索引（rtype 分派）。
fn apply_frame(
    index: &mut WalIndex,
    seg_id: u64,
    meta: &crate::wal::segment::FrameMeta,
    payload: &[u8],
) -> Result<(), ReplayError> {
    match meta.rtype {
        RecordType::Pad => Ok(()), // 段尾填充，跳过
        RecordType::Data => {
            let now = chrono::Utc::now().timestamp();
            index
                .apply_data(seg_id, meta.offset, meta.crc, meta.lsn, payload, now)
                .map_err(|_| ReplayError::MalformedPayload {
                    seg_id,
                    lsn: meta.lsn,
                    rtype: meta.rtype.to_u8(),
                })
        }
        RecordType::Delete => {
            index
                .apply_delete(meta.lsn, payload)
                .map_err(|_| ReplayError::MalformedPayload {
                    seg_id,
                    lsn: meta.lsn,
                    rtype: meta.rtype.to_u8(),
                })
        }
        RecordType::CkptAnchor => {
            if payload.len() == 16 {
                let ckpt_seq = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                let applied_lsn = u64::from_le_bytes(payload[8..16].try_into().unwrap());
                index.apply_ckpt_anchor(ckpt_seq, applied_lsn, meta.lsn);
                Ok(())
            } else {
                Err(ReplayError::MalformedPayload {
                    seg_id,
                    lsn: meta.lsn,
                    rtype: meta.rtype.to_u8(),
                })
            }
        }
        // ATTR / SNAP_TAKE / SNAP_DROP / VOLUME_META：payload 语义在 P2+
        // 接入快照与属性功能时消费；P1 仅要求帧级完整性（CRC/链已校验）。
        RecordType::Attr | RecordType::SnapTake | RecordType::SnapDrop | RecordType::VolumeMeta => {
            log::debug!(
                "replay: payload of rtype {:#04x} at lsn {} accepted (P1 placeholder)",
                meta.rtype.to_u8(),
                meta.lsn
            );
            Ok(())
        }
    }
}

/// 物理截断段文件到 `len` 并 fsync（tolerate_tail）。
fn truncate_segment(path: &Path, len: u64) -> std::io::Result<()> {
    let f = std::fs::OpenOptions::new().write(true).open(path)?;
    f.set_len(len)?;
    f.sync_data()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::segment::{seg_file_name, SegWriter};
    use std::io::{Seek as _, Write as _};

    const SEG_SIZE: u64 = 4096;

    fn data_payload(needle_id: u64, data_len: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(12 + data_len);
        v.extend_from_slice(&needle_id.to_le_bytes());
        v.extend_from_slice(&(data_len as u32).to_le_bytes());
        v.extend_from_slice(&vec![0xab; data_len]);
        v
    }

    fn delete_payload(needle_id: u64) -> Vec<u8> {
        let mut v = Vec::with_capacity(24);
        v.extend_from_slice(&needle_id.to_le_bytes());
        v.extend_from_slice(&1000i64.to_le_bytes());
        v.extend_from_slice(&8000i64.to_le_bytes());
        v
    }

    /// 依次填满段，写入 lsn 1..=total 的 DATA 记录（needle_id == lsn），
    /// 模拟滚动跨段写入。返回最后写入的 lsn。
    fn build_wal(dir: &Path, total: u64, data_len: usize) -> u64 {
        let mut mf = SegManifest::load(dir, SEG_SIZE).unwrap();
        let mut lsn = 1u64;
        loop {
            let seg_id = mf.next_seg_id();
            let mut w = SegWriter::create(
                &dir.join(seg_file_name(seg_id)),
                seg_id,
                1,
                lsn,
                SEG_SIZE,
                false,
            )
            .unwrap();
            let base = lsn;
            while lsn <= total {
                match w.append(RecordType::Data, 0, lsn, &data_payload(lsn, data_len)) {
                    Ok(_) => lsn += 1,
                    Err(SegmentError::SegmentFull { .. }) => break,
                    Err(e) => panic!("append failed: {e}"),
                }
            }
            w.seal().unwrap();
            drop(w);
            mf.register(seg_id, base, 1);
            if lsn > total {
                break;
            }
        }
        lsn - 1
    }

    #[test]
    fn replay_multi_segment_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        build_wal(dir.path(), 100, 64); // 3+ 段滚动
        let mf = SegManifest::load(dir.path(), SEG_SIZE).unwrap();
        assert!(mf.segments().len() > 1, "expect multiple segments");

        let r1 = replay_all(dir.path(), &mf, true).unwrap();
        assert_eq!(r1.index.needle_count(), 100);
        assert_eq!(r1.index.stats().used_bytes, 100 * 64);
        r1.index.assert_consistent();
        assert!(r1.truncated.is_empty());

        // 重放幂等：第二次重放结果与第一次完全一致。
        let r2 = replay_all(dir.path(), &mf, true).unwrap();
        assert_eq!(r1.index.stats(), r2.index.stats());
        assert_eq!(r1.index.max_needle_id(), r2.index.max_needle_id());
        assert_eq!(r1.index.last_lsn(), r2.index.last_lsn());
        for i in 1..=100u64 {
            assert_eq!(r1.index.lookup(i), r2.index.lookup(i));
        }
    }

    #[test]
    fn replay_delete_and_revive_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let mf_path = dir.path();
        let mut mf = SegManifest::load(mf_path, SEG_SIZE).unwrap();
        let seg_id = 1;
        let mut w = SegWriter::create(
            &mf_path.join(seg_file_name(seg_id)),
            seg_id,
            1,
            1,
            SEG_SIZE,
            false,
        )
        .unwrap();
        w.append(RecordType::Data, 0, 1, &data_payload(5, 100))
            .unwrap();
        w.append(RecordType::Delete, 0, 2, &delete_payload(5))
            .unwrap();
        w.append(RecordType::Data, 0, 3, &data_payload(5, 200))
            .unwrap(); // 复活
        w.append(RecordType::Data, 0, 4, &data_payload(6, 50))
            .unwrap();
        w.append(RecordType::Delete, 0, 5, &delete_payload(6))
            .unwrap(); // 保持删除
        w.seal().unwrap();
        drop(w);
        mf.register(seg_id, 1, 1);

        let r = replay_all(mf_path, &mf, true).unwrap();
        let idx = &r.index;
        // needle 5：复活后指向第三条 DATA。
        let e5 = idx.lookup(5).unwrap();
        assert_eq!(e5.data_len, 200);
        assert_eq!(e5.version_lsn, 3);
        // needle 6：tombstone。
        let tb6 = idx.tombstone_of(6).unwrap();
        assert_eq!(tb6.version_lsn, 5);
        assert_eq!(
            idx.stats(),
            crate::wal::index::IndexStats {
                used_bytes: 200,
                // needle5 覆写淘汰的 100（死副本账本）+ needle6 tombstone 的 50
                garbage_bytes: 150,
                active_count: 1,
                deleted_count: 1
            }
        );
        idx.assert_consistent();
        assert_eq!(idx.dead_copies().len(), 1);
        assert_eq!(idx.dead_copies()[0].data_len, 100);

        // restore 语义：tombstone 保留期内恢复（用第二次重放得到的
        // 独立 index 验证，等价于恢复路径）。
        let r2 = replay_all(mf_path, &mf, true).unwrap();
        let mut idx2 = r2.index;
        idx2.restore(6).unwrap();
        assert_eq!(idx2.lookup(6).unwrap().data_len, 50);
        idx2.assert_consistent();
    }

    #[test]
    fn tolerate_tail_truncates_last_segment() {
        let dir = tempfile::tempdir().unwrap();
        build_wal(dir.path(), 100, 64);
        let mf = SegManifest::load(dir.path(), SEG_SIZE).unwrap();
        let last = *mf.segments().last().unwrap();

        // 在最后一段尾部追加一个撕裂帧（模拟崩溃半写）。
        // 撕裂帧的 lsn 取超集值，反正帧不完整会被截断，不参与重放。
        let path = dir.path().join(seg_file_name(last.seg_id));
        let (mut w, _) = SegWriter::reopen(&path, SEG_SIZE).unwrap();
        let (bytes, _) = crate::wal::frame::encode_frame(
            w.last_crc(),
            RecordType::Data,
            0,
            100_000,
            &data_payload(999, 64),
        );
        let pos = w.write_pos();
        w.file_mut().seek(std::io::SeekFrom::Start(pos)).unwrap();
        w.file_mut().write_all(&bytes[..bytes.len() / 2]).unwrap();
        drop(w);

        // tolerate_tail=true：截断并重放成功，结果 == 完整帧集合的终态。
        let r = replay_all(dir.path(), &mf, true).unwrap();
        assert_eq!(r.truncated.get(&last.seg_id), Some(&pos));
        assert_eq!(r.index.lookup(999), None, "torn frame must not be visible");
        r.index.assert_consistent();

        // 截断后重放幂等（再跑一次不再报撕裂）。
        let mf2 = SegManifest::load(dir.path(), SEG_SIZE).unwrap();
        let r2 = replay_all(dir.path(), &mf2, true).unwrap();
        assert!(r2.truncated.is_empty());
        assert_eq!(r.index.stats(), r2.index.stats());

        // tolerate_tail=false：同样的撕裂 → 拒绝。
        let (mut w2, _) = SegWriter::reopen(&path, SEG_SIZE).unwrap();
        let (bytes2, _) = crate::wal::frame::encode_frame(
            w2.last_crc(),
            RecordType::Data,
            0,
            100_001,
            &data_payload(998, 64),
        );
        let pos2 = w2.write_pos();
        w2.file_mut().seek(std::io::SeekFrom::Start(pos2)).unwrap();
        w2.file_mut()
            .write_all(&bytes2[..bytes2.len() / 2])
            .unwrap();
        drop(w2);
        assert!(matches!(
            replay_all(dir.path(), &mf2, false),
            Err(ReplayError::TornNonTail { .. })
        ));
    }

    #[test]
    fn torn_non_tail_segment_rejected() {
        let dir = tempfile::tempdir().unwrap();
        build_wal(dir.path(), 40, 64); // 至少两段
        let mf = SegManifest::load(dir.path(), SEG_SIZE).unwrap();
        assert!(mf.segments().len() >= 2);
        let first = mf.segments()[0];
        let path = dir.path().join(seg_file_name(first.seg_id));

        // 在第一段（非最后段）尾部制造撕裂：先 reopen 拿到合法末尾，
        // 再把文件截短 10 字节切进最后一个完整帧。
        let (w, _) = SegWriter::reopen(&path, SEG_SIZE).unwrap();
        let valid = w.write_pos();
        drop(w);
        {
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(valid - 10).unwrap(); // 切进最后一个完整帧
            f.sync_all().unwrap();
        }

        assert!(matches!(
            replay_all(dir.path(), &mf, true),
            Err(ReplayError::TornNonTail { .. })
        ));
    }

    #[test]
    fn lsn_regression_detected() {
        let dir = tempfile::tempdir().unwrap();
        // 段 1：lsn 1..5；段 2：重复 lsn 1..5 → 回退。
        for (seg_id, base) in [(1u64, 1u64), (2, 1)] {
            let mut w = SegWriter::create(
                &dir.path().join(seg_file_name(seg_id)),
                seg_id,
                1,
                base,
                SEG_SIZE,
                false,
            )
            .unwrap();
            for i in 0..5u64 {
                w.append(RecordType::Data, 0, base + i, &data_payload(i, 32))
                    .unwrap();
            }
            w.seal().unwrap();
        }
        let mf = SegManifest::load(dir.path(), SEG_SIZE).unwrap();
        assert!(matches!(
            replay_all(dir.path(), &mf, true),
            Err(ReplayError::LsnRegression { .. })
        ));
    }

    #[test]
    fn ckpt_anchor_replayed() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegWriter::create(&dir.path().join(seg_file_name(1)), 1, 1, 1, SEG_SIZE, false)
            .unwrap();
        w.append(RecordType::Data, 0, 1, &data_payload(1, 16))
            .unwrap();
        let mut p = Vec::new();
        p.extend_from_slice(&7u64.to_le_bytes());
        p.extend_from_slice(&1u64.to_le_bytes());
        w.append(RecordType::CkptAnchor, 0, 2, &p).unwrap();
        w.seal().unwrap();
        drop(w);

        let mf = SegManifest::load(dir.path(), SEG_SIZE).unwrap();
        let r = replay_all(dir.path(), &mf, true).unwrap();
        assert_eq!(r.index.last_ckpt_anchor(), Some((7, 1)));
    }
}
