//! WAL 重放器（方案 §5 / 附录 B S3）。
//!
//! 按段序（seg_id 升序）扫描全部段，将记录应用到 [`WalIndex`]。恢复
//! 策略（P1，无 checkpoint）：
//! - 最后一个段的尾部撕裂（torn tail）→ 三档恢复模式均截断到最后一个
//!   完整帧（未完成写入，非校验失败）；
//! - 中部断链（非尾段撕裂 / CRC 失败 / LSN 回退 / payload 损坏）→
//!   Absolute 拒载，TolerateTail/PointInTime 截断停点段、弃置后继段，
//!   恢复结果等于"最后完整记录前缀"的终态（I1/I2）；
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

/// 恢复模式（方案 §10）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecoveryMode {
    /// 默认。最后段尾撕裂：截断到最后完整帧继续（crash 常态，静默处理）；
    /// 中部断链/损坏：告警 + 停在最后完整记录（动作同 [`RecoveryMode::PointInTime`]，
    /// 防止静默跳过造成状态机分叉）。
    #[default]
    TolerateTail,
    /// 有副本环境：中部损坏即停在最后完整记录，物理截断损坏段并弃置后继段，
    /// 交由副本修复。
    PointInTime,
    /// 审计/WORM：任何中部 CRC/哈希链/单调序校验失败即拒绝挂载；
    /// 最后段尾撕裂仍按未完成写入截断（不属于校验失败）。
    Absolute,
}

/// 中部断链停点：该段被截断到 `offset`，其后段全部弃置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopPoint {
    pub seg_id: u64,
    /// 最后一个完整可重放帧之后的字节偏移（段已物理截断到此）。
    pub offset: u64,
    pub reason: StopReason,
}

/// 中部断链原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// 非尾部段出现撕裂（帧头声明长度未完整落盘）。
    TornNonTail,
    /// CRC/哈希链校验失败。
    Corrupt,
    /// LSN 未单调递增（段集错乱/串段）。
    LsnRegression,
    /// 帧结构合法但 payload 语义非法（记录体损坏）。
    MalformedPayload,
}

/// 重放结果。
#[derive(Debug)]
pub struct ReplayResult {
    pub index: WalIndex,
    /// 重放帧总数（含 PAD）。
    pub frames_replayed: u64,
    /// 因 lsn ≤ skip_lsn 被跳过的帧数（checkpoint 前缀，CRC/链已校验）。
    pub frames_skipped: u64,
    /// 物理截断的段（seg_id → 截断位置）；含尾撕裂与中部断链停点。
    pub truncated: HashMap<u64, u64>,
    /// 中部断链停点（None = 全程完整）。
    pub stop: Option<StopPoint>,
    /// 停点之后被弃置的段（seg_id 升序；调用方负责删除段文件）。
    pub abandoned: Vec<u64>,
    /// 重放覆盖的段数（弃置前的清单段数）。
    pub segments_scanned: usize,
}

/// 对 `dir` 内 manifest 列出的全部段执行重放。
///
/// 尾撕裂（仅最后一段）在三档模式下都物理截断到最后一个完整帧
/// （fsync 截断结果），区别仅在调用方告警级别。中部断链按
/// [`RecoveryMode`] 处理：`Absolute` 直接报错拒载；其余两档截断损坏段
/// 到最后完整帧、停止扫描并返回弃置段清单。截断/停点动作幂等，
/// 重放可安全重复执行。
///
/// `index`：重放初态（checkpoint 装载路径传入 ckpt 索引，全量重放传空）。
/// `skip_lsn`：重放起点（方案 §10 步骤 4）——lsn ≤ skip_lsn 的记录由
/// checkpoint 索引承载，跳过 apply，但帧级 CRC/哈希链校验照常执行
/// （完整性检查不因加速而弱化）。全量重放传 0。
pub fn replay_all(
    dir: &Path,
    manifest: &SegManifest,
    index: WalIndex,
    mode: RecoveryMode,
    skip_lsn: u64,
) -> Result<ReplayResult, ReplayError> {
    replay_with(dir, manifest, index, mode, skip_lsn, None)
}

/// 带强制停点的重放（engine 在「ckpt 装载重放发现停点 → 丢弃 ckpt 全量
/// 重放」时使用：首轮已物理截断损坏段，全量重放无法再检出损坏，必须把
/// 首轮停点传入——扫到该段干净结束时即在该位置停扫，后续段一律弃置）。
pub(super) fn replay_with(
    dir: &Path,
    manifest: &SegManifest,
    index: WalIndex,
    mode: RecoveryMode,
    skip_lsn: u64,
    forced_stop: Option<StopPoint>,
) -> Result<ReplayResult, ReplayError> {
    let mut index = index;
    let mut frames_replayed = 0u64;
    let mut frames_skipped = 0u64;
    let mut truncated: HashMap<u64, u64> = HashMap::new();
    let mut stop: Option<StopPoint> = None;
    let segments = manifest.segments();
    let last_idx = segments.len().saturating_sub(1);

    'outer: for (i, entry) in segments.iter().enumerate() {
        let seg_id = entry.seg_id;
        let path = dir.join(crate::wal::segment::seg_file_name(seg_id));
        let mut reader =
            SegReader::open(&path).map_err(|source| ReplayError::Segment { seg_id, source })?;

        loop {
            match reader.next_frame() {
                Ok(Some(frame)) => {
                    // checkpoint 前缀跳过：CRC/链校验已在 next_frame 内完成，
                    // 仅跳过 apply（不更新 last_lsn，单调检查不适用）。
                    if frame.meta.rtype != RecordType::Pad && frame.meta.lsn <= skip_lsn {
                        frames_skipped += 1;
                        continue;
                    }
                    // LSN 跨段单调递增（PAD 不消耗 lsn 语义，仅跳过）。
                    if frame.meta.rtype != RecordType::Pad {
                        let lsn = frame.meta.lsn;
                        let prev = index.last_lsn();
                        if lsn <= prev {
                            if mode == RecoveryMode::Absolute {
                                return Err(ReplayError::LsnRegression {
                                    seg_id,
                                    prev,
                                    got: lsn,
                                });
                            }
                            // 停在越界帧之前（meta.offset 即帧起始）。
                            let cut = frame.meta.offset;
                            truncate_segment(&path, cut).map_err(|source| {
                                ReplayError::Segment {
                                    seg_id,
                                    source: SegmentError::Io {
                                        path: path.clone(),
                                        source,
                                    },
                                }
                            })?;
                            truncated.insert(seg_id, cut);
                            stop = Some(StopPoint {
                                seg_id,
                                offset: cut,
                                reason: StopReason::LsnRegression,
                            });
                            break 'outer;
                        }
                    }
                    if let Err(e) = apply_frame(&mut index, seg_id, &frame.meta, &frame.payload) {
                        // payload 语义损坏：Absolute 拒载；其余档停在该帧之前。
                        if matches!(e, ReplayError::MalformedPayload { .. })
                            && mode != RecoveryMode::Absolute
                        {
                            let cut = frame.meta.offset;
                            truncate_segment(&path, cut).map_err(|source| {
                                ReplayError::Segment {
                                    seg_id,
                                    source: SegmentError::Io {
                                        path: path.clone(),
                                        source,
                                    },
                                }
                            })?;
                            truncated.insert(seg_id, cut);
                            stop = Some(StopPoint {
                                seg_id,
                                offset: cut,
                                reason: StopReason::MalformedPayload,
                            });
                            break 'outer;
                        }
                        return Err(e);
                    }
                    frames_replayed += 1;
                }
                Ok(None) => {
                    // 强制停点（首轮 ckpt 重放已截断损坏段，本轮扫到该段
                    // 干净结束处即停，后续段由调用方弃置）。
                    if let Some(fs) = forced_stop {
                        if fs.seg_id == seg_id {
                            truncated.insert(seg_id, fs.offset);
                            stop = Some(fs);
                            break 'outer;
                        }
                    }
                    break;
                }
                Err(crate::wal::segment::ScanError::TornTail { .. }) => {
                    let is_tail = i == last_idx;
                    let valid_end = reader.valid_end();
                    if !is_tail && mode == RecoveryMode::Absolute {
                        return Err(ReplayError::TornNonTail {
                            seg_id,
                            offset: valid_end,
                        });
                    }
                    // 尾撕裂三档均截断（未完成写入，非校验失败）；
                    // 非尾撕裂在容忍档按中部断链处理。
                    truncate_segment(&path, valid_end).map_err(|source| ReplayError::Segment {
                        seg_id,
                        source: SegmentError::Io {
                            path: path.clone(),
                            source,
                        },
                    })?;
                    truncated.insert(seg_id, valid_end);
                    if !is_tail {
                        stop = Some(StopPoint {
                            seg_id,
                            offset: valid_end,
                            reason: StopReason::TornNonTail,
                        });
                        break 'outer;
                    }
                    break;
                }
                Err(crate::wal::segment::ScanError::Corrupt(e)) => {
                    let valid_end = reader.valid_end();
                    if mode == RecoveryMode::Absolute {
                        return Err(ReplayError::Corrupt {
                            seg_id,
                            offset: valid_end,
                            source: e,
                        });
                    }
                    // 中部损坏：截断损坏段到最后完整帧，停在该点。
                    truncate_segment(&path, valid_end).map_err(|source| ReplayError::Segment {
                        seg_id,
                        source: SegmentError::Io {
                            path: path.clone(),
                            source,
                        },
                    })?;
                    truncated.insert(seg_id, valid_end);
                    stop = Some(StopPoint {
                        seg_id,
                        offset: valid_end,
                        reason: StopReason::Corrupt,
                    });
                    break 'outer;
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

    // 停点之后的全部段弃置（seg_id 升序，供调用方删文件 + 清单移除）。
    let abandoned = match stop {
        Some(sp) => segments
            .iter()
            .filter(|e| e.seg_id > sp.seg_id)
            .map(|e| e.seg_id)
            .collect(),
        None => Vec::new(),
    };

    Ok(ReplayResult {
        index,
        frames_replayed,
        frames_skipped,
        truncated,
        stop,
        abandoned,
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
        RecordType::GcMigrate => {
            // 条件应用（缺席重建 / 版本匹配覆写 / 其余跳过；purge 压制），
            // 与在线路径同语义，见 WalIndex::apply_gc_migrate。
            let now = chrono::Utc::now().timestamp();
            index
                .apply_gc_migrate(seg_id, meta.offset, meta.crc, meta.lsn, payload, now)
                .map_err(|_| ReplayError::MalformedPayload {
                    seg_id,
                    lsn: meta.lsn,
                    rtype: meta.rtype.to_u8(),
                })
                .map(|_| ())
        }
        RecordType::TombPurge => {
            // 持久化 purge 标记：搬账本 + 登记压制（防孤儿副本复活）。
            index
                .apply_tomb_purge(meta.lsn, payload)
                .map_err(|_| ReplayError::MalformedPayload {
                    seg_id,
                    lsn: meta.lsn,
                    rtype: meta.rtype.to_u8(),
                })
        }
        // ATTR / SNAP_TAKE / SNAP_DROP：payload 语义在 P2+ 接入快照与
        // 属性功能时消费；当前仅要求帧级完整性（CRC/链已校验）。
        RecordType::Attr | RecordType::SnapTake | RecordType::SnapDrop => {
            log::debug!(
                "replay: payload of rtype {:#04x} at lsn {} accepted (placeholder)",
                meta.rtype.to_u8(),
                meta.lsn
            );
            Ok(())
        }
        RecordType::VolumeMeta => {
            // 卷元数据（§11.1 容量伸缩）：推进索引内 volume_size。
            index.apply_volume_meta(meta.lsn, payload).map_err(|_| {
                ReplayError::MalformedPayload {
                    seg_id,
                    lsn: meta.lsn,
                    rtype: meta.rtype.to_u8(),
                }
            })?;
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
    use crate::wal::segment::{seg_file_name, SegWriter, SEG_HEADER_SIZE};
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

        let r1 = replay_all(
            dir.path(),
            &mf,
            WalIndex::new(),
            RecoveryMode::TolerateTail,
            0,
        )
        .unwrap();
        assert_eq!(r1.index.needle_count(), 100);
        assert_eq!(r1.index.stats().used_bytes, 100 * 64);
        r1.index.assert_consistent();
        assert!(r1.truncated.is_empty());

        // 重放幂等：第二次重放结果与第一次完全一致。
        let r2 = replay_all(
            dir.path(),
            &mf,
            WalIndex::new(),
            RecoveryMode::TolerateTail,
            0,
        )
        .unwrap();
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

        let r = replay_all(mf_path, &mf, WalIndex::new(), RecoveryMode::TolerateTail, 0).unwrap();
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
                // needle5 覆写淘汰的 100 进 garbage；needle6 tombstone 的 50 进 staging
                staging_bytes: 50,
                garbage_bytes: 100,
                pinned_bytes: 0,
                active_count: 1,
                deleted_count: 1
            }
        );
        idx.assert_consistent();
        assert_eq!(idx.dead_copies().len(), 1);
        assert_eq!(idx.dead_copies()[0].data_len, 100);

        // restore 语义：tombstone 保留期内恢复（用第二次重放得到的
        // 独立 index 验证，等价于恢复路径）。
        let r2 = replay_all(mf_path, &mf, WalIndex::new(), RecoveryMode::TolerateTail, 0).unwrap();
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
        let r = replay_all(
            dir.path(),
            &mf,
            WalIndex::new(),
            RecoveryMode::TolerateTail,
            0,
        )
        .unwrap();
        assert_eq!(r.truncated.get(&last.seg_id), Some(&pos));
        assert_eq!(r.index.lookup(999), None, "torn frame must not be visible");
        r.index.assert_consistent();

        // 截断后重放幂等（再跑一次不再报撕裂）。
        let mf2 = SegManifest::load(dir.path(), SEG_SIZE).unwrap();
        let r2 = replay_all(
            dir.path(),
            &mf2,
            WalIndex::new(),
            RecoveryMode::TolerateTail,
            0,
        )
        .unwrap();
        assert!(r2.truncated.is_empty());
        assert_eq!(r.index.stats(), r2.index.stats());

        // Absolute：尾撕裂同样截断（未完成写入不属于校验失败）。
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
        let r3 = replay_all(dir.path(), &mf2, WalIndex::new(), RecoveryMode::Absolute, 0).unwrap();
        assert_eq!(r3.truncated.get(&last.seg_id), Some(&pos2));
        assert!(r3.stop.is_none());
        assert_eq!(r3.index.lookup(998), None);
    }

    #[test]
    fn torn_non_tail_matrix() {
        // 每档独立布局：容忍档会物理截断损坏段，不能跨模式复用。
        let setup = |lsns: u64| {
            let dir = tempfile::tempdir().unwrap();
            build_wal(dir.path(), lsns, 64); // 至少两段
            let mf = SegManifest::load(dir.path(), SEG_SIZE).unwrap();
            assert!(mf.segments().len() >= 2);
            let first = mf.segments()[0];
            let path = dir.path().join(seg_file_name(first.seg_id));
            // 在第一段（非最后段）尾部切进最后一个完整帧制造撕裂。
            let (w, _) = SegWriter::reopen(&path, SEG_SIZE).unwrap();
            let valid = w.write_pos();
            drop(w);
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(valid - 10).unwrap();
            f.sync_all().unwrap();
            (dir, mf, first, valid)
        };

        // Absolute：中部断链拒载（不截断）。
        let (dir, mf, first, valid) = setup(40);
        assert!(matches!(
            replay_all(dir.path(), &mf, WalIndex::new(), RecoveryMode::Absolute, 0),
            Err(ReplayError::TornNonTail { .. })
        ));
        let meta = std::fs::metadata(dir.path().join(seg_file_name(first.seg_id))).unwrap();
        assert_eq!(meta.len(), valid - 10, "absolute must not truncate");

        // TolerateTail / PointInTime：停在撕裂帧之前（帧边界），截断损坏段
        // 并列出弃置后继段；只有连续前缀的 needle 进入索引。
        for mode in [RecoveryMode::TolerateTail, RecoveryMode::PointInTime] {
            let (dir, mf, first, _valid) = setup(40);
            let r = replay_all(dir.path(), &mf, WalIndex::new(), mode, 0).unwrap();
            let sp = r.stop.expect("mode must stop mid-chain");
            assert_eq!(sp.seg_id, first.seg_id);
            assert_eq!(sp.reason, StopReason::TornNonTail);
            assert!(sp.offset < 4096);
            assert!(!r.abandoned.is_empty());
            assert!(r.abandoned.iter().all(|&id| id > first.seg_id));
            let max_lsn = r.index.last_lsn();
            assert!(max_lsn < 40);
            assert_eq!(
                r.index.needle_count() as u64,
                max_lsn,
                "only contiguous prefix needles present"
            );
        }
    }

    #[test]
    fn lsn_regression_matrix() {
        let setup = || {
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
            (dir, mf)
        };

        // Absolute：回退即拒载。
        let (dir, mf) = setup();
        assert!(matches!(
            replay_all(dir.path(), &mf, WalIndex::new(), RecoveryMode::Absolute, 0),
            Err(ReplayError::LsnRegression { .. })
        ));

        // 容忍档：停在越界帧之前（段 2 零记录应用），段 2 弃置。
        for mode in [RecoveryMode::TolerateTail, RecoveryMode::PointInTime] {
            let (dir, mf) = setup();
            let r = replay_all(dir.path(), &mf, WalIndex::new(), mode, 0).unwrap();
            let sp = r.stop.expect("must stop");
            assert_eq!(sp.reason, StopReason::LsnRegression);
            assert_eq!(sp.seg_id, 2);
            assert_eq!(sp.offset, SEG_HEADER_SIZE as u64, "cut before frame 1");
            assert_eq!(r.abandoned, Vec::<u64>::new(), "seg 2 is the last");
            assert_eq!(r.index.last_lsn(), 5);
        }
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
        let r = replay_all(
            dir.path(),
            &mf,
            WalIndex::new(),
            RecoveryMode::TolerateTail,
            0,
        )
        .unwrap();
        assert_eq!(r.index.last_ckpt_anchor(), Some((7, 1)));
    }
}
