//! WAL 内存索引（方案 §4.3 / §4.4）。
//!
//! `WalIndex` 是重放终态的内存表达：活跃 needle 索引 + tombstone 区 +
//! 空间统计。统计与索引内容必须严格一致（不变量 I4 的重放侧），每次
//! apply 都同步维护统计。
//!
//! 重放幂等性：apply 系列以 `version_lsn` 做单调性防护——到达记录的
//! lsn 不大于该 id 当前版本 lsn 时跳过，因此对同一 index 重复应用同一
//! 批记录（或乱序重放）结果不变。

use std::collections::HashMap;

use crate::wal::frame::FRAME_HEADER_SIZE;

/// 活跃 needle 索引条目。
///
/// `offset` 为 payload 区起点（帧起始 + 26B 帧头），`read(seg_id, offset,
/// data_len)` 可直接定位数据；`crc` 为覆盖整帧（type|flags|lsn|payload）
/// 的帧 CRC，读路径校验时重算比对。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeedleEntry {
    pub needle_id: u64,
    pub seg_id: u64,
    pub offset: u64,
    pub data_len: u32,
    pub crc: u32,
    pub version_lsn: u64,
    pub created_at: i64,
    pub flags: u32,
}

/// tombstone 条目（已删除，保留期内可 restore）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TombstoneEntry {
    pub needle_id: u64,
    pub seg_id: u64,
    pub offset: u64,
    pub data_len: u32,
    pub crc: u32,
    /// 产生该 tombstone 的 DELETE 记录 lsn（幂等防护 + 与 DATA 比新旧）。
    pub version_lsn: u64,
    pub deleted_at: i64,
    pub retention_until: i64,
}

/// 索引统计（与索引内容严格一致，I4；四项空间统计 §9.3 的重放侧）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IndexStats {
    /// 活跃 needle 数据字节之和。
    pub used_bytes: u64,
    /// tombstone 保留期内的物理字节（可 restore，purge 后转 garbage）。
    pub staging_bytes: u64,
    /// 被覆写/复活/purge 淘汰的旧版本数据字节之和（GC 待回收）。
    pub garbage_bytes: u64,
    /// 被活跃快照钉住的旧版本字节（P3 快照接入，本阶段恒 0）。
    pub pinned_bytes: u64,
    pub active_count: u64,
    pub deleted_count: u64,
}

/// 死副本：被覆写/复活淘汰的旧版本物理位置（P1 仅记账，P2 GC 消费）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadCopy {
    pub seg_id: u64,
    /// payload 区起点（与 [`NeedleEntry::offset`] 同语义）。
    pub offset: u64,
    pub data_len: u32,
    pub crc: u32,
    pub version_lsn: u64,
}

/// 重放/应用错误。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IndexError {
    #[error("payload too short for {op}: got {got} bytes")]
    ShortPayload { op: &'static str, got: usize },
}

/// WAL 内存索引。
#[derive(Debug, Default)]
pub struct WalIndex {
    needles: HashMap<u64, NeedleEntry>,
    tombstones: HashMap<u64, TombstoneEntry>,
    /// 被覆写/复活淘汰的旧版本死副本（garbage 的可核对账本）。
    dead_copies: Vec<DeadCopy>,
    stats: IndexStats,
    /// 已见最大非 PAD 记录 lsn。
    last_lsn: u64,
    /// 已见最大 needle_id（引擎 open 后 +1 作为 next_needle_id）。
    max_needle_id: u64,
    /// 最近 CKPT_ANCHOR（P1 留位，仅记录）。
    last_ckpt_anchor: Option<(u64, u64)>,
}

impl WalIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn stats(&self) -> IndexStats {
        self.stats
    }

    pub fn last_lsn(&self) -> u64 {
        self.last_lsn
    }

    pub fn max_needle_id(&self) -> u64 {
        self.max_needle_id
    }

    pub fn needle_count(&self) -> u64 {
        self.needles.len() as u64
    }

    pub fn tombstone_count(&self) -> u64 {
        self.tombstones.len() as u64
    }

    pub fn lookup(&self, needle_id: u64) -> Option<&NeedleEntry> {
        self.needles.get(&needle_id)
    }

    pub fn tombstone_of(&self, needle_id: u64) -> Option<&TombstoneEntry> {
        self.tombstones.get(&needle_id)
    }

    pub fn needles(&self) -> impl Iterator<Item = &NeedleEntry> {
        self.needles.values()
    }

    pub fn tombstones(&self) -> impl Iterator<Item = &TombstoneEntry> {
        self.tombstones.values()
    }

    /// 死副本清单（覆写/复活淘汰的旧版本，P2 GC 回收依据）。
    pub fn dead_copies(&self) -> &[DeadCopy] {
        &self.dead_copies
    }

    pub fn last_ckpt_anchor(&self) -> Option<(u64, u64)> {
        self.last_ckpt_anchor
    }

    /// 应用一条 DATA 记录（payload = `needle_id u64 | data_len u32 | data`）。
    ///
    /// 语义（按 LSN 顺序应用最后一态）：
    /// - 新 id → 建立活跃条目；
    /// - 覆写活跃 id（更高 lsn）→ 新条目替换，旧版本计入垃圾；
    /// - 活跃 tombstone 之后的 DATA → restore：tombstone 转活跃；
    /// - lsn 不大于当前版本 → 跳过（幂等防护）。
    pub fn apply_data(
        &mut self,
        seg_id: u64,
        frame_offset: u64,
        frame_crc: u32,
        lsn: u64,
        payload: &[u8],
        now: i64,
    ) -> Result<(), IndexError> {
        if payload.len() < 12 {
            return Err(IndexError::ShortPayload {
                op: "DATA",
                got: payload.len(),
            });
        }
        let needle_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
        let data_len = u32::from_le_bytes(payload[8..12].try_into().unwrap()) as u64;
        if payload.len() != 12 + data_len as usize {
            return Err(IndexError::ShortPayload {
                op: "DATA",
                got: payload.len(),
            });
        }

        if needle_id > self.max_needle_id {
            self.max_needle_id = needle_id;
        }
        if lsn > self.last_lsn {
            self.last_lsn = lsn;
        }

        // 幂等防护：lsn 不高于当前版本时跳过。
        if let Some(cur) = self.needles.get(&needle_id) {
            if lsn <= cur.version_lsn {
                return Ok(());
            }
        }
        if let Some(tb) = self.tombstones.get(&needle_id) {
            if lsn <= tb.version_lsn {
                return Ok(());
            }
        }

        let entry = NeedleEntry {
            needle_id,
            seg_id,
            offset: frame_offset + FRAME_HEADER_SIZE as u64,
            data_len: data_len as u32,
            crc: frame_crc,
            version_lsn: lsn,
            created_at: now,
            flags: 0,
        };

        // tombstone → revive（复活）语义：tombstone 引用的旧副本转为死
        // 副本账本（staging 迁出、garbage 迁入），新 DATA 副本成为活跃版本。
        if let Some(tb) = self.tombstones.remove(&needle_id) {
            self.dead_copies.push(DeadCopy {
                seg_id: tb.seg_id,
                offset: tb.offset,
                data_len: tb.data_len,
                crc: tb.crc,
                version_lsn: tb.version_lsn,
            });
            self.stats.deleted_count -= 1;
            self.stats.staging_bytes -= tb.data_len as u64;
            self.stats.garbage_bytes += tb.data_len as u64;
            self.stats.used_bytes += data_len;
            self.stats.active_count += 1;
            self.needles.insert(needle_id, entry);
            return Ok(());
        }

        match self.needles.insert(needle_id, entry) {
            None => {
                // 新条目。
                self.stats.used_bytes += data_len;
                self.stats.active_count += 1;
            }
            Some(old) => {
                // 覆写：旧副本转死副本账本，used 调整差值。
                self.dead_copies.push(DeadCopy {
                    seg_id: old.seg_id,
                    offset: old.offset,
                    data_len: old.data_len,
                    crc: old.crc,
                    version_lsn: old.version_lsn,
                });
                self.stats.garbage_bytes += old.data_len as u64;
                self.stats.used_bytes = self
                    .stats
                    .used_bytes
                    .saturating_sub(old.data_len as u64)
                    .saturating_add(data_len);
            }
        }
        Ok(())
    }

    /// 应用一条 DELETE 记录（tombstone，保留期内可 restore）。
    pub fn apply_delete(&mut self, frame_lsn: u64, payload: &[u8]) -> Result<(), IndexError> {
        if payload.len() != 24 {
            return Err(IndexError::ShortPayload {
                op: "DELETE",
                got: payload.len(),
            });
        }
        let needle_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
        let deleted_at = i64::from_le_bytes(payload[8..16].try_into().unwrap());
        let retention_until = i64::from_le_bytes(payload[16..24].try_into().unwrap());

        if frame_lsn > self.last_lsn {
            self.last_lsn = frame_lsn;
        }

        // 幂等防护。
        if let Some(tb) = self.tombstones.get(&needle_id) {
            if frame_lsn <= tb.version_lsn {
                return Ok(());
            }
        }
        if let Some(cur) = self.needles.get(&needle_id) {
            if frame_lsn <= cur.version_lsn {
                return Ok(());
            }
        }

        // DELETE 作用于不存在的 id：忽略（LSN 顺序保证 DELETE 前必有
        // DATA；async 窗口内两者同组提交同生共死，不会出现孤儿 DELETE）。
        let Some(active) = self.needles.remove(&needle_id) else {
            return Ok(());
        };

        self.stats.used_bytes -= active.data_len as u64;
        self.stats.active_count -= 1;
        self.stats.staging_bytes += active.data_len as u64;
        self.stats.deleted_count += 1;
        self.tombstones.insert(
            needle_id,
            TombstoneEntry {
                needle_id,
                seg_id: active.seg_id,
                offset: active.offset,
                data_len: active.data_len,
                crc: active.crc,
                version_lsn: frame_lsn,
                deleted_at,
                retention_until,
            },
        );
        Ok(())
    }

    /// 记录 CKPT_ANCHOR（P1 留位）。
    pub fn apply_ckpt_anchor(&mut self, ckpt_seq: u64, applied_lsn: u64, frame_lsn: u64) {
        if frame_lsn > self.last_lsn {
            self.last_lsn = frame_lsn;
        }
        if self
            .last_ckpt_anchor
            .map(|(_, l)| frame_lsn > l)
            .unwrap_or(true)
        {
            self.last_ckpt_anchor = Some((ckpt_seq, applied_lsn));
        }
    }

    /// 推进重放游标到 checkpoint 的 applied_lsn（恢复路径装配）。
    ///
    /// checkpoint 条目的 version_lsn 都 ≤ applied_lsn，load_from 推导的
    /// last_lsn 可能小于 applied_lsn（例如只有 CKPT_ANCHOR 在其后）；
    /// 恢复重放以 applied_lsn 为起点，跳过前缀后从 applied_lsn+1 继续。
    pub fn advance_replay_cursor(&mut self, applied_lsn: u64) {
        self.last_lsn = self.last_lsn.max(applied_lsn);
    }

    /// 保留期内 restore：tombstone 转活跃（引用原位置数据，不搬移）。
    pub fn restore(&mut self, needle_id: u64) -> Result<(), IndexError> {
        let Some(tb) = self.tombstones.remove(&needle_id) else {
            return Ok(()); // 已活跃或不存在：幂等
        };
        self.stats.deleted_count -= 1;
        self.stats.staging_bytes -= tb.data_len as u64;
        self.stats.used_bytes += tb.data_len as u64;
        self.stats.active_count += 1;
        self.needles.insert(
            needle_id,
            NeedleEntry {
                needle_id: tb.needle_id,
                seg_id: tb.seg_id,
                offset: tb.offset,
                data_len: tb.data_len,
                crc: tb.crc,
                version_lsn: tb.version_lsn,
                created_at: tb.deleted_at,
                flags: 0,
            },
        );
        Ok(())
    }

    /// purge 已过保留期的 tombstone（case C）：物理副本迁入死副本账本
    /// （staging → garbage），此后该副本只等 GC 按段回收。返回 purge 数。
    pub fn purge_expired(&mut self, now: i64) -> usize {
        let expired: Vec<u64> = self
            .tombstones
            .values()
            .filter(|tb| tb.retention_until <= now)
            .map(|tb| tb.needle_id)
            .collect();
        for needle_id in &expired {
            if let Some(tb) = self.tombstones.remove(needle_id) {
                self.dead_copies.push(DeadCopy {
                    seg_id: tb.seg_id,
                    offset: tb.offset,
                    data_len: tb.data_len,
                    crc: tb.crc,
                    version_lsn: tb.version_lsn,
                });
                self.stats.deleted_count -= 1;
                self.stats.staging_bytes -= tb.data_len as u64;
                self.stats.garbage_bytes += tb.data_len as u64;
            }
        }
        expired.len()
    }

    /// 校验统计与索引内容严格一致（I4）。
    ///
    /// garbage_bytes == 死副本账本字节之和；staging_bytes == tombstone
    /// 字节之和，逐字节核对。
    pub fn assert_consistent(&self) {
        let used: u64 = self.needles.values().map(|e| e.data_len as u64).sum();
        let dead_list: u64 = self.dead_copies.iter().map(|d| d.data_len as u64).sum();
        let dead_tomb: u64 = self.tombstones.values().map(|e| e.data_len as u64).sum();
        assert_eq!(self.stats.used_bytes, used, "used_bytes mismatch");
        assert_eq!(
            self.stats.garbage_bytes, dead_list,
            "garbage mismatch (dead copy ledger)"
        );
        assert_eq!(
            self.stats.staging_bytes, dead_tomb,
            "staging mismatch (tombstones)"
        );
        assert_eq!(self.stats.active_count, self.needles.len() as u64);
        assert_eq!(self.stats.deleted_count, self.tombstones.len() as u64);
    }

    /// 从 checkpoint 条目重建索引（恢复路径装配，统计同步重算）。
    ///
    /// 重放游标（last_lsn/max_needle_id）从条目 version_lsn / needle_id
    /// 推导；CKPT_ANCHOR 等 lsn 高于全部条目的记录由重放侧补齐。
    pub fn load_from(
        &mut self,
        needles: impl Iterator<Item = NeedleEntry>,
        tombstones: impl Iterator<Item = TombstoneEntry>,
        dead: impl Iterator<Item = DeadCopy>,
    ) {
        for n in needles {
            self.max_needle_id = self.max_needle_id.max(n.needle_id);
            self.last_lsn = self.last_lsn.max(n.version_lsn);
            self.stats.used_bytes += n.data_len as u64;
            self.stats.active_count += 1;
            self.needles.insert(n.needle_id, n);
        }
        for t in tombstones {
            self.last_lsn = self.last_lsn.max(t.version_lsn);
            self.stats.staging_bytes += t.data_len as u64;
            self.stats.deleted_count += 1;
            self.tombstones.insert(t.needle_id, t);
        }
        for d in dead {
            self.stats.garbage_bytes += d.data_len as u64;
            self.dead_copies.push(d);
        }
    }

    /// 冻结索引快照（checkpoint 写入用；P2 简化为锁内克隆）。
    ///
    /// 返回 (needles, tombstones, dead 账本, 统计)。统计由调用方按过滤
    /// 后的条目重算时忽略本统计（checkpoint 路径）。
    pub fn clone_state(&self) -> (Vec<NeedleEntry>, Vec<TombstoneEntry>, Vec<DeadCopy>) {
        (
            self.needles.values().cloned().collect(),
            self.tombstones.values().cloned().collect(),
            self.dead_copies.clone(),
        )
    }
}

/// DATA 记录的头部解析（不复制数据本体，重放路径零拷贝）。
pub fn parse_data_header(payload: &[u8]) -> Option<(u64, u32)> {
    if payload.len() < 12
        || payload.len() != 12 + u32::from_le_bytes(payload[8..12].try_into().unwrap()) as usize
    {
        return None;
    }
    Some((
        u64::from_le_bytes(payload[0..8].try_into().unwrap()),
        u32::from_le_bytes(payload[8..12].try_into().unwrap()),
    ))
}

// RT_DATA 语义见 frame 模块；此处不重复引入。

#[cfg(test)]
mod tests {
    use super::*;

    fn data_payload(needle_id: u64, data_len: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(12 + data_len);
        v.extend_from_slice(&needle_id.to_le_bytes());
        v.extend_from_slice(&(data_len as u32).to_le_bytes());
        v.extend_from_slice(&vec![0u8; data_len]);
        v
    }

    fn delete_payload(needle_id: u64, deleted_at: i64, retention_until: i64) -> Vec<u8> {
        let mut v = Vec::with_capacity(24);
        v.extend_from_slice(&needle_id.to_le_bytes());
        v.extend_from_slice(&deleted_at.to_le_bytes());
        v.extend_from_slice(&retention_until.to_le_bytes());
        v
    }

    #[test]
    fn data_apply_and_overwrite() {
        let mut idx = WalIndex::new();
        idx.apply_data(1, 68, 0xaaaa, 1, &data_payload(10, 100), 1000)
            .unwrap();
        let e = idx.lookup(10).unwrap();
        assert_eq!(e.seg_id, 1);
        assert_eq!(e.offset, 68 + 26);
        assert_eq!(e.data_len, 100);
        assert_eq!(e.version_lsn, 1);
        assert_eq!(
            idx.stats(),
            IndexStats {
                used_bytes: 100,
                staging_bytes: 0,
                garbage_bytes: 0,
                pinned_bytes: 0,
                active_count: 1,
                deleted_count: 0
            }
        );

        // 覆写：旧版本计垃圾。
        idx.apply_data(1, 300, 0xbbbb, 2, &data_payload(10, 150), 1001)
            .unwrap();
        let e = idx.lookup(10).unwrap();
        assert_eq!(e.offset, 300 + 26);
        assert_eq!(e.version_lsn, 2);
        assert_eq!(
            idx.stats(),
            IndexStats {
                used_bytes: 150,
                staging_bytes: 0,
                garbage_bytes: 100,
                pinned_bytes: 0,
                active_count: 1,
                deleted_count: 0
            }
        );
        idx.assert_consistent();
    }

    #[test]
    fn stale_lsn_skipped_idempotent() {
        let mut idx = WalIndex::new();
        idx.apply_data(1, 68, 1, 5, &data_payload(1, 50), 0)
            .unwrap();
        let snap = idx.stats();

        // 旧 lsn 重放（乱序/重复）→ 跳过。
        idx.apply_data(1, 999, 2, 3, &data_payload(1, 80), 0)
            .unwrap();
        assert_eq!(idx.lookup(1).unwrap().offset, 68 + 26);
        assert_eq!(idx.stats(), snap);

        // 同 lsn 重复 DATA → 跳过（重放两次结果一致）。
        idx.apply_data(1, 68, 1, 5, &data_payload(1, 50), 0)
            .unwrap();
        assert_eq!(idx.stats(), snap);
    }

    #[test]
    fn delete_then_restore() {
        let mut idx = WalIndex::new();
        idx.apply_data(2, 68, 1, 1, &data_payload(7, 200), 100)
            .unwrap();

        idx.apply_delete(2, &delete_payload(7, 500, 500 + 7 * 86400))
            .unwrap();
        assert!(idx.lookup(7).is_none());
        let tb = idx.tombstone_of(7).unwrap();
        assert_eq!(tb.version_lsn, 2);
        assert_eq!(tb.data_len, 200);
        assert_eq!(
            idx.stats(),
            IndexStats {
                used_bytes: 0,
                staging_bytes: 200,
                garbage_bytes: 0,
                pinned_bytes: 0,
                active_count: 0,
                deleted_count: 1
            }
        );
        idx.assert_consistent();

        // 保留期内 restore：引用原位置，不搬移。
        idx.restore(7).unwrap();
        let e = idx.lookup(7).unwrap();
        assert_eq!(e.seg_id, 2);
        assert_eq!(e.offset, 68 + 26);
        assert_eq!(
            idx.stats(),
            IndexStats {
                used_bytes: 200,
                staging_bytes: 0,
                garbage_bytes: 0,
                pinned_bytes: 0,
                active_count: 1,
                deleted_count: 0
            }
        );
        idx.assert_consistent();

        // restore 幂等。
        idx.restore(7).unwrap();
        assert_eq!(idx.stats().used_bytes, 200);
    }

    /// purge 过期 tombstone：staging → garbage（死副本账本），未过期不动。
    #[test]
    fn purge_expired_moves_staging_to_garbage() {
        let mut idx = WalIndex::new();
        // needle 1：deleted_at=100，保留到 100+86400；needle 2：已过期。
        idx.apply_data(1, 68, 1, 1, &data_payload(1, 100), 0).unwrap();
        idx.apply_data(1, 68, 2, 2, &data_payload(2, 50), 0).unwrap();
        idx.apply_delete(3, &delete_payload(1, 100, 100 + 86400))
            .unwrap();
        idx.apply_delete(4, &delete_payload(2, 100, 50))
            .unwrap();
        assert_eq!(idx.stats().staging_bytes, 150);
        assert_eq!(idx.stats().garbage_bytes, 0);

        let now = 200i64; // needle 2 过期（retention_until=50），needle 1 未过期
        let purged = idx.purge_expired(now);
        assert_eq!(purged, 1);
        assert_eq!(
            idx.stats(),
            IndexStats {
                used_bytes: 0,
                staging_bytes: 100,
                garbage_bytes: 50,
                pinned_bytes: 0,
                active_count: 0,
                deleted_count: 1
            }
        );
        assert!(idx.tombstone_of(1).is_some(), "未过期 tombstone 保留");
        assert!(idx.tombstone_of(2).is_none());
        assert_eq!(idx.dead_copies().len(), 1);
        assert_eq!(idx.dead_copies()[0].data_len, 50);
        idx.assert_consistent();

        // 保留期过后再 purge：needle 1 迁出。
        assert_eq!(idx.purge_expired(100 + 86400 + 1), 1);
        assert_eq!(idx.stats().staging_bytes, 0);
        assert_eq!(idx.stats().garbage_bytes, 150);
        assert_eq!(idx.stats().deleted_count, 0);
        idx.assert_consistent();

        // 幂等：无过期 tombstone 时返回 0。
        assert_eq!(idx.purge_expired(999_999), 0);
    }

    #[test]
    fn data_after_delete_revives() {
        let mut idx = WalIndex::new();
        idx.apply_data(1, 68, 1, 1, &data_payload(3, 40), 0)
            .unwrap();
        idx.apply_delete(2, &delete_payload(3, 10, 20)).unwrap();
        assert_eq!(idx.tombstone_count(), 1);

        // DELETE 之后的 DATA（新位置）→ 复活；旧副本留在死副本账本。
        idx.apply_data(4, 500, 3, 3, &data_payload(3, 60), 0)
            .unwrap();
        assert!(idx.tombstone_of(3).is_none());
        let e = idx.lookup(3).unwrap();
        assert_eq!(e.seg_id, 4);
        assert_eq!(e.data_len, 60);
        assert_eq!(idx.dead_copies().len(), 1);
        assert_eq!(idx.dead_copies()[0].data_len, 40);
        assert_eq!(
            idx.stats(),
            IndexStats {
                used_bytes: 60,
                staging_bytes: 0,
                garbage_bytes: 40,
                pinned_bytes: 0,
                active_count: 1,
                deleted_count: 0
            }
        );
        idx.assert_consistent();
    }

    #[test]
    fn delete_unknown_id_ignored() {
        let mut idx = WalIndex::new();
        idx.apply_delete(1, &delete_payload(99, 1, 2)).unwrap();
        assert_eq!(idx.stats(), IndexStats::default());
        assert_eq!(idx.last_lsn(), 1);
    }

    #[test]
    fn malformed_payload_rejected() {
        let mut idx = WalIndex::new();
        assert!(matches!(
            idx.apply_data(1, 0, 0, 1, &[0u8; 5], 0),
            Err(IndexError::ShortPayload { .. })
        ));
        // data_len 与实际不符。
        assert!(matches!(
            idx.apply_data(1, 0, 0, 1, &[0u8; 11], 0),
            Err(IndexError::ShortPayload { .. })
        ));
        assert!(matches!(
            idx.apply_delete(1, &[0u8; 23]),
            Err(IndexError::ShortPayload { .. })
        ));
    }

    #[test]
    fn max_needle_and_last_lsn_tracked() {
        let mut idx = WalIndex::new();
        idx.apply_data(1, 0, 0, 3, &data_payload(9, 10), 0).unwrap();
        idx.apply_data(1, 0, 0, 7, &data_payload(100, 10), 0)
            .unwrap();
        assert_eq!(idx.max_needle_id(), 100);
        assert_eq!(idx.last_lsn(), 7);
    }
}
