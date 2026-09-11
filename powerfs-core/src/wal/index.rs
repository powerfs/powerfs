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

use crate::wal::frame::{VolumeMetaPayload, FRAME_HEADER_SIZE};

/// needle 条目 flags：条目指向 GC_MIGRATE 帧（payload 形态
/// `needle_id|expect_version|data_len|data`，20B 头；读路径据此分派帧
/// 解析与 CRC 校验种类，见 `WalEngine::read`）。
pub const NEEDLE_FLAG_MIGRATED: u32 = 0x01;

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
    /// tombstone purge 标记（needle_id → purge 记录 lsn）。重放/应用时
    /// 压制该 needle 一切 lsn ≤ purge_lsn 的记录（孤儿副本防复活，§8
    /// case C）；checkpoint 持久化，GC 在更老段全部回收后修剪。
    purged: HashMap<u64, u64>,
    /// 卷逻辑容量（字节，§11；由 VOLUME_META 记录推进，checkpoint
    /// 持久化；0 = 未设置，引擎启动时回退 superblock/config 基线）。
    volume_size: u64,
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

        // purge 压制：该 needle 已被持久化删除标记覆盖（lsn ≤ purge_lsn
        // 的记录在重放时是无主孤儿，§8 case C）。更新的 DATA 是删除后
        // 的全新写入——移除标记、正常建条目（新世代）。
        if let Some(purge_lsn) = self.purged.get(&needle_id).copied() {
            if lsn <= purge_lsn {
                return Ok(());
            }
            self.purged.remove(&needle_id);
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

    /// 应用一条 GC 搬移记录（GC_MIGRATE，条件应用）。
    ///
    /// payload = `needle_id u64 | expect_version u64 | data_len u32 | data`。
    /// 仅当目标 needle 当前版本 lsn == expect_version 时应用（覆写路径，
    /// 旧版本转死副本账本——原段物理副本自此只等 GC 回收）；否则跳过。
    /// 无论应用与否都推进 last_lsn（重放游标单调）。返回是否应用。
    pub fn apply_gc_migrate(
        &mut self,
        seg_id: u64,
        frame_offset: u64,
        frame_crc: u32,
        lsn: u64,
        payload: &[u8],
        now: i64,
    ) -> Result<bool, IndexError> {
        if payload.len() < 20 {
            return Err(IndexError::ShortPayload {
                op: "GC_MIGRATE",
                got: payload.len(),
            });
        }
        let needle_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
        let expect_version = u64::from_le_bytes(payload[8..16].try_into().unwrap());
        let data_len = u32::from_le_bytes(payload[16..20].try_into().unwrap()) as u64;
        if payload.len() != 20 + data_len as usize {
            return Err(IndexError::ShortPayload {
                op: "GC_MIGRATE",
                got: payload.len(),
            });
        }

        if lsn > self.last_lsn {
            self.last_lsn = lsn;
        }
        if needle_id > self.max_needle_id {
            self.max_needle_id = needle_id;
        }

        // 在线与重放共用同一套条件（记录自包含 expect_version，不依赖
        // 应用时序）：
        // - purge 标记覆盖（lsn ≤ purge_lsn）→ 压制（已删 needle 的孤儿
        //   副本，重放防复活）；
        // - 已有更新 tombstone（delete 晚于本搬移）→ 删除胜出，跳过；
        // - 存在活跃条目且版本 == expect_version → 覆写为搬移副本；
        // - 存在活跃条目但版本不符（已被并发覆写/二次搬移）→ 用户胜出；
        // - 条目缺席且无 tombstone/purge → 应用：原段已被 case A 删除，
        //   本记录是该 needle 唯一可重放副本，据此重建条目。
        if self
            .purged
            .get(&needle_id)
            .map(|p| lsn <= *p)
            .unwrap_or(false)
        {
            return Ok(false);
        }
        if self.tombstones.contains_key(&needle_id) {
            return Ok(false);
        }
        let absent = match self.needles.get(&needle_id) {
            Some(cur) if cur.version_lsn == expect_version => false,
            Some(_) => return Ok(false),
            None => true,
        };

        let entry = NeedleEntry {
            needle_id,
            seg_id,
            offset: frame_offset + FRAME_HEADER_SIZE as u64,
            data_len: data_len as u32,
            crc: frame_crc,
            version_lsn: lsn,
            created_at: now,
            flags: NEEDLE_FLAG_MIGRATED,
        };
        if absent {
            // 重放重建：原段（含 DATA 历史与可能的覆写链）已物理回收。
            self.stats.used_bytes += data_len;
            self.stats.active_count += 1;
            self.needles.insert(needle_id, entry);
            return Ok(true);
        }
        let old = self.needles.insert(needle_id, entry).unwrap();
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
        Ok(true)
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

    /// 当前卷逻辑容量（0 = 尚无 VOLUME_META/checkpoint 基线）。
    pub fn volume_size(&self) -> u64 {
        self.volume_size
    }

    /// 启动基线注入（无 VOLUME_META 记录时取 superblock/config）。
    pub fn set_volume_size(&mut self, size: u64) {
        self.volume_size = size;
    }

    /// 应用 VOLUME_META（§11.1 容量伸缩）：推进卷容量并消费 LSN。
    /// 返回生效后的 volume_size。mask/values 无法解释 → payload 损坏。
    pub fn apply_volume_meta(&mut self, frame_lsn: u64, payload: &[u8]) -> Result<u64, IndexError> {
        let parsed = VolumeMetaPayload::decode(payload).map_err(|_| IndexError::ShortPayload {
            op: "volume_meta",
            got: payload.len(),
        })?;
        let Some(new_size) = parsed.take_volume_size() else {
            return Err(IndexError::ShortPayload {
                op: "volume_meta.size",
                got: payload.len(),
            });
        };
        self.volume_size = new_size;
        if frame_lsn > self.last_lsn {
            self.last_lsn = frame_lsn;
        }
        Ok(new_size)
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

    /// 已过保留期的 tombstone 快照（GC case C 扫描用；实际 purge 经
    /// 持久化 TOMB_PURGE 记录走 [`WalIndex::apply_tomb_purge`]）。
    pub fn expired_tombstones(&self, now: i64) -> impl Iterator<Item = &TombstoneEntry> {
        self.tombstones
            .values()
            .filter(move |tb| tb.retention_until <= now)
    }

    /// 应用 TOMB_PURGE 记录（§8 case C 持久化标记）。
    ///
    /// payload = `needle_id u64 | delete_lsn u64`；`frame_lsn` 为 purge
    /// 记录自身 lsn（标记强度以它为准）。将对应 tombstone 的物理副本
    /// 迁入死副本账本（staging → garbage）并登记 purge 标记；tombstone
    /// 已不在（重复应用/重放旧标记）时仅维护标记的最大 lsn。
    pub fn apply_tomb_purge(&mut self, frame_lsn: u64, payload: &[u8]) -> Result<(), IndexError> {
        if payload.len() != 16 {
            return Err(IndexError::ShortPayload {
                op: "TOMB_PURGE",
                got: payload.len(),
            });
        }
        let needle_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
        let _delete_lsn = u64::from_le_bytes(payload[8..16].try_into().unwrap());

        if frame_lsn > self.last_lsn {
            self.last_lsn = frame_lsn;
        }

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
        }
        // 标记取最大 lsn（同一 needle 重复世代的 purge/重放幂等）。
        let slot = self.purged.entry(needle_id).or_insert(0);
        if frame_lsn > *slot {
            *slot = frame_lsn;
        }
        Ok(())
    }

    /// purge 标记表（checkpoint 持久化用）。
    pub fn purged_markers(&self) -> &HashMap<u64, u64> {
        &self.purged
    }

    /// 修剪失效 purge 标记：丢弃 `purge_lsn < min_surviving_base_lsn` 的
    /// 标记（现存段不可能再含其压制对象）。返回移除数。
    pub fn prune_purge_markers(&mut self, min_surviving_base_lsn: u64) -> usize {
        let before = self.purged.len();
        self.purged.retain(|_, lsn| *lsn >= min_surviving_base_lsn);
        before - self.purged.len()
    }

    /// purge 已过保留期的 tombstone（case C 的纯内存形态）：物理副本迁入
    /// 死副本账本（staging → garbage），不写持久化标记。仅供单元测试；
    /// 在线 GC 路径必须走 [`WalIndex::apply_tomb_purge`]（标记防复活）。
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
        purged: impl Iterator<Item = (u64, u64)>,
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
        for (needle_id, purge_lsn) in purged {
            self.last_lsn = self.last_lsn.max(purge_lsn);
            let slot = self.purged.entry(needle_id).or_insert(0);
            if purge_lsn > *slot {
                *slot = purge_lsn;
            }
        }
    }

    /// 整段回收（GC case A）：清除该段全部死副本账本并扣减 garbage。
    ///
    /// 段文件删除后，段内记录从盘面消失，恢复重放不再产生这些 dead
    /// 条目；内存账本同步清除使统计与恢复后重放终态一致（I4）。返回
    /// 回收字节数。
    pub fn remove_seg_garbage(&mut self, seg_id: u64) -> u64 {
        let mut reclaimed = 0u64;
        self.dead_copies.retain(|d| {
            if d.seg_id == seg_id {
                reclaimed += d.data_len as u64;
                false
            } else {
                true
            }
        });
        self.stats.garbage_bytes = self.stats.garbage_bytes.saturating_sub(reclaimed);
        reclaimed
    }

    /// 冻结索引快照（checkpoint 写入用；P2 简化为锁内克隆）。
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
        idx.apply_data(1, 68, 1, 1, &data_payload(1, 100), 0)
            .unwrap();
        idx.apply_data(1, 68, 2, 2, &data_payload(2, 50), 0)
            .unwrap();
        idx.apply_delete(3, &delete_payload(1, 100, 100 + 86400))
            .unwrap();
        idx.apply_delete(4, &delete_payload(2, 100, 50)).unwrap();
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

    fn migrate_payload(needle_id: u64, expect_version: u64, data_len: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(20 + data_len);
        v.extend_from_slice(&needle_id.to_le_bytes());
        v.extend_from_slice(&expect_version.to_le_bytes());
        v.extend_from_slice(&(data_len as u32).to_le_bytes());
        v.extend_from_slice(&vec![0xcc; data_len]);
        v
    }

    /// 重放缺席应用：原段已回收，GC_MIGRATE 是 needle 唯一可重放副本。
    #[test]
    fn gc_migrate_absent_rebuilds_entry() {
        let mut idx = WalIndex::new();
        let applied = idx
            .apply_gc_migrate(3, 100, 0x1234, 11, &migrate_payload(7, 5, 40), 1000)
            .unwrap();
        assert!(applied);
        let e = idx.lookup(7).unwrap();
        assert_eq!(e.seg_id, 3);
        assert_eq!(e.data_len, 40);
        assert_eq!(e.version_lsn, 11);
        assert_eq!(e.flags, NEEDLE_FLAG_MIGRATED);
        assert_eq!(idx.stats().used_bytes, 40);
        assert_eq!(idx.stats().active_count, 1);
        idx.assert_consistent();
    }

    /// 版本不符（用户并发覆写胜出）→ 跳过，不产生账本条变。
    #[test]
    fn gc_migrate_version_mismatch_skips() {
        let mut idx = WalIndex::new();
        idx.apply_data(1, 0, 0, 5, &data_payload(7, 40), 0).unwrap();
        idx.apply_data(1, 0, 0, 9, &data_payload(7, 40), 0).unwrap(); // 用户覆写 lsn9
        let applied = idx
            .apply_gc_migrate(2, 0, 0, 11, &migrate_payload(7, 5, 40), 0)
            .unwrap();
        assert!(!applied);
        assert_eq!(idx.lookup(7).unwrap().version_lsn, 9);
        assert_eq!(idx.stats().garbage_bytes, 40); // 仅用户覆写产生一条死副本
        idx.assert_consistent();
    }

    /// tombstone 晚于搬移 → 删除胜出，跳过（在线并发删除窗口）。
    #[test]
    fn gc_migrate_after_tombstone_skips() {
        let mut idx = WalIndex::new();
        idx.apply_data(1, 0, 0, 5, &data_payload(7, 40), 0).unwrap();
        idx.apply_delete(6, &delete_payload(7, 0, 86400)).unwrap();
        let applied = idx
            .apply_gc_migrate(2, 0, 0, 7, &migrate_payload(7, 5, 40), 0)
            .unwrap();
        assert!(!applied);
        assert!(idx.lookup(7).is_none());
        assert!(idx.tombstone_of(7).is_some());
        idx.assert_consistent();
    }

    /// purge 标记压制更老孤儿副本；标记之后的新写入是新世代（清除标记）。
    #[test]
    fn purge_marker_suppresses_orphans_and_allows_new_generation() {
        let mut idx = WalIndex::new();
        idx.load_from(
            std::iter::empty(),
            std::iter::empty(),
            std::iter::empty(),
            std::iter::once((7u64, 20u64)),
        );

        // lsn11 的孤儿 GC_MIGRATE / DATA 被压制（重放防复活）。
        assert!(!idx
            .apply_gc_migrate(3, 0, 0, 11, &migrate_payload(7, 5, 40), 0)
            .unwrap());
        idx.apply_data(3, 0, 0, 12, &data_payload(7, 40), 0)
            .unwrap();
        assert!(idx.lookup(7).is_none(), "被压制的孤儿不得复活");

        // lsn21 的删除后新写入允许建立，且清除旧标记。
        idx.apply_data(3, 0, 0, 21, &data_payload(7, 30), 0)
            .unwrap();
        assert_eq!(idx.lookup(7).unwrap().data_len, 30);
        assert!(!idx.purged_markers().contains_key(&7));
        idx.assert_consistent();
    }

    /// TOMB_PURGE 应用：tombstone 转死副本账本 + 登记标记，幂等取最大。
    #[test]
    fn tomb_purge_applies_and_is_idempotent() {
        let mut idx = WalIndex::new();
        idx.apply_data(1, 0, 0, 5, &data_payload(7, 40), 0).unwrap();
        idx.apply_delete(6, &delete_payload(7, 0, 0)).unwrap();
        let mut p = Vec::new();
        p.extend_from_slice(&7u64.to_le_bytes());
        p.extend_from_slice(&6u64.to_le_bytes());
        idx.apply_tomb_purge(10, &p).unwrap();
        assert!(idx.tombstone_of(7).is_none());
        assert_eq!(idx.purged_markers().get(&7), Some(&10));
        assert_eq!(idx.stats().staging_bytes, 0);
        assert_eq!(idx.stats().garbage_bytes, 40);

        // 重放旧标记不回退强度。
        idx.apply_tomb_purge(8, &p).unwrap();
        assert_eq!(idx.purged_markers().get(&7), Some(&10));
        idx.assert_consistent();
    }

    /// 标记修剪：purge_lsn < 现存最老段 base_lsn 时丢弃。
    #[test]
    fn purge_markers_pruned_by_min_base_lsn() {
        let mut idx = WalIndex::new();
        idx.load_from(
            std::iter::empty(),
            std::iter::empty(),
            std::iter::empty(),
            [(1u64, 10u64), (2u64, 20u64)].into_iter(),
        );
        assert_eq!(idx.prune_purge_markers(15), 1);
        assert!(!idx.purged_markers().contains_key(&1));
        assert_eq!(idx.purged_markers().get(&2), Some(&20));
    }

    #[test]
    fn volume_meta_apply_and_malformed_reject() {
        use crate::wal::frame::VolumeMetaPayload;
        let mut idx = WalIndex::new();
        assert_eq!(idx.volume_size(), 0);

        let buf = VolumeMetaPayload::volume_size(4096);
        assert_eq!(idx.apply_volume_meta(3, &buf).unwrap(), 4096);
        assert_eq!(idx.volume_size(), 4096);
        assert_eq!(idx.last_lsn(), 3);

        // 旧 LSN 重放不回退容量字段以外的游标，但容量以记录内容为准。
        assert_eq!(
            idx.apply_volume_meta(2, &VolumeMetaPayload::volume_size(2048))
                .unwrap(),
            2048
        );
        assert_eq!(idx.volume_size(), 2048);
        assert_eq!(idx.last_lsn(), 3);

        // 损坏 payload：拒绝且不改动容量。
        let bad = VolumeMetaPayload {
            field_mask: 0,
            values: bytes::Bytes::new(),
        };
        let mut bad_buf = Vec::new();
        bad.encode(&mut bad_buf);
        assert!(idx.apply_volume_meta(4, &bad_buf).is_err());
        assert!(idx.apply_volume_meta(4, &[1, 0, 0, 0, 0]).is_err());
        assert_eq!(idx.volume_size(), 2048);
        assert_eq!(idx.last_lsn(), 3);
    }
}
