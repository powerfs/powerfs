//! WalVolume：WalEngine 对 v1 Volume 公开 API 面的适配层（方案 §13）。
//!
//! 目标是服务层（VolumeServer / net_handler / StorageManager）零改动：
//! 方法签名与 [`crate::volume::Volume`] 一致，语义按 WAL 引擎模型映射：
//!
//! - 写入 = 一条 DATA 记录入组提交（超写合法，旧版本进 garbage，无
//!   append-only 孤儿副本问题，因此不做 v1 的同内容去重）；
//! - `write_needle_blob` 的部分写 = 读旧版本 RMW 合并后整条覆写（off=0
//!   的整块写走快路径，kernel 4MB chunk 顺序写不产生额外读放大）；
//! - `flush_specific_needles` = 组提交持久化屏障（FlushNeedles 语义锚点，
//!   方案 §5.2）；`force_sync_on_write=true` 映射 CommitMode::Strict。
//! - blob 写 ack 时数据已进 WAL 流并建立索引（read-your-writes），比 v1
//!   的 coalescer 内存 ack 更早可读。
//!
//! P1 已知限制（方案 §17：GC/checkpoint/快照属 P2/P3）：
//! - `worm_lock` / `restore_needle` 未实现（ATTR 记录与 restore API 留位）；
//! - `compact` 返回 (0,0)（搬移式 GC 属 P2）；
//! - NeedleInfo.checksum 映射为帧 CRC32C 的 u64 扩展（帧 CRC 覆盖 payload
//!   含数据，作为完整性校验值有效；数据级独立 checksum 随 P2 checkpoint
//!   落盘，§4.4 已预留字段）。

use bytes::Bytes;
use chrono::Utc;
use powerfs_common::error::{PowerFsError, Result};
use powerfs_common::types::{
    ChecksumAlgorithm, Collection, DiskType, NeedleId, NeedleInfo, Ttl, VolumeId, VolumeInfo,
    VolumeState,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

use crate::volume::ScrubResult;
use crate::wal::commit::CommitMode;
use crate::wal::engine::{EngineError, WalEngine, WalEngineConfig};

fn engine_err(e: EngineError) -> PowerFsError {
    match e {
        EngineError::NotFound(id) => PowerFsError::NeedleNotFound(NeedleId(id)),
        EngineError::Locked(_) => {
            PowerFsError::Internal(format!("wal volume locked by another writer: {e}"))
        }
        EngineError::VolumeFull { .. } => PowerFsError::OutOfSpace,
        EngineError::ResizeTooSmall {
            requested,
            required,
        } => PowerFsError::InvalidRequest(format!(
            "shrink below occupied bytes: requested {requested}, used+staging+pinned {required}"
        )),
        other => PowerFsError::Internal(format!("wal engine: {other}")),
    }
}

pub struct WalVolume {
    info: RwLock<VolumeInfo>,
    engine: WalEngine,
    checksum_algorithm: ChecksumAlgorithm,
    /// force_sync_on_write=true → 每次写 strict（durable 后 ack）。
    strict: bool,
    compacting: AtomicBool,
}

/// WAL 卷管理面详细统计（远程面 admin stats；free_bytes 未限定容量时
/// 为 u64::MAX，与 [`crate::wal::engine::EngineStats`] 同口径）。
#[derive(Debug, Clone, Copy)]
pub struct WalAdminStats {
    pub volume_size: u64,
    pub free_bytes: u64,
    pub used_bytes: u64,
    pub staging_bytes: u64,
    pub garbage_bytes: u64,
    pub pinned_bytes: u64,
    pub active_count: u64,
    pub deleted_count: u64,
    pub segments: usize,
    pub last_ckpt_seq: u64,
    pub last_ckpt_lsn: u64,
    pub durable_lsn: u64,
    pub is_full: bool,
}

#[allow(clippy::result_large_err)]
impl WalVolume {
    pub fn new(
        id: VolumeId,
        node_id: &str,
        path: &str,
        size: u64,
        algorithm: ChecksumAlgorithm,
        mut wal_config: WalEngineConfig,
        strict: bool,
    ) -> Result<Self> {
        let volume_path = std::path::Path::new(path).join(format!("volume_{}", id.0));
        std::fs::create_dir_all(&volume_path)?;

        wal_config.volume_id = id.0;
        wal_config.volume_size = size;
        let engine = WalEngine::open(&volume_path, wal_config).map_err(engine_err)?;

        // 容量以引擎持久状态为准（VOLUME_META 记录 / checkpoint /
        // superblock 基线），构造参数仅在从未 resize 时生效。
        let effective_size = engine.volume_size();

        // 重放终态恢复 info：used 取索引统计，next_file_key 接续 max+1。
        let stats = engine.stats().index;
        let info = VolumeInfo {
            id,
            node_id: powerfs_common::types::NodeId(node_id.to_string()),
            collection: Collection::default(),
            size: effective_size,
            used: stats.used_bytes,
            replica_count: 3,
            ttl: Ttl::default(),
            disk_type: DiskType::default(),
            state: if engine.is_full() {
                VolumeState::Full
            } else {
                VolumeState::Available
            },
            created_at: Utc::now(),
            modified_at: Utc::now(),
            next_file_key: engine.next_needle_id(),
        };

        Ok(WalVolume {
            info: RwLock::new(info),
            engine,
            checksum_algorithm: algorithm,
            strict,
            compacting: AtomicBool::new(false),
        })
    }

    fn mode(&self) -> CommitMode {
        if self.strict {
            CommitMode::Strict
        } else {
            CommitMode::Async
        }
    }

    /// 从引擎统计刷新 info.used，并按 free ≤ 0 规则切换
    /// Available ↔ Full（§11；删除经 purge/GC 释放后恢复）。
    fn refresh_used(&self, guard: &mut VolumeInfo) {
        let stats = self.engine.stats();
        guard.used = stats.index.used_bytes;
        guard.size = stats.volume_size;
        match guard.state {
            VolumeState::Full if stats.free_bytes > 0 => guard.state = VolumeState::Available,
            VolumeState::Available if stats.free_bytes == 0 => guard.state = VolumeState::Full,
            _ => {}
        }
    }

    /// 引擎返回 VolumeFull 时把卷状态置 Full，再转成服务层错误。
    fn mark_full_on_oom(&self, e: EngineError) -> PowerFsError {
        if matches!(e, EngineError::VolumeFull { .. }) {
            let mut guard = self.info.write().unwrap();
            if guard.state == VolumeState::Available {
                guard.state = VolumeState::Full;
                guard.modified_at = Utc::now();
            }
        }
        engine_err(e)
    }

    fn needle_info_from(&self, id: VolumeId, e: &crate::wal::index::NeedleEntry) -> NeedleInfo {
        NeedleInfo {
            id: NeedleId(e.needle_id),
            volume_id: id,
            data_size: e.data_len,
            offset: e.offset,
            // 帧 CRC 覆盖 payload 含数据：完整性校验值语义映射（见模块注释）。
            checksum: e.crc as u64,
            checksum_algorithm: self.checksum_algorithm,
            last_verified_at: None,
            verification_count: 0,
            deleted_at: None,
            delete_retention_until: None,
            worm_retention_until: None,
            created_at: chrono::DateTime::from_timestamp(e.created_at, 0).unwrap_or_else(Utc::now),
            ec_enabled: false,
            ec_k: None,
            ec_m: None,
            ec_shards: Vec::new(),
        }
    }

    pub fn get_stats(&self) -> (u64, u64, u64) {
        let info = self.info.read().unwrap();
        (info.used, info.size, self.engine.stats().index.active_count)
    }

    pub fn id(&self) -> VolumeId {
        self.info.read().unwrap().id
    }

    pub fn info(&self) -> VolumeInfo {
        self.info.read().unwrap().clone()
    }

    pub fn set_collection(&self, collection: Collection) {
        let mut info = self.info.write().unwrap();
        info.collection = collection;
        info.modified_at = Utc::now();
    }

    pub fn state(&self) -> VolumeState {
        self.info.read().unwrap().state
    }

    pub fn size(&self) -> u64 {
        self.info.read().unwrap().size
    }

    pub fn used(&self) -> u64 {
        self.info.read().unwrap().used
    }

    pub fn free_space(&self) -> u64 {
        let free = self.engine.free_bytes();
        if free == u64::MAX {
            // 未限定容量：沿用 size − used 的展示口径。
            let info = self.info.read().unwrap();
            info.size.saturating_sub(info.used)
        } else {
            free
        }
    }

    /// 容量伸缩（§11.1 VOLUME_META）：grow 直接生效；shrink 低于
    /// used+staging+pinned 返回 InvalidRequest。落盘并同步 superblock。
    pub fn resize(&self, new_size: u64) -> Result<()> {
        self.engine.resize(new_size).map_err(engine_err)?;
        let mut guard = self.info.write().unwrap();
        guard.size = new_size;
        guard.modified_at = Utc::now();
        match guard.state {
            VolumeState::Full if !self.engine.is_full() => guard.state = VolumeState::Available,
            VolumeState::Available if self.engine.is_full() => guard.state = VolumeState::Full,
            _ => {}
        }
        Ok(())
    }

    /// WAL 专有：手动触发一轮段 GC（远程面 admin gc；§8）。
    pub fn wal_gc(&self) -> Result<crate::wal::gc::GcOutcome> {
        self.engine.gc().map_err(engine_err)
    }

    /// WAL 专有：手动触发一次 checkpoint（远程面 admin checkpoint；§7）。
    pub fn wal_checkpoint(&self) -> Result<crate::wal::engine::CkptOutcome> {
        self.engine.checkpoint().map_err(engine_err)
    }

    /// WAL 专有：管理面详细统计（四项空间账 + ckpt/段概览，§9.3/§19.2）。
    pub fn wal_admin_stats(&self) -> WalAdminStats {
        let s = self.engine.stats();
        WalAdminStats {
            volume_size: s.volume_size,
            free_bytes: s.free_bytes,
            used_bytes: s.index.used_bytes,
            staging_bytes: s.index.staging_bytes,
            garbage_bytes: s.index.garbage_bytes,
            pinned_bytes: s.index.pinned_bytes,
            active_count: s.index.active_count,
            deleted_count: s.index.deleted_count,
            segments: s.segments,
            last_ckpt_seq: s.last_ckpt_seq,
            last_ckpt_lsn: s.last_ckpt_lsn,
            durable_lsn: s.durable_lsn,
            is_full: self.engine.is_full(),
        }
    }

    pub fn write_needle(&self, file_key: u64, data: Bytes) -> Result<NeedleInfo> {
        let mut info_guard = self.info.write().unwrap();
        if info_guard.state != VolumeState::Available {
            return Err(PowerFsError::InvalidVolumeState(
                "volume not available".to_string(),
            ));
        }

        let actual_key = if file_key == 0 {
            let key = self.engine.alloc_needle_id();
            info_guard.next_file_key = key + 1;
            key
        } else {
            file_key
        };

        // 容量准入（与引擎同口径：used+staging+pinned；引擎侧为最终
        // 权威，这里避免无谓的 needle id 分配与入队）。
        let s = self.engine.stats();
        let occupied = s.index.used_bytes + s.index.staging_bytes + s.index.pinned_bytes;
        if s.volume_size > 0 && occupied + data.len() as u64 > s.volume_size {
            info_guard.state = VolumeState::Full;
            return Err(PowerFsError::OutOfSpace);
        }

        let receipt = self
            .engine
            .write(actual_key, &data, self.mode())
            .map_err(|e| {
                if matches!(e, EngineError::VolumeFull { .. }) {
                    info_guard.state = VolumeState::Full;
                }
                engine_err(e)
            })?;

        let needle_info = NeedleInfo {
            id: NeedleId(actual_key),
            volume_id: info_guard.id,
            data_size: data.len() as u32,
            offset: receipt.placement.offset,
            checksum: receipt.placement.crc as u64,
            checksum_algorithm: self.checksum_algorithm,
            last_verified_at: None,
            verification_count: 0,
            deleted_at: None,
            delete_retention_until: None,
            worm_retention_until: None,
            created_at: Utc::now(),
            ec_enabled: false,
            ec_k: None,
            ec_m: None,
            ec_shards: Vec::new(),
        };

        self.refresh_used(&mut info_guard);
        info_guard.modified_at = Utc::now();
        Ok(needle_info)
    }

    pub fn read_needle(&self, needle_id: &NeedleId) -> Result<Bytes> {
        self.engine
            .read(needle_id.0)
            .map(Bytes::from)
            .map_err(engine_err)
    }

    pub fn delete_needle(&self, needle_id: &NeedleId) -> Result<()> {
        self.engine
            .delete(needle_id.0, self.mode())
            .map_err(engine_err)?;
        let mut info_guard = self.info.write().unwrap();
        self.refresh_used(&mut info_guard);
        info_guard.modified_at = Utc::now();
        Ok(())
    }

    pub fn restore_needle(&self, _needle_id: &NeedleId) -> Result<()> {
        Err(PowerFsError::InvalidRequest(
            "restore not implemented in wal engine (P2, 方案 §5.3)".to_string(),
        ))
    }

    pub fn worm_lock(&self, _needle_id: &NeedleId, _retention_days: i64) -> Result<()> {
        Err(PowerFsError::InvalidRequest(
            "worm lock not implemented in wal engine (ATTR 记录留位, 方案 §4.3)".to_string(),
        ))
    }

    pub fn gc_cleanup(&self) -> Result<usize> {
        // P1 无 tombstone purge（保留期 7 天内可 restore，GC 属 P2）。
        Ok(0)
    }

    pub fn get_needle_info(&self, needle_id: &NeedleId) -> Option<NeedleInfo> {
        let id = self.id();
        self.engine
            .needle_entry(needle_id.0)
            .map(|e| self.needle_info_from(id, &e))
    }

    pub fn count(&self) -> usize {
        self.engine.stats().index.active_count as usize
    }

    pub fn list_needles(&self) -> Result<Vec<(NeedleId, NeedleInfo)>> {
        let id = self.id();
        Ok(self
            .engine
            .needle_entries()
            .iter()
            .map(|e| (NeedleId(e.needle_id), self.needle_info_from(id, e)))
            .collect())
    }

    pub fn set_read_only(&self) {
        let mut info = self.info.write().unwrap();
        info.state = VolumeState::ReadOnly;
        info.modified_at = Utc::now();
    }

    pub fn set_deleting(&self) {
        let mut info = self.info.write().unwrap();
        info.state = VolumeState::Deleting;
        info.modified_at = Utc::now();
    }

    pub fn is_full(&self) -> bool {
        self.state() == VolumeState::Full
    }

    pub fn is_read_only(&self) -> bool {
        self.state() == VolumeState::ReadOnly
    }

    pub fn is_deleting(&self) -> bool {
        self.state() == VolumeState::Deleting
    }

    pub fn is_available(&self) -> bool {
        self.state() == VolumeState::Available
    }

    pub fn compact(&self) -> Result<(u64, u64)> {
        // 映射到段 GC 一轮（§8：purge + 搬移 + 整段回收），返回
        // (整段回收字节, 搬移 needle 数) 与 v1 compact 回执同形。
        let out = self.engine.gc().map_err(engine_err)?;
        Ok((out.reclaimed_bytes, out.migrated_needles as u64))
    }

    pub fn is_compacting(&self) -> bool {
        self.compacting.load(Ordering::SeqCst)
    }

    pub fn should_compact(&self) -> bool {
        // GC 触发策略属 P2（按段 garbage ratio）。
        false
    }

    pub fn write_needle_blob(
        &self,
        file_key: u64,
        offset: i64,
        size: i32,
        data: Bytes,
        _cookie: u32,
    ) -> Result<()> {
        {
            let info_guard = self.info.read().unwrap();
            if info_guard.state != VolumeState::Available {
                return Err(PowerFsError::InvalidVolumeState(
                    "volume not available".to_string(),
                ));
            }
        }
        let data_offset = offset as usize;
        let data_size = std::cmp::min(size as usize, data.len());

        // 快路径：从 0 起的整块写（kernel 4MB chunk 主路径），无读放大。
        if data_offset == 0 && data_size >= data.len() {
            self.engine
                .write(file_key, &data[..data_size], self.mode())
                .map_err(|e| self.mark_full_on_oom(e))?;
        } else {
            // 部分写：读旧版本 RMW 合并（needle 不存在 → 稀疏写，0 填充）。
            let old = self.engine.read(file_key).unwrap_or_default();
            let end = data_offset + data_size;
            let mut buf = vec![0u8; std::cmp::max(old.len(), end)];
            buf[..old.len()].copy_from_slice(&old);
            buf[data_offset..end].copy_from_slice(&data[..data_size]);
            self.engine
                .write(file_key, &buf, self.mode())
                .map_err(|e| self.mark_full_on_oom(e))?;
        }

        let mut info_guard = self.info.write().unwrap();
        self.refresh_used(&mut info_guard);
        info_guard.modified_at = Utc::now();
        Ok(())
    }

    pub fn read_needle_blob(&self, file_key: u64, offset: i64, size: i32) -> Result<Bytes> {
        let data = self.engine.read(file_key).map_err(engine_err)?;
        let data_offset = offset as usize;
        if data_offset >= data.len() {
            // 超界返回空（v1 短读语义）。
            return Ok(Bytes::new());
        }
        let available = data.len() - data_offset;
        let read_size = (size as usize).min(available);
        Ok(Bytes::from(
            data[data_offset..data_offset + read_size].to_vec(),
        ))
    }

    pub fn read_needle_meta(&self, file_key: u64) -> Option<NeedleInfo> {
        self.get_needle_info(&NeedleId(file_key))
    }

    pub fn deleted_count(&self) -> usize {
        self.engine.stats().index.deleted_count as usize
    }

    pub fn verify_needle(&self, needle_id: &NeedleId) -> Result<bool> {
        // 读路径 verify_on_read 重算帧 CRC：读成功即完整。
        match self.engine.read(needle_id.0) {
            Ok(_) => Ok(true),
            Err(EngineError::NotFound(_)) => Err(engine_err(EngineError::NotFound(needle_id.0))),
            Err(EngineError::ReadCorrupt { .. }) => Ok(false),
            Err(e) => Err(engine_err(e)),
        }
    }

    pub fn scrub_volume(&self) -> ScrubResult {
        let mut result = ScrubResult::default();
        for e in self.engine.needle_entries() {
            result.total += 1;
            match self.engine.read(e.needle_id) {
                Ok(_) => result.verified += 1,
                Err(EngineError::ReadCorrupt { .. }) => {
                    result.corrupted += 1;
                    result.corrupted_needles.push(NeedleId(e.needle_id));
                }
                Err(err) => {
                    log::warn!(
                        "scrub_volume: read needle {} (seg {} off {}) failed: {}",
                        e.needle_id,
                        e.seg_id,
                        e.offset,
                        err
                    );
                    result.errors += 1;
                    result.corrupted_needles.push(NeedleId(e.needle_id));
                }
            }
        }
        result
    }

    pub fn flush_all_dirty(&self) -> usize {
        // 数据在入队时已进 WAL 流并建立索引（无 coalescer 脏区），无物化动作。
        0
    }

    pub fn flush_expired_dirty(&self) -> usize {
        0
    }

    /// FlushNeedles 屏障：等待 ids 中最高 version_lsn durable（方案 §5.2）。
    pub fn flush_specific_needles(&self, needle_ids: &[NeedleId]) -> Result<usize> {
        let mut max_lsn = 0u64;
        let mut hit = 0usize;
        {
            for id in needle_ids {
                if let Some(e) = self.engine.needle_entry(id.0) {
                    max_lsn = max_lsn.max(e.version_lsn);
                    hit += 1;
                }
            }
        }
        if max_lsn > 0 {
            self.engine.flush(max_lsn).map_err(engine_err)?;
        }
        Ok(hit)
    }

    /// 空闲持久化：async 窗口内有未 durable 记录时刷一次（组提交侧聚合）。
    /// 返回 true 表示执行了 fsync。
    pub fn sync_wal_if_dirty(&self) -> Result<bool> {
        if self.engine.durable_lsn() < self.engine.flushed_lsn() {
            self.engine.flush_all().map_err(engine_err)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// WAL 引擎句柄（admin/统计扩展用）。
    pub fn engine(&self) -> &WalEngine {
        &self.engine
    }
}
