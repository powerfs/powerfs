//! VolumeEngine：v1/v2 引擎并行切换的统一分发层（方案 §13 / §17 P1）。
//!
//! `Volume`（v1 needle 引擎）与 [`crate::wal_volume::WalVolume`]（v2 WAL
//! 引擎）共享同一公开 API 面；`VolumeEngine` 以 enum 分发让 StorageManager
//! 与服务层持有统一句柄，`volume_engine=needle|wal` 配置决定新建卷的分支。
//! 选择 enum 而非 trait：避免将 40+ 个方法（含返回内部引用的签名）迁移到
//! 动态分发，且分支数固定为二，match 委托零成本直通。

use bytes::Bytes;
use powerfs_common::error::{PowerFsError, Result};
use powerfs_common::types::{Collection, NeedleId, NeedleInfo, VolumeId, VolumeInfo, VolumeState};
use std::sync::Arc;

use crate::storage_backend::StorageBackend;
use crate::volume::{ScrubResult, Volume};
use crate::wal_volume::WalVolume;

/// 引擎选择（来自 `volume_engine` 配置项）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EngineKind {
    /// v1 needle 引擎（默认）。
    #[default]
    Needle,
    /// v2 WAL 统一日志引擎。
    Wal,
}

impl EngineKind {
    /// 解析配置字符串（缺省/空 → needle；非法值报错）。
    pub fn parse(s: Option<&str>) -> Result<Self> {
        let s = s.map(|s| s.trim().to_ascii_lowercase()).unwrap_or_default();
        match s.as_str() {
            "" | "needle" => Ok(EngineKind::Needle),
            "wal" => Ok(EngineKind::Wal),
            other => Err(PowerFsError::InvalidRequest(format!(
                "invalid volume_engine '{other}' (expected \"needle\" or \"wal\")"
            ))),
        }
    }
}

pub enum VolumeEngine {
    Needle(Box<Volume>),
    Wal(Box<WalVolume>),
}

#[allow(clippy::result_large_err)]
impl VolumeEngine {
    // ── 元数据面 ────────────────────────────────────────────────

    pub fn get_stats(&self) -> (u64, u64, u64) {
        match self {
            VolumeEngine::Needle(v) => v.get_stats(),
            VolumeEngine::Wal(v) => v.get_stats(),
        }
    }

    pub fn id(&self) -> VolumeId {
        match self {
            VolumeEngine::Needle(v) => v.id(),
            VolumeEngine::Wal(v) => v.id(),
        }
    }

    pub fn info(&self) -> VolumeInfo {
        match self {
            VolumeEngine::Needle(v) => v.info(),
            VolumeEngine::Wal(v) => v.info(),
        }
    }

    pub fn set_collection(&self, collection: Collection) {
        match self {
            VolumeEngine::Needle(v) => v.set_collection(collection),
            VolumeEngine::Wal(v) => v.set_collection(collection),
        }
    }

    pub fn state(&self) -> VolumeState {
        match self {
            VolumeEngine::Needle(v) => v.state(),
            VolumeEngine::Wal(v) => v.state(),
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            VolumeEngine::Needle(v) => v.size(),
            VolumeEngine::Wal(v) => v.size(),
        }
    }

    pub fn used(&self) -> u64 {
        match self {
            VolumeEngine::Needle(v) => v.used(),
            VolumeEngine::Wal(v) => v.used(),
        }
    }

    pub fn free_space(&self) -> u64 {
        match self {
            VolumeEngine::Needle(v) => v.free_space(),
            VolumeEngine::Wal(v) => v.free_space(),
        }
    }

    pub fn set_read_only(&self) {
        match self {
            VolumeEngine::Needle(v) => v.set_read_only(),
            VolumeEngine::Wal(v) => v.set_read_only(),
        }
    }

    pub fn set_deleting(&self) {
        match self {
            VolumeEngine::Needle(v) => v.set_deleting(),
            VolumeEngine::Wal(v) => v.set_deleting(),
        }
    }

    pub fn is_full(&self) -> bool {
        match self {
            VolumeEngine::Needle(v) => v.is_full(),
            VolumeEngine::Wal(v) => v.is_full(),
        }
    }

    pub fn is_read_only(&self) -> bool {
        match self {
            VolumeEngine::Needle(v) => v.is_read_only(),
            VolumeEngine::Wal(v) => v.is_read_only(),
        }
    }

    pub fn is_deleting(&self) -> bool {
        match self {
            VolumeEngine::Needle(v) => v.is_deleting(),
            VolumeEngine::Wal(v) => v.is_deleting(),
        }
    }

    pub fn is_available(&self) -> bool {
        match self {
            VolumeEngine::Needle(v) => v.is_available(),
            VolumeEngine::Wal(v) => v.is_available(),
        }
    }

    // ── 数据面 ──────────────────────────────────────────────────

    pub fn write_needle(&self, file_key: u64, data: Bytes) -> Result<NeedleInfo> {
        match self {
            VolumeEngine::Needle(v) => v.write_needle(file_key, data),
            VolumeEngine::Wal(v) => v.write_needle(file_key, data),
        }
    }

    pub fn read_needle(&self, needle_id: &NeedleId) -> Result<Bytes> {
        match self {
            VolumeEngine::Needle(v) => v.read_needle(needle_id),
            VolumeEngine::Wal(v) => v.read_needle(needle_id),
        }
    }

    pub fn delete_needle(&self, needle_id: &NeedleId) -> Result<()> {
        match self {
            VolumeEngine::Needle(v) => v.delete_needle(needle_id),
            VolumeEngine::Wal(v) => v.delete_needle(needle_id),
        }
    }

    pub fn restore_needle(&self, needle_id: &NeedleId) -> Result<()> {
        match self {
            VolumeEngine::Needle(v) => v.restore_needle(needle_id),
            VolumeEngine::Wal(v) => v.restore_needle(needle_id),
        }
    }

    pub fn worm_lock(&self, needle_id: &NeedleId, retention_days: i64) -> Result<()> {
        match self {
            VolumeEngine::Needle(v) => v.worm_lock(needle_id, retention_days),
            VolumeEngine::Wal(v) => v.worm_lock(needle_id, retention_days),
        }
    }

    pub fn write_needle_blob(
        &self,
        file_key: u64,
        offset: i64,
        size: i32,
        data: Bytes,
        cookie: u32,
    ) -> Result<()> {
        match self {
            VolumeEngine::Needle(v) => v.write_needle_blob(file_key, offset, size, data, cookie),
            VolumeEngine::Wal(v) => v.write_needle_blob(file_key, offset, size, data, cookie),
        }
    }

    pub fn read_needle_blob(&self, file_key: u64, offset: i64, size: i32) -> Result<Bytes> {
        match self {
            VolumeEngine::Needle(v) => v.read_needle_blob(file_key, offset, size),
            VolumeEngine::Wal(v) => v.read_needle_blob(file_key, offset, size),
        }
    }

    pub fn read_needle_meta(&self, file_key: u64) -> Option<NeedleInfo> {
        match self {
            VolumeEngine::Needle(v) => v.read_needle_meta(file_key),
            VolumeEngine::Wal(v) => v.read_needle_meta(file_key),
        }
    }

    pub fn get_needle_info(&self, needle_id: &NeedleId) -> Option<NeedleInfo> {
        match self {
            VolumeEngine::Needle(v) => v.get_needle_info(needle_id),
            VolumeEngine::Wal(v) => v.get_needle_info(needle_id),
        }
    }

    // ── flush / 持久化屏障 ──────────────────────────────────────

    pub fn flush_specific_needles(&self, needle_ids: &[NeedleId]) -> Result<usize> {
        match self {
            VolumeEngine::Needle(v) => v.flush_specific_needles(needle_ids),
            VolumeEngine::Wal(v) => v.flush_specific_needles(needle_ids),
        }
    }

    pub fn flush_all_dirty(&self) -> usize {
        match self {
            VolumeEngine::Needle(v) => v.flush_all_dirty(),
            VolumeEngine::Wal(v) => v.flush_all_dirty(),
        }
    }

    pub fn flush_expired_dirty(&self) -> usize {
        match self {
            VolumeEngine::Needle(v) => v.flush_expired_dirty(),
            VolumeEngine::Wal(v) => v.flush_expired_dirty(),
        }
    }

    /// 空闲 WAL 同步（后台维护线程调用）。返回 true 表示执行了 fsync。
    pub fn sync_wal_if_dirty(&self) -> Result<bool> {
        match self {
            VolumeEngine::Needle(v) => v.index().sync_wal_if_dirty(),
            VolumeEngine::Wal(v) => v.sync_wal_if_dirty(),
        }
    }

    // ── 枚举 / 校验 / 回收 ──────────────────────────────────────

    pub fn count(&self) -> usize {
        match self {
            VolumeEngine::Needle(v) => v.count(),
            VolumeEngine::Wal(v) => v.count(),
        }
    }

    pub fn list_needles(&self) -> Result<Vec<(NeedleId, NeedleInfo)>> {
        match self {
            VolumeEngine::Needle(v) => v.list_needles(),
            VolumeEngine::Wal(v) => v.list_needles(),
        }
    }

    pub fn deleted_count(&self) -> usize {
        match self {
            VolumeEngine::Needle(v) => v.deleted_count(),
            VolumeEngine::Wal(v) => v.deleted_count(),
        }
    }

    pub fn verify_needle(&self, needle_id: &NeedleId) -> Result<bool> {
        match self {
            VolumeEngine::Needle(v) => v.verify_needle(needle_id),
            VolumeEngine::Wal(v) => v.verify_needle(needle_id),
        }
    }

    pub fn scrub_volume(&self) -> ScrubResult {
        match self {
            VolumeEngine::Needle(v) => v.scrub_volume(),
            VolumeEngine::Wal(v) => v.scrub_volume(),
        }
    }

    pub fn compact(&self) -> Result<(u64, u64)> {
        match self {
            VolumeEngine::Needle(v) => v.compact(),
            VolumeEngine::Wal(v) => v.compact(),
        }
    }

    pub fn is_compacting(&self) -> bool {
        match self {
            VolumeEngine::Needle(v) => v.is_compacting(),
            VolumeEngine::Wal(v) => v.is_compacting(),
        }
    }

    pub fn should_compact(&self) -> bool {
        match self {
            VolumeEngine::Needle(v) => v.should_compact(),
            VolumeEngine::Wal(v) => v.should_compact(),
        }
    }

    pub fn gc_cleanup(&self) -> Result<usize> {
        match self {
            VolumeEngine::Needle(v) => v.gc_cleanup(),
            VolumeEngine::Wal(v) => v.gc_cleanup(),
        }
    }

    /// 删除卷时清理引擎侧的 backend 注册（v1 backend 卷位图；WAL 引擎
    /// 无 backend 注册，仅需 StorageManager 统一删除卷目录）。
    pub fn remove_backend_volume(&self, backend: &Arc<dyn StorageBackend>) -> Result<()> {
        match self {
            VolumeEngine::Needle(v) => backend
                .delete_volume(v.id().0)
                .map_err(|e| PowerFsError::Storage(e.to_string())),
            VolumeEngine::Wal(_) => Ok(()),
        }
    }
}
