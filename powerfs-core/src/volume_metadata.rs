use chrono::Utc;
use log::{debug, info, warn};
use powerfs_common::{
    error::{PowerFsError, Result},
    types::{NeedleId, NeedleInfo},
    utils::ChecksumAlgorithm,
    volume_config::{AllocationStats, VolumeConfig},
};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, WriteBatch, DB};
use serde_json;
use std::path::Path;
use std::sync::Arc;

/// RocksDB Column Family 名称
const CF_CONFIG: &str = "config";
const CF_NEEDLES: &str = "needles";
const CF_ALLOCATION: &str = "allocation";
const CF_DELETED: &str = "deleted";

/// 配置单例的 key
const KEY_CONFIG: &[u8] = b"volume_config";
const KEY_ALLOCATION: &[u8] = b"allocation_stats";

/// Volume 元数据管理器 — 封装 RocksDB，提供原子读写操作
pub struct VolumeMetadata {
    db: Arc<DB>,
}

impl VolumeMetadata {
    /// 打开或创建 Volume 元数据数据库
    pub fn open(path: &Path) -> Result<Self> {
        let mut cf_opts = rocksdb::Options::default();
        cf_opts.set_max_write_buffer_number(4);
        cf_opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB

        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_background_jobs(4);

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_CONFIG, cf_opts.clone()),
            ColumnFamilyDescriptor::new(CF_NEEDLES, cf_opts.clone()),
            ColumnFamilyDescriptor::new(CF_ALLOCATION, cf_opts.clone()),
            ColumnFamilyDescriptor::new(CF_DELETED, cf_opts.clone()),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)
            .map_err(|e| PowerFsError::Internal(format!("Failed to open RocksDB: {}", e)))?;

        info!("VolumeMetadata opened at {:?}", path);

        Ok(Self { db: Arc::new(db) })
    }

    fn cf_config(&self) -> &ColumnFamily {
        self.db.cf_handle(CF_CONFIG).expect("config CF must exist")
    }

    fn cf_needles(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_NEEDLES)
            .expect("needles CF must exist")
    }

    fn cf_allocation(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_ALLOCATION)
            .expect("allocation CF must exist")
    }

    fn cf_deleted(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_DELETED)
            .expect("deleted CF must exist")
    }

    // ===== Config CF =====

    /// 读取 Volume 配置
    pub fn get_config(&self) -> Result<Option<VolumeConfig>> {
        let data = self
            .db
            .get_cf(self.cf_config(), KEY_CONFIG)
            .map_err(|e| PowerFsError::Internal(format!("RocksDB get config failed: {}", e)))?;

        match data {
            Some(bytes) => {
                let config: VolumeConfig = serde_json::from_slice(&bytes).map_err(|e| {
                    PowerFsError::Internal(format!("Deserialize config failed: {}", e))
                })?;
                Ok(Some(config))
            }
            None => Ok(None),
        }
    }

    /// 写入 Volume 配置（仅在首次创建时调用）
    pub fn put_config(&self, config: &VolumeConfig) -> Result<()> {
        let data = serde_json::to_vec(config)
            .map_err(|e| PowerFsError::Internal(format!("Serialize config failed: {}", e)))?;

        self.db
            .put_cf(self.cf_config(), KEY_CONFIG, data)
            .map_err(|e| PowerFsError::Internal(format!("RocksDB put config failed: {}", e)))?;

        debug!("Volume config saved: volume_id={}", config.volume_id);
        Ok(())
    }

    // ===== Allocation CF =====

    /// 读取分配状态
    pub fn get_allocation(&self) -> Result<AllocationStats> {
        let data = self
            .db
            .get_cf(self.cf_allocation(), KEY_ALLOCATION)
            .map_err(|e| PowerFsError::Internal(format!("RocksDB get allocation failed: {}", e)))?;

        match data {
            Some(bytes) => {
                let stats: AllocationStats = serde_json::from_slice(&bytes).map_err(|e| {
                    PowerFsError::Internal(format!("Deserialize allocation failed: {}", e))
                })?;
                Ok(stats)
            }
            None => Ok(AllocationStats::default()),
        }
    }

    /// 写入分配状态
    pub fn put_allocation(&self, stats: &AllocationStats) -> Result<()> {
        let data = serde_json::to_vec(stats)
            .map_err(|e| PowerFsError::Internal(format!("Serialize allocation failed: {}", e)))?;

        self.db
            .put_cf(self.cf_allocation(), KEY_ALLOCATION, data)
            .map_err(|e| PowerFsError::Internal(format!("RocksDB put allocation failed: {}", e)))?;

        Ok(())
    }

    // ===== Needles CF =====

    /// 读取 Needle 信息
    pub fn get_needle(&self, needle_id: &NeedleId) -> Result<Option<NeedleInfo>> {
        let key = needle_id.0.to_be_bytes();
        let data = self
            .db
            .get_cf(self.cf_needles(), key)
            .map_err(|e| PowerFsError::Internal(format!("RocksDB get needle failed: {}", e)))?;

        match data {
            Some(bytes) => {
                let info: NeedleInfo = serde_json::from_slice(&bytes).map_err(|e| {
                    PowerFsError::Internal(format!("Deserialize needle failed: {}", e))
                })?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    /// 写入 Needle 信息
    pub fn put_needle(&self, info: &NeedleInfo) -> Result<()> {
        let key = info.id.0.to_be_bytes();
        let data = serde_json::to_vec(info)
            .map_err(|e| PowerFsError::Internal(format!("Serialize needle failed: {}", e)))?;

        self.db
            .put_cf(self.cf_needles(), key, data)
            .map_err(|e| PowerFsError::Internal(format!("RocksDB put needle failed: {}", e)))?;

        Ok(())
    }

    /// 删除 Needle（从 needles CF 移除，完整 NeedleInfo 存入 deleted CF）
    pub fn delete_needle(&self, needle_id: &NeedleId) -> Result<Option<NeedleInfo>> {
        let key = needle_id.0.to_be_bytes();

        // 先读取现有 needle 信息
        let existing = self.get_needle(needle_id)?;

        if let Some(mut info) = existing.clone() {
            let now = Utc::now();

            // 原子操作：从 needles CF 删除 + 写入 deleted CF（保留完整 NeedleInfo）
            let mut batch = WriteBatch::default();
            batch.delete_cf(self.cf_needles(), key);
            info.deleted_at = Some(now);
            info.delete_retention_until = Some(now + chrono::Duration::days(7));
            let deleted_data = serde_json::to_vec(&info).map_err(|e| {
                PowerFsError::Internal(format!("Serialize deleted needle failed: {}", e))
            })?;
            batch.put_cf(self.cf_deleted(), key, deleted_data);

            self.db.write(batch).map_err(|e| {
                PowerFsError::Internal(format!("RocksDB batch delete failed: {}", e))
            })?;

            debug!("Needle {} moved to deleted CF", needle_id.0);
        }

        Ok(existing)
    }

    /// 列出所有 Needle（用于扫描重建或 compact）
    pub fn list_needles(&self) -> Result<Vec<(NeedleId, NeedleInfo)>> {
        let iter = self
            .db
            .iterator_cf(self.cf_needles(), rocksdb::IteratorMode::Start);

        let mut result = Vec::new();
        for item in iter {
            let (key, value) =
                item.map_err(|e| PowerFsError::Internal(format!("RocksDB iterator error: {}", e)))?;

            if key.len() == 8 {
                let id = u64::from_be_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]);
                let info: NeedleInfo = serde_json::from_slice(&value).map_err(|e| {
                    PowerFsError::Internal(format!("Deserialize needle failed: {}", e))
                })?;
                result.push((NeedleId(id), info));
            }
        }

        Ok(result)
    }

    /// 列出所有已删除的 Needle（用于 compact 和 GC）
    pub fn list_deleted(&self) -> Result<Vec<(NeedleId, NeedleInfo)>> {
        let iter = self
            .db
            .iterator_cf(self.cf_deleted(), rocksdb::IteratorMode::Start);

        let mut result = Vec::new();
        for item in iter {
            let (key, value) =
                item.map_err(|e| PowerFsError::Internal(format!("RocksDB iterator error: {}", e)))?;

            if key.len() == 8 {
                let id = u64::from_be_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]);
                let info: NeedleInfo = serde_json::from_slice(&value).map_err(|e| {
                    PowerFsError::Internal(format!("Deserialize deleted needle failed: {}", e))
                })?;
                result.push((NeedleId(id), info));
            }
        }

        Ok(result)
    }

    /// 启动时重建 allocation 统计：扫描 needles CF + deleted CF
    /// 返回 (used_bytes, append_offset, active_count, deleted_count)
    ///
    /// used_bytes: 活跃 needle 总大小（逻辑空间使用，删除后已回收）
    /// append_offset: 物理文件末尾（包括 deleted needle 的 hole，append-only）
    /// free_bytes = volume_size - used_bytes（逻辑可用空间）
    pub fn rebuild_allocation_stats(&self) -> Result<(u64, u64, u64, u64)> {
        use powerfs_common::constants::{
            NEEDLE_FOOTER_SIZE, NEEDLE_HEADER_SIZE, VOLUME_DATA_OFFSET,
        };

        let mut max_end: u64 = VOLUME_DATA_OFFSET;
        let mut used_bytes: u64 = 0;
        let mut active_count: u64 = 0;

        // 扫描 needles CF（活跃 needle）—— 算入 used_bytes
        for (_, info) in self.iter() {
            let needle_size =
                (NEEDLE_HEADER_SIZE as u64) + (info.data_size as u64) + (NEEDLE_FOOTER_SIZE as u64);
            let end = info.offset.saturating_add(needle_size);
            if end > max_end {
                max_end = end;
            }
            used_bytes = used_bytes.saturating_add(needle_size);
            active_count += 1;
        }

        // 扫描 deleted CF（已删除 needle）—— 不算入 used_bytes（逻辑空间已回收），
        // 但仍影响 append_offset（物理文件末尾，hole 不可复用，需 compact 回收）
        let deleted_items = self.list_deleted()?;
        let deleted_count = deleted_items.len() as u64;
        for (_, info) in &deleted_items {
            let needle_size =
                (NEEDLE_HEADER_SIZE as u64) + (info.data_size as u64) + (NEEDLE_FOOTER_SIZE as u64);
            let end = info.offset.saturating_add(needle_size);
            if end > max_end {
                max_end = end;
            }
        }

        Ok((used_bytes, max_end, active_count, deleted_count))
    }

    /// GC 清理：永久删除 deleted CF 中已过期的条目，返回清理数量
    pub fn purge_expired_deleted(&self) -> Result<usize> {
        let now = Utc::now();
        let deleted_items = self.list_deleted()?;
        let mut purged = 0;

        for (needle_id, info) in &deleted_items {
            if let Some(retention_until) = info.delete_retention_until {
                if retention_until < now {
                    let key = needle_id.0.to_be_bytes();
                    self.db.delete_cf(self.cf_deleted(), key).map_err(|e| {
                        PowerFsError::Internal(format!("RocksDB purge deleted failed: {}", e))
                    })?;
                    purged += 1;
                }
            }
        }

        // 更新 deleted_count
        if purged > 0 {
            let mut stats = self.get_allocation()?;
            stats.deleted_count = stats.deleted_count.saturating_sub(purged as u64);
            stats.last_modified_at = now.timestamp();
            self.put_allocation(&stats)?;
        }

        if purged > 0 {
            info!("GC purged {} expired deleted needles", purged);
        }

        Ok(purged)
    }

    // ===== 原子写入操作 =====

    /// 原子写入 Needle + 更新分配状态，返回更新后的 AllocationStats
    pub fn write_needle_atomic(
        &self,
        info: &NeedleInfo,
        data_size: u64,
        volume_size: u64,
    ) -> Result<AllocationStats> {
        let _ = volume_size; // volume_size used in stats calculation below
        let mut batch = WriteBatch::default();

        // 写入 needle 信息
        let key = info.id.0.to_be_bytes();
        let needle_data = serde_json::to_vec(info)
            .map_err(|e| PowerFsError::Internal(format!("Serialize needle failed: {}", e)))?;
        batch.put_cf(self.cf_needles(), key, needle_data);

        // 更新分配状态
        let mut stats = self.get_allocation()?;

        // 检查是否是覆盖写入（同一 needle_id 已存在）
        let is_update = matches!(self.get_needle(&info.id), Ok(Some(_)));

        stats.used_bytes += data_size;
        stats.free_bytes = volume_size.saturating_sub(stats.used_bytes);
        stats.append_offset += data_size;
        stats.next_needle_id = stats.next_needle_id.max(info.id.0 + 1);

        // 仅在新增 needle 时增加 active_count（覆盖写入时 count 不变）
        if !is_update {
            stats.active_count += 1;
        }

        stats.last_modified_at = Utc::now().timestamp();

        let alloc_data = serde_json::to_vec(&stats)
            .map_err(|e| PowerFsError::Internal(format!("Serialize allocation failed: {}", e)))?;
        batch.put_cf(self.cf_allocation(), KEY_ALLOCATION, alloc_data);

        self.db
            .write(batch)
            .map_err(|e| PowerFsError::Internal(format!("RocksDB atomic write failed: {}", e)))?;

        debug!(
            "Atomic write: needle={}, used={}, free={}, offset={}",
            info.id.0, stats.used_bytes, stats.free_bytes, stats.append_offset
        );

        Ok(stats)
    }

    /// 原子删除 Needle + 更新分配状态，返回被删除的 NeedleInfo
    /// 硬删除策略：从 needles CF 移除，完整 NeedleInfo（含 deleted_at）存入 deleted CF
    pub fn delete_needle_atomic(
        &self,
        needle_id: &NeedleId,
        volume_size: u64,
    ) -> Result<Option<NeedleInfo>> {
        use powerfs_common::constants::{NEEDLE_FOOTER_SIZE, NEEDLE_HEADER_SIZE};

        let existing = self.get_needle(needle_id)?;

        if let Some(mut info) = existing.clone() {
            let now = Utc::now();
            let now_ts = now.timestamp();
            let mut batch = WriteBatch::default();

            // 从 needles CF 删除
            let key = needle_id.0.to_be_bytes();
            batch.delete_cf(self.cf_needles(), key);

            // 标记 deleted_at 和 delete_retention_until，存入 deleted CF（保留完整信息以支持恢复）
            info.deleted_at = Some(now);
            info.delete_retention_until = Some(now + chrono::Duration::days(7));
            let deleted_data = serde_json::to_vec(&info).map_err(|e| {
                PowerFsError::Internal(format!("Serialize deleted needle failed: {}", e))
            })?;
            batch.put_cf(self.cf_deleted(), key, deleted_data);

            // 更新分配状态：立即回收逻辑空间（used_bytes 减少）
            // 物理空间回收由 compact 机制处理（hole 不可复用，但逻辑上空间已释放）
            let mut stats = self.get_allocation()?;
            let needle_size =
                (NEEDLE_HEADER_SIZE as u64) + (info.data_size as u64) + (NEEDLE_FOOTER_SIZE as u64);
            stats.used_bytes = stats.used_bytes.saturating_sub(needle_size);
            stats.free_bytes = volume_size.saturating_sub(stats.used_bytes);
            stats.active_count = stats.active_count.saturating_sub(1);
            stats.deleted_count += 1;
            stats.last_modified_at = now_ts;

            let alloc_data = serde_json::to_vec(&stats).map_err(|e| {
                PowerFsError::Internal(format!("Serialize allocation failed: {}", e))
            })?;
            batch.put_cf(self.cf_allocation(), KEY_ALLOCATION, alloc_data);

            self.db.write(batch).map_err(|e| {
                PowerFsError::Internal(format!("RocksDB atomic delete failed: {}", e))
            })?;

            debug!(
                "Atomic delete: needle={}, freed={} bytes, used={}, free={}, active={}, deleted={}",
                needle_id.0,
                needle_size,
                stats.used_bytes,
                stats.free_bytes,
                stats.active_count,
                stats.deleted_count
            );
        }

        Ok(existing)
    }

    /// 原子恢复 Needle：从 deleted CF 移回 needles CF，更新分配状态
    pub fn restore_needle_atomic(
        &self,
        needle_id: &NeedleId,
        volume_size: u64,
    ) -> Result<Option<NeedleInfo>> {
        use powerfs_common::constants::{NEEDLE_FOOTER_SIZE, NEEDLE_HEADER_SIZE};

        let key = needle_id.0.to_be_bytes();

        // 从 deleted CF 读取
        let data = self
            .db
            .get_cf(self.cf_deleted(), key)
            .map_err(|e| PowerFsError::Internal(format!("RocksDB get deleted failed: {}", e)))?;

        if let Some(bytes) = data {
            let mut info: NeedleInfo = serde_json::from_slice(&bytes).map_err(|e| {
                PowerFsError::Internal(format!("Deserialize deleted needle failed: {}", e))
            })?;

            let now_ts = Utc::now().timestamp();
            let mut batch = WriteBatch::default();

            // 清除删除标记，放回 needles CF
            info.deleted_at = None;
            info.delete_retention_until = None;
            let needle_data = serde_json::to_vec(&info)
                .map_err(|e| PowerFsError::Internal(format!("Serialize needle failed: {}", e)))?;
            batch.put_cf(self.cf_needles(), key, needle_data);

            // 从 deleted CF 移除
            batch.delete_cf(self.cf_deleted(), key);

            // 更新分配状态：恢复 needle 时增加 used_bytes（与 delete 对称）
            let mut stats = self.get_allocation()?;
            let needle_size =
                (NEEDLE_HEADER_SIZE as u64) + (info.data_size as u64) + (NEEDLE_FOOTER_SIZE as u64);
            stats.used_bytes = stats.used_bytes.saturating_add(needle_size);
            stats.free_bytes = volume_size.saturating_sub(stats.used_bytes);
            stats.active_count += 1;
            stats.deleted_count = stats.deleted_count.saturating_sub(1);
            stats.last_modified_at = now_ts;

            let alloc_data = serde_json::to_vec(&stats).map_err(|e| {
                PowerFsError::Internal(format!("Serialize allocation failed: {}", e))
            })?;
            batch.put_cf(self.cf_allocation(), KEY_ALLOCATION, alloc_data);

            self.db.write(batch).map_err(|e| {
                PowerFsError::Internal(format!("RocksDB atomic restore failed: {}", e))
            })?;

            debug!(
                "Atomic restore: needle={}, restored={} bytes, used={}, free={}, active={}, deleted={}",
                needle_id.0, needle_size, stats.used_bytes, stats.free_bytes,
                stats.active_count, stats.deleted_count
            );

            Ok(Some(info))
        } else {
            Ok(None)
        }
    }

    /// Compact 后清理 deleted CF 并更新分配状态，返回更新后的 AllocationStats
    pub fn compact_cleanup(&self, freed_bytes: u64, volume_size: u64) -> Result<AllocationStats> {
        let mut batch = WriteBatch::default();

        // 清空 deleted CF
        let deleted_items = self.list_deleted()?;
        for (id, _) in &deleted_items {
            let key = id.0.to_be_bytes();
            batch.delete_cf(self.cf_deleted(), key);
        }

        // 更新分配状态
        let mut stats = self.get_allocation()?;
        stats.used_bytes = stats.used_bytes.saturating_sub(freed_bytes);
        stats.free_bytes = volume_size.saturating_sub(stats.used_bytes);
        stats.deleted_count = 0;
        stats.last_modified_at = Utc::now().timestamp();

        let alloc_data = serde_json::to_vec(&stats)
            .map_err(|e| PowerFsError::Internal(format!("Serialize allocation failed: {}", e)))?;
        batch.put_cf(self.cf_allocation(), KEY_ALLOCATION, alloc_data);

        self.db.write(batch).map_err(|e| {
            PowerFsError::Internal(format!("RocksDB compact cleanup failed: {}", e))
        })?;

        info!(
            "Compact cleanup: freed={} bytes, {} deleted needles removed, used={}, free={}",
            freed_bytes,
            deleted_items.len(),
            stats.used_bytes,
            stats.free_bytes
        );

        Ok(stats)
    }

    /// 创建 RocksDB Checkpoint（L2 快照）
    pub fn create_checkpoint(&self, checkpoint_path: &Path) -> Result<()> {
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db).map_err(|e| {
            PowerFsError::Internal(format!("Failed to create checkpoint object: {}", e))
        })?;

        // 如果目标目录存在，先删除
        if checkpoint_path.exists() {
            std::fs::remove_dir_all(checkpoint_path).map_err(|e| {
                PowerFsError::Internal(format!("Failed to remove old checkpoint: {}", e))
            })?;
        }

        checkpoint
            .create_checkpoint(checkpoint_path)
            .map_err(|e| PowerFsError::Internal(format!("Failed to create checkpoint: {}", e)))?;

        info!("Checkpoint created at {:?}", checkpoint_path);
        Ok(())
    }

    /// 获取 Needle 数量
    pub fn needle_count(&self) -> Result<u64> {
        let stats = self.get_allocation()?;
        Ok(stats.active_count)
    }

    /// 从 volume.data 扫描重建索引（L4 终极恢复）
    pub fn rebuild_from_data_scan(
        &self,
        data_path: &Path,
        volume_id: u64,
        volume_size: u64,
    ) -> Result<usize> {
        use std::io::{Read, Seek, SeekFrom};

        info!(
            "Rebuilding VolumeMetadata from data scan: {:?}, volume_id={}",
            data_path, volume_id
        );

        if !data_path.exists() {
            warn!("Data file not found, skipping rebuild: {:?}", data_path);
            return Ok(0);
        }

        let mut file = std::fs::File::open(data_path)
            .map_err(|e| PowerFsError::Internal(format!("Failed to open data file: {}", e)))?;

        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);

        if file_size == 0 {
            warn!("Data file is empty, nothing to rebuild");
            return Ok(0);
        }

        let mut offset: u64 = 0;
        let mut count = 0;
        let mut stats = AllocationStats::default();

        // Needle header: [NeedleId(8B)][Size(4B)]
        const HEADER_SIZE: usize = 12;

        while offset + HEADER_SIZE as u64 <= file_size {
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| PowerFsError::Internal(format!("Seek failed: {}", e)))?;

            let mut header = [0u8; HEADER_SIZE];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(_) => break, // 到达文件末尾
            }

            let needle_id = u64::from_be_bytes([
                header[0], header[1], header[2], header[3], header[4], header[5], header[6],
                header[7],
            ]);
            let data_size = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);

            if needle_id == 0 || data_size == 0 {
                // 空洞或损坏区域，跳过
                offset += HEADER_SIZE as u64;
                continue;
            }

            // 创建 NeedleInfo
            let info = NeedleInfo {
                id: NeedleId(needle_id),
                volume_id: powerfs_common::types::VolumeId(volume_id),
                data_size,
                offset,
                checksum: 0,
                checksum_algorithm: ChecksumAlgorithm::CRC32C,
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

            // 写入 RocksDB
            self.put_needle(&info)?;

            let total_needle_size = HEADER_SIZE as u64 + data_size as u64 + 8; // +8 for footer
            stats.used_bytes += total_needle_size;
            stats.active_count += 1;
            stats.next_needle_id = stats.next_needle_id.max(needle_id + 1);
            stats.append_offset = offset + total_needle_size;

            offset += total_needle_size;
            count += 1;
        }

        stats.free_bytes = volume_size.saturating_sub(stats.used_bytes);
        stats.last_modified_at = Utc::now().timestamp();
        self.put_allocation(&stats)?;

        info!(
            "Rebuild complete: {} needles recovered, used={}, free={}, offset={}",
            count, stats.used_bytes, stats.free_bytes, stats.append_offset
        );

        Ok(count)
    }
}

impl Drop for VolumeMetadata {
    fn drop(&mut self) {
        // RocksDB 在 Arc 引用计数归零时自动关闭
        debug!("VolumeMetadata dropped");
    }
}

// ===== NeedleIndex trait 实现 =====

use crate::index::NeedleIndex;

impl NeedleIndex for VolumeMetadata {
    fn get(&self, needle_id: &NeedleId) -> Option<NeedleInfo> {
        match self.get_needle(needle_id) {
            Ok(info) => info,
            Err(e) => {
                warn!("NeedleIndex::get failed for needle {}: {}", needle_id.0, e);
                None
            }
        }
    }

    fn insert(&self, needle_id: NeedleId, info: NeedleInfo) {
        debug_assert_eq!(needle_id.0, info.id.0, "needle_id mismatch in insert");
        if let Err(e) = self.put_needle(&info) {
            warn!(
                "NeedleIndex::insert failed for needle {}: {}",
                needle_id.0, e
            );
        }
    }

    fn remove(&self, needle_id: &NeedleId) -> Option<NeedleInfo> {
        match self.delete_needle(needle_id) {
            Ok(info) => info,
            Err(e) => {
                warn!(
                    "NeedleIndex::remove failed for needle {}: {}",
                    needle_id.0, e
                );
                None
            }
        }
    }

    fn contains(&self, needle_id: &NeedleId) -> bool {
        match self.get_needle(needle_id) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(e) => {
                warn!(
                    "NeedleIndex::contains failed for needle {}: {}",
                    needle_id.0, e
                );
                false
            }
        }
    }

    fn len(&self) -> usize {
        match self.needle_count() {
            Ok(count) => count as usize,
            Err(e) => {
                warn!("NeedleIndex::len failed: {}", e);
                0
            }
        }
    }

    fn iter(&self) -> Vec<(NeedleId, NeedleInfo)> {
        match self.list_needles() {
            Ok(list) => list,
            Err(e) => {
                warn!("NeedleIndex::iter failed: {}", e);
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use powerfs_common::types::{ChecksumAlgorithm, NeedleId, VolumeId};

    #[test]
    fn test_volume_metadata_basic() {
        let dir = tempfile::tempdir().unwrap();
        let meta = VolumeMetadata::open(dir.path()).unwrap();

        // 初始状态：无配置
        assert!(meta.get_config().unwrap().is_none());

        // 写入配置
        let config = VolumeConfig {
            volume_id: 1,
            backend_type: 0,
            disk_uuid: "test-uuid".to_string(),
            fs_type: "ext4".to_string(),
            file_path: "/tmp/volume.data".to_string(),
            volume_size: 1024 * 1024 * 1024,
            needle_header_size: 12,
            needle_footer_size: 8,
            collection_name: "default".to_string(),
            replication_config: "000".to_string(),
            node_id: "node-1".to_string(),
            created_at: Utc::now().timestamp(),
        };
        meta.put_config(&config).unwrap();
        assert!(meta.get_config().unwrap().is_some());

        // 初始分配状态
        let stats = meta.get_allocation().unwrap();
        assert_eq!(stats.used_bytes, 0);
        assert_eq!(stats.active_count, 0);
    }

    #[test]
    fn test_needle_atomic_write() {
        let dir = tempfile::tempdir().unwrap();
        let meta = VolumeMetadata::open(dir.path()).unwrap();

        let volume_size = 1024 * 1024 * 1024u64;
        let info = NeedleInfo {
            id: NeedleId(1),
            volume_id: VolumeId(1),
            data_size: 100,
            offset: 0,
            checksum: 12345,
            checksum_algorithm: ChecksumAlgorithm::CRC32C,
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

        // 写入
        meta.write_needle_atomic(&info, 120, volume_size).unwrap();

        // 读取验证
        let loaded = meta.get_needle(&NeedleId(1)).unwrap().unwrap();
        assert_eq!(loaded.id.0, 1);
        assert_eq!(loaded.data_size, 100);

        // 分配状态验证
        let stats = meta.get_allocation().unwrap();
        assert_eq!(stats.used_bytes, 120);
        assert_eq!(stats.active_count, 1);
        assert_eq!(stats.next_needle_id, 2);
    }

    #[test]
    fn test_needle_atomic_delete() {
        let dir = tempfile::tempdir().unwrap();
        let meta = VolumeMetadata::open(dir.path()).unwrap();

        let volume_size = 1024 * 1024 * 1024u64;
        let info = NeedleInfo {
            id: NeedleId(1),
            volume_id: VolumeId(1),
            data_size: 100,
            offset: 0,
            checksum: 12345,
            checksum_algorithm: ChecksumAlgorithm::CRC32C,
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

        meta.write_needle_atomic(&info, 120, volume_size).unwrap();

        // 删除
        let deleted = meta
            .delete_needle_atomic(&NeedleId(1), volume_size)
            .unwrap();
        assert!(deleted.is_some());

        // needles CF 中应不存在
        assert!(meta.get_needle(&NeedleId(1)).unwrap().is_none());

        // deleted CF 中应有记录
        let deleted_list = meta.list_deleted().unwrap();
        assert_eq!(deleted_list.len(), 1);

        // 分配状态
        let stats = meta.get_allocation().unwrap();
        assert_eq!(stats.active_count, 0);
        assert_eq!(stats.deleted_count, 1);
    }

    #[test]
    fn test_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let meta = VolumeMetadata::open(dir.path()).unwrap();

        // 写入一些数据
        let config = VolumeConfig {
            volume_id: 1,
            backend_type: 0,
            disk_uuid: "test".to_string(),
            fs_type: "ext4".to_string(),
            file_path: "/tmp/data".to_string(),
            volume_size: 1024,
            needle_header_size: 12,
            needle_footer_size: 8,
            collection_name: "default".to_string(),
            replication_config: "000".to_string(),
            node_id: "node-1".to_string(),
            created_at: Utc::now().timestamp(),
        };
        meta.put_config(&config).unwrap();

        // 创建 checkpoint
        let checkpoint_dir = dir.path().join("checkpoint");
        meta.create_checkpoint(&checkpoint_dir).unwrap();
        assert!(checkpoint_dir.exists());

        // 从 checkpoint 恢复
        let restored = VolumeMetadata::open(&checkpoint_dir).unwrap();
        let config2 = restored.get_config().unwrap().unwrap();
        assert_eq!(config2.volume_id, 1);
    }
}
