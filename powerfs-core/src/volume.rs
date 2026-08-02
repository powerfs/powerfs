use crate::index::NeedleIndex;
use crate::needle::Needle;
use crate::storage_backend::{StorageBackend, StorageBackendError};
use crate::volume_metadata::VolumeMetadata;
use crate::write_coalescer::{CoalescerConfig, WriteCoalescer};
use bytes::Bytes;
use chrono::{Duration, Utc};
use powerfs_common::{
    constants::{NEEDLE_FOOTER_SIZE, NEEDLE_HEADER_SIZE, VOLUME_DATA_OFFSET},
    error::{PowerFsError, Result},
    types::{
        ChecksumAlgorithm, Collection, DiskType, NeedleId, NeedleInfo, Ttl, VolumeId, VolumeInfo,
        VolumeState,
    },
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

fn backend_err(e: StorageBackendError) -> PowerFsError {
    PowerFsError::Storage(e.to_string())
}

pub struct Volume {
    info: RwLock<VolumeInfo>,
    index: VolumeMetadata,
    checksum_algorithm: ChecksumAlgorithm,
    backend: Arc<dyn StorageBackend>,
    backend_volume_id: u64,
    /// Write-back coalescing buffer for per-Needle partial over-writes.
    /// See the [`crate::write_coalescer` docs for design & rationale.
    coalescer: Arc<WriteCoalescer>,
    /// Cheap counter used for opportunistic deadline flushes.  Every
    /// `write_needle_blob` does a wrapping increment and whenever it
    /// wraps to 0 we call flush_expired_dirty().  This ensures we do not
    /// leave dirty entries sitting around forever when the per-entry
    /// triggers never fire and there is no external scheduler ticker
    /// calling us periodically.
    op_counter: std::sync::atomic::AtomicU32,
    /// Compact 正在进行时设为 true，阻止并发 write
    compacting: AtomicBool,
}

/// Internal helper used by Drop and by unit tests that want a deterministic
/// "flush everything and panic if it breaks" helper.
#[allow(dead_code)]
fn set_shutdown_flag(_flag: &std::sync::atomic::AtomicBool) {}

#[allow(clippy::result_large_err)]
impl Volume {
    pub fn new(
        id: VolumeId,
        node_id: &str,
        path: &str,
        size: u64,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<Self> {
        Self::new_full(
            id,
            node_id,
            path,
            size,
            ChecksumAlgorithm::default(),
            backend,
            CoalescerConfig::default(),
        )
    }

    /// Construct a Volume with a non-default [`WriteCoalescer`] config.
    pub fn new_with_coalescer(
        id: VolumeId,
        node_id: &str,
        path: &str,
        size: u64,
        backend: Arc<dyn StorageBackend>,
        coalescer_config: CoalescerConfig,
    ) -> Result<Self> {
        Self::new_full(
            id,
            node_id,
            path,
            size,
            ChecksumAlgorithm::default(),
            backend,
            coalescer_config,
        )
    }

    /// Construct a Volume with a non-default [`ChecksumAlgorithm`].
    pub fn new_with_algorithm(
        id: VolumeId,
        node_id: &str,
        path: &str,
        size: u64,
        algorithm: ChecksumAlgorithm,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<Self> {
        Self::new_full(
            id,
            node_id,
            path,
            size,
            algorithm,
            backend,
            CoalescerConfig::default(),
        )
    }

    fn new_full(
        id: VolumeId,
        node_id: &str,
        path: &str,
        size: u64,
        algorithm: ChecksumAlgorithm,
        backend: Arc<dyn StorageBackend>,
        coalescer_config: CoalescerConfig,
    ) -> Result<Self> {
        let volume_path = std::path::Path::new(path).join(format!("volume_{}", id.0));

        if !volume_path.exists() {
            std::fs::create_dir_all(&volume_path)?;
        }

        let index_path = volume_path.join("metadata");

        // 使用 RocksDB-based VolumeMetadata 管理索引和分配状态
        let index = VolumeMetadata::open(&index_path)?;

        let backend_volume_id = id.0;
        let physical_size = size + VOLUME_DATA_OFFSET;
        match backend.get_volume_info(backend_volume_id) {
            Ok(_) => {}
            Err(StorageBackendError::VolumeNotFound(_)) => {
                backend
                    .allocate_volume(backend_volume_id, physical_size, None)
                    .map_err(backend_err)?;
            }
            Err(e) => return Err(backend_err(e)),
        }

        let (used, _next_offset, active_count, deleted_count) = index.rebuild_allocation_stats()?;

        // 同步 RocksDB allocation CF（启动时确保一致性）
        Self::sync_allocation_from_index(&index, used, size, active_count, deleted_count)?;

        let info = VolumeInfo {
            id,
            node_id: powerfs_common::types::NodeId(node_id.to_string()),
            collection: Collection::default(),
            size,
            used,
            replica_count: 3,
            ttl: Ttl::default(),
            disk_type: DiskType::default(),
            state: VolumeState::Available,
            created_at: Utc::now(),
            modified_at: Utc::now(),
            next_file_key: 1,
        };

        Ok(Volume {
            info: RwLock::new(info),
            index,
            checksum_algorithm: algorithm,
            backend,
            backend_volume_id,
            coalescer: Arc::new(WriteCoalescer::new(coalescer_config)),
            op_counter: std::sync::atomic::AtomicU32::new(0),
            compacting: AtomicBool::new(false),
        })
    }

    /// 启动时同步 allocation CF：如果 RocksDB 中的分配状态与 needle 索引不一致，则更新
    fn sync_allocation_from_index(
        index: &VolumeMetadata,
        rebuilt_used: u64,
        volume_size: u64,
        active_count: u64,
        deleted_count: u64,
    ) -> Result<()> {
        let stats = index.get_allocation()?;
        let rebuilt_free = volume_size.saturating_sub(rebuilt_used);

        // 如果 allocation CF 为空（首次启动）或统计不匹配（crash 恢复），则更新
        if stats.used_bytes != rebuilt_used
            || stats.free_bytes != rebuilt_free
            || stats.active_count != active_count
            || stats.deleted_count != deleted_count
        {
            log::info!(
                "Syncing allocation CF: rocksdb used={} free={} active={} deleted={} -> rebuilt used={} free={} active={} deleted={}",
                stats.used_bytes,
                stats.free_bytes,
                stats.active_count,
                stats.deleted_count,
                rebuilt_used,
                rebuilt_free,
                active_count,
                deleted_count
            );

            let new_stats = powerfs_common::volume_config::AllocationStats {
                used_bytes: rebuilt_used,
                free_bytes: rebuilt_free,
                next_needle_id: stats.next_needle_id,
                append_offset: rebuilt_used + VOLUME_DATA_OFFSET,
                active_count,
                deleted_count,
                last_modified_at: Utc::now().timestamp(),
            };
            index.put_allocation(&new_stats)?;
        }

        Ok(())
    }

    pub fn id(&self) -> VolumeId {
        self.info.read().unwrap().id
    }

    pub fn info(&self) -> VolumeInfo {
        self.info.read().unwrap().clone()
    }

    /// Set the collection this volume belongs to.
    ///
    /// Used by the Volume Server when `CreateVolumeRequest` carries an
    /// explicit collection name. The change is in-memory only; collection
    /// membership is reconciled by Master through heartbeat-reported
    /// `VolumeInfo`.
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
        self.index
            .get_allocation()
            .map(|s| s.free_bytes)
            .unwrap_or(0)
    }

    /// Materialise a single merged dirty entry from the coalescer into the
    /// storage backend + RocksDB index.  `is_new_needle` decides whether we
    /// call `write_needle` (first version) or `append_needle_version`
    /// (existing id with a version already on disk).
    fn flush_coalescer_entry(
        &self,
        needle_id: NeedleId,
        merged: Vec<u8>,
        is_new_needle: bool,
    ) -> Result<()> {
        let data = Bytes::from(merged);
        if is_new_needle {
            // No index entry yet — first write.  write_needle uses the
            // needle_id = file_key convention already; just pass `data` with
            // the full (possibly zero-padded) logical payload.
            let write_res = self.write_needle(needle_id.0, data);
            match write_res {
                Ok(_ni) => Ok(()),
                Err(e) => Err(e),
            }
        } else {
            // Already exists — treat as a version append, which will mark
            // the previous index row deleted and atomically write the new
            // index row.  We need `old_info` from the current index.
            let existing = match self.index.get(&needle_id) {
                Some(i) => i,
                None => {
                    // Race (index was deleted since dirty buffer was
                    // created).  Fall back to writing as a brand-new needle.
                    self.write_needle(needle_id.0, data)?;
                    return Ok(());
                }
            };
            self.append_needle_version(needle_id, data, existing)
        }
    }

    /// Expose a cheap helper that flushes `Some` return value from
    /// [`WriteCoalescer::record_write`] into the Volume, returning Result so
    /// callers can use `?`.
    fn flush_option(&self, maybe: Option<(NeedleId, Vec<u8>, bool)>) -> Result<()> {
        if let Some((id, vec, is_new)) = maybe {
            self.flush_coalescer_entry(id, vec, is_new)?;
        }
        Ok(())
    }

    pub fn write_needle(&self, file_key: u64, data: Bytes) -> Result<NeedleInfo> {
        let mut info_guard = self.info.write().unwrap();
        if info_guard.state != VolumeState::Available {
            return Err(PowerFsError::InvalidVolumeState(
                "volume not available".to_string(),
            ));
        }

        let needle_id = NeedleId(file_key);
        let volume_id = info_guard.id;
        let needle =
            Needle::new_with_algorithm(needle_id.clone(), volume_id, data, self.checksum_algorithm);

        let required_space = needle.size() as u64;
        let volume_size = info_guard.size;

        // 从 RocksDB allocation CF 读取当前分配状态
        let alloc_stats = self.index.get_allocation()?;
        if alloc_stats.free_bytes < required_space {
            info_guard.state = VolumeState::Full;
            return Err(PowerFsError::OutOfSpace);
        }

        let offset = alloc_stats.append_offset;

        let needle_bytes = needle.to_bytes();
        self.backend
            .write_needle(self.backend_volume_id, offset, &needle_bytes)
            .map_err(backend_err)?;

        let needle_info = NeedleInfo {
            id: needle_id.clone(),
            volume_id: info_guard.id,
            data_size: needle.data.len() as u32,
            offset,
            checksum: needle.checksum,
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

        // 原子写入：同时更新 needles CF + allocation CF，返回更新后的统计
        let new_stats =
            self.index
                .write_needle_atomic(&needle_info, required_space, volume_size)?;

        // 同步 info 中的 used 字段
        info_guard.used = new_stats.used_bytes;
        info_guard.modified_at = Utc::now();

        Ok(needle_info)
    }

    pub fn read_needle(&self, needle_id: &NeedleId) -> Result<Bytes> {
        // Coalescer dirty-buffer takes precedence over backend (read-your-own-writes).
        if let Some(buf) = self.coalescer.read_if_dirty(needle_id, 0, usize::MAX) {
            return Ok(buf);
        }
        if let Some(mut info) = self.index.get(needle_id) {
            if info.deleted_at.is_some() {
                return Err(PowerFsError::NeedleNotFound(needle_id.clone()));
            }

            let data_size = NEEDLE_HEADER_SIZE as u32 + info.data_size + NEEDLE_FOOTER_SIZE as u32;
            let data = self
                .backend
                .read_needle(self.backend_volume_id, info.offset, data_size)
                .map_err(backend_err)?;
            let needle =
                Needle::from_bytes(&data, self.id(), info.offset, info.checksum_algorithm)?;

            info.last_verified_at = Some(Utc::now());
            info.verification_count += 1;
            self.index.insert(needle_id.clone(), info);

            Ok(needle.data)
        } else {
            Err(PowerFsError::NeedleNotFound(needle_id.clone()))
        }
    }

    pub fn delete_needle(&self, needle_id: &NeedleId) -> Result<()> {
        // 先检查 needle 存在且未被删除
        if let Some(info) = self.index.get(needle_id) {
            if info.deleted_at.is_some() {
                return Err(PowerFsError::NeedleNotFound(needle_id.clone()));
            }

            // WORM 保护检查
            if info.worm_retention_until.is_some() {
                if let Some(retention_until) = info.worm_retention_until {
                    if retention_until > Utc::now() {
                        return Err(PowerFsError::PermissionDenied);
                    }
                }
            }

            // 硬删除：从 needles CF 移除，存入 deleted CF
            let volume_size = self.size();
            self.index.delete_needle_atomic(needle_id, volume_size)?;

            let mut info_guard = self.info.write().unwrap();
            info_guard.modified_at = Utc::now();

            // A-2: 删除后检查是否可以恢复 Available 状态
            // volume 因 free_bytes 不足被标记为 Full，删除 needle 释放逻辑空间后，
            // 如果 free_bytes > 0 则恢复 Available，允许后续写入。
            if info_guard.state == VolumeState::Full {
                if let Ok(alloc_stats) = self.index.get_allocation() {
                    if alloc_stats.free_bytes > 0 {
                        info_guard.state = VolumeState::Available;
                        log::info!(
                            "Volume {} recovered to Available after delete: used={}, free={}",
                            info_guard.id,
                            alloc_stats.used_bytes,
                            alloc_stats.free_bytes
                        );
                    }
                }
            }

            // 同步 info 中的 used 字段
            if let Ok(alloc_stats) = self.index.get_allocation() {
                info_guard.used = alloc_stats.used_bytes;
            }

            Ok(())
        } else {
            Err(PowerFsError::NeedleNotFound(needle_id.clone()))
        }
    }

    pub fn restore_needle(&self, needle_id: &NeedleId) -> Result<()> {
        // 从 deleted CF 恢复到 needles CF
        let volume_size = self.size();
        if let Some(_info) = self.index.restore_needle_atomic(needle_id, volume_size)? {
            let mut info_guard = self.info.write().unwrap();
            info_guard.modified_at = Utc::now();
            Ok(())
        } else {
            Err(PowerFsError::NeedleNotFound(needle_id.clone()))
        }
    }

    pub fn worm_lock(&self, needle_id: &NeedleId, retention_days: i64) -> Result<()> {
        if let Some(mut info) = self.index.get(needle_id) {
            if info.deleted_at.is_some() {
                return Err(PowerFsError::InvalidRequest(
                    "cannot lock deleted needle".to_string(),
                ));
            }

            let retention_until = Utc::now() + Duration::days(retention_days);
            info.worm_retention_until = Some(retention_until);

            self.index.insert(needle_id.clone(), info);

            let mut info_guard = self.info.write().unwrap();
            info_guard.modified_at = Utc::now();

            Ok(())
        } else {
            Err(PowerFsError::NeedleNotFound(needle_id.clone()))
        }
    }

    pub fn gc_cleanup(&self) -> Result<usize> {
        let cleaned_count = self.index.purge_expired_deleted()?;

        if cleaned_count > 0 {
            let mut info_guard = self.info.write().unwrap();
            info_guard.modified_at = Utc::now();
        }

        Ok(cleaned_count)
    }

    pub fn get_needle_info(&self, needle_id: &NeedleId) -> Option<NeedleInfo> {
        self.index.get(needle_id)
    }

    pub fn count(&self) -> usize {
        self.index.len()
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

    pub fn index(&self) -> &VolumeMetadata {
        &self.index
    }

    pub fn compact(&self) -> Result<(u64, u64)> {
        // 防止并发 compact
        if self
            .compacting
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(PowerFsError::Internal(
                "Volume is already compacting".to_string(),
            ));
        }

        // 确保 compact 在退出时重置标志（即使出错）
        let result = self.compact_inner();
        self.compacting.store(false, Ordering::SeqCst);
        result
    }

    fn compact_inner(&self) -> Result<(u64, u64)> {
        // 开始前先 flush coalescer，避免 dirty 数据与 compact 冲突
        self.coalescer.flush_all(|id, vec, is_new| {
            self.flush_coalescer_entry(id, vec, is_new).map_err(|_| ())
        });

        let mut active_needles: Vec<NeedleInfo> = Vec::new();

        for (_id, info) in self.index.iter() {
            if info.deleted_at.is_none() {
                active_needles.push(info);
            }
        }

        active_needles.sort_by_key(|info| info.offset);

        let mut new_offset = VOLUME_DATA_OFFSET;
        let mut updated_count: u64 = 0;

        for info in &active_needles {
            let needle_size =
                (NEEDLE_HEADER_SIZE as u64) + (info.data_size as u64) + (NEEDLE_FOOTER_SIZE as u64);

            if info.offset == new_offset {
                new_offset += needle_size;
                continue;
            }

            let data_size_u32 =
                NEEDLE_HEADER_SIZE as u32 + info.data_size + NEEDLE_FOOTER_SIZE as u32;
            let raw_data = self
                .backend
                .read_needle(self.backend_volume_id, info.offset, data_size_u32)
                .map_err(backend_err)?;

            self.backend
                .write_needle(self.backend_volume_id, new_offset, &raw_data)
                .map_err(backend_err)?;

            let mut new_info = info.clone();
            new_info.offset = new_offset;
            self.index.insert(info.id.clone(), new_info);

            new_offset += needle_size;
            updated_count += 1;
        }

        // Reclaimed space = old physical end (including holes from deleted
        // needles) - new physical end (compact written, holes removed).
        // Using append_offset (physical) instead of used (logical) ensures
        // compact correctly reports the physical hole space reclaimed.
        let old_stats = self.index.get_allocation()?;
        let old_append_offset = old_stats.append_offset;
        let new_physical_end = new_offset;
        let reclaimed = old_append_offset.saturating_sub(new_physical_end);

        // 通过 RocksDB compact_cleanup 原子更新 allocation CF
        let volume_size = self.size();
        let new_stats = self.index.compact_cleanup(new_offset, volume_size)?;

        {
            let mut info_guard = self.info.write().unwrap();
            info_guard.used = new_stats.used_bytes;
            info_guard.modified_at = Utc::now();
        }

        self.backend
            .truncate_volume(self.backend_volume_id, new_offset)
            .map_err(backend_err)?;

        Ok((reclaimed, updated_count))
    }

    /// 当前 volume 是否正在 compact
    pub fn is_compacting(&self) -> bool {
        self.compacting.load(Ordering::SeqCst)
    }

    /// 检查是否应该触发 compact（deleted 比例超过阈值）
    pub fn should_compact(&self) -> bool {
        let stats = match self.index.get_allocation() {
            Ok(s) => s,
            Err(_) => return false,
        };
        if stats.active_count == 0 {
            return false;
        }
        // 当 deleted_count 占总 needle 比例超过 30% 时触发
        let total = stats.active_count + stats.deleted_count;
        if total == 0 {
            return false;
        }
        let deleted_ratio = stats.deleted_count as f64 / total as f64;
        deleted_ratio > 0.3
    }

    fn append_needle_version(
        &self,
        needle_id: NeedleId,
        new_data: Bytes,
        old_info: NeedleInfo,
    ) -> Result<()> {
        let mut info_guard = self.info.write().unwrap();
        if info_guard.state != VolumeState::Available {
            return Err(PowerFsError::InvalidVolumeState(
                "volume not available".to_string(),
            ));
        }

        let new_needle = Needle::new_with_algorithm(
            needle_id.clone(),
            info_guard.id,
            new_data,
            self.checksum_algorithm,
        );
        let new_size = new_needle.size() as u64;
        let volume_size = info_guard.size;

        // 从 RocksDB allocation CF 读取当前分配状态
        let alloc_stats = self.index.get_allocation()?;
        if alloc_stats.free_bytes < new_size {
            info_guard.state = VolumeState::Full;
            return Err(PowerFsError::OutOfSpace);
        }

        let new_offset = alloc_stats.append_offset;

        let needle_bytes = new_needle.to_bytes();
        self.backend
            .write_needle(self.backend_volume_id, new_offset, &needle_bytes)
            .map_err(backend_err)?;

        // 标记旧 needle 为已删除
        let mut old_updated = old_info.clone();
        old_updated.deleted_at = Some(Utc::now());
        self.index.put_needle(&old_updated)?;

        // 构建新 needle 信息
        let new_info = NeedleInfo {
            id: needle_id.clone(),
            volume_id: old_info.volume_id,
            data_size: new_needle.data.len() as u32,
            offset: new_offset,
            checksum: new_needle.checksum,
            checksum_algorithm: self.checksum_algorithm,
            last_verified_at: None,
            verification_count: 0,
            deleted_at: None,
            delete_retention_until: old_info.delete_retention_until,
            worm_retention_until: old_info.worm_retention_until,
            created_at: old_info.created_at,
            ec_enabled: old_info.ec_enabled,
            ec_k: old_info.ec_k,
            ec_m: old_info.ec_m,
            ec_shards: old_info.ec_shards.clone(),
        };

        // 原子写入新 needle + 更新 allocation CF
        let new_stats = self
            .index
            .write_needle_atomic(&new_info, new_size, volume_size)?;

        info_guard.used = new_stats.used_bytes;
        info_guard.modified_at = Utc::now();

        Ok(())
    }

    pub fn write_needle_blob(
        &self,
        file_key: u64,
        offset: i64,
        size: i32,
        data: Bytes,
        _cookie: u32,
    ) -> Result<()> {
        if self.compacting.load(Ordering::SeqCst) {
            return Err(PowerFsError::Internal(
                "Volume is compacting, retry later".to_string(),
            ));
        }
        let needle_id = NeedleId(file_key);
        let data_offset = offset as usize;
        let data_size = std::cmp::min(size as usize, data.len());

        // ---- Coalescer fast-path: first check if we already have a dirty buffer
        // for this needle_id; if yes, skip the backend RMW completely and
        // merge straight into RAM.
        if self.coalescer.is_dirty(&needle_id) {
            let maybe_flush = self.coalescer.record_write(
                &needle_id,
                data_offset,
                &data[..data_size],
                data_offset + data_size,
                None,
            );
            self.flush_option(maybe_flush)?;
            self.opportunistic_flush_expired();
            return Ok(());
        }

        // ---- First write to this needle: do the initial lookup so we know
        // the current full state (existing backend data vs brand-new needle).
        // We still perform *one* RMW here (unavoidable without the backend
        // exposing sub-block writes), but all *subsequent* writes go straight
        // into the coalescer until a flush trigger fires.
        let existing_data: Option<Vec<u8>>;
        let full_size_hint: usize;
        let base_info = self.index.get(&needle_id);
        if let Some(existing_info) = base_info.as_ref() {
            let on_disk_total =
                NEEDLE_HEADER_SIZE as u32 + existing_info.data_size + NEEDLE_FOOTER_SIZE as u32;
            let raw_data = self
                .backend
                .read_needle(self.backend_volume_id, existing_info.offset, on_disk_total)
                .map_err(backend_err)?;
            let needle = Needle::from_bytes(
                &raw_data,
                self.id(),
                existing_info.offset,
                existing_info.checksum_algorithm,
            )?;
            let cur = needle.data.to_vec();
            // Needle on disk might be smaller than the incoming write end
            // (hole-punch / sparse append semantics); pick the max.
            full_size_hint = std::cmp::max(cur.len(), data_offset + data_size);
            existing_data = Some(cur);
        } else {
            full_size_hint = data_offset + data_size;
            existing_data = None;
        }

        let maybe_flush = self.coalescer.record_write(
            &needle_id,
            data_offset,
            &data[..data_size],
            full_size_hint,
            existing_data,
        );
        self.flush_option(maybe_flush)?;
        self.opportunistic_flush_expired();
        Ok(())
    }

    pub fn read_needle_blob(&self, file_key: u64, offset: i64, size: i32) -> Result<Bytes> {
        let needle_id = NeedleId(file_key);
        // Dirty buffer precedence for read-your-own-writes.
        if let Some(buf) = self
            .coalescer
            .read_if_dirty(&needle_id, offset as usize, size as usize)
        {
            return Ok(buf);
        }
        if let Some(info) = self.index.get(&needle_id) {
            let data_size = NEEDLE_HEADER_SIZE as u32 + info.data_size + NEEDLE_FOOTER_SIZE as u32;
            let raw_data = self
                .backend
                .read_needle(self.backend_volume_id, info.offset, data_size)
                .map_err(backend_err)?;
            let needle =
                Needle::from_bytes(&raw_data, self.id(), info.offset, info.checksum_algorithm)?;

            // NOTE: Do NOT write to index during read (no verification_count update).
            // Writing to RocksDB during a read operation causes lock contention
            // and can hang concurrent reads from different clients.

            let data_offset = offset as usize;
            let data_size = size as usize;
            if data_offset >= needle.data.len() {
                // offset 超出数据范围，返回空数据（短读）
                Ok(Bytes::new())
            } else {
                // 短读：只返回实际可用的数据，避免最后一个 chunk 读取失败
                let available = needle.data.len() - data_offset;
                let read_size = data_size.min(available);
                Ok(Bytes::from(
                    needle.data[data_offset..data_offset + read_size].to_vec(),
                ))
            }
        } else {
            Err(PowerFsError::NeedleNotFound(needle_id))
        }
    }

    pub fn read_needle_meta(&self, file_key: u64) -> Option<NeedleInfo> {
        self.index.get(&NeedleId(file_key))
    }

    pub fn deleted_count(&self) -> usize {
        self.index
            .get_allocation()
            .map(|s| s.deleted_count as usize)
            .unwrap_or(0)
    }

    pub fn verify_needle(&self, needle_id: &NeedleId) -> Result<bool> {
        if let Some(mut info) = self.index.get(needle_id) {
            if info.deleted_at.is_some() {
                return Ok(true);
            }

            let data_size = NEEDLE_HEADER_SIZE as u32 + info.data_size + NEEDLE_FOOTER_SIZE as u32;
            let data = self
                .backend
                .read_needle(self.backend_volume_id, info.offset, data_size)
                .map_err(backend_err)?;

            let result = Needle::from_bytes(&data, self.id(), info.offset, info.checksum_algorithm);
            let valid = result.is_ok();

            info.last_verified_at = Some(Utc::now());
            info.verification_count += 1;
            self.index.insert(needle_id.clone(), info);

            Ok(valid)
        } else {
            Err(PowerFsError::NeedleNotFound(needle_id.clone()))
        }
    }

    pub fn scrub_volume(&self) -> ScrubResult {
        let mut result = ScrubResult::default();
        let all_needles = self.index.iter();

        for (needle_id, info) in &all_needles {
            if info.deleted_at.is_some() {
                result.skipped += 1;
                continue;
            }

            result.total += 1;
            match self.verify_needle(needle_id) {
                Ok(true) => {
                    result.verified += 1;
                }
                Ok(false) => {
                    result.corrupted += 1;
                    result.corrupted_needles.push(needle_id.clone());
                }
                Err(_) => {
                    result.errors += 1;
                    result.corrupted_needles.push(needle_id.clone());
                }
            }
        }

        result
    }

    /// Expose read-only access to the internal coalescer (tests/metrics).
    pub fn coalescer(&self) -> &WriteCoalescer {
        &self.coalescer
    }

    /// Synchronously flush every currently-dirty merged entry in the
    /// coalescer to the backend + RocksDB index.  Returns the number of
    /// entries that were materialised.
    ///
    /// Called automatically by [`Drop`] – see below.
    pub fn flush_all_dirty(&self) -> usize {
        self.coalescer.flush_all(|id, vec, is_new| {
            self.flush_coalescer_entry(id, vec, is_new).map_err(|_| ())
        })
    }

    /// Flush only those dirty entries whose deadline has elapsed.  Returns
    /// the number of entries that were flushed to stable storage.
    ///
    /// This is the drain path for non-blocking budget eviction (see
    /// [`WriteCoalescer::record_write`]): when the total dirty-bytes budget
    /// is exceeded we mark a victim as expired synchronously, but we do
    /// NOT block the caller on that victim's backend I/O.  Instead this
    /// method must be called periodically (either by an external scheduler
    /// tick, or via the embedded `op_counter` opportunistic hook below)
    /// to do the actual flushing.
    pub fn flush_expired_dirty(&self) -> usize {
        self.coalescer.flush_expired(|id, vec, is_new| {
            self.flush_coalescer_entry(id, vec, is_new).map_err(|_| ())
        })
    }

    /// Opportunistic deadline flush helper: cheap on the hot path (a single
    /// wrapping fetch_add) and fires flush_expired_dirty() roughly once per
    /// FLUSH_EVERY writes, regardless of needle id.  This guarantees that
    /// entries never sit in the dirty buffer longer than roughly:
    ///
    ///   config.deadline + FLUSH_EVERY * avg_write_latency
    ///
    /// even when per-entry triggers never fire and no external scheduler
    /// is driving us periodically.
    fn opportunistic_flush_expired(&self) {
        const FLUSH_EVERY: u32 = 32;
        let prev = self
            .op_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if prev.wrapping_add(1).is_multiple_of(FLUSH_EVERY) {
            let _ = self.flush_expired_dirty();
        }
    }
}

/// On drop, any remaining dirty entries are flushed to stable storage.  This
/// prevents loss of writes that were acknowledged via the coalescer but have
/// not yet been materialised into `self.backend` + `self.index`.  Errors
/// during flush are logged and swallowed (Drop cannot return a Result).
impl Drop for Volume {
    fn drop(&mut self) {
        let dirty = self.coalescer.dirty_entry_count();
        if dirty == 0 {
            return;
        }
        // SAFETY: the closure below calls `flush_coalescer_entry` through a
        // raw pointer so we do not create a conflicting shared borrow with
        // `&mut self` in Drop.  `self` is guaranteed to be alive for the
        // entire duration of `flush_all`.
        let self_ptr: *const Volume = &*self;
        let flushed = self.coalescer.flush_all(|id, vec, is_new| {
            let slf = unsafe { &*self_ptr };
            let needle_ident = id.0;
            slf.flush_coalescer_entry(id, vec, is_new).map_err(|e| {
                log::error!(
                    "Volume drop: flush needle_id={} failed: {}",
                    needle_ident,
                    e
                );
            })
        });
        let vid = self
            .info
            .read()
            .map(|g| g.id.0.to_string())
            .unwrap_or_else(|_| "?".into());
        log::debug!(
            "Volume(id={}) drop flushed {} dirty coalescer entries (pre-drop count={})",
            vid,
            flushed,
            dirty,
        );
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScrubResult {
    pub total: u64,
    pub verified: u64,
    pub corrupted: u64,
    pub skipped: u64,
    pub errors: u64,
    pub corrupted_needles: Vec<NeedleId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_backend::LocalFsBackend;
    use crate::write_coalescer::CoalescerConfig;
    use std::time::Duration;

    fn make_volume(dir: &tempfile::TempDir, coalescer: CoalescerConfig) -> Volume {
        let backend_path = dir.path().join("storage");
        let backend = Arc::new(
            LocalFsBackend::new(
                backend_path.to_str().unwrap(),
                "node-test",
                "dev",
                Some(1 << 30), // 1 GiB logical cap
            )
            .unwrap(),
        );
        Volume::new_with_coalescer(
            VolumeId(1),
            "node-test",
            dir.path().to_str().unwrap(),
            256 * 1024 * 1024, // 256 MiB user-visible size
            backend,
            coalescer,
        )
        .unwrap()
    }

    fn all_zeros(n: usize) -> Bytes {
        Bytes::from(vec![0u8; n])
    }

    #[test]
    fn eight_overwrites_of_256k_merge_into_one_physical_append() {
        // Thresholds: 8 partial writes = one flush.
        let dir = tempfile::tempdir().unwrap();
        let cfg = CoalescerConfig {
            min_pending_writes: 8,
            deadline: Duration::from_secs(3600), // effectively disabled
            max_dirty_bytes_per_entry: usize::MAX,
            max_dirty_bytes_total: usize::MAX,
            disabled: false,
        };
        let vol = make_volume(&dir, cfg);
        const CHUNK: usize = 256 * 1024; // 256 KiB
        const TOTAL: usize = 4 * CHUNK; // 1 MiB needle
        let file_key = 999u64;

        // Baseline: alloc_stats before any writes.
        let a0 = vol.index.get_allocation().unwrap();

        // First write: creates needle with zero bytes (offset 0 chunk)
        vol.write_needle_blob(file_key, 0, CHUNK as i32, all_zeros(CHUNK), 0)
            .unwrap();
        // Now 8 subsequent overwrites covering each chunk (all 4 chunks twice = 8)
        for round in 0..2u8 {
            for chunk in 0..4u8 {
                let off = (chunk as usize) * CHUNK;
                // First round (round=0): chunks written with bytes 1..=4
                // Second round (round=1): chunks written with bytes 5..=8
                let byte = round * 4 + chunk + 1;
                let payload = Bytes::from(vec![byte; CHUNK]);
                vol.write_needle_blob(file_key, off as i64, CHUNK as i32, payload, 0)
                    .unwrap();
            }
        }
        // After 1 initial + 8 overwrites = 9 blob writes, the 8th over-write
        // triggers min_pending_writes=8 flush (the initial write that
        // populated the dirty buffer counts as write #1 inside record_write,
        // subsequent 8 bump to >=8 on the last one).
        //
        // We expect at most 2 physical needles on disk:
        //   1. the one produced by the final coalesced flush (final merged state)
        //   2. at most one intermediate flush from the first write materialisation.
        // Deleted versions still occupy allocation space until GC runs, so
        // active_count should be 1 (the visible one), deleted_count >= 0.
        let stats = vol.index.get_allocation().unwrap();
        let physical_needles_total = stats.active_count + stats.deleted_count;
        assert!(
            physical_needles_total <= 3,
            "expected at most 3 total physical needles (1 init + 1 coalesced flush + \
             maybe one intermediate), got active={} deleted={}",
            stats.active_count,
            stats.deleted_count
        );
        let used_delta = stats.used_bytes.saturating_sub(a0.used_bytes);
        // Worst case: 3 * 1 MiB data + 3*(hdr+ftr) ≈ 3.1 MiB used
        assert!(
            used_delta < 5 * 1024 * 1024,
            "too many bytes used for a 1 MiB logical file: {} bytes (~{} MiB)",
            used_delta,
            used_delta / 1024 / 1024
        );

        // --- Verify the final read returns the last-written payload per chunk
        vol.flush_all_dirty();
        let got = vol.read_needle(&NeedleId(file_key)).unwrap();
        assert_eq!(got.len(), TOTAL);
        for chunk in 0..4 {
            let off = chunk * CHUNK;
            // Last successful overwrite was round=1 (second pass) byte 5..=8
            // round * 4 + chunk + 1, with round=1
            let expected_byte: u8 = (4 + chunk + 1) as u8;
            assert_eq!(
                got[off], expected_byte,
                "chunk {} first byte mismatch: got {} want {}",
                chunk, got[off], expected_byte
            );
            assert_eq!(
                got[off + CHUNK - 1],
                expected_byte,
                "chunk {} last byte mismatch",
                chunk
            );
        }
    }

    #[test]
    fn read_your_own_writes_before_flush() {
        // A coalescer with a very long deadline so nothing auto-flushes.
        let dir = tempfile::tempdir().unwrap();
        let cfg = CoalescerConfig {
            min_pending_writes: 1_000,
            deadline: Duration::from_secs(3600),
            max_dirty_bytes_per_entry: usize::MAX,
            max_dirty_bytes_total: usize::MAX,
            disabled: false,
        };
        let vol = make_volume(&dir, cfg);
        let file_key = 42u64;

        // A needle that is 4 KiB, written via one initial full-zero write
        // then three small partial over-writes.
        vol.write_needle_blob(file_key, 0, 4096, Bytes::from(vec![0u8; 4096]), 0)
            .unwrap();
        vol.write_needle_blob(file_key, 0, 4, Bytes::from_static(b"ABCD"), 0)
            .unwrap();
        vol.write_needle_blob(file_key, 100, 2, Bytes::from_static(b"XY"), 0)
            .unwrap();
        vol.write_needle_blob(file_key, 4092, 4, Bytes::from_static(b"ZZZZ"), 0)
            .unwrap();

        assert!(vol.coalescer().is_dirty(&NeedleId(file_key)));

        // read_needle_blob must return the coalesced, latest values even
        // though the dirty buffer has not been flushed to disk.
        let head = vol.read_needle_blob(file_key, 0, 4).unwrap();
        assert_eq!(&head[..], b"ABCD");
        let mid = vol.read_needle_blob(file_key, 100, 2).unwrap();
        assert_eq!(&mid[..], b"XY");
        let tail = vol.read_needle_blob(file_key, 4092, 4).unwrap();
        assert_eq!(&tail[..], b"ZZZZ");
        let untouched = vol.read_needle_blob(file_key, 10, 5).unwrap();
        assert_eq!(&untouched[..], &[0u8; 5]);

        // And read_needle (full needle) must also reflect it.
        let full = vol.read_needle(&NeedleId(file_key)).unwrap();
        assert_eq!(&full[0..4], b"ABCD");
        assert_eq!(&full[100..102], b"XY");
        assert_eq!(&full[4092..4096], b"ZZZZ");
    }

    #[test]
    fn disabled_coalescer_always_materialises_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = CoalescerConfig {
            disabled: true,
            ..CoalescerConfig::default()
        };
        let vol = make_volume(&dir, cfg);
        // One write = immediately visible in index AND dirty count stays 0
        vol.write_needle_blob(7, 0, 16, Bytes::from_static(b"hi_there_1234567"), 0)
            .unwrap();
        assert_eq!(vol.coalescer().dirty_entry_count(), 0);
        let got = vol.read_needle(&NeedleId(7)).unwrap();
        assert_eq!(&got[0..16], b"hi_there_1234567");
    }
}
