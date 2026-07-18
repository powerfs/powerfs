use crate::index::{NeedleIndex, PersistentIndex};
use crate::needle::Needle;
use crate::storage_backend::{StorageBackend, StorageBackendError};
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
use std::sync::{Arc, RwLock};

fn backend_err(e: StorageBackendError) -> PowerFsError {
    PowerFsError::Storage(e.to_string())
}

pub struct Volume {
    info: RwLock<VolumeInfo>,
    index: Box<dyn NeedleIndex>,
    free_space: RwLock<u64>,
    next_offset: RwLock<u64>,
    checksum_algorithm: ChecksumAlgorithm,
    backend: Arc<dyn StorageBackend>,
    backend_volume_id: u64,
}

#[allow(clippy::result_large_err)]
impl Volume {
    pub fn new(
        id: VolumeId,
        node_id: &str,
        path: &str,
        size: u64,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<Self> {
        Self::new_with_algorithm(
            id,
            node_id,
            path,
            size,
            ChecksumAlgorithm::default(),
            backend,
        )
    }

    pub fn new_with_algorithm(
        id: VolumeId,
        node_id: &str,
        path: &str,
        size: u64,
        algorithm: ChecksumAlgorithm,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<Self> {
        let volume_path = std::path::Path::new(path).join(format!("volume_{}", id.0));

        if !volume_path.exists() {
            std::fs::create_dir_all(&volume_path)?;
        }

        let index_path = volume_path.join("index");

        let index: Box<dyn NeedleIndex> =
            Box::new(PersistentIndex::new(index_path.to_str().unwrap())?);

        let backend_volume_id = id.0 as u64;
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

        let (used, next_offset) = Self::rebuild_metadata_from_index(index.as_ref(), size);
        let free_space = size.saturating_sub(used);

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
            free_space: RwLock::new(free_space),
            next_offset: RwLock::new(next_offset),
            checksum_algorithm: algorithm,
            backend,
            backend_volume_id,
        })
    }

    fn rebuild_metadata_from_index(index: &dyn NeedleIndex, volume_size: u64) -> (u64, u64) {
        let mut max_end: u64 = VOLUME_DATA_OFFSET;

        for (_needle_id, info) in index.iter() {
            let needle_size =
                (NEEDLE_HEADER_SIZE as u64) + (info.data_size as u64) + (NEEDLE_FOOTER_SIZE as u64);
            let end = info.offset.saturating_add(needle_size);
            if end > max_end {
                max_end = end;
            }
        }

        let used = max_end.saturating_sub(VOLUME_DATA_OFFSET);
        let used = if used > volume_size {
            volume_size
        } else {
            used
        };

        (used, max_end)
    }

    pub fn id(&self) -> VolumeId {
        self.info.read().unwrap().id
    }

    pub fn info(&self) -> VolumeInfo {
        self.info.read().unwrap().clone()
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
        *self.free_space.read().unwrap()
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
        let mut free_space_guard = self.free_space.write().unwrap();
        if *free_space_guard < required_space {
            info_guard.state = VolumeState::Full;
            return Err(PowerFsError::OutOfSpace);
        }

        let mut next_offset_guard = self.next_offset.write().unwrap();
        let offset = *next_offset_guard;

        let needle_bytes = needle.to_bytes();
        self.backend
            .write_needle(self.backend_volume_id, offset, &needle_bytes)
            .map_err(backend_err)?;

        *next_offset_guard += required_space;
        *free_space_guard -= required_space;
        info_guard.used += required_space;
        info_guard.modified_at = Utc::now();

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

        drop(next_offset_guard);
        drop(free_space_guard);
        drop(info_guard);

        self.index.insert(needle_id, needle_info.clone());

        Ok(needle_info)
    }

    pub fn read_needle(&self, needle_id: &NeedleId) -> Result<Bytes> {
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
        if let Some(mut info) = self.index.get(needle_id) {
            if info.deleted_at.is_some() {
                return Err(PowerFsError::NeedleNotFound(needle_id.clone()));
            }

            if info.worm_retention_until.is_some() {
                if let Some(retention_until) = info.worm_retention_until {
                    if retention_until > Utc::now() {
                        return Err(PowerFsError::PermissionDenied);
                    }
                }
            }

            info.deleted_at = Some(Utc::now());
            info.delete_retention_until = Some(Utc::now() + Duration::days(7));

            self.index.insert(needle_id.clone(), info);

            let mut info_guard = self.info.write().unwrap();
            info_guard.modified_at = Utc::now();

            Ok(())
        } else {
            Err(PowerFsError::NeedleNotFound(needle_id.clone()))
        }
    }

    pub fn restore_needle(&self, needle_id: &NeedleId) -> Result<()> {
        if let Some(mut info) = self.index.get(needle_id) {
            if info.deleted_at.is_none() {
                return Err(PowerFsError::InvalidRequest(
                    "needle is not deleted".to_string(),
                ));
            }

            if let Some(retention_until) = info.delete_retention_until {
                if retention_until < Utc::now() {
                    return Err(PowerFsError::NeedleNotFound(needle_id.clone()));
                }
            }

            info.deleted_at = None;
            info.delete_retention_until = None;

            self.index.insert(needle_id.clone(), info);

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
        let mut cleaned_count = 0;
        let now = Utc::now();

        let needles = self.index.iter();
        for (needle_id, info) in needles {
            if let Some(retention_until) = info.delete_retention_until {
                if retention_until < now && self.index.remove(&needle_id).is_some() {
                    let mut info_guard = self.info.write().unwrap();
                    info_guard.modified_at = Utc::now();
                    cleaned_count += 1;
                }
            }
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

    pub fn index(&self) -> &dyn NeedleIndex {
        self.index.as_ref()
    }

    pub fn compact(&self) -> Result<(u64, u64)> {
        let mut active_needles: Vec<NeedleInfo> = Vec::new();

        for (_id, info) in self.index.iter() {
            if info.deleted_at.is_none() {
                active_needles.push(info);
            }
        }

        active_needles.sort_by_key(|info| info.offset);

        let mut new_offset = VOLUME_DATA_OFFSET;
        let mut reclaimed: u64 = 0;
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

        let old_used = self.used();
        let new_used = new_offset.saturating_sub(VOLUME_DATA_OFFSET);
        if new_used < old_used {
            reclaimed = old_used - new_used;
        }

        {
            let mut info_guard = self.info.write().unwrap();
            info_guard.used = new_used;
            info_guard.modified_at = Utc::now();
        }
        *self.next_offset.write().unwrap() = new_offset;
        *self.free_space.write().unwrap() = self.size().saturating_sub(new_used);

        self.backend
            .truncate_volume(self.backend_volume_id, new_offset)
            .map_err(backend_err)?;

        Ok((reclaimed, updated_count))
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

        let mut free_space_guard = self.free_space.write().unwrap();
        if *free_space_guard < new_size {
            info_guard.state = VolumeState::Full;
            return Err(PowerFsError::OutOfSpace);
        }

        let mut next_offset_guard = self.next_offset.write().unwrap();
        let new_offset = *next_offset_guard;

        let needle_bytes = new_needle.to_bytes();
        self.backend
            .write_needle(self.backend_volume_id, new_offset, &needle_bytes)
            .map_err(backend_err)?;

        *next_offset_guard += new_size;
        *free_space_guard -= new_size;
        info_guard.used += new_size;
        info_guard.modified_at = Utc::now();

        let mut old_updated = old_info.clone();
        old_updated.deleted_at = Some(Utc::now());
        drop(next_offset_guard);
        drop(free_space_guard);
        drop(info_guard);

        self.index.insert(needle_id.clone(), old_updated);

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
        self.index.insert(needle_id, new_info);

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
        let needle_id = NeedleId(file_key);
        if let Some(existing_info) = self.index.get(&needle_id) {
            let data_size =
                NEEDLE_HEADER_SIZE as u32 + existing_info.data_size + NEEDLE_FOOTER_SIZE as u32;
            let raw_data = self
                .backend
                .read_needle(self.backend_volume_id, existing_info.offset, data_size)
                .map_err(backend_err)?;
            let needle = Needle::from_bytes(
                &raw_data,
                self.id(),
                existing_info.offset,
                existing_info.checksum_algorithm,
            )?;
            let data_offset = offset as usize;
            let data_end = data_offset + size as usize;
            let mut data_vec = needle.data.to_vec();
            if data_end > data_vec.len() {
                data_vec.resize(data_end, 0);
            }
            data_vec[data_offset..data_end].copy_from_slice(&data);

            self.append_needle_version(needle_id, Bytes::from(data_vec), existing_info)?;
        } else {
            let data_size = (offset as u64 + size as u64) as usize;
            let mut full_data = vec![0u8; data_size];
            let write_offset = offset as usize;
            let copy_len = std::cmp::min(data.len(), size as usize);
            full_data[write_offset..write_offset + copy_len].copy_from_slice(&data[..copy_len]);
            self.write_needle(file_key, Bytes::from(full_data))?;
        }
        Ok(())
    }

    pub fn read_needle_blob(&self, file_key: u64, offset: i64, size: i32) -> Result<Bytes> {
        let needle_id = NeedleId(file_key);
        if let Some(mut info) = self.index.get(&needle_id) {
            let data_size = NEEDLE_HEADER_SIZE as u32 + info.data_size + NEEDLE_FOOTER_SIZE as u32;
            let raw_data = self
                .backend
                .read_needle(self.backend_volume_id, info.offset, data_size)
                .map_err(backend_err)?;
            let needle =
                Needle::from_bytes(&raw_data, self.id(), info.offset, info.checksum_algorithm)?;

            info.last_verified_at = Some(Utc::now());
            info.verification_count += 1;
            self.index.insert(needle_id, info);

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
        0
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
