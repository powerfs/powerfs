use crate::index::{NeedleIndex, PersistentIndex};
use crate::needle::Needle;
use bytes::Bytes;
use chrono::{Duration, Utc};
use powerfs_common::{
    constants::{NEEDLE_FOOTER_SIZE, NEEDLE_HEADER_SIZE, VOLUME_DATA_OFFSET},
    error::{PowerFsError, Result},
    types::{
        ChecksumAlgorithm, Collection, DiskType, NeedleId, NeedleInfo, Ttl, VolumeId, VolumeInfo,
        VolumeState,
    },
    utils::Checksum,
};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::RwLock;

pub struct Volume {
    info: RwLock<VolumeInfo>,
    file: RwLock<File>,
    index: Box<dyn NeedleIndex>,
    free_space: RwLock<u64>,
    next_offset: RwLock<u64>,
    checksum_algorithm: ChecksumAlgorithm,
}

#[allow(clippy::result_large_err)]
impl Volume {
    pub fn new(id: VolumeId, node_id: &str, path: &str, size: u64) -> Result<Self> {
        let volume_path = Path::new(path).join(format!("volume_{}", id.0));

        if !volume_path.exists() {
            std::fs::create_dir_all(&volume_path)?;
        }

        let data_file_path = volume_path.join("data");
        let index_path = volume_path.join("index");

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&data_file_path)?;

        let file_size = file.metadata()?.len();
        if file_size < size {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&data_file_path)?
                .set_len(size)?;
        }

        let index: Box<dyn NeedleIndex> =
            Box::new(PersistentIndex::new(index_path.to_str().unwrap())?);

        let info = VolumeInfo {
            id,
            node_id: powerfs_common::types::NodeId(node_id.to_string()),
            collection: Collection::default(),
            size,
            used: 0,
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
            file: RwLock::new(file),
            index,
            free_space: RwLock::new(size),
            next_offset: RwLock::new(VOLUME_DATA_OFFSET),
            checksum_algorithm: ChecksumAlgorithm::default(),
        })
    }

    pub fn new_with_algorithm(
        id: VolumeId,
        node_id: &str,
        path: &str,
        size: u64,
        algorithm: ChecksumAlgorithm,
    ) -> Result<Self> {
        let volume_path = Path::new(path).join(format!("volume_{}", id.0));

        if !volume_path.exists() {
            std::fs::create_dir_all(&volume_path)?;
        }

        let data_file_path = volume_path.join("data");
        let index_path = volume_path.join("index");

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&data_file_path)?;

        let file_size = file.metadata()?.len();
        if file_size < size {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&data_file_path)?
                .set_len(size)?;
        }

        let index: Box<dyn NeedleIndex> =
            Box::new(PersistentIndex::new(index_path.to_str().unwrap())?);

        let info = VolumeInfo {
            id,
            node_id: powerfs_common::types::NodeId(node_id.to_string()),
            collection: Collection::default(),
            size,
            used: 0,
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
            file: RwLock::new(file),
            index,
            free_space: RwLock::new(size),
            next_offset: RwLock::new(VOLUME_DATA_OFFSET),
            checksum_algorithm: algorithm,
        })
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

        let mut file_guard = self.file.write().unwrap();
        needle.write_to(&mut *file_guard, offset)?;

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

        drop(file_guard);
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

            let mut file_guard = self.file.write().unwrap();
            let needle = Needle::read_from(&mut *file_guard, info.offset, self.id())?;
            drop(file_guard);

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
                if retention_until < now {
                    if let Some(removed_info) = self.index.remove(&needle_id) {
                        let mut info_guard = self.info.write().unwrap();
                        let mut free_space_guard = self.free_space.write().unwrap();

                        let needle_size = (NEEDLE_HEADER_SIZE
                            + removed_info.data_size as usize
                            + NEEDLE_FOOTER_SIZE) as u64;
                        info_guard.used -= needle_size;
                        *free_space_guard += needle_size;
                        info_guard.modified_at = Utc::now();

                        cleaned_count += 1;
                    }
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
            let mut file_guard = self.file.write().unwrap();
            let needle = Needle::read_from(&mut *file_guard, existing_info.offset, self.id())?;
            let data_offset = offset as usize;
            let data_end = data_offset + size as usize;
            let mut data_vec = needle.data.to_vec();
            if data_end > data_vec.len() {
                data_vec.resize(data_end, 0);
            }
            data_vec[data_offset..data_end].copy_from_slice(&data);
            let checksum_val = Checksum::compute(&data_vec, self.checksum_algorithm);
            let updated_needle = Needle {
                id: needle.id,
                volume_id: needle.volume_id,
                data: Bytes::from(data_vec),
                offset: needle.offset,
                checksum: checksum_val.as_u64(),
                checksum_algorithm: self.checksum_algorithm,
            };
            updated_needle.write_to(&mut *file_guard, existing_info.offset)?;
            let mut info_guard = self.info.write().unwrap();
            info_guard.modified_at = Utc::now();
            drop(file_guard);
            let updated_info = NeedleInfo {
                id: needle_id,
                volume_id: info_guard.id,
                data_size: updated_needle.data.len() as u32,
                offset: existing_info.offset,
                checksum: updated_needle.checksum,
                checksum_algorithm: self.checksum_algorithm,
                last_verified_at: None,
                verification_count: 0,
                deleted_at: existing_info.deleted_at,
                delete_retention_until: existing_info.delete_retention_until,
                worm_retention_until: existing_info.worm_retention_until,
                created_at: existing_info.created_at,
                ec_enabled: existing_info.ec_enabled,
                ec_k: existing_info.ec_k,
                ec_m: existing_info.ec_m,
                ec_shards: existing_info.ec_shards.clone(),
            };
            drop(info_guard);
            self.index.insert(NeedleId(file_key), updated_info);
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
            let mut file_guard = self.file.write().unwrap();
            let needle = Needle::read_from(&mut *file_guard, info.offset, self.id())?;
            drop(file_guard);

            info.last_verified_at = Some(Utc::now());
            info.verification_count += 1;
            self.index.insert(needle_id, info);

            let data_offset = offset as usize;
            let data_size = size as usize;
            if data_offset + data_size <= needle.data.len() {
                Ok(Bytes::from(
                    needle.data[data_offset..data_offset + data_size].to_vec(),
                ))
            } else {
                Err(PowerFsError::InvalidRequest(
                    "invalid offset or size".to_string(),
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
}
