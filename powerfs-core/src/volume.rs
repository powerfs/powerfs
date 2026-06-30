use crate::index::{MemoryIndex, NeedleIndex, PersistentIndex};
use crate::needle::Needle;
use bytes::Bytes;
use chrono::Utc;
use powerfs_common::{
    constants::VOLUME_DATA_OFFSET,
    error::{PowerFsError, Result},
    types::{NeedleId, NeedleInfo, VolumeId, VolumeInfo, VolumeState},
    utils::generate_needle_id,
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

        let index: Box<dyn NeedleIndex> = if index_path.exists() {
            Box::new(PersistentIndex::new(index_path.to_str().unwrap())?)
        } else {
            Box::new(MemoryIndex::new(10000))
        };

        let info = VolumeInfo {
            id: id.clone(),
            node_id: powerfs_common::types::NodeId(node_id.to_string()),
            size,
            used: 0,
            replica_count: 3,
            state: VolumeState::Available,
            created_at: Utc::now(),
            modified_at: Utc::now(),
        };

        Ok(Volume {
            info: RwLock::new(info),
            file: RwLock::new(file),
            index,
            free_space: RwLock::new(size),
            next_offset: RwLock::new(VOLUME_DATA_OFFSET),
        })
    }

    pub fn id(&self) -> VolumeId {
        self.info.read().unwrap().id.clone()
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

    pub fn write_needle(&self, data: Bytes) -> Result<NeedleInfo> {
        let mut info_guard = self.info.write().unwrap();
        if info_guard.state != VolumeState::Available {
            return Err(PowerFsError::InvalidVolumeState(
                "volume not available".to_string(),
            ));
        }

        let needle_id = generate_needle_id();
        let needle = Needle::new(needle_id.clone(), self.id(), data);

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

        let needle_info = needle.to_info();
        self.index.insert(needle_id, needle_info.clone());

        Ok(needle_info)
    }

    pub fn read_needle(&self, needle_id: &NeedleId) -> Result<Bytes> {
        if let Some(info) = self.index.get(needle_id) {
            let mut file_guard = self.file.write().unwrap();
            let needle = Needle::read_from(&mut *file_guard, info.offset, self.id())?;
            Ok(needle.data)
        } else {
            Err(PowerFsError::NeedleNotFound(needle_id.clone()))
        }
    }

    pub fn delete_needle(&self, needle_id: &NeedleId) -> Result<()> {
        if let Some(info) = self.index.remove(needle_id) {
            let mut info_guard = self.info.write().unwrap();
            let mut free_space_guard = self.free_space.write().unwrap();

            info_guard.used -= info.data_size as u64 + 24;
            *free_space_guard += info.data_size as u64 + 24;
            info_guard.modified_at = Utc::now();

            Ok(())
        } else {
            Err(PowerFsError::NeedleNotFound(needle_id.clone()))
        }
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
}
