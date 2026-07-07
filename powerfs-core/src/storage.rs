use crate::volume::Volume;
use powerfs_common::{
    error::{PowerFsError, Result},
    types::{ChecksumAlgorithm, NodeId, VolumeId, VolumeInfo},
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct StorageManager {
    volumes: RwLock<HashMap<VolumeId, Arc<Volume>>>,
    node_id: NodeId,
    data_path: String,
    checksum_algorithm: ChecksumAlgorithm,
}

#[allow(clippy::result_large_err)]
impl StorageManager {
    pub fn new(node_id: NodeId, data_path: String) -> Self {
        StorageManager {
            volumes: RwLock::new(HashMap::new()),
            node_id,
            data_path,
            checksum_algorithm: ChecksumAlgorithm::default(),
        }
    }

    pub fn new_with_algorithm(
        node_id: NodeId,
        data_path: String,
        algorithm: ChecksumAlgorithm,
    ) -> Self {
        StorageManager {
            volumes: RwLock::new(HashMap::new()),
            node_id,
            data_path,
            checksum_algorithm: algorithm,
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn create_volume(&self, volume_id: VolumeId, size: u64) -> Result<VolumeInfo> {
        let mut volumes = self.volumes.write().unwrap();

        if volumes.contains_key(&volume_id) {
            return Err(PowerFsError::VolumeExists(volume_id));
        }

        let volume = Arc::new(Volume::new_with_algorithm(
            volume_id,
            &self.node_id.0,
            &self.data_path,
            size,
            self.checksum_algorithm,
        )?);

        let info = volume.info();
        volumes.insert(volume_id, volume);

        Ok(info)
    }

    pub fn get_volume(&self, volume_id: &VolumeId) -> Option<Arc<Volume>> {
        self.volumes.read().unwrap().get(volume_id).cloned()
    }

    pub fn delete_volume(&self, volume_id: &VolumeId) -> Result<()> {
        let mut volumes = self.volumes.write().unwrap();

        if let Some(volume) = volumes.remove(volume_id) {
            volume.set_deleting();

            let volume_path =
                std::path::Path::new(&self.data_path).join(format!("volume_{}", volume.id().0));

            if volume_path.exists() {
                std::fs::remove_dir_all(&volume_path)?;
            }

            Ok(())
        } else {
            Err(PowerFsError::VolumeNotFound(*volume_id))
        }
    }

    pub fn list_volumes(&self) -> Vec<VolumeInfo> {
        self.volumes
            .read()
            .unwrap()
            .values()
            .map(|v| v.info())
            .collect()
    }

    pub fn volume_count(&self) -> usize {
        self.volumes.read().unwrap().len()
    }

    pub fn total_space(&self) -> u64 {
        self.volumes
            .read()
            .unwrap()
            .values()
            .map(|v| v.size())
            .sum()
    }

    pub fn used_space(&self) -> u64 {
        self.volumes
            .read()
            .unwrap()
            .values()
            .map(|v| v.used())
            .sum()
    }

    pub fn free_space(&self) -> u64 {
        self.volumes
            .read()
            .unwrap()
            .values()
            .map(|v| v.free_space())
            .sum()
    }

    pub fn find_available_volume(&self) -> Option<VolumeId> {
        self.volumes
            .read()
            .unwrap()
            .values()
            .find(|v| v.is_available() && !v.is_full())
            .map(|v| v.id())
    }

    pub fn load_volumes(&self) -> Result<()> {
        let volumes_dir = std::path::Path::new(&self.data_path);

        if !volumes_dir.exists() {
            std::fs::create_dir_all(volumes_dir)?;
            return Ok(());
        }

        let mut volumes = self.volumes.write().unwrap();

        for entry in std::fs::read_dir(volumes_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(stripped) = dir_name.strip_prefix("volume_") {
                        if let Ok(vid) = stripped.parse::<u32>() {
                            let volume_id = VolumeId(vid);
                            if let std::collections::hash_map::Entry::Vacant(e) =
                                volumes.entry(volume_id)
                            {
                                let volume = Arc::new(Volume::new_with_algorithm(
                                    volume_id,
                                    &self.node_id.0,
                                    &self.data_path,
                                    powerfs_common::constants::DEFAULT_VOLUME_SIZE,
                                    self.checksum_algorithm,
                                )?);
                                e.insert(volume);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
