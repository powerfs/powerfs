use crate::storage_backend::{LocalFsBackend, StorageBackend, StorageBackendError};
use crate::volume::{ScrubResult, Volume};
use powerfs_common::{
    error::{PowerFsError, Result},
    types::{ChecksumAlgorithm, NeedleId, NodeId, VolumeId, VolumeInfo},
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct StorageManager {
    volumes: RwLock<HashMap<VolumeId, Arc<Volume>>>,
    node_id: NodeId,
    data_path: String,
    checksum_algorithm: ChecksumAlgorithm,
    backend: Arc<dyn StorageBackend>,
}

fn backend_err(e: StorageBackendError) -> PowerFsError {
    PowerFsError::Storage(e.to_string())
}

#[allow(clippy::result_large_err)]
impl StorageManager {
    pub fn new(node_id: NodeId, data_path: String) -> Result<Self> {
        let default_capacity = 100 * 1024 * 1024 * 1024; // 100GB
        let backend = Arc::new(
            LocalFsBackend::new(&data_path, &node_id.0, "default", default_capacity)
                .map_err(backend_err)?,
        );
        Ok(StorageManager {
            volumes: RwLock::new(HashMap::new()),
            node_id,
            data_path,
            checksum_algorithm: ChecksumAlgorithm::default(),
            backend,
        })
    }

    pub fn new_with_backend(
        node_id: NodeId,
        data_path: String,
        backend: Arc<dyn StorageBackend>,
    ) -> Self {
        StorageManager {
            volumes: RwLock::new(HashMap::new()),
            node_id,
            data_path,
            checksum_algorithm: ChecksumAlgorithm::default(),
            backend,
        }
    }

    pub fn new_with_algorithm(
        node_id: NodeId,
        data_path: String,
        algorithm: ChecksumAlgorithm,
    ) -> Result<Self> {
        let default_capacity = 100 * 1024 * 1024 * 1024; // 100GB
        let backend = Arc::new(
            LocalFsBackend::new(&data_path, &node_id.0, "default", default_capacity)
                .map_err(backend_err)?,
        );
        Ok(StorageManager {
            volumes: RwLock::new(HashMap::new()),
            node_id,
            data_path,
            checksum_algorithm: algorithm,
            backend,
        })
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn backend(&self) -> Arc<dyn StorageBackend> {
        self.backend.clone()
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
            self.backend.clone(),
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

            self.backend
                .delete_volume(volume_id.0 as u64)
                .map_err(backend_err)?;

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
                                    self.backend.clone(),
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

    pub fn verify_needle(&self, volume_id: &VolumeId, needle_id: &NeedleId) -> Result<bool> {
        let volume = self
            .get_volume(volume_id)
            .ok_or_else(|| PowerFsError::VolumeNotFound(*volume_id))?;
        volume.verify_needle(needle_id)
    }

    pub fn scrub_volume(&self, volume_id: &VolumeId) -> Result<ScrubResult> {
        let volume = self
            .get_volume(volume_id)
            .ok_or_else(|| PowerFsError::VolumeNotFound(*volume_id))?;
        Ok(volume.scrub_volume())
    }

    pub fn scrub_all_volumes(&self) -> Vec<(VolumeId, ScrubResult)> {
        let mut results = Vec::new();
        let volumes = self.list_volumes();
        for volume_info in volumes {
            if let Ok(volume) = self
                .get_volume(&volume_info.id)
                .ok_or_else(|| PowerFsError::VolumeNotFound(volume_info.id))
            {
                results.push((volume_info.id, volume.scrub_volume()));
            }
        }
        results
    }

    pub fn compact_volume(&self, volume_id: &VolumeId) -> Result<(u64, u64)> {
        let volume = self
            .get_volume(volume_id)
            .ok_or_else(|| PowerFsError::VolumeNotFound(*volume_id))?;
        volume.compact()
    }

    pub fn compact_all_volumes(&self) -> Vec<(VolumeId, Result<(u64, u64)>)> {
        let mut results = Vec::new();
        let volumes = self.list_volumes();
        for volume_info in volumes {
            if let Ok(volume) = self
                .get_volume(&volume_info.id)
                .ok_or_else(|| PowerFsError::VolumeNotFound(volume_info.id))
            {
                results.push((volume_info.id, volume.compact()));
            }
        }
        results
    }
}
