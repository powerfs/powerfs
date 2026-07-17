use crate::storage_backend::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

#[derive(Debug, Clone, PartialEq)]
pub enum MigrationType {
    FullMigration,
    DrainVolume,
    DrainDevice,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MigrationStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct DataMigrationTask {
    pub task_id: String,
    pub source_volume_id: u64,
    pub target_volume_id: Option<u64>,
    pub target_device_id: Option<String>,
    pub migration_type: MigrationType,
    pub status: MigrationStatus,
    pub progress_percent: f64,
    pub bytes_transferred: u64,
    pub total_bytes: u64,
    pub started_at: Option<Instant>,
    pub completed_at: Option<Instant>,
    pub error_message: Option<String>,
}

pub struct DataMigrationManager {
    backend: Arc<dyn StorageBackend>,
    tasks: Arc<RwLock<HashMap<String, DataMigrationTask>>>,
    _running_tasks: Arc<RwLock<Vec<String>>>,
}

impl DataMigrationManager {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        DataMigrationManager {
            backend,
            tasks: Arc::new(RwLock::new(HashMap::new())),
            _running_tasks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn create_volume_migration(
        &self,
        source_volume_id: u64,
        target_device_id: Option<&str>,
    ) -> StorageResult<String> {
        let task_id = format!("migrate_{}_{}", source_volume_id, uuid::Uuid::new_v4());

        let source_info = self.backend.get_volume_info(source_volume_id)?;

        let task = DataMigrationTask {
            task_id: task_id.clone(),
            source_volume_id,
            target_volume_id: None,
            target_device_id: target_device_id.map(|s| s.to_string()),
            migration_type: MigrationType::FullMigration,
            status: MigrationStatus::Pending,
            progress_percent: 0.0,
            bytes_transferred: 0,
            total_bytes: source_info.used_size,
            started_at: None,
            completed_at: None,
            error_message: None,
        };

        self.tasks.write().unwrap().insert(task_id.clone(), task);

        Ok(task_id)
    }

    pub fn create_device_drain(
        &self,
        source_device_id: &str,
        target_device_ids: Option<Vec<String>>,
    ) -> StorageResult<Vec<String>> {
        let volumes = self.backend.get_volumes_on_device(source_device_id)?;
        let mut task_ids = Vec::new();

        for volume_id in volumes {
            let task_id = format!("drain_{}_{}", volume_id, uuid::Uuid::new_v4());
            let source_info = self.backend.get_volume_info(volume_id)?;

            let task = DataMigrationTask {
                task_id: task_id.clone(),
                source_volume_id: volume_id,
                target_volume_id: None,
                target_device_id: target_device_ids.as_ref().and_then(|v| v.first().cloned()),
                migration_type: MigrationType::DrainDevice,
                status: MigrationStatus::Pending,
                progress_percent: 0.0,
                bytes_transferred: 0,
                total_bytes: source_info.used_size,
                started_at: None,
                completed_at: None,
                error_message: None,
            };

            self.tasks.write().unwrap().insert(task_id.clone(), task);
            task_ids.push(task_id);
        }

        Ok(task_ids)
    }

    pub fn get_migration_status(&self, task_id: &str) -> StorageResult<DataMigrationTask> {
        let tasks = self.tasks.read().unwrap();
        tasks.get(task_id).cloned().ok_or_else(|| {
            StorageBackendError::BackendError(format!("Task not found: {}", task_id))
        })
    }

    pub fn list_migrations(&self, status: Option<MigrationStatus>) -> Vec<DataMigrationTask> {
        let tasks = self.tasks.read().unwrap();
        if let Some(s) = status {
            tasks.values().filter(|t| t.status == s).cloned().collect()
        } else {
            tasks.values().cloned().collect()
        }
    }

    pub fn get_drain_progress(&self, device_id: &str) -> StorageResult<DeviceDrainProgress> {
        let tasks = self.tasks.read().unwrap();
        let drain_tasks: Vec<&DataMigrationTask> = tasks
            .values()
            .filter(|t| t.migration_type == MigrationType::DrainDevice)
            .filter(|t| {
                t.target_device_id
                    .as_deref()
                    .map(|d| d == device_id)
                    .unwrap_or(false)
                    || t.source_volume_id != 0
            })
            .collect();

        let volumes_on_device = self.backend.get_volumes_on_device(device_id)?;
        let total_volumes = volumes_on_device.len();

        let completed_volumes = drain_tasks
            .iter()
            .filter(|t| t.status == MigrationStatus::Completed)
            .count();

        let total_bytes: u64 = drain_tasks.iter().map(|t| t.total_bytes).sum();
        let transferred_bytes: u64 = drain_tasks.iter().map(|t| t.bytes_transferred).sum();

        let in_progress: Vec<String> = drain_tasks
            .iter()
            .filter(|t| t.status == MigrationStatus::Running)
            .map(|t| t.task_id.clone())
            .collect();

        Ok(DeviceDrainProgress {
            device_id: device_id.to_string(),
            total_volumes,
            completed_volumes,
            total_bytes,
            transferred_bytes,
            in_progress_tasks: in_progress,
            estimated_remaining_secs: 0,
        })
    }

    pub fn pause_migration(&self, task_id: &str) -> StorageResult<()> {
        let mut tasks = self.tasks.write().unwrap();
        if let Some(task) = tasks.get_mut(task_id) {
            if task.status == MigrationStatus::Running {
                task.status = MigrationStatus::Paused;
            }
            Ok(())
        } else {
            Err(StorageBackendError::BackendError(format!(
                "Task not found: {}",
                task_id
            )))
        }
    }

    pub fn resume_migration(&self, task_id: &str) -> StorageResult<()> {
        let mut tasks = self.tasks.write().unwrap();
        if let Some(task) = tasks.get_mut(task_id) {
            if task.status == MigrationStatus::Paused {
                task.status = MigrationStatus::Running;
            }
            Ok(())
        } else {
            Err(StorageBackendError::BackendError(format!(
                "Task not found: {}",
                task_id
            )))
        }
    }

    pub fn cancel_migration(&self, task_id: &str) -> StorageResult<()> {
        let mut tasks = self.tasks.write().unwrap();
        if let Some(task) = tasks.get_mut(task_id) {
            task.status = MigrationStatus::Cancelled;
            Ok(())
        } else {
            Err(StorageBackendError::BackendError(format!(
                "Task not found: {}",
                task_id
            )))
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeviceDrainProgress {
    pub device_id: String,
    pub total_volumes: usize,
    pub completed_volumes: usize,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub in_progress_tasks: Vec<String>,
    pub estimated_remaining_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_backend::LocalFsBackend;
    use tempfile::tempdir;

    fn create_test_manager() -> (
        Arc<DataMigrationManager>,
        Arc<LocalFsBackend>,
        tempfile::TempDir,
    ) {
        let dir = tempdir().unwrap();
        let backend = Arc::new(
            LocalFsBackend::new(
                dir.path().to_str().unwrap(),
                "test-node",
                "dev0",
                100 * 1024 * 1024,
            )
            .unwrap(),
        );
        let manager = Arc::new(DataMigrationManager::new(backend.clone()));
        (manager, backend, dir)
    }

    #[test]
    fn test_create_volume_migration() {
        let (manager, backend, _dir) = create_test_manager();
        backend.allocate_volume(1, 10 * 1024 * 1024, None).unwrap();

        let task_id = manager.create_volume_migration(1, None).unwrap();
        assert!(!task_id.is_empty());

        let task = manager.get_migration_status(&task_id).unwrap();
        assert_eq!(task.status, MigrationStatus::Pending);
        assert_eq!(task.source_volume_id, 1);
    }

    #[test]
    fn test_list_migrations() {
        let (manager, backend, _dir) = create_test_manager();
        backend.allocate_volume(1, 1024 * 1024, None).unwrap();
        backend.allocate_volume(2, 2 * 1024 * 1024, None).unwrap();

        manager.create_volume_migration(1, None).unwrap();
        manager.create_volume_migration(2, None).unwrap();

        let all = manager.list_migrations(None);
        assert_eq!(all.len(), 2);

        let pending = manager.list_migrations(Some(MigrationStatus::Pending));
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn test_device_drain() {
        let (manager, backend, _dir) = create_test_manager();
        backend.allocate_volume(1, 1024 * 1024, None).unwrap();
        backend.allocate_volume(2, 2 * 1024 * 1024, None).unwrap();

        let devices = backend.list_devices().unwrap();
        let device_id = &devices[0].device_id;

        let task_ids = manager.create_device_drain(device_id, None).unwrap();
        assert_eq!(task_ids.len(), 2);
    }

    #[test]
    fn test_pause_resume_cancel() {
        let (manager, backend, _dir) = create_test_manager();
        backend.allocate_volume(1, 1024 * 1024, None).unwrap();

        let task_id = manager.create_volume_migration(1, None).unwrap();

        {
            let mut tasks = manager.tasks.write().unwrap();
            if let Some(task) = tasks.get_mut(&task_id) {
                task.status = MigrationStatus::Running;
            }
        }

        manager.pause_migration(&task_id).unwrap();
        let task = manager.get_migration_status(&task_id).unwrap();
        assert_eq!(task.status, MigrationStatus::Paused);

        manager.resume_migration(&task_id).unwrap();
        let task = manager.get_migration_status(&task_id).unwrap();
        assert_eq!(task.status, MigrationStatus::Running);

        manager.cancel_migration(&task_id).unwrap();
        let task = manager.get_migration_status(&task_id).unwrap();
        assert_eq!(task.status, MigrationStatus::Cancelled);
    }

    #[test]
    fn test_get_migration_not_found() {
        let (manager, _backend, _dir) = create_test_manager();
        let result = manager.get_migration_status("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_drain_progress() {
        let (manager, backend, _dir) = create_test_manager();
        backend.allocate_volume(1, 1024 * 1024, None).unwrap();

        let devices = backend.list_devices().unwrap();
        let device_id = &devices[0].device_id;

        manager.create_device_drain(device_id, None).unwrap();

        let progress = manager.get_drain_progress(device_id).unwrap();
        assert_eq!(progress.total_volumes, 1);
        assert_eq!(progress.completed_volumes, 0);
    }
}
