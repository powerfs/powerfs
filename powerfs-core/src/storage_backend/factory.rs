use crate::config::Config;
use crate::storage_backend::*;
use std::sync::Arc;

type Result<T> = StorageResult<T>;

pub struct BackendFactory;

impl BackendFactory {
    pub fn create(config: &BackendConfig) -> Result<Arc<dyn StorageBackend + Send + Sync>> {
        match config.backend_type {
            BackendType::LocalFile => {
                let details = match &config.config {
                    BackendConfigDetails::LocalFile(d) => d,
                    _ => {
                        return Err(StorageBackendError::InvalidOperation(
                            "config mismatch: expected local_file config".to_string(),
                        ));
                    }
                };

                if details.devices.is_empty() {
                    return Err(StorageBackendError::InvalidOperation(
                        "local_file backend requires at least one device".to_string(),
                    ));
                }

                let device = &details.devices[0];
                let backend = LocalFsBackend::new(
                    &details.data_dir,
                    &config.node_id,
                    &device.name,
                    device.total_capacity,
                )?;
                Ok(Arc::new(backend))
            }
            BackendType::Spdk => {
                #[cfg(feature = "spdk")]
                {
                    let details = match &config.config {
                        BackendConfigDetails::Spdk(d) => d,
                        _ => {
                            return Err(StorageBackendError::InvalidOperation(
                                "config mismatch: expected spdk config".to_string(),
                            ));
                        }
                    };

                    let backend = SpdkBackend::new(&config.node_id, None)?;
                    for device in &details.devices {
                        backend.add_device(
                            &device.name,
                            &device.transport_string,
                            device.capacity,
                        )?;
                    }
                    Ok(Arc::new(backend))
                }
                #[cfg(not(feature = "spdk"))]
                {
                    Err(StorageBackendError::InvalidOperation(
                        "SPDK backend not compiled. Enable 'spdk' or 'spdk-stub' feature."
                            .to_string(),
                    ))
                }
            }
        }
    }

    pub fn create_from_config(config: &Config) -> Result<Arc<dyn StorageBackend + Send + Sync>> {
        Self::create(&config.storage.backend)
    }

    pub fn create_local_file(
        data_dir: &str,
        node_id: &str,
        device_name: &str,
        total_capacity: u64,
    ) -> Result<Arc<dyn StorageBackend + Send + Sync>> {
        let config = BackendConfig {
            backend_type: BackendType::LocalFile,
            node_id: node_id.to_string(),
            config: BackendConfigDetails::LocalFile(LocalFileBackendConfig {
                data_dir: data_dir.to_string(),
                devices: vec![LocalFileDeviceConfig {
                    name: device_name.to_string(),
                    total_capacity,
                }],
            }),
        };
        Self::create(&config)
    }

    #[cfg(feature = "spdk")]
    pub fn create_spdk(
        node_id: &str,
        devices: Vec<SpdkDeviceConfig>,
    ) -> Result<Arc<dyn StorageBackend + Send + Sync>> {
        let config = BackendConfig {
            backend_type: BackendType::Spdk,
            node_id: node_id.to_string(),
            config: BackendConfigDetails::Spdk(SpdkBackendConfig { devices }),
        };
        Self::create(&config)
    }
}
