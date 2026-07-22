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
                // A configured capacity of 0 means "auto-detect from statvfs".
                let device_capacity = if device.total_capacity > 0 {
                    Some(device.total_capacity)
                } else {
                    None
                };
                let backend = LocalFsBackend::new(
                    &details.data_dir,
                    &config.node_id,
                    &device.name,
                    device_capacity,
                )?;
                Ok(Arc::new(backend))
            }
            BackendType::Spdk => {
                #[cfg(any(feature = "spdk", feature = "spdk-stub"))]
                {
                    // 只校验配置 + 创建 SpdkBackend (内部调 powerfs_spdk_init 初始化 SPDK 环境)。
                    // 设备 attach 不在这里做 — SPDK subsystem 初始化是异步的,
                    // 需要等服务 ready 后通过 RPC 异步 attach。
                    // 见 SpdkBackend::attach_devices_from_config 和 main.rs 的后台任务。
                    let details = match &config.config {
                        BackendConfigDetails::Spdk(d) => d,
                        _ => {
                            return Err(StorageBackendError::InvalidOperation(
                                "config mismatch: expected spdk config".to_string(),
                            ));
                        }
                    };

                    let rpc_path = details.rpc_socket_path.as_deref();
                    let backend = SpdkBackend::new(&config.node_id, rpc_path)?;
                    Ok(Arc::new(backend))
                }
                #[cfg(not(any(feature = "spdk", feature = "spdk-stub")))]
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
            config: BackendConfigDetails::Spdk(SpdkBackendConfig {
                devices,
                rpc_socket_path: None,
                local_tgt: None,
                nvmf: None,
            }),
        };
        Self::create(&config)
    }
}
