use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeviceType {
    LocalFile,
    SpdkNvme,
    NvmeOfRdma,
    NvmeOfTcp,
}

impl fmt::Display for DeviceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeviceType::LocalFile => write!(f, "local_file"),
            DeviceType::SpdkNvme => write!(f, "spdk_nvme"),
            DeviceType::NvmeOfRdma => write!(f, "nvmeof_rdma"),
            DeviceType::NvmeOfTcp => write!(f, "nvmeof_tcp"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeviceStatus {
    Online,
    ReadOnly,
    Offline,
    Draining,
    Excluded,
}

impl fmt::Display for DeviceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeviceStatus::Online => write!(f, "online"),
            DeviceStatus::ReadOnly => write!(f, "read_only"),
            DeviceStatus::Offline => write!(f, "offline"),
            DeviceStatus::Draining => write!(f, "draining"),
            DeviceStatus::Excluded => write!(f, "excluded"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Degraded,
    Critical,
    Failed,
}

impl fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Warning => write!(f, "warning"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Critical => write!(f, "critical"),
            HealthStatus::Failed => write!(f, "failed"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceLocation {
    pub node_id: String,
    pub device_id: String,
    pub zone: String,
    pub rack: Option<String>,
    pub data_center: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDevice {
    pub device_id: String,
    pub device_type: DeviceType,
    pub total_capacity: u64,
    pub used_space: u64,
    pub free_space: u64,
    pub location: DeviceLocation,
    pub status: DeviceStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmartInfo {
    pub temperature_celsius: f64,
    pub power_on_hours: u64,
    pub unsafe_shutdowns: u64,
    pub media_errors: u64,
    pub error_log_entries: u64,
    pub data_units_read: u64,
    pub data_units_written: u64,
    pub host_read_commands: u64,
    pub host_write_commands: u64,
    pub available_spare_percent: f64,
    pub available_spare_threshold_percent: f64,
    pub percentage_used: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceHealth {
    pub device_id: String,
    pub device_type: DeviceType,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub utilization_percent: f64,
    pub read_iops: f64,
    pub write_iops: f64,
    pub read_bandwidth_bps: u64,
    pub write_bandwidth_bps: u64,
    pub avg_latency_us: f64,
    pub p99_latency_us: f64,
    pub smart_info: Option<SmartInfo>,
    pub health_status: HealthStatus,
    pub last_checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolumeState {
    Active,
    Migrating,
    ReadOnly,
    Deleting,
}

impl fmt::Display for VolumeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VolumeState::Active => write!(f, "active"),
            VolumeState::Migrating => write!(f, "migrating"),
            VolumeState::ReadOnly => write!(f, "read_only"),
            VolumeState::Deleting => write!(f, "deleting"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeStorageInfo {
    pub volume_id: u64,
    pub device_id: String,
    pub total_size: u64,
    pub used_size: u64,
    pub physical_offset: u64,
    pub volume_state: VolumeState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocateVolumeResult {
    pub volume_id: u64,
    pub device_id: String,
    pub allocated_size: u64,
    pub volume_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeSet {
    pub device_id: String,
    pub volumes: Vec<u64>,
    pub total_capacity: u64,
    pub total_used: u64,
    pub total_free: u64,
    pub health_status: HealthStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExcludedDevice {
    pub device_id: String,
    pub reason: String,
    pub excluded_at: DateTime<Utc>,
    pub excluded_by: String,
    pub auto_drain: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ChecksumAlgorithm {
    #[default]
    Crc32c,
    Crc64,
    Xxhash3_64,
    Blake3,
    Sha256,
}

impl fmt::Display for ChecksumAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChecksumAlgorithm::Crc32c => write!(f, "crc32c"),
            ChecksumAlgorithm::Crc64 => write!(f, "crc64"),
            ChecksumAlgorithm::Xxhash3_64 => write!(f, "xxhash3_64"),
            ChecksumAlgorithm::Blake3 => write!(f, "blake3"),
            ChecksumAlgorithm::Sha256 => write!(f, "sha256"),
        }
    }
}
