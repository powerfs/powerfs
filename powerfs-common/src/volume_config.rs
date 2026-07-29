use serde::{Deserialize, Serialize};

/// Volume 不可变配置，存入 RocksDB "config" CF
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeConfig {
    pub volume_id: u64,
    pub backend_type: u8, // 0=LocalFile, 1=SPDK-NVMe, 2=RBD, 3=S3
    pub disk_uuid: String,
    pub fs_type: String,
    pub file_path: String,
    pub volume_size: u64,
    pub needle_header_size: u32,
    pub needle_footer_size: u32,
    pub collection_name: String,
    pub replication_config: String,
    pub node_id: String,
    pub created_at: i64,
}

/// Volume 分配状态，存入 RocksDB "allocation" CF
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationStats {
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub next_needle_id: u64,
    pub append_offset: u64,
    pub active_count: u64,
    pub deleted_count: u64,
    pub last_modified_at: i64,
}

impl Default for AllocationStats {
    fn default() -> Self {
        Self {
            used_bytes: 0,
            free_bytes: 0,
            next_needle_id: 1,
            append_offset: 0,
            active_count: 0,
            deleted_count: 0,
            last_modified_at: 0,
        }
    }
}

/// 已删除 Needle 信息，存入 RocksDB "deleted" CF
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletedInfo {
    pub deleted_at: i64,
    pub original_size: u64,
}
