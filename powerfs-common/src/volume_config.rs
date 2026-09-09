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
    /// 物理垃圾字节（append-only 数据文件中不可复用、只能由 compact 回收的空间）：
    /// 覆写/重试同一 needle_id 时遗留的旧物理副本 + 已删除 needle 的物理 hole。
    /// compact 重写存活 needle 并 truncate 后清零。
    /// `#[serde(default)]` 兼容升级前 RocksDB 中的旧统计（反序列化为 0，
    /// 启动时由 rebuild_allocation_stats / sync_allocation_from_index 重算）。
    #[serde(default)]
    pub garbage_bytes: u64,
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
            garbage_bytes: 0,
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
