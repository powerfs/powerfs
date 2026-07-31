//! Collection management module (Step 1: core data structures and Master API).
//!
//! This module introduces the P0 extended attributes for PowerFS collections:
//! lifecycle status, storage policy, capacity quota, volume allocation mode
//! and runtime stats. The [`CollectionManager`] uses a [`DashMap`] so reads
//! remain lock-free, while writes go through the standard dashmap shard locks.
//!
//! Backward compatibility: the existing "default" collection is always
//! materialized on construction via [`CollectionManager::with_default`].

use dashmap::DashMap;
use log::info;
use powerfs_common::{
    error::{PowerFsError, Result},
    types::{Collection, VolumeId, VolumeInfo, VolumeState},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Collection lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CollectionStatus {
    /// Normal: accepts reads and writes.
    #[default]
    Active,
    /// Read-only: rejects writes.
    Readonly,
    /// Archived: data may reside on cold storage.
    Archived,
    /// Soft-deleted: pending cleanup.
    Deleted,
}

impl CollectionStatus {
    /// Returns true when the collection accepts write traffic.
    pub fn is_writable(self) -> bool {
        matches!(self, CollectionStatus::Active)
    }
}

/// Storage redundancy mode (replication or erasure coding).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RedundancyMode {
    /// Simple replication with `copies` replicas.
    Replication { copies: u32 },
    /// Erasure coding with `data_shards` + `parity_shards`.
    ErasureCoding {
        data_shards: u32,
        parity_shards: u32,
        algorithm: String,
    },
}

impl Default for RedundancyMode {
    fn default() -> Self {
        RedundancyMode::Replication { copies: 1 }
    }
}

/// Storage policy bundling redundancy and write quorum.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StoragePolicy {
    pub name: String,
    pub redundancy: RedundancyMode,
    pub min_write_nodes: u32,
}

/// How volumes are selected for a collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VolumeAllocationMode {
    /// Master auto-allocates writable volumes.
    Auto {
        /// Pre-allocated volume count hint.
        count: u32,
        /// Per-volume size in bytes.
        volume_size: u64,
    },
    /// Only the listed volume ids are used.
    Manual {
        /// Explicitly pinned volume ids.
        volume_ids: Vec<u64>,
    },
    /// Pinned volumes plus auto-allocation fallback.
    Hybrid {
        /// Fixed volume ids that always serve the collection.
        fixed_volume_ids: Vec<u64>,
        /// Additional auto-allocated volume count.
        auto_count: u32,
    },
}

impl Default for VolumeAllocationMode {
    fn default() -> Self {
        VolumeAllocationMode::Auto {
            count: 0,
            volume_size: 0,
        }
    }
}

/// Collection extended attributes (P0).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    /// Globally unique collection name.
    pub name: String,
    /// Lifecycle status.
    pub status: CollectionStatus,
    /// Storage policy (redundancy + write quorum).
    pub storage_policy: StoragePolicy,
    /// Disk type hint (hdd/ssd/nvme/mixed).
    pub disk_type: String,
    /// Capacity quota in bytes; 0 means unlimited.
    pub capacity_quota_bytes: u64,
    /// Pre-allocated volume count.
    pub volume_count: u32,
    /// TTL in seconds; 0 means never expire.
    pub ttl_seconds: u32,
    /// Creation timestamp (unix seconds).
    pub created_at: i64,
    /// Last update timestamp (unix seconds).
    pub updated_at: i64,
    /// Free-form description.
    pub description: String,
    /// Volume allocation strategy.
    pub volume_allocation: VolumeAllocationMode,
    /// Volume blacklist: these ids are never assigned to this collection.
    pub excluded_volume_ids: Vec<u64>,
}

impl CollectionInfo {
    /// Returns true when the collection accepts write traffic.
    pub fn is_writable(&self) -> bool {
        self.status.is_writable()
    }

    /// Build a sensible default `CollectionInfo` for the given name.
    ///
    /// Used for backward-compatible auto-creation (e.g. "default") and as a
    /// starting point for the extended create API.
    pub fn default_for(name: &str) -> Self {
        let now = chrono::Utc::now().timestamp();
        CollectionInfo {
            name: name.to_string(),
            status: CollectionStatus::Active,
            storage_policy: StoragePolicy::default(),
            disk_type: String::new(),
            capacity_quota_bytes: 0,
            volume_count: 0,
            ttl_seconds: 0,
            created_at: now,
            updated_at: now,
            description: String::new(),
            volume_allocation: VolumeAllocationMode::default(),
            excluded_volume_ids: Vec::new(),
        }
    }
}

/// Runtime stats for a collection, computed from the volume table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CollectionStats {
    /// Total bytes used across all volumes.
    pub used_bytes: u64,
    /// Approximate file count (sum of next_file_key - 1).
    pub file_count: u64,
    /// Total volume count.
    pub volume_count: u32,
    /// Volumes currently accepting writes.
    pub writable_volume_count: u32,
    /// Cumulative read ops (placeholder for future stats integration).
    pub read_ops: u64,
    /// Cumulative write ops (placeholder for future stats integration).
    pub write_ops: u64,
    /// Cumulative bytes read (placeholder for future stats integration).
    pub read_bytes: u64,
    /// Cumulative bytes written (placeholder for future stats integration).
    pub write_bytes: u64,
}

/// Manages `CollectionInfo` entries with lock-free reads via [`DashMap`].
///
/// Clone is intentionally cheap-ish: it rebuilds a new DashMap from the
/// snapshot of the source. This mirrors the deep-clone pattern used by
/// `MasterNode::Clone` for the other RwLock-wrapped maps.
#[derive(Clone)]
pub struct CollectionManager {
    collections: DashMap<String, CollectionInfo>,
}

impl CollectionManager {
    /// Create an empty manager.
    pub fn new() -> Self {
        CollectionManager {
            collections: DashMap::new(),
        }
    }

    /// Create a manager pre-loaded with the "default" collection.
    ///
    /// This guarantees backward compatibility: any code path that looks up
    /// "default" always finds it, even before any explicit creation call.
    pub fn with_default() -> Self {
        let mgr = Self::new();
        mgr.ensure_default();
        mgr
    }

    /// Insert the "default" collection if absent.
    pub fn ensure_default(&self) {
        if !self.collections.contains_key("default") {
            let info = CollectionInfo::default_for("default");
            self.collections.insert("default".to_string(), info);
            info!("Created default collection in CollectionManager");
        }
    }

    /// Insert the named collection with default attributes if absent.
    pub fn ensure_collection(&self, name: &str) {
        if !self.collections.contains_key(name) {
            let info = CollectionInfo::default_for(name);
            self.collections.insert(name.to_string(), info);
            info!("Ensured collection exists in CollectionManager: {}", name);
        }
    }

    /// Create a new collection. Fails on empty name, duplicate, or if the
    /// reserved "default" name is supplied while it already exists.
    pub fn create_collection(&self, info: CollectionInfo) -> Result<()> {
        if info.name.is_empty() {
            return Err(PowerFsError::InvalidRequest(
                "collection name cannot be empty".to_string(),
            ));
        }
        if self.collections.contains_key(&info.name) {
            return Err(PowerFsError::InvalidRequest(format!(
                "collection {} already exists",
                info.name
            )));
        }
        let name = info.name.clone();
        self.collections.insert(name.clone(), info);
        info!("Created collection: {}", name);
        Ok(())
    }

    /// Remove a collection. The "default" collection cannot be deleted.
    pub fn delete_collection(&self, name: &str) -> Result<()> {
        if name == "default" {
            return Err(PowerFsError::InvalidRequest(
                "cannot delete default collection".to_string(),
            ));
        }
        if self.collections.remove(name).is_none() {
            return Err(PowerFsError::InvalidRequest(format!(
                "collection {} not found",
                name
            )));
        }
        info!("Deleted collection: {}", name);
        Ok(())
    }

    /// Fetch a snapshot of a collection.
    pub fn get_collection(&self, name: &str) -> Option<CollectionInfo> {
        self.collections.get(name).map(|r| r.clone())
    }

    /// Snapshot all collections.
    pub fn list_collections(&self) -> Vec<CollectionInfo> {
        self.collections.iter().map(|r| r.clone()).collect()
    }

    /// Update a collection in place. `created_at` is preserved and
    /// `updated_at` is refreshed. Renames are rejected.
    pub fn update_collection(&self, name: &str, info: CollectionInfo) -> Result<()> {
        if info.name != name {
            return Err(PowerFsError::InvalidRequest(
                "collection name cannot be changed".to_string(),
            ));
        }
        let mut entry = self.collections.get_mut(name).ok_or_else(|| {
            PowerFsError::InvalidRequest(format!("collection {} not found", name))
        })?;
        let created_at = entry.created_at;
        let mut new_info = info;
        new_info.created_at = created_at;
        new_info.updated_at = chrono::Utc::now().timestamp();
        *entry = new_info;
        Ok(())
    }

    /// Compute runtime stats for a collection from the given volume table.
    ///
    /// Volumes whose `collection` field matches `name` contribute to the
    /// stats. `file_count` is approximated as `next_file_key - 1`.
    pub fn compute_stats(
        &self,
        name: &str,
        volumes: &HashMap<VolumeId, VolumeInfo>,
    ) -> CollectionStats {
        let mut stats = CollectionStats::default();
        let target = Collection(name.to_string());
        for vol in volumes.values() {
            if vol.collection != target {
                continue;
            }
            stats.volume_count += 1;
            stats.used_bytes += vol.used;
            stats.file_count += vol.next_file_key.saturating_sub(1);
            if matches!(vol.state, VolumeState::Creating | VolumeState::Available) {
                stats.writable_volume_count += 1;
            }
        }
        stats
    }

    /// Check whether the collection is within its capacity quota.
    ///
    /// `capacity_quota_bytes == 0` is treated as unlimited. Otherwise the
    /// aggregate `used_bytes` must remain below the quota.
    pub fn check_capacity(
        &self,
        name: &str,
        volumes: &HashMap<VolumeId, VolumeInfo>,
    ) -> Result<()> {
        let info = self.get_collection(name).ok_or_else(|| {
            PowerFsError::InvalidRequest(format!("collection {} not found", name))
        })?;
        if info.capacity_quota_bytes == 0 {
            return Ok(());
        }
        let stats = self.compute_stats(name, volumes);
        if stats.used_bytes >= info.capacity_quota_bytes {
            return Err(PowerFsError::InvalidRequest(format!(
                "collection {} capacity quota exhausted: used={} quota={}",
                name, stats.used_bytes, info.capacity_quota_bytes
            )));
        }
        Ok(())
    }

    /// Returns true if the collection exists and is writable.
    pub fn is_writable(&self, name: &str) -> bool {
        self.collections
            .get(name)
            .map(|r| r.is_writable())
            .unwrap_or(false)
    }

    /// Serialize all collections to JSON for persistence.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let snapshot: Vec<CollectionInfo> = self.list_collections();
        serde_json::to_vec(&snapshot).map_err(|e| PowerFsError::Internal(e.to_string()))
    }

    /// Replace all collections from a JSON snapshot produced by [`serialize`].
    /// The "default" collection is always re-ensured after restore.
    pub fn restore(&self, data: &[u8]) -> Result<()> {
        let snapshot: Vec<CollectionInfo> =
            serde_json::from_slice(data).map_err(|e| PowerFsError::Internal(e.to_string()))?;
        self.collections.clear();
        for info in snapshot {
            self.collections.insert(info.name.clone(), info);
        }
        self.ensure_default();
        Ok(())
    }
}

impl Default for CollectionManager {
    fn default() -> Self {
        Self::with_default()
    }
}

impl std::fmt::Debug for CollectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<String> = self.collections.iter().map(|r| r.key().clone()).collect();
        f.debug_struct("CollectionManager")
            .field("count", &names.len())
            .field("names", &names)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use powerfs_common::types::{DiskType, NodeId, Ttl};

    fn make_volume(
        volume_id: u64,
        collection: &str,
        used: u64,
        size: u64,
        state: VolumeState,
    ) -> VolumeInfo {
        VolumeInfo {
            id: VolumeId(volume_id),
            node_id: NodeId("node-1".to_string()),
            collection: Collection(collection.to_string()),
            size,
            used,
            replica_count: 1,
            ttl: Ttl::default(),
            disk_type: DiskType::default(),
            state,
            created_at: chrono::Utc::now(),
            modified_at: chrono::Utc::now(),
            next_file_key: 1,
        }
    }

    #[test]
    fn test_default_collection_loaded() {
        let mgr = CollectionManager::with_default();
        assert!(mgr.get_collection("default").is_some());
        assert!(mgr.is_writable("default"));
    }

    #[test]
    fn test_create_and_delete() {
        let mgr = CollectionManager::with_default();
        let mut info = CollectionInfo::default_for("test-coll");
        info.capacity_quota_bytes = 1000;
        mgr.create_collection(info).unwrap();
        assert!(mgr.get_collection("test-coll").is_some());
        assert!(mgr.delete_collection("test-coll").is_ok());
        assert!(mgr.get_collection("test-coll").is_none());
    }

    #[test]
    fn test_create_duplicate_rejected() {
        let mgr = CollectionManager::with_default();
        let info = CollectionInfo::default_for("dup");
        mgr.create_collection(info).unwrap();
        let info2 = CollectionInfo::default_for("dup");
        assert!(mgr.create_collection(info2).is_err());
    }

    #[test]
    fn test_create_empty_name_rejected() {
        let mgr = CollectionManager::with_default();
        let info = CollectionInfo::default_for("");
        assert!(mgr.create_collection(info).is_err());
    }

    #[test]
    fn test_cannot_delete_default() {
        let mgr = CollectionManager::with_default();
        assert!(mgr.delete_collection("default").is_err());
    }

    #[test]
    fn test_compute_stats_filters_by_collection() {
        let mgr = CollectionManager::with_default();
        let mut volumes = HashMap::new();
        volumes.insert(
            VolumeId(1),
            make_volume(1, "default", 100, 1000, VolumeState::Available),
        );
        volumes.insert(
            VolumeId(2),
            make_volume(2, "default", 200, 1000, VolumeState::ReadOnly),
        );
        volumes.insert(
            VolumeId(3),
            make_volume(3, "other", 500, 1000, VolumeState::Available),
        );
        let stats = mgr.compute_stats("default", &volumes);
        assert_eq!(stats.volume_count, 2);
        assert_eq!(stats.used_bytes, 300);
        assert_eq!(stats.writable_volume_count, 1);
    }

    #[test]
    fn test_check_capacity_ok_under_quota() {
        let mgr = CollectionManager::with_default();
        let mut info = CollectionInfo::default_for("cap-test");
        info.capacity_quota_bytes = 1000;
        mgr.create_collection(info).unwrap();
        let mut volumes = HashMap::new();
        volumes.insert(
            VolumeId(1),
            make_volume(1, "cap-test", 100, 1000, VolumeState::Available),
        );
        assert!(mgr.check_capacity("cap-test", &volumes).is_ok());
    }

    #[test]
    fn test_check_capacity_rejected_when_exhausted() {
        let mgr = CollectionManager::with_default();
        let mut info = CollectionInfo::default_for("cap-exhaust");
        info.capacity_quota_bytes = 100;
        mgr.create_collection(info).unwrap();
        let mut volumes = HashMap::new();
        volumes.insert(
            VolumeId(1),
            make_volume(1, "cap-exhaust", 200, 1000, VolumeState::Available),
        );
        assert!(mgr.check_capacity("cap-exhaust", &volumes).is_err());
    }

    #[test]
    fn test_capacity_zero_means_unlimited() {
        let mgr = CollectionManager::with_default();
        let mut volumes = HashMap::new();
        volumes.insert(
            VolumeId(1),
            make_volume(1, "default", 9_999_999, 1000, VolumeState::Available),
        );
        assert!(mgr.check_capacity("default", &volumes).is_ok());
    }

    #[test]
    fn test_check_capacity_unknown_collection() {
        let mgr = CollectionManager::with_default();
        let volumes = HashMap::new();
        assert!(mgr.check_capacity("does-not-exist", &volumes).is_err());
    }

    #[test]
    fn test_status_writable_matrix() {
        let mut info = CollectionInfo::default_for("x");
        assert!(info.is_writable());
        info.status = CollectionStatus::Readonly;
        assert!(!info.is_writable());
        info.status = CollectionStatus::Archived;
        assert!(!info.is_writable());
        info.status = CollectionStatus::Deleted;
        assert!(!info.is_writable());
    }

    #[test]
    fn test_serialize_restore_roundtrip() {
        let mgr = CollectionManager::with_default();
        let mut info = CollectionInfo::default_for("persist-test");
        info.description = "test desc".to_string();
        info.capacity_quota_bytes = 12345;
        mgr.create_collection(info).unwrap();
        let data = mgr.serialize().unwrap();
        let mgr2 = CollectionManager::new();
        mgr2.restore(&data).unwrap();
        assert!(mgr2.get_collection("default").is_some());
        let restored = mgr2.get_collection("persist-test").unwrap();
        assert_eq!(restored.description, "test desc");
        assert_eq!(restored.capacity_quota_bytes, 12345);
    }

    #[test]
    fn test_update_collection_preserves_created_at() {
        let mgr = CollectionManager::with_default();
        let info = CollectionInfo::default_for("upd-test");
        mgr.create_collection(info).unwrap();
        let original = mgr.get_collection("upd-test").unwrap();
        let mut new_info = CollectionInfo::default_for("upd-test");
        new_info.description = "updated".to_string();
        // Force a different created_at to verify it is preserved.
        new_info.created_at = original.created_at - 1000;
        mgr.update_collection("upd-test", new_info).unwrap();
        let got = mgr.get_collection("upd-test").unwrap();
        assert_eq!(got.description, "updated");
        assert_eq!(got.created_at, original.created_at);
    }

    #[test]
    fn test_update_collection_rejects_rename() {
        let mgr = CollectionManager::with_default();
        let info = CollectionInfo::default_for("rename-test");
        mgr.create_collection(info).unwrap();
        let new_info = CollectionInfo::default_for("other-name");
        assert!(mgr.update_collection("rename-test", new_info).is_err());
    }

    #[test]
    fn test_update_unknown_collection() {
        let mgr = CollectionManager::with_default();
        let info = CollectionInfo::default_for("nope");
        assert!(mgr.update_collection("nope", info).is_err());
    }

    #[test]
    fn test_ensure_collection_idempotent() {
        let mgr = CollectionManager::with_default();
        mgr.ensure_collection("ensured");
        mgr.ensure_collection("ensured");
        assert!(mgr.get_collection("ensured").is_some());
        assert_eq!(
            mgr.list_collections()
                .iter()
                .filter(|c| c.name == "ensured")
                .count(),
            1
        );
    }

    #[test]
    fn test_default_for_sets_active_status() {
        let info = CollectionInfo::default_for("anything");
        assert_eq!(info.status, CollectionStatus::Active);
        assert_eq!(info.capacity_quota_bytes, 0);
        assert!(info.excluded_volume_ids.is_empty());
    }

    #[test]
    fn test_redundancy_default_is_replication() {
        let policy = StoragePolicy::default();
        assert_eq!(policy.min_write_nodes, 0);
        match policy.redundancy {
            RedundancyMode::Replication { copies } => assert_eq!(copies, 1),
            _ => panic!("expected Replication by default"),
        }
    }

    #[test]
    fn test_volume_allocation_default_is_auto() {
        let info = CollectionInfo::default_for("x");
        match info.volume_allocation {
            VolumeAllocationMode::Auto { count, volume_size } => {
                assert_eq!(count, 0);
                assert_eq!(volume_size, 0);
            }
            _ => panic!("expected Auto by default"),
        }
    }
}
