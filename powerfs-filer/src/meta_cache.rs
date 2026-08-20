//! Filer-side metadata staging cache.
//!
//! When `async_meta_persist` is enabled (default), `CreateInode` +
//! `AddDirEntry` are staged in this cache and immediately visible to reads,
//! while the Raft propose happens (waiting for commit, NOT apply).
//!
//! Only **dirty** (modified via `setattr`/`update_size_chunks`) and
//! **deleted** (`unlink`/`rmdir`) entries go through synchronous Raft
//! `propose` (waiting for quorum commit).
//!
//! ## State transitions
//!
//! ```text
//! create() → Staging → (Raft apply) → [removed from cache, served by ShardStore]
//! setattr() → (synchronous Raft, no staging needed)
//! unlink() → (synchronous Raft, no staging needed)
//! leader change → invalidate_all() → [all staging cleared]
//! ```
//!
//! ## Why must persist go through Raft?
//!
//! If create writes directly to RocksDB without Raft:
//! - Leader writes local RocksDB → returns success
//! - Leader crashes → Follower takes over
//! - Follower's RocksDB doesn't have this entry → data loss → cannot failover
//!
//! Raft provides fault-tolerant failover:
//! - Leader writes Raft log → replicates to Followers → quorum commit
//! - Leader crashes → Follower has complete Raft log → replays apply → data intact
//!
//! MetaCache staging is NOT persistence — it only accelerates read visibility.
//! Real persistence is Raft commit → ShardStore (RocksDB).

use std::collections::HashMap;
use std::sync::RwLock;

use log::{debug, trace, warn};

use crate::shard_store::InodeInfo;

/// Key for directory entry cache: (parent_inode, name).
type DirEntryKey = (u64, String);

/// Staging cache for newly created metadata entries.
///
/// Entries live here between the moment `create_inode()` stages them and
/// the moment Raft applies them to `ShardStore`. Once `ShardStore` has the
/// entry, the staging copy is redundant and can be swept.
pub struct MetaCache {
    /// Newly created inodes awaiting Raft apply.
    staging_inodes: RwLock<HashMap<u64, InodeInfo>>,

    /// Newly created directory entries awaiting Raft apply.
    staging_direntries: RwLock<HashMap<DirEntryKey, u64>>,

    /// Recently deleted inodes (pending Raft commit).
    deleted_inodes: RwLock<HashMap<u64, std::time::Instant>>,

    /// Recently deleted directory entries (pending Raft commit).
    deleted_direntries: RwLock<HashMap<DirEntryKey, std::time::Instant>>,
}

impl MetaCache {
    pub fn new() -> Self {
        Self {
            staging_inodes: RwLock::new(HashMap::new()),
            staging_direntries: RwLock::new(HashMap::new()),
            deleted_inodes: RwLock::new(HashMap::new()),
            deleted_direntries: RwLock::new(HashMap::new()),
        }
    }

    // ---------- create staging ----------

    /// Stage a newly created inode + directory entry.
    ///
    /// Called BEFORE `propose` so the entry is visible to reads
    /// immediately, bridging the commit→apply gap.
    pub fn stage_create(&self, info: InodeInfo, parent_inode: u64, name: &str) {
        let ino = info.inode;
        {
            let mut inodes = self.staging_inodes.write().unwrap();
            inodes.insert(ino, info);
        }
        {
            let mut entries = self.staging_direntries.write().unwrap();
            entries.insert((parent_inode, name.to_string()), ino);
        }
        // Clear any stale deletion markers for this inode
        {
            let mut del_inodes = self.deleted_inodes.write().unwrap();
            del_inodes.remove(&ino);
        }
        {
            let mut del_entries = self.deleted_direntries.write().unwrap();
            del_entries.remove(&(parent_inode, name.to_string()));
        }

        trace!(
            "MetaCache::stage_create: inode={} parent={} name={}",
            ino, parent_inode, name
        );
    }

    // ---------- read path ----------

    /// Try to get an inode from the staging cache.
    ///
    /// Returns:
    /// - `Some(Some(info))` if found in staging (newly created, pending apply)
    /// - `Some(None)` if the inode was recently deleted (return ENOENT)
    /// - `None` if not in staging → caller should check ShardStore
    pub fn get_inode(&self, inode: u64) -> Option<Option<InodeInfo>> {
        // Check deleted first — a pending delete takes precedence
        if self.deleted_inodes.read().unwrap().contains_key(&inode) {
            return Some(None);
        }
        // Check staging
        if let Some(info) = self.staging_inodes.read().unwrap().get(&inode) {
            return Some(Some(info.clone()));
        }
        // Not in cache → caller checks ShardStore
        None
    }

    /// Try to get a directory entry from the staging cache.
    ///
    /// Returns:
    /// - `Some(Some(child_ino))` if found in staging
    /// - `Some(None)` if recently deleted (return ENOENT)
    /// - `None` if not in staging → caller should check ShardStore
    pub fn get_direntry(&self, parent_inode: u64, name: &str) -> Option<Option<u64>> {
        let key = (parent_inode, name.to_string());

        // Check deleted first
        if self.deleted_direntries.read().unwrap().contains_key(&key) {
            return Some(None);
        }
        // Check staging
        if let Some(&child_ino) = self.staging_direntries.read().unwrap().get(&key) {
            return Some(Some(child_ino));
        }
        None
    }

    // ---------- delete staging ----------

    /// Mark an inode as deleted (pending Raft commit).
    pub fn stage_delete_inode(&self, inode: u64) {
        self.staging_inodes.write().unwrap().remove(&inode);
        self.deleted_inodes
            .write()
            .unwrap()
            .insert(inode, std::time::Instant::now());
    }

    /// Mark a directory entry as deleted (pending Raft commit).
    pub fn stage_delete_direntry(&self, parent_inode: u64, name: &str) {
        let key = (parent_inode, name.to_string());
        self.staging_direntries.write().unwrap().remove(&key);
        self.deleted_direntries
            .write()
            .unwrap()
            .insert(key, std::time::Instant::now());
    }

    // ---------- Raft apply confirmation ----------

    /// Called when Raft has applied a `CreateInode` command.
    pub fn confirm_create_inode(&self, inode: u64) {
        self.staging_inodes.write().unwrap().remove(&inode);
    }

    /// Called when Raft has applied an `AddDirEntry` command.
    pub fn confirm_add_direntry(&self, parent_inode: u64, name: &str) {
        self.staging_direntries
            .write()
            .unwrap()
            .remove(&(parent_inode, name.to_string()));
    }

    /// Called when Raft has applied a `DeleteInode` command.
    pub fn confirm_delete_inode(&self, inode: u64) {
        self.deleted_inodes.write().unwrap().remove(&inode);
    }

    /// Called when Raft has applied a `RemoveDirEntry` command.
    pub fn confirm_remove_direntry(&self, parent_inode: u64, name: &str) {
        self.deleted_direntries
            .write()
            .unwrap()
            .remove(&(parent_inode, name.to_string()));
    }

    // ---------- bulk operations ----------

    /// Remove a specific staging entry (used when propose fails).
    ///
    /// Called when Raft propose fails (e.g., lost leadership): the staged
    /// entry is NOT committed, so it must be removed to prevent reads from
    /// seeing uncommitted data.
    pub fn invalidate_staging(&self, inode: u64, parent_inode: u64, name: &str) {
        self.staging_inodes.write().unwrap().remove(&inode);
        self.staging_direntries
            .write()
            .unwrap()
            .remove(&(parent_inode, name.to_string()));
    }

    /// Clear all staging and deletion entries.
    ///
    /// Called on leader change (cache epoch mismatch): the old leader's
    /// pending Raft entries may not survive the transition, so all staged
    /// creates are invalidated. The client will retry on the new leader.
    pub fn invalidate_all(&self) {
        let staged_inodes = self.staging_inodes.read().unwrap().len();
        let staged_direntries = self.staging_direntries.read().unwrap().len();
        let deleted_inodes = self.deleted_inodes.read().unwrap().len();
        let deleted_direntries = self.deleted_direntries.read().unwrap().len();

        self.staging_inodes.write().unwrap().clear();
        self.staging_direntries.write().unwrap().clear();
        self.deleted_inodes.write().unwrap().clear();
        self.deleted_direntries.write().unwrap().clear();

        if staged_inodes > 0 || staged_direntries > 0 || deleted_inodes > 0 || deleted_direntries > 0 {
            warn!(
                "MetaCache::invalidate_all: cleared {} staging_inodes + {} staging_direntries + {} deleted_inodes + {} deleted_direntries",
                staged_inodes, staged_direntries, deleted_inodes, deleted_direntries
            );
        }
    }

    /// Sweep expired deletion markers.
    pub fn sweep_expired_deletions(&self, max_age: std::time::Duration) {
        let now = std::time::Instant::now();

        let expired_inodes = {
            let mut del = self.deleted_inodes.write().unwrap();
            let before = del.len();
            del.retain(|_, ts| now.duration_since(*ts) < max_age);
            before - del.len()
        };

        let expired_direntries = {
            let mut del = self.deleted_direntries.write().unwrap();
            let before = del.len();
            del.retain(|_, ts| now.duration_since(*ts) < max_age);
            before - del.len()
        };

        if expired_inodes > 0 || expired_direntries > 0 {
            debug!(
                "MetaCache::sweep_expired_deletions: removed {} expired inode deletions + {} expired direntry deletions",
                expired_inodes, expired_direntries
            );
        }
    }

    // ---------- stats ----------

    pub fn staging_inode_count(&self) -> usize {
        self.staging_inodes.read().unwrap().len()
    }

    pub fn staging_direntry_count(&self) -> usize {
        self.staging_direntries.read().unwrap().len()
    }
}

impl Default for MetaCache {
    fn default() -> Self {
        Self::new()
    }
}
