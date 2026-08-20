//! Filer-side metadata staging cache.
//!
//! When `async_meta_persist` is enabled (default), `CreateInode` +
//! `AddDirEntry` are staged in this cache and immediately visible to reads,
//! while the Raft propose happens (waiting for commit, NOT apply).
//!
//! Entries transition through [`CacheState`] states:
//!
//! ```text
//! create() → Staging → (Raft apply + confirm_create) → Clean
//! setattr() → Dirty (local) → (Raft apply + confirm_dirty) → Clean
//! unlink()  → Deleted (local) → (Raft apply + confirm_delete) → removed
//! leader change → invalidate_all() → all staging/dirty/deleted cleared
//! ```
//!
//! `Clean` entries are cached for **read acceleration**: subsequent lookups /
//! getattrs hit MetaCache first and avoid RocksDB deserialization.
//!
//! MetaCache staging is NOT persistence — it only accelerates visibility.
//! Real persistence is Raft commit → ShardStore (RocksDB).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Duration;

use log::{debug, trace, warn};

use crate::shard_store::InodeInfo;

/// Key for directory entry cache: (parent_inode, name).
type DirEntryKey = (u64, String);

/// Lifecycle state for a cached inode / dir entry.
///
/// Mirrors the design-doc `CacheState` state machine (minus `Trimming` — we
/// add DirFrag-level trim in a later phase).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheState {
    /// Persistent copy (RocksDB) is authoritative and in sync.
    /// Can be returned directly; can be evicted by future trim logic.
    Clean,
    /// Newly created, waiting for Raft apply.
    /// Must NOT be evicted; reads return the staged value directly.
    Staging,
    /// Modified locally, SetAttr Raft command in flight / waiting for apply.
    /// Must NOT be evicted; reads return the dirty (pending) value.
    Dirty,
    /// Deleted locally, RemoveDirEntry/DeleteInode Raft command in flight.
    /// Must NOT be evicted; reads against this entry return ENOENT.
    Deleted,
}

/// An inode cached in memory.
///
/// `info` is always the "intended state":
/// - Staging → what we proposed to Raft (creates visible immediately)
/// - Dirty   → what we proposed via SetAttr (visible immediately)
/// - Clean   → what RocksDB confirmed on apply or on last read
pub struct CachedInode {
    /// Full inode metadata (same shape as `InodeInfo` in RocksDB).
    pub info: InodeInfo,

    /// Current state.
    pub state: CacheState,

    /// Approximate last-access monotonic ms (used by future LRU trim).
    pub last_access_ms: AtomicU64,

    /// Outstanding lease / references from FUSE clients.
    /// Trim will NOT evict entries where `refcount > 0`.
    pub refcount: AtomicU32,

    /// Raft-applied version. For Clean entries this is the last SetAttr /
    /// Create raft version; for Staging/Dirty this is 0.
    pub raft_version: AtomicU64,
}

impl CachedInode {
    pub fn new(info: InodeInfo, state: CacheState) -> Self {
        Self {
            info,
            state,
            last_access_ms: AtomicU64::new(now_ms()),
            refcount: AtomicU32::new(0),
            raft_version: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn touch(&self) {
        self.last_access_ms.store(now_ms(), Ordering::Relaxed);
    }
}

/// A cached directory entry: parent + name → child inode, plus a state.
///
/// The "intended value" rule matches `CachedInode`:
/// - Staging/Dirty/Clean → child_inode returned to the caller
/// - Deleted → ENOENT
struct CachedDirEntry {
    child_inode: u64,
    state: CacheState,
    last_access_ms: AtomicU64,
}

impl CachedDirEntry {
    fn new(child_inode: u64, state: CacheState) -> Self {
        Self {
            child_inode,
            state,
            last_access_ms: AtomicU64::new(now_ms()),
        }
    }

    #[inline]
    fn touch(&self) {
        self.last_access_ms.store(now_ms(), Ordering::Relaxed);
    }
}

/// Filer meta cache (Phase 1 + Phase 2 foundations).
pub struct MetaCache {
    /// Inode cache by inode number.
    inode_table: RwLock<HashMap<u64, CachedInode>>,

    /// Directory entry cache by (parent_inode, name).
    direntry_table: RwLock<HashMap<DirEntryKey, CachedDirEntry>>,
}

impl Default for MetaCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaCache {
    pub fn new() -> Self {
        Self {
            inode_table: RwLock::new(HashMap::new()),
            direntry_table: RwLock::new(HashMap::new()),
        }
    }

    // ---------- create staging ----------

    /// Stage a newly created inode + directory entry as `Staging` state.
    ///
    /// Called BEFORE `propose` so the entry is visible to reads immediately,
    /// bridging the commit→apply gap.
    pub fn stage_create(&self, info: InodeInfo, parent_inode: u64, name: &str) {
        let ino = info.inode;
        {
            let mut tbl = self.inode_table.write().unwrap();
            tbl.insert(ino, CachedInode::new(info, CacheState::Staging));
        }
        {
            let mut tbl = self.direntry_table.write().unwrap();
            let key = (parent_inode, name.to_string());
            tbl.insert(key, CachedDirEntry::new(ino, CacheState::Staging));
            // Clean up any stale Deleted marker for the same entry (recreate).
        }
        trace!(
            "MetaCache::stage_create: inode={} parent={} name={}",
            ino,
            parent_inode,
            name
        );
    }

    // ---------- setattr dirty ----------

    /// Mark a cached inode as dirty in response to a SetAttr call.
    ///
    /// `updater` is applied to the cached copy so subsequent reads see the
    /// new value immediately (even though RocksDB apply is still pending).
    /// If the inode is not cached, `fallback_current` is used to seed it.
    pub fn mark_dirty<F>(&self, inode: u64, fallback_current: Option<InodeInfo>, mut updater: F)
    where
        F: FnMut(&mut InodeInfo),
    {
        let mut tbl = self.inode_table.write().unwrap();
        match tbl.get_mut(&inode) {
            Some(existing) => {
                updater(&mut existing.info);
                existing.state = CacheState::Dirty;
                existing.touch();
            }
            None => {
                if let Some(mut info) = fallback_current {
                    updater(&mut info);
                    let ci = CachedInode::new(info, CacheState::Dirty);
                    ci.touch();
                    tbl.insert(inode, ci);
                }
            }
        }
    }

    // ---------- delete staging ----------

    /// Mark an inode and its directory entry as `Deleted` (pending Raft).
    ///
    /// After this call, reads return ENOENT. The marker is cleared on apply
    /// or by `invalidate_all` on leader change.
    pub fn stage_delete(&self, parent_inode: u64, name: &str, inode: u64) {
        // 1. inode table → state = Deleted
        {
            let mut tbl = self.inode_table.write().unwrap();
            if let Some(ci) = tbl.get_mut(&inode) {
                ci.state = CacheState::Deleted;
            } else {
                // Seed a Deleted tombstone so later reads hit ENOENT without
                // checking ShardStore. info is unused (Deleted returns None).
                let tomb = InodeInfo::tombstone(inode);
                let ci = CachedInode::new(tomb, CacheState::Deleted);
                ci.refcount.store(0, Ordering::Relaxed);
                tbl.insert(inode, ci);
            }
        }
        // 2. direntry table → state = Deleted
        {
            let mut tbl = self.direntry_table.write().unwrap();
            let key = (parent_inode, name.to_string());
            if let Some(de) = tbl.get_mut(&key) {
                de.state = CacheState::Deleted;
                de.touch();
            } else {
                let de = CachedDirEntry::new(inode, CacheState::Deleted);
                de.touch();
                tbl.insert(key, de);
            }
        }
    }

    // ---------- read path ----------

    /// Try to read an inode from the cache.
    ///
    /// Returns:
    /// - `Some(Some(info))` → found (Staging/Clean/Dirty).
    /// - `Some(None)`       → known-Deleted by a pending unlink; return ENOENT.
    /// - `None`             → cache miss, caller must check ShardStore and
    ///   call `cache_put_clean()` on a hit to populate.
    pub fn get_inode(&self, inode: u64) -> Option<Option<InodeInfo>> {
        let tbl = self.inode_table.read().unwrap();
        let ci = tbl.get(&inode)?;
        ci.touch();
        match ci.state {
            CacheState::Deleted => Some(None),
            _ => Some(Some(ci.info.clone())),
        }
    }

    /// Populate (or refresh) the cache with a Clean copy just read from
    /// ShardStore. No-ops if the entry exists and is not Clean-compatible
    /// (i.e. it's Staging/Dirty/Deleted and being actively managed).
    pub fn cache_put_clean(&self, info: InodeInfo) {
        let ino = info.inode;
        let mut tbl = self.inode_table.write().unwrap();
        // Do NOT clobber Staging / Dirty / Deleted entries — those are the
        // authoritative "intended state". Only populate on miss or on a
        // stale Clean copy.
        let needs_insert = match tbl.get(&ino) {
            None => true,
            Some(existing) => existing.state == CacheState::Clean,
        };
        if needs_insert {
            let ci = CachedInode::new(info, CacheState::Clean);
            tbl.insert(ino, ci);
        }
    }

    /// Try to read a directory entry from the cache.
    ///
    /// Same semantics as [`Self::get_inode`] but for dir entries.
    pub fn get_direntry(&self, parent_inode: u64, name: &str) -> Option<Option<u64>> {
        let key = (parent_inode, name.to_string());
        let tbl = self.direntry_table.read().unwrap();
        let de = tbl.get(&key)?;
        de.touch();
        match de.state {
            CacheState::Deleted => Some(None),
            _ => Some(Some(de.child_inode)),
        }
    }

    /// Populate a Clean directory entry mapping (read back from ShardStore).
    pub fn cache_put_clean_direntry(&self, parent_inode: u64, name: &str, child_inode: u64) {
        let key = (parent_inode, name.to_string());
        let mut tbl = self.direntry_table.write().unwrap();
        let needs_insert = match tbl.get(&key) {
            None => true,
            Some(existing) => existing.state == CacheState::Clean,
        };
        if needs_insert {
            let de = CachedDirEntry::new(child_inode, CacheState::Clean);
            tbl.insert(key, de);
        }
    }

    // ---------- Raft apply confirmations ----------

    /// Raft applied `CreateInode` → the ShardStore copy is authoritative.
    /// Demote `Staging` → `Clean` (keep cached for reads).
    pub fn confirm_create_inode(&self, inode: u64) {
        let mut tbl = self.inode_table.write().unwrap();
        if let Some(ci) = tbl.get_mut(&inode) {
            if ci.state == CacheState::Staging {
                ci.state = CacheState::Clean;
            }
        }
    }

    /// Raft applied `AddDirEntry` → promote `Staging` dir entry → `Clean`.
    pub fn confirm_add_direntry(&self, parent_inode: u64, name: &str) {
        let key = (parent_inode, name.to_string());
        let mut tbl = self.direntry_table.write().unwrap();
        if let Some(de) = tbl.get_mut(&key) {
            if de.state == CacheState::Staging {
                de.state = CacheState::Clean;
            }
        }
    }

    /// Raft applied a SetAttr → the cached Dirty copy is now consistent
    /// with RocksDB → state becomes Clean.
    pub fn confirm_dirty(&self, inode: u64) {
        let mut tbl = self.inode_table.write().unwrap();
        if let Some(ci) = tbl.get_mut(&inode) {
            if ci.state == CacheState::Dirty {
                ci.state = CacheState::Clean;
            }
        }
    }

    /// Raft applied `DeleteInode` → drop inode cache entry entirely.
    pub fn confirm_delete_inode(&self, inode: u64) {
        let mut tbl = self.inode_table.write().unwrap();
        tbl.remove(&inode);
    }

    /// Raft applied `RemoveDirEntry` → drop dir-entry cache entry entirely.
    pub fn confirm_remove_direntry(&self, parent_inode: u64, name: &str) {
        let key = (parent_inode, name.to_string());
        let mut tbl = self.direntry_table.write().unwrap();
        tbl.remove(&key);
    }

    // ---------- bulk / recovery ops ----------

    /// Remove a specific staging entry (used when Raft propose fails).
    /// The entry never committed, so we cannot leave it visible to reads.
    pub fn invalidate_staging(&self, inode: u64, parent_inode: u64, name: &str) {
        // Clean up inode: remove only if it was still Staging.
        {
            let mut tbl = self.inode_table.write().unwrap();
            let should_remove = tbl
                .get(&inode)
                .map(|ci| ci.state == CacheState::Staging)
                .unwrap_or(false);
            if should_remove {
                tbl.remove(&inode);
            }
        }
        // Clean up direntry: same rule.
        {
            let key = (parent_inode, name.to_string());
            let mut tbl = self.direntry_table.write().unwrap();
            let should_remove = tbl
                .get(&key)
                .map(|de| de.state == CacheState::Staging)
                .unwrap_or(false);
            if should_remove {
                tbl.remove(&key);
            }
        }
    }

    /// Clear ALL in-memory cache state.
    ///
    /// Called on leader change: the old leader's pending Raft entries may
    /// not survive the transition. We drop everything (Clean included)
    /// because on a new leader we cannot trust which in-flight proposals
    /// the previous leader broadcast before losing leadership. The client
    /// retries on the new leader and the cache repopulates from ShardStore.
    pub fn invalidate_all(&self) {
        let (inode_count, de_count) = {
            let it = self.inode_table.read().unwrap();
            let dt = self.direntry_table.read().unwrap();
            (it.len(), dt.len())
        };
        self.inode_table.write().unwrap().clear();
        self.direntry_table.write().unwrap().clear();

        if inode_count > 0 || de_count > 0 {
            warn!(
                "MetaCache::invalidate_all: dropped {} inodes + {} dir entries (leader change)",
                inode_count, de_count
            );
        }
    }

    /// Sweep entries left in `Deleted` state past `max_age`.
    ///
    /// Guard against orphan Deleted tombstones when a Raft apply callback
    /// is delayed or lost (e.g. shard transfer, node isolation).
    pub fn sweep_expired_deletions(&self, max_age: Duration) {
        let now_ms = now_ms();

        let removed_inodes = {
            let mut tbl = self.inode_table.write().unwrap();
            let before = tbl.len();
            tbl.retain(|ino, ci| {
                if ci.state != CacheState::Deleted {
                    return true;
                }
                // Deleted entries don't carry an insert timestamp — reuse
                // `last_access_ms` as a proxy (it was stamped at insert).
                let age_ms = now_ms.saturating_sub(ci.last_access_ms.load(Ordering::Relaxed));
                Duration::from_millis(age_ms) < max_age || {
                    debug!(
                        "MetaCache sweep: expired Deleted inode tombstone (ino={})",
                        ino
                    );
                    false
                }
            });
            before - tbl.len()
        };

        let removed_dirs = {
            let mut tbl = self.direntry_table.write().unwrap();
            let before = tbl.len();
            tbl.retain(|(parent, name), de| {
                if de.state != CacheState::Deleted {
                    return true;
                }
                let age_ms = now_ms.saturating_sub(de.last_access_ms.load(Ordering::Relaxed));
                Duration::from_millis(age_ms) < max_age || {
                    debug!(
                        "MetaCache sweep: expired Deleted direntry tombstone (parent={}, name={})",
                        parent, name
                    );
                    false
                }
            });
            before - tbl.len()
        };

        if removed_inodes > 0 || removed_dirs > 0 {
            debug!(
                "MetaCache::sweep_expired_deletions: removed {} inode tombstones + {} direntry tombstones",
                removed_inodes, removed_dirs
            );
        }
    }

    // ---------- stats ----------

    pub fn staging_inode_count(&self) -> usize {
        self.inode_table
            .read()
            .unwrap()
            .values()
            .filter(|c| c.state == CacheState::Staging)
            .count()
    }

    pub fn staging_direntry_count(&self) -> usize {
        self.direntry_table
            .read()
            .unwrap()
            .values()
            .filter(|d| d.state == CacheState::Staging)
            .count()
    }

    /// Split stat counters (for Prometheus / admin API later).
    pub fn state_counts(&self) -> HashMap<CacheState, usize> {
        let mut m = HashMap::new();
        for ci in self.inode_table.read().unwrap().values() {
            *m.entry(ci.state).or_insert(0) += 1;
        }
        m
    }
}

// ---------- helpers ----------

fn now_ms() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// Re-export so callers (meta_shard_manager) don't have to import state types
// from this file individually.
pub use CacheState as MetaCacheState;
