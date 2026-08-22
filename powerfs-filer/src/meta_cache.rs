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
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use log::{debug, info, trace, warn};

use crate::cap_manager::CapSet;
use crate::shard_store::{InodeInfo, StoredFileChunk};

/// Key for directory entry cache: (parent_inode, name).
type DirEntryKey = (u64, String);

/// Lifecycle state for a cached inode / dir entry.
///
/// Mirrors the design-doc `CacheState` state machine.
/// `Trimming` was added in Phase 2 to avoid a race where a `Clean` entry
/// is picked up by `trim_pass()` and a concurrent read miss backfills it
/// simultaneously; setting `Trimming` short-circuits both until the evictor
/// drops it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheState {
    /// Persistent copy (RocksDB) is authoritative and in sync.
    /// Can be returned directly; can be evicted by trim logic.
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
    /// Phase 2 transient state: picked up by `trim_pass()` and about to be
    /// dropped. Reads treat it as a miss (fall through to RocksDB); a
    /// concurrent `cache_put_clean()` will overwrite it atomically.
    Trimming,
}

/// An inode cached in memory.
///
/// `info` is always the "intended state":
/// - Staging → what we proposed to Raft (creates visible immediately)
/// - Dirty   → what we proposed via SetAttr (visible immediately)
/// - Clean   → what RocksDB confirmed on apply or on last read
///
/// #  alignment: client_caps
///
/// `client_caps` mirrors 's `CInode::client_caps` — an in-memory map
/// from `client_id` to the cap this Filer leader has granted to that
/// client for this inode. Like :
/// - The map is **ephemeral** — it is NOT serialized to RocksDB. Only
///   `info` (the `InodeInfo`) is persisted; cap state is leader-local
///   runtime state that is rebuilt from scratch on leader failover (all
///   clients must re-acquire caps from the new leader).
/// - The map is the **authoritative in-memory record** of who holds
///   what caps; the `CapManager::inodes` HashMap is a fast-path index
///   for the cap state machine but defers to this map for persistence
///   boundaries.
/// - `loner_cap` mirrors 's `CInode::loner_cap` — the single client
///   granted exclusive caps, used as a fast-path check for "no conflict"
///   without scanning the whole map.
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

    /// Per-client caps granted for this inode  `client_caps`).
    ///
    /// Keyed by `client_id`. Each entry records the `cap_id`, the cap
    /// bits currently issued, and the epoch at grant time. This map is
    /// the inode-embedded mirror of `CapManager`'s state and is kept in
    /// sync by `cap_attach` / `cap_detach`.
    ///
    /// **Not serialized** — cap state is leader-local and rebuilt on
    /// failover. Only `info` is persisted to RocksDB.
    pub(crate) client_caps: Mutex<HashMap<String, InodeCapRecord>>,

    /// The single client currently holding exclusive caps, if any
    ///  `loner_cap`). `-1` (encoded as `None`) means no loner.
    /// Used as a fast-path: if a new open comes in and `loner_cap` is
    /// `None` or matches the opener, no conflict scan is needed.
    pub(crate) loner_cap: Mutex<Option<String>>,
}

/// A cap record embedded in `CachedInode::client_caps` — the per-inode
/// mirror of `CapHolder` from `cap_manager.rs`.
///
/// Deliberately smaller than `CapHolder`: it only stores what the inode
/// needs to know for serialization boundaries and conflict checks. The
/// full state machine (revokes history, recall_in_flight, etc.) lives
/// in `CapHolder` under `CapManager`.
#[derive(Clone, Debug)]
pub(crate) struct InodeCapRecord {
    /// Global monotonic cap id  `cap_id`).
    #[allow(dead_code)]
    pub cap_id: u64,
    /// Currently issued caps  `_issued`).
    pub caps: CapSet,
    /// Epoch at grant time — used for fencing stale clients.
    #[allow(dead_code)]
    pub epoch: u64,
    /// True if the client is a writer (opened O_WRONLY/O_RDWR).
    pub is_writer: bool,
}

impl CachedInode {
    pub fn new(info: InodeInfo, state: CacheState) -> Self {
        Self {
            info,
            state,
            last_access_ms: AtomicU64::new(now_ms()),
            refcount: AtomicU32::new(0),
            raft_version: AtomicU64::new(0),
            client_caps: Mutex::new(HashMap::new()),
            loner_cap: Mutex::new(None),
        }
    }

    #[inline]
    pub fn touch(&self) {
        self.last_access_ms.store(now_ms(), Ordering::Relaxed);
    }

    /// Attach a cap record for `client_id`  `CInode::add_client_cap`).
    /// Called by `CapManager::open_grant` via `MetaCache::cap_attach` to
    /// mirror the grant into the inode-embedded `client_caps` map.
    pub(crate) fn add_client_cap(
        &self,
        client_id: &str,
        cap_id: u64,
        caps: CapSet,
        epoch: u64,
        is_writer: bool,
    ) {
        let mut cm = self.client_caps.lock().unwrap();
        let was_empty = cm.is_empty();
        cm.insert(
            client_id.to_string(),
            InodeCapRecord {
                cap_id,
                caps,
                epoch,
                is_writer,
            },
        );
        // First cap → ref the inode  `get(PIN_CAPS)`). We don't
        // need an explicit PIN since MetaCache's `refcount` is already
        // bumped by `CapManager::open_grant` via `incr_refcount`.
        if was_empty && is_writer && caps.is_exclusive() {
            *self.loner_cap.lock().unwrap() = Some(client_id.to_string());
        }
    }

    /// Remove a client's cap record  `CInode::remove_client_cap`).
    /// Called by `CapManager::release_cap` / `close_session` /
    /// `drain_expired_recalls` via `MetaCache::cap_detach`.
    pub(crate) fn remove_client_cap(&self, client_id: &str) {
        let mut cm = self.client_caps.lock().unwrap();
        cm.remove(client_id);
        // Clear loner_cap if it pointed at the removed client.
        let mut loner = self.loner_cap.lock().unwrap();
        if loner.as_deref() == Some(client_id) {
            *loner = None;
        }
        // If exactly one exclusive writer remains, promote it to loner.
        if cm.len() == 1 {
            let sole = cm.iter().next().unwrap();
            if sole.1.is_writer && sole.1.caps.is_exclusive() {
                *loner = Some(sole.0.clone());
            }
        }
    }

    /// Snapshot of `(client_id, caps)` pairs — used for diagnostics
    /// and leader-handoff recall  `CInode::export_client_caps`).
    #[allow(dead_code)]
    pub(crate) fn snapshot_client_caps(&self) -> Vec<(String, CapSet, u64 /* epoch */)> {
        self.client_caps
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.caps, v.epoch))
            .collect()
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

// ========================================================================
// Phase 2: TrimController
// ========================================================================

/// Memory bookkeeping and high/low watermark configuration for the
/// per-shard MetaCache (Phase 2 single-entry trim; Phase 4 will upgrade
/// this to DirFrag-granular eviction).
///
/// Memory usage is **estimated**, not precise (`malloc`/`HashMap` reallocs
/// are out-of-band). It is accurate enough for coarse-grained trim
/// thresholds that keep the Filer from ballooning to multi-GB under
/// workloads that create 100k+ files.
pub(crate) struct TrimController {
    /// Hard upper bound, in bytes. Default: 2 GiB.
    pub memory_limit_bytes: usize,
    /// Start trimming when usage exceeds this fraction of the limit.
    /// Default: 0.8.
    pub high_watermark: f64,
    /// Stop trimming once usage drops below this fraction of the limit.
    /// Default: 0.6.
    pub low_watermark: f64,
    /// For inodes in state = Staging, evict them if the Raft apply
    /// confirm hasn't arrived within this duration (proposal was likely
    /// lost in a leader-change window). Default: 30 s.
    pub staging_timeout_ms: u64,
    /// For inodes/dentries in state = Deleted, evict the tombstone if
    /// Raft confirm hasn't arrived within this duration. Default: 60 s.
    /// (Already exists via `sweep_expired_deletions`; moved here so all
    /// timeouts live in one place.)
    pub deleted_timeout_ms: u64,

    // --- runtime counters (exposed via MetaCacheStats / Prometheus) ---
    pub current_usage_bytes: AtomicUsize,
    pub trim_total: AtomicU64,
    pub staging_timeout_total: AtomicU64,
    pub deleted_timeout_total: AtomicU64,
    /// Phase 3: cumulative lease-recall notifications pushed to clients.
    pub recall_total: AtomicU64,
    /// Phase 3: cumulative times a recall was suppressed because the
    /// inode was still in recall_cooldown (client hasn't had enough
    /// time to release since the last recall).
    pub recall_cooldown_skips: AtomicU64,
    /// Phase 3: cumulative refcount leak fixes (refcount > 0 but no
    /// active lease in InodeLeaseManager, force-reset to 0).
    pub refcount_leak_fixes: AtomicU64,
}

impl TrimController {
    /// Load configuration from env vars (same convention as
    /// `InodeLeaseManager`) with safe defaults.
    pub fn from_env_or_defaults() -> Self {
        // 2 GiB default keeps MetaCache well below typical 8-16 GiB
        // Filer boxes; operators can override per deployment.
        let memory_limit_bytes = std::env::var("POWERFS_MC_MEMORY_LIMIT_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(2 * 1024 * 1024 * 1024);

        let high_watermark = std::env::var("POWERFS_MC_HIGH_WATERMARK")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .map(|v| v.clamp(0.5, 1.0))
            .unwrap_or(0.8);

        let low_watermark = std::env::var("POWERFS_MC_LOW_WATERMARK")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .map(|v| v.clamp(0.3, high_watermark - 0.05))
            .unwrap_or(0.6);

        let staging_timeout_ms = std::env::var("POWERFS_MC_STAGING_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30_000);

        let deleted_timeout_ms = std::env::var("POWERFS_MC_DELETED_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60_000);

        Self {
            memory_limit_bytes,
            high_watermark,
            low_watermark,
            staging_timeout_ms,
            deleted_timeout_ms,
            current_usage_bytes: AtomicUsize::new(0),
            trim_total: AtomicU64::new(0),
            staging_timeout_total: AtomicU64::new(0),
            deleted_timeout_total: AtomicU64::new(0),
            recall_total: AtomicU64::new(0),
            recall_cooldown_skips: AtomicU64::new(0),
            refcount_leak_fixes: AtomicU64::new(0),
        }
    }

    #[inline]
    pub(crate) fn high_bytes(&self) -> usize {
        (self.memory_limit_bytes as f64 * self.high_watermark) as usize
    }

    #[inline]
    pub(crate) fn low_bytes(&self) -> usize {
        (self.memory_limit_bytes as f64 * self.low_watermark) as usize
    }

    #[inline]
    pub(crate) fn charge(&self, bytes: usize) {
        self.current_usage_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn release(&self, bytes: usize) {
        let _ = self.current_usage_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn over_high(&self) -> bool {
        self.current_usage_bytes.load(Ordering::Relaxed) > self.high_bytes()
    }
}

/// Estimate how many heap bytes a cached inode occupies (shallow +
/// `InodeInfo` inline variable-length fields like `symlink_target` and
/// chunks Vec). This is intentionally a loose upper bound: overcharging
/// makes trim run earlier and safer, under-charging would cause OOMs.
#[inline]
fn estimate_inode_bytes(info: &InodeInfo) -> usize {
    // Variable-length tail fields carried by InodeInfo. We deliberately
    // estimate the heap-allocated parts conservatively (a little over is
    // safer than under). The compiler-generated size_of::<CachedInode>()
    // covers just the struct layout (Vec words, etc.); we separately add
    // the heap-resident tail capacity for each vector/string/map.
    let variable: usize = info
        .symlink_target
        .as_ref()
        .map(|s| s.capacity())
        .unwrap_or(0)
        .saturating_add(
            info.chunks
                .capacity()
                .saturating_mul(std::mem::size_of::<StoredFileChunk>()),
        )
        .saturating_add(info.inline_data.as_ref().map(|d| d.capacity()).unwrap_or(0))
        // "extended" = xattrs HashMap<String, Vec<u8>>; 128 bytes/entry is
        // a rough upper bound of k + v + HashMap per-entry bookkeeping.
        .saturating_add(info.extended.len().saturating_mul(128))
        .saturating_add(info.name.capacity())
        .saturating_add(info.fid.as_ref().map(|s| s.capacity()).unwrap_or(0))
        .saturating_add(info.etag.as_ref().map(|s| s.capacity()).unwrap_or(0));
    std::mem::size_of::<CachedInode>()
        .saturating_add(variable)
        .max(512)
}

#[inline]
fn estimate_dentry_bytes(name: &str) -> usize {
    // CachedDirEntry fixed (≈ 40B) + name string heap allocation
    // (rounded up to next 8 + allocator bookkeeping ~16B per String).
    std::mem::size_of::<CachedDirEntry>()
        .saturating_add(name.len())
        .saturating_add(16)
        .max(96)
}

/// Filer meta cache (Phase 1 + Phase 2 + Phase 3 recall).
pub struct MetaCache {
    /// Inode cache by inode number.
    inode_table: RwLock<HashMap<u64, CachedInode>>,

    /// Directory entry cache by (parent_inode, name).
    direntry_table: RwLock<HashMap<DirEntryKey, CachedDirEntry>>,

    // ----- Phase 2 trim controller (shared; per-shard MetaCache has one) -----
    pub(crate) trim: TrimController,

    // ----- Phase 3: recall cooldown tracking -----
    //
    // When trim_pass identifies Clean entries with refcount > 0 as
    // recall candidates, we push an Invalidate to the client and record
    // the timestamp here. Subsequent trim passes skip inodes still in
    // cooldown (default 5 s) to give the client time to release the
    // lease before we recall again, avoiding recall→reacquire storms.
    recall_cooldown: RwLock<HashMap<u64, u64 /* last_recall_ms */>>,

    // ----- Prometheus-friendly cumulative counters -----
    //
    // Mirroring the lease-manager pattern (`powerfs-filer/src/metrics.rs`):
    // these atomics store absolute totals; the metrics endpoint reads them
    // via `stats()` and sets `IntGauge`s that a Prometheus scraper then
    // treats as counters (because monotonic). Using Atomics here keeps
    // MetaCache lock-free on the hot path; the read-only snapshots never
    // contend with `mark_dirty`/`stage_delete` writers.
    pub(crate) inode_hit_total: AtomicU64,
    pub(crate) inode_miss_total: AtomicU64,
    pub(crate) inode_deleted_served_total: AtomicU64,
    pub(crate) direntry_hit_total: AtomicU64,
    pub(crate) direntry_miss_total: AtomicU64,
    pub(crate) direntry_deleted_served_total: AtomicU64,
    pub(crate) dirty_mark_total: AtomicU64,
    pub(crate) dirty_confirm_total: AtomicU64,
    pub(crate) stage_delete_total: AtomicU64,
    pub(crate) backfill_clean_total: AtomicU64,
    pub(crate) invalidate_all_total: AtomicU64,
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
            trim: TrimController::from_env_or_defaults(),
            recall_cooldown: RwLock::new(HashMap::new()),
            inode_hit_total: AtomicU64::new(0),
            inode_miss_total: AtomicU64::new(0),
            inode_deleted_served_total: AtomicU64::new(0),
            direntry_hit_total: AtomicU64::new(0),
            direntry_miss_total: AtomicU64::new(0),
            direntry_deleted_served_total: AtomicU64::new(0),
            dirty_mark_total: AtomicU64::new(0),
            dirty_confirm_total: AtomicU64::new(0),
            stage_delete_total: AtomicU64::new(0),
            backfill_clean_total: AtomicU64::new(0),
            invalidate_all_total: AtomicU64::new(0),
        }
    }

    // ---------- create staging ----------

    /// Stage a newly created inode + directory entry as `Staging` state.
    ///
    /// Called BEFORE `propose` so the entry is visible to reads immediately,
    /// bridging the commit→apply gap.
    pub fn stage_create(&self, info: InodeInfo, parent_inode: u64, name: &str) {
        let ino = info.inode;
        let ino_bytes = estimate_inode_bytes(&info);
        let dentry_bytes = estimate_dentry_bytes(name);
        {
            let mut tbl = self.inode_table.write().unwrap();
            if let Some(old) = tbl.insert(ino, CachedInode::new(info, CacheState::Staging)) {
                self.trim.release(estimate_inode_bytes(&old.info));
            }
            self.trim.charge(ino_bytes);
        }
        {
            let mut tbl = self.direntry_table.write().unwrap();
            let key = (parent_inode, name.to_string());
            if tbl
                .insert(key, CachedDirEntry::new(ino, CacheState::Staging))
                .is_some()
            {
                // Replacement: approximate released bytes using the same name
                // length (estimate is loose upper bound anyway; small ±error
                // vs bookkeeping perfection is acceptable).
                self.trim.release(dentry_bytes);
            }
            self.trim.charge(dentry_bytes);
        }
        trace!(
            "MetaCache::stage_create: inode={} parent={} name={} bytes_ino={} bytes_de={}",
            ino,
            parent_inode,
            name,
            ino_bytes,
            dentry_bytes
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
        let did_mark = match tbl.get_mut(&inode) {
            Some(existing) => {
                let old_bytes = estimate_inode_bytes(&existing.info);
                updater(&mut existing.info);
                let new_bytes = estimate_inode_bytes(&existing.info);
                if new_bytes >= old_bytes {
                    self.trim.charge(new_bytes - old_bytes);
                } else {
                    self.trim.release(old_bytes - new_bytes);
                }
                existing.state = CacheState::Dirty;
                existing.touch();
                true
            }
            None => {
                if let Some(mut info) = fallback_current {
                    updater(&mut info);
                    let new_bytes = estimate_inode_bytes(&info);
                    let ci = CachedInode::new(info, CacheState::Dirty);
                    ci.touch();
                    tbl.insert(inode, ci);
                    self.trim.charge(new_bytes);
                    true
                } else {
                    false
                }
            }
        };
        if did_mark {
            self.dirty_mark_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Project UpdateInodeSizeChunks into MetaCache (Ceph MDS projected state model).
    ///
    /// Updates size/chunks/inline_data on a cached inode so subsequent
    /// `get_inode` calls return the new values immediately — before Raft
    /// apply completes on ShardStore. This closes the T1.3 visibility gap:
    ///
    ///   1. `create` stages an inode in MetaCache (state=Staging, size=0,
    ///      chunks=[])
    ///   2. `update_inode_size_chunks` proposes Raft log but (in async mode)
    ///      does NOT wait for apply
    ///   3. Cross-client `getattr` → `get_inode` → MetaCache hit → returns
    ///      stale size=0, chunks=[] → reader EIO
    ///
    /// With this method, step 2 also updates MetaCache so step 3 sees the
    /// new size/chunks. This mirrors Ceph MDS's projected state: memory
    /// updates precede journal apply.
    ///
    /// If the inode is not in MetaCache, do nothing — `get_inode` will
    /// fetch from ShardStore after Raft apply. Only Staging/Clean/Dirty
    /// entries are updated; Deleted/Trimming are left alone.
    pub fn project_update_size_chunks(
        &self,
        inode: u64,
        size: u64,
        chunks: Vec<crate::shard_store::StoredFileChunk>,
        inline_data: Option<Vec<u8>>,
        is_append: bool,
    ) {
        let mut tbl = self.inode_table.write().unwrap();
        let Some(existing) = tbl.get_mut(&inode) else {
            return;
        };
        match existing.state {
            CacheState::Deleted | CacheState::Trimming => return,
            CacheState::Staging | CacheState::Clean | CacheState::Dirty => {}
        }
        let old_bytes = estimate_inode_bytes(&existing.info);
        if is_append {
            existing.info.size = std::cmp::max(existing.info.size, size);
        } else {
            existing.info.size = size;
        }
        existing.info.chunks = chunks;
        if let Some(data) = inline_data {
            existing.info.inline_data = Some(data);
        }
        let new_bytes = estimate_inode_bytes(&existing.info);
        if new_bytes >= old_bytes {
            self.trim.charge(new_bytes - old_bytes);
        } else {
            self.trim.release(old_bytes - new_bytes);
        }
        existing.touch();
    }

    // ---------- delete staging ----------

    /// Mark an inode and its directory entry as `Deleted` (pending Raft).
    ///
    /// After this call, reads return ENOENT. The marker is cleared on apply
    /// or by `invalidate_all` on leader change.
    pub fn stage_delete(&self, parent_inode: u64, name: &str, inode: u64) {
        self.stage_delete_total.fetch_add(1, Ordering::Relaxed);
        // 1. inode table → state = Deleted
        {
            let mut tbl = self.inode_table.write().unwrap();
            if let Some(ci) = tbl.get_mut(&inode) {
                ci.state = CacheState::Deleted;
            } else {
                // Seed a Deleted tombstone so later reads hit ENOENT without
                // checking ShardStore. info is unused (Deleted returns None).
                let tomb = InodeInfo::tombstone(inode);
                let tomb_bytes = estimate_inode_bytes(&tomb);
                let ci = CachedInode::new(tomb, CacheState::Deleted);
                ci.refcount.store(0, Ordering::Relaxed);
                tbl.insert(inode, ci);
                self.trim.charge(tomb_bytes);
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
                let de_bytes = estimate_dentry_bytes(name);
                let de = CachedDirEntry::new(inode, CacheState::Deleted);
                de.touch();
                tbl.insert(key, de);
                self.trim.charge(de_bytes);
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
        let ci = match tbl.get(&inode) {
            Some(ci) => ci,
            None => {
                self.inode_miss_total.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        ci.touch();
        match ci.state {
            CacheState::Deleted => {
                self.inode_deleted_served_total
                    .fetch_add(1, Ordering::Relaxed);
                Some(None)
            }
            CacheState::Trimming => {
                // Transient eviction-in-progress; treat as miss so caller
                // goes to ShardStore. A concurrent `cache_put_clean` will
                // overwrite our Trimming entry with a fresh Clean copy;
                // that's safe because the trimmer will remove ours in the
                // same pass (key already marked and won't double-release).
                self.inode_miss_total.fetch_add(1, Ordering::Relaxed);
                None
            }
            _ => {
                self.inode_hit_total.fetch_add(1, Ordering::Relaxed);
                Some(Some(ci.info.clone()))
            }
        }
    }

    /// Populate (or refresh) the cache with a Clean copy just read from
    /// ShardStore. No-ops if the entry exists and is not Clean-compatible
    /// (i.e. it's Staging/Dirty/Deleted and being actively managed).
    pub fn cache_put_clean(&self, info: InodeInfo) {
        let ino = info.inode;
        let new_bytes = estimate_inode_bytes(&info);
        let mut tbl = self.inode_table.write().unwrap();
        match tbl.get(&ino) {
            None => {
                let ci = CachedInode::new(info, CacheState::Clean);
                tbl.insert(ino, ci);
                self.trim.charge(new_bytes);
                self.backfill_clean_total.fetch_add(1, Ordering::Relaxed);
            }
            Some(existing)
                if existing.state == CacheState::Clean
                    || existing.state == CacheState::Trimming =>
            {
                // Replace Clean (refresh) or overwrite Trimming (race where
                // miss-read concurrently backfills during trim_pass).
                let old_bytes = estimate_inode_bytes(&existing.info);
                let ci = CachedInode::new(info, CacheState::Clean);
                tbl.insert(ino, ci);
                if new_bytes >= old_bytes {
                    self.trim.charge(new_bytes - old_bytes);
                } else {
                    self.trim.release(old_bytes - new_bytes);
                }
                self.backfill_clean_total.fetch_add(1, Ordering::Relaxed);
            }
            // Staging/Dirty/Deleted: leave alone; in-flight Raft manages it.
            _ => {}
        }
    }

    /// Try to read a directory entry from the cache.
    ///
    /// Same semantics as [`Self::get_inode`] but for dir entries.
    pub fn get_direntry(&self, parent_inode: u64, name: &str) -> Option<Option<u64>> {
        let key = (parent_inode, name.to_string());
        let tbl = self.direntry_table.read().unwrap();
        let de = match tbl.get(&key) {
            Some(de) => de,
            None => {
                self.direntry_miss_total.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        de.touch();
        match de.state {
            CacheState::Deleted => {
                self.direntry_deleted_served_total
                    .fetch_add(1, Ordering::Relaxed);
                Some(None)
            }
            CacheState::Trimming => {
                self.direntry_miss_total.fetch_add(1, Ordering::Relaxed);
                None
            }
            _ => {
                self.direntry_hit_total.fetch_add(1, Ordering::Relaxed);
                Some(Some(de.child_inode))
            }
        }
    }

    /// Populate a Clean directory entry mapping (read back from ShardStore).
    pub fn cache_put_clean_direntry(&self, parent_inode: u64, name: &str, child_inode: u64) {
        let de_bytes = estimate_dentry_bytes(name);
        let key = (parent_inode, name.to_string());
        let mut tbl = self.direntry_table.write().unwrap();
        match tbl.get(&key) {
            None => {
                let de = CachedDirEntry::new(child_inode, CacheState::Clean);
                tbl.insert(key, de);
                self.trim.charge(de_bytes);
                self.backfill_clean_total.fetch_add(1, Ordering::Relaxed);
            }
            Some(existing)
                if existing.state == CacheState::Clean
                    || existing.state == CacheState::Trimming =>
            {
                let de = CachedDirEntry::new(child_inode, CacheState::Clean);
                tbl.insert(key, de);
                // dentry size estimate is name + constant; we use the same
                // name so old_bytes == new_bytes up to estimator rounding,
                // which is acceptable (no-op charge/delta tiny).
                self.backfill_clean_total.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
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
                drop(tbl);
                self.dirty_confirm_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Raft applied `DeleteInode` → drop inode cache entry entirely.
    pub fn confirm_delete_inode(&self, inode: u64) {
        let mut tbl = self.inode_table.write().unwrap();
        if let Some(old) = tbl.remove(&inode) {
            self.trim.release(estimate_inode_bytes(&old.info));
        }
    }

    /// Raft applied `RemoveDirEntry` → drop dir-entry cache entry entirely.
    pub fn confirm_remove_direntry(&self, parent_inode: u64, name: &str) {
        let key = (parent_inode, name.to_string());
        let mut tbl = self.direntry_table.write().unwrap();
        if tbl.remove(&key).is_some() {
            self.trim.release(estimate_dentry_bytes(name));
        }
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
                if let Some(old) = tbl.remove(&inode) {
                    self.trim.release(estimate_inode_bytes(&old.info));
                }
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
            if should_remove && tbl.remove(&key).is_some() {
                self.trim.release(estimate_dentry_bytes(name));
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
        self.invalidate_all_total.fetch_add(1, Ordering::Relaxed);
        let (inode_count, de_count) = {
            let it = self.inode_table.read().unwrap();
            let dt = self.direntry_table.read().unwrap();
            (it.len(), dt.len())
        };
        self.inode_table.write().unwrap().clear();
        self.direntry_table.write().unwrap().clear();
        // HashMap 全部清掉，usage 直接置 0。（下次 put 会重新 charge，
        // 下一轮 sweep 前有一点误差，无关紧要。）
        self.trim.current_usage_bytes.store(0, Ordering::Relaxed);
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

        let (removed_inodes, ino_bytes_released) = {
            let mut tbl = self.inode_table.write().unwrap();
            let before = tbl.len();
            let mut bytes = 0usize;
            tbl.retain(|ino, ci| {
                if ci.state != CacheState::Deleted {
                    return true;
                }
                let age_ms = now_ms.saturating_sub(ci.last_access_ms.load(Ordering::Relaxed));
                if Duration::from_millis(age_ms) < max_age {
                    return true;
                }
                debug!(
                    "MetaCache sweep: expired Deleted inode tombstone (ino={})",
                    ino
                );
                bytes = bytes.saturating_add(estimate_inode_bytes(&ci.info));
                false
            });
            (before - tbl.len(), bytes)
        };

        let (removed_dirs, de_bytes_released) = {
            let mut tbl = self.direntry_table.write().unwrap();
            let before = tbl.len();
            let mut bytes = 0usize;
            tbl.retain(|(parent, name), de| {
                if de.state != CacheState::Deleted {
                    return true;
                }
                let age_ms = now_ms.saturating_sub(de.last_access_ms.load(Ordering::Relaxed));
                if Duration::from_millis(age_ms) < max_age {
                    return true;
                }
                debug!(
                    "MetaCache sweep: expired Deleted direntry tombstone (parent={}, name={})",
                    parent, name
                );
                bytes = bytes.saturating_add(estimate_dentry_bytes(name));
                false
            });
            (before - tbl.len(), bytes)
        };

        let total_removed = removed_inodes.saturating_add(removed_dirs);
        if total_removed > 0 {
            self.trim
                .deleted_timeout_total
                .fetch_add(total_removed as u64, Ordering::Relaxed);
            self.trim
                .release(ino_bytes_released.saturating_add(de_bytes_released));
            debug!(
                "MetaCache::sweep_expired_deletions: removed {} inode tombstones + {} direntry tombstones (freed ~{} bytes)",
                removed_inodes,
                removed_dirs,
                ino_bytes_released.saturating_add(de_bytes_released)
            );
        }
    }

    // ---------- Phase 2: sweep staging + trim pass ----------

    /// Sweep inode & dentry entries stuck in `Staging` state past
    /// `TrimController::staging_timeout_ms`.
    ///
    /// Symmetric to `sweep_expired_deletions`: if a Raft proposal was made
    /// but the leader changed before apply returned, the Staging copy may
    /// never be confirmed; sweeping it out ensures the next read falls
    /// through to ShardStore (which either has it if the Raft commit
    /// propagated, or doesn't — the client retries).
    pub fn sweep_expired_staging(&self) {
        let now = now_ms();
        let timeout_ms = self.trim.staging_timeout_ms;

        let (removed_inodes, ino_bytes) = {
            let mut tbl = self.inode_table.write().unwrap();
            let before = tbl.len();
            let mut bytes = 0usize;
            tbl.retain(|ino, ci| {
                if ci.state != CacheState::Staging {
                    return true;
                }
                let age = now.saturating_sub(ci.last_access_ms.load(Ordering::Relaxed));
                if age < timeout_ms {
                    return true;
                }
                debug!(
                    "MetaCache sweep: expired Staging inode (ino={} age={}ms)",
                    ino, age
                );
                bytes = bytes.saturating_add(estimate_inode_bytes(&ci.info));
                false
            });
            (before - tbl.len(), bytes)
        };

        let (removed_des, de_bytes) = {
            let mut tbl = self.direntry_table.write().unwrap();
            let before = tbl.len();
            let mut bytes = 0usize;
            tbl.retain(|(_parent, name), de| {
                if de.state != CacheState::Staging {
                    return true;
                }
                let age = now.saturating_sub(de.last_access_ms.load(Ordering::Relaxed));
                if age < timeout_ms {
                    return true;
                }
                bytes = bytes.saturating_add(estimate_dentry_bytes(name));
                false
            });
            (before - tbl.len(), bytes)
        };

        let total = removed_inodes.saturating_add(removed_des);
        if total > 0 {
            self.trim
                .staging_timeout_total
                .fetch_add(total as u64, Ordering::Relaxed);
            self.trim.release(ino_bytes.saturating_add(de_bytes));
            info!(
                "MetaCache::sweep_expired_staging: dropped {} inode staging + {} dentry staging (proposal lost? freed ~{} bytes)",
                removed_inodes,
                removed_des,
                ino_bytes.saturating_add(de_bytes)
            );
        }
    }

    /// Single-pass trim: if memory usage is above `high_watermark`, evict
    /// Clean, refcount == 0 inodes & dentries in LRU order until usage
    /// falls under `low_watermark`.
    ///
    /// Phase 2 implementation: per-entry LRU (not DirFrag). See Phase 4
    /// section of the design doc for the follow-up DirFrag-granular
    /// upgrade path.
    ///
    /// Phase 3: after evicting all refcount==0 candidates, if still
    /// over high_watermark, collects Clean entries with refcount > 0
    /// as `recall_candidates` so the caller can push Invalidate
    /// notifications to the holding clients. Once clients release their
    /// leases, refcount drops to 0 and the next trim_pass can evict.
    pub fn trim_pass(&self) -> TrimResult {
        if !self.trim.over_high() {
            return TrimResult::default();
        }
        let low = self.trim.low_bytes();
        // ----- Step 1: collect Clean + refcount == 0 candidates, sorted by LRU -----
        //
        // We hold the write lock for the whole scan + remove phase. Inode
        // table & direntry table are both under `RwLock<HashMap<...>>`;
        // typical trim scans at ~1M entries/sec on modern CPUs, which is
        // acceptable for a background thread that only runs after memory
        // is already above 80 % of a 2 GiB ceiling.
        let mut evicted_total = 0usize;
        let mut bytes_freed_total = 0usize;

        // Inodes: iterate, mark candidates = Trimming, collect (inode,
        // last_access, est_bytes). `retain`-after-scan would miss bytes
        // estimate, so do two passes.
        let ino_candidates: Vec<(u64, u64, usize)> = {
            let tbl = self.inode_table.read().unwrap();
            let mut v = Vec::with_capacity(tbl.len().min(4096 * 16));
            for (&ino, ci) in tbl.iter() {
                if ci.state != CacheState::Clean {
                    continue;
                }
                if ci.refcount.load(Ordering::Relaxed) != 0 {
                    continue;
                }
                v.push((
                    ino,
                    ci.last_access_ms.load(Ordering::Relaxed),
                    estimate_inode_bytes(&ci.info),
                ));
            }
            v.sort_unstable_by_key(|(_, ts, _)| *ts); // oldest first (LRU)
            v
        };
        {
            let mut tbl = self.inode_table.write().unwrap();
            for (ino, _ts, est_bytes) in ino_candidates.iter().copied() {
                if self
                    .trim
                    .current_usage_bytes
                    .load(Ordering::Relaxed)
                    .saturating_sub(bytes_freed_total)
                    <= low
                {
                    break;
                }
                // Double-check state under write lock (could have become
                // Dirty/Staging between the read snapshot and now).
                let should_remove = tbl
                    .get(&ino)
                    .map(|ci| {
                        ci.state == CacheState::Clean && ci.refcount.load(Ordering::Relaxed) == 0
                    })
                    .unwrap_or(false);
                if !should_remove {
                    continue;
                }
                // Atomically mark Trimming then drop. (Marking is optional
                // here because we're already under write lock; but keeping
                // the state transition explicit makes the semantics
                // consistent with concurrent-reads-outside-lock reasoning.)
                if let Some(entry) = tbl.get_mut(&ino) {
                    entry.state = CacheState::Trimming;
                }
                if let Some(old) = tbl.remove(&ino) {
                    bytes_freed_total = bytes_freed_total.saturating_add(est_bytes);
                    evicted_total += 1;
                    let _ = old; // bytes already accounted via est_bytes above
                }
            }
        }

        // Dentries: same pattern. We don't check a linked inode's refcount
        // because dentries can outlive or predate their cached inode;
        // the pair gets trimmed independently (reads handle miss → RocksDB
        // backfill correctly either way).
        let de_candidates: Vec<((u64, String), u64, usize)> = {
            let tbl = self.direntry_table.read().unwrap();
            let mut v = Vec::with_capacity(tbl.len().min(4096 * 16));
            for (key, de) in tbl.iter() {
                if de.state != CacheState::Clean {
                    continue;
                }
                v.push((
                    key.clone(),
                    de.last_access_ms.load(Ordering::Relaxed),
                    estimate_dentry_bytes(&key.1),
                ));
            }
            v.sort_unstable_by_key(|(_, ts, _)| *ts);
            v
        };
        {
            let mut tbl = self.direntry_table.write().unwrap();
            for (key, _ts, est_bytes) in de_candidates.into_iter() {
                if self
                    .trim
                    .current_usage_bytes
                    .load(Ordering::Relaxed)
                    .saturating_sub(bytes_freed_total)
                    <= low
                {
                    break;
                }
                let should_remove = tbl
                    .get(&key)
                    .map(|de| de.state == CacheState::Clean)
                    .unwrap_or(false);
                if !should_remove {
                    continue;
                }
                if tbl.remove(&key).is_some() {
                    bytes_freed_total = bytes_freed_total.saturating_add(est_bytes);
                    evicted_total += 1;
                }
            }
        }

        if evicted_total > 0 {
            self.trim.release(bytes_freed_total);
            self.trim
                .trim_total
                .fetch_add(evicted_total as u64, Ordering::Relaxed);
            info!(
                "MetaCache::trim_pass: evicted {} entries (~{} bytes freed; usage now ~{} bytes, low watermark ~{} bytes)",
                evicted_total,
                bytes_freed_total,
                self.trim.current_usage_bytes.load(Ordering::Relaxed),
                low
            );
        }

        // ----- Phase 3: collect recall candidates if still over high -----
        //
        // If we evicted everything we could but usage is still above
        // high_watermark, the remaining Clean entries with refcount > 0
        // are blocking further eviction. Collect them (up to a batch
        // limit) so the caller can push Invalidate to their lease
        // holders; after clients release, the next trim_pass will
        // actually evict them.
        let mut recall_candidates = Vec::new();
        if self.trim.over_high() {
            let now = now_ms();
            let cooldown_ms = 5_000u64; // 5 s recall cooldown
            let batch_limit = 64usize; // max recalls per trim_pass

            // Sweep expired cooldown entries (client had enough time).
            {
                let mut cd = self.recall_cooldown.write().unwrap();
                cd.retain(|_, ts| now.saturating_sub(*ts) < cooldown_ms * 2);
            }

            let tbl = self.inode_table.read().unwrap();
            let cd = self.recall_cooldown.read().unwrap();
            for (&ino, ci) in tbl.iter() {
                if recall_candidates.len() >= batch_limit {
                    break;
                }
                if ci.state != CacheState::Clean {
                    continue;
                }
                let rc = ci.refcount.load(Ordering::Relaxed);
                if rc == 0 {
                    continue;
                }
                // Skip if still in cooldown (client hasn't had time to
                // release since last recall).
                if let Some(&last_ts) = cd.get(&ino) {
                    if now.saturating_sub(last_ts) < cooldown_ms {
                        self.trim
                            .recall_cooldown_skips
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                }
                recall_candidates.push(ino);
            }
        }

        TrimResult {
            evicted: evicted_total,
            recall_candidates,
        }
    }

    // ---------- Phase 3: refcount + recall ----------

    /// Increment the lease refcount on a cached inode.
    ///
    /// Called by MetaShardManager when a client acquires / renews an
    /// inode lease. trim_pass will NOT evict entries with refcount > 0.
    #[inline]
    pub fn incr_refcount(&self, inode: u64) {
        let tbl = self.inode_table.read().unwrap();
        if let Some(ci) = tbl.get(&inode) {
            ci.refcount.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Decrement the lease refcount on a cached inode.
    ///
    /// Called by MetaShardManager when a client releases / loses (TTL
    /// expiry, disconnect) an inode lease. Saturating to 0.
    #[inline]
    pub fn decr_refcount(&self, inode: u64) {
        let tbl = self.inode_table.read().unwrap();
        if let Some(ci) = tbl.get(&inode) {
            let _ = ci
                .refcount
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                });
        }
    }

    /// Mark an inode as "recalled" — records the current timestamp in
    /// the cooldown map so subsequent trim_passes skip it for 5 s.
    ///
    /// Called by MetaShardManager after pushing an Invalidate to the
    /// client holding the lease.
    pub fn mark_recalled(&self, inode: u64) {
        let mut cd = self.recall_cooldown.write().unwrap();
        cd.insert(inode, now_ms());
        self.trim.recall_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Attach a cap record to a cached inode  `CInode::add_client_cap`).
    ///
    /// Called by `CapManager::open_grant` to mirror a cap grant into the
    /// inode-embedded `client_caps` map. If the inode is not currently
    /// cached (e.g. it was trimmed), the attach is silently dropped —
    /// the `CapManager::inodes` HashMap remains the authority for the
    /// cap state machine, and the inode-embedded copy is only used for
    /// serialization boundaries and conflict fast-paths.
    pub fn cap_attach(
        &self,
        inode: u64,
        client_id: &str,
        cap_id: u64,
        caps: CapSet,
        epoch: u64,
        is_writer: bool,
    ) {
        let tbl = self.inode_table.read().unwrap();
        if let Some(ci) = tbl.get(&inode) {
            ci.add_client_cap(client_id, cap_id, caps, epoch, is_writer);
        }
    }

    /// Detach a client's cap from a cached inode
    /// `CInode::remove_client_cap`).
    ///
    /// Called by `CapManager::release_cap` / `close_session` /
    /// `drain_expired_recalls` to keep the inode-embedded `client_caps`
    /// map in sync with the `CapManager` state.
    pub fn cap_detach(&self, inode: u64, client_id: &str) {
        let tbl = self.inode_table.read().unwrap();
        if let Some(ci) = tbl.get(&inode) {
            ci.remove_client_cap(client_id);
        }
    }

    /// Fix refcount leaks: for every Clean inode with refcount > 0,
    /// the caller provides a `is_lease_active` predicate. If the
    /// predicate returns false (no active lease in InodeLeaseManager),
    /// the refcount is force-reset to 0 and the leak counter bumped.
    ///
    /// Called periodically from the GC loop to recover from edge cases
    /// where a lease was expired/revoked but `decr_refcount` was missed
    /// (e.g. client crash before sending ReleaseInodeLease).
    pub fn sweep_leaked_refcounts<F>(&self, is_lease_active: F)
    where
        F: Fn(u64) -> bool,
    {
        let mut fixed = 0u64;
        let tbl = self.inode_table.read().unwrap();
        for (&ino, ci) in tbl.iter() {
            if ci.state != CacheState::Clean {
                continue;
            }
            if ci.refcount.load(Ordering::Relaxed) == 0 {
                continue;
            }
            if !is_lease_active(ino) {
                ci.refcount.store(0, Ordering::Relaxed);
                fixed += 1;
            }
        }
        if fixed > 0 {
            self.trim
                .refcount_leak_fixes
                .fetch_add(fixed, Ordering::Relaxed);
            warn!(
                "MetaCache::sweep_leaked_refcounts: fixed {} leaked refcounts",
                fixed
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

    /// Directory entry state breakdown (mirrors `state_counts` for dentries).
    pub fn direntry_state_counts(&self) -> HashMap<CacheState, usize> {
        let mut m = HashMap::new();
        for de in self.direntry_table.read().unwrap().values() {
            *m.entry(de.state).or_insert(0) += 1;
        }
        m
    }

    /// Consistent snapshot of all cumulative counters + current state counts.
    ///
    /// Used by `/metrics` scrape and the `/admin/meta-cache-stats` JSON
    /// endpoint. We read atomics with `Relaxed` ordering because Prometheus
    /// counters are monotonic best-effort (no cross-counter coherence
    /// required between scrape boundaries).
    pub fn stats(&self) -> MetaCacheStats {
        use Ordering::Relaxed;
        let state_counts = self.state_counts();
        let de_counts = self.direntry_state_counts();
        MetaCacheStats {
            inode_hit_total: self.inode_hit_total.load(Relaxed),
            inode_miss_total: self.inode_miss_total.load(Relaxed),
            inode_deleted_served_total: self.inode_deleted_served_total.load(Relaxed),
            direntry_hit_total: self.direntry_hit_total.load(Relaxed),
            direntry_miss_total: self.direntry_miss_total.load(Relaxed),
            direntry_deleted_served_total: self.direntry_deleted_served_total.load(Relaxed),
            dirty_mark_total: self.dirty_mark_total.load(Relaxed),
            dirty_confirm_total: self.dirty_confirm_total.load(Relaxed),
            stage_delete_total: self.stage_delete_total.load(Relaxed),
            backfill_clean_total: self.backfill_clean_total.load(Relaxed),
            invalidate_all_total: self.invalidate_all_total.load(Relaxed),
            trim_total: self.trim.trim_total.load(Relaxed),
            staging_timeout_total: self.trim.staging_timeout_total.load(Relaxed),
            deleted_timeout_total: self.trim.deleted_timeout_total.load(Relaxed),
            recall_total: self.trim.recall_total.load(Relaxed),
            recall_cooldown_skips: self.trim.recall_cooldown_skips.load(Relaxed),
            refcount_leak_fixes: self.trim.refcount_leak_fixes.load(Relaxed),
            memory_usage_bytes: self.trim.current_usage_bytes.load(Relaxed) as u64,
            memory_limit_bytes: self.trim.memory_limit_bytes as u64,
            memory_high_watermark_bytes: self.trim.high_bytes() as u64,
            memory_low_watermark_bytes: self.trim.low_bytes() as u64,
            inode_clean_count: *state_counts.get(&CacheState::Clean).unwrap_or(&0),
            inode_staging_count: *state_counts.get(&CacheState::Staging).unwrap_or(&0),
            inode_dirty_count: *state_counts.get(&CacheState::Dirty).unwrap_or(&0),
            inode_deleted_count: *state_counts.get(&CacheState::Deleted).unwrap_or(&0),
            inode_trimming_count: *state_counts.get(&CacheState::Trimming).unwrap_or(&0),
            direntry_clean_count: *de_counts.get(&CacheState::Clean).unwrap_or(&0),
            direntry_staging_count: *de_counts.get(&CacheState::Staging).unwrap_or(&0),
            direntry_dirty_count: *de_counts.get(&CacheState::Dirty).unwrap_or(&0),
            direntry_deleted_count: *de_counts.get(&CacheState::Deleted).unwrap_or(&0),
            direntry_trimming_count: *de_counts.get(&CacheState::Trimming).unwrap_or(&0),
        }
    }
}

/// Snapshot returned by [`MetaCache::stats`] — consumed by Prometheus
/// refresh + JSON admin handler. All counts are `u64` (same width as the
/// backing Atomics) so JSON serializers / Prometheus i64 gauges keep fit.
#[derive(Debug, Clone)]
pub struct MetaCacheStats {
    pub inode_hit_total: u64,
    pub inode_miss_total: u64,
    pub inode_deleted_served_total: u64,
    pub direntry_hit_total: u64,
    pub direntry_miss_total: u64,
    pub direntry_deleted_served_total: u64,
    pub dirty_mark_total: u64,
    pub dirty_confirm_total: u64,
    pub stage_delete_total: u64,
    pub backfill_clean_total: u64,
    pub invalidate_all_total: u64,
    pub trim_total: u64,
    pub staging_timeout_total: u64,
    pub deleted_timeout_total: u64,
    /// Phase 3: cumulative lease-recall notifications pushed.
    pub recall_total: u64,
    /// Phase 3: recalls skipped due to cooldown.
    pub recall_cooldown_skips: u64,
    /// Phase 3: refcount leaks fixed by sweep_leaked_refcounts.
    pub refcount_leak_fixes: u64,
    pub memory_usage_bytes: u64,
    pub memory_limit_bytes: u64,
    pub memory_high_watermark_bytes: u64,
    pub memory_low_watermark_bytes: u64,
    pub inode_clean_count: usize,
    pub inode_staging_count: usize,
    pub inode_dirty_count: usize,
    pub inode_deleted_count: usize,
    pub inode_trimming_count: usize,
    pub direntry_clean_count: usize,
    pub direntry_staging_count: usize,
    pub direntry_dirty_count: usize,
    pub direntry_deleted_count: usize,
    pub direntry_trimming_count: usize,
}

/// Result of a single `trim_pass()` invocation.
///
/// Phase 3 extends the plain `evicted: usize` with `recall_candidates`:
/// inodes that are Clean but pinned by refcount > 0, preventing
/// eviction. The GC loop pushes Invalidate to their lease holders so
/// they release; the next trim_pass can then evict them.
#[derive(Debug, Default)]
pub struct TrimResult {
    /// Number of entries actually evicted this pass.
    pub evicted: usize,
    /// Inodes that need lease recall (Clean + refcount > 0). The
    /// caller should look up lease holders and push Invalidate.
    pub recall_candidates: Vec<u64>,
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

// ========================================================================
// Phase 3 Lease Recall tests
// ========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode_lease_manager::InodeLeaseManager;
    use crate::shard_store::InodeInfo;
    use std::sync::Arc;

    /// Build a MetaCache with a tiny memory limit so tests can trigger
    /// trim_pass without inserting thousands of entries.
    ///
    /// Each cached inode is charged at least 512 bytes (see
    /// `estimate_inode_bytes` → `.max(512)`), so a 2 KiB limit overflows
    /// after ~4 entries.
    fn make_mc_with_limit(limit_bytes: usize) -> MetaCache {
        let mut mc = MetaCache::new();
        mc.trim.memory_limit_bytes = limit_bytes;
        mc
    }

    /// Insert a minimal Clean inode into the cache at the given inode number.
    fn insert_clean(mc: &MetaCache, inode: u64) {
        mc.cache_put_clean(InodeInfo::tombstone(inode));
    }

    // ---- T1: trim_pass evicts refcount==0 first, collects pinned as recall ----

    #[test]
    fn test_trim_evicts_zero_refcount_and_collects_recall_candidates() {
        // 5 pinned (refcount > 0) + 5 evictable (refcount == 0) = 10 entries
        // Each ~512 B → ~5120 B total.  Limit 2048, high 0.8*2048=1638,
        // low 0.6*2048=1228.  After evicting all 5 evictable entries
        // (freeing 2560 B → usage=2560), still > high (1638) → collect
        // the 5 pinned as recall_candidates.
        let mc = make_mc_with_limit(2048);

        // Insert 5 evictable + 5 pinned inodes
        let evictable: Vec<u64> = (100..105).collect();
        let pinned: Vec<u64> = (200..205).collect();
        for &ino in evictable.iter().chain(pinned.iter()) {
            insert_clean(&mc, ino);
        }
        // Bump refcount on pinned inodes (simulates active lease)
        for &ino in &pinned {
            mc.incr_refcount(ino);
        }

        let usage_before = mc.trim.current_usage_bytes.load(Ordering::Relaxed);
        assert!(
            usage_before > mc.trim.high_bytes(),
            "usage {} should exceed high_bytes {}",
            usage_before,
            mc.trim.high_bytes()
        );

        let result = mc.trim_pass();

        // All 5 evictable entries should have been evicted
        assert_eq!(result.evicted, 5, "should evict all 5 refcount==0 entries");

        // The 5 pinned entries should be recall_candidates
        assert_eq!(
            result.recall_candidates.len(),
            5,
            "should collect 5 recall candidates"
        );
        for &ino in &pinned {
            assert!(
                result.recall_candidates.contains(&ino),
                "pinned inode {} should be in recall_candidates",
                ino
            );
        }

        // Pinned inodes should still be in the cache
        for &ino in &pinned {
            assert!(
                mc.get_inode(ino).is_some(),
                "pinned inode {} should still be cached",
                ino
            );
        }
        // Evicted inodes should be gone
        for &ino in &evictable {
            assert!(
                mc.get_inode(ino).is_none(),
                "evictable inode {} should have been evicted",
                ino
            );
        }
    }

    // ---- T2: recall_cooldown skips recently recalled inodes ----

    #[test]
    fn test_recall_cooldown_skips_recently_recalled() {
        let mc = make_mc_with_limit(1024);

        // Insert 3 pinned inodes (3 * 512 = 1536 > high=819)
        let pinned: Vec<u64> = (300..303).collect();
        for &ino in &pinned {
            insert_clean(&mc, ino);
            mc.incr_refcount(ino);
        }

        // First trim_pass: should collect all 3 as recall_candidates
        let r1 = mc.trim_pass();
        assert_eq!(r1.recall_candidates.len(), 3, "first pass collects all 3");

        // Mark them as recalled (simulates GC pushing Invalidate)
        for &ino in &r1.recall_candidates {
            mc.mark_recalled(ino);
        }

        let cooldown_skips_before = mc.trim.recall_cooldown_skips.load(Ordering::Relaxed);

        // Second trim_pass immediately: should skip all 3 (in cooldown)
        let r2 = mc.trim_pass();
        assert_eq!(
            r2.recall_candidates.len(),
            0,
            "second pass should skip all (in cooldown)"
        );

        let cooldown_skips_after = mc.trim.recall_cooldown_skips.load(Ordering::Relaxed);
        assert_eq!(
            cooldown_skips_after - cooldown_skips_before,
            3,
            "should record 3 cooldown skips"
        );
    }

    // ---- T3: releasing lease (decr_refcount) enables eviction ----

    #[test]
    fn test_decr_refcount_enables_eviction() {
        let mc = make_mc_with_limit(1024);

        // Insert 3 pinned inodes
        let pinned: Vec<u64> = (400..403).collect();
        for &ino in &pinned {
            insert_clean(&mc, ino);
            mc.incr_refcount(ino);
        }

        // trim_pass: all 3 are recall_candidates, none evicted
        let r1 = mc.trim_pass();
        assert_eq!(r1.evicted, 0, "nothing evicted while pinned");
        assert_eq!(r1.recall_candidates.len(), 3, "all 3 are recall candidates");

        // Simulate client releasing lease on inode 400
        mc.decr_refcount(400);

        // Next trim_pass: inode 400 should now be evictable
        let r2 = mc.trim_pass();
        assert!(
            r2.evicted >= 1,
            "inode 400 should be evicted after refcount drop (got {})",
            r2.evicted
        );
        assert!(
            mc.get_inode(400).is_none(),
            "inode 400 should have been evicted"
        );
        // Remaining 2 should still be cached
        assert!(mc.get_inode(401).is_some(), "inode 401 still pinned");
        assert!(mc.get_inode(402).is_some(), "inode 402 still pinned");
    }

    // ---- T4: sweep_leaked_refcounts fixes stale refcounts ----

    #[test]
    fn test_sweep_leaked_refcounts() {
        let mut mc = make_mc_with_limit(4096); // large enough that trim won't fire

        // Insert 2 inodes with leaked refcounts (refcount > 0 but no active lease)
        insert_clean(&mc, 500);
        insert_clean(&mc, 501);
        mc.incr_refcount(500);
        mc.incr_refcount(501);

        let leaks_before = mc.trim.refcount_leak_fixes.load(Ordering::Relaxed);

        // sweep: predicate says NO active lease for both → reset to 0
        mc.sweep_leaked_refcounts(|_inode| false);

        let leaks_after = mc.trim.refcount_leak_fixes.load(Ordering::Relaxed);
        assert_eq!(
            leaks_after - leaks_before,
            2,
            "should fix 2 leaked refcounts"
        );

        // Now with refcount == 0, trim should be able to evict them
        // (if memory were over high watermark)
        mc.trim.memory_limit_bytes = 512; // shrink to force trim
        let r = mc.trim_pass();
        assert!(r.evicted >= 2, "both should be evicted after leak fix");
    }

    // ---- T5: batch_limit caps recall_candidates at 64 ----

    #[test]
    fn test_recall_batch_limit_64() {
        let mc = make_mc_with_limit(1024);

        // Insert 100 pinned inodes (far exceeding batch_limit=64)
        for ino in 600..700u64 {
            insert_clean(&mc, ino);
            mc.incr_refcount(ino);
        }

        let r = mc.trim_pass();
        assert!(
            r.recall_candidates.len() <= 64,
            "recall_candidates should be capped at 64, got {}",
            r.recall_candidates.len()
        );
        assert!(
            !r.recall_candidates.is_empty(),
            "should collect at least some recall candidates"
        );
    }

    // ---- T6: no recall when under high watermark ----

    #[test]
    fn test_no_recall_under_high_watermark() {
        let mc = make_mc_with_limit(4096); // high = 3276

        // Insert 2 pinned inodes (2 * 512 = 1024 < 3276)
        for ino in 700..702u64 {
            insert_clean(&mc, ino);
            mc.incr_refcount(ino);
        }

        let r = mc.trim_pass();
        assert_eq!(r.evicted, 0, "nothing to evict (under high watermark)");
        assert_eq!(
            r.recall_candidates.len(),
            0,
            "no recall candidates (under high watermark)"
        );
    }

    // ---- T7: full integration with InodeLeaseManager ----

    #[test]
    fn test_lease_manager_recall_integration() {
        // limit=512 → high=409, low=307.  3 inodes × 512 B = 1536 > 409.
        // After release, all 3 have refcount=0; trim evicts until ≤ 307:
        // 1536→1024→512→0, all 3 evicted.
        let mc = Arc::new(make_mc_with_limit(512));
        let lease_mgr = InodeLeaseManager::new().with_meta_cache(mc.clone());

        let client_id = "test-client-1";
        let inodes: Vec<u64> = (800..803).collect();

        // 1) Insert Clean inodes into cache
        for &ino in &inodes {
            insert_clean(&mc, ino);
        }

        // 2) Acquire leases → refcount should bump
        let mut tokens = Vec::new();
        for &ino in &inodes {
            let res = lease_mgr.acquire(ino, client_id, 60_000).unwrap();
            tokens.push((ino, res.token));
        }

        // Verify refcount > 0 on all leased inodes
        for &ino in &inodes {
            let tbl = mc.inode_table.read().unwrap();
            let rc = tbl.get(&ino).map(|c| c.refcount.load(Ordering::Relaxed));
            assert_eq!(
                rc,
                Some(1),
                "inode {} refcount should be 1 after acquire",
                ino
            );
        }

        // 3) trim_pass → should collect recall_candidates
        let r1 = mc.trim_pass();
        assert_eq!(r1.evicted, 0, "nothing evicted (all pinned by leases)");
        assert_eq!(
            r1.recall_candidates.len(),
            3,
            "all 3 leased inodes should be recall candidates"
        );

        // 4) Verify get_holder returns the correct client for each recall candidate
        for &ino in &r1.recall_candidates {
            let holder = lease_mgr.get_holder(ino);
            assert_eq!(
                holder.as_deref(),
                Some(client_id),
                "get_holder should return the lease holder for inode {}",
                ino
            );
        }

        // 5) Mark recalled (simulates GC pushing Invalidate)
        for &ino in &r1.recall_candidates {
            mc.mark_recalled(ino);
        }

        // 6) Simulate client releasing leases after receiving Invalidate
        for (ino, token) in &tokens {
            lease_mgr.release(*ino, client_id, token).unwrap();
        }

        // Verify refcount == 0 after release
        for &ino in &inodes {
            let tbl = mc.inode_table.read().unwrap();
            let rc = tbl.get(&ino).map(|c| c.refcount.load(Ordering::Relaxed));
            assert_eq!(
                rc,
                Some(0),
                "inode {} refcount should be 0 after release",
                ino
            );
        }

        // 7) Wait for cooldown to expire (5s), then trim_pass should evict
        std::thread::sleep(std::time::Duration::from_millis(5_100));

        let r2 = mc.trim_pass();
        assert!(
            r2.evicted >= 3,
            "all 3 inodes should be evicted after lease release + cooldown (got {})",
            r2.evicted
        );
        for &ino in &inodes {
            assert!(
                mc.get_inode(ino).is_none(),
                "inode {} should have been evicted",
                ino
            );
        }

        // 8) Verify metrics
        let stats = mc.stats();
        assert!(
            stats.recall_total >= 3,
            "recall_total should be >= 3 (got {})",
            stats.recall_total
        );
    }

    // ---- T8: sweep_leaked_refcounts with active lease is NOT reset ----

    #[test]
    fn test_sweep_leaked_refcounts_preserves_active_lease() {
        let mc = Arc::new(make_mc_with_limit(4096));
        let lease_mgr = InodeLeaseManager::new().with_meta_cache(mc.clone());

        insert_clean(&mc, 900);
        // Acquire a real lease → refcount bumps to 1
        lease_mgr.acquire(900, "active-client", 60_000).unwrap();

        let leaks_before = mc.trim.refcount_leak_fixes.load(Ordering::Relaxed);

        // sweep: predicate delegates to lease_mgr.get_holder which returns
        // Some("active-client") → lease IS active → refcount preserved
        mc.sweep_leaked_refcounts(|ino| lease_mgr.get_holder(ino).is_some());

        let leaks_after = mc.trim.refcount_leak_fixes.load(Ordering::Relaxed);
        assert_eq!(
            leaks_after - leaks_before,
            0,
            "should NOT fix refcount when lease is active"
        );

        // Verify refcount still 1
        let tbl = mc.inode_table.read().unwrap();
        let rc = tbl.get(&900).map(|c| c.refcount.load(Ordering::Relaxed));
        assert_eq!(rc, Some(1), "refcount should still be 1");
    }
}
