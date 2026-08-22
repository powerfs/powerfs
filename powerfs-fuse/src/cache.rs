use bytes::Bytes;
use log::{debug, warn};
use lru::LruCache;
use powerfs_common::types::Fid;
use powerfs_master::proto::FileChunk;
use powerfs_orset::CachedFileChunk;
use std::collections::HashMap;
use std::num::NonZero;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

pub const ROOT_INODE: u64 = 1;
pub const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024; // 1MB - unified with stripe_size for Stripe/WideStripe

pub fn chunk_from_proto(chunk: FileChunk) -> CachedFileChunk {
    CachedFileChunk {
        offset: chunk.offset,
        size: chunk.size,
        mtime: chunk.mtime,
        needle_id: chunk.needle_id,
        volume_id: chunk.volume_id,
        crc32: chunk.crc32,
    }
}

/// Cache entry lifecycle state (see docs/cache-entry-state-machine-design.md).
///
/// Phase 1: introduced as observational field; logic still uses existing
/// `cached_at`/`pinned_inodes` mechanisms. Phase 2+ will make this the
/// authoritative state used by `try_transition()` guards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EntryState {
    /// Just allocated, attributes not yet populated.
    #[default]
    New,
    /// Attributes populated and consistent with Filer, no dirty data.
    Clean,
    /// Has unsynced local modifications (dirty chunks / pending setattr).
    Dirty,
    /// Currently syncing to Filer/Volume Server.
    Flushing,
    /// TTL expired or Invalidate received; needs refresh on next access.
    Stale,
    /// Deleted (unlink/rmdir), pending removal from cache.
    Tombstone,
}

/// Orthogonal hold state: whether the inode is open (lease-held).
///
/// Independent of `EntryState` — a Dirty+Pinned entry is the common
/// "open file with unsynced writes" case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HoldState {
    /// No open handle, no lease. TTL applies, LRU may evict.
    #[default]
    Unpinned,
    /// At least one open handle holds a data lease. TTL bypassed,
    /// cannot evict. Reference-counted for concurrent open/release.
    Pinned { open_count: u32 },
}

impl HoldState {
    pub fn is_pinned(&self) -> bool {
        matches!(self, HoldState::Pinned { .. })
    }
}

#[derive(Debug, Clone)]
pub struct CachedEntry {
    pub inode: u64,
    pub parent: u64,
    pub name: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    pub symlink_target: Option<String>,
    pub nlink: u32,
    pub fid: Option<Fid>,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: i64,
    pub mtime: i64,
    pub ctime: i64,
    pub xattrs: HashMap<String, Vec<u8>>,
    pub chunks: Vec<CachedFileChunk>,
    pub hard_link_id: String,
    pub hard_link_counter: u32,
    pub content_size: u64,
    pub disk_size: u64,
    pub generation: u64,
    /// P3: Placement strategy (Stripe/WideStripe). None = Flat/Inline (use fid/chunks).
    /// When Some(Stripe), write/read handlers use Placement::locate() to route I/O
    /// to the correct volume based on file offset.
    pub placement: Option<powerfs_layout::Placement>,
    /// P6: 可靠性策略 (SingleReplica/Replicated/EC).
    /// EC 模式下, chunks 前 data 个为数据 shard, 后 parity 个为校验 shard.
    /// 读路径: 先读 data shard, 失败时读 parity + 剩余 data 重建.
    pub reliability: powerfs_layout::reliability::Reliability,
    /// P4: 副本 chunk 列表 (scrubber 异步复制).
    /// 读路径 failover: 主 volume 不可用时从副本 volume 读取相同 needle_id.
    pub replica_chunks: Vec<CachedFileChunk>,
    /// 方案 B (S5): Filer 返回的权威 shard_id (来自 MetadataAttr.shard_id).
    /// 操作时优先用缓存值, 免去 ShardMap::route(inode) 计算;
    /// None 时回退到 calculate_shard_id(inode).
    pub shard_id: Option<u64>,
    /// When this cache entry was populated (used for TTL fallback)
    pub cached_at: Instant,
    /// Lifecycle state (Phase 1: observational; Phase 2+: authoritative).
    pub state: EntryState,
    /// Hold state (open/lease reference count). Orthogonal to `state`.
    pub hold: HoldState,
    /// §13 Cap model: per-inode cap record.
    ///
    /// Embedded directly in the cache entry. When `Some`, this client
    /// holds a capability grant from the Filer leader for this inode;
    /// the `issued` bits determine whether local cache reads/writes
    /// are authoritative (CAP_R/CAP_W) and whether setattr is allowed
    /// (CAP_X).
    ///
    /// `dirty_caps` records which cap bits have unsynced local state
    /// (write data, setattr metadata) — checked by `process_recall` to
    /// decide between ImmediateAck and FlushThenAck on server recall.
    ///
    /// Cleared on `release()` (last close) via `take_cap`.
    pub cap: Option<crate::client_cap::ClientCap>,

    /// Dentry-level lease (per-name, like Ceph's Dentry::lease_ttl).
    ///
    /// When `Some` and `expire_at > now`, this dentry is authoritative:
    ///   - Positive dentry (inode != 0): cached attr is valid
    ///   - Negative dentry (inode == 0): file truly doesn't exist
    ///
    /// Granted by the Filer in lookup/readdir responses. Invalidated when
    /// the parent directory's version changes (detected via `dir_version`
    /// mismatch or explicit Invalidate notification).
    pub dentry_lease: Option<DentryLease>,

    /// The parent directory's version (shared_gen) when this dentry was
    /// last validated. Compared against the cached `dir_version` to detect
    /// stale dentries after lease expiry (Ceph's cap_shared_gen mechanism).
    pub dir_shared_gen: u64,
}

/// Per-dentry lease metadata, granted by the Filer.
///
/// Modeled after Ceph's Dentry lease (Dentry.h: lease_mds, lease_ttl,
/// lease_seq, lease_gen). The Filer issues a lease TTL in lookup/readdir
/// responses; the client stores it here and trusts the dentry (positive
/// or negative) until `expire_at`.
#[derive(Debug, Clone)]
pub struct DentryLease {
    /// When this lease expires (client-local clock).
    pub expire_at: Instant,
    /// Filer-issued lease duration (ms), for refresh accounting.
    pub duration_ms: u64,
    /// Filer node id that issued the lease (for future lease migration).
    pub issuer: u64,
}

/// Result of checking a dentry's lease status (three-layer, like Ceph).
///
/// Layer 1: per-dentry lease (DentryLease::expire_at)
/// Layer 2: dir shared_gen match + dir_complete (I_COMPLETE)
/// Layer 3: RPC to Filer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DentryLeaseStatus {
    /// Dentry lease is valid → trust cache (positive or negative).
    LeaseValid,
    /// Lease expired but shared_gen matches + dir is complete → trust cache.
    SharedGenValid,
    /// No entry in cache, but dir is complete → ENOENT (negative complete).
    NegativeComplete,
    /// Lease expired or no lease, shared_gen mismatch → must RPC.
    Expired,
    /// No cached entry, dir not complete → must RPC.
    Miss,
}

impl CachedEntry {
    /// Attempt a state transition. Returns `false` if the transition is not
    /// allowed by the state machine (see design doc §2.5 transition matrix).
    ///
    /// Phase 1: only logs the transition; existing logic still uses
    /// `cached_at`/`pinned_inodes`. Phase 2+ will enforce the guard.
    #[allow(dead_code)]
    pub fn try_transition(&mut self, target: EntryState) -> bool {
        use EntryState::*;
        let allowed = match (self.state, target) {
            (New, Clean) | (New, Dirty) | (New, Stale) | (New, Tombstone) => true,
            (Clean, Dirty) | (Clean, Stale) | (Clean, Tombstone) => true,
            (Dirty, Flushing) | (Dirty, Tombstone) => true,
            (Flushing, Clean) | (Flushing, Dirty) => true,
            (Stale, Clean) | (Stale, Dirty) | (Stale, Tombstone) => true,
            // Dirty/Flushing -> Stale: explicitly forbidden (core rule:
            // local authoritative data must not be invalidated).
            (Dirty, Stale) | (Flushing, Stale) => false,
            // Same-state transition allowed (refresh cached_at etc.)
            (s, t) if s == t => true,
            _ => false,
        };
        if allowed {
            debug!(
                "EntryState: inode={} transition {:?} -> {:?}",
                self.inode, self.state, target
            );
            self.state = target;
        } else {
            warn!(
                "EntryState: inode={} transition {:?} -> {:?} REJECTED",
                self.inode, self.state, target
            );
        }
        allowed
    }

    /// Increment open/lease reference count (orthogonal to lifecycle state).
    #[allow(dead_code)]
    pub fn pin(&mut self) {
        match self.hold {
            HoldState::Unpinned => {
                self.hold = HoldState::Pinned { open_count: 1 };
            }
            HoldState::Pinned { ref mut open_count } => {
                *open_count += 1;
            }
        }
        debug!(
            "EntryState: inode={} pin -> hold={:?}",
            self.inode, self.hold
        );
    }

    /// Decrement open/lease reference count. Returns `true` if fully released.
    #[allow(dead_code)]
    pub fn unpin(&mut self) -> bool {
        let released = match self.hold {
            HoldState::Pinned { ref mut open_count } => {
                if *open_count > 0 {
                    *open_count -= 1;
                }
                if *open_count == 0 {
                    self.hold = HoldState::Unpinned;
                    true
                } else {
                    false
                }
            }
            HoldState::Unpinned => false,
        };
        debug!(
            "EntryState: inode={} unpin -> hold={:?} released={}",
            self.inode, self.hold, released
        );
        released
    }
}

#[derive(Debug, Default)]
pub struct UpdateAttrParams {
    pub mode: Option<u32>,
    pub size: Option<u64>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub atime: Option<i64>,
    pub mtime: Option<i64>,
}

/// Directory listing cache entry with TTL
struct DirCacheEntry {
    entries: Vec<(u64, String, bool)>, // (inode, name, is_dir)
    cached_at: Instant,
    /// Directory version (shared_gen) from the Filer when this listing
    /// was fetched. Compared against incoming Invalidate notifications
    /// to detect stale listings (Ceph's I_COMPLETE + shared_gen mechanism).
    dir_version: u64,
    /// Whether this directory listing is complete (I_COMPLETE equivalent).
    /// Set true after a full readdir; cleared on any invalidate.
    /// When true, negative dentries (cache miss) can be trusted locally.
    complete: bool,
}

/// Default TTL for metadata cache entries (seconds). Entries older than this are
/// treated as stale and will be fetched from the server on next access.
/// This provides a safety net when Invalidation notifications are lost.
///
/// Phase 2: Extended from 2s to 30s now that callback invalidation is wired
/// up. The Filer pushes Invalidate notifications when directory metadata
/// changes, so the TTL is only a fallback for lost notifications.
const DEFAULT_METADATA_TTL: Duration = Duration::from_secs(30);

/// Metadata cache for FUSE filesystem
pub struct MetadataCache {
    /// inode -> entry mapping (LRU, capacity 10000)
    inode_cache: RwLock<LruCache<u64, CachedEntry>>,
    /// path -> inode mapping
    path_map: RwLock<HashMap<String, u64>>,
    /// parent inode -> directory listing cache (TTL 5s)
    dir_cache: RwLock<HashMap<u64, DirCacheEntry>>,
    /// next inode number (starts at 2, 1 is root)
    next_inode: AtomicU64,
    /// TTL for directory cache
    dir_cache_ttl: Duration,
    /// TTL for metadata cache entries (fallback, 1-2s)
    metadata_ttl: Duration,
    /// Latest known generation per path (from notifications)
    path_generations: RwLock<HashMap<String, u64>>,
    /// Per-directory version (shared_gen) tracking.
    ///
    /// Updated from:
    ///   - Filer lookup/readdir responses (carries dir_version)
    ///   - Invalidate notifications (increments to match server)
    ///
    /// Compared against CachedEntry::dir_shared_gen to detect stale
    /// dentries after their per-dentry lease expires.
    dir_versions: RwLock<HashMap<u64, u64>>,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self::with_capacity(10000)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_ttl(capacity, DEFAULT_METADATA_TTL)
    }

    /// Create a MetadataCache with a custom metadata TTL.
    /// Used by tests to avoid long sleep times.
    #[allow(dead_code)]
    pub fn with_capacity_and_ttl(capacity: usize, metadata_ttl: Duration) -> Self {
        let cache = MetadataCache {
            inode_cache: RwLock::new(LruCache::new(
                NonZero::new(capacity).unwrap_or(NonZero::new(10000).unwrap()),
            )),
            path_map: RwLock::new(HashMap::new()),
            dir_cache: RwLock::new(HashMap::new()),
            next_inode: AtomicU64::new(2),
            dir_cache_ttl: Duration::from_secs(5),
            metadata_ttl,
            path_generations: RwLock::new(HashMap::new()),
            dir_versions: RwLock::new(HashMap::new()),
        };
        // Initialize root directory (inode 1)
        let now = chrono::Utc::now().timestamp();
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        cache.insert(CachedEntry {
            inode: 1,
            parent: 1,
            name: String::new(),
            is_dir: true,
            is_symlink: false,
            symlink_target: None,
            nlink: 2,
            fid: None,
            size: 4096,
            mode: 0o777,
            uid,
            gid,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 4096,
            disk_size: 4096,
            generation: 1,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });
        cache
    }

    /// Allocate a new inode number
    pub fn allocate_inode(&self) -> u64 {
        self.next_inode.fetch_add(1, Ordering::SeqCst)
    }

    /// Get an entry by inode. Returns None if the inode is not cached, if
    /// the entry is in Stale/Tombstone state, or if the TTL safety net has
    /// expired (fallback when Invalidation notifications are lost).
    ///
    /// Phase 3: the state machine is authoritative. The check order is:
    ///   1. Stale/Tombstone → None (explicit state, set by InvalidateHandler
    ///      or unlink/rmdir)
    ///   2. Dirty/Flushing → Some (local authoritative data, TTL bypassed)
    ///   3. Pinned → Some (open file with lease, TTL bypassed)
    ///   4. TTL expired (Clean/New only) → mark Stale, return None
    ///
    /// The TTL check is a safety net for lost Invalidate notifications, not
    /// the primary invalidation mechanism. InvalidateHandler sets Stale
    /// directly when the Filer pushes a notification.
    /// Look up a cached entry by (parent_inode, name).
    /// Used by the dentry lease check in lookup — when the lease is valid
    /// but the entry might be positive or negative.
    pub fn get_inode_by_name(&self, parent_inode: u64, name: &str) -> Option<CachedEntry> {
        let cache = self.inode_cache.read().unwrap();
        for (_, entry) in cache.iter() {
            if entry.parent == parent_inode && entry.name == name {
                // Only return non-Stale, non-Tombstone entries
                if entry.state == EntryState::Stale || entry.state == EntryState::Tombstone {
                    return None;
                }
                return Some(entry.clone());
            }
        }
        None
    }

    pub fn get_inode(&self, inode: u64) -> Option<CachedEntry> {
        let mut cache = self.inode_cache.write().unwrap();
        let entry = cache.get(&inode).cloned()?;

        // 1. Stale/Tombstone: state machine says "needs refresh" or "deleted".
        //    Return None to trigger a fresh Filer fetch on the next access.
        //    This is the authoritative check — even if cached_at is fresh,
        //    a Stale entry must not be served (e.g., root inode marked Stale
        //    by InvalidateHandler, or an entry invalidated between TTL checks).
        if entry.state == EntryState::Stale || entry.state == EntryState::Tombstone {
            debug!(
                "MetadataCache: inode {} is {:?} (state machine), returning None to trigger refresh",
                inode, entry.state
            );
            return None;
        }

        // 2. Dirty/Flushing: local authoritative data, TTL must NOT expire.
        //    Core state machine rule: Dirty/Flushing → Stale is forbidden.
        let has_authoritative_data =
            entry.state == EntryState::Dirty || entry.state == EntryState::Flushing;
        if has_authoritative_data {
            return Some(entry);
        }

        // 3. Pinned (open file with lease): TTL bypassed to prevent cache
        //    miss during slow writes that exceed metadata_ttl.
        if entry.hold.is_pinned() {
            return Some(entry);
        }

        // 3.5. §13 Cap model: if the entry has a cap with CAP_R, the
        //      local cache is authoritative. The server has granted this
        //      client permission to cache reads, so TTL must not expire
        //      the entry. This is what makes cap-protected reads skip
        //      the Filer refresh RPC.
        //
        //      Note: Dirty/Flushing already returned in step 2; this
        //      handles the Clean+Cap case (e.g., a read-only open with
        //      CAP_R but no local modifications).
        if entry
            .cap
            .as_ref()
            .map(|c| c.can_cache_reads())
            .unwrap_or(false)
        {
            return Some(entry);
        }

        // 4. TTL safety net: Clean/New entries that haven't been refreshed
        //    within metadata_ttl are marked Stale. This catches lost
        //    Invalidate notifications (network issues, Filer restart).
        if entry.cached_at.elapsed() > self.metadata_ttl {
            if let Some(e) = cache.get_mut(&inode) {
                e.try_transition(EntryState::Stale);
            }
            debug!(
                "MetadataCache: inode {} TTL expired (age={:?} > ttl={:?}, state={:?}), marking Stale",
                inode,
                entry.cached_at.elapsed(),
                self.metadata_ttl,
                entry.state,
            );
            None
        } else {
            Some(entry)
        }
    }

    /// Pin an inode to skip TTL expiry (called on open).
    /// Uses reference counting: each open increments the count, each release
    /// decrements. The inode is only unpinned when the count reaches 0.
    /// This prevents concurrent open/release (different FUSE workers) from
    /// prematurely unpinning an inode that is still open by another handle.
    ///
    /// Phase 3: entry.hold is the single authoritative source for pin
    /// status. The old pinned_inodes HashMap has been removed.
    pub fn pin_inode(&self, inode: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        let new_count = if let Some(e) = cache.get_mut(&inode) {
            e.pin();
            match e.hold {
                HoldState::Pinned { open_count } => open_count,
                HoldState::Unpinned => 0,
            }
        } else {
            0
        };
        debug!(
            "pin_inode: inode={} new_count={} thread={:?}",
            inode,
            new_count,
            std::thread::current().id()
        );
    }

    /// Unpin an inode (called on release/close).
    /// Decrements the reference count; only removes when count reaches 0.
    ///
    /// Phase 3: entry.hold is the single authoritative source for pin
    /// status. The old pinned_inodes HashMap has been removed.
    ///
    /// Returns `true` if the inode was fully released (open_count reached 0),
    /// `false` if other handles are still open.
    pub fn unpin_inode(&self, inode: u64) -> bool {
        let was_pinned;
        let released;
        {
            let mut cache = self.inode_cache.write().unwrap();
            was_pinned = cache
                .peek(&inode)
                .map(|e| e.hold.is_pinned())
                .unwrap_or(false);
            released = if let Some(e) = cache.get_mut(&inode) {
                e.unpin()
            } else {
                false
            };
        }
        // RACE_TRACE: Log unpin with caller context to detect the race where
        // the background flusher unpins a still-open inode, allowing the
        // InvalidateHandler to evict it mid-write.
        debug!(
            "unpin_inode: inode={} was_pinned={} released={} thread={:?}",
            inode,
            was_pinned,
            released,
            std::thread::current().id()
        );
        released
    }

    /// Check if an inode is pinned (open). Pinned inodes hold a data lease,
    /// so their cached metadata/data is authoritative and should not be
    /// invalidated by server-pushed Invalidate notifications. This prevents
    /// a self-invalidation race where a client's own setattr triggers an
    /// Invalidate that evicts the entry it just updated (causing ENOENT).
    ///
    /// Phase 3: entry.hold is the single authoritative source. Reads from
    /// inode_cache without TTL check (peek) to avoid side effects.
    pub fn is_pinned(&self, inode: u64) -> bool {
        let cache = self.inode_cache.read().unwrap();
        cache
            .peek(&inode)
            .map(|e| e.hold.is_pinned())
            .unwrap_or(false)
    }

    /// Get the EntryState of a cached inode without TTL check.
    /// Returns None if the inode is not in the cache.
    /// Used by InvalidateHandler to decide whether to skip invalidation
    /// (Dirty/Flushing entries have local authoritative data).
    pub fn get_entry_state(&self, inode: u64) -> Option<EntryState> {
        let cache = self.inode_cache.read().unwrap();
        cache.peek(&inode).map(|e| e.state)
    }

    /// Peek at a cached entry without TTL check. Returns None only if the
    /// inode is truly not in the cache (not just TTL-expired).
    /// Used by FUSE callbacks (setxattr, etc.) that need to verify inode
    /// existence without triggering a stale-cache refresh — the entry is
    /// still physically present, just past its TTL.
    pub fn peek_inode(&self, inode: u64) -> Option<CachedEntry> {
        let cache = self.inode_cache.read().unwrap();
        cache.peek(&inode).cloned()
    }

    /// Mark an inode as Dirty (has unsynced local modifications).
    /// Called by write/setattr paths after modifying local data/metadata.
    pub fn mark_dirty(&self, inode: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(e) = cache.get_mut(&inode) {
            e.try_transition(EntryState::Dirty);
        }
    }

    /// §13 Cap model: mark inode Dirty AND mark CAP_W on the cap.
    ///
    /// Called by the **write path** after buffering data locally. The
    /// CAP_W dirty bit is what `process_recall` checks to decide
    /// FlushThenAck vs ImmediateAck on server recall — without this,
    /// a recall would ACK immediately and lose dirty data.
    pub fn mark_dirty_cap_w(&self, inode: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(e) = cache.get_mut(&inode) {
            e.try_transition(EntryState::Dirty);
            if let Some(cap) = e.cap.as_mut() {
                cap.mark_dirty(crate::client_cap::CapSet::CAP_W);
            }
        }
    }

    /// §13 Cap model: mark inode Dirty AND mark CAP_X on the cap.
    ///
    /// Called by the **setattr path** after applying local metadata
    /// changes (size/mode/uid/gid). The CAP_X dirty bit ensures
    /// `process_recall` flushes metadata before ACKing a recall.
    pub fn mark_dirty_cap_x(&self, inode: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(e) = cache.get_mut(&inode) {
            e.try_transition(EntryState::Dirty);
            if let Some(cap) = e.cap.as_mut() {
                cap.mark_dirty(crate::client_cap::CapSet::CAP_X);
            }
        }
    }

    /// Mark an inode as Flushing (currently syncing to Filer/Volume).
    /// Called by flusher before starting RPC.
    pub fn mark_flushing(&self, inode: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(e) = cache.get_mut(&inode) {
            e.try_transition(EntryState::Flushing);
        }
    }

    /// Mark an inode as Clean (synced, no dirty data).
    /// Called by flusher after successful RPC completion.
    pub fn mark_clean(&self, inode: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(e) = cache.get_mut(&inode) {
            // Flushing→Clean is allowed; also update cached_at to prevent
            // immediate TTL expiry after sync.
            e.try_transition(EntryState::Clean);
            e.cached_at = Instant::now();
        }
    }

    /// Mark an inode as Stale (needs refresh on next access).
    /// Called by InvalidateHandler for the root inode (which must never be
    /// evicted). The Stale state causes get_inode to return None, triggering
    /// a re-fetch from the Filer on the next access.
    pub fn mark_stale(&self, inode: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(e) = cache.get_mut(&inode) {
            e.try_transition(EntryState::Stale);
        }
    }

    // ========================================================================
    // §13 Cap model: cap operations on CachedEntry::cap
    //
    // All methods are synchronous (no RPC) — they just mutate the
    // in-cache record.
    // ========================================================================

    /// Grant (or replace) the cap on an inode.
    /// Called from the `CapOpenGrant` response path or `CapUpgradeNotify`.
    ///
    /// If the inode is not in the cache, the grant is silently dropped —
    /// the caller (open path) is expected to have populated the cache
    /// before issuing CapOpenGrant.
    pub fn grant_cap(&self, inode: u64, cap: crate::client_cap::ClientCap) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(e) = cache.get_mut(&inode) {
            e.cap = Some(cap);
            // The server's cap grant affirms that our cached metadata is
            // current. Refresh cached_at and clear any TTL-induced Stale
            // state so subsequent get_inode calls bypass the TTL safety net.
            e.cached_at = Instant::now();
            if e.state == EntryState::Stale {
                e.try_transition(EntryState::Clean);
            }
        } else {
            log::warn!(
                "MetadataCache::grant_cap: inode={} not in cache — cap grant dropped",
                inode
            );
        }
    }

    /// Take (remove) the cap from an inode.
    /// Returns the removed cap so the caller can use its `token` for the
    /// `CapRelease` RPC. Called on `release()` (last close).
    pub fn take_cap(&self, inode: u64) -> Option<crate::client_cap::ClientCap> {
        let mut cache = self.inode_cache.write().unwrap();
        cache.get_mut(&inode).and_then(|e| e.cap.take())
    }

    /// Get a snapshot of the cap for an inode.
    /// Returns `None` if the inode has no cap or is not in cache.
    pub fn get_cap(&self, inode: u64) -> Option<crate::client_cap::ClientCap> {
        let cache = self.inode_cache.read().unwrap();
        cache.peek(&inode).and_then(|e| e.cap.clone())
    }

    /// Mutate the cap for an inode under a closure. Returns `None`
    /// if the inode has no cap (not in cache, or cap not granted yet).
    ///
    /// This is the primary entry point for:
    /// - `mark_cap_dirty(inode, CAP_W|CAP_X)` — write/setattr paths
    /// - `apply_recall` / `apply_upgrade` — recall/upgrade notifications
    /// - `mark_flushed` — after successful flush
    pub fn with_cap_mut<R>(
        &self,
        inode: u64,
        f: impl FnOnce(&mut crate::client_cap::ClientCap) -> R,
    ) -> Option<R> {
        let mut cache = self.inode_cache.write().unwrap();
        cache.get_mut(&inode).and_then(|e| e.cap.as_mut().map(f))
    }

    /// Mark cap bits dirty.
    /// Called by write (CAP_W) and setattr (CAP_X) paths to record
    /// unsynced local state. The `process_recall` function checks
    /// `dirty_caps` to decide between ImmediateAck and FlushThenAck.
    ///
    /// No-op if the inode has no cap (e.g., read-only open or cap not
    /// yet granted — in those cases the write/setattr would have already
    /// failed earlier via `can_cache_writes` / `can_modify_meta` checks).
    pub fn mark_cap_dirty(&self, inode: u64, bits: crate::client_cap::CapSet) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(e) = cache.get_mut(&inode) {
            if let Some(cap) = e.cap.as_mut() {
                cap.mark_dirty(bits);
            }
        }
    }

    /// Mark the cap as flushed — clear `flushing_caps`.
    /// Called by `CapFlusher::flush_and_sync` after a successful flush,
    /// and by `release()` after syncing dirty data.
    pub fn mark_cap_flushed(&self, inode: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(e) = cache.get_mut(&inode) {
            if let Some(cap) = e.cap.as_mut() {
                cap.mark_flushed();
            }
        }
    }

    /// True if the inode has a cap with CAP_R (read cache permission).
    /// Used by the read path to decide whether local cache is authoritative.
    pub fn can_cache_reads(&self, inode: u64) -> bool {
        self.get_cap(inode)
            .map(|c| c.can_cache_reads())
            .unwrap_or(false)
    }

    /// True if the inode has a cap with CAP_W (write cache permission).
    /// Used by the write path to decide whether local buffering is allowed.
    pub fn can_cache_writes(&self, inode: u64) -> bool {
        self.get_cap(inode)
            .map(|c| c.can_cache_writes())
            .unwrap_or(false)
    }

    /// True if the inode has a cap with CAP_X (metadata modify permission).
    /// Used by the setattr path to decide whether local metadata updates
    /// are allowed without an immediate RPC.
    pub fn can_modify_meta(&self, inode: u64) -> bool {
        self.get_cap(inode)
            .map(|c| c.can_modify_meta())
            .unwrap_or(false)
    }

    /// Get path by walking up parent chain
    pub fn get_path_by_parent_chain(&self, inode: u64) -> Option<String> {
        if inode == 1 {
            return Some("/".to_string());
        }
        let entry = self.get_inode(inode)?;
        let mut parts = vec![entry.name.clone()];
        let mut current = entry.parent;
        let mut visited = std::collections::HashSet::new();
        visited.insert(inode);

        while current != 1 {
            if !visited.insert(current) {
                warn!("Cycle detected in parent chain for inode: {}", inode);
                return None;
            }
            let parent_entry = self.get_inode(current)?;
            parts.push(parent_entry.name.clone());
            current = parent_entry.parent;
        }
        parts.reverse();
        let mut path = String::from("/");
        for part in parts {
            if path != "/" {
                path.push('/');
            }
            path.push_str(&part);
        }
        Some(path)
    }

    /// Get inode by full path
    pub fn get_path(&self, path: &str) -> Option<u64> {
        let path_map = self.path_map.read().unwrap();
        path_map.get(path).copied()
    }

    /// Insert an entry as Pinned (open_count=1). Used by create/open callbacks
    /// where the entry is new (not yet in cache) and must be pinned from the
    /// moment it enters the cache. This replaces the old pattern of calling
    /// pin_inode() before insert(), which was a no-op when the inode was not
    /// yet in the cache (Phase 3: entry.hold is authoritative, not pinned_inodes).
    pub fn insert_pinned(&self, mut entry: CachedEntry) {
        entry.hold = HoldState::Pinned { open_count: 1 };
        self.insert(entry);
    }

    /// Insert an entry into the cache (only update path_map, keep existing inode_cache entry for hard links)
    pub fn insert(&self, mut entry: CachedEntry) {
        let inode = entry.inode;
        // === CORRECTNESS GUARD: a new/cached entry must never be its own parent
        // If the incoming entry has parent == inode, that is a fatal metadata
        // inconsistency from either the Filer response or a stale call-site
        // path. Rewrite parent to 1 (root) and log ERROR so we never create a
        // self-referential chain (which would cycle every insert() call).
        if inode != 1 && entry.parent == inode {
            log::error!(
                "METADATA_CORRUPTION: new entry inode={} has parent==self; rewriting parent=1 to break self-cycle. name={:?}",
                inode, entry.name
            );
            entry.parent = 1;
        }
        let parent = entry.parent;
        let path = if inode == 1 {
            String::from("/")
        } else {
            // === CORRECTNESS GUARD ==========================================
            // Build the parent-chain path while holding a SINGLE read-lock on
            // inode_cache. Previously each get_inode() call acquired and
            // released the lock independently, allowing concurrent EVICT +
            // re-INSERT to modify an ancestor's parent mid-traversal. That
            // could produce a cycle: A→B→C→A, causing stat() calls against
            // the cached path to fail with EIO.
            //
            // A cycle in the parent chain is a metadata corruption event —
            // it must never happen silently. When a cycle is detected, we
            // log an ERROR and degrade to a synthetic path so the caller can
            // continue, but the event is loud enough to warrant fsck-level
            // investigation.
            let cache_guard = self.inode_cache.read().unwrap();
            let mut parts = Vec::new();
            parts.push(entry.name.clone());
            let mut current = parent;
            let mut visited = std::collections::HashSet::new();
            visited.insert(inode);
            while current != 1 {
                if !visited.insert(current) {
                    log::error!(
                        "METADATA_CORRUPTION: cycle in parent chain while inserting inode={} name={:?}; \
                         visited_ancestors={:?}, cycle_node={}. \
                         The filesystem cannot trust this cached path — verify with fsck.",
                        inode, entry.name, visited, current
                    );
                    break;
                }
                if let Some(e) = cache_guard.peek(&current) {
                    parts.push(e.name.clone());
                    current = e.parent;
                } else {
                    break;
                }
            }
            drop(cache_guard);
            parts.reverse();
            let mut path = String::from("/");
            for part in parts {
                if path != "/" {
                    path.push('/');
                }
                path.push_str(&part);
            }
            path
        };

        {
            let mut path_map = self.path_map.write().unwrap();
            path_map.insert(path, inode);
        }
        // Invalidate dir cache BEFORE inserting the new entry.
        // invalidate_dir removes all children of parent from inode_cache;
        // calling it after insert would evict the just-inserted entry.
        self.invalidate_dir(parent);
        // Update inode_cache:
        // - If this is a new inode: insert the entry as-is
        // - If the inode exists but name/parent changed: treat as rename, update the entry
        // - If the inode exists with same name/parent: treat as hard link, preserve
        //   the original hard_link_id and update hard_link_counter if provided
        // Always update cached_at to prevent premature expiration
        //
        // === CORRECTNESS GUARD: rename must not create a cycle =================
        // Compute would_cycle BEFORE entering the mutable borrow to avoid
        // borrowing cache as both mutable & immutable in the same block. If
        // the incoming new_parent equals the inode, or a traversal from
        // new_parent up to root would re-enter `inode` (meaning we would make
        // `inode` an ancestor of itself), the rename creates a cycle.
        //
        // This is defense-in-depth: Filer-side apply_rename is authoritative,
        // but a client must never mirror a corrupt parent chain into its own
        // cache, even transiently, because insert() builds paths from the
        // parent chain and a cycle yields bogus cached paths → stat EIO.
        //
        // Special case: the root inode (=1) conventionally has parent=1
        // (self-parent). It is NOT a cycle; skip the check entirely for it.
        let new_parent = entry.parent;
        let would_cycle: bool = if inode == 1 {
            false
        } else {
            let ro_cache = self.inode_cache.read().unwrap();
            let mut wc = new_parent == inode;
            if !wc {
                let mut cur = new_parent;
                let mut visited2 = std::collections::HashSet::new();
                visited2.insert(inode);
                while cur != 1 {
                    if !visited2.insert(cur) {
                        wc = true;
                        break;
                    }
                    if cur == inode {
                        wc = true;
                        break;
                    }
                    match ro_cache.peek(&cur) {
                        Some(e) => cur = e.parent,
                        None => break,
                    }
                }
            }
            wc
        };

        let mut cache = self.inode_cache.write().unwrap();
        if let Some(existing) = cache.get_mut(&inode) {
            if existing.name != entry.name || existing.parent != entry.parent {
                if would_cycle {
                    log::error!(
                        "METADATA_CORRUPTION: rename would create cycle for inode={} \
                         existing_parent={} new_parent={} new_name={:?}. \
                         Rejecting rename update (keeping old entry) to preserve cache integrity.",
                        inode,
                        existing.parent,
                        new_parent,
                        entry.name
                    );
                    // Keep existing entry untouched; update cached_at only.
                    existing.cached_at = std::time::Instant::now();
                } else {
                    // Rename: replace with the new entry's metadata.
                    // Preserve xattrs from the old entry — they are local-only
                    // (not stored in the Filer) and would be lost on replace.
                    // Preserve cap — cap is inode-bound, not path-bound; a
                    // rename/link must not cause the client to lose its cap
                    // state (otherwise mark_dirty becomes no-op and recall
                    // would lose dirty data).
                    let preserved_xattrs = std::mem::take(&mut existing.xattrs);
                    let preserved_cap = existing.cap.take();
                    *existing = entry;
                    existing.xattrs = preserved_xattrs;
                    existing.cap = preserved_cap;
                }
            } else {
                // Same name/parent: update metadata fields from the new entry.
                //
                // Previously this branch only updated cached_at, which caused a
                // critical bug: when `open` refreshed fid/chunks from the Filer
                // (via get_entry_by_inode → entry_to_cached → insert), the
                // existing cache entry (from a prior lookup that set fid=None,
                // chunks=[]) was NOT updated. The read path then saw fid=None
                // and returned EIO, and sync_size_chunks_on_close synced
                // chunks=[] to the Filer, corrupting cross-client reads.
                //
                // Fix: update all metadata fields (fid, chunks, size, mode,
                // uid/gid, timestamps) from the new entry, preserving only
                // hard_link_id/counter semantics.
                if !entry.hard_link_id.is_empty() && existing.hard_link_id.is_empty() {
                    existing.hard_link_id = entry.hard_link_id.clone();
                }
                if entry.hard_link_counter > 0 {
                    existing.hard_link_counter = entry.hard_link_counter;
                }
                // Update fid: prefer the new entry's fid if present, otherwise
                // keep the existing one (don't overwrite a valid fid with None)
                if entry.fid.is_some() {
                    existing.fid = entry.fid.clone();
                }
                // Update chunks: prefer the new entry's chunks if non-empty
                if !entry.chunks.is_empty() {
                    existing.chunks = entry.chunks.clone();
                }
                // Update size/content_size from the new entry.
                //
                // Defensive guard: when the inode is pinned (open), preserve
                // the larger content_size to avoid overwriting unflushed writes
                // with stale Filer data. A concurrent open() by another FUSE
                // worker can call insert() with a Filer response that predates
                // the current write session (sync_size_chunks_on_close hasn't
                // run yet). Without this guard, the stale content_size=0 would
                // overwrite the locally-written content_size, causing release's
                // sync to send size=0 to the Filer — breaking cross-client reads.
                //
                // Truncate (which legitimately shrinks size) goes through
                // setattr(), not insert(), so this guard is safe.
                let is_pinned = existing.hold.is_pinned();
                if is_pinned && existing.content_size > entry.content_size {
                    debug!(
                        "insert: preserving larger content_size for pinned inode={} (existing={}, filer={})",
                        inode, existing.content_size, entry.content_size
                    );
                } else {
                    existing.size = entry.size;
                    existing.content_size = entry.content_size;
                }
                existing.mode = entry.mode;
                existing.uid = entry.uid;
                existing.gid = entry.gid;
                existing.atime = entry.atime;
                existing.mtime = entry.mtime;
                existing.ctime = entry.ctime;
                existing.nlink = entry.nlink;
                existing.is_dir = entry.is_dir;
                existing.is_symlink = entry.is_symlink;
                if entry.is_symlink {
                    // Only overwrite symlink_target if the new value is non-empty.
                    // Filer lookup responses may omit symlink_target, causing
                    // entry_to_cached to produce Some(""). Overwriting with an
                    // empty string breaks readlink after a successful symlink()
                    // that cached the correct target.
                    if entry
                        .symlink_target
                        .as_deref()
                        .is_some_and(|t| !t.is_empty())
                    {
                        existing.symlink_target = entry.symlink_target.clone();
                    }
                }
                existing.disk_size = entry.disk_size;
                existing.generation = entry.generation;
                // Update reliability and placement from the new entry.
                // When the Filer migrates a file (e.g., Flat → EC), the
                // chunks array changes but without updating reliability,
                // the read path would still use the Flat read path for
                // EC-encoded chunks, causing CRC32 mismatches and EIO.
                existing.reliability = entry.reliability.clone();
                existing.placement = entry.placement.clone();
                // Always update cached_at to prevent TTL expiration issues
                existing.cached_at = Instant::now();
            }
        } else {
            // Use push instead of put to capture the evicted entry. If the
            // evicted entry is a pinned (open) inode, re-insert it to prevent
            // loss of metadata needed by sync_size_chunks_on_close. Without
            // this, high-concurrency workloads (e.g. IO500 mdtest) fill the
            // LRU cache and evict open file entries, causing close to skip
            // size/chunks sync → cross-client stale metadata / data loss.
            let evicted = cache.push(inode, entry);
            if let Some((evicted_inode, evicted_entry)) = evicted {
                // Phase 3: check evicted_entry.hold directly instead of the
                // old pinned_inodes HashMap. This is also more correct: no
                // TOCTOU between eviction and pin check.
                if evicted_inode != inode && evicted_entry.hold.is_pinned() {
                    // Re-insert the evicted pinned entry. This may evict
                    // another entry, but pinned entries are few relative to
                    // the 10000-entry cache, so cascading eviction of another
                    // pinned entry is rare. If it happens, that entry's close
                    // will still have its ChunkCache data for best-effort sync.
                    let _ = cache.push(evicted_inode, evicted_entry);
                }
            }
        }
        // Phase 1: record Clean transition for inserted/updated entry.
        // try_transition logs REJECTED if entry is Dirty — that indicates
        // insert() is overwriting dirty data with a Filer response, which
        // may be a bug (open refresh overwriting unflushed writes).
        if let Some(e) = cache.get_mut(&inode) {
            e.try_transition(EntryState::Clean);
        }
        drop(cache);
    }

    /// Force-update content_size and size for an inode, bypassing the
    /// defensive guard in insert(). Used by open() when the Filer's value
    /// is authoritative (no local chunk data — cache was invalidated).
    pub fn set_content_size(&self, inode: u64, size: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            entry.content_size = size;
            entry.size = size;
        }
    }

    /// Remove an entry by inode
    pub fn remove(&self, inode: u64) {
        let entry = {
            let mut cache = self.inode_cache.write().unwrap();
            cache.pop(&inode)
        };
        if let Some(entry) = entry {
            let mut path_map = self.path_map.write().unwrap();
            let paths_to_remove: Vec<String> = path_map
                .iter()
                .filter(|(_, &ino)| ino == inode)
                .map(|(path, _)| path.clone())
                .collect();
            for path in paths_to_remove {
                path_map.remove(&path);
            }
            drop(path_map);
            self.invalidate_dir(entry.parent);
        }
    }

    /// Remove a specific path for an inode (used for hard links)
    pub fn remove_path(&self, inode: u64, path: &str) {
        // Remove the specific path mapping
        {
            let mut path_map = self.path_map.write().unwrap();
            path_map.remove(path);
        }

        // Check if this was the entry stored in inode_cache (just check if entry exists)
        let has_cache_entry = {
            let cache = self.inode_cache.read().unwrap();
            cache.contains(&inode)
        };

        // Now check if the path we're removing matches the cached entry's path
        let should_update = if has_cache_entry {
            let entry_path = self.get_path_by_parent_chain(inode);
            if let Some(ep) = entry_path {
                ep == path
            } else {
                false
            }
        } else {
            false
        };

        if should_update {
            // Find another path for this inode and update the inode_cache entry
            {
                let path_map = self.path_map.read().unwrap();
                let mut new_path = None;
                for (p, ino) in path_map.iter() {
                    if *ino == inode && p != path {
                        new_path = Some(p.clone());
                        break;
                    }
                }
                drop(path_map);

                if let Some(np) = new_path {
                    // Get current entry and update its name
                    let mut cache = self.inode_cache.write().unwrap();
                    if let Some(entry) = cache.get_mut(&inode) {
                        // Extract name from the new path
                        let name = np.rsplit('/').next().unwrap_or("").to_string();
                        entry.name = name;
                    }
                }
            }
        }

        // Invalidate the parent directory cache
        if let Some(entry) = self.get_inode(inode) {
            self.invalidate_dir(entry.parent);
        }
    }

    /// Invalidate directory listing cache for a parent inode.
    ///
    /// 清空 dir_cache（目录列表缓存），确保 `list_children` 不会返回过时的条目。
    /// 这是 CRDT delta sync 联动失效的关键：puller 拉取到 Remove delta 后，
    /// 必须清空 dir_cache 中的旧目录列表，否则 readdir 会继续返回已删除的文件。
    ///
    /// 注意：只清空 dir_cache（目录列表），不清空 inode_cache 中的子条目。
    /// inode_cache 的条目由各自的 TTL 和显式 remove() 管理生命周期。
    /// 清空 inode_cache 子条目会导致刚 insert 的条目被误删（create→insert→invalidate_dir 竞态）。
    pub fn invalidate_dir(&self, parent_inode: u64) {
        let mut dir_cache = self.dir_cache.write().unwrap();
        dir_cache.remove(&parent_inode);
        // Bump dir_version so stale dentries with dir_shared_gen mismatch
        // are detected on next lookup (Ceph's clear_dir_complete_and_ordered).
        let mut versions = self.dir_versions.write().unwrap();
        let v = versions.entry(parent_inode).or_insert(0);
        *v = v.wrapping_add(1);
    }

    /// Get the current dir_version (shared_gen) for a parent directory.
    /// Returns 0 if unknown (first access — no dentry can match yet).
    pub fn get_dir_version(&self, parent_inode: u64) -> u64 {
        self.dir_versions
            .read()
            .unwrap()
            .get(&parent_inode)
            .copied()
            .unwrap_or(0)
    }

    /// Update dir_version from a Filer response (lookup/readdir carry the
    /// parent's version). Only moves forward — never decreases.
    pub fn update_dir_version(&self, parent_inode: u64, version: u64) {
        let mut versions = self.dir_versions.write().unwrap();
        let entry = versions.entry(parent_inode).or_insert(0);
        if version > *entry {
            *entry = version;
        }
    }

    /// Check if a dentry's dir_shared_gen matches the parent's current
    /// dir_version. When true and the parent dir_cache is complete,
    /// the dentry (positive or negative) can be trusted without RPC.
    pub fn dentry_shared_gen_valid(&self, parent_inode: u64, dn_shared_gen: u64) -> bool {
        let versions = self.dir_versions.read().unwrap();
        let current = versions.get(&parent_inode).copied().unwrap_or(0);
        dn_shared_gen == current
    }

    /// Check if the parent directory's listing is complete (I_COMPLETE
    /// equivalent). When true, negative dentries are trustworthy.
    pub fn is_dir_complete(&self, parent_inode: u64) -> bool {
        let dir_cache = self.dir_cache.read().unwrap();
        dir_cache
            .get(&parent_inode)
            .map(|e| e.complete)
            .unwrap_or(false)
    }

    /// Mark a directory's listing as complete (after full readdir).
    pub fn mark_dir_complete(&self, parent_inode: u64, version: u64) {
        let mut dir_cache = self.dir_cache.write().unwrap();
        if let Some(entry) = dir_cache.get_mut(&parent_inode) {
            entry.complete = true;
            entry.dir_version = version;
        }
    }

    /// Grant a dentry lease on a cached entry (positive or negative).
    /// Called after a Filer lookup/readdir response that carries a
    /// dentry lease TTL.
    pub fn grant_dentry_lease(&self, parent_inode: u64, name: &str, duration_ms: u64, issuer: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        // Find entry by (parent, name) — dentry leases are per-name, not
        // per-inode (negative dentries have no inode). We iterate since
        // the cache is keyed by inode.
        for (_, entry) in cache.iter_mut() {
            if entry.parent == parent_inode && entry.name == name {
                entry.dentry_lease = Some(DentryLease {
                    expire_at: Instant::now() + Duration::from_millis(duration_ms),
                    duration_ms,
                    issuer,
                });
                entry.dir_shared_gen = self.get_dir_version(parent_inode);
                return;
            }
        }
        // Entry not found — dentry lease for a name not in cache.
        // This can happen for negative dentries that were evicted.
        // The lease is lost; next lookup will RPC and re-acquire.
    }

    /// Check if a cached dentry (by parent + name) has a valid lease.
    /// Returns Some(true) if lease is valid (trust cache, skip RPC),
    /// Some(false) if lease expired but shared_gen still matches
    /// (trust if dir_complete), None if no entry or lease missing.
    pub fn check_dentry_lease(&self, parent_inode: u64, name: &str) -> DentryLeaseStatus {
        let cache = self.inode_cache.read().unwrap();
        for (_, entry) in cache.iter() {
            if entry.parent == parent_inode && entry.name == name {
                // Layer 1: dentry lease valid?
                if let Some(ref lease) = entry.dentry_lease {
                    if Instant::now() < lease.expire_at {
                        return DentryLeaseStatus::LeaseValid;
                    }
                }
                // Layer 2: shared_gen matches and dir is complete?
                if self.dentry_shared_gen_valid(parent_inode, entry.dir_shared_gen)
                    && self.is_dir_complete(parent_inode)
                {
                    return DentryLeaseStatus::SharedGenValid;
                }
                return DentryLeaseStatus::Expired;
            }
        }
        // No cached entry for this name.
        // Layer 2 (negative): dir complete + shared_gen current → ENOENT
        if self.is_dir_complete(parent_inode) {
            return DentryLeaseStatus::NegativeComplete;
        }
        DentryLeaseStatus::Miss
    }

    /// Invalidate (clear) dentry leases for all children of a directory.
    /// Called when the parent receives an Invalidate notification.
    pub fn invalidate_dentry_leases(&self, parent_inode: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        let mut count = 0u32;
        for (_, entry) in cache.iter_mut() {
            if entry.parent == parent_inode && entry.dentry_lease.is_some() {
                entry.dentry_lease = None;
                count += 1;
            }
        }
        if count > 0 {
            log::debug!(
                "MetadataCache: cleared {} dentry leases for parent={}",
                count,
                parent_inode
            );
        }
    }

    /// Mark all cached entries as Stale. Called when Filer reconnects or
    /// leader changes, to handle potentially missed Invalidate notifications
    /// (inspired by JuiceFS redisCache.onInvalidateConnect which Purges all
    /// caches on reconnect).
    ///
    /// Does NOT delete entries — entries with local authoritative data
    /// (Dirty/Flushing) are preserved and will be synced normally.
    /// Non-dirty entries are marked Stale so the next access fetches fresh
    /// data from the Filer.
    pub fn invalidate_all(&self) {
        let mut cache = self.inode_cache.write().unwrap();
        let mut invalidated = 0u32;
        let mut preserved = 0u32;
        let mut caps_cleared = 0u32;
        for (_, entry) in cache.iter_mut() {
            // §13 Cap model: clear cap on EVERY entry, including Dirty/Flushing.
            //
            // The Filer-side cap_manager resets on leader switch (new leader
            // has no record of the old leader's cap holders), so any retained
            // cap token is stale — a subsequent `take_cap()` would return a
            // cap whose token the new Filer doesn't recognize, causing
            // `cap_release` RPCs to silently fail (best-effort, but stale).
            //
            // Clearing cap does NOT lose data: dirty data lives in
            // `ChunkCache` / `inline_buffers` (not in the cap field), and
            // release's `sync_size_chunks_on_close` re-syncs it to the new
            // Filer. After clearing, the next `open()` re-grants a fresh cap
            // via `grant_cap` from the new leader's response.
            //
            // `mark_dirty_cap_w` / `mark_dirty_cap_x` are no-ops when cap is
            // None, but `mark_dirty(inode)` (ChunkCache dirty tracking) still
            // works, so the flusher path is unaffected.
            if entry.cap.take().is_some() {
                caps_cleared += 1;
            }
            // Dirty/Flushing entries have local authoritative data that
            // must not be invalidated — they will be synced via flusher.
            if entry.state == EntryState::Dirty || entry.state == EntryState::Flushing {
                preserved += 1;
                continue;
            }
            // Force TTL expiry by setting cached_at far in the past.
            // get_inode() will return None, triggering a fresh Filer fetch.
            entry.cached_at = Instant::now() - self.metadata_ttl * 2;
            entry.try_transition(EntryState::Stale);
            invalidated += 1;
        }
        drop(cache);

        // Clear dir_cache — directory listings are no longer trustworthy
        let dir_count = self.dir_cache.read().unwrap().len();
        self.dir_cache.write().unwrap().clear();

        log::warn!(
            "MetadataCache: invalidate_all — {} entries marked Stale, {} Dirty/Flushing preserved, {} caps cleared, {} dir_cache entries cleared",
            invalidated,
            preserved,
            caps_cleared,
            dir_count
        );
    }

    /// Get directory listing (returns cached if fresh, None if needs refresh)
    pub fn get_dir_listing(&self, parent_inode: u64) -> Option<Vec<(u64, String, bool)>> {
        let dir_cache = self.dir_cache.read().unwrap();
        if let Some(entry) = dir_cache.get(&parent_inode) {
            if entry.cached_at.elapsed() < self.dir_cache_ttl {
                return Some(entry.entries.clone());
            }
        }
        None
    }

    /// Set directory listing cache
    pub fn set_dir_listing(&self, parent_inode: u64, entries: Vec<(u64, String, bool)>) {
        let version = self.get_dir_version(parent_inode);
        let mut dir_cache = self.dir_cache.write().unwrap();
        dir_cache.insert(
            parent_inode,
            DirCacheEntry {
                entries,
                cached_at: Instant::now(),
                dir_version: version,
                complete: true, // full readdir → I_COMPLETE
            },
        );
    }

    /// Get path for an inode by walking up the tree
    pub fn inode_to_path(&self, inode: u64) -> Option<String> {
        let path_map = self.path_map.read().unwrap();
        for (path, ino) in path_map.iter() {
            if *ino == inode {
                return Some(path.clone());
            }
        }
        None
    }

    /// Update file size
    pub fn update_size(&self, inode: u64, size: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            entry.size = size;
            entry.content_size = size;
            entry.mtime = chrono::Utc::now().timestamp();
            entry.cached_at = Instant::now();
        }
    }

    /// Update FID (file ID)
    pub fn update_fid(&self, inode: u64, fid: Fid) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            entry.fid = Some(fid);
        }
    }

    /// Update chunks 列表（write/flush 后调用，确保 close 时 sync 正确的 chunks 到 filer）
    pub fn update_chunks(&self, inode: u64, chunks: Vec<powerfs_orset::CachedFileChunk>) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            entry.chunks = chunks;
        }
    }

    /// Truncate the chunks metadata list for an inode to `new_size`.
    /// - Chunks entirely beyond `new_size` are removed from the metadata list.
    /// - Chunks that straddle `new_size` have their `size` truncated.
    /// - Chunks entirely within `new_size` are preserved.
    ///
    /// This is separate from `ChunkCache::truncate_chunks` (which truncates
    /// the data cache). Both must be called during truncate to prevent
    /// stale reads: without this, the read path uses the old chunks list
    /// to fetch data from the Volume Server, returning pre-truncate data
    /// for regions that should be holes.
    pub fn truncate_chunks_metadata(&self, inode: u64, new_size: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            let before_len = entry.chunks.len();
            entry.chunks.retain(|c| c.offset < new_size);
            for chunk in entry.chunks.iter_mut() {
                let chunk_end = chunk.offset + chunk.size;
                if chunk_end > new_size {
                    chunk.size = new_size - chunk.offset;
                }
            }
            log::debug!(
                "truncate_chunks_metadata: inode={} new_size={} chunks {} -> {}",
                inode,
                new_size,
                before_len,
                entry.chunks.len()
            );
        }
    }

    /// Update chunk sizes after a write to reflect the actual data layout.
    ///
    /// This fixes a critical bug where the `if Some(fid)` write branch only called
    /// `update_size` (updating `content_size`) but never updated `chunks[].size`,
    /// leaving `chunks[0].size` stuck at 0 from `create()`. When `sync_size_chunks_on_close`
    /// synced this stale chunks list to the Filer, subsequent reads from other clients
    /// got `chunks[0].size=0`, causing read failures or incorrect fallback size estimation.
    ///
    /// For each chunk touched by the write range `[offset, offset+length)`, this method
    /// updates the chunk's `size` to cover the valid data end. If a chunk entry doesn't
    /// exist at the expected offset, it is created using the provided FID.
    pub fn update_chunk_sizes_after_write(
        &self,
        inode: u64,
        offset: u64,
        length: u64,
        chunk_size: u64,
        fid: &Fid,
    ) {
        if length == 0 {
            return;
        }
        let write_end = offset + length;
        let start_chunk_idx = offset / chunk_size;
        let end_chunk_idx = (write_end - 1) / chunk_size;

        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            let mtime = entry.mtime.max(0) as u64;
            let chunks_before = entry.chunks.iter().map(|c| c.offset).collect::<Vec<_>>();
            log::debug!(
                "update_chunk_sizes_after_write: inode={} offset={} len={} chunk_idx={}..={} chunks_before={:?} fid.file_key={}",
                inode, offset, length, start_chunk_idx, end_chunk_idx, chunks_before, fid.file_key
            );

            for chunk_idx in start_chunk_idx..=end_chunk_idx {
                // Safety: chunk_idx must fit within FILE_KEY_BLOCK_SIZE to avoid
                // needle ID overflow into the next file's block (2TB max @ 2MB chunks).
                if chunk_idx >= powerfs_common::constants::FILE_KEY_BLOCK_SIZE {
                    log::error!(
                        "chunk_idx {} exceeds FILE_KEY_BLOCK_SIZE {} (file too large, max 2TB)",
                        chunk_idx,
                        powerfs_common::constants::FILE_KEY_BLOCK_SIZE
                    );
                    break;
                }
                let chunk_offset = chunk_idx * chunk_size;
                let chunk_data_end = write_end.min(chunk_offset + chunk_size);
                let chunk_data_size = chunk_data_end - chunk_offset;

                if let Some(chunk) = entry.chunks.iter_mut().find(|c| c.offset == chunk_offset) {
                    // Extend the chunk size to cover the new write
                    chunk.size = chunk.size.max(chunk_data_size);
                } else {
                    // Create a new chunk entry for this offset
                    entry.chunks.push(CachedFileChunk {
                        offset: chunk_offset,
                        size: chunk_data_size,
                        mtime,
                        needle_id: fid.file_key.saturating_add(chunk_idx),
                        volume_id: fid.volume_id.0,
                        crc32: 0,
                    });
                    log::debug!(
                        "update_chunk_sizes_after_write: inode={} added new chunk at offset={} needle_id={}",
                        inode, chunk_offset, fid.file_key.saturating_add(chunk_idx)
                    );
                }
            }
            let chunks_after = entry.chunks.iter().map(|c| c.offset).collect::<Vec<_>>();
            log::debug!(
                "update_chunk_sizes_after_write: inode={} chunks_after={:?}",
                inode,
                chunks_after
            );
        }
    }

    /// P3: Update chunk sizes after a Stripe write.
    ///
    /// Unlike Flat mode (which uses a single fid for all chunks), Stripe mode
    /// distributes chunks across multiple volumes. Each 1MB sub-chunk within a
    /// stripe unit gets its own needle_id (`base_needle + chunk_idx_within_unit`).
    /// The volume_id comes from the stripe unit's pre-allocated chunk.
    pub fn update_chunk_sizes_after_write_stripe(
        &self,
        inode: u64,
        offset: u64,
        length: u64,
        chunk_size: u64,
        placement: &powerfs_layout::Placement,
        stripe_chunks: &[CachedFileChunk],
    ) {
        if length == 0 {
            return;
        }
        let write_end = offset + length;
        let start_chunk_idx = offset / chunk_size;
        let end_chunk_idx = (write_end - 1) / chunk_size;

        let stripe_size = match placement {
            powerfs_layout::Placement::Stripe { stripe_size, .. }
            | powerfs_layout::Placement::WideStripe { stripe_size, .. } => *stripe_size,
            _ => return,
        };
        let stripe_size = stripe_size.max(1);

        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            let mtime = entry.mtime.max(0) as u64;

            for chunk_idx in start_chunk_idx..=end_chunk_idx {
                let chunk_offset = chunk_idx * chunk_size;
                let chunk_data_end = write_end.min(chunk_offset + chunk_size);
                let chunk_data_size = chunk_data_end - chunk_offset;

                // Resolve (volume_id, needle_id) for this chunk via stripe mapping
                let stripe_unit_idx = (chunk_offset / stripe_size) as usize;
                if stripe_unit_idx >= stripe_chunks.len() {
                    break; // beyond pre-allocated range
                }
                let base = &stripe_chunks[stripe_unit_idx];
                let chunk_idx_within_unit = (chunk_offset % stripe_size) / chunk_size.max(1);
                let needle_id = base.needle_id.saturating_add(chunk_idx_within_unit);
                let volume_id = base.volume_id;

                if let Some(chunk) = entry.chunks.iter_mut().find(|c| c.offset == chunk_offset) {
                    chunk.size = chunk.size.max(chunk_data_size);
                } else {
                    entry.chunks.push(CachedFileChunk {
                        offset: chunk_offset,
                        size: chunk_data_size,
                        mtime,
                        needle_id,
                        volume_id,
                        crc32: 0,
                    });
                }
            }
        }
    }

    /// Update CRC32 for a chunk at the given offset after flushing to Volume Server.
    /// Called from flush_dirty_chunks_impl after successful write_blob.
    pub fn update_chunk_crc32(&self, inode: u64, chunk_offset: u64, crc32: u32) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            if let Some(chunk) = entry.chunks.iter_mut().find(|c| c.offset == chunk_offset) {
                chunk.crc32 = crc32;
            }
        }
    }

    pub fn update_attr(&self, inode: u64, params: UpdateAttrParams) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            if let Some(m) = params.mode {
                entry.mode = m;
            }
            if let Some(s) = params.size {
                entry.size = s;
            }
            if let Some(u) = params.uid {
                entry.uid = u;
            }
            if let Some(g) = params.gid {
                entry.gid = g;
            }
            if let Some(a) = params.atime {
                entry.atime = a;
            }
            if let Some(mt) = params.mtime {
                entry.mtime = mt;
            }
            let now = chrono::Utc::now().timestamp();
            entry.ctime = now;
            entry.cached_at = Instant::now();
        }
    }

    /// List children of a directory from cache by scanning inode_cache
    /// parent field. Supports hard links (same inode, different names).
    pub fn list_children(&self, parent_inode: u64) -> Vec<(u64, String, bool)> {
        // Scan inode_cache directly by parent field, bypassing path_map.
        // path_map is unreliable for child enumeration because insert()
        // builds paths by walking the parent chain, which can truncate if
        // an ancestor is missing from cache. This caused cp -prf to miss
        // children after large directory copies (path_map had stale/truncated
        // paths, so the prefix scan skipped valid entries).
        //
        // inode_cache is the authoritative source: every cached entry has a
        // correct `parent` field set at insert time. Scanning by parent is
        // O(n) over the cache but avoids all path-construction bugs.
        // Use peek() (not get()) to bypass TTL — entries physically present
        // in the cache have correct is_dir regardless of TTL expiry.
        let cache = self.inode_cache.read().unwrap();
        let children: Vec<(u64, String, bool)> = cache
            .iter()
            .filter(|(_, e)| e.parent == parent_inode && e.inode != parent_inode)
            .map(|(_, e)| (e.inode, e.name.clone(), e.is_dir))
            .collect();

        log::debug!(
            "list_children(parent_inode={}) returned {} children: {:?}",
            parent_inode,
            children.len(),
            children
                .iter()
                .map(|(ino, name, is_dir)| format!(
                    "(inode={}, name={}, is_dir={})",
                    ino, name, is_dir
                ))
                .collect::<Vec<_>>()
        );

        children
    }

    /// Rename an entry
    pub fn rename(
        &self,
        olddir: u64,
        oldname: &str,
        newdir: u64,
        newname: &str,
    ) -> Result<(), String> {
        // Find the source entry
        let entry = {
            let children = self.list_children(olddir);
            let mut found = None;
            for (ino, name, _) in children {
                if name == oldname {
                    found = self.get_inode(ino);
                    break;
                }
            }
            found.ok_or_else(|| "source not found".to_string())?
        };

        let inode = entry.inode;
        let old_parent = entry.parent;

        // Remove the source entry from path_map BEFORE updating its name,
        // so that the target lookup below doesn't find the source entry.
        let old_path = self.inode_to_path(inode).unwrap_or_default();
        {
            let mut path_map = self.path_map.write().unwrap();
            path_map.remove(&old_path);
        }

        // If target exists, remove it BEFORE renaming the source.
        // This must happen while the source still has its old name,
        // otherwise list_children(newdir) would find the renamed source
        // entry (which now has name == newname) and remove it instead.
        if olddir == newdir {
            // Same directory: list children and find target by name.
            // The source entry still has oldname, so this is safe.
            let children = self.list_children(newdir);
            for (target_ino, name, _) in children {
                if name == newname && target_ino != inode {
                    let _ = self.remove_entry_only(target_ino);
                    break;
                }
            }
        } else {
            // Different directory: just check by name.
            let children = self.list_children(newdir);
            for (target_ino, name, _) in children {
                if name == newname {
                    let _ = self.remove_entry_only(target_ino);
                    break;
                }
            }
        }

        // Now update the source entry's parent and name
        {
            let mut cache = self.inode_cache.write().unwrap();
            if let Some(e) = cache.get_mut(&inode) {
                e.parent = newdir;
                e.name = newname.to_string();
                let now = chrono::Utc::now().timestamp();
                e.ctime = now;
                e.mtime = now;
            }
        }

        // Insert new path
        let new_entry = self
            .get_inode(inode)
            .ok_or_else(|| "inode not found in cache after update".to_string())?;
        let new_path = if newdir == 1 {
            format!("/{}", new_entry.name)
        } else {
            let parent_path = self.inode_to_path(newdir).unwrap_or_default();
            format!("{}/{}", parent_path, new_entry.name)
        };
        {
            let mut path_map = self.path_map.write().unwrap();
            path_map.insert(new_path, inode);
        }

        // POSIX: rename updates mtime/ctime of both parent directories.
        // This is critical for kernel readdir cache invalidation: the kernel
        // compares the directory's mtime (fetched via getattr) with its
        // cached value to decide whether to serve stale readdir entries.
        // Without this update, the kernel's readdir cache returns the old
        // source name for up to 1s after the rename.
        {
            let now = chrono::Utc::now().timestamp();
            let mut cache = self.inode_cache.write().unwrap();
            if let Some(e) = cache.get_mut(&old_parent) {
                e.mtime = now;
                e.ctime = now;
                e.cached_at = Instant::now();
            }
            if old_parent != newdir {
                if let Some(e) = cache.get_mut(&newdir) {
                    e.mtime = now;
                    e.ctime = now;
                    e.cached_at = Instant::now();
                }
            }
        }

        // Invalidate old and new directory caches
        self.invalidate_dir(old_parent);
        if old_parent != newdir {
            self.invalidate_dir(newdir);
        }

        Ok(())
    }

    /// Remove entry from cache only (without deleting data)
    fn remove_entry_only(&self, inode: u64) -> Option<CachedEntry> {
        let entry = {
            let mut cache = self.inode_cache.write().unwrap();
            cache.pop(&inode)
        };
        if let Some(ref e) = entry {
            let path = self.inode_to_path(inode).unwrap_or_default();
            let mut path_map = self.path_map.write().unwrap();
            path_map.remove(&path);
            self.invalidate_dir(e.parent);
        }
        entry
    }

    /// Increment nlink count
    pub fn inc_nlink(&self, inode: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            entry.nlink += 1;
        }
    }

    /// Decrement nlink count, returns true if nlink reaches 0
    pub fn dec_nlink(&self, inode: u64) -> bool {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            if entry.nlink > 0 {
                entry.nlink -= 1;
            }
            return entry.nlink == 0;
        }
        false
    }

    /// Get nlink count
    pub fn get_nlink(&self, inode: u64) -> u32 {
        let cache = self.inode_cache.read().unwrap();
        cache.peek(&inode).map(|e| e.nlink).unwrap_or(0)
    }

    /// Update symlink target
    pub fn set_symlink_target(&self, inode: u64, target: String) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            entry.is_symlink = true;
            entry.size = target.len() as u64;
            entry.symlink_target = Some(target);
        }
    }

    /// Get symlink target
    pub fn get_symlink_target(&self, inode: u64) -> Option<String> {
        let cache = self.inode_cache.read().unwrap();
        cache.peek(&inode).and_then(|e| e.symlink_target.clone())
    }

    /// Set extended attribute
    pub fn set_xattr(&self, inode: u64, name: &str, value: &[u8]) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            entry.xattrs.insert(name.to_string(), value.to_vec());
        }
    }

    /// Get extended attribute
    pub fn get_xattr(&self, inode: u64, name: &str) -> Option<Vec<u8>> {
        let cache = self.inode_cache.read().unwrap();
        cache.peek(&inode).and_then(|e| e.xattrs.get(name).cloned())
    }

    /// Remove extended attribute
    pub fn remove_xattr(&self, inode: u64, name: &str) -> bool {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            entry.xattrs.remove(name)
        } else {
            None
        }
        .is_some()
    }

    /// List extended attributes
    pub fn list_xattrs(&self, inode: u64) -> Vec<String> {
        let cache = self.inode_cache.read().unwrap();
        cache
            .peek(&inode)
            .map(|e| e.xattrs.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Lookup an entry by parent inode and name
    pub fn lookup_in_cache(&self, parent: u64, name: &str) -> Option<CachedEntry> {
        let children = self.list_children(parent);
        for (inode, child_name, _) in children {
            if child_name == name {
                return self.get_inode(inode);
            }
        }
        None
    }

    /// Invalidate cache entry by path
    pub fn invalidate_path(&self, path: &str) {
        let maybe_inode = {
            let path_map = self.path_map.read().unwrap();
            path_map.get(path).copied()
        };
        if let Some(inode) = maybe_inode {
            self.remove(inode);
            debug!("Invalidated cache for path: {} (inode: {})", path, inode);
        }

        let parent_path = if let Some(last_slash) = path.rfind('/') {
            if last_slash == 0 {
                "/"
            } else {
                &path[..last_slash]
            }
        } else {
            "/"
        };

        let maybe_parent_inode = {
            let path_map = self.path_map.read().unwrap();
            path_map.get(parent_path).copied()
        };
        if let Some(parent_inode) = maybe_parent_inode {
            let mut dir_cache = self.dir_cache.write().unwrap();
            dir_cache.remove(&parent_inode);
            debug!("Invalidated directory cache for: {}", parent_path);
        }
    }

    /// Clear all cache entries and re-initialize root directory.
    /// Called when JOB_COMPLETE notification is received.
    pub fn clear_all(&self) {
        {
            let mut cache = self.inode_cache.write().unwrap();
            cache.clear();
        }
        {
            let mut path_map = self.path_map.write().unwrap();
            path_map.clear();
        }
        {
            let mut dir_cache = self.dir_cache.write().unwrap();
            dir_cache.clear();
        }
        {
            let mut gens = self.path_generations.write().unwrap();
            gens.clear();
        }

        let now = chrono::Utc::now().timestamp();
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let mut cache = self.inode_cache.write().unwrap();
        cache.put(
            1,
            CachedEntry {
                inode: 1,
                parent: 1,
                name: String::new(),
                is_dir: true,
                is_symlink: false,
                symlink_target: None,
                nlink: 2,
                fid: None,
                size: 4096,
                mode: 0o755,
                uid,
                gid,
                atime: now,
                mtime: now,
                ctime: now,
                xattrs: HashMap::new(),
                chunks: Vec::new(),
                hard_link_id: String::new(),
                hard_link_counter: 0,
                content_size: 4096,
                disk_size: 4096,
                generation: 1,
                placement: None,
                reliability: powerfs_layout::reliability::Reliability::default(),
                replica_chunks: Vec::new(),
                shard_id: None,
                cached_at: Instant::now(),
                state: EntryState::default(),
                hold: HoldState::default(),
                cap: None,
                dentry_lease: None,
                dir_shared_gen: 0,
            },
        );
        drop(cache);
        let mut path_map = self.path_map.write().unwrap();
        path_map.insert("/".to_string(), 1);

        debug!("Metadata cache fully cleared and root re-initialized");
    }

    /// Invalidate a specific inode from cache (called on server notification)
    ///
    /// This removes the inode entry and invalidates its parent directory listing.
    /// The next access will re-fetch fresh data from the server.
    pub fn invalidate_inode(&self, inode: u64) {
        // RACE_TRACE: Capture full context before invalidation to diagnose
        // the B1/B3 write ENOENT race. Log pinned state, chunk state, and
        // thread ID to correlate with concurrent pin/unpin/flush operations.
        let was_pinned = self.is_pinned(inode);
        let has_chunks = self.chunk_cache_has_chunks_for_trace(inode);
        let has_dirty = self.chunk_cache_has_dirty_for_trace(inode);

        // Get parent before removing
        let parent = {
            let cache = self.inode_cache.read().unwrap();
            cache.peek(&inode).map(|e| e.parent)
        };

        // Remove the inode entry
        self.remove(inode);

        // Invalidate parent directory listing
        if let Some(p) = parent {
            self.invalidate_dir(p);
        }

        warn!(
            "invalidate_inode: inode={} parent={:?} was_pinned={} has_chunks={} has_dirty={} thread={:?}",
            inode, parent, was_pinned, has_chunks, has_dirty, std::thread::current().id()
        );
    }

    /// Trace helper: check if chunk cache has any chunks for this inode.
    /// Used by invalidate_inode for RACE_TRACE logging only.
    fn chunk_cache_has_chunks_for_trace(&self, inode: u64) -> bool {
        // Access the chunk_cache reference if available; this is a best-effort
        // trace. The MetadataCache doesn't own the ChunkCache, so we can only
        // check the inode_cache for now. The InvalidateHandler has the real
        // has_chunks check.
        self.inode_cache.read().unwrap().contains(&inode)
    }

    /// Trace helper: check if chunk cache has dirty chunks for this inode.
    fn chunk_cache_has_dirty_for_trace(&self, _inode: u64) -> bool {
        // MetadataCache doesn't have access to ChunkCache's dirty state.
        // The InvalidateHandler has the real has_dirty_chunks check.
        false
    }

    /// Check if a cached inode's version is stale compared to the given version
    ///
    /// Returns true if the cache entry is stale (server version > cached version)
    /// or if the inode is not in cache.
    pub fn is_inode_stale(&self, inode: u64, server_version: u64) -> bool {
        let cache = self.inode_cache.read().unwrap();
        match cache.peek(&inode) {
            Some(entry) => server_version > entry.generation,
            None => true,
        }
    }

    /// Update the latest known generation for a path (from notifications)
    pub fn update_path_generation(&self, path: &str, generation: u64) {
        let mut gens = self.path_generations.write().unwrap();
        gens.insert(path.to_string(), generation);
    }

    /// Get the latest known generation for a path
    pub fn get_path_generation(&self, path: &str) -> Option<u64> {
        let gens = self.path_generations.read().unwrap();
        gens.get(path).copied()
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_inode() {
        let cache = MetadataCache::new();
        let root = cache.get_inode(1).unwrap();
        assert!(root.is_dir);
        assert_eq!(root.inode, 1);
    }

    #[test]
    fn test_allocate_inode() {
        let cache = MetadataCache::new();
        let ino1 = cache.allocate_inode();
        let ino2 = cache.allocate_inode();
        assert_eq!(ino1, 2);
        assert_eq!(ino2, 3);
    }

    #[test]
    fn test_insert_and_get() {
        let cache = MetadataCache::new();
        let inode = cache.allocate_inode();
        let now = chrono::Utc::now().timestamp();
        cache.insert(CachedEntry {
            inode,
            parent: 1,
            name: "test.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 100,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        let entry = cache.get_inode(inode).unwrap();
        assert_eq!(entry.name, "test.txt");
        assert!(!entry.is_dir);
        assert_eq!(entry.size, 100);
        assert_eq!(entry.nlink, 1);
    }

    #[test]
    fn test_remove() {
        let cache = MetadataCache::new();
        let inode = cache.allocate_inode();
        let now = chrono::Utc::now().timestamp();
        cache.insert(CachedEntry {
            inode,
            parent: 1,
            name: "temp.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        assert!(cache.get_inode(inode).is_some());
        cache.remove(inode);
        assert!(cache.get_inode(inode).is_none());
    }

    #[test]
    fn test_list_children() {
        let cache = MetadataCache::new();
        let now = chrono::Utc::now().timestamp();

        for name in &["a.txt", "b.txt", "c.txt"] {
            let ino = cache.allocate_inode();
            cache.insert(CachedEntry {
                inode: ino,
                parent: 1,
                name: name.to_string(),
                is_dir: false,
                is_symlink: false,
                symlink_target: None,
                nlink: 1,
                fid: None,
                size: 0,
                mode: 0o644,
                uid: 0,
                gid: 0,
                atime: now,
                mtime: now,
                ctime: now,
                xattrs: HashMap::new(),
                chunks: Vec::new(),
                hard_link_id: String::new(),
                hard_link_counter: 0,
                content_size: 0,
                disk_size: 0,
                generation: 0,
                placement: None,
                reliability: powerfs_layout::reliability::Reliability::default(),
                replica_chunks: Vec::new(),
                shard_id: None,
                cached_at: Instant::now(),
                state: EntryState::default(),
                hold: HoldState::default(),
                cap: None,
                dentry_lease: None,
                dir_shared_gen: 0,
            });
        }

        let children = cache.list_children(1);
        assert_eq!(children.len(), 3);
    }

    #[test]
    fn test_update_size() {
        let cache = MetadataCache::new();
        let inode = cache.allocate_inode();
        let now = chrono::Utc::now().timestamp();
        cache.insert(CachedEntry {
            inode,
            parent: 1,
            name: "file.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        cache.update_size(inode, 1024);
        let entry = cache.get_inode(inode).unwrap();
        assert_eq!(entry.size, 1024);
    }

    #[test]
    fn test_rename_file() {
        let cache = MetadataCache::new();
        let now = chrono::Utc::now().timestamp();
        let ino = cache.allocate_inode();
        cache.insert(CachedEntry {
            inode: ino,
            parent: 1,
            name: "old.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 100,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        cache.rename(1, "old.txt", 1, "new.txt").unwrap();

        assert!(cache.lookup_in_cache(1, "old.txt").is_none());
        let entry = cache.lookup_in_cache(1, "new.txt").unwrap();
        assert_eq!(entry.inode, ino);
        assert_eq!(entry.name, "new.txt");
    }

    #[test]
    fn test_nlink() {
        let cache = MetadataCache::new();
        let inode = cache.allocate_inode();
        let now = chrono::Utc::now().timestamp();
        cache.insert(CachedEntry {
            inode,
            parent: 1,
            name: "file.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        assert_eq!(cache.get_nlink(inode), 1);
        cache.inc_nlink(inode);
        assert_eq!(cache.get_nlink(inode), 2);
        assert!(!cache.dec_nlink(inode));
        assert_eq!(cache.get_nlink(inode), 1);
        assert!(cache.dec_nlink(inode));
        assert_eq!(cache.get_nlink(inode), 0);
    }

    #[test]
    fn test_symlink() {
        let cache = MetadataCache::new();
        let inode = cache.allocate_inode();
        let now = chrono::Utc::now().timestamp();
        cache.insert(CachedEntry {
            inode,
            parent: 1,
            name: "link".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o777,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        cache.set_symlink_target(inode, "/target/path".to_string());
        let entry = cache.get_inode(inode).unwrap();
        assert!(entry.is_symlink);
        assert_eq!(entry.size, 12);
        assert_eq!(
            cache.get_symlink_target(inode),
            Some("/target/path".to_string())
        );
    }

    #[test]
    fn test_metadata_ttl_expiry() {
        // Use a short TTL (1s) to keep the test fast
        let cache = MetadataCache::with_capacity_and_ttl(100, Duration::from_secs(1));
        let inode = cache.allocate_inode();
        let now = chrono::Utc::now().timestamp();
        cache.insert(CachedEntry {
            inode,
            parent: 1,
            name: "ttl-file.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        // Fresh entry should be returned
        assert!(cache.get_inode(inode).is_some());

        // Wait for TTL (1s) to expire
        std::thread::sleep(Duration::from_secs(2));

        // Now the entry should be treated as stale -> None
        assert!(
            cache.get_inode(inode).is_none(),
            "TTL-expired entry should be treated as stale"
        );
    }

    #[test]
    fn test_metadata_ttl_refresh_on_update() {
        // Use a short TTL (2s) to keep the test fast
        let cache = MetadataCache::with_capacity_and_ttl(100, Duration::from_secs(2));
        let inode = cache.allocate_inode();
        let now = chrono::Utc::now().timestamp();
        cache.insert(CachedEntry {
            inode,
            parent: 1,
            name: "refresh-file.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        // Wait some time (but less than TTL)
        std::thread::sleep(Duration::from_millis(500));

        // Refresh the TTL via update_size
        cache.update_size(inode, 4096);

        // Wait for original TTL to have elapsed, but refreshed entry should still be fresh
        std::thread::sleep(Duration::from_millis(1700));

        assert!(
            cache.get_inode(inode).is_some(),
            "Refresh on update_size should keep entry fresh"
        );
    }
}

#[derive(Debug, Clone)]
pub struct ChunkData {
    pub data: Bytes, // Bytes enables O(1) clone (reference count) in flush path
    pub offset: u64,
    pub size: u64,
    pub mtime: u64,
    pub crc32: u32,
    pub dirty: bool,
}

const NUM_SHARDS: usize = 16;
type ShardMap = HashMap<(u64, u64), ChunkData>;

pub struct ChunkCache {
    shards: Vec<RwLock<ShardMap>>,
    chunk_size: u64,
    max_bytes: usize,
    current_bytes: AtomicU64,
}

fn shard_idx(key: &(u64, u64)) -> usize {
    let hash = key.0.wrapping_add(key.1);
    (hash as usize) % NUM_SHARDS
}

impl ChunkCache {
    pub fn new(chunk_size: u64, max_chunks: usize) -> Self {
        let max_bytes = chunk_size as usize * max_chunks;
        Self::with_shards(chunk_size, max_bytes)
    }

    pub fn with_max_bytes(chunk_size: u64, max_bytes: usize) -> Self {
        Self::with_shards(chunk_size, max_bytes)
    }

    fn with_shards(chunk_size: u64, max_bytes: usize) -> Self {
        let shards = (0..NUM_SHARDS)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        ChunkCache {
            shards,
            chunk_size,
            max_bytes,
            current_bytes: AtomicU64::new(0),
        }
    }

    pub fn with_defaults() -> Self {
        // 512MB 缓存上限（2GB 容器内存下安全）
        ChunkCache::with_max_bytes(DEFAULT_CHUNK_SIZE, 512 * 1024 * 1024)
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub fn get_chunk_index(&self, offset: u64) -> u64 {
        offset / self.chunk_size
    }

    pub fn get_chunk_offset(&self, offset: u64) -> u64 {
        offset % self.chunk_size
    }

    pub fn get(&self, inode: u64, offset: u64) -> Option<ChunkData> {
        let chunk_index = self.get_chunk_index(offset);
        let key = (inode, chunk_index);
        let shard = shard_idx(&key);
        let cache = self.shards[shard].read().unwrap();
        cache.get(&key).cloned()
    }

    pub fn modify<F>(&self, inode: u64, offset: u64, f: F) -> bool
    where
        F: FnOnce(&mut ChunkData),
    {
        let chunk_index = self.get_chunk_index(offset);
        let key = (inode, chunk_index);
        let shard = shard_idx(&key);
        let mut cache = self.shards[shard].write().unwrap();
        if let Some(chunk) = cache.get_mut(&key) {
            let old_len = chunk.data.len() as u64;
            f(chunk);
            // Mark chunk as dirty after modification so the flusher knows it
            // needs to be written to the volume server, and evict_if_needed
            // won't evict it before the data is persisted. Without this, a
            // chunk that was previously flushed (dirty=false) and then modified
            // via modify() could be evicted with unflushed data.
            chunk.dirty = true;
            let new_len = chunk.data.len() as u64;
            if new_len != old_len {
                // Update current_bytes to reflect the size change caused by
                // the closure (e.g., resize). Without this, put/evict/remove
                // would later subtract the actual data.len() which differs
                // from what was originally added, causing current_bytes
                // underflow (wrapping to near u64::MAX) and breaking eviction.
                if new_len > old_len {
                    self.current_bytes
                        .fetch_add(new_len - old_len, Ordering::SeqCst);
                } else {
                    self.current_bytes
                        .fetch_sub(old_len - new_len, Ordering::SeqCst);
                }
            }
            true
        } else {
            false
        }
    }

    pub fn put(&self, inode: u64, offset: u64, data: Bytes, mtime: u64, crc32: u32) {
        let chunk_index = self.get_chunk_index(offset);
        let key = (inode, chunk_index);
        let shard = shard_idx(&key);
        let data_len = data.len();
        let needs_eviction;

        {
            let mut cache = self.shards[shard].write().unwrap();

            if let Some(old) = cache.get(&key) {
                let old_len = old.data.len() as u64;
                self.current_bytes.fetch_sub(old_len, Ordering::SeqCst);
            }

            cache.insert(
                key,
                ChunkData {
                    data,
                    offset: chunk_index * self.chunk_size,
                    size: self.chunk_size,
                    mtime,
                    crc32,
                    dirty: true,
                },
            );
            self.current_bytes
                .fetch_add(data_len as u64, Ordering::SeqCst);

            needs_eviction = self.max_bytes > 0
                && self.current_bytes.load(Ordering::SeqCst) > self.max_bytes as u64;
        }

        // Evict AFTER releasing the shard write lock to avoid cross-shard deadlock
        if needs_eviction {
            self.evict_if_needed();
        }
    }

    fn evict_if_needed(&self) {
        if self.max_bytes == 0 {
            return;
        }
        loop {
            if self.current_bytes.load(Ordering::SeqCst) <= self.max_bytes as u64 {
                break;
            }

            // Find the oldest non-dirty chunk across ALL shards.
            // Collect under read locks, then release before acquiring write lock
            // to avoid deadlock with concurrent shard write locks held by put/modify.
            let mut oldest: Option<(usize, (u64, u64))> = None;
            let mut oldest_mtime = u64::MAX;
            for (shard_idx, shard) in self.shards.iter().enumerate() {
                let cache = shard.read().unwrap();
                for (key, chunk) in cache.iter() {
                    if !chunk.dirty && chunk.mtime < oldest_mtime {
                        oldest_mtime = chunk.mtime;
                        oldest = Some((shard_idx, *key));
                    }
                }
            }

            if let Some((shard_idx, key)) = oldest {
                let shard = &self.shards[shard_idx];
                let mut cache = shard.write().unwrap();
                if let Some(removed) = cache.remove(&key) {
                    self.current_bytes
                        .fetch_sub(removed.data.len() as u64, Ordering::SeqCst);
                }
            } else {
                warn!(
                    "ChunkCache: cache full but all chunks are dirty, cannot evict. current_bytes={}, max_bytes={}",
                    self.current_bytes.load(Ordering::SeqCst),
                    self.max_bytes
                );
                break;
            }
        }
    }

    /// Evict the oldest NON-DIRTY chunks until the cache holds at most
    /// `target_bytes` bytes of resident data. Unlike `evict_if_needed` which
    /// only kicks in after `current > max_bytes`, this method is explicitly
    /// called by the write backpressure path after a flush pass so that the
    /// cache can drop from the middle region (target < current < max) back
    /// down to target — otherwise clean (read/pinned-after-flush) chunks
    /// permanently occupy the headroom, causing 64 futile flush iterations
    /// followed by a spurious BACKPRESSURE FAILURE → user-visible EIO on
    /// an otherwise healthy backend.
    ///
    /// Returns the number of bytes freed so callers can log progress.
    pub fn evict_clean_to(&self, target_bytes: u64) -> u64 {
        if self.max_bytes == 0 {
            return 0;
        }
        let mut freed: u64 = 0;
        loop {
            if self.current_bytes.load(Ordering::SeqCst) <= target_bytes {
                break;
            }

            let mut oldest: Option<(usize, (u64, u64))> = None;
            let mut oldest_mtime = u64::MAX;
            for (shard_idx, shard) in self.shards.iter().enumerate() {
                let cache = shard.read().unwrap();
                for (key, chunk) in cache.iter() {
                    if !chunk.dirty && chunk.mtime < oldest_mtime {
                        oldest_mtime = chunk.mtime;
                        oldest = Some((shard_idx, *key));
                    }
                }
            }

            if let Some((shard_idx, key)) = oldest {
                let shard = &self.shards[shard_idx];
                let mut cache = shard.write().unwrap();
                if let Some(removed) = cache.remove(&key) {
                    let sz = removed.data.len() as u64;
                    self.current_bytes.fetch_sub(sz, Ordering::SeqCst);
                    freed = freed.saturating_add(sz);
                }
            } else {
                warn!(
                    "ChunkCache evict_clean_to: no clean chunks available, \
                     cannot drop from current={} to target={}. All resident \
                     chunks are dirty; backpressure depends on flusher progress.",
                    self.current_bytes.load(Ordering::SeqCst),
                    target_bytes
                );
                break;
            }
        }
        freed
    }

    /// 移除指定 inode 中 chunk offset >= max_offset 的所有 chunk（用于 truncate）
    pub fn remove_after(&self, inode: u64, max_offset: u64) {
        let first_to_remove = if max_offset == 0 {
            0
        } else {
            (max_offset - 1) / self.chunk_size + 1
        };

        for shard in self.shards.iter() {
            let mut cache = shard.write().unwrap();
            let keys_to_remove: Vec<_> = cache
                .iter()
                .filter(|((ino, chunk_idx), _)| *ino == inode && *chunk_idx >= first_to_remove)
                .map(|(k, v)| (*k, v.data.len() as u64))
                .collect();
            for (key, data_len) in keys_to_remove {
                cache.remove(&key);
                self.current_bytes.fetch_sub(data_len, Ordering::SeqCst);
            }
        }
    }

    /// 获取当前缓存总字节数
    pub fn current_bytes(&self) -> u64 {
        self.current_bytes.load(Ordering::SeqCst)
    }

    /// 获取最大缓存字节数
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn remove(&self, inode: u64) {
        for shard in self.shards.iter() {
            let mut cache = shard.write().unwrap();
            let keys_to_remove: Vec<_> = cache
                .iter()
                .filter(|((ino, _), _)| *ino == inode)
                .map(|(k, v)| (*k, v.data.len() as u64))
                .collect();
            for (key, data_len) in keys_to_remove {
                cache.remove(&key);
                self.current_bytes.fetch_sub(data_len, Ordering::SeqCst);
            }
        }
    }

    pub fn remove_chunk(&self, inode: u64, offset: u64) {
        let chunk_index = self.get_chunk_index(offset);
        let key = (inode, chunk_index);
        let shard = shard_idx(&key);
        let mut cache = self.shards[shard].write().unwrap();
        if let Some(removed) = cache.remove(&key) {
            self.current_bytes
                .fetch_sub(removed.data.len() as u64, Ordering::SeqCst);
        }
    }

    pub fn remove_inode_chunks(&self, inode: u64) {
        self.remove(inode);
    }

    /// Truncate chunks for an inode to `new_size`.
    /// - Chunks entirely beyond `new_size` are removed.
    /// - Chunks that straddle `new_size` are truncated (data beyond new_size is discarded).
    /// - Chunks entirely within `new_size` are preserved (including dirty data).
    ///
    /// This prevents stale data from being read after truncate-down + truncate-up sequences.
    pub fn truncate_chunks(&self, inode: u64, new_size: u64) {
        let chunk_size = self.chunk_size;
        for shard in self.shards.iter() {
            let mut cache = shard.write().unwrap();
            let entries: Vec<_> = cache
                .iter()
                .filter(|((ino, _), _)| *ino == inode)
                .map(|(k, v)| (*k, v.data.len()))
                .collect();
            for (key, data_len) in entries {
                let chunk_offset = key.1 * chunk_size;
                if chunk_offset >= new_size {
                    // Chunk entirely beyond new_size — remove
                    if let Some(removed) = cache.remove(&key) {
                        self.current_bytes
                            .fetch_sub(removed.data.len() as u64, Ordering::SeqCst);
                    }
                } else if chunk_offset + data_len as u64 > new_size {
                    // Chunk straddles new_size — truncate data
                    let keep_len = (new_size - chunk_offset) as usize;
                    if let Some(chunk) = cache.get_mut(&key) {
                        let old_len = chunk.data.len();
                        chunk.data.truncate(keep_len);
                        let freed = old_len - keep_len;
                        if freed > 0 {
                            self.current_bytes.fetch_sub(freed as u64, Ordering::SeqCst);
                        }
                    }
                }
                // Chunks entirely within new_size are kept as-is
            }
        }
    }

    /// Check if the inode has any dirty (unflushed) chunks.
    /// Used by open() to decide whether it's safe to clear the ChunkCache:
    /// if dirty chunks exist, clearing would lose unflushed data.
    pub fn has_dirty_chunks(&self, inode: u64) -> bool {
        for shard in self.shards.iter() {
            let cache = shard.read().unwrap();
            for ((ino, _), chunk) in cache.iter() {
                if *ino == inode && chunk.dirty {
                    return true;
                }
            }
        }
        false
    }

    /// Check if the inode has ANY chunks (dirty or clean) in the cache.
    /// Used by open()'s unsynced-write guard to distinguish between:
    /// - Local data that was flushed but not yet committed to the Filer
    ///   (has_chunks=true, has_dirty=false → guard applies)
    /// - Stale cache after an Invalidate notification invalidated both
    ///   metadata and chunk caches (has_chunks=false → guard must NOT
    ///   apply, the Filer's value is authoritative)
    pub fn has_chunks(&self, inode: u64) -> bool {
        for shard in self.shards.iter() {
            let cache = shard.read().unwrap();
            for (ino, _) in cache.keys() {
                if *ino == inode {
                    return true;
                }
            }
        }
        false
    }

    /// 清除指定 inode 的所有脏标记（flush 完成后调用）
    pub fn clear_dirty(&self, inode: u64) {
        for shard in self.shards.iter() {
            let mut cache = shard.write().unwrap();
            for ((ino, _), chunk) in cache.iter_mut() {
                if *ino == inode {
                    chunk.dirty = false;
                }
            }
        }
    }

    /// 清除指定 inode 中特定 chunk 的脏标记。
    /// 在 flush_dirty_chunks_impl 成功写入 volume server 后调用，
    /// 使这些 chunk 可被 evict_if_needed 驱逐。
    /// 只清除传入的 chunk_idx 对应的 chunk，不影响同 inode 的其他 chunk。
    pub fn clear_dirty_for_chunks(&self, inode: u64, chunk_indices: &[u64]) {
        for shard in self.shards.iter() {
            let mut cache = shard.write().unwrap();
            for ((ino, idx), chunk) in cache.iter_mut() {
                if *ino == inode && chunk_indices.contains(idx) {
                    chunk.dirty = false;
                }
            }
        }
    }

    pub fn dirty_chunks(&self) -> u64 {
        let mut count: u64 = 0;
        for shard in &self.shards {
            let cache = shard.read().unwrap();
            count += cache.values().filter(|chunk| chunk.dirty).count() as u64;
        }
        count
    }

    pub fn dirty_bytes(&self) -> u64 {
        let mut total: u64 = 0;
        for shard in &self.shards {
            let cache = shard.read().unwrap();
            total += cache
                .values()
                .filter(|chunk| chunk.dirty)
                .map(|chunk| chunk.data.len() as u64)
                .sum::<u64>();
        }
        total
    }

    pub fn clear(&self) {
        for shard in self.shards.iter() {
            let mut cache = shard.write().unwrap();
            cache.clear();
        }
        self.current_bytes.store(0, Ordering::SeqCst);
    }

    pub fn len(&self) -> usize {
        let mut total: usize = 0;
        for shard in &self.shards {
            let cache = shard.read().unwrap();
            total += cache.len();
        }
        total
    }

    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.read().unwrap().is_empty())
    }

    pub fn prefetch(&self, inode: u64, start_offset: u64, end_offset: u64) -> Vec<(u64, u64)> {
        let start_chunk = self.get_chunk_index(start_offset);
        let end_chunk = if end_offset == 0 {
            0
        } else {
            self.get_chunk_index(end_offset - 1)
        };
        let mut missing = Vec::new();

        for chunk_index in start_chunk..=end_chunk {
            let key = (inode, chunk_index);
            let shard = shard_idx(&key);
            let cache = self.shards[shard].read().unwrap();
            if !cache.contains_key(&key) {
                missing.push((chunk_index * self.chunk_size, self.chunk_size));
            }
        }

        missing
    }
}

impl Default for ChunkCache {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod cap_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_cap_grant_take_and_get() {
        let cache = MetadataCache::new();
        let inode = 100u64;
        let now = chrono::Utc::now().timestamp();
        cache.insert(CachedEntry {
            inode,
            parent: 1,
            name: "cap_test.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        // No cap initially.
        assert!(cache.get_cap(inode).is_none());
        assert!(!cache.can_cache_writes(inode));
        assert!(!cache.can_cache_reads(inode));

        // Grant EXCLUSIVE cap (write open).
        let cap = crate::client_cap::ClientCap::new(
            1,
            "tok-1".into(),
            crate::client_cap::CapSet::EXCLUSIVE,
            1,
            true,
            100,
        );
        cache.grant_cap(inode, cap);

        // Cap is now present.
        let got = cache.get_cap(inode).unwrap();
        assert!(got.can_cache_writes());
        assert!(got.can_cache_reads());
        assert!(got.can_modify_meta());
        assert!(cache.can_cache_writes(inode));
        assert!(cache.can_cache_reads(inode));

        // Take cap (release).
        let taken = cache.take_cap(inode).unwrap();
        assert_eq!(taken.token, "tok-1");
        assert!(cache.get_cap(inode).is_none());
        assert!(!cache.can_cache_writes(inode));
    }

    #[test]
    fn test_cap_mark_dirty_and_flushed() {
        let cache = MetadataCache::new();
        let inode = 200u64;
        let now = chrono::Utc::now().timestamp();
        cache.insert(CachedEntry {
            inode,
            parent: 1,
            name: "dirty_test.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        let cap = crate::client_cap::ClientCap::new(
            2,
            "tok-2".into(),
            crate::client_cap::CapSet::EXCLUSIVE,
            1,
            true,
            200,
        );
        cache.grant_cap(inode, cap);

        // Write path: mark_dirty_cap_w.
        cache.mark_dirty_cap_w(inode);
        let got = cache.get_cap(inode).unwrap();
        assert!(got.dirty_caps.contains(crate::client_cap::CapSet::CAP_W));
        // Entry state should be Dirty.
        let entry = cache.get_inode(inode).unwrap();
        assert_eq!(entry.state, EntryState::Dirty);

        // Flush: mark_cap_flushed clears flushing_caps (after recall
        // moves dirty→flushing).
        cache.mark_cap_flushed(inode);
        let got = cache.get_cap(inode).unwrap();
        assert!(got.flushing_caps.is_empty());
    }

    #[test]
    fn test_cap_get_inode_ttl_bypass_with_cap_r() {
        let cache = MetadataCache::new();
        let inode = 300u64;
        let now = chrono::Utc::now().timestamp();
        // Insert with an old cached_at to simulate TTL expiry.
        let old_instant = Instant::now() - std::time::Duration::from_secs(300);
        cache.insert(CachedEntry {
            inode,
            parent: 1,
            name: "ttl_test.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 42,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 42,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: old_instant,
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        // Without cap: TTL expired → get_inode returns None.
        assert!(cache.get_inode(inode).is_none());

        // Grant CAP_R cap.
        let cap = crate::client_cap::ClientCap::new(
            3,
            "tok-3".into(),
            crate::client_cap::CapSet::CAP_R,
            1,
            false,
            300,
        );
        cache.grant_cap(inode, cap);

        // With CAP_R: TTL bypassed → get_inode returns Some.
        let entry = cache.get_inode(inode).unwrap();
        assert_eq!(entry.size, 42);
    }

    #[test]
    fn test_cap_grant_on_nonexistent_inode_drops() {
        let cache = MetadataCache::new();
        let cap = crate::client_cap::ClientCap::new(
            4,
            "tok-4".into(),
            crate::client_cap::CapSet::EXCLUSIVE,
            1,
            true,
            400,
        );
        // Grant on an inode not in cache — should not panic.
        cache.grant_cap(999, cap);
        assert!(cache.get_cap(999).is_none());
    }

    #[test]
    fn test_cap_with_cap_mut_recall_flow() {
        let cache = MetadataCache::new();
        let inode = 500u64;
        let now = chrono::Utc::now().timestamp();
        cache.insert(CachedEntry {
            inode,
            parent: 1,
            name: "recall_test.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        let cap = crate::client_cap::ClientCap::new(
            5,
            "tok-5".into(),
            crate::client_cap::CapSet::EXCLUSIVE,
            1,
            true,
            500,
        );
        cache.grant_cap(inode, cap);

        // Simulate write (mark dirty) then recall.
        cache.mark_dirty_cap_w(inode);

        let action = cache.with_cap_mut(inode, |cap| {
            crate::client_cap::process_recall(cap, crate::client_cap::CapSet::CAP_R, 2)
        });
        assert!(action.is_some());
        match action.unwrap() {
            crate::client_cap::RecallAction::FlushThenAck { flushing_caps } => {
                assert!(flushing_caps.contains(crate::client_cap::CapSet::CAP_W));
            }
            _ => panic!("expected FlushThenAck"),
        }

        // After recall, CAP_W is gone, CAP_R retained.
        let got = cache.get_cap(inode).unwrap();
        assert!(!got.can_cache_writes());
        assert!(got.can_cache_reads());
    }

    /// Helper: build a CachedEntry with the given inode, name, and state.
    fn make_entry_with_state(inode: u64, name: &str, state: EntryState) -> CachedEntry {
        let now = chrono::Utc::now().timestamp();
        CachedEntry {
            inode,
            parent: 1,
            name: name.to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state,
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        }
    }

    /// Helper: grant an EXCLUSIVE cap on `inode` with token `tok`.
    fn grant_exclusive_cap(cache: &MetadataCache, inode: u64, tok: &str) {
        let cap = crate::client_cap::ClientCap::new(
            1,
            tok.into(),
            crate::client_cap::CapSet::EXCLUSIVE,
            1,
            true,
            100,
        );
        cache.grant_cap(inode, cap);
    }

    /// #4 重连 cap 恢复 — invalidate_all 必须清除 Clean entry 的 cap 字段。
    ///
    /// 场景：客户端持有 cap，Filer leader 切换 → cache_epoch 变化 →
    /// `check_cache_epoch` 调用 `invalidate_all`。Clean entry 的 cap 字段
    /// 必须被清，否则后续 `take_cap` 返回 stale token，cap_release RPC
    /// 在新 Filer 上失败。
    #[test]
    fn test_invalidate_all_clears_cap_on_clean_entry() {
        let cache = MetadataCache::new();
        let inode = 600u64;
        cache.insert(make_entry_with_state(inode, "clean.txt", EntryState::Clean));
        grant_exclusive_cap(&cache, inode, "tok-clean");

        // Pre-condition: cap is present.
        assert!(cache.get_cap(inode).is_some());

        cache.invalidate_all();

        // Cap must be cleared — take_cap returns None (no stale RPC).
        assert!(
            cache.take_cap(inode).is_none(),
            "invalidate_all must clear cap on Clean entry to avoid stale cap_release"
        );
        // And get_cap also returns None.
        assert!(cache.get_cap(inode).is_none());
    }

    /// #4 重连 cap 恢复 — invalidate_all 必须清除 Dirty entry 的 cap 字段。
    ///
    /// 场景：写者持 EXCLUSIVE cap，write 后 mark_dirty_cap_w（state=Dirty），
    /// 此时 Filer leader 切换 → invalidate_all。Dirty 状态的 entry 本身
    /// 保留（数据待 sync），但 cap 字段必须清：Filer 端 cap_manager
    /// 已 reset，stale token 100% 无效。dirty 数据在 ChunkCache，不受
    /// 影响；下次 release 走 sync_size_chunks_on_close 重写到新 Filer。
    #[test]
    fn test_invalidate_all_clears_cap_on_dirty_entry() {
        let cache = MetadataCache::new();
        let inode = 601u64;
        // Insert as Clean, then grant cap, then mark dirty via write path.
        cache.insert(make_entry_with_state(inode, "dirty.txt", EntryState::Clean));
        grant_exclusive_cap(&cache, inode, "tok-dirty");
        cache.mark_dirty_cap_w(inode);

        // Pre-conditions: cap present, entry is Dirty, CAP_W is dirty.
        assert!(cache.get_cap(inode).is_some());
        assert_eq!(
            cache.peek_inode(inode).map(|e| e.state).unwrap(),
            EntryState::Dirty
        );
        let cap_before = cache.get_cap(inode).unwrap();
        assert!(cap_before.dirty_caps.contains(crate::client_cap::CapSet::CAP_W));

        cache.invalidate_all();

        // Cap must be cleared even on Dirty entry.
        assert!(
            cache.take_cap(inode).is_none(),
            "invalidate_all must clear cap on Dirty entry — Filer cap_manager reset on leader switch"
        );

        // Dirty entry itself is preserved (local authoritative data).
        let entry = cache.peek_inode(inode).expect("Dirty entry must be preserved");
        assert_eq!(entry.state, EntryState::Dirty);
    }

    /// #4 重连 cap 恢复 — invalidate_all 必须清除 Flushing entry 的 cap 字段。
    ///
    /// 场景：写者 flush 中（state=Flushing，flushing_caps 非空），Filer
    /// leader 切换。Flushing entry 保留，但 cap 必须清：之前发的 flush
    /// RPC 不会收到新 Filer 的 ACK，stale flushing_caps 字段没有意义。
    #[test]
    fn test_invalidate_all_clears_cap_on_flushing_entry() {
        let cache = MetadataCache::new();
        let inode = 602u64;
        cache.insert(make_entry_with_state(inode, "flushing.txt", EntryState::Clean));
        grant_exclusive_cap(&cache, inode, "tok-flush");
        // mark_dirty_cap_w sets Dirty + CAP_W dirty, then mark_flushing
        // transitions to Flushing.
        cache.mark_dirty_cap_w(inode);
        cache.mark_flushing(inode);

        // Pre-condition: entry is Flushing with cap present.
        assert_eq!(
            cache.peek_inode(inode).map(|e| e.state).unwrap(),
            EntryState::Flushing
        );
        assert!(cache.get_cap(inode).is_some());

        cache.invalidate_all();

        // Cap cleared on Flushing entry.
        assert!(
            cache.take_cap(inode).is_none(),
            "invalidate_all must clear cap on Flushing entry"
        );

        // Flushing entry itself is preserved.
        let entry = cache.peek_inode(inode).expect("Flushing entry must be preserved");
        assert_eq!(entry.state, EntryState::Flushing);
    }

    /// #4 重连 cap 恢复 — invalidate_all 清除多个 inode 的 cap。
    ///
    /// 覆盖混合场景：3 个 inode（Clean/Dirty/Flushing）+ 1 个无 cap 的
    /// Clean inode。验证所有有 cap 的 entry 都被清，无 cap 的 entry
    /// 不出错。
    #[test]
    fn test_invalidate_all_clears_caps_across_mixed_entries() {
        let cache = MetadataCache::new();

        // Inode 700: Clean with cap.
        cache.insert(make_entry_with_state(700, "a.txt", EntryState::Clean));
        grant_exclusive_cap(&cache, 700, "tok-a");

        // Inode 701: Dirty with cap.
        cache.insert(make_entry_with_state(701, "b.txt", EntryState::Clean));
        grant_exclusive_cap(&cache, 701, "tok-b");
        cache.mark_dirty_cap_w(701);

        // Inode 702: Clean WITHOUT cap (cap was never granted).
        cache.insert(make_entry_with_state(702, "c.txt", EntryState::Clean));

        // Inode 703: Flushing with cap.
        cache.insert(make_entry_with_state(703, "d.txt", EntryState::Clean));
        grant_exclusive_cap(&cache, 703, "tok-d");
        cache.mark_dirty_cap_w(703);
        cache.mark_flushing(703);

        // Pre-conditions.
        assert!(cache.get_cap(700).is_some());
        assert!(cache.get_cap(701).is_some());
        assert!(cache.get_cap(702).is_none());
        assert!(cache.get_cap(703).is_some());

        cache.invalidate_all();

        // All caps cleared.
        assert!(cache.take_cap(700).is_none(), "inode 700 cap must be cleared");
        assert!(cache.take_cap(701).is_none(), "inode 701 cap must be cleared");
        assert!(cache.take_cap(702).is_none(), "inode 702 had no cap, still None");
        assert!(cache.take_cap(703).is_none(), "inode 703 cap must be cleared");

        // Dirty/Flushing entries preserved; Clean entries' state is now Stale.
        assert_eq!(
            cache.peek_inode(700).map(|e| e.state).unwrap(),
            EntryState::Stale,
            "Clean entry → Stale after invalidate_all"
        );
        assert_eq!(
            cache.peek_inode(701).map(|e| e.state).unwrap(),
            EntryState::Dirty,
            "Dirty entry preserved"
        );
        assert_eq!(
            cache.peek_inode(702).map(|e| e.state).unwrap(),
            EntryState::Stale,
            "Clean entry without cap → Stale"
        );
        assert_eq!(
            cache.peek_inode(703).map(|e| e.state).unwrap(),
            EntryState::Flushing,
            "Flushing entry preserved"
        );
    }
}

#[cfg(test)]
mod chunk_cache_tests {
    use super::*;

    #[test]
    fn test_chunk_cache_basic() {
        let cache = ChunkCache::new(1024, 10);
        let inode = 100;

        assert!(cache.get(inode, 0).is_none());

        cache.put(inode, 0, vec![0u8; 1024].into(), 1234567890, 0);
        let chunk = cache.get(inode, 0).unwrap();
        assert_eq!(chunk.data.len(), 1024);
        assert_eq!(chunk.offset, 0);
        assert_eq!(chunk.mtime, 1234567890);
    }

    #[test]
    fn test_chunk_cache_remove() {
        let cache = ChunkCache::new(1024, 10);
        let inode = 100;

        cache.put(inode, 0, vec![0u8; 1024].into(), 1234567890, 0);
        cache.put(inode, 1024, vec![1u8; 1024].into(), 1234567891, 1);

        assert!(cache.get(inode, 0).is_some());
        assert!(cache.get(inode, 1024).is_some());

        cache.remove(inode);

        assert!(cache.get(inode, 0).is_none());
        assert!(cache.get(inode, 1024).is_none());
    }

    #[test]
    fn test_chunk_cache_prefetch() {
        let cache = ChunkCache::new(1024, 10);
        let inode = 100;

        cache.put(inode, 0, vec![0u8; 1024].into(), 1234567890, 0);

        let missing = cache.prefetch(inode, 0, 3072);
        assert_eq!(missing.len(), 2);
        assert_eq!(missing[0], (1024, 1024));
        assert_eq!(missing[1], (2048, 1024));
    }

    #[test]
    fn test_chunk_index() {
        let cache = ChunkCache::new(1024, 10);

        assert_eq!(cache.get_chunk_index(0), 0);
        assert_eq!(cache.get_chunk_index(512), 0);
        assert_eq!(cache.get_chunk_index(1024), 1);
        assert_eq!(cache.get_chunk_index(1536), 1);
        assert_eq!(cache.get_chunk_index(2048), 2);

        assert_eq!(cache.get_chunk_offset(0), 0);
        assert_eq!(cache.get_chunk_offset(512), 512);
        assert_eq!(cache.get_chunk_offset(1024), 0);
        assert_eq!(cache.get_chunk_offset(1536), 512);
    }

    #[test]
    fn test_chunk_cache_lru_eviction() {
        // max_bytes = 2048，只能容纳 2 个 1024 字节 chunk
        let cache = ChunkCache::with_max_bytes(1024, 2048);
        let inode = 100;

        // 放入 2 个 chunk（刚好填满）
        cache.put(inode, 0, vec![0u8; 1024].into(), 1000, 0); // mtime=1000
        cache.put(inode, 1024, vec![1u8; 1024].into(), 2000, 1); // mtime=2000
        assert_eq!(cache.current_bytes(), 2048);

        // 清除脏标记（模拟 flush 完成后的状态）
        cache.clear_dirty(inode);

        // 放入第 3 个 chunk 触发 LRU 淘汰
        cache.put(inode, 2048, vec![2u8; 1024].into(), 3000, 2); // mtime=3000

        // 最旧的（mtime=1000）应被淘汰，剩下较新的两个
        assert!(
            cache.get(inode, 0).is_none(),
            "oldest chunk should be evicted"
        );
        assert!(cache.get(inode, 1024).is_some());
        assert!(cache.get(inode, 2048).is_some());

        // 当前字节数不超过 max_bytes
        assert!(cache.current_bytes() <= 2048);
    }

    #[test]
    fn test_chunk_cache_remove_after_truncate() {
        let cache = ChunkCache::new(1024, 100);
        let inode = 100;

        // 放入 4 个 chunk（offset 0, 1024, 2048, 3072）
        cache.put(inode, 0, vec![0u8; 1024].into(), 1000, 0);
        cache.put(inode, 1024, vec![1u8; 1024].into(), 1000, 1);
        cache.put(inode, 2048, vec![2u8; 1024].into(), 1000, 2);
        cache.put(inode, 3072, vec![3u8; 1024].into(), 1000, 3);

        // truncate 到 2048：移除 offset >= 2048 的 chunk
        cache.remove_after(inode, 2048);

        assert!(cache.get(inode, 0).is_some(), "chunk at 0 should remain");
        assert!(
            cache.get(inode, 1024).is_some(),
            "chunk at 1024 should remain"
        );
        assert!(
            cache.get(inode, 2048).is_none(),
            "chunk at 2048 should be removed"
        );
        assert!(
            cache.get(inode, 3072).is_none(),
            "chunk at 3072 should be removed"
        );
    }

    #[test]
    fn test_chunk_cache_byte_tracking() {
        let cache = ChunkCache::with_max_bytes(1024, 10240);
        let inode = 100;

        assert_eq!(cache.current_bytes(), 0);

        cache.put(inode, 0, vec![0u8; 512].into(), 1000, 0);
        assert_eq!(cache.current_bytes(), 512);

        cache.put(inode, 1024, vec![1u8; 256].into(), 1000, 1);
        assert_eq!(cache.current_bytes(), 768);

        // 替换已有 chunk（512 → 1024）
        cache.put(inode, 0, vec![2u8; 1024].into(), 1000, 0);
        assert_eq!(cache.current_bytes(), 1280); // 1024 + 256

        cache.remove(inode);
        assert_eq!(cache.current_bytes(), 0);
    }

    #[test]
    fn test_path_generation_update_and_get() {
        let cache = MetadataCache::new();

        assert!(cache.get_path_generation("/test/file.txt").is_none());

        cache.update_path_generation("/test/file.txt", 5);
        assert_eq!(cache.get_path_generation("/test/file.txt"), Some(5));

        cache.update_path_generation("/test/file.txt", 10);
        assert_eq!(cache.get_path_generation("/test/file.txt"), Some(10));
    }

    #[test]
    fn test_path_generation_stale_detection() {
        let cache = MetadataCache::new();
        let ino = cache.allocate_inode();

        cache.insert(CachedEntry {
            inode: ino,
            parent: 1,
            name: "stale.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 1,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        // No generation tracking -> not stale
        assert!(cache.get_path_generation("/stale.txt").is_none());

        // Updated generation > cached generation -> stale
        cache.update_path_generation("/stale.txt", 5);
        let cached_gen = cache.get_inode(ino).unwrap().generation;
        assert!(cache
            .get_path_generation("/stale.txt")
            .is_some_and(|g| g > cached_gen));

        // Same generation -> not stale
        cache.update_path_generation("/stale.txt", 1);
        assert!(cache
            .get_path_generation("/stale.txt")
            .is_none_or(|g| g <= cached_gen));
    }

    #[test]
    fn test_clear_all_empties_and_reinitializes() {
        let cache = MetadataCache::new();
        let ino = cache.allocate_inode();

        cache.insert(CachedEntry {
            inode: ino,
            parent: 1,
            name: "clear_test.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 1,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        cache.update_path_generation("/clear_test.txt", 5);

        assert!(cache.get_inode(ino).is_some());

        cache.clear_all();

        assert!(cache.get_inode(ino).is_none(), "Entry should be cleared");
        assert!(
            cache.get_inode(1).is_some(),
            "Root should be re-initialized"
        );
        assert!(
            cache.get_path_generation("/clear_test.txt").is_none(),
            "Generation tracking should be cleared"
        );
    }

    /// Phase 4: Flushing→Dirty transition on flush failure.
    /// Verifies that after a failed flush, the entry state recovers to Dirty
    /// (not stuck in Flushing), so subsequent flush cycles and invalidate
    /// handling treat it as having local authoritative data.
    #[test]
    fn test_state_transition_flushing_to_dirty_on_failure() {
        let cache = MetadataCache::new();
        let ino = cache.allocate_inode();
        cache.insert(CachedEntry {
            inode: ino,
            parent: 1,
            name: "flush_fail.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 1,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        // New → Dirty (write path)
        cache.mark_dirty(ino);
        assert_eq!(cache.get_entry_state(ino).unwrap(), EntryState::Dirty);

        // Dirty → Flushing (flusher starts RPC)
        cache.mark_flushing(ino);
        assert_eq!(cache.get_entry_state(ino).unwrap(), EntryState::Flushing);

        // Flushing → Dirty (flush failure recovery, Phase 4)
        cache.mark_dirty(ino);
        assert_eq!(
            cache.get_entry_state(ino).unwrap(),
            EntryState::Dirty,
            "Flushing→Dirty on failure must recover to Dirty, not stay Flushing"
        );

        // Dirty → Flushing → Clean (successful flush path)
        cache.mark_flushing(ino);
        cache.mark_clean(ino);
        assert_eq!(cache.get_entry_state(ino).unwrap(), EntryState::Clean);
    }

    /// Phase 4: concurrent write during Flushing keeps entry Dirty.
    /// Verifies that a write (mark_dirty) during Flushing transitions to Dirty,
    /// and the subsequent mark_clean is REJECTED (Dirty→Clean not allowed),
    /// preventing data loss when a concurrent write happens during flush.
    #[test]
    fn test_concurrent_write_during_flushing_keeps_dirty() {
        let cache = MetadataCache::new();
        let ino = cache.allocate_inode();
        cache.insert(CachedEntry {
            inode: ino,
            parent: 1,
            name: "concurrent.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 1,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        // Write → Dirty → Flushing (flusher starts)
        cache.mark_dirty(ino);
        cache.mark_flushing(ino);
        assert_eq!(cache.get_entry_state(ino).unwrap(), EntryState::Flushing);

        // Concurrent write during Flushing: Flushing → Dirty (allowed)
        cache.mark_dirty(ino);
        assert_eq!(cache.get_entry_state(ino).unwrap(), EntryState::Dirty);

        // Flusher completes RPC, calls mark_clean: Dirty → Clean REJECTED
        cache.mark_clean(ino);
        assert_eq!(
            cache.get_entry_state(ino).unwrap(),
            EntryState::Dirty,
            "Dirty→Clean must be rejected: concurrent write made entry Dirty during flush"
        );
    }

    /// Phase 3: entry.hold is the authoritative source for pin status.
    /// Verifies that pin_inode/unpin_inode/is_pinned all operate on
    /// entry.hold directly (no separate pinned_inodes HashMap).
    #[test]
    fn test_pin_unpin_via_entry_hold() {
        let cache = MetadataCache::with_capacity_and_ttl(100, Duration::from_secs(60));
        let ino = cache.allocate_inode();
        cache.insert(CachedEntry {
            inode: ino,
            parent: 1,
            name: "pinned.txt".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 1,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        });

        // Initially not pinned
        assert!(!cache.is_pinned(ino));

        // Pin once (open)
        cache.pin_inode(ino);
        assert!(cache.is_pinned(ino));

        // Pin again (second open — reference counting)
        cache.pin_inode(ino);
        assert!(cache.is_pinned(ino));

        // Unpin once (first close — still pinned by second handle)
        cache.unpin_inode(ino);
        assert!(
            cache.is_pinned(ino),
            "should still be pinned after first unpin"
        );

        // Unpin again (second close — fully released)
        cache.unpin_inode(ino);
        assert!(
            !cache.is_pinned(ino),
            "should be unpinned after all handles closed"
        );
    }

    /// Phase 3: pinned inodes bypass TTL expiry via entry.hold.
    /// Verifies that a pinned inode survives TTL expiry, while an unpinned
    /// inode with the same TTL is expired.
    #[test]
    fn test_pinned_inode_bypasses_ttl_via_hold() {
        let cache = MetadataCache::with_capacity_and_ttl(100, Duration::from_millis(50));
        let pinned_ino = cache.allocate_inode();
        let unpinned_ino = cache.allocate_inode();

        let make_entry = |ino: u64| CachedEntry {
            inode: ino,
            parent: 1,
            name: format!("file_{}.txt", ino),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 1,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        };

        cache.insert(make_entry(pinned_ino));
        cache.insert(make_entry(unpinned_ino));

        // Pin one inode
        cache.pin_inode(pinned_ino);

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(100));

        // Pinned inode should still be retrievable
        assert!(
            cache.get_inode(pinned_ino).is_some(),
            "pinned inode should bypass TTL expiry"
        );
        // Unpinned inode should be expired
        assert!(
            cache.get_inode(unpinned_ino).is_none(),
            "unpinned inode should be expired by TTL"
        );
    }

    /// Phase 3: LRU eviction preserves pinned entries via entry.hold.
    /// Verifies that a pinned entry is re-inserted when evicted by LRU.
    #[test]
    fn test_lru_eviction_preserves_pinned_via_hold() {
        // Small cache to force eviction
        let cache = MetadataCache::with_capacity_and_ttl(3, Duration::from_secs(60));

        let make_entry = |ino: u64, name: &str| CachedEntry {
            inode: ino,
            parent: 1,
            name: name.to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 1,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        };

        // Insert 3 entries (fills cache: root=1, ino=2, ino=3, ino=4 → cap=3 means 3 entries after root)
        let ino_a = cache.allocate_inode(); // 2
        let ino_b = cache.allocate_inode(); // 3
        let ino_c = cache.allocate_inode(); // 4
        cache.insert(make_entry(ino_a, "a"));
        cache.insert(make_entry(ino_b, "b"));
        cache.insert(make_entry(ino_c, "c"));

        // Pin entry B (middle of LRU)
        cache.pin_inode(ino_b);

        // Insert a new entry to trigger eviction
        let ino_d = cache.allocate_inode(); // 5
        cache.insert(make_entry(ino_d, "d"));

        // Pinned entry B should still be in cache
        assert!(
            cache.peek_inode(ino_b).is_some(),
            "pinned entry should survive LRU eviction"
        );
        assert!(
            cache.is_pinned(ino_b),
            "pinned entry should still be marked as pinned after re-insertion"
        );
    }

    #[test]
    fn test_stale_state_returns_none_regardless_of_ttl() {
        // Phase 3: state machine is authoritative. A Stale entry must return
        // None from get_inode even if cached_at is fresh (TTL not expired).
        // This is the key fix — previously, mark_stale() set the state but
        // get_inode only checked TTL, so Stale entries were still served
        // until TTL expired.
        let cache = MetadataCache::with_capacity_and_ttl(100, Duration::from_secs(60));

        let make_entry = |ino: u64, name: &str| CachedEntry {
            inode: ino,
            parent: 1,
            name: name.to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 1,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        };

        let ino = cache.allocate_inode();
        cache.insert(make_entry(ino, "stale-test"));
        assert!(cache.get_inode(ino).is_some());

        // Mark Stale via mark_stale (used by InvalidateHandler for root inode)
        cache.mark_stale(ino);

        // get_inode must return None even though cached_at is fresh (60s TTL)
        assert!(
            cache.get_inode(ino).is_none(),
            "Stale entry must return None regardless of TTL"
        );

        // Entry is still physically in cache (peek bypasses state check)
        assert!(
            cache.peek_inode(ino).is_some(),
            "Stale entry should still be physically in cache"
        );
    }

    #[test]
    fn test_dirty_state_bypasses_ttl() {
        // Phase 3: Dirty entries have local authoritative data and must NOT
        // be expired by TTL, even if cached_at is very old.
        let cache = MetadataCache::with_capacity_and_ttl(100, Duration::from_millis(1));

        let make_entry = |ino: u64, name: &str| CachedEntry {
            inode: ino,
            parent: 1,
            name: name.to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 1,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        };

        let ino = cache.allocate_inode();
        cache.insert(make_entry(ino, "dirty-test"));

        // Mark as Dirty (has unsynced local modifications)
        cache.mark_dirty(ino);

        // Wait for TTL to expire (1ms)
        std::thread::sleep(Duration::from_millis(10));

        // Dirty entry must still be returned despite TTL expiry
        assert!(
            cache.get_inode(ino).is_some(),
            "Dirty entry must bypass TTL expiry"
        );
    }

    #[test]
    fn test_new_to_stale_transition_allowed() {
        // Phase 3: New→Stale is now allowed (added to transition matrix).
        // This is needed for invalidate_all() and the TTL safety net to
        // correctly mark unpopulated New entries as Stale.
        let mut entry = CachedEntry {
            inode: 42,
            parent: 1,
            name: "new-entry".to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 1,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::New,
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        };

        assert!(
            entry.try_transition(EntryState::Stale),
            "New→Stale should be allowed"
        );
        assert_eq!(entry.state, EntryState::Stale);
    }
}
