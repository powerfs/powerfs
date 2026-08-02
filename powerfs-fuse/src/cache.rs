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
pub const DEFAULT_CHUNK_SIZE: u64 = 2 * 1024 * 1024; // 2MB - balance performance and memory

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
    /// When this cache entry was populated (used for TTL fallback)
    pub cached_at: Instant,
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
    /// Pinned inodes (open files): skip TTL expiry to prevent cache miss
    /// during slow writes that exceed metadata_ttl. Uses reference counting
    /// so concurrent open/release on the same inode (different FUSE workers)
    /// don't prematurely unpin.
    pinned_inodes: RwLock<HashMap<u64, u32>>,
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
            pinned_inodes: RwLock::new(HashMap::new()),
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
            cached_at: Instant::now(),
        });
        cache
    }

    /// Allocate a new inode number
    pub fn allocate_inode(&self) -> u64 {
        self.next_inode.fetch_add(1, Ordering::SeqCst)
    }

    /// Get an entry by inode. Returns None if the inode is not cached or if
    /// the entry has exceeded the metadata TTL (used as a fallback when
    /// Invalidation notifications are lost).
    pub fn get_inode(&self, inode: u64) -> Option<CachedEntry> {
        let mut cache = self.inode_cache.write().unwrap();
        let entry = cache.get(&inode).cloned()?;
        // Pinned inodes (open files) skip TTL expiry to prevent cache miss
        // during slow writes that exceed metadata_ttl.
        let is_pinned = self.pinned_inodes.read().unwrap().contains_key(&inode);
        if !is_pinned && entry.cached_at.elapsed() > self.metadata_ttl {
            // Entry too old, treat as stale
            debug!(
                "MetadataCache: inode {} cache expired (age={:?} > ttl={:?}), treating as stale",
                inode,
                entry.cached_at.elapsed(),
                self.metadata_ttl,
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
    pub fn pin_inode(&self, inode: u64) {
        let mut pinned = self.pinned_inodes.write().unwrap();
        *pinned.entry(inode).or_insert(0) += 1;
    }

    /// Unpin an inode (called on release/close).
    /// Decrements the reference count; only removes when count reaches 0.
    pub fn unpin_inode(&self, inode: u64) {
        let mut pinned = self.pinned_inodes.write().unwrap();
        if let Some(count) = pinned.get_mut(&inode) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                pinned.remove(&inode);
            }
        }
    }

    /// Check if an inode is pinned (open). Pinned inodes hold a data lease,
    /// so their cached metadata/data is authoritative and should not be
    /// invalidated by server-pushed Invalidate notifications. This prevents
    /// a self-invalidation race where a client's own setattr triggers an
    /// Invalidate that evicts the entry it just updated (causing ENOENT).
    pub fn is_pinned(&self, inode: u64) -> bool {
        self.pinned_inodes.read().unwrap().contains_key(&inode)
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

    /// Insert an entry into the cache (only update path_map, keep existing inode_cache entry for hard links)
    pub fn insert(&self, entry: CachedEntry) {
        let parent = entry.parent;
        let inode = entry.inode;
        let path = if inode == 1 {
            String::from("/")
        } else {
            let mut parts = Vec::new();
            parts.push(entry.name.clone());
            let mut current = parent;
            let mut visited = std::collections::HashSet::new();
            visited.insert(inode);
            while current != 1 {
                if !visited.insert(current) {
                    warn!("Detected cycle in path construction, breaking");
                    break;
                }
                if let Some(e) = self.get_inode(current) {
                    parts.push(e.name.clone());
                    current = e.parent;
                } else {
                    break;
                }
            }
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
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(existing) = cache.get_mut(&inode) {
            if existing.name != entry.name || existing.parent != entry.parent {
                // Rename: replace with the new entry's metadata
                *existing = entry;
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
                let is_pinned = self.pinned_inodes.read().unwrap().contains_key(&inode);
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
                    existing.symlink_target = entry.symlink_target.clone();
                }
                existing.disk_size = entry.disk_size;
                existing.generation = entry.generation;
                // Always update cached_at to prevent TTL expiration issues
                existing.cached_at = Instant::now();
            }
        } else {
            cache.put(inode, entry);
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
        let mut dir_cache = self.dir_cache.write().unwrap();
        dir_cache.insert(
            parent_inode,
            DirCacheEntry {
                entries,
                cached_at: Instant::now(),
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

            for chunk_idx in start_chunk_idx..=end_chunk_idx {
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
                }
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

    /// List children of a directory from cache (supports hard links via path_map)
    pub fn list_children(&self, parent_inode: u64) -> Vec<(u64, String, bool)> {
        let parent_path = match self.inode_to_path(parent_inode) {
            Some(p) => p,
            None => {
                // Fallback: use inode_cache only
                let cache = self.inode_cache.read().unwrap();
                let mut children = Vec::new();
                for (_, entry) in cache.iter() {
                    if entry.parent == parent_inode && entry.inode != parent_inode {
                        children.push((entry.inode, entry.name.clone(), entry.is_dir));
                    }
                }
                return children;
            }
        };

        let path_map = self.path_map.read().unwrap();
        // Acquire inode_cache read lock once for is_dir lookups.
        // Use direct cache access (not get_inode) to bypass TTL — entries
        // physically present in inode_cache have correct is_dir regardless
        // of TTL expiry. Using get_inode here causes directories to be
        // misreported as files when their cache entries expire (TTL=2s),
        // breaking find/cp -prf recursion after large directory copies.
        let inode_cache = self.inode_cache.read().unwrap();
        let mut children = Vec::new();

        let prefix = if parent_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", parent_path)
        };

        // Collect all entries from path_map that are direct children
        // Each path represents a separate directory entry, even if they point to the same inode (hard links)
        for (path, ino) in path_map.iter() {
            if path.starts_with(&prefix) && *ino != parent_inode {
                // Check if it's a direct child (no additional slashes beyond the prefix)
                let relative = &path[prefix.len()..];
                if !relative.is_empty() && !relative.contains('/') {
                    // Get is_dir from inode_cache directly (no TTL check, no LRU touch)
                    let is_dir = inode_cache.peek(ino).map(|e| e.is_dir).unwrap_or(false);
                    children.push((*ino, relative.to_string(), is_dir));
                }
            }
        }
        drop(path_map);

        // Also check inode_cache for any entries that might not be in path_map
        // Use a set of (inode, name) pairs to avoid duplicates, but allow hard links with different names
        let mut seen_pairs = std::collections::HashSet::new();
        for (_, entry) in inode_cache.iter() {
            if entry.parent == parent_inode && entry.inode != parent_inode {
                let pair = (entry.inode, entry.name.clone());
                if seen_pairs.insert(pair) {
                    // Check if this entry is already in children.
                    // If it is but has wrong is_dir (from path_map scan with
                    // expired get_inode), update it with the correct value.
                    let existing = children
                        .iter_mut()
                        .find(|(ino, name, _)| *ino == entry.inode && *name == entry.name);
                    if let Some(existing) = existing {
                        existing.2 = entry.is_dir;
                    } else {
                        children.push((entry.inode, entry.name.clone(), entry.is_dir));
                    }
                }
            }
        }

        drop(inode_cache);

        // Debug logging
        log::debug!(
            "list_children(parent_inode={}, parent_path={}) returned {} children: {:?}",
            parent_inode,
            parent_path,
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
        // Find the entry
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

        // Check if target exists
        let target_exists = {
            let children = self.list_children(newdir);
            children.iter().any(|(_, name, _)| name == newname)
        };

        // Remove old entry from cache temporarily, update, and re-insert
        let inode = entry.inode;
        let old_parent = entry.parent;

        // Remove from old path
        let old_path = self.inode_to_path(inode).unwrap_or_default();
        {
            let mut path_map = self.path_map.write().unwrap();
            path_map.remove(&old_path);
        }

        // Update entry
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

        // If target exists, remove it first
        if target_exists {
            let children = self.list_children(newdir);
            for (ino, name, _) in children {
                if name == newname {
                    // Don't actually delete data here, just remove from cache
                    // The caller should handle data deletion
                    let _ = self.remove_entry_only(ino);
                    break;
                }
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
                cached_at: Instant::now(),
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

        debug!("Invalidated cache for inode: {}", inode);
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
            cached_at: Instant::now(),
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
            cached_at: Instant::now(),
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
                cached_at: Instant::now(),
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
            cached_at: Instant::now(),
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
            cached_at: Instant::now(),
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
            cached_at: Instant::now(),
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
            cached_at: Instant::now(),
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
            cached_at: Instant::now(),
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
            cached_at: Instant::now(),
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
            cached_at: Instant::now(),
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
            cached_at: Instant::now(),
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
}
