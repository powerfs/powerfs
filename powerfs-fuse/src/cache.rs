use log::{debug, warn};
use lru::LruCache;
use powerfs_common::types::Fid;
use powerfs_master::proto::FileChunk;
use std::collections::HashMap;
use std::num::NonZero;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

pub const ROOT_INODE: u64 = 1;
pub const DEFAULT_CHUNK_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct CachedFileChunk {
    pub offset: u64,
    pub size: u64,
    pub mtime: u64,
    pub fid: String,
    pub cookie: u32,
    pub crc32: u32,
}

impl From<FileChunk> for CachedFileChunk {
    fn from(chunk: FileChunk) -> Self {
        CachedFileChunk {
            offset: chunk.offset,
            size: chunk.size,
            mtime: chunk.mtime,
            fid: chunk.fid,
            cookie: chunk.cookie,
            crc32: chunk.crc32,
        }
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
    /// Latest known generation per path (from notifications)
    path_generations: RwLock<HashMap<String, u64>>,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self::with_capacity(10000)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let cache = MetadataCache {
            inode_cache: RwLock::new(LruCache::new(
                NonZero::new(capacity).unwrap_or(NonZero::new(10000).unwrap()),
            )),
            path_map: RwLock::new(HashMap::new()),
            dir_cache: RwLock::new(HashMap::new()),
            next_inode: AtomicU64::new(2),
            dir_cache_ttl: Duration::from_secs(5),
            path_generations: RwLock::new(HashMap::new()),
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
        });
        cache
    }

    /// Allocate a new inode number
    pub fn allocate_inode(&self) -> u64 {
        self.next_inode.fetch_add(1, Ordering::SeqCst)
    }

    /// Get an entry by inode
    pub fn get_inode(&self, inode: u64) -> Option<CachedEntry> {
        let mut cache = self.inode_cache.write().unwrap();
        cache.get(&inode).cloned()
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

    /// Insert an entry into the cache
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
        {
            let mut cache = self.inode_cache.write().unwrap();
            cache.put(inode, entry);
        }
        self.invalidate_dir(parent);
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

    /// Invalidate directory listing cache for a parent inode
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
            entry.mtime = chrono::Utc::now().timestamp();
        }
    }

    /// Update FID (file ID)
    pub fn update_fid(&self, inode: u64, fid: Fid) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            entry.fid = Some(fid);
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
        }
    }

    /// List children of a directory from cache
    pub fn list_children(&self, parent_inode: u64) -> Vec<(u64, String, bool)> {
        let cache = self.inode_cache.read().unwrap();
        let mut children = Vec::new();
        for (_, entry) in cache.iter() {
            if entry.parent == parent_inode && entry.inode != parent_inode {
                children.push((entry.inode, entry.name.clone(), entry.is_dir));
            }
        }
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
            },
        );
        drop(cache);
        let mut path_map = self.path_map.write().unwrap();
        path_map.insert("/".to_string(), 1);

        debug!("Metadata cache fully cleared and root re-initialized");
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
}

#[derive(Debug, Clone)]
pub struct ChunkData {
    pub data: Vec<u8>,
    pub offset: u64,
    pub size: u64,
    pub mtime: u64,
    pub crc32: u32,
}

pub struct ChunkCache {
    cache: RwLock<HashMap<(u64, u64), ChunkData>>,
    chunk_size: u64,
}

impl ChunkCache {
    pub fn new(chunk_size: u64, _max_chunks: usize) -> Self {
        ChunkCache {
            cache: RwLock::new(HashMap::new()),
            chunk_size,
        }
    }

    pub fn with_defaults() -> Self {
        ChunkCache::new(DEFAULT_CHUNK_SIZE, 100)
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
        let cache = self.cache.read().unwrap();
        cache.get(&(inode, chunk_index)).cloned()
    }

    pub fn modify<F>(&self, inode: u64, offset: u64, f: F) -> bool
    where
        F: FnOnce(&mut ChunkData),
    {
        let chunk_index = self.get_chunk_index(offset);
        let mut cache = self.cache.write().unwrap();
        if let Some(chunk) = cache.get_mut(&(inode, chunk_index)) {
            f(chunk);
            true
        } else {
            false
        }
    }

    pub fn put(&self, inode: u64, offset: u64, data: Vec<u8>, mtime: u64, crc32: u32) {
        let chunk_index = self.get_chunk_index(offset);
        let mut cache = self.cache.write().unwrap();
        cache.insert(
            (inode, chunk_index),
            ChunkData {
                data,
                offset: chunk_index * self.chunk_size,
                size: self.chunk_size,
                mtime,
                crc32,
            },
        );
    }

    pub fn remove(&self, inode: u64) {
        let mut cache = self.cache.write().unwrap();
        let keys_to_remove: Vec<_> = cache
            .iter()
            .filter(|((ino, _), _)| *ino == inode)
            .map(|(k, _)| *k)
            .collect();
        for key in keys_to_remove {
            cache.remove(&key);
        }
    }

    pub fn remove_chunk(&self, inode: u64, offset: u64) {
        let chunk_index = self.get_chunk_index(offset);
        let mut cache = self.cache.write().unwrap();
        cache.remove(&(inode, chunk_index));
    }

    pub fn remove_inode_chunks(&self, inode: u64) {
        let mut cache = self.cache.write().unwrap();
        cache.retain(|key, _| key.0 != inode);
    }

    pub fn clear(&self) {
        let mut cache = self.cache.write().unwrap();
        cache.clear();
    }

    pub fn len(&self) -> usize {
        let cache = self.cache.read().unwrap();
        cache.len()
    }

    pub fn is_empty(&self) -> bool {
        let cache = self.cache.read().unwrap();
        cache.is_empty()
    }

    pub fn prefetch(&self, inode: u64, start_offset: u64, end_offset: u64) -> Vec<(u64, u64)> {
        let start_chunk = self.get_chunk_index(start_offset);
        let end_chunk = if end_offset == 0 {
            0
        } else {
            self.get_chunk_index(end_offset - 1)
        };
        let mut missing = Vec::new();

        {
            let cache = self.cache.read().unwrap();
            for chunk_index in start_chunk..=end_chunk {
                if !cache.contains_key(&(inode, chunk_index)) {
                    missing.push((chunk_index * self.chunk_size, self.chunk_size));
                }
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

        cache.put(inode, 0, vec![0u8; 1024], 1234567890, 0);
        let chunk = cache.get(inode, 0).unwrap();
        assert_eq!(chunk.data.len(), 1024);
        assert_eq!(chunk.offset, 0);
        assert_eq!(chunk.mtime, 1234567890);
    }

    #[test]
    fn test_chunk_cache_remove() {
        let cache = ChunkCache::new(1024, 10);
        let inode = 100;

        cache.put(inode, 0, vec![0u8; 1024], 1234567890, 0);
        cache.put(inode, 1024, vec![1u8; 1024], 1234567891, 1);

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

        cache.put(inode, 0, vec![0u8; 1024], 1234567890, 0);

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
