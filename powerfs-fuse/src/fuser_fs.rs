use crate::cache::{CachedEntry, CachedFileChunk, ChunkCache, MetadataCache};
use crate::client::{PowerFuseClient, SyncFuseClient};
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use log::{debug, error, info, warn};
use powerfs_common::error::{PowerFsError, Result};
use powerfs_common::types::Fid;
use powerfs_master::proto::powerfs::Entry as FilerEntry;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio::runtime::Handle;

const TTL: Duration = Duration::from_secs(1);

struct WriteBufferEntry {
    offset: u64,
    data: Vec<u8>,
}

struct WriteBuffer {
    buffers: RwLock<HashMap<u64, Vec<WriteBufferEntry>>>,
    max_entries: usize,
}

impl WriteBuffer {
    fn new(max_entries: usize) -> Self {
        Self {
            buffers: RwLock::new(HashMap::new()),
            max_entries,
        }
    }

    fn add(&self, inode: u64, offset: u64, data: &[u8]) -> bool {
        let mut buffers = self.buffers.write().unwrap();
        let entries = buffers.entry(inode).or_default();

        let entry = WriteBufferEntry {
            offset,
            data: data.to_vec(),
        };
        entries.push(entry);

        entries.len() >= self.max_entries
    }

    fn take(&self, inode: u64) -> Vec<WriteBufferEntry> {
        let mut buffers = self.buffers.write().unwrap();
        buffers.remove(&inode).unwrap_or_default()
    }
}

struct PowerFsFuserFs {
    client: Arc<SyncFuseClient>,
    cache: Arc<MetadataCache>,
    chunk_cache: Arc<ChunkCache>,
    collection: String,
    replication: String,
    dirty_chunks: Arc<RwLock<HashSet<(u64, u64)>>>,
    has_dirty: Arc<std::sync::atomic::AtomicBool>,
    write_buffer: Arc<WriteBuffer>,
}

impl PowerFsFuserFs {
    fn new(
        client: Arc<SyncFuseClient>,
        cache: Arc<MetadataCache>,
        chunk_cache: Arc<ChunkCache>,
        collection: String,
        replication: String,
        write_buffer: Arc<WriteBuffer>,
    ) -> Self {
        Self {
            client,
            cache,
            chunk_cache,
            collection,
            replication,
            dirty_chunks: Arc::new(RwLock::new(HashSet::new())),
            has_dirty: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            write_buffer,
        }
    }

    fn flush_dirty_chunks(&self, inode: u64) -> std::io::Result<()> {
        let dirty: Vec<(u64, u64)> = {
            let dirty_set = self.dirty_chunks.read().unwrap();
            dirty_set
                .iter()
                .filter(|(ino, _)| *ino == inode)
                .cloned()
                .collect()
        };

        if dirty.is_empty() {
            return Ok(());
        }

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let fid = entry
            .fid
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;

        let locations = self.client.lookup_volume(fid.volume_id).map_err(|e| {
            error!("lookup_volume failed: {}", e);
            std::io::Error::from_raw_os_error(libc::EIO)
        })?;

        let loc = locations
            .first()
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;
        let addr = PowerFuseClient::location_to_grpc_addr(loc);
        let chunk_size = self.chunk_cache.chunk_size();

        let mut entries = Vec::new();
        let mut chunks = Vec::new();

        for (_, chunk_idx) in &dirty {
            let chunk_offset = chunk_idx * chunk_size;
            let chunk_data = self.chunk_cache.get(inode, chunk_offset);

            if let Some(chunk_data) = chunk_data {
                let data_len = chunk_data.data.len();
                entries.push((chunk_offset as i64, data_len as i32, chunk_data.data, 0u32));

                chunks.push(powerfs_master::proto::powerfs::FileChunk {
                    offset: chunk_offset,
                    size: data_len as u64,
                    mtime: chunk_data.mtime,
                    fid: fid.to_string(),
                    cookie: 0,
                    crc32: chunk_data.crc32,
                });
            }
        }

        if !entries.is_empty() {
            self.client
                .batch_write_blob(&addr, fid.volume_id.0, fid.file_key, entries)
                .map_err(|e| {
                    error!("batch_write_blob failed: {}", e);
                    std::io::Error::from_raw_os_error(libc::EIO)
                })?;
        }

        let mut dirty_set = self.dirty_chunks.write().unwrap();
        dirty_set.retain(|(ino, _)| *ino != inode);

        let path = self.cache.inode_to_path(inode).unwrap_or_default();
        if !path.is_empty() && !chunks.is_empty() {
            let filer_entry = powerfs_master::proto::powerfs::Entry {
                name: entry.name.clone(),
                directory: self.cache.inode_to_path(entry.parent).unwrap_or_default(),
                attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                    ino: entry.inode,
                    mode: entry.mode | 0o100000,
                    nlink: entry.nlink,
                    uid: entry.uid,
                    gid: entry.gid,
                    rdev: 0,
                    size: entry.size,
                    blksize: 4096,
                    blocks: entry.size.div_ceil(512),
                    atime: entry.atime as u64,
                    mtime: entry.mtime as u64,
                    ctime: entry.ctime as u64,
                    crtime: entry.ctime as u64,
                    perm: 0,
                }),
                chunks,
                hard_link_id: entry.hard_link_id.clone(),
                hard_link_counter: entry.hard_link_counter,
                extended: HashMap::new(),
                content_size: entry.content_size,
                disk_size: entry.disk_size,
                ttl: String::new(),
                symlink_target: String::new(),
            };

            if let Err(e) = self.client.update_entry(&filer_entry) {
                warn!("Failed to update entry on master: {}", e);
            }
        }

        Ok(())
    }

    fn flush_all_dirty_chunks(&self) -> std::io::Result<()> {
        let dirty: Vec<(u64, u64)> = {
            let dirty_set = self.dirty_chunks.read().unwrap();
            dirty_set.iter().cloned().collect()
        };

        if dirty.is_empty() {
            return Ok(());
        }

        let inodes: HashSet<u64> = dirty.iter().map(|(ino, _)| *ino).collect();

        for inode in inodes {
            let _ = self.flush_dirty_chunks(inode);
        }

        Ok(())
    }

    fn flush_write_buffer(&self, inode: u64, entries: &[WriteBufferEntry]) {
        if entries.is_empty() {
            return;
        }

        let chunk_size = self.chunk_cache.chunk_size();
        let mut merged_data: HashMap<u64, Vec<u8>> = HashMap::new();

        for entry in entries {
            let start_chunk_idx = entry.offset / chunk_size;
            let end_chunk_idx = (entry.offset + entry.data.len() as u64).div_ceil(chunk_size);

            for chunk_idx in start_chunk_idx..=end_chunk_idx {
                let _chunk_offset = chunk_idx * chunk_size;
                let data_start_in_chunk = if chunk_idx == start_chunk_idx {
                    entry.offset % chunk_size
                } else {
                    0
                };
                let data_end_in_chunk = if chunk_idx == end_chunk_idx {
                    std::cmp::min(data_start_in_chunk + entry.data.len() as u64, chunk_size)
                } else {
                    chunk_size
                };

                let src_start = if chunk_idx == start_chunk_idx {
                    0
                } else {
                    ((chunk_idx - start_chunk_idx) * chunk_size - (entry.offset % chunk_size))
                        as usize
                };
                let src_end = src_start + (data_end_in_chunk - data_start_in_chunk) as usize;

                if src_end > entry.data.len() {
                    continue;
                }

                let merged = merged_data
                    .entry(chunk_idx)
                    .or_insert_with(|| vec![0u8; chunk_size as usize]);
                let dst_start = data_start_in_chunk as usize;
                let dst_end = data_end_in_chunk as usize;
                if dst_end <= merged.len() && src_end <= entry.data.len() {
                    merged[dst_start..dst_end].copy_from_slice(&entry.data[src_start..src_end]);
                }
            }
        }

        for (chunk_idx, data) in merged_data {
            let chunk_offset = chunk_idx * chunk_size;
            let now = chrono::Utc::now().timestamp() as u64;
            self.chunk_cache.put(inode, chunk_offset, data, now, 0);

            let mut dirty_set = self.dirty_chunks.write().unwrap();
            dirty_set.insert((inode, chunk_idx));
            self.has_dirty
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn create_file_attr(&self, entry: &CachedEntry) -> FileAttr {
        let file_type = if entry.is_symlink {
            FileType::Symlink
        } else if entry.is_dir {
            FileType::Directory
        } else {
            FileType::RegularFile
        };

        FileAttr {
            ino: entry.inode,
            size: entry.size,
            blocks: entry.size.div_ceil(512),
            atime: std::time::UNIX_EPOCH + std::time::Duration::from_secs(entry.atime as u64),
            mtime: std::time::UNIX_EPOCH + std::time::Duration::from_secs(entry.mtime as u64),
            ctime: std::time::UNIX_EPOCH + std::time::Duration::from_secs(entry.ctime as u64),
            crtime: std::time::UNIX_EPOCH + std::time::Duration::from_secs(entry.ctime as u64),
            kind: file_type,
            perm: entry.mode as u16,
            nlink: entry.nlink,
            uid: entry.uid,
            gid: entry.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn lookup_in_cache(&self, parent: u64, name: &str) -> Option<CachedEntry> {
        self.cache.lookup_in_cache(parent, name)
    }

    fn entry_to_cached(&self, parent: u64, entry: &FilerEntry) -> CachedEntry {
        let attrs = entry.attributes.as_ref();
        let chunks = entry
            .chunks
            .iter()
            .map(|chunk| CachedFileChunk {
                offset: chunk.offset,
                size: chunk.size,
                mtime: chunk.mtime,
                fid: chunk.fid.clone(),
                cookie: chunk.cookie,
                crc32: chunk.crc32,
            })
            .collect();

        let fid = entry.chunks.first().and_then(|chunk| {
            info!("Parsing fid from chunk: {}", chunk.fid);
            let result = Fid::from_string(&chunk.fid);
            info!("Fid parse result: {:?}", result);
            result.ok()
        });

        let mode_val = attrs.map(|a| a.mode).unwrap_or(0);
        let file_type = mode_val & 0o170000;
        let is_dir = file_type == 0o040000;
        let is_symlink = file_type == 0o120000;

        CachedEntry {
            inode: attrs.map(|a| a.ino).unwrap_or(0),
            parent,
            name: entry.name.clone(),
            is_dir,
            is_symlink,
            symlink_target: if is_symlink {
                Some(entry.symlink_target.clone())
            } else {
                None
            },
            nlink: attrs.map(|a| a.nlink).unwrap_or(1),
            fid,
            size: attrs.map(|a| a.size).unwrap_or(0),
            mode: attrs.map(|a| a.mode & 0o7777).unwrap_or(0o644),
            uid: attrs.map(|a| a.uid).unwrap_or(0),
            gid: attrs.map(|a| a.gid).unwrap_or(0),
            atime: attrs.map(|a| a.atime as i64).unwrap_or(0),
            mtime: attrs.map(|a| a.mtime as i64).unwrap_or(0),
            ctime: attrs.map(|a| a.ctime as i64).unwrap_or(0),
            xattrs: HashMap::new(),
            chunks,
            hard_link_id: entry.hard_link_id.clone(),
            hard_link_counter: entry.hard_link_counter,
            content_size: entry.content_size,
            disk_size: entry.disk_size,
        }
    }
}

impl Clone for PowerFsFuserFs {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            cache: self.cache.clone(),
            chunk_cache: self.chunk_cache.clone(),
            collection: self.collection.clone(),
            replication: self.replication.clone(),
            dirty_chunks: self.dirty_chunks.clone(),
            has_dirty: self.has_dirty.clone(),
            write_buffer: self.write_buffer.clone(),
        }
    }
}

impl Filesystem for PowerFsFuserFs {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut KernelConfig,
    ) -> std::result::Result<(), i32> {
        info!("FUSE filesystem initialized");
        Ok(())
    }

    fn destroy(&mut self) {
        info!("FUSE filesystem destroyed");
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_str().unwrap_or("");
        debug!("lookup: parent={}, name={}", parent, name_str);

        if let Some(entry) = self.lookup_in_cache(parent, name_str) {
            let attr = self.create_file_attr(&entry);
            reply.entry(&TTL, &attr, 0);
            return;
        }

        let parent_path = self
            .cache
            .inode_to_path(parent)
            .unwrap_or_else(|| "/".to_string());
        let lookup_path = if parent_path == "/" {
            format!("/{}", name_str)
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        match self.client.get_entry(&lookup_path) {
            Ok(Some(entry)) => {
                info!(
                    "lookup found entry: path={}, chunks={}, content_size={}",
                    lookup_path,
                    entry.chunks.len(),
                    entry.content_size
                );
                let cached = self.entry_to_cached(parent, &entry);
                self.cache.insert(cached.clone());
                let attr = self.create_file_attr(&cached);
                reply.entry(&TTL, &attr, 0);
            }
            Ok(None) => reply.error(libc::ENOENT),
            Err(e) => {
                warn!("lookup entry failed: {}", e);
                reply.error(libc::ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, inode: u64, reply: ReplyAttr) {
        debug!("getattr: inode={}", inode);

        if let Some(entry) = self.cache.get_inode(inode) {
            let attr = self.create_file_attr(&entry);
            reply.attr(&TTL, &attr);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        debug!(
            "setattr: inode={}, mode={:?}, uid={:?}, gid={:?}, size={:?}",
            inode, mode, uid, gid, size
        );

        let _entry = match self.cache.get_inode(inode) {
            Some(e) => e,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let now = chrono::Utc::now().timestamp();

        let atime_val = match atime {
            Some(TimeOrNow::Now) => Some(now),
            Some(TimeOrNow::SpecificTime(t)) => Some(
                (t.duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()) as i64,
            ),
            None => None,
        };

        let mtime_val = match mtime {
            Some(TimeOrNow::Now) => Some(now),
            Some(TimeOrNow::SpecificTime(t)) => Some(
                (t.duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()) as i64,
            ),
            None => None,
        };

        self.cache.update_attr(
            inode,
            crate::cache::UpdateAttrParams {
                mode,
                size,
                uid,
                gid,
                atime: atime_val,
                mtime: mtime_val,
            },
        );

        if let Some(updated) = self.cache.get_inode(inode) {
            let new_attr = self.create_file_attr(&updated);
            reply.attr(&TTL, &new_attr);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "mkdir: parent={}, name={}, mode={:o}",
            parent, name_str, mode
        );

        if self.lookup_in_cache(parent, name_str).is_some() {
            reply.error(libc::EEXIST);
            return;
        }

        let inode = self.cache.allocate_inode();
        let now = chrono::Utc::now().timestamp();
        let entry = CachedEntry {
            inode,
            parent,
            name: name_str.to_string(),
            is_dir: true,
            is_symlink: false,
            symlink_target: None,
            nlink: 2,
            fid: None,
            size: 0,
            mode: mode & 0o7777,
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
        };
        self.cache.insert(entry.clone());

        let parent_path = self
            .cache
            .inode_to_path(parent)
            .unwrap_or_else(|| "/".to_string());
        let filer_entry = FilerEntry {
            name: name_str.to_string(),
            directory: parent_path,
            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                ino: inode,
                mode: mode | 0o040000,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                size: 0,
                blksize: 4096,
                blocks: 0,
                atime: now as u64,
                mtime: now as u64,
                ctime: now as u64,
                crtime: now as u64,
                perm: 0,
            }),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            extended: HashMap::new(),
            content_size: 0,
            disk_size: 0,
            ttl: String::new(),
            symlink_target: String::new(),
        };

        if let Err(e) = self.client.create_entry(filer_entry) {
            warn!("Failed to create directory entry on master: {}", e);
        }

        let attr = self.create_file_attr(&entry);
        reply.entry(&TTL, &attr, 0);
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap_or("");
        debug!("rmdir: parent={}, name={}", parent, name_str);

        let entry = match self.lookup_in_cache(parent, name_str) {
            Some(e) => e,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if !entry.is_dir {
            reply.error(libc::ENOTDIR);
            return;
        }

        if !self.cache.list_children(entry.inode).is_empty() {
            reply.error(libc::ENOTEMPTY);
            return;
        }

        let entry_path = self
            .cache
            .inode_to_path(entry.inode)
            .unwrap_or_else(|| "/".to_string());
        if let Err(e) = self.client.delete_entry(&entry_path, true) {
            warn!("Failed to delete directory entry on master: {}", e);
        }

        self.cache.remove(entry.inode);
        reply.ok();
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap_or("");
        debug!("unlink: parent={}, name={}", parent, name_str);

        let entry = match self.lookup_in_cache(parent, name_str) {
            Some(e) => e,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let should_delete = self.cache.dec_nlink(entry.inode);

        if should_delete {
            if let Some(fid) = &entry.fid {
                match self.client.lookup_volume(fid.volume_id) {
                    Ok(locations) => {
                        if let Some(loc) = locations.first() {
                            let addr = PowerFuseClient::location_to_grpc_addr(loc);
                            if let Err(e) =
                                self.client
                                    .delete_data(&addr, fid.volume_id.0, fid.file_key)
                            {
                                warn!("Failed to delete data: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to lookup volume: {}", e);
                    }
                }
            }

            let entry_path = self
                .cache
                .inode_to_path(entry.inode)
                .unwrap_or_else(|| "/".to_string());
            if let Err(e) = self.client.delete_entry(&entry_path, false) {
                warn!("Failed to delete entry on master: {}", e);
            }
        }

        self.cache.remove(entry.inode);
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "create: parent={}, name={}, mode={:o}, flags={:o}",
            parent, name_str, mode, flags
        );

        let exists_in_cache = self.lookup_in_cache(parent, name_str).is_some();

        if exists_in_cache && (flags & libc::O_EXCL) != 0 {
            reply.error(libc::EEXIST);
            return;
        }

        let inode = self.cache.allocate_inode();
        let now = chrono::Utc::now().timestamp();
        let entry = CachedEntry {
            inode,
            parent,
            name: name_str.to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: mode & 0o7777,
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
        };
        self.cache.insert(entry.clone());

        let parent_path = self
            .cache
            .inode_to_path(parent)
            .unwrap_or_else(|| "/".to_string());
        let filer_entry = FilerEntry {
            name: name_str.to_string(),
            directory: parent_path,
            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                ino: inode,
                mode: mode | 0o100000,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                size: 0,
                blksize: 4096,
                blocks: 0,
                atime: now as u64,
                mtime: now as u64,
                ctime: now as u64,
                crtime: now as u64,
                perm: 0,
            }),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            extended: HashMap::new(),
            content_size: 0,
            disk_size: 0,
            ttl: String::new(),
            symlink_target: String::new(),
        };

        if let Err(e) = self.client.create_entry(filer_entry) {
            warn!("Failed to create file entry on master: {}", e);
        }

        let attr = self.create_file_attr(&entry);
        reply.created(&TTL, &attr, 0, 0, 0);
    }

    fn open(&mut self, _req: &Request<'_>, inode: u64, _flags: i32, reply: ReplyOpen) {
        debug!("open: inode={}", inode);
        if let Some(entry) = self.cache.get_inode(inode) {
            let _attr = self.create_file_attr(&entry);
            reply.opened(0, 0);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        debug!("read: inode={}, offset={}, size={}", inode, offset, size);

        let entry = match self.cache.get_inode(inode) {
            Some(e) => e,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let offset_u64 = offset as u64;
        if offset_u64 >= entry.size {
            reply.data(&[]);
            return;
        }

        let actual_size = std::cmp::min(size as u64, entry.size - offset_u64) as usize;
        let mut result = vec![0u8; actual_size];

        let chunk_size = self.chunk_cache.chunk_size();
        let start_chunk_idx = offset_u64 / chunk_size;
        let end_chunk_idx = (offset_u64 + actual_size as u64).div_ceil(chunk_size);

        for chunk_idx in start_chunk_idx..=end_chunk_idx {
            let chunk_offset = chunk_idx * chunk_size;
            let chunk_data = self.chunk_cache.get(inode, chunk_offset);

            let chunk_data = match chunk_data {
                Some(d) => d,
                None => {
                    let write_buffer_entries = self.write_buffer.take(inode);
                    if !write_buffer_entries.is_empty() {
                        self.flush_write_buffer(inode, &write_buffer_entries);
                        match self.chunk_cache.get(inode, chunk_offset) {
                            Some(d) => d,
                            None => {
                                let is_dirty = {
                                    let dirty_set = self.dirty_chunks.read().unwrap();
                                    dirty_set.contains(&(inode, chunk_idx))
                                };
                                if is_dirty {
                                    info!("read: chunk {} is dirty, flushing first", chunk_idx);
                                    if let Err(e) = self.flush_dirty_chunks(inode) {
                                        warn!("Failed to flush dirty chunks: {}", e);
                                        reply.error(libc::EIO);
                                        return;
                                    }
                                    match self.chunk_cache.get(inode, chunk_offset) {
                                        Some(d) => d,
                                        None => {
                                            warn!(
                                                "read: chunk {} still not available after flush",
                                                chunk_idx
                                            );
                                            reply.error(libc::EIO);
                                            return;
                                        }
                                    }
                                } else {
                                    warn!(
                                        "read: chunk {} not available after flush_write_buffer",
                                        chunk_idx
                                    );
                                    reply.error(libc::EIO);
                                    return;
                                }
                            }
                        }
                    } else {
                        let is_dirty = {
                            let dirty_set = self.dirty_chunks.read().unwrap();
                            dirty_set.contains(&(inode, chunk_idx))
                        };
                        if is_dirty {
                            info!("read: chunk {} is dirty, flushing first", chunk_idx);
                            if let Err(e) = self.flush_dirty_chunks(inode) {
                                warn!("Failed to flush dirty chunks: {}", e);
                                reply.error(libc::EIO);
                                return;
                            }
                            match self.chunk_cache.get(inode, chunk_offset) {
                                Some(d) => d,
                                None => {
                                    warn!(
                                        "read: chunk {} still not available after flush",
                                        chunk_idx
                                    );
                                    reply.error(libc::EIO);
                                    return;
                                }
                            }
                        } else {
                            match &entry.fid {
                                Some(fid) => {
                                    let locations = match self.client.lookup_volume(fid.volume_id) {
                                        Ok(l) => l,
                                        Err(e) => {
                                            error!("lookup_volume failed: {}", e);
                                            reply.error(libc::EIO);
                                            return;
                                        }
                                    };
                                    let loc = match locations.first() {
                                        Some(l) => l,
                                        None => {
                                            error!("no volume location available");
                                            reply.error(libc::EIO);
                                            return;
                                        }
                                    };
                                    let addr = PowerFuseClient::location_to_grpc_addr(loc);
                                    match self.client.read_blob(
                                        &addr,
                                        fid.volume_id.0,
                                        fid.file_key,
                                        chunk_offset as i64,
                                        chunk_size as i32,
                                    ) {
                                        Ok(data) => {
                                            self.chunk_cache.put(inode, chunk_offset, data, 0, 0);
                                            match self.chunk_cache.get(inode, chunk_offset) {
                                                Some(d) => d,
                                                None => {
                                                    warn!(
                                                        "read: chunk {} not in cache after put",
                                                        chunk_idx
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            warn!("read_blob failed: {}", e);
                                            continue;
                                        }
                                    }
                                }
                                None => {
                                    continue;
                                }
                            }
                        }
                    }
                }
            };

            let data_start_in_chunk = if chunk_idx == start_chunk_idx {
                offset_u64 % chunk_size
            } else {
                0
            };
            let data_end_in_chunk = if chunk_idx == end_chunk_idx {
                std::cmp::min(
                    (offset_u64 + actual_size as u64) % chunk_size,
                    chunk_data.data.len() as u64,
                )
            } else {
                chunk_data.data.len() as u64
            };

            if data_start_in_chunk < data_end_in_chunk {
                let src_start = data_start_in_chunk as usize;
                let src_end = data_end_in_chunk as usize;
                let dst_start = if chunk_idx == start_chunk_idx {
                    0
                } else {
                    ((chunk_idx - start_chunk_idx) * chunk_size + data_start_in_chunk
                        - (offset_u64 % chunk_size)) as usize
                };
                let dst_end = dst_start + (src_end - src_start);

                if dst_end <= result.len() && src_end <= chunk_data.data.len() {
                    result[dst_start..dst_end]
                        .copy_from_slice(&chunk_data.data[src_start..src_end]);
                }
            }
        }

        reply.data(&result);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        debug!(
            "write: inode={}, offset={}, size={}",
            inode,
            offset,
            data.len()
        );

        let offset_u64 = offset as u64;
        let data_len = data.len();
        let new_size = offset_u64 + data_len as u64;

        self.cache.update_size(inode, new_size);

        let chunk_size = self.chunk_cache.chunk_size();

        if data_len < chunk_size as usize / 4 {
            let should_flush = self.write_buffer.add(inode, offset_u64, data);
            if should_flush {
                let entries = self.write_buffer.take(inode);
                self.flush_write_buffer(inode, &entries);
            }
            reply.written(data_len as u32);
            return;
        }

        let start_chunk_idx = offset_u64 / chunk_size;
        let end_chunk_idx = (offset_u64 + data_len as u64).div_ceil(chunk_size);

        for chunk_idx in start_chunk_idx..=end_chunk_idx {
            let chunk_offset = chunk_idx * chunk_size;

            let data_start_in_chunk = if chunk_idx == start_chunk_idx {
                offset_u64 % chunk_size
            } else {
                0
            };
            let data_end_in_chunk = if chunk_idx == end_chunk_idx {
                std::cmp::min(data_start_in_chunk + data_len as u64, chunk_size)
            } else {
                chunk_size
            };

            let src_start = if chunk_idx == start_chunk_idx {
                0
            } else {
                ((chunk_idx - start_chunk_idx) * chunk_size - (offset_u64 % chunk_size)) as usize
            };
            let src_end = src_start + (data_end_in_chunk - data_start_in_chunk) as usize;

            if src_end > data.len() {
                continue;
            }

            let modified = self.chunk_cache.modify(inode, chunk_offset, |chunk| {
                let dst_start = data_start_in_chunk as usize;
                let dst_end = data_end_in_chunk as usize;
                if dst_end <= chunk.data.len() && src_end <= data.len() {
                    chunk.data[dst_start..dst_end].copy_from_slice(&data[src_start..src_end]);
                    chunk.mtime = chrono::Utc::now().timestamp() as u64;
                }
            });

            if !modified {
                let entry = match self.cache.get_inode(inode) {
                    Some(e) => e,
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                };

                let mut initial_data = vec![0u8; chunk_size as usize];

                if let Some(fid) = &entry.fid {
                    let locations = match self.client.lookup_volume(fid.volume_id) {
                        Ok(l) => l,
                        Err(e) => {
                            error!("lookup_volume failed: {}", e);
                            reply.error(libc::EIO);
                            return;
                        }
                    };
                    if let Some(loc) = locations.first() {
                        let addr = PowerFuseClient::location_to_grpc_addr(loc);
                        match self.client.read_blob(
                            &addr,
                            fid.volume_id.0,
                            fid.file_key,
                            chunk_offset as i64,
                            chunk_size as i32,
                        ) {
                            Ok(existing) => {
                                initial_data[..existing.len()].copy_from_slice(&existing);
                            }
                            Err(e) => {
                                warn!("read_blob for write failed: {}", e);
                            }
                        }
                    }
                }

                let dst_start = data_start_in_chunk as usize;
                let dst_end = data_end_in_chunk as usize;
                if dst_end <= initial_data.len() && src_end <= data.len() {
                    initial_data[dst_start..dst_end].copy_from_slice(&data[src_start..src_end]);
                }

                let now = chrono::Utc::now().timestamp() as u64;
                self.chunk_cache
                    .put(inode, chunk_offset, initial_data, now, 0);
            }

            let mut dirty_set = self.dirty_chunks.write().unwrap();
            dirty_set.insert((inode, chunk_idx));
            self.has_dirty
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        reply.written(data_len as u32);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        debug!("flush: inode={}", inode);
        if let Err(e) = self.flush_dirty_chunks(inode) {
            warn!("flush failed: {}", e);
        }
        reply.ok();
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("release: inode={}, flush={}", inode, _flush);
        let write_buffer_entries = self.write_buffer.take(inode);
        if !write_buffer_entries.is_empty() {
            self.flush_write_buffer(inode, &write_buffer_entries);
        }
        if let Err(e) = self.flush_dirty_chunks(inode) {
            warn!("release flush failed: {}", e);
        }
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        debug!("fsync: inode={}", inode);
        if let Err(e) = self.flush_dirty_chunks(inode) {
            warn!("fsync failed: {}", e);
            reply.error(libc::EIO);
            return;
        }
        reply.ok();
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir: inode={}, offset={}", inode, offset);

        let entry = match self.cache.get_inode(inode) {
            Some(e) => e,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if !entry.is_dir {
            reply.error(libc::ENOTDIR);
            return;
        }

        let children = self.cache.list_children(inode);

        let mut idx = offset as usize;
        for (child_inode, ref child_name, _is_dir) in children.iter().skip(idx) {
            let child_entry = match self.cache.get_inode(*child_inode) {
                Some(e) => e,
                None => continue,
            };

            let file_type = if child_entry.is_symlink {
                FileType::Symlink
            } else if child_entry.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };

            if !reply.add(
                *child_inode,
                (idx + 1) as i64,
                file_type,
                child_name.as_str(),
            ) {
                break;
            }
            idx += 1;
        }

        reply.ok();
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let name_str = name.to_str().unwrap_or("");
        let new_name_str = new_name.to_str().unwrap_or("");
        debug!(
            "rename: parent={}, name={}, new_parent={}, new_name={}",
            parent, name_str, new_parent, new_name_str
        );

        let entry = match self.lookup_in_cache(parent, name_str) {
            Some(e) => e,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let old_path = self
            .cache
            .inode_to_path(entry.inode)
            .unwrap_or_else(|| "/".to_string());
        let new_parent_path = self
            .cache
            .inode_to_path(new_parent)
            .unwrap_or_else(|| "/".to_string());

        let target_path = if new_parent_path == "/" {
            format!("/{}", new_name_str)
        } else {
            format!("{}/{}", new_parent_path, new_name_str)
        };

        let target_entry = self.lookup_in_cache(new_parent, new_name_str);

        if let Some(target) = target_entry {
            if target.is_dir && !entry.is_dir {
                reply.error(libc::ENOTDIR);
                return;
            }

            let should_delete_target = self.cache.dec_nlink(target.inode);

            if should_delete_target {
                if let Some(fid) = &target.fid {
                    match self.client.lookup_volume(fid.volume_id) {
                        Ok(locations) => {
                            if let Some(loc) = locations.first() {
                                let addr = PowerFuseClient::location_to_grpc_addr(loc);
                                if let Err(e) =
                                    self.client
                                        .delete_data(&addr, fid.volume_id.0, fid.file_key)
                                {
                                    warn!("Failed to delete target data: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to lookup target volume: {}", e);
                        }
                    }
                }

                if let Err(e) = self.client.delete_entry(&target_path, target.is_dir) {
                    warn!("Failed to delete target entry: {}", e);
                }
            }

            self.cache.remove(target.inode);
        }

        let _ = self
            .cache
            .rename(parent, name_str, new_parent, new_name_str);

        let filer_entry = FilerEntry {
            name: new_name_str.to_string(),
            directory: new_parent_path,
            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                ino: entry.inode,
                mode: if entry.is_dir {
                    entry.mode | 0o040000
                } else {
                    entry.mode | 0o100000
                },
                nlink: entry.nlink,
                uid: entry.uid,
                gid: entry.gid,
                rdev: 0,
                size: entry.size,
                blksize: 4096,
                blocks: entry.size.div_ceil(512),
                atime: entry.atime as u64,
                mtime: entry.mtime as u64,
                ctime: chrono::Utc::now().timestamp() as u64,
                crtime: entry.ctime as u64,
                perm: 0,
            }),
            chunks: entry
                .chunks
                .iter()
                .map(|c| powerfs_master::proto::powerfs::FileChunk {
                    offset: c.offset,
                    size: c.size,
                    mtime: c.mtime,
                    fid: c.fid.clone(),
                    cookie: c.cookie,
                    crc32: c.crc32,
                })
                .collect(),
            hard_link_id: entry.hard_link_id,
            hard_link_counter: entry.hard_link_counter,
            extended: HashMap::new(),
            content_size: entry.content_size,
            disk_size: entry.disk_size,
            ttl: String::new(),
            symlink_target: entry.symlink_target.unwrap_or_default(),
        };

        if let Err(e) = self.client.delete_entry(&old_path, entry.is_dir) {
            warn!("Failed to delete old entry: {}", e);
        }
        if let Err(e) = self.client.create_entry(filer_entry) {
            warn!("Failed to create new entry: {}", e);
        }

        reply.ok();
    }

    fn symlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        link: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let name_str = name.to_str().unwrap_or("");
        let link_str = link.to_str().unwrap_or("");
        debug!(
            "symlink: parent={}, name={}, link={}",
            parent, name_str, link_str
        );

        let inode = self.cache.allocate_inode();
        let now = chrono::Utc::now().timestamp();
        let entry = CachedEntry {
            inode,
            parent,
            name: name_str.to_string(),
            is_dir: false,
            is_symlink: true,
            symlink_target: Some(link_str.to_string()),
            nlink: 1,
            fid: None,
            size: link_str.len() as u64,
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
            content_size: link_str.len() as u64,
            disk_size: 0,
        };
        self.cache.insert(entry.clone());

        let parent_path = self
            .cache
            .inode_to_path(parent)
            .unwrap_or_else(|| "/".to_string());
        let filer_entry = FilerEntry {
            name: name_str.to_string(),
            directory: parent_path,
            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                ino: inode,
                mode: 0o120777,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                size: link_str.len() as u64,
                blksize: 4096,
                blocks: 0,
                atime: now as u64,
                mtime: now as u64,
                ctime: now as u64,
                crtime: now as u64,
                perm: 0,
            }),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            extended: HashMap::new(),
            content_size: link_str.len() as u64,
            disk_size: 0,
            ttl: String::new(),
            symlink_target: link_str.to_string(),
        };

        if let Err(e) = self.client.create_entry(filer_entry) {
            warn!("Failed to create symlink entry on master: {}", e);
        }

        let attr = self.create_file_attr(&entry);
        reply.entry(&TTL, &attr, 0);
    }

    fn readlink(&mut self, _req: &Request<'_>, inode: u64, reply: ReplyData) {
        debug!("readlink: inode={}", inode);

        let entry = match self.cache.get_inode(inode) {
            Some(e) => e,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if !entry.is_symlink {
            reply.error(libc::EINVAL);
            return;
        }

        if let Some(target) = &entry.symlink_target {
            reply.data(target.as_bytes());
        } else {
            reply.data(&[]);
        }
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        new_parent: u64,
        new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        let new_name_str = new_name.to_str().unwrap_or("");
        debug!(
            "link: inode={}, new_parent={}, new_name={}",
            inode, new_parent, new_name_str
        );

        let entry = match self.cache.get_inode(inode) {
            Some(e) => e,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        self.cache.inc_nlink(inode);

        let new_inode = self.cache.allocate_inode();
        let now = chrono::Utc::now().timestamp();
        let new_entry = CachedEntry {
            inode: new_inode,
            parent: new_parent,
            name: new_name_str.to_string(),
            is_dir: entry.is_dir,
            is_symlink: entry.is_symlink,
            symlink_target: entry.symlink_target.clone(),
            nlink: entry.nlink + 1,
            fid: entry.fid.clone(),
            size: entry.size,
            mode: entry.mode,
            uid: entry.uid,
            gid: entry.gid,
            atime: entry.atime,
            mtime: entry.mtime,
            ctime: now,
            xattrs: entry.xattrs.clone(),
            chunks: entry.chunks.clone(),
            hard_link_id: entry.hard_link_id.clone(),
            hard_link_counter: entry.hard_link_counter + 1,
            content_size: entry.content_size,
            disk_size: entry.disk_size,
        };
        self.cache.insert(new_entry.clone());

        let parent_path = self
            .cache
            .inode_to_path(new_parent)
            .unwrap_or_else(|| "/".to_string());
        let filer_entry = FilerEntry {
            name: new_name_str.to_string(),
            directory: parent_path,
            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                ino: new_inode,
                mode: if entry.is_dir {
                    entry.mode | 0o040000
                } else {
                    entry.mode | 0o100000
                },
                nlink: entry.nlink + 1,
                uid: entry.uid,
                gid: entry.gid,
                rdev: 0,
                size: entry.size,
                blksize: 4096,
                blocks: entry.size.div_ceil(512),
                atime: entry.atime as u64,
                mtime: entry.mtime as u64,
                ctime: now as u64,
                crtime: entry.ctime as u64,
                perm: 0,
            }),
            chunks: entry
                .chunks
                .iter()
                .map(|c| powerfs_master::proto::powerfs::FileChunk {
                    offset: c.offset,
                    size: c.size,
                    mtime: c.mtime,
                    fid: c.fid.clone(),
                    cookie: c.cookie,
                    crc32: c.crc32,
                })
                .collect(),
            hard_link_id: entry.hard_link_id.clone(),
            hard_link_counter: entry.hard_link_counter + 1,
            extended: HashMap::new(),
            content_size: entry.content_size,
            disk_size: entry.disk_size,
            ttl: String::new(),
            symlink_target: entry.symlink_target.clone().unwrap_or_default(),
        };

        if let Err(e) = self.client.create_entry(filer_entry) {
            warn!("Failed to create hard link entry on master: {}", e);
        }

        let attr = self.create_file_attr(&new_entry);
        reply.entry(&TTL, &attr, 0);
    }

    fn statfs(&mut self, _req: &Request<'_>, _inode: u64, reply: ReplyStatfs) {
        debug!("statfs");
        reply.statfs(
            1024 * 1024 * 1024,
            1024 * 1024 * 1024,
            1024 * 1024 * 1024,
            1000000,
            1000000,
            4096,
            255,
            4096,
        );
    }
}

pub struct FuserApp {
    mount_point: String,
    master_addr: String,
    collection: String,
    replication: String,
    num_threads: usize,
    runtime_handle: Handle,
}

impl FuserApp {
    pub async fn new(
        master_addr: &str,
        mount_point: &str,
        collection: &str,
        replication: &str,
        num_threads: usize,
    ) -> Result<Self> {
        let runtime_handle = Handle::try_current()
            .map_err(|e| PowerFsError::Internal(format!("no tokio runtime: {}", e)))?;

        Ok(Self {
            mount_point: mount_point.to_string(),
            master_addr: master_addr.to_string(),
            collection: collection.to_string(),
            replication: replication.to_string(),
            num_threads,
            runtime_handle,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting FUSE session on {} with master {} ({} threads)",
            self.mount_point, self.master_addr, self.num_threads
        );

        let grpc_client = PowerFuseClient::new(&self.master_addr, self.runtime_handle.clone());
        let sync_client = Arc::new(SyncFuseClient::new(grpc_client));

        let cache = Arc::new(MetadataCache::new());
        let chunk_cache = Arc::new(ChunkCache::with_defaults());
        let write_buffer = Arc::new(WriteBuffer::new(64));

        let fs = PowerFsFuserFs::new(
            sync_client.clone(),
            cache.clone(),
            chunk_cache.clone(),
            self.collection.clone(),
            self.replication.clone(),
            write_buffer.clone(),
        );

        let fs_clone = fs.clone();
        std::thread::spawn(move || loop {
            if fs_clone
                .has_dirty
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                let _ = fs_clone.flush_all_dirty_chunks();
                fs_clone
                    .has_dirty
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }
            std::thread::sleep(Duration::from_millis(100));
        });

        let options = vec![
            MountOption::FSName("powerfs".to_string()),
            MountOption::AutoUnmount,
            MountOption::DefaultPermissions,
        ];

        let (sender, receiver) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                tokio::signal::ctrl_c().await.ok();
                let _ = sender.send(());
            });
        });

        let fs_for_mount = fs.clone();
        let mount_point_clone = self.mount_point.clone();
        let options_clone = options.clone();

        let session_handle = std::thread::Builder::new()
            .name("fuse_server".to_string())
            .spawn(move || {
                info!("FUSE server started");
                let _ = fuser::spawn_mount2(fs_for_mount, mount_point_clone, &options_clone);
                warn!("FUSE server exited");
            })
            .map_err(|e| PowerFsError::Internal(format!("failed to spawn fuse thread: {}", e)))?;

        let _ = receiver.recv();

        info!("Received Ctrl+C, unmounting...");

        session_handle.join().ok();

        info!("FUSE session ended");
        Ok(())
    }
}
