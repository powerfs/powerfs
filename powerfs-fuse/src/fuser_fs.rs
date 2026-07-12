use crate::cache::{ChunkCache, MetadataCache};
use crate::client::{PowerFuseClient, SyncFuseClient};
use crate::error::parse_master_error;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use log::{debug, error, info, warn};
use powerfs_common::error::{PowerFsError, Result};
use powerfs_common::types::Fid;
use powerfs_master::proto::powerfs::{Entry as FilerEntry, MetadataNotification};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use uuid;

const TTL: Duration = Duration::from_secs(0);

pub struct WriteBufferEntry {
    pub offset: u64,
    pub data: Vec<u8>,
}

pub struct WriteBuffer {
    buffers: RwLock<HashMap<u64, Vec<WriteBufferEntry>>>,
    max_entries: usize,
}

impl WriteBuffer {
    pub fn new(max_entries: usize) -> Self {
        Self {
            buffers: RwLock::new(HashMap::new()),
            max_entries,
        }
    }

    pub fn add(&self, inode: u64, offset: u64, data: &[u8]) -> bool {
        let mut buffers = self.buffers.write().unwrap();
        let entries = buffers.entry(inode).or_default();

        let entry = WriteBufferEntry {
            offset,
            data: data.to_vec(),
        };
        entries.push(entry);

        entries.len() >= self.max_entries
    }

    pub fn take(&self, inode: u64) -> Vec<WriteBufferEntry> {
        let mut buffers = self.buffers.write().unwrap();
        buffers.remove(&inode).unwrap_or_default()
    }

    pub fn get_max_write_offset(&self, inode: u64) -> u64 {
        let buffers = self.buffers.read().unwrap();
        if let Some(entries) = buffers.get(&inode) {
            entries
                .iter()
                .map(|e| e.offset + e.data.len() as u64)
                .max()
                .unwrap_or(0)
        } else {
            0
        }
    }
}

#[derive(Clone)]
struct LeaseInfo {
    lease_id: String,
    path: String,
    duration_ms: u64,
    acquired_at: std::time::Instant,
}

struct PowerFsFuserFs {
    client: Arc<SyncFuseClient>,
    chunk_cache: Arc<ChunkCache>,
    collection: String,
    replication: String,
    dirty_chunks: Arc<RwLock<HashSet<(u64, u64)>>>,
    has_dirty: Arc<std::sync::atomic::AtomicBool>,
    write_buffer: Arc<WriteBuffer>,
    leases: Arc<RwLock<HashMap<u64, Vec<LeaseInfo>>>>,
    master_epoch: Arc<std::sync::atomic::AtomicU64>,
    client_id: String,
    job_id: String,
    notifier: Arc<std::sync::Mutex<Option<fuser::Notifier>>>,
    pending_requests: Arc<RwLock<HashMap<u64, CancellationToken>>>,
    request_id_counter: Arc<std::sync::atomic::AtomicU64>,
    flush_locks: Arc<RwLock<HashMap<u64, Arc<std::sync::Mutex<()>>>>>,
}

impl PowerFsFuserFs {
    #[allow(clippy::too_many_arguments)]
    fn new(
        client: Arc<SyncFuseClient>,
        chunk_cache: Arc<ChunkCache>,
        collection: String,
        replication: String,
        write_buffer: Arc<WriteBuffer>,
        client_id: String,
        job_id: String,
    ) -> Self {
        Self {
            client,
            chunk_cache,
            collection,
            replication,
            dirty_chunks: Arc::new(RwLock::new(HashSet::new())),
            has_dirty: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            write_buffer,
            leases: Arc::new(RwLock::new(HashMap::new())),
            master_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            client_id,
            job_id,
            notifier: Arc::new(std::sync::Mutex::new(None)),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            request_id_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            flush_locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn invalidate_kernel_dentry(&self, parent: u64, name: &str) {
        let notifier = self.notifier.clone();
        let name = name.to_string();
        std::thread::spawn(move || {
            let notifier_guard = notifier.lock().unwrap();
            if let Some(n) = notifier_guard.as_ref() {
                if let Err(e) = n.inval_entry(parent, OsStr::new(&name)) {
                    debug!(
                        "Failed to invalidate kernel dentry (parent={}, name={}): {}",
                        parent, name, e
                    );
                }
            }
        });
    }

    fn invalidate_kernel_inode(&self, inode: u64) {
        let notifier = self.notifier.clone();
        std::thread::spawn(move || {
            let notifier_guard = notifier.lock().unwrap();
            if let Some(n) = notifier_guard.as_ref() {
                if let Err(e) = n.inval_inode(inode, 0, -1) {
                    debug!("Failed to invalidate kernel inode ({}): {}", inode, e);
                }
            }
        });
    }

    #[allow(dead_code)]
    fn register_request(&self) -> u64 {
        let request_id = self
            .request_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let token = CancellationToken::new();
        self.pending_requests
            .write()
            .unwrap()
            .insert(request_id, token);
        request_id
    }

    #[allow(dead_code)]
    fn unregister_request(&self, request_id: u64) {
        self.pending_requests.write().unwrap().remove(&request_id);
    }

    #[allow(dead_code)]
    fn cancel_request(&self, request_id: u64) {
        if let Some(token) = self.pending_requests.write().unwrap().remove(&request_id) {
            token.cancel();
            debug!("Cancelled request {}", request_id);
        }
    }

    fn flush_dirty_chunks(&self, inode: u64, max_write_offset: u64) -> std::io::Result<()> {
        let flush_lock = {
            let mut locks = self.flush_locks.write().unwrap();
            locks
                .entry(inode)
                .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
                .clone()
        };
        let _guard = flush_lock.lock().unwrap();

        let dirty: Vec<(u64, u64)> = {
            let dirty_set = self.dirty_chunks.read().unwrap();
            dirty_set
                .iter()
                .filter(|(ino, _)| *ino == inode)
                .cloned()
                .collect()
        };

        let chunk_size = self.chunk_cache.chunk_size();
        let actual_max_write_offset = if max_write_offset > 0 {
            max_write_offset
        } else {
            let mut computed_max = 0;
            let mut max_chunk_offset = 0;
            for (_, chunk_idx) in &dirty {
                let chunk_offset = chunk_idx * chunk_size;
                if chunk_offset > max_chunk_offset {
                    max_chunk_offset = chunk_offset;
                }
            }
            if max_chunk_offset > 0 {
                if let Some(chunk_data) = self.chunk_cache.get(inode, max_chunk_offset) {
                    for (i, byte) in chunk_data.data.iter().enumerate().rev() {
                        if *byte != 0 {
                            computed_max = max_chunk_offset + i as u64 + 1;
                            break;
                        }
                    }
                }
            }
            computed_max
        };

        println!("[FLUSH_DIRTY] START inode={}, dirty_chunks={}, max_write_offset={}, actual_max_write_offset={}", inode, dirty.len(), max_write_offset, actual_max_write_offset);
        info!(
            "[FLUSH_DIRTY] START inode={}, dirty_chunks={}, max_write_offset={}, actual_max_write_offset={}",
            inode,
            dirty.len(),
            max_write_offset,
            actual_max_write_offset
        );

        if dirty.is_empty() {
            info!(
                "[FLUSH_DIRTY] No dirty chunks for inode={}, returning OK",
                inode
            );
            return Ok(());
        }

        info!("[FLUSH_DIRTY] BEFORE get_entry_by_inode: inode={}", inode);
        let (entry, path) = match self.client.get_entry_by_inode(inode) {
            Ok(Some(e)) => {
                let attrs = e.0.attributes.as_ref();
                let mode_val = attrs.map(|a| a.mode).unwrap_or(0);
                let file_type = mode_val & 0o170000;
                info!(
                    "[FLUSH_DIRTY] AFTER get_entry_by_inode: inode={}, entry.name={}, path={}, chunks.len={}, mode={:o}, file_type={:o}, is_symlink={}, symlink_target='{}'",
                    inode, e.0.name, e.1, e.0.chunks.len(), mode_val, file_type, file_type == 0o120000, e.0.symlink_target
                );
                e
            }
            Ok(None) => {
                error!(
                    "[FLUSH_DIRTY] get_entry_by_inode returned None for inode={}",
                    inode
                );
                return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("[FLUSH_DIRTY] get_entry_by_inode failed: {}", fs_error);
                return Err(std::io::Error::from_raw_os_error(fs_error.to_errno()));
            }
        };

        let existing_fid = entry
            .chunks
            .iter()
            .find(|c| !c.fid.is_empty())
            .and_then(|c| Fid::from_string(&c.fid).ok());

        info!("[FLUSH_DIRTY] existing_fid: {:?}", existing_fid);

        let fid = if let Some(fid) = existing_fid {
            info!("[FLUSH_DIRTY] Using existing fid: {}", fid);
            fid
        } else {
            info!("[FLUSH_DIRTY] No existing fid, assigning new one");
            let (new_fid, _, _, _) = self
                .client
                .assign_fid(&self.collection, &self.replication)
                .map_err(|e| {
                    let fs_error = parse_master_error(&e);
                    error!("[FLUSH_DIRTY] assign_fid failed: {}", fs_error);
                    std::io::Error::from_raw_os_error(fs_error.to_errno())
                })?;

            info!(
                "[FLUSH_DIRTY] Assigned new fid for inode {}: {}",
                inode, new_fid
            );
            new_fid
        };

        info!(
            "[FLUSH_DIRTY] fid.volume_id={}, fid.file_key={}",
            fid.volume_id, fid.file_key
        );

        let locations = self.client.lookup_volume(fid.volume_id).map_err(|e| {
            let fs_error = parse_master_error(&e);
            error!("[FLUSH_DIRTY] lookup_volume failed: {}", fs_error);
            std::io::Error::from_raw_os_error(fs_error.to_errno())
        })?;

        info!("[FLUSH_DIRTY] Found {} volume locations", locations.len());

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
            info!(
                "[FLUSH_DIRTY] Writing {} entries to volume addr={}",
                entries.len(),
                addr
            );
            self.client
                .batch_write_blob(&addr, fid.volume_id.0, fid.file_key, entries)
                .map_err(|e| {
                    let fs_error = crate::error::parse_volume_error(&e);
                    error!("[FLUSH_DIRTY] batch_write_blob FAILED: {}", fs_error);
                    std::io::Error::from_raw_os_error(fs_error.to_errno())
                })?;
            info!("[FLUSH_DIRTY] batch_write_blob OK");
        } else {
            info!("[FLUSH_DIRTY] No entries to write to volume");
        }

        let directory = if let Some(last_slash) = path.rfind('/') {
            if last_slash == 0 {
                "/".to_string()
            } else {
                path[..last_slash].to_string()
            }
        } else {
            "/".to_string()
        };

        info!(
            "[FLUSH_DIRTY] path.is_empty()={}, chunks.is_empty()={}",
            path.is_empty(),
            chunks.is_empty()
        );
        if !path.is_empty() && !chunks.is_empty() {
            let attrs = entry.attributes.as_ref();

            let mut content_size = entry.content_size;
            let mut size = attrs.map(|a| a.size).unwrap_or(0);

            info!(
                "[FLUSH_DIRTY] BEFORE size update: entry.content_size={}, attrs.size={}, max_write_offset={}",
                content_size, size, max_write_offset
            );

            if actual_max_write_offset > 0 {
                if actual_max_write_offset > size {
                    size = actual_max_write_offset;
                    content_size = actual_max_write_offset;
                    info!(
                        "[FLUSH_DIRTY] AFTER size update: expanded size from {} to {}",
                        attrs.map(|a| a.size).unwrap_or(0),
                        actual_max_write_offset
                    );
                } else if actual_max_write_offset < size {
                    size = actual_max_write_offset;
                    content_size = actual_max_write_offset;
                    info!(
                        "[FLUSH_DIRTY] AFTER size update: truncated size from {} to {}",
                        attrs.map(|a| a.size).unwrap_or(0),
                        actual_max_write_offset
                    );
                }
            } else {
                info!("[FLUSH_DIRTY] Skipping size update: max_write_offset is 0, keeping existing size {}", size);
                content_size = entry.content_size;
                size = attrs.map(|a| a.size).unwrap_or(0);
            }

            info!(
                "[FLUSH_DIRTY] FINAL size values: content_size={}, size={}",
                content_size, size
            );

            let mode_val = attrs.map(|a| a.mode).unwrap_or(0);
            let file_type = mode_val & 0o170000;
            let is_symlink = file_type == 0o120000;

            let is_symlink_v2 = !entry.symlink_target.is_empty();
            let is_dir = attrs
                .map(|a| (a.mode & 0o170000) == 0o040000)
                .unwrap_or(false);

            info!(
            "[FLUSH_DIRTY] FILE_TYPE_DETERMINATION: is_symlink_v1(mode_based)={}, is_symlink_v2(target_based)={}, symlink_target_len={}, symlink_target_debug={:?}",
            is_symlink, is_symlink_v2, entry.symlink_target.len(), entry.symlink_target
        );

            let file_type_mode = if is_dir {
                0o040000
            } else if is_symlink_v2 {
                0o120000
            } else {
                0o100000
            };

            let mode_val = (attrs.map(|a| a.mode & 0o7777).unwrap_or(0o644)) | file_type_mode;

            info!(
                "[FLUSH_DIRTY] FINAL_MODE: mode_val={:o}, file_type_mode={:o}, permissions={:o}",
                mode_val,
                file_type_mode,
                attrs.map(|a| a.mode & 0o7777).unwrap_or(0o644)
            );

            let filer_entry = powerfs_master::proto::powerfs::Entry {
                name: entry.name,
                directory,
                attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                    ino: inode,
                    mode: mode_val,
                    nlink: attrs.map(|a| a.nlink).unwrap_or(1),
                    uid: attrs.map(|a| a.uid).unwrap_or(0),
                    gid: attrs.map(|a| a.gid).unwrap_or(0),
                    rdev: 0,
                    size,
                    blksize: 4096,
                    blocks: size.div_ceil(512),
                    atime: attrs.map(|a| a.atime).unwrap_or(0),
                    mtime: chrono::Utc::now().timestamp() as u64,
                    ctime: chrono::Utc::now().timestamp() as u64,
                    crtime: attrs.map(|a| a.crtime).unwrap_or(0),
                    perm: 0,
                }),
                chunks,
                hard_link_id: entry.hard_link_id,
                hard_link_counter: entry.hard_link_counter,
                extended: HashMap::new(),
                content_size,
                disk_size: entry.disk_size,
                ttl: String::new(),
                symlink_target: entry.symlink_target,
                owner: String::new(),
                generation: entry.generation,
            };

            if let Err(e) = self.client.update_entry(&filer_entry, &self.client_id) {
                error!("[FLUSH_DIRTY] update_entry FAILED: {}", e);
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            } else {
                info!(
                    "[FLUSH_DIRTY] update_entry OK, invalidating inode {}",
                    inode
                );
                self.invalidate_kernel_inode(inode);
            }
        } else {
            info!(
                "[FLUSH_DIRTY] Skipping update_entry: path empty={}, chunks empty={}",
                path.is_empty(),
                chunks.is_empty()
            );
        }

        let mut dirty_set = self.dirty_chunks.write().unwrap();
        dirty_set.retain(|(ino, _)| *ino != inode);
        info!("[FLUSH_DIRTY] Cleaned dirty_chunks for inode={}", inode);

        info!("[FLUSH_DIRTY] END inode={}, returning OK", inode);
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
            let max_write_offset = self.write_buffer.get_max_write_offset(inode);
            let _ = self.flush_dirty_chunks(inode, max_write_offset);
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

            for chunk_idx in start_chunk_idx..end_chunk_idx {
                let _chunk_offset = chunk_idx * chunk_size;
                let data_start_in_chunk = if chunk_idx == start_chunk_idx {
                    entry.offset % chunk_size
                } else {
                    0
                };
                let data_end_in_chunk = if chunk_idx == end_chunk_idx - 1 {
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

            let existing_chunk = self.chunk_cache.get(inode, chunk_offset);
            info!(
                "flush_write_buffer: inode={}, chunk_idx={}, has_existing_chunk={}, data_non_zero={}",
                inode,
                chunk_idx,
                existing_chunk.is_some(),
                data.iter().filter(|&&b| b != 0).count()
            );

            if let Some(existing_chunk) = existing_chunk {
                let mut merged_chunk = existing_chunk.data.clone();
                let mut changed = false;
                for (i, byte) in data.iter().enumerate() {
                    if *byte != 0 {
                        merged_chunk[i] = *byte;
                        changed = true;
                    }
                }
                let merged_non_zero = merged_chunk.iter().filter(|&&b| b != 0).count();
                info!(
                    "flush_write_buffer: merged existing chunk, non-zero bytes after merge: {}",
                    merged_non_zero
                );
                if changed {
                    self.chunk_cache
                        .put(inode, chunk_offset, merged_chunk, now, 0);
                }
            } else {
                self.chunk_cache.put(inode, chunk_offset, data, now, 0);
            }

            let mut dirty_set = self.dirty_chunks.write().unwrap();
            dirty_set.insert((inode, chunk_idx));
            self.has_dirty
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn create_file_attr_from_entry(&self, entry: &FilerEntry) -> FileAttr {
        let attrs = entry.attributes.as_ref();
        let mode_val = attrs.map(|a| a.mode).unwrap_or(0);
        let file_type = mode_val & 0o170000;

        let kind = match file_type {
            0o040000 => FileType::Directory,
            0o120000 => FileType::Symlink,
            _ => FileType::RegularFile,
        };

        FileAttr {
            ino: attrs.map(|a| a.ino).unwrap_or(0),
            size: attrs.map(|a| a.size).unwrap_or(0),
            blocks: attrs.map(|a| a.size.div_ceil(512)).unwrap_or(0),
            atime: std::time::UNIX_EPOCH
                + std::time::Duration::from_secs(attrs.map(|a| a.atime).unwrap_or(0)),
            mtime: std::time::UNIX_EPOCH
                + std::time::Duration::from_secs(attrs.map(|a| a.mtime).unwrap_or(0)),
            ctime: std::time::UNIX_EPOCH
                + std::time::Duration::from_secs(attrs.map(|a| a.ctime).unwrap_or(0)),
            crtime: std::time::UNIX_EPOCH
                + std::time::Duration::from_secs(attrs.map(|a| a.ctime).unwrap_or(0)),
            kind,
            perm: (attrs.map(|a| a.mode & 0o7777).unwrap_or(0o644)) as u16,
            nlink: attrs.map(|a| a.nlink).unwrap_or(1),
            uid: attrs.map(|a| a.uid).unwrap_or(0),
            gid: attrs.map(|a| a.gid).unwrap_or(0),
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn readdir_root(&self, mut reply: ReplyDirectory, offset: i64) {
        let idx = offset as usize;

        if idx == 0 && !reply.add(1, 1, FileType::Directory, ".") {
            reply.ok();
            return;
        }

        if idx <= 1 && !reply.add(1, 2, FileType::Directory, "..") {
            reply.ok();
            return;
        }

        match self.client.list_entries(1, 1000, "") {
            Ok(entries) => {
                for (i, entry) in entries.iter().enumerate() {
                    let entry_idx = 2 + i;
                    if entry_idx >= idx {
                        let child_ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
                        let mode_val = entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0);
                        let file_type = mode_val & 0o170000;

                        let kind = match file_type {
                            0o040000 => FileType::Directory,
                            0o120000 => FileType::Symlink,
                            _ => FileType::RegularFile,
                        };

                        let next_offset = (entry_idx + 1) as i64;
                        if !reply.add(child_ino, next_offset, kind, &entry.name) {
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                error!("readdir_root: list_entries failed: {}", e);
            }
        }

        reply.ok();
    }
}

impl Clone for PowerFsFuserFs {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            chunk_cache: self.chunk_cache.clone(),
            collection: self.collection.clone(),
            replication: self.replication.clone(),
            dirty_chunks: self.dirty_chunks.clone(),
            has_dirty: self.has_dirty.clone(),
            write_buffer: self.write_buffer.clone(),
            leases: self.leases.clone(),
            master_epoch: self.master_epoch.clone(),
            client_id: self.client_id.clone(),
            job_id: self.job_id.clone(),
            notifier: self.notifier.clone(),
            pending_requests: self.pending_requests.clone(),
            request_id_counter: self.request_id_counter.clone(),
            flush_locks: self.flush_locks.clone(),
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

        match self.client.lookup_directory_entry(parent, name_str) {
            Ok(Some(entry)) => {
                info!(
                    "lookup found entry: name={}, chunks={}, content_size={}",
                    entry.name,
                    entry.chunks.len(),
                    entry.content_size
                );
                let attr = self.create_file_attr_from_entry(&entry);
                reply.entry(&TTL, &attr, 0);
            }
            Ok(None) => reply.error(libc::ENOENT),
            Err(e) => {
                warn!("lookup entry failed: {}", e);
                reply.error(libc::ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, inode: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if inode == 1 {
            let attr = FileAttr {
                ino: 1,
                size: 0,
                blocks: 0,
                atime: std::time::UNIX_EPOCH,
                mtime: std::time::UNIX_EPOCH,
                ctime: std::time::UNIX_EPOCH,
                crtime: std::time::UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o777,
                nlink: 2,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                rdev: 0,
                blksize: 4096,
                flags: 0,
            };
            reply.attr(&TTL, &attr);
            return;
        }

        match self.client.get_entry_by_inode(inode) {
            Ok(Some((entry, _))) => {
                let attr = self.create_file_attr_from_entry(&entry);
                debug!(
                    "getattr: inode={}, name='{}', mode={:o}, size={}",
                    inode, entry.name, attr.perm, attr.size
                );
                reply.attr(&TTL, &attr);
            }
            Ok(None) => {
                error!(
                    "getattr: inode {} not found on master (returned None)",
                    inode
                );
                reply.error(libc::ENOENT);
            }
            Err(e) => {
                error!("getattr: inode {} failed: {}", inode, e);
                reply.error(libc::ENOENT);
            }
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
        info!(
            "[SETATTR] START inode={}, mode={:?}, uid={:?}, gid={:?}, size={:?}, atime={:?}, mtime={:?}",
            inode, mode, uid, gid, size, atime, mtime
        );

        let (entry, path) = match self.client.get_entry_by_inode(inode) {
            Ok(Some(e)) => {
                let attrs = e.0.attributes.as_ref();
                let mode_val = attrs.map(|a| a.mode).unwrap_or(0);
                let file_type = mode_val & 0o170000;
                let entry_size = e.0.attributes.as_ref().map(|a| a.size).unwrap_or(0);
                let entry_content_size = e.0.content_size;
                info!(
                    "[SETATTR] get_entry_by_inode OK: entry.name={}, entry.directory={}, path={}, mode={:o}, file_type={:o}, is_symlink={}, symlink_target='{}', size={}, content_size={}",
                    e.0.name, e.0.directory, e.1, mode_val, file_type, file_type == 0o120000, e.0.symlink_target, entry_size, entry_content_size
                );
                e
            }
            Ok(None) => {
                error!(
                    "[SETATTR] get_entry_by_inode returned None for inode={}",
                    inode
                );
                reply.error(libc::ENOENT);
                return;
            }
            Err(e) => {
                error!(
                    "[SETATTR] get_entry_by_inode failed for inode={}: {}",
                    inode, e
                );
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

        let attrs = entry.attributes.as_ref();
        let mode_val = attrs.map(|a| a.mode).unwrap_or(0);
        let file_type_bits = mode_val & 0o170000;
        let is_dir = file_type_bits == 0o040000;
        let is_symlink = file_type_bits == 0o120000;

        let new_mode = mode.unwrap_or_else(|| attrs.map(|a| a.mode & 0o7777).unwrap_or(0o644));
        let new_uid = uid.unwrap_or_else(|| attrs.map(|a| a.uid).unwrap_or(0));
        let new_gid = gid.unwrap_or_else(|| attrs.map(|a| a.gid).unwrap_or(0));
        let new_size = size.unwrap_or_else(|| attrs.map(|a| a.size).unwrap_or(0));
        let new_atime = atime_val.unwrap_or_else(|| attrs.map(|a| a.atime as i64).unwrap_or(0));
        let new_mtime = mtime_val.unwrap_or_else(|| attrs.map(|a| a.mtime as i64).unwrap_or(0));
        let new_nlink = attrs.map(|a| a.nlink).unwrap_or(1);
        let old_ctime = attrs.map(|a| a.ctime).unwrap_or(0);

        let directory = if let Some(last_slash) = path.rfind('/') {
            if last_slash == 0 {
                "/".to_string()
            } else {
                path[..last_slash].to_string()
            }
        } else {
            "/".to_string()
        };

        let final_mode = if is_dir {
            new_mode | 0o040000
        } else if is_symlink {
            new_mode | 0o120000
        } else {
            new_mode | 0o100000
        };

        info!(
            "[SETATTR] filer_entry mode construction: is_dir={}, is_symlink={}, new_mode={:o}, final_mode={:o}",
            is_dir, is_symlink, new_mode, final_mode
        );

        let filer_entry = FilerEntry {
            name: entry.name.clone(),
            directory,
            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                ino: inode,
                mode: final_mode,
                nlink: new_nlink,
                uid: new_uid,
                gid: new_gid,
                rdev: 0,
                size: new_size,
                blksize: 4096,
                blocks: new_size.div_ceil(512),
                atime: new_atime as u64,
                mtime: new_mtime as u64,
                ctime: now as u64,
                crtime: old_ctime,
                perm: 0,
            }),
            chunks: entry.chunks,
            hard_link_id: entry.hard_link_id,
            hard_link_counter: entry.hard_link_counter,
            extended: HashMap::new(),
            content_size: entry.content_size,
            disk_size: entry.disk_size,
            ttl: String::new(),
            symlink_target: entry.symlink_target,
            owner: String::new(),
            generation: entry.generation,
        };

        info!(
            "[SETATTR] Calling update_entry: path={}, name={}, uid={}, gid={}, mode={:o}",
            path, entry.name, new_uid, new_gid, new_mode
        );

        match self.client.update_entry(&filer_entry, &self.client_id) {
            Ok(_) => {
                info!("[SETATTR] update_entry OK for path={}", path);
                match self.client.get_entry(&path) {
                    Ok(Some(updated_entry)) => {
                        let updated_mode = updated_entry
                            .attributes
                            .as_ref()
                            .map(|a| a.mode)
                            .unwrap_or(0);
                        let updated_size = updated_entry
                            .attributes
                            .as_ref()
                            .map(|a| a.size)
                            .unwrap_or(0);
                        info!(
                            "[SETATTR] get_entry after update OK: name={}, uid={}, gid={}, mode={:o}, size={}",
                            updated_entry.name,
                            updated_entry.attributes.as_ref().map(|a| a.uid).unwrap_or(0),
                            updated_entry.attributes.as_ref().map(|a| a.gid).unwrap_or(0),
                            updated_mode,
                            updated_size,
                        );
                        let new_attr = self.create_file_attr_from_entry(&updated_entry);
                        reply.attr(&TTL, &new_attr);
                    }
                    Ok(None) => {
                        error!(
                            "[SETATTR] get_entry after update returned None for path={}",
                            path
                        );
                        reply.error(libc::ENOENT);
                    }
                    Err(e) => {
                        error!(
                            "[SETATTR] get_entry after update failed for path={}: {}",
                            path, e
                        );
                        reply.error(libc::ENOENT);
                    }
                }
            }
            Err(e) => {
                error!("[SETATTR] update_entry FAILED for path={}: {}", path, e);
                reply.error(libc::EIO);
            }
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
        info!(
            "mkdir: parent={}, name={}, mode={:o}",
            parent, name_str, mode
        );

        let parent_path = match self.client.get_entry_by_inode(parent) {
            Ok(Some((_, p))) => p,
            Ok(None) => {
                error!("mkdir: parent inode {} not found on master", parent);
                reply.error(libc::ENOENT);
                return;
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("mkdir: failed to get parent entry: {}", fs_error);
                reply.error(fs_error.to_errno());
                return;
            }
        };

        debug!(
            "mkdir: parent_inode={}, parent_path='{}'",
            parent, parent_path
        );

        let now = chrono::Utc::now().timestamp();
        let dir_path = if parent_path == "/" || parent_path.is_empty() {
            format!("/{}", name_str)
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        debug!("mkdir: dir_path='{}'", dir_path);

        match self.client.get_entry(&dir_path) {
            Ok(Some(_)) => {
                debug!("mkdir: directory '{}' already exists", dir_path);
                reply.error(libc::EEXIST);
                return;
            }
            Ok(None) => {}
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("mkdir: lookup failed: {}", fs_error);
                reply.error(fs_error.to_errno());
                return;
            }
        }

        let filer_entry = FilerEntry {
            name: name_str.to_string(),
            directory: parent_path.clone(),
            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                ino: 0,
                mode: mode | 0o040000,
                nlink: 2,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
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
            owner: String::new(),
            generation: 0,
        };

        match self.client.create_entry(filer_entry, &self.client_id) {
            Ok(master_inode) => {
                match self.client.get_entry_by_inode(master_inode) {
                    Ok(Some((entry, _))) => {
                        let attr = self.create_file_attr_from_entry(&entry);
                        reply.entry(&TTL, &attr, 0);
                    }
                    Ok(None) => {
                        let attr = FileAttr {
                            ino: master_inode,
                            size: 0,
                            blocks: 0,
                            atime: std::time::UNIX_EPOCH
                                + std::time::Duration::from_secs(now as u64),
                            mtime: std::time::UNIX_EPOCH
                                + std::time::Duration::from_secs(now as u64),
                            ctime: std::time::UNIX_EPOCH
                                + std::time::Duration::from_secs(now as u64),
                            crtime: std::time::UNIX_EPOCH
                                + std::time::Duration::from_secs(now as u64),
                            kind: FileType::Directory,
                            perm: (mode & 0o7777) as u16,
                            nlink: 2,
                            uid: 0,
                            gid: 0,
                            rdev: 0,
                            blksize: 4096,
                            flags: 0,
                        };
                        reply.entry(&TTL, &attr, 0);
                    }
                    Err(e) => {
                        let fs_error = parse_master_error(&e);
                        error!("mkdir: failed to get created entry: {}", fs_error);
                        reply.error(fs_error.to_errno());
                        return;
                    }
                }
                self.invalidate_kernel_dentry(parent, name_str);
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("Failed to create directory entry on master: {}", fs_error);
                reply.error(fs_error.to_errno());
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap_or("");
        debug!("rmdir: parent={}, name={}", parent, name_str);

        let parent_path = match self.client.get_entry_by_inode(parent) {
            Ok(Some((_, p))) => p,
            Ok(None) => {
                error!("rmdir: parent inode {} not found on master", parent);
                reply.error(libc::ENOENT);
                return;
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("rmdir: failed to get parent entry: {}", fs_error);
                reply.error(fs_error.to_errno());
                return;
            }
        };

        let dir_path = if parent_path == "/" {
            format!("/{}", name_str)
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        match self.client.get_entry(&dir_path) {
            Ok(Some(entry)) => {
                let attrs = entry.attributes.as_ref();
                if attrs.is_none() || (attrs.unwrap().mode & 0o170000) != 0o040000 {
                    reply.error(libc::ENOTDIR);
                    return;
                }

                let ino = attrs.unwrap().ino;
                match self.client.list_entries(ino, 1000, "") {
                    Ok(entries) => {
                        if !entries.is_empty() {
                            reply.error(libc::ENOTEMPTY);
                            return;
                        }
                    }
                    Err(e) => {
                        let fs_error = parse_master_error(&e);
                        error!("rmdir: failed to list entries: {}", fs_error);
                        reply.error(fs_error.to_errno());
                        return;
                    }
                }

                match self.client.delete_entry(ino, true, &self.client_id) {
                    Ok(_) => {
                        reply.ok();
                        self.invalidate_kernel_dentry(parent, name_str);
                        if let Some(attr) = attrs {
                            self.invalidate_kernel_inode(attr.ino);
                        }
                    }
                    Err(e) => {
                        let fs_error = parse_master_error(&e);
                        error!("Failed to delete directory entry on master: {}", fs_error);
                        reply.error(fs_error.to_errno());
                    }
                }
            }
            Ok(None) => {
                reply.error(libc::ENOENT);
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("rmdir: failed to get entry: {}", fs_error);
                reply.error(fs_error.to_errno());
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap_or("");
        debug!("unlink: parent={}, name={}", parent, name_str);

        let parent_path = match self.client.get_entry_by_inode(parent) {
            Ok(Some((_, p))) => p,
            Ok(None) => {
                error!("unlink: parent inode {} not found on master", parent);
                reply.error(libc::ENOENT);
                return;
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("unlink: failed to get parent entry: {}", fs_error);
                reply.error(fs_error.to_errno());
                return;
            }
        };

        let entry_path = if parent_path == "/" {
            format!("/{}", name_str)
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        match self.client.get_entry(&entry_path) {
            Ok(Some(entry)) => {
                if let Some(fid) = entry
                    .chunks
                    .first()
                    .and_then(|chunk| Fid::from_string(&chunk.fid).ok())
                {
                    match self.client.lookup_volume(fid.volume_id) {
                        Ok(locations) => {
                            if let Some(loc) = locations.first() {
                                let addr = PowerFuseClient::location_to_grpc_addr(loc);
                                if let Err(e) =
                                    self.client
                                        .delete_data(&addr, fid.volume_id.0, fid.file_key)
                                {
                                    let fs_error = crate::error::parse_volume_error(&e);
                                    error!("Failed to delete data: {}", fs_error);
                                }
                            }
                        }
                        Err(e) => {
                            let fs_error = parse_master_error(&e);
                            error!("Failed to lookup volume: {}", fs_error);
                        }
                    }
                }

                let ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
                match self.client.delete_entry(ino, false, &self.client_id) {
                    Ok(_) => {
                        reply.ok();
                        self.invalidate_kernel_dentry(parent, name_str);
                        if let Some(attrs) = entry.attributes {
                            self.invalidate_kernel_inode(attrs.ino);
                        }
                    }
                    Err(e) => {
                        let fs_error = parse_master_error(&e);
                        error!("Failed to delete entry on master: {}", fs_error);
                        reply.error(fs_error.to_errno());
                    }
                }
            }
            Ok(None) => {
                reply.error(libc::ENOENT);
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("unlink: failed to get entry: {}", fs_error);
                reply.error(fs_error.to_errno());
            }
        }
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
        info!(
            "create: parent={}, name={}, mode={:o}, flags={:o}",
            parent, name_str, mode, flags
        );

        let parent_path = match self.client.get_entry_by_inode(parent) {
            Ok(Some((_, p))) => p,
            Ok(None) => {
                error!("create: parent inode {} not found on master", parent);
                reply.error(libc::ENOENT);
                return;
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("create: failed to get parent entry: {}", fs_error);
                reply.error(fs_error.to_errno());
                return;
            }
        };

        let now = chrono::Utc::now().timestamp();
        let file_path = if parent_path == "/" {
            format!("/{}", name_str)
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        let existing_entry = if (flags & libc::O_EXCL) == 0 {
            match self.client.get_entry(&file_path) {
                Ok(Some(entry)) => Some(entry),
                Ok(None) => None,
                Err(e) => {
                    let fs_error = parse_master_error(&e);
                    error!("create: lookup failed: {}", fs_error);
                    reply.error(fs_error.to_errno());
                    return;
                }
            }
        } else {
            match self.client.get_entry(&file_path) {
                Ok(Some(_)) => {
                    reply.error(libc::EEXIST);
                    return;
                }
                Ok(None) => None,
                Err(e) => {
                    let fs_error = parse_master_error(&e);
                    error!("create: lookup failed: {}", fs_error);
                    reply.error(fs_error.to_errno());
                    return;
                }
            }
        };

        let current_uid = unsafe { libc::getuid() };
        let current_gid = unsafe { libc::getgid() };

        let filer_entry = if let Some(existing) = &existing_entry {
            let existing_attrs = existing.attributes.as_ref();
            let existing_size = existing_attrs.map(|a| a.size).unwrap_or(0);
            let should_truncate = (flags & libc::O_TRUNC) != 0;
            let new_size = if should_truncate { 0 } else { existing_size };

            FilerEntry {
                name: name_str.to_string(),
                directory: parent_path.clone(),
                attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                    ino: existing_attrs.map(|a| a.ino).unwrap_or(0),
                    mode: mode | 0o100000,
                    nlink: existing_attrs.map(|a| a.nlink).unwrap_or(1),
                    uid: current_uid,
                    gid: current_gid,
                    rdev: 0,
                    size: new_size,
                    blksize: 4096,
                    blocks: new_size.div_ceil(512),
                    atime: existing_attrs.map(|a| a.atime).unwrap_or(now as u64),
                    mtime: now as u64,
                    ctime: now as u64,
                    crtime: existing_attrs.map(|a| a.crtime).unwrap_or(now as u64),
                    perm: 0,
                }),
                chunks: if should_truncate {
                    Vec::new()
                } else {
                    existing.chunks.clone()
                },
                hard_link_id: existing.hard_link_id.clone(),
                hard_link_counter: existing.hard_link_counter,
                extended: existing.extended.clone(),
                content_size: new_size,
                disk_size: if should_truncate {
                    0
                } else {
                    existing.disk_size
                },
                ttl: existing.ttl.clone(),
                symlink_target: existing.symlink_target.clone(),
                owner: existing.owner.clone(),
                generation: existing.generation,
            }
        } else {
            FilerEntry {
                name: name_str.to_string(),
                directory: parent_path.clone(),
                attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                    ino: 0,
                    mode: mode | 0o100000,
                    nlink: 1,
                    uid: current_uid,
                    gid: current_gid,
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
                owner: String::new(),
                generation: 0,
            }
        };

        match self.client.create_entry(filer_entry, &self.client_id) {
            Ok(master_inode) => {
                match self
                    .client
                    .acquire_lease(&parent_path, &self.client_id, 30000)
                {
                    Ok((lease_id, epoch)) => {
                        self.master_epoch
                            .store(epoch, std::sync::atomic::Ordering::SeqCst);
                        let lease_info = LeaseInfo {
                            lease_id: lease_id.clone(),
                            path: parent_path,
                            duration_ms: 30000,
                            acquired_at: std::time::Instant::now(),
                        };
                        let mut leases = self.leases.write().unwrap();
                        leases.entry(parent).or_default().push(lease_info);
                    }
                    Err(e) => {
                        let fs_error = parse_master_error(&e);
                        warn!(
                            "Failed to acquire lease for parent directory {}: {}",
                            parent_path, fs_error
                        );
                    }
                }

                match self
                    .client
                    .acquire_lease(&file_path, &self.client_id, 30000)
                {
                    Ok((lease_id, epoch)) => {
                        self.master_epoch
                            .store(epoch, std::sync::atomic::Ordering::SeqCst);
                        let lease_info = LeaseInfo {
                            lease_id: lease_id.clone(),
                            path: file_path,
                            duration_ms: 30000,
                            acquired_at: std::time::Instant::now(),
                        };
                        let mut leases = self.leases.write().unwrap();
                        leases.entry(master_inode).or_default().push(lease_info);
                    }
                    Err(e) => {
                        let fs_error = parse_master_error(&e);
                        warn!(
                            "Failed to acquire lease for created file {}: {}",
                            file_path, fs_error
                        );
                    }
                }

                let attr = FileAttr {
                    ino: master_inode,
                    size: 0,
                    blocks: 0,
                    atime: std::time::UNIX_EPOCH + std::time::Duration::from_secs(now as u64),
                    mtime: std::time::UNIX_EPOCH + std::time::Duration::from_secs(now as u64),
                    ctime: std::time::UNIX_EPOCH + std::time::Duration::from_secs(now as u64),
                    crtime: std::time::UNIX_EPOCH + std::time::Duration::from_secs(now as u64),
                    kind: FileType::RegularFile,
                    perm: (mode & 0o7777) as u16,
                    nlink: 1,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                };
                reply.created(&TTL, &attr, 0, 0, 0);
                self.invalidate_kernel_dentry(parent, name_str);
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("Failed to create file entry on master: {}", fs_error);
                reply.error(fs_error.to_errno());
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, inode: u64, flags: i32, reply: ReplyOpen) {
        info!("fuser_fs::open called: inode={}, flags={:x}", inode, flags);
        debug!("open: inode={}, flags={:x}", inode, flags);

        let (entry, path) = match self.client.get_entry_by_inode(inode) {
            Ok(Some((e, p))) => (e, p),
            Ok(None) => {
                error!("open: inode {} not found on master", inode);
                reply.error(libc::ENOENT);
                return;
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("open: failed to get entry: {}", fs_error);
                reply.error(fs_error.to_errno());
                return;
            }
        };

        if let Some(attrs) = entry.attributes {
            let file_type_bits = attrs.mode & 0o170000;
            info!(
                "open: mode={:o}, file_type_bits={:o}, is_dir={}",
                attrs.mode,
                file_type_bits,
                file_type_bits == 0o040000
            );
            if file_type_bits == 0o040000 {
                info!("open: inode {} is a directory, returning EISDIR", inode);
                reply.error(libc::EISDIR);
                return;
            }
        }

        match self.client.acquire_lease(&path, &self.client_id, 30000) {
            Ok((lease_id, epoch)) => {
                self.master_epoch
                    .store(epoch, std::sync::atomic::Ordering::SeqCst);
                let lease_info = LeaseInfo {
                    lease_id: lease_id.clone(),
                    path: path.clone(),
                    duration_ms: 30000,
                    acquired_at: std::time::Instant::now(),
                };
                let mut leases = self.leases.write().unwrap();
                leases.entry(inode).or_default().push(lease_info);
                debug!(
                    "Acquired lease for inode {} (path: {}, epoch: {}, duration: 30s)",
                    inode, path, epoch
                );
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                warn!(
                    "Failed to acquire lease for inode {} (path: {}): {}",
                    inode, path, fs_error
                );
            }
        }

        reply.opened(0, 0);
    }

    fn opendir(&mut self, _req: &Request<'_>, inode: u64, _flags: i32, reply: ReplyOpen) {
        info!("fuser_fs::opendir called: inode={}", inode);
        debug!("opendir: inode={}", inode);
        reply.opened(0, 0);
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
        info!("read: inode={}, offset={}, size={}", inode, offset, size);

        let write_buffer_entries = self.write_buffer.take(inode);
        if !write_buffer_entries.is_empty() {
            info!("read: flushing write buffer before read");
            self.flush_write_buffer(inode, &write_buffer_entries);
        }

        let entry = match self.client.get_entry_by_inode(inode) {
            Ok(Some((entry, _))) => entry,
            Ok(None) => {
                error!("read: inode {} not found on master", inode);
                reply.error(libc::ENOENT);
                return;
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                error!("read: failed to get entry: {}", fs_error);
                reply.error(fs_error.to_errno());
                return;
            }
        };

        let file_size = entry.attributes.as_ref().map(|a| a.size).unwrap_or(0);
        info!("read: file_size from master={}", file_size);

        let mut actual_file_size = file_size;
        let chunk_size = self.chunk_cache.chunk_size();

        let num_chunks = file_size.div_ceil(chunk_size);
        for chunk_idx in 0..=num_chunks {
            let chunk_offset = chunk_idx * chunk_size;
            if let Some(chunk_data) = self.chunk_cache.get(inode, chunk_offset) {
                for (i, byte) in chunk_data.data.iter().enumerate().rev() {
                    if *byte != 0 {
                        let data_size = chunk_offset + i as u64 + 1;
                        if data_size > actual_file_size {
                            actual_file_size = data_size;
                        }
                        break;
                    }
                }
            }
        }

        info!("read: actual_file_size={}", actual_file_size);

        let offset_u64 = offset as u64;
        if offset_u64 >= actual_file_size {
            reply.data(&[]);
            return;
        }

        let actual_size = std::cmp::min(size as u64, actual_file_size - offset_u64) as usize;
        info!("read: actual_size={}", actual_size);
        let mut result = vec![0u8; actual_size];

        let chunk_size = self.chunk_cache.chunk_size();
        let start_chunk_idx = offset_u64 / chunk_size;
        let end_chunk_idx = (offset_u64 + actual_size as u64).div_ceil(chunk_size);

        for chunk_idx in start_chunk_idx..end_chunk_idx {
            let chunk_offset = chunk_idx * chunk_size;
            if chunk_offset >= actual_file_size {
                continue;
            }
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
                                    info!("read: chunk {} is dirty, reading from cache", chunk_idx);
                                    match self.chunk_cache.get(inode, chunk_offset) {
                                        Some(d) => d,
                                        None => {
                                            warn!(
                                                "read: chunk {} is dirty but not in cache",
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
                            info!("read: chunk {} is dirty, reading from cache", chunk_idx);
                            match self.chunk_cache.get(inode, chunk_offset) {
                                Some(d) => d,
                                None => {
                                    warn!("read: chunk {} is dirty but not in cache", chunk_idx);
                                    reply.error(libc::EIO);
                                    return;
                                }
                            }
                        } else {
                            let chunk_fid = entry
                                .chunks
                                .iter()
                                .find(|c| c.offset == chunk_offset)
                                .and_then(|c| {
                                    if c.fid.is_empty() {
                                        None
                                    } else {
                                        Some(c.fid.clone())
                                    }
                                });
                            match chunk_fid {
                                Some(fid_str) => {
                                    let fid = match Fid::from_string(&fid_str) {
                                        Ok(f) => f,
                                        Err(e) => {
                                            error!("invalid fid format: {}", e);
                                            continue;
                                        }
                                    };
                                    let locations = match self.client.lookup_volume(fid.volume_id) {
                                        Ok(l) => l,
                                        Err(e) => {
                                            let fs_error = parse_master_error(&e);
                                            error!("lookup_volume failed: {}", fs_error);
                                            reply.error(fs_error.to_errno());
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
                                            let fs_error = crate::error::parse_volume_error(&e);
                                            error!("read_blob failed: {}", fs_error);
                                            reply.error(fs_error.to_errno());
                                            return;
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
            let data_end_in_chunk = if chunk_idx == end_chunk_idx - 1 {
                std::cmp::min(
                    data_start_in_chunk + actual_size as u64
                        - (chunk_idx - start_chunk_idx) * chunk_size,
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

        let chunk_size = self.chunk_cache.chunk_size();
        info!(
            "write: inode={}, offset={}, size={}, chunk_size={}, threshold={}",
            inode,
            offset_u64,
            data_len,
            chunk_size,
            chunk_size / 4
        );

        if data_len < chunk_size as usize / 4 {
            info!(
                "write: small write, using write_buffer, inode={}, offset={}, size={}",
                inode, offset_u64, data_len
            );
            self.write_buffer.add(inode, offset_u64, data);

            let new_file_size = offset_u64 + data_len as u64;
            if let Ok(Some((entry, _))) = self.client.get_entry_by_inode(inode) {
                if let Some(attrs) = entry.attributes {
                    if new_file_size > attrs.size {
                        info!(
                            "write (small): updating file size from {} to {}",
                            attrs.size, new_file_size
                        );
                        let filer_entry = powerfs_master::proto::powerfs::Entry {
                            name: entry.name,
                            directory: entry.directory,
                            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                                ino: inode,
                                mode: attrs.mode,
                                nlink: attrs.nlink,
                                uid: attrs.uid,
                                gid: attrs.gid,
                                rdev: 0,
                                size: new_file_size,
                                blksize: 4096,
                                blocks: new_file_size.div_ceil(512),
                                atime: attrs.atime,
                                mtime: chrono::Utc::now().timestamp() as u64,
                                ctime: chrono::Utc::now().timestamp() as u64,
                                crtime: attrs.crtime,
                                perm: 0,
                            }),
                            chunks: entry.chunks,
                            hard_link_id: entry.hard_link_id,
                            hard_link_counter: entry.hard_link_counter,
                            extended: HashMap::new(),
                            content_size: new_file_size,
                            disk_size: entry.disk_size,
                            ttl: String::new(),
                            symlink_target: entry.symlink_target,
                            owner: String::new(),
                            generation: entry.generation,
                        };
                        if let Err(e) = self.client.update_entry(&filer_entry, &self.client_id) {
                            warn!("Failed to update file size on master (small write): {}", e);
                        } else {
                            info!(
                                "Successfully updated file size on master to {}",
                                new_file_size
                            );
                            self.invalidate_kernel_inode(inode);
                        }
                    }
                }
            }

            reply.written(data_len as u32);
            return;
        }

        let start_chunk_idx = offset_u64 / chunk_size;
        let end_chunk_idx = (offset_u64 + data_len as u64).div_ceil(chunk_size);

        for chunk_idx in start_chunk_idx..end_chunk_idx {
            let chunk_offset = chunk_idx * chunk_size;

            let data_start_in_chunk = if chunk_idx == start_chunk_idx {
                offset_u64 % chunk_size
            } else {
                0
            };
            let data_end_in_chunk = if chunk_idx == end_chunk_idx - 1 {
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
                let entry = match self.client.get_entry_by_inode(inode) {
                    Ok(Some((entry, _))) => entry,
                    Ok(None) => {
                        error!("write: inode {} not found on master", inode);
                        reply.error(libc::ENOENT);
                        return;
                    }
                    Err(e) => {
                        let fs_error = parse_master_error(&e);
                        error!("write: failed to get entry: {}", fs_error);
                        reply.error(fs_error.to_errno());
                        return;
                    }
                };

                let mut initial_data = vec![0u8; chunk_size as usize];

                let chunk_fid = entry
                    .chunks
                    .iter()
                    .find(|c| c.offset == chunk_offset)
                    .and_then(|c| {
                        if c.fid.is_empty() {
                            None
                        } else {
                            Some(c.fid.clone())
                        }
                    });
                if let Some(fid_str) = chunk_fid {
                    let fid = match Fid::from_string(&fid_str) {
                        Ok(f) => f,
                        Err(e) => {
                            error!("invalid fid format: {}", e);
                            continue;
                        }
                    };
                    let locations = match self.client.lookup_volume(fid.volume_id) {
                        Ok(l) => l,
                        Err(e) => {
                            let fs_error = parse_master_error(&e);
                            error!("lookup_volume failed: {}", fs_error);
                            reply.error(fs_error.to_errno());
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
                                let fs_error = crate::error::parse_volume_error(&e);
                                warn!("read_blob for write failed: {}", fs_error);
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

        let new_file_size = offset_u64 + data_len as u64;
        debug!(
            "write: inode={}, offset={}, data_len={}, new_file_size={}",
            inode, offset_u64, data_len, new_file_size
        );

        if let Ok(Some((entry, _))) = self.client.get_entry_by_inode(inode) {
            if let Some(attrs) = entry.attributes {
                debug!("write: current file size on master: {}", attrs.size);
                if new_file_size > attrs.size {
                    debug!(
                        "write: updating file size from {} to {}",
                        attrs.size, new_file_size
                    );
                    let filer_entry = powerfs_master::proto::powerfs::Entry {
                        name: entry.name,
                        directory: entry.directory,
                        attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                            ino: inode,
                            mode: attrs.mode,
                            nlink: attrs.nlink,
                            uid: attrs.uid,
                            gid: attrs.gid,
                            rdev: 0,
                            size: new_file_size,
                            blksize: 4096,
                            blocks: new_file_size.div_ceil(512),
                            atime: attrs.atime,
                            mtime: chrono::Utc::now().timestamp() as u64,
                            ctime: chrono::Utc::now().timestamp() as u64,
                            crtime: attrs.crtime,
                            perm: 0,
                        }),
                        chunks: entry.chunks,
                        hard_link_id: entry.hard_link_id,
                        hard_link_counter: entry.hard_link_counter,
                        extended: HashMap::new(),
                        content_size: new_file_size,
                        disk_size: entry.disk_size,
                        ttl: String::new(),
                        symlink_target: entry.symlink_target,
                        owner: String::new(),
                        generation: entry.generation,
                    };
                    if let Err(e) = self.client.update_entry(&filer_entry, &self.client_id) {
                        warn!("Failed to update file size on master: {}", e);
                    } else {
                        debug!("write: file size updated on master, invalidating kernel inode");
                        self.invalidate_kernel_inode(inode);
                    }
                } else {
                    debug!(
                        "write: new_file_size ({}) not larger than current size ({})",
                        new_file_size, attrs.size
                    );
                }
            } else {
                debug!("write: no attributes found for inode {}", inode);
            }
        } else {
            debug!("write: entry not found for inode {}", inode);
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

        let max_write_offset = self.write_buffer.get_max_write_offset(inode);
        let entries = self.write_buffer.take(inode);
        if !entries.is_empty() {
            self.flush_write_buffer(inode, &entries);
        }

        if let Err(e) = self.flush_dirty_chunks(inode, max_write_offset) {
            error!("flush failed: {}", e);
            reply.error(libc::EIO);
            return;
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
        info!("release: inode={}, flush={}", inode, _flush);
        let max_write_offset = self.write_buffer.get_max_write_offset(inode);
        let write_buffer_entries = self.write_buffer.take(inode);
        info!(
            "release: write_buffer_entries.len()={}",
            write_buffer_entries.len()
        );
        if !write_buffer_entries.is_empty() {
            info!(
                "release: flushing write buffer with {} entries",
                write_buffer_entries.len()
            );
            self.flush_write_buffer(inode, &write_buffer_entries);
        }

        let mut actual_max_write_offset = max_write_offset;
        if actual_max_write_offset == 0 {
            let chunk_size = self.chunk_cache.chunk_size();
            let mut max_chunk_offset = 0;
            {
                let dirty_set = self.dirty_chunks.read().unwrap();
                for (_, chunk_idx) in dirty_set.iter().filter(|(ino, _)| *ino == inode) {
                    let chunk_offset = chunk_idx * chunk_size;
                    if chunk_offset > max_chunk_offset {
                        max_chunk_offset = chunk_offset;
                    }
                }
            }
            if max_chunk_offset > 0 {
                if let Some(chunk_data) = self.chunk_cache.get(inode, max_chunk_offset) {
                    for (i, byte) in chunk_data.data.iter().enumerate().rev() {
                        if *byte != 0 {
                            actual_max_write_offset = max_chunk_offset + i as u64 + 1;
                            break;
                        }
                    }
                }
            }
        }
        info!(
            "release: actual_max_write_offset={}",
            actual_max_write_offset
        );

        if let Err(e) = self.flush_dirty_chunks(inode, actual_max_write_offset) {
            error!("release flush failed: {}", e);
            reply.error(libc::EIO);
            return;
        }

        // Pop one lease info for this inode (supports multiple concurrent opens).
        let lease_info = {
            let mut leases = self.leases.write().unwrap();
            if let Some(lease_list) = leases.get_mut(&inode) {
                lease_list.pop()
            } else {
                None
            }
        };

        if let Some(info) = lease_info {
            // Synchronously release the lease so it is freed on master before we
            // return; otherwise the lease could outlive the close and block
            // invalidations meant for other clients.
            if let Err(e) = self.client.release_lease(&info.lease_id) {
                warn!("Failed to release lease {}: {}", info.lease_id, e);
            } else {
                debug!("Released lease: {}", info.lease_id);
            }
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
        let max_write_offset = self.write_buffer.get_max_write_offset(inode);
        if let Err(e) = self.flush_dirty_chunks(inode, max_write_offset) {
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

        if inode == 1 {
            self.readdir_root(reply, offset);
            return;
        }

        let (_, path) = match self.client.get_entry_by_inode(inode) {
            Ok(Some(e)) => e,
            _ => {
                error!("readdir: inode {} not found on master", inode);
                reply.error(libc::ENOENT);
                return;
            }
        };

        let parent_inode = match self.client.get_entry_by_inode(inode) {
            Ok(Some((entry, _))) => {
                let attrs = entry.attributes.as_ref();
                if attrs.is_none() || (attrs.unwrap().mode & 0o170000) != 0o040000 {
                    reply.error(libc::ENOTDIR);
                    return;
                }
                match self.client.get_entry(&path) {
                    Ok(Some(entry)) => {
                        if let Some(parent_path) = entry.directory.strip_suffix("/") {
                            if parent_path.is_empty() {
                                1
                            } else {
                                match self.client.get_entry(parent_path) {
                                    Ok(Some(e)) => {
                                        e.attributes.as_ref().map(|a| a.ino).unwrap_or(1)
                                    }
                                    _ => 1,
                                }
                            }
                        } else {
                            1
                        }
                    }
                    _ => 1,
                }
            }
            _ => 1,
        };

        let mut idx = offset as usize;

        if idx == 0 {
            if !reply.add(inode, 1, FileType::Directory, ".") {
                reply.ok();
                return;
            }
            idx = 1;
        }

        if idx == 1 {
            if !reply.add(parent_inode, 2, FileType::Directory, "..") {
                reply.ok();
                return;
            }
            idx = 2;
        }

        match self.client.list_entries(inode, 1000, "") {
            Ok(entries) => {
                for (i, entry) in entries.iter().enumerate() {
                    let entry_idx = 2 + i;
                    if entry_idx >= idx {
                        let child_ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
                        let mode_val = entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0);
                        let file_type = mode_val & 0o170000;

                        let kind = match file_type {
                            0o040000 => FileType::Directory,
                            0o120000 => FileType::Symlink,
                            _ => FileType::RegularFile,
                        };

                        let next_offset = (entry_idx + 1) as i64;
                        if !reply.add(child_ino, next_offset, kind, &entry.name) {
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                error!("readdir: list_entries failed: {}", e);
            }
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

        let parent_path = match self.client.get_entry_by_inode(parent) {
            Ok(Some((_, p))) => p,
            _ => {
                error!("rename: parent inode {} not found on master", parent);
                reply.error(libc::ENOENT);
                return;
            }
        };

        let new_parent_path = match self.client.get_entry_by_inode(new_parent) {
            Ok(Some((_, p))) => p,
            _ => {
                error!(
                    "rename: new_parent inode {} not found on master",
                    new_parent
                );
                reply.error(libc::ENOENT);
                return;
            }
        };

        let old_path = if parent_path == "/" {
            format!("/{}", name_str)
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        let target_path = if new_parent_path == "/" {
            format!("/{}", new_name_str)
        } else {
            format!("{}/{}", new_parent_path, new_name_str)
        };

        let entry = match self.client.get_entry(&old_path) {
            Ok(Some(e)) => e,
            Ok(None) => {
                error!("rename: entry {} not found on master", old_path);
                reply.error(libc::ENOENT);
                return;
            }
            Err(e) => {
                error!("rename: failed to get entry {}: {}", old_path, e);
                reply.error(libc::EIO);
                return;
            }
        };

        let entry_inode = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);

        match self
            .client
            .acquire_lease(&parent_path, &self.client_id, 30000)
        {
            Ok((lease_id, epoch)) => {
                self.master_epoch
                    .store(epoch, std::sync::atomic::Ordering::SeqCst);
                let lease_info = LeaseInfo {
                    lease_id: lease_id.clone(),
                    path: parent_path.clone(),
                    duration_ms: 30000,
                    acquired_at: std::time::Instant::now(),
                };
                let mut leases = self.leases.write().unwrap();
                leases.entry(parent).or_default().push(lease_info);
            }
            Err(e) => {
                warn!(
                    "Failed to acquire lease for parent directory {}: {}",
                    parent_path, e
                );
            }
        }

        if parent != new_parent {
            match self
                .client
                .acquire_lease(&new_parent_path, &self.client_id, 30000)
            {
                Ok((lease_id, epoch)) => {
                    self.master_epoch
                        .store(epoch, std::sync::atomic::Ordering::SeqCst);
                    let lease_info = LeaseInfo {
                        lease_id: lease_id.clone(),
                        path: new_parent_path.clone(),
                        duration_ms: 30000,
                        acquired_at: std::time::Instant::now(),
                    };
                    let mut leases = self.leases.write().unwrap();
                    leases.entry(new_parent).or_default().push(lease_info);
                }
                Err(e) => {
                    warn!(
                        "Failed to acquire lease for new parent directory {}: {}",
                        new_parent_path, e
                    );
                }
            }
        }

        let target_entry = self.client.get_entry(&target_path).ok().flatten();

        if let Some(target) = target_entry {
            let target_attrs = target.attributes.as_ref();
            let target_is_dir = target_attrs
                .map(|a| (a.mode & 0o170000) == 0o040000)
                .unwrap_or(false);
            let entry_attrs = entry.attributes.as_ref();
            let entry_is_dir = entry_attrs
                .map(|a| (a.mode & 0o170000) == 0o040000)
                .unwrap_or(false);

            if target_is_dir && !entry_is_dir {
                reply.error(libc::ENOTDIR);
                return;
            }

            let target_ino = target_attrs.map(|a| a.ino).unwrap_or(0);
            if let Err(e) = self
                .client
                .delete_entry(target_ino, target_is_dir, &self.client_id)
            {
                warn!("Failed to delete target entry: {}", e);
            }
        }

        match self
            .client
            .rename_entry(parent, name_str, new_parent, new_name_str, &self.client_id)
        {
            Ok(_) => {
                reply.ok();
                self.invalidate_kernel_dentry(parent, name_str);
                if parent != new_parent {
                    self.invalidate_kernel_dentry(new_parent, new_name_str);
                }
                self.invalidate_kernel_inode(entry_inode);
            }
            Err(e) => {
                error!("Failed to rename entry: {}", e);
                reply.error(libc::EIO);
            }
        }
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

        let parent_path = match self.client.get_entry_by_inode(parent) {
            Ok(Some((_, p))) => p,
            _ => {
                error!("symlink: parent inode {} not found on master", parent);
                reply.error(libc::ENOENT);
                return;
            }
        };

        let now = chrono::Utc::now().timestamp();
        let filer_entry = FilerEntry {
            name: name_str.to_string(),
            directory: parent_path,
            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                ino: 0,
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
            owner: String::new(),
            generation: 0,
        };

        match self.client.create_entry(filer_entry, &self.client_id) {
            Ok(master_inode) => {
                match self.client.get_entry_by_inode(master_inode) {
                    Ok(Some((entry, _))) => {
                        let attr = self.create_file_attr_from_entry(&entry);
                        reply.entry(&TTL, &attr, 0);
                    }
                    _ => {
                        reply.error(libc::ENOENT);
                    }
                }
                self.invalidate_kernel_dentry(parent, name_str);
            }
            Err(e) => {
                error!("Failed to create symlink entry on master: {}", e);
                reply.error(libc::EIO);
            }
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, inode: u64, reply: ReplyData) {
        debug!("readlink: inode={}", inode);

        match self.client.get_entry_by_inode(inode) {
            Ok(Some((entry, _))) => {
                if !entry.symlink_target.is_empty() {
                    reply.data(entry.symlink_target.as_bytes());
                } else {
                    reply.data(&[]);
                }
            }
            _ => {
                reply.error(libc::ENOENT);
            }
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

        let (entry, _) = match self.client.get_entry_by_inode(inode) {
            Ok(Some(e)) => e,
            _ => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let parent_path = match self.client.get_entry_by_inode(new_parent) {
            Ok(Some((_, p))) => p,
            _ => {
                error!("link: new_parent inode {} not found on master", new_parent);
                reply.error(libc::ENOENT);
                return;
            }
        };

        let attrs = entry.attributes.as_ref();
        let is_dir = attrs
            .map(|a| (a.mode & 0o170000) == 0o040000)
            .unwrap_or(false);
        let mode = attrs.map(|a| a.mode & 0o7777).unwrap_or(0o644);
        let nlink = attrs.map(|a| a.nlink).unwrap_or(1);
        let uid = attrs.map(|a| a.uid).unwrap_or(0);
        let gid = attrs.map(|a| a.gid).unwrap_or(0);
        let size = attrs.map(|a| a.size).unwrap_or(0);
        let atime = attrs.map(|a| a.atime as i64).unwrap_or(0);
        let mtime = attrs.map(|a| a.mtime as i64).unwrap_or(0);
        let ctime = attrs.map(|a| a.ctime as i64).unwrap_or(0);

        let now = chrono::Utc::now().timestamp();
        let filer_entry = FilerEntry {
            name: new_name_str.to_string(),
            directory: parent_path,
            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                ino: 0,
                mode: if is_dir {
                    mode | 0o040000
                } else {
                    mode | 0o100000
                },
                nlink: nlink + 1,
                uid,
                gid,
                rdev: 0,
                size,
                blksize: 4096,
                blocks: size.div_ceil(512),
                atime: atime as u64,
                mtime: mtime as u64,
                ctime: now as u64,
                crtime: ctime as u64,
                perm: 0,
            }),
            chunks: entry.chunks,
            hard_link_id: entry.hard_link_id,
            hard_link_counter: entry.hard_link_counter + 1,
            extended: HashMap::new(),
            content_size: entry.content_size,
            disk_size: entry.disk_size,
            ttl: String::new(),
            symlink_target: entry.symlink_target,
            owner: String::new(),
            generation: entry.generation,
        };

        match self.client.create_entry(filer_entry, &self.client_id) {
            Ok(master_inode) => {
                match self.client.get_entry_by_inode(master_inode) {
                    Ok(Some((new_entry, _))) => {
                        let attr = self.create_file_attr_from_entry(&new_entry);
                        reply.entry(&TTL, &attr, 0);
                    }
                    _ => {
                        reply.error(libc::ENOENT);
                    }
                }
                self.invalidate_kernel_dentry(new_parent, new_name_str);
            }
            Err(e) => {
                error!("Failed to create hard link entry on master: {}", e);
                reply.error(libc::EIO);
            }
        }
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

#[allow(clippy::too_many_arguments)]
async fn metadata_subscription_loop(
    client: Arc<PowerFuseClient>,
    cache: Arc<MetadataCache>,
    chunk_cache: Arc<ChunkCache>,
    leases: Arc<RwLock<HashMap<u64, Vec<LeaseInfo>>>>,
    master_epoch: Arc<std::sync::atomic::AtomicU64>,
    job_id: String,
    client_id: String,
) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    loop {
        info!("Starting metadata subscription...");
        match client.subscribe_metadata("/").await {
            Ok(mut stream) => {
                backoff = Duration::from_secs(1);
                while let Ok(notification) = stream.message().await {
                    if let Some(notif) = notification {
                        let notif_epoch = notif.epoch;
                        let local_epoch = master_epoch.load(std::sync::atomic::Ordering::SeqCst);
                        if notif_epoch > local_epoch {
                            warn!(
                                "Master epoch changed: {} -> {} (Master restarted), clearing all local leases",
                                local_epoch, notif_epoch
                            );
                            let mut leases_guard = leases.write().unwrap();
                            let cleared = leases_guard.len();
                            leases_guard.clear();
                            master_epoch.store(notif_epoch, std::sync::atomic::Ordering::SeqCst);
                            debug!("Cleared {} lease entries due to epoch change", cleared);
                        }
                        handle_metadata_notification(
                            &cache,
                            &chunk_cache,
                            &notif,
                            &leases,
                            &job_id,
                            &client_id,
                        );
                    }
                }
                warn!("Metadata subscription stream closed, reconnecting...");
            }
            Err(e) => {
                error!("Metadata subscription failed: {}", e);
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = std::cmp::min(backoff * 2, max_backoff);
    }
}

fn handle_metadata_notification(
    cache: &Arc<MetadataCache>,
    chunk_cache: &Arc<ChunkCache>,
    notification: &MetadataNotification,
    _leases: &Arc<RwLock<HashMap<u64, Vec<LeaseInfo>>>>,
    _job_id: &str,
    client_id: &str,
) {
    if !notification.source_client_id.is_empty() && notification.source_client_id == client_id {
        return;
    }

    if notification.generation > 0 {
        cache.update_path_generation(&notification.path, notification.generation);
    }

    let invalidate_path_with_chunks = |path: &str| {
        let inode = cache.get_path(path);
        cache.invalidate_path(path);
        if let Some(inode) = inode {
            chunk_cache.remove_inode_chunks(inode);
        }
    };

    match notification.event_type() {
        powerfs_master::proto::powerfs::metadata_notification::EventType::Create => {
            let path = notification.path.clone();
            debug!("Received CREATE notification for: {}", path);
            invalidate_path_with_chunks(&path);
        }
        powerfs_master::proto::powerfs::metadata_notification::EventType::Update => {
            let path = notification.path.clone();
            debug!("Received UPDATE notification for: {}", path);
            invalidate_path_with_chunks(&path);
        }
        powerfs_master::proto::powerfs::metadata_notification::EventType::Delete => {
            let path = notification.path.clone();
            debug!("Received DELETE notification for: {}", path);
            invalidate_path_with_chunks(&path);
        }
        powerfs_master::proto::powerfs::metadata_notification::EventType::Rename => {
            let old_path = notification.old_path.clone();
            let new_path = notification.path.clone();
            debug!("Received RENAME notification: {} -> {}", old_path, new_path);
            invalidate_path_with_chunks(&old_path);
            invalidate_path_with_chunks(&new_path);
        }
        powerfs_master::proto::powerfs::metadata_notification::EventType::JobComplete => {
            debug!("Received JOB_COMPLETE notification, clearing entire metadata cache");
            cache.clear_all();
            chunk_cache.clear();
        }
    }
}

async fn lease_renewal_loop(
    client: Arc<PowerFuseClient>,
    leases: Arc<RwLock<HashMap<u64, Vec<LeaseInfo>>>>,
    master_epoch: Arc<std::sync::atomic::AtomicU64>,
) {
    let check_interval = Duration::from_secs(5);

    loop {
        tokio::time::sleep(check_interval).await;

        let leases_to_renew: Vec<LeaseInfo> = {
            let leases_guard = leases.read().unwrap();
            let now = std::time::Instant::now();
            leases_guard
                .values()
                .flatten()
                .filter(|info| {
                    let elapsed = now.duration_since(info.acquired_at);
                    let remaining = info.duration_ms.saturating_sub(elapsed.as_millis() as u64);
                    remaining < info.duration_ms / 3
                })
                .cloned()
                .collect()
        };

        for lease_info in leases_to_renew {
            match client
                .renew_lease(&lease_info.lease_id, lease_info.duration_ms)
                .await
            {
                Ok((true, epoch)) => {
                    master_epoch.store(epoch, std::sync::atomic::Ordering::SeqCst);
                    let mut leases_guard = leases.write().unwrap();
                    for lease_list in leases_guard.values_mut() {
                        if let Some(l) = lease_list
                            .iter_mut()
                            .find(|l| l.lease_id == lease_info.lease_id)
                        {
                            l.acquired_at = std::time::Instant::now();
                        }
                    }
                    debug!("Renewed lease: {}", lease_info.lease_id);
                }
                Ok((false, _)) => {
                    debug!(
                        "Lease {} (path: {}) no longer exists on master, removing locally",
                        lease_info.lease_id, lease_info.path
                    );
                    let mut leases_guard = leases.write().unwrap();
                    for lease_list in leases_guard.values_mut() {
                        lease_list.retain(|l| l.lease_id != lease_info.lease_id);
                    }
                }
                Err(e) => {
                    warn!("Failed to renew lease {}: {}", lease_info.lease_id, e);
                }
            }
        }
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
        let sync_client = Arc::new(SyncFuseClient::new(grpc_client.clone()));

        let cache = Arc::new(MetadataCache::new());
        let chunk_cache = Arc::new(ChunkCache::with_defaults());
        let write_buffer = Arc::new(WriteBuffer::new(64));

        let client_id = uuid::Uuid::new_v4().to_string();
        let job_id = std::env::var("POWERFS_JOB_ID").unwrap_or_default();
        let fs = PowerFsFuserFs::new(
            sync_client.clone(),
            chunk_cache.clone(),
            self.collection.clone(),
            self.replication.clone(),
            write_buffer.clone(),
            client_id.clone(),
            job_id.clone(),
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

        let cache_clone = cache.clone();
        let leases_clone = fs.leases.clone();
        let epoch_clone = fs.master_epoch.clone();
        let job_id_clone = job_id.clone();
        let client_id_clone = client_id.clone();
        let chunk_cache_clone = chunk_cache.clone();
        let client_clone = grpc_client.clone();
        tokio::spawn(async move {
            metadata_subscription_loop(
                client_clone,
                cache_clone,
                chunk_cache_clone,
                leases_clone,
                epoch_clone,
                job_id_clone,
                client_id_clone,
            )
            .await;
        });

        let leases_renewal = fs.leases.clone();
        let epoch_renewal = fs.master_epoch.clone();
        let client_renewal = grpc_client.clone();
        tokio::spawn(async move {
            lease_renewal_loop(client_renewal, leases_renewal, epoch_renewal).await;
        });

        if !job_id.is_empty() {
            let job_name = std::env::var("POWERFS_JOB_NAME").unwrap_or_else(|_| job_id.clone());
            let sync_client_clone = sync_client.clone();
            let client_id_clone = client_id.clone();
            tokio::spawn(async move {
                info!("Registering client to job: {} ({})", job_id, job_name);
                match sync_client_clone.register_job_client(&job_id, &job_name, &client_id_clone) {
                    Ok(_) => {
                        info!("Successfully registered to job: {}", job_id);
                    }
                    Err(e) => {
                        warn!("Failed to register to job {}: {}", job_id, e);
                    }
                }
            });
        }

        let options = vec![
            MountOption::FSName("powerfs".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowOther,
            MountOption::DefaultPermissions,
        ];

        let fs_for_mount = fs.clone();
        let mount_point_clone = self.mount_point.clone();
        let options_clone = options.clone();

        let session_handle = std::thread::Builder::new()
            .name("fuse_server".to_string())
            .spawn(move || {
                info!("FUSE server thread started, calling mount2...");
                if let Err(e) = fuser::mount2(fs_for_mount, &mount_point_clone, &options_clone) {
                    error!("Failed to mount FUSE: {}", e);
                } else {
                    info!("FUSE mount completed");
                }
                warn!("FUSE server exited");
            })
            .map_err(|e| PowerFsError::Internal(format!("failed to spawn fuse thread: {}", e)))?;

        let _ = session_handle.join();

        info!("FUSE session ended");
        Ok(())
    }
}
