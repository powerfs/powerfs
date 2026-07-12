use crate::cache::{CachedEntry, CachedFileChunk, ChunkCache, MetadataCache, ROOT_INODE};
use crate::client::{PowerFuseClient, SyncFuseClient};
use fuse_backend_rs::api::filesystem::{
    Context, DirEntry, Entry, FileLock, FileSystem, GetxattrReply, ListxattrReply, ZeroCopyReader,
    ZeroCopyWriter,
};
use fuse_backend_rs::api::server::Server;
use fuse_backend_rs::transport::{FuseChannel, FuseSession};
use log::{debug, error, info, warn};
use powerfs_common::error::{PowerFsError, Result};
use powerfs_common::types::Fid;
use powerfs_master::proto::powerfs::Entry as FilerEntry;
use std::collections::{HashMap, HashSet};
use std::ffi::CStr;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;
use tokio::runtime::Handle;

const TTL: Duration = Duration::from_secs(1);
const PREFETCH_CHUNKS: u64 = 2;

/// FUSE application that manages the mount lifecycle
pub struct FuseApp {
    mount_point: String,
    master_addr: String,
    collection: String,
    replication: String,
    runtime_handle: Handle,
}

impl FuseApp {
    pub async fn new(
        master_addr: &str,
        mount_point: &str,
        collection: &str,
        replication: &str,
    ) -> Result<Self> {
        let runtime_handle = Handle::try_current()
            .map_err(|e| PowerFsError::Internal(format!("no tokio runtime: {}", e)))?;

        Ok(FuseApp {
            mount_point: mount_point.to_string(),
            master_addr: master_addr.to_string(),
            collection: collection.to_string(),
            replication: replication.to_string(),
            runtime_handle,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting FUSE session on {} with master {}",
            self.mount_point, self.master_addr
        );

        let grpc_client = PowerFuseClient::new(&self.master_addr, self.runtime_handle.clone());
        let sync_client = Arc::new(SyncFuseClient::new(grpc_client));

        let cache = Arc::new(MetadataCache::new());

        let fs = PowerFsFs {
            client: sync_client.clone(),
            cache: cache.clone(),
            chunk_cache: Arc::new(ChunkCache::with_defaults()),
            collection: self.collection.clone(),
            replication: self.replication.clone(),
            locks: Arc::new(RwLock::new(HashMap::new())),
            dirty_chunks: Arc::new(RwLock::new(HashSet::new())),
            has_dirty: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        let fs_arc = Arc::new(fs);
        let bg_fs = fs_arc.clone();
        thread::spawn(move || loop {
            if bg_fs.has_dirty.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = bg_fs.flush_all_dirty_chunks();
                bg_fs
                    .has_dirty
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }
            thread::sleep(Duration::from_millis(100));
        });

        let mut session =
            FuseSession::new(Path::new(&self.mount_point), "powerfs", "powerfs", false).map_err(
                |e| PowerFsError::Internal(format!("failed to create fuse session: {}", e)),
            )?;

        session
            .mount()
            .map_err(|e| PowerFsError::Internal(format!("failed to mount fuse: {}", e)))?;

        info!("FUSE mounted at: {}", self.mount_point);

        let server = Arc::new(Server::new(fs_arc));

        let mut fuse_server = FuseServer {
            server: server.clone(),
            ch: session.new_channel().map_err(|e| {
                PowerFsError::Internal(format!("failed to create fuse channel: {}", e))
            })?,
        };

        let handle = std::thread::Builder::new()
            .name("fuse_server".to_string())
            .spawn(move || {
                info!("FUSE service thread started");
                let _ = fuse_server.svc_loop();
                warn!("FUSE service thread exited");
            })
            .map_err(|e| PowerFsError::Internal(format!("failed to spawn fuse thread: {}", e)))?;

        tokio::signal::ctrl_c()
            .await
            .map_err(|e| PowerFsError::Internal(format!("signal error: {}", e)))?;

        info!("Received Ctrl+C, unmounting...");
        session.wake().ok();
        session.umount().ok();
        let _ = handle.join();

        info!("FUSE session ended");
        Ok(())
    }
}

struct FuseServer {
    server: Arc<Server<Arc<PowerFsFs>>>,
    ch: FuseChannel,
}

impl FuseServer {
    fn svc_loop(&mut self) -> std::result::Result<(), std::io::Error> {
        loop {
            if let Some((reader, writer)) = self
                .ch
                .get_request()
                .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?
            {
                if let Err(e) = self
                    .server
                    .handle_message(reader, writer.into(), None, None)
                {
                    match e {
                        fuse_backend_rs::Error::EncodeMessage(ref e)
                            if e.raw_os_error() == Some(libc::EBADF) =>
                        {
                            break;
                        }
                        _ => {
                            error!("Handling fuse message failed: {:?}", e);
                            continue;
                        }
                    }
                }
            } else {
                info!("FUSE server exiting");
                break;
            }
        }
        Ok(())
    }
}

type FileLocks = HashMap<u64, Vec<FileLock>>;

struct PowerFsFs {
    client: Arc<SyncFuseClient>,
    cache: Arc<MetadataCache>,
    chunk_cache: Arc<ChunkCache>,
    collection: String,
    replication: String,
    locks: Arc<RwLock<FileLocks>>,
    dirty_chunks: Arc<RwLock<HashSet<(u64, u64)>>>,
    has_dirty: Arc<std::sync::atomic::AtomicBool>,
}

impl PowerFsFs {
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

        let mut chunks = Vec::new();

        for (_, chunk_idx) in &dirty {
            let chunk_offset = chunk_idx * chunk_size;
            let chunk_data = self.chunk_cache.get(inode, chunk_offset);

            if let Some(chunk_data) = chunk_data {
                let data_len = chunk_data.data.len();
                self.client
                    .write_blob(
                        &addr,
                        fid.volume_id.0,
                        fid.file_key,
                        chunk_offset as i64,
                        data_len as i32,
                        chunk_data.data,
                        0,
                    )
                    .map_err(|e| {
                        error!("write_blob failed: {}", e);
                        std::io::Error::from_raw_os_error(libc::EIO)
                    })?;

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
                owner: String::new(),
                generation: entry.generation,
            };

            if let Err(e) = self.client.update_entry(&filer_entry, "") {
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

    fn create_stat(&self, entry: &CachedEntry) -> libc::stat64 {
        let mut attr: libc::stat64 = unsafe { std::mem::zeroed() };
        attr.st_ino = entry.inode;
        attr.st_mode = if entry.is_symlink {
            (entry.mode | 0o120000) as libc::mode_t
        } else if entry.is_dir {
            (entry.mode | 0o040000) as libc::mode_t
        } else {
            (entry.mode | 0o100000) as libc::mode_t
        };
        attr.st_nlink = entry.nlink as u64;
        attr.st_uid = entry.uid;
        attr.st_gid = entry.gid;
        attr.st_size = entry.size as i64;
        attr.st_blksize = 4096;
        attr.st_blocks = entry.size.div_ceil(512) as i64;
        attr.st_atime = entry.atime;
        attr.st_mtime = entry.mtime;
        attr.st_ctime = entry.ctime;
        attr
    }

    fn create_fuse_entry(&self, cached: &CachedEntry) -> Entry {
        Entry {
            inode: cached.inode,
            generation: 0,
            attr: self.create_stat(cached),
            attr_flags: 0,
            attr_timeout: TTL,
            entry_timeout: TTL,
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
        info!(
            "entry_to_cached: name={}, fid={:?}, chunks={}",
            entry.name,
            fid,
            entry.chunks.len()
        );

        let mode_val = attrs.map(|a| a.mode).unwrap_or(0);
        let file_type = mode_val & 0o170000;
        let is_dir = file_type == 0o040000;
        let is_symlink = file_type == 0o120000;
        info!(
            "entry_to_cached: name={}, mode={:o}, file_type={:o}, is_dir={}, is_symlink={}",
            entry.name, mode_val, file_type, is_dir, is_symlink
        );

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
            generation: entry.generation,
        }
    }
}

impl FileSystem for PowerFsFs {
    type Inode = u64;
    type Handle = u64;

    fn init(
        &self,
        _capable: fuse_backend_rs::api::filesystem::FsOptions,
    ) -> std::io::Result<fuse_backend_rs::api::filesystem::FsOptions> {
        Ok(fuse_backend_rs::api::filesystem::FsOptions::WRITEBACK_CACHE)
    }

    fn lookup(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<Entry> {
        let name_str = name.to_str().unwrap_or("");
        debug!("lookup: parent={}, name={}", parent, name_str);

        if let Some(entry) = self.lookup_in_cache(parent, name_str) {
            return Ok(self.create_fuse_entry(&entry));
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
                info!(
                    "cached entry: fid={:?}, chunks={}",
                    cached.fid.is_some(),
                    cached.chunks.len()
                );
                self.cache.insert(cached.clone());
                Ok(self.create_fuse_entry(&cached))
            }
            Ok(None) => Err(std::io::Error::from_raw_os_error(libc::ENOENT)),
            Err(e) => {
                warn!("lookup entry failed: {}", e);
                Err(std::io::Error::from_raw_os_error(libc::ENOENT))
            }
        }
    }

    fn getattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Option<Self::Handle>,
    ) -> std::io::Result<(libc::stat64, Duration)> {
        debug!("getattr: inode={}", inode);

        if let Some(entry) = self.cache.get_inode(inode) {
            Ok((self.create_stat(&entry), TTL))
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ENOENT))
        }
    }

    fn setattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        attr: libc::stat64,
        _handle: Option<Self::Handle>,
        valid: fuse_backend_rs::abi::fuse_abi::SetattrValid,
    ) -> std::io::Result<(libc::stat64, Duration)> {
        debug!("setattr: inode={}, valid={:?}", inode, valid);

        self.cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let mode = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::MODE) {
            Some(attr.st_mode & 0o7777)
        } else {
            None
        };
        let size = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::SIZE) {
            Some(attr.st_size as u64)
        } else {
            None
        };
        let uid = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::UID) {
            Some(attr.st_uid)
        } else {
            None
        };
        let gid = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::GID) {
            Some(attr.st_gid)
        } else {
            None
        };

        let now = chrono::Utc::now().timestamp();
        let atime = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::ATIME_NOW) {
            Some(now)
        } else if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::ATIME) {
            Some(attr.st_atime)
        } else {
            None
        };
        let mtime = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::MTIME_NOW) {
            Some(now)
        } else if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::MTIME) {
            Some(attr.st_mtime)
        } else {
            None
        };

        self.cache.update_attr(
            inode,
            crate::cache::UpdateAttrParams {
                mode,
                size,
                uid,
                gid,
                atime,
                mtime,
            },
        );

        if let Some(updated) = self.cache.get_inode(inode) {
            Ok((self.create_stat(&updated), TTL))
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ENOENT))
        }
    }

    fn mkdir(
        &self,
        ctx: &Context,
        parent: Self::Inode,
        name: &CStr,
        mode: u32,
        _umask: u32,
    ) -> std::io::Result<Entry> {
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "mkdir: parent={}, name={}, mode={:o}",
            parent, name_str, mode
        );

        if self.lookup_in_cache(parent, name_str).is_some() {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        let now = chrono::Utc::now().timestamp();

        let parent_path = if let Some(path) = self.cache.inode_to_path(parent) {
            path
        } else {
            match self.client.get_entry_by_inode(parent) {
                Ok(Some((_, path))) => path,
                _ => {
                    error!("Failed to get parent path for inode {}", parent);
                    return Err(std::io::Error::from_raw_os_error(libc::EIO));
                }
            }
        };

        let filer_entry = FilerEntry {
            name: name_str.to_string(),
            directory: parent_path,
            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                ino: 0,
                mode: mode | 0o040000,
                nlink: 2,
                uid: ctx.uid,
                gid: ctx.gid,
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

        let inode = self.client.create_entry(filer_entry, "").map_err(|e| {
            error!("Failed to create directory entry on master: {}", e);
            std::io::Error::from_raw_os_error(libc::EIO)
        })?;

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
            uid: ctx.uid,
            gid: ctx.gid,
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
        };
        self.cache.insert(entry.clone());

        Ok(self.create_fuse_entry(&entry))
    }

    fn rmdir(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        debug!("rmdir: parent={}, name={}", parent, name_str);

        let entry = self
            .lookup_in_cache(parent, name_str)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        if !entry.is_dir {
            return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR));
        }

        if !self.cache.list_children(entry.inode).is_empty() {
            return Err(std::io::Error::from_raw_os_error(libc::ENOTEMPTY));
        }

        match self.client.delete_entry(entry.inode, true, "") {
            Ok(_) => {}
            Err(e) => {
                warn!("Failed to delete directory entry on master: {}", e);
            }
        }

        self.cache.remove(entry.inode);
        Ok(())
    }

    fn unlink(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        debug!("unlink: parent={}, name={}", parent, name_str);

        let entry = self
            .lookup_in_cache(parent, name_str)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let should_delete = self.cache.dec_nlink(entry.inode);

        if should_delete {
            if let Some(fid) = &entry.fid {
                let volume_id = fid.volume_id.0;
                match self.client.lookup_volume(fid.volume_id) {
                    Ok(locations) => {
                        if let Some(loc) = locations.first() {
                            let addr = PowerFuseClient::location_to_grpc_addr(loc);
                            if let Err(e) = self.client.delete_data(&addr, volume_id, fid.file_key)
                            {
                                warn!("Failed to delete remote data: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to lookup volume for deletion: {}", e);
                    }
                }
            }

            match self.client.delete_entry(entry.inode, false, "") {
                Ok(_) => {}
                Err(e) => {
                    warn!("Failed to delete file entry on master: {}", e);
                }
            }

            self.cache.remove(entry.inode);
        }

        Ok(())
    }

    fn create(
        &self,
        ctx: &Context,
        parent: Self::Inode,
        name: &CStr,
        args: fuse_backend_rs::abi::fuse_abi::CreateIn,
    ) -> std::io::Result<(
        Entry,
        Option<Self::Handle>,
        fuse_backend_rs::abi::fuse_abi::OpenOptions,
        Option<u32>,
    )> {
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "create: parent={}, name={}, mode={:o}",
            parent, name_str, args.mode
        );

        if self.lookup_in_cache(parent, name_str).is_some() {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        let now = chrono::Utc::now().timestamp();

        let (fid, _location, _stripe_fids, _stripe_locations) = self
            .client
            .assign_fid(&self.collection, &self.replication)
            .map_err(|e| {
                error!("assign_fid failed: {}", e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;

        let fid_str = fid.to_string();

        let parent_path = if let Some(path) = self.cache.inode_to_path(parent) {
            path
        } else {
            match self.client.get_entry_by_inode(parent) {
                Ok(Some((_, path))) => path,
                _ => {
                    error!("Failed to get parent path for inode {}", parent);
                    return Err(std::io::Error::from_raw_os_error(libc::EIO));
                }
            }
        };

        let filer_entry = FilerEntry {
            name: name_str.to_string(),
            directory: parent_path,
            attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                ino: 0,
                mode: args.mode | 0o100000,
                nlink: 1,
                uid: ctx.uid,
                gid: ctx.gid,
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

        let inode = self.client.create_entry(filer_entry, "").map_err(|e| {
            error!("Failed to create file entry on master: {}", e);
            std::io::Error::from_raw_os_error(libc::EIO)
        })?;

        let entry = CachedEntry {
            inode,
            parent,
            name: name_str.to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: Some(fid),
            size: 0,
            mode: args.mode & 0o7777,
            uid: ctx.uid,
            gid: ctx.gid,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: vec![crate::cache::CachedFileChunk {
                offset: 0,
                size: 0,
                mtime: now as u64,
                fid: fid_str,
                cookie: 0,
                crc32: 0,
            }],
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
        };
        self.cache.insert(entry.clone());

        Ok((
            self.create_fuse_entry(&entry),
            Some(inode),
            fuse_backend_rs::abi::fuse_abi::OpenOptions::empty(),
            None,
        ))
    }

    fn open(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _flags: u32,
        _fuse_flags: u32,
    ) -> std::io::Result<(
        Option<Self::Handle>,
        fuse_backend_rs::abi::fuse_abi::OpenOptions,
        Option<u32>,
    )> {
        eprintln!(
            "fuse::open called: inode={}, ROOT_INODE={}",
            inode, ROOT_INODE
        );
        debug!("open: inode={}", inode);

        if inode == ROOT_INODE {
            eprintln!("fuse::open: inode is root, returning EISDIR");
            return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
        }

        if let Some(entry) = self.cache.get_inode(inode) {
            eprintln!(
                "fuse::open: found entry in cache, is_dir={}, mode={:o}",
                entry.is_dir, entry.mode
            );
            if entry.is_dir {
                eprintln!("fuse::open: entry is directory, returning EISDIR");
                return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
            }
            Ok((
                Some(inode),
                fuse_backend_rs::abi::fuse_abi::OpenOptions::empty(),
                None,
            ))
        } else {
            eprintln!("fuse::open: entry not found in cache, returning ENOENT");
            Err(std::io::Error::from_raw_os_error(libc::ENOENT))
        }
    }

    fn read(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        w: &mut dyn ZeroCopyWriter,
        size: u32,
        offset: u64,
        _lock_owner: Option<u64>,
        _flags: u32,
    ) -> std::io::Result<usize> {
        debug!("read: inode={}, size={}, offset={}", inode, size, offset);

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let fid = entry
            .fid
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;

        let file_size = entry.size;
        if offset >= file_size {
            return Ok(0);
        }

        let end_offset = std::cmp::min(offset + size as u64, file_size);
        let chunk_size = self.chunk_cache.chunk_size();

        let start_chunk = self.chunk_cache.get_chunk_index(offset);
        let _end_chunk = self
            .chunk_cache
            .get_chunk_index(end_offset.saturating_sub(1));

        let prefetch_end = std::cmp::min(end_offset + PREFETCH_CHUNKS * chunk_size, file_size);
        let prefetch_end_chunk = if prefetch_end == 0 {
            0
        } else {
            self.chunk_cache.get_chunk_index(prefetch_end - 1)
        };

        info!(
            "read: inode={}, fid={:?}, volume_id={}",
            inode, fid, fid.volume_id
        );
        let locations = self.client.lookup_volume(fid.volume_id).map_err(|e| {
            error!(
                "lookup_volume failed: volume_id={}, error={}",
                fid.volume_id, e
            );
            std::io::Error::from_raw_os_error(libc::EIO)
        })?;

        let loc = locations
            .first()
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;
        let addr = PowerFuseClient::location_to_grpc_addr(loc);

        for chunk_idx in start_chunk..=prefetch_end_chunk {
            let chunk_offset = chunk_idx * chunk_size;
            if self.chunk_cache.get(inode, chunk_offset).is_none() {
                let read_size = std::cmp::min(chunk_size, file_size - chunk_offset);
                match self.client.read_blob(
                    &addr,
                    fid.volume_id.0,
                    fid.file_key,
                    chunk_offset as i64,
                    read_size as i32,
                ) {
                    Ok(data) => {
                        let mtime = entry.mtime as u64;
                        self.chunk_cache.put(inode, chunk_offset, data, mtime, 0);
                    }
                    Err(e) => {
                        if e.contains("needle not found") {
                            info!("read_blob: needle not found in volume, checking dirty chunks");
                            let is_dirty = {
                                let dirty_set = self.dirty_chunks.read().unwrap();
                                dirty_set.contains(&(inode, chunk_idx))
                            };
                            if is_dirty {
                                info!("read_blob: chunk {} is dirty, flushing first", chunk_idx);
                                let _ = self.flush_dirty_chunks(inode);
                                match self.client.read_blob(
                                    &addr,
                                    fid.volume_id.0,
                                    fid.file_key,
                                    chunk_offset as i64,
                                    read_size as i32,
                                ) {
                                    Ok(data) => {
                                        let mtime = entry.mtime as u64;
                                        self.chunk_cache.put(inode, chunk_offset, data, mtime, 0);
                                    }
                                    Err(e2) => {
                                        error!("read_blob failed after flush: {}", e2);
                                        return Err(std::io::Error::from_raw_os_error(libc::EIO));
                                    }
                                }
                            } else {
                                info!(
                                    "read_blob: chunk {} not in dirty chunks, filling with zeros",
                                    chunk_idx
                                );
                                let mtime = entry.mtime as u64;
                                self.chunk_cache.put(
                                    inode,
                                    chunk_offset,
                                    vec![0; read_size as usize],
                                    mtime,
                                    0,
                                );
                            }
                        } else {
                            error!("read_blob failed: {}", e);
                            return Err(std::io::Error::from_raw_os_error(libc::EIO));
                        }
                    }
                }
            }
        }

        let mut total_written = 0usize;
        let mut current_offset = offset;
        let end = end_offset;

        while current_offset < end {
            let chunk_data = self
                .chunk_cache
                .get(inode, current_offset)
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;

            let chunk_start = self.chunk_cache.get_chunk_offset(current_offset) as usize;
            let bytes_left_in_chunk =
                (chunk_data.data.len() - chunk_start).min((end - current_offset) as usize);

            let slice = &chunk_data.data[chunk_start..chunk_start + bytes_left_in_chunk];
            w.write_all(slice)?;
            total_written += bytes_left_in_chunk;
            current_offset += bytes_left_in_chunk as u64;
        }

        Ok(total_written)
    }

    fn write(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        r: &mut dyn ZeroCopyReader,
        size: u32,
        offset: u64,
        _lock_owner: Option<u64>,
        _delayed_write: bool,
        _flags: u32,
        _fuse_flags: u32,
    ) -> std::io::Result<usize> {
        debug!("write: inode={}, size={}, offset={}", inode, size, offset);

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let mut buf = vec![0u8; size as usize];
        let read_len = r.read(&mut buf).unwrap_or(0);
        if read_len == 0 {
            return Ok(0);
        }
        buf.truncate(read_len);

        let chunk_size = self.chunk_cache.chunk_size();

        if let Some(_fid) = &entry.fid {
            let end_offset = offset + read_len as u64;
            let start_chunk = self.chunk_cache.get_chunk_index(offset);
            let end_chunk = if end_offset == 0 {
                0
            } else {
                self.chunk_cache.get_chunk_index(end_offset - 1)
            };

            let mut data_offset = 0u64;
            let mut current_offset = offset;

            for chunk_idx in start_chunk..=end_chunk {
                let chunk_start_offset = chunk_idx * chunk_size;
                let in_chunk_start = current_offset.saturating_sub(chunk_start_offset) as usize;
                let bytes_to_write = std::cmp::min(
                    read_len as u64 - data_offset,
                    chunk_size - in_chunk_start as u64,
                ) as usize;

                let mtime = entry.mtime as u64;
                let modified = self.chunk_cache.modify(inode, chunk_start_offset, |chunk| {
                    let needed_len = in_chunk_start + bytes_to_write;
                    if chunk.data.len() < needed_len {
                        chunk.data.resize(needed_len, 0);
                    }
                    chunk.data[in_chunk_start..in_chunk_start + bytes_to_write].copy_from_slice(
                        &buf[data_offset as usize..data_offset as usize + bytes_to_write],
                    );
                    chunk.mtime = mtime;
                });

                if !modified {
                    let mut new_data = vec![0u8; in_chunk_start + bytes_to_write];
                    new_data[in_chunk_start..in_chunk_start + bytes_to_write].copy_from_slice(
                        &buf[data_offset as usize..data_offset as usize + bytes_to_write],
                    );
                    self.chunk_cache
                        .put(inode, chunk_start_offset, new_data, mtime, 0);
                }

                let mut dirty = self.dirty_chunks.write().unwrap();
                dirty.insert((inode, chunk_idx));
                self.has_dirty
                    .store(true, std::sync::atomic::Ordering::Relaxed);

                data_offset += bytes_to_write as u64;
                current_offset += bytes_to_write as u64;
            }

            let new_size = offset + read_len as u64;
            if new_size > entry.size {
                self.cache.update_size(inode, new_size);
            }
        } else {
            let (fid, _location, _stripe_fids, _stripe_locations) = self
                .client
                .assign_fid(&self.collection, &self.replication)
                .map_err(|e| {
                    error!("assign_fid failed: {}", e);
                    std::io::Error::from_raw_os_error(libc::EIO)
                })?;

            self.cache.update_fid(inode, fid.clone());
            let new_size = offset + read_len as u64;
            self.cache.update_size(inode, new_size);

            let mtime = entry.mtime as u64;
            self.chunk_cache.put(inode, 0, buf, mtime, 0);

            let mut dirty = self.dirty_chunks.write().unwrap();
            dirty.insert((inode, 0));
            self.has_dirty
                .store(true, std::sync::atomic::Ordering::Relaxed);

            let parent_path = self.cache.inode_to_path(entry.parent).unwrap_or_default();
            let filer_entry = powerfs_master::proto::powerfs::Entry {
                name: entry.name.clone(),
                directory: parent_path,
                attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                    ino: entry.inode,
                    mode: entry.mode | 0o100000,
                    nlink: entry.nlink,
                    uid: entry.uid,
                    gid: entry.gid,
                    rdev: 0,
                    size: new_size,
                    blksize: 4096,
                    blocks: new_size.div_ceil(512) as u64,
                    atime: entry.atime as u64,
                    mtime: entry.mtime as u64,
                    ctime: entry.ctime as u64,
                    crtime: entry.ctime as u64,
                    perm: 0,
                }),
                chunks: vec![powerfs_master::proto::powerfs::FileChunk {
                    offset: 0,
                    size: new_size,
                    mtime,
                    fid: fid.to_string(),
                    cookie: 0,
                    crc32: 0,
                }],
                hard_link_id: entry.hard_link_id.clone(),
                hard_link_counter: entry.hard_link_counter,
                extended: HashMap::new(),
                content_size: entry.content_size,
                disk_size: entry.disk_size,
                ttl: String::new(),
                symlink_target: String::new(),
                owner: String::new(),
                generation: entry.generation,
            };

            if let Err(e) = self.client.update_entry(&filer_entry, "") {
                warn!("Failed to update entry on master: {}", e);
            }
        }

        Ok(read_len)
    }

    fn release(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _flags: u32,
        _handle: Self::Handle,
        _flush: bool,
        _flock_release: bool,
        _lock_owner: Option<u64>,
    ) -> std::io::Result<()> {
        let _ = self.flush_dirty_chunks(inode);
        Ok(())
    }

    fn readdir(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        _size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry) -> std::io::Result<usize>,
    ) -> std::io::Result<()> {
        debug!("readdir: inode={}, offset={}", inode, offset);

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        if !entry.is_dir {
            return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR));
        }

        let mut idx = 0u64;

        if offset <= idx
            && add_entry(DirEntry {
                ino: inode,
                offset: idx + 1,
                type_: 0o040000,
                name: ".".as_bytes(),
            })
            .is_err()
        {
            return Ok(());
        }
        idx += 1;

        if offset <= idx {
            let parent = if inode == ROOT_INODE {
                ROOT_INODE
            } else {
                entry.parent
            };
            if add_entry(DirEntry {
                ino: parent,
                offset: idx + 1,
                type_: 0o040000,
                name: "..".as_bytes(),
            })
            .is_err()
            {
                return Ok(());
            }
        }
        idx += 1;

        let mut children = self.cache.list_children(inode);
        if children.is_empty() {
            if let Ok(entries) = self.client.list_entries(inode, 1000, "") {
                for child_entry in entries {
                    let cached = self.entry_to_cached(inode, &child_entry);
                    self.cache.insert(cached);
                }
                children = self.cache.list_children(inode);
            }
        }

        for (child_ino, child_name, is_dir) in children {
            idx += 1;
            if offset < idx {
                let type_ = if is_dir { 0o040000 } else { 0o100000 };
                if add_entry(DirEntry {
                    ino: child_ino,
                    offset: idx,
                    type_,
                    name: child_name.as_bytes(),
                })
                .is_err()
                {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn rename(
        &self,
        _ctx: &Context,
        olddir: Self::Inode,
        oldname: &CStr,
        newdir: Self::Inode,
        newname: &CStr,
        flags: u32,
    ) -> std::io::Result<()> {
        let old_str = oldname.to_str().unwrap_or("");
        let new_str = newname.to_str().unwrap_or("");
        debug!(
            "rename: olddir={}, oldname={}, newdir={}, newname={}, flags={}",
            olddir, old_str, newdir, new_str, flags
        );

        let no_replace = (flags & 1) != 0;
        if no_replace && self.lookup_in_cache(newdir, new_str).is_some() {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        if let Some(target) = self.lookup_in_cache(newdir, new_str) {
            if target.is_dir && !self.cache.list_children(target.inode).is_empty() {
                return Err(std::io::Error::from_raw_os_error(libc::ENOTEMPTY));
            }
        }

        let entry = self
            .lookup_in_cache(olddir, old_str)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        self.cache
            .rename(olddir, old_str, newdir, new_str)
            .map_err(|e| {
                error!("rename failed: {}", e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;

        let new_parent_path = self
            .cache
            .inode_to_path(newdir)
            .unwrap_or_else(|| "/".to_string());

        let filer_entry = FilerEntry {
            name: new_str.to_string(),
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
                crtime: entry.atime as u64,
                perm: 0,
            }),
            chunks: entry
                .chunks
                .iter()
                .map(|chunk| powerfs_master::proto::powerfs::FileChunk {
                    offset: chunk.offset,
                    size: chunk.size,
                    mtime: chunk.mtime,
                    fid: chunk.fid.clone(),
                    cookie: chunk.cookie,
                    crc32: chunk.crc32,
                })
                .collect(),
            hard_link_id: entry.hard_link_id.clone(),
            hard_link_counter: entry.hard_link_counter,
            extended: HashMap::new(),
            content_size: entry.content_size,
            disk_size: entry.disk_size,
            ttl: String::new(),
            symlink_target: entry.symlink_target.clone().unwrap_or_default(),
            owner: String::new(),
            generation: entry.generation,
        };

        match self.client.delete_entry(entry.inode, entry.is_dir, "") {
            Ok(_) => {}
            Err(e) => {
                warn!("Failed to delete old entry on master during rename: {}", e);
            }
        }

        match self.client.create_entry(filer_entry, "") {
            Ok(_) => {}
            Err(e) => {
                warn!("Failed to create new entry on master during rename: {}", e);
            }
        }

        Ok(())
    }

    fn symlink(
        &self,
        _ctx: &Context,
        linkname: &CStr,
        parent: Self::Inode,
        name: &CStr,
    ) -> std::io::Result<Entry> {
        let name_str = name.to_str().unwrap_or("");
        let link_str = linkname.to_str().unwrap_or("");
        debug!(
            "symlink: parent={}, name={}, target={}",
            parent, name_str, link_str
        );

        if self.lookup_in_cache(parent, name_str).is_some() {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        let parent_path = self
            .cache
            .inode_to_path(parent)
            .unwrap_or_else(|| "/".to_string());

        let now = chrono::Utc::now().timestamp() as u64;
        let entry = FilerEntry {
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
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                perm: 0o777,
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

        let inode = match self.client.create_entry(entry, "") {
            Ok(ino) => ino,
            Err(e) => {
                error!("create_entry failed for symlink: {}", e);
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
        };

        let cached_entry = CachedEntry {
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
            atime: now as i64,
            mtime: now as i64,
            ctime: now as i64,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: link_str.len() as u64,
            disk_size: 0,
            generation: 0,
        };
        self.cache.insert(cached_entry.clone());
        Ok(self.create_fuse_entry(&cached_entry))
    }

    fn readlink(&self, _ctx: &Context, inode: Self::Inode) -> std::io::Result<Vec<u8>> {
        debug!("readlink: inode={}", inode);

        let target = self
            .cache
            .get_symlink_target(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        Ok(target.into_bytes())
    }

    fn link(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        newparent: Self::Inode,
        newname: &CStr,
    ) -> std::io::Result<Entry> {
        let name_str = newname.to_str().unwrap_or("");
        debug!(
            "link: inode={}, newparent={}, newname={}",
            inode, newparent, name_str
        );

        if self.lookup_in_cache(newparent, name_str).is_some() {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        if entry.is_dir {
            return Err(std::io::Error::from_raw_os_error(libc::EPERM));
        }

        self.cache.inc_nlink(inode);

        let new_entry = CachedEntry {
            inode,
            parent: newparent,
            name: name_str.to_string(),
            is_dir: false,
            is_symlink: entry.is_symlink,
            symlink_target: entry.symlink_target.clone(),
            nlink: self.cache.get_nlink(inode),
            fid: entry.fid.clone(),
            size: entry.size,
            mode: entry.mode,
            uid: entry.uid,
            gid: entry.gid,
            atime: entry.atime,
            mtime: entry.mtime,
            ctime: chrono::Utc::now().timestamp(),
            xattrs: entry.xattrs.clone(),
            chunks: entry.chunks.clone(),
            hard_link_id: entry.hard_link_id.clone(),
            hard_link_counter: entry.hard_link_counter,
            content_size: entry.content_size,
            disk_size: entry.disk_size,
            generation: 0,
        };

        self.cache.insert(new_entry.clone());

        Ok(self.create_fuse_entry(&new_entry))
    }

    fn statfs(&self, _ctx: &Context, _inode: Self::Inode) -> std::io::Result<libc::statvfs64> {
        debug!("statfs");

        let mut st: libc::statvfs64 = unsafe { std::mem::zeroed() };
        st.f_bsize = 4096;
        st.f_frsize = 4096;
        let total_blocks: u64 = (1u64 << 40) / 4096;
        st.f_blocks = total_blocks;
        st.f_bfree = total_blocks * 8 / 10;
        st.f_bavail = total_blocks * 8 / 10;
        st.f_files = 10_000_000;
        st.f_ffree = 9_900_000;
        st.f_favail = 9_900_000;
        st.f_namemax = 255;
        Ok(st)
    }

    fn access(&self, _ctx: &Context, inode: Self::Inode, mask: u32) -> std::io::Result<()> {
        debug!("access: inode={}, mask={}", inode, mask);

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        if entry.uid == 0 {
            return Ok(());
        }

        let mode = entry.mode;
        let readable = (mode & 0o444) != 0;
        let writable = (mode & 0o222) != 0;
        let executable = (mode & 0o111) != 0;

        let r_ok = (mask & libc::R_OK as u32) == 0 || readable;
        let w_ok = (mask & libc::W_OK as u32) == 0 || writable;
        let x_ok = (mask & libc::X_OK as u32) == 0 || executable;

        if r_ok && w_ok && x_ok {
            Ok(())
        } else {
            Err(std::io::Error::from_raw_os_error(libc::EACCES))
        }
    }

    fn fsync(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _datasync: bool,
        _handle: Self::Handle,
    ) -> std::io::Result<()> {
        debug!("fsync: inode={}", inode);
        self.flush_dirty_chunks(inode)
    }

    fn fallocate(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        mode: u32,
        offset: u64,
        length: u64,
    ) -> std::io::Result<()> {
        debug!(
            "fallocate: inode={}, mode={}, offset={}, length={}",
            inode, mode, offset, length
        );

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        if entry.is_dir {
            return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
        }

        let new_size = offset + length;
        if new_size > entry.size {
            self.cache.update_size(inode, new_size);
        }

        Ok(())
    }

    fn getlk(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        _owner: u64,
        lock: FileLock,
        _flags: u32,
    ) -> std::io::Result<FileLock> {
        debug!(
            "getlk: inode={}, start={}, end={}, type={}",
            inode, lock.start, lock.end, lock.lock_type
        );

        let locks = self.locks.read().unwrap();
        if let Some(inode_locks) = locks.get(&inode) {
            for existing_lock in inode_locks {
                if existing_lock.start < lock.end
                    && existing_lock.end > lock.start
                    && existing_lock.lock_type != lock.lock_type
                {
                    return Ok(FileLock {
                        start: existing_lock.start,
                        end: existing_lock.end,
                        lock_type: existing_lock.lock_type,
                        pid: existing_lock.pid,
                    });
                }
            }
        }

        Ok(FileLock {
            start: lock.start,
            end: lock.end,
            lock_type: lock.lock_type,
            pid: 0,
        })
    }

    fn setlk(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        owner: u64,
        lock: FileLock,
        _flags: u32,
    ) -> std::io::Result<()> {
        debug!(
            "setlk: inode={}, owner={}, start={}, end={}, type={}",
            inode, owner, lock.start, lock.end, lock.lock_type
        );

        let mut locks = self.locks.write().unwrap();
        let inode_locks = locks.entry(inode).or_default();

        if lock.lock_type == 0 {
            inode_locks.retain(|l| l.start != lock.start || l.end != lock.end);
            return Ok(());
        }

        for existing_lock in &*inode_locks {
            if existing_lock.start < lock.end
                && existing_lock.end > lock.start
                && existing_lock.lock_type != lock.lock_type
            {
                return Err(std::io::Error::from_raw_os_error(libc::EAGAIN));
            }
        }

        inode_locks.push(FileLock {
            start: lock.start,
            end: lock.end,
            lock_type: lock.lock_type,
            pid: lock.pid,
        });

        Ok(())
    }

    fn setlkw(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        owner: u64,
        lock: FileLock,
        _flags: u32,
    ) -> std::io::Result<()> {
        debug!(
            "setlkw: inode={}, owner={}, start={}, end={}, type={}",
            inode, owner, lock.start, lock.end, lock.lock_type
        );
        self.setlk(_ctx, inode, _handle, owner, lock, _flags)
    }

    fn setxattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        name: &CStr,
        value: &[u8],
        _flags: u32,
    ) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        debug!("setxattr: inode={}, name={}", inode, name_str);

        if self.cache.get_inode(inode).is_none() {
            return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
        }

        self.cache.set_xattr(inode, name_str, value);
        Ok(())
    }

    fn getxattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        name: &CStr,
        size: u32,
    ) -> std::io::Result<GetxattrReply> {
        let name_str = name.to_str().unwrap_or("");
        debug!("getxattr: inode={}, name={}", inode, name_str);

        if self.cache.get_inode(inode).is_none() {
            return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
        }

        if let Some(value) = self.cache.get_xattr(inode, name_str) {
            if size == 0 {
                Ok(GetxattrReply::Count(value.len() as u32))
            } else if value.len() > size as usize {
                Err(std::io::Error::from_raw_os_error(libc::ERANGE))
            } else {
                Ok(GetxattrReply::Value(value))
            }
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ENODATA))
        }
    }

    fn listxattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        size: u32,
    ) -> std::io::Result<ListxattrReply> {
        debug!("listxattr: inode={}", inode);

        if self.cache.get_inode(inode).is_none() {
            return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
        }

        let xattrs = self.cache.list_xattrs(inode);
        let mut buf = Vec::new();
        for name in xattrs {
            buf.extend_from_slice(name.as_bytes());
            buf.push(0);
        }

        if size == 0 {
            Ok(ListxattrReply::Count(buf.len() as u32))
        } else if buf.len() > size as usize {
            Err(std::io::Error::from_raw_os_error(libc::ERANGE))
        } else {
            Ok(ListxattrReply::Names(buf))
        }
    }

    fn removexattr(&self, _ctx: &Context, inode: Self::Inode, name: &CStr) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        debug!("removexattr: inode={}, name={}", inode, name_str);

        if self.cache.get_inode(inode).is_none() {
            return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
        }

        if !self.cache.remove_xattr(inode, name_str) {
            return Err(std::io::Error::from_raw_os_error(libc::ENODATA));
        }

        Ok(())
    }

    fn fsyncdir(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _datasync: bool,
        _handle: Self::Handle,
    ) -> std::io::Result<()> {
        debug!("fsyncdir: inode={}", _inode);
        Ok(())
    }
}
