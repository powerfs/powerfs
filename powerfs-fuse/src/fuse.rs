use crate::cache::{CachedEntry, ChunkCache, MetadataCache, ROOT_INODE};
use fuse_backend_rs::api::filesystem::{
    Context, DirEntry, Entry, FileLock, FileSystem, GetxattrReply, ListxattrReply, ZeroCopyReader,
    ZeroCopyWriter,
};
use fuse_backend_rs::api::server::Server;
use fuse_backend_rs::transport::{FuseChannel, FuseSession};
use log::{debug, error, info, warn};
use powerfs_common::error::{PowerFsError, Result};
use powerfs_common::types::Fid;
use powerfs_fuse_core::SyncFuseClientFacade;
use powerfs_master::proto::powerfs::Entry as FilerEntry;
use powerfs_orset::CachedFileChunk;
use std::collections::{HashMap, HashSet};
use std::ffi::CStr;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

const TTL: Duration = Duration::from_secs(1);
const PREFETCH_CHUNKS: u64 = 2;
const FUSE_APPEND: u32 = 0x400;

/// FUSE application that manages the mount lifecycle
#[allow(dead_code)]
pub struct FuseApp {
    mount_point: String,
    master_addresses: Vec<String>,
    collection: String,
    replication: String,
    master_net_port: u16,
    master_grpc_port: u16,
    volume_net_port: u16,
    volume_addrs: Vec<String>,
    filer_addr: String,
    filer_net_port: u16,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl FuseApp {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        master_addrs: &[String],
        mount_point: &str,
        collection: &str,
        replication: &str,
        master_net_port: u16,
        master_grpc_port: u16,
        volume_net_port: u16,
        volume_addrs: Vec<String>,
        filer_addr: String,
        filer_net_port: u16,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self> {
        Ok(FuseApp {
            mount_point: mount_point.to_string(),
            master_addresses: master_addrs.to_vec(),
            collection: collection.to_string(),
            replication: replication.to_string(),
            master_net_port,
            master_grpc_port,
            volume_net_port,
            volume_addrs,
            filer_addr,
            filer_net_port,
            runtime,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting FUSE session on {} with masters {}",
            self.mount_point,
            self.master_addresses.join(", ")
        );

        // Extract host from master address
        // Strip protocol prefix (http://, https://) if present
        let master_full = self
            .master_addresses
            .first()
            .ok_or_else(|| {
                PowerFsError::Internal("master_addresses is empty (must be configured)".to_string())
            })?
            .clone();
        let master_addr = {
            let without_proto = master_full
                .strip_prefix("http://")
                .or_else(|| master_full.strip_prefix("https://"))
                .unwrap_or(&master_full);
            without_proto
                .split(':')
                .next()
                .ok_or_else(|| {
                    PowerFsError::Internal(format!(
                        "Cannot parse host from master address: {}",
                        master_full
                    ))
                })?
                .to_string()
        };

        let facade_config = powerfs_fuse_core::FuseClientFacadeConfig {
            master_addr: master_addr.clone(),
            master_port: self.master_net_port,
            volume_net_port: self.volume_net_port,
            volume_addrs: self.volume_addrs.clone(),
            filer_addr: self.filer_addr.clone(),
            filer_port: self.filer_net_port,
            request_timeout: Duration::from_secs(30),
            client_identity: powerfs_fuse_core::ClientIdentity::default(),
            master_grpc_endpoint: Some(format!("http://{}:{}", master_addr, self.master_grpc_port)),
            mount_point: self.mount_point.clone(),
            collection: self.collection.clone(),
            replication: self.replication.clone(),
        };

        let facade = Arc::new(
            powerfs_fuse_core::FuseClientFacade::build_from_config(facade_config)
                .await
                .map_err(|e| {
                    PowerFsError::Internal(format!("Failed to build FuseClientFacade: {}", e))
                })?,
        );

        let sync_client = Arc::new(powerfs_fuse_core::SyncFuseClientFacade::new(
            facade,
            self.runtime.clone(),
        ));

        let cache = Arc::new(MetadataCache::new());

        let fs = PowerFsFs {
            client: sync_client.clone(),
            cache: cache.clone(),
            chunk_cache: Arc::new(ChunkCache::with_defaults()),
            collection: self.collection.clone(),
            replication: self.replication.clone(),
            locks: Arc::new(RwLock::new(HashMap::new())),
            dirty_shards: (0..NUM_DIRTY_SHARDS)
                .map(|_| Arc::new(RwLock::new(HashSet::new())))
                .collect(),
            has_dirty: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            write_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            stripe_size: 64 * 1024 * 1024, // 64MB per stripe
            lease_duration_ms: 30000,      // 30 seconds lease
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
    client: Arc<SyncFuseClientFacade>,
    cache: Arc<MetadataCache>,
    chunk_cache: Arc<ChunkCache>,
    collection: String,
    replication: String,
    locks: Arc<RwLock<FileLocks>>,
    dirty_shards: DirtyShards,
    has_dirty: Arc<std::sync::atomic::AtomicBool>,
    write_locks: WriteLocks,
    stripe_size: u64,
    lease_duration_ms: u64,
}

const NUM_DIRTY_SHARDS: usize = 16;

type WriteLockMap = HashMap<(u64, u64), Arc<std::sync::Mutex<()>>>;
type WriteLocks = Arc<std::sync::Mutex<WriteLockMap>>;
type DirtyShardSet = HashSet<(u64, u64)>;
type DirtyShards = Vec<Arc<RwLock<DirtyShardSet>>>;

/// RAII guard that releases a Volume lease on drop, ensuring cleanup on all paths.
struct LeaseGuard<'a> {
    token: String,
    volume_id: u64,
    inode: u64,
    client_id: String,
    client: &'a Arc<SyncFuseClientFacade>,
    released: bool,
}

impl<'a> LeaseGuard<'a> {
    fn new(
        token: String,
        volume_id: u64,
        inode: u64,
        client_id: String,
        client: &'a Arc<SyncFuseClientFacade>,
    ) -> Self {
        Self {
            token,
            volume_id,
            inode,
            client_id,
            client,
            released: false,
        }
    }

    fn token(&self) -> &str {
        &self.token
    }

    fn do_release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        debug!(
            "LeaseGuard: releasing lease for inode={}, token={}",
            self.inode, self.token
        );
        if let Err(e) = self
            .client
            .release_lease(self.volume_id, self.inode, &self.client_id)
        {
            warn!(
                "LeaseGuard: lease release failed for inode={}: {}",
                self.inode, e
            );
        }
    }
}

impl<'a> Drop for LeaseGuard<'a> {
    fn drop(&mut self) {
        self.do_release();
    }
}

impl PowerFsFs {
    fn get_write_lock(&self, inode: u64, chunk_idx: u64) -> Arc<std::sync::Mutex<()>> {
        let key = (inode, chunk_idx);
        let mut locks = self.write_locks.lock().unwrap();
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
            .clone()
    }

    fn dirty_shard_idx(key: &(u64, u64)) -> usize {
        let hash = key.0.wrapping_add(key.1);
        (hash as usize) % NUM_DIRTY_SHARDS
    }

    fn mark_dirty(&self, inode: u64, chunk_idx: u64) {
        let key = (inode, chunk_idx);
        let shard = &self.dirty_shards[Self::dirty_shard_idx(&key)];
        let mut set = shard.write().unwrap();
        set.insert(key);
        self.has_dirty
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn drain_dirty_for_inode(&self, inode: u64) -> Vec<(u64, u64)> {
        let mut result = Vec::new();
        for shard in &self.dirty_shards {
            let mut set = shard.write().unwrap();
            let keys: Vec<_> = set
                .iter()
                .filter(|(ino, _)| *ino == inode)
                .cloned()
                .collect();
            for k in &keys {
                set.remove(k);
            }
            result.extend(keys);
        }
        result
    }

    fn all_dirty_inodes(&self) -> HashSet<u64> {
        let mut inodes = HashSet::new();
        for shard in &self.dirty_shards {
            let set = shard.read().unwrap();
            for (ino, _) in set.iter() {
                inodes.insert(*ino);
            }
        }
        inodes
    }

    fn flush_dirty_chunks(&self, inode: u64, lease_token: Option<&str>) -> std::io::Result<()> {
        let dirty = self.drain_dirty_for_inode(inode);

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

        let addr = self.client.get_volume_addr(fid.volume_id.0).map_err(|e| {
            error!("get_volume_addr failed: {}", e);
            std::io::Error::from_raw_os_error(libc::EIO)
        })?;

        let chunk_size = self.chunk_cache.chunk_size();

        let mut chunks = Vec::new();

        for (_, chunk_idx) in &dirty {
            let chunk_offset = chunk_idx * chunk_size;
            let chunk_data = self.chunk_cache.get(inode, chunk_offset);

            if let Some(chunk_data) = chunk_data {
                let data_len = chunk_data.data.len();
                self.client
                    .write_blob_with_lease(
                        &addr,
                        fid.volume_id.0,
                        fid.file_key,
                        inode,
                        chunk_offset as i64,
                        data_len as i32,
                        chunk_data.data,
                        lease_token,
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

        // Dirty entries already drained by drain_dirty_for_inode above

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

            // Update entry inline (not in background thread) to avoid
            // sharing the TCP stream across multiple Tokio runtimes
            if let Err(e) = self.client.update_entry(&filer_entry, "", 0, false) {
                warn!("Failed to update entry on master: {}", e);
            }
        }

        Ok(())
    }

    fn flush_all_dirty_chunks(&self) -> std::io::Result<()> {
        let inodes = self.all_dirty_inodes();

        if inodes.is_empty() {
            return Ok(());
        }

        for inode in inodes {
            let _ = self.flush_dirty_chunks(inode, None);
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

        // Compute file size: prefer attrs.size, fall back to content_size,
        // and finally compute from chunks if both are 0.
        let attrs_size = attrs.map(|a| a.size).unwrap_or(0);
        let computed_size = if attrs_size > 0 {
            attrs_size
        } else if entry.content_size > 0 {
            entry.content_size
        } else {
            // Compute from chunks: max(end_offset) across all chunks
            entry
                .chunks
                .iter()
                .map(|c| c.offset + c.size)
                .max()
                .unwrap_or(0)
        };
        info!(
            "entry_to_cached: name={}, attrs_size={}, content_size={}, computed_size={}",
            entry.name, attrs_size, entry.content_size, computed_size
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
            size: computed_size,
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
            cached_at: Instant::now(),
        }
    }

    /// 解析路径到 inode，优先缓存，然后查 Filer
    pub fn resolve_path_inode(&self, path: &str) -> Option<u64> {
        if path.is_empty() || path == "/" {
            return Some(ROOT_INODE);
        }

        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        let mut current: u64 = ROOT_INODE;

        for part in &parts {
            // Try cache first
            if let Some(entry) = self.cache.lookup_in_cache(current, part) {
                current = entry.inode;
                continue;
            }
            // Try filer
            match self.client.get_entry_by_parent(current, part) {
                Ok(Some(entry)) => {
                    // Cache it
                    let cached = self.entry_to_cached(current, &entry);
                    self.cache.insert(cached.clone());
                    current = cached.inode;
                }
                _ => return None,
            }
        }
        Some(current)
    }
}

impl FileSystem for PowerFsFs {
    type Inode = u64;
    type Handle = u64;

    fn init(
        &self,
        _capable: fuse_backend_rs::api::filesystem::FsOptions,
    ) -> std::io::Result<fuse_backend_rs::api::filesystem::FsOptions> {
        // Disable WRITEBACK_CACHE for immediate metadata sync across clients
        Ok(fuse_backend_rs::api::filesystem::FsOptions::empty())
    }

    fn lookup(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<Entry> {
        let name_str = name.to_str().unwrap_or("");
        debug!("lookup: parent={}, name={}", parent, name_str);

        if let Some(entry) = self.lookup_in_cache(parent, name_str) {
            return Ok(self.create_fuse_entry(&entry));
        }

        // 使用 parent_ino 直接查询，避免路径解析错误
        match self.client.get_entry_by_parent(parent, name_str) {
            Ok(Some(entry)) => {
                info!(
                    "lookup found entry: parent={}, name={}, chunks={}, content_size={}",
                    parent,
                    name_str,
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
            debug!("getattr: cache hit for inode={}", inode);
            return Ok((self.create_stat(&entry), TTL));
        }

        // Cache miss: 查询 Filer 获取真实属性
        debug!("getattr: cache miss for inode={}, querying filer", inode);
        let result = self.client.get_entry_by_inode(inode);
        debug!(
            "getattr: get_entry_by_inode result for inode={}: is_ok={}, is_none={}",
            inode,
            result.is_ok(),
            result.as_ref().map(|r| r.is_none()).unwrap_or(false)
        );

        match result {
            Ok(Some((filer_entry, path))) => {
                // Resolve parent inode from the path
                let parent = if path.is_empty() || path == "/" {
                    ROOT_INODE
                } else {
                    // Get parent path (strip last component)
                    let parent_path = match path.rfind('/') {
                        Some(0) => "/".to_string(),
                        Some(pos) => path[..pos].to_string(),
                        None => "/".to_string(),
                    };
                    // Try to resolve parent inode via lookup chain
                    self.resolve_path_inode(&parent_path).unwrap_or(ROOT_INODE)
                };

                let cached = self.entry_to_cached(parent, &filer_entry);
                self.cache.insert(cached.clone());
                info!(
                    "getattr: fetched inode={} from filer, name={}, parent={}",
                    inode, cached.name, parent
                );
                Ok((self.create_stat(&cached), TTL))
            }
            Ok(None) => {
                warn!("getattr: inode={} not found in filer", inode);
                Err(std::io::Error::from_raw_os_error(libc::ENOENT))
            }
            Err(e) => {
                warn!("getattr: failed to query filer for inode={}: {}", inode, e);
                Err(std::io::Error::from_raw_os_error(libc::EIO))
            }
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

        // Persist the updated entry to Filer
        if let Some(updated) = self.cache.get_inode(inode) {
            let filer_entry = FilerEntry {
                name: updated.name.clone(),
                directory: String::new(), // not used in update flow
                attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
                    ino: updated.inode,
                    mode: updated.mode,
                    nlink: 1,
                    uid: updated.uid,
                    gid: updated.gid,
                    rdev: 0,
                    size: updated.size,
                    blksize: 4096,
                    blocks: updated.size.div_ceil(512),
                    atime: updated.atime as u64,
                    mtime: updated.mtime as u64,
                    ctime: updated.ctime as u64,
                    crtime: chrono::Utc::now().timestamp() as u64,
                    perm: 0,
                }),
                chunks: updated
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
                hard_link_id: updated.hard_link_id.clone(),
                hard_link_counter: updated.hard_link_counter,
                extended: HashMap::new(),
                content_size: updated.content_size,
                disk_size: updated.disk_size,
                ttl: String::new(),
                symlink_target: updated.symlink_target.clone().unwrap_or_default(),
                owner: String::new(),
                generation: updated.generation,
            };

            if let Err(e) = self.client.update_entry(&filer_entry, "", 0, false) {
                warn!(
                    "setattr: failed to persist to filer for inode={}: {}",
                    inode, e
                );
                // Continue anyway - local cache is already updated
            }

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

        let inode = self.client.create_entry(&filer_entry, "").map_err(|e| {
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
            cached_at: Instant::now(),
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

        // Non-blocking delete: spawn a background thread to delete the entry
        let client = self.client.clone();
        let name_owned = name_str.to_string();
        std::thread::spawn(
            move || match client.delete_entry(parent, &name_owned, true, "") {
                Ok(_) => {}
                Err(e) => {
                    warn!("Failed to delete directory entry on master: {}", e);
                }
            },
        );

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

        // Build the correct path for this specific entry (not the inode_cache path)
        let parent_path = self.cache.inode_to_path(parent);
        let entry_path: Option<String> = if let Some(pp) = parent_path {
            if pp == "/" {
                Some(format!("/{}", name_str))
            } else {
                Some(format!("{}/{}", pp, name_str))
            }
        } else {
            None
        };

        if should_delete {
            // Last hard link - delete the actual data and remove all cache entries
            if let Some(fid) = &entry.fid {
                let volume_id = fid.volume_id.0;
                match self.client.get_volume_addr(fid.volume_id.0) {
                    Ok(addr) => {
                        if let Err(e) = self.client.delete_data(&addr, volume_id, fid.file_key) {
                            warn!("Failed to delete remote data: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to get volume addr for deletion: {}", e);
                    }
                }
            }

            // Non-blocking delete: spawn a background thread to delete the entry
            let client = self.client.clone();
            let name_owned = name_str.to_string();
            std::thread::spawn(
                move || match client.delete_entry(parent, &name_owned, false, "") {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Failed to delete file entry on master: {}", e);
                    }
                },
            );

            self.cache.remove(entry.inode);
        } else {
            // Not the last hard link - just remove the path mapping
            if let Some(path) = entry_path {
                self.cache.remove_path(entry.inode, &path);
            }
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
            chunks: vec![powerfs_master::proto::powerfs::FileChunk {
                offset: 0,
                size: 0,
                mtime: now as u64,
                fid: fid_str.clone(),
                cookie: fid.cookie as u32,
                crc32: 0,
            }],
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

        let inode = self.client.create_entry(&filer_entry, "").map_err(|e| {
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
            chunks: vec![CachedFileChunk {
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
            cached_at: Instant::now(),
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
        debug!("open: inode={}", inode);

        if inode == ROOT_INODE {
            debug!("open: inode is root, returning EISDIR");
            return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
        }

        if let Some(entry) = self.cache.get_inode(inode) {
            debug!(
                "open: found entry in cache, is_dir={}, mode={:o}",
                entry.is_dir, entry.mode
            );
            if entry.is_dir {
                debug!("open: entry is directory, returning EISDIR");
                return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
            }
            Ok((
                Some(inode),
                fuse_backend_rs::abi::fuse_abi::OpenOptions::empty(),
                None,
            ))
        } else {
            debug!("open: entry not found in cache, returning ENOENT");
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

        let chunk_size = self.chunk_cache.chunk_size();

        let file_size = if entry.size > 0 {
            entry.size
        } else if !entry.chunks.is_empty() {
            let max_chunk_end = entry
                .chunks
                .iter()
                .map(|c| c.offset + if c.size > 0 { c.size } else { chunk_size })
                .max()
                .unwrap_or(0);
            if max_chunk_end > 0 {
                log::warn!(
                    "read: file_size=0, using chunk-based size estimate={}",
                    max_chunk_end
                );
                max_chunk_end
            } else {
                0
            }
        } else {
            0
        };

        if offset >= file_size && !entry.chunks.is_empty() {
            log::warn!(
                "read: offset >= file_size but chunks exist, proceeding. inode={}",
                inode
            );
        } else if offset >= file_size {
            return Ok(0);
        }

        let end_offset = std::cmp::min(offset + size as u64, file_size);

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

        debug!(
            "read: inode={}, fid={:?}, volume_id={}",
            inode, fid, fid.volume_id
        );

        let needs_remote_read = (start_chunk..=prefetch_end_chunk).any(|chunk_idx| {
            self.chunk_cache
                .get(inode, chunk_idx * chunk_size)
                .is_none()
        });

        let client_id = self.client.client_id();
        let mut _lease_guard: Option<LeaseGuard> = None;

        // Acquire shared read lease if we need remote reads
        if needs_remote_read {
            let stripe_start = offset / self.stripe_size;
            let stripe_end = if end_offset > 0 {
                (end_offset - 1) / self.stripe_size + 1
            } else {
                1
            };
            let stripe_count = stripe_end - stripe_start;

            debug!(
                "read: acquiring read lease for inode={}, stripe_start={}, stripe_count={}",
                inode, stripe_start, stripe_count
            );

            match self.client.acquire_lease(
                fid.volume_id.0,
                inode,
                stripe_start,
                stripe_count,
                &client_id,
                false, // shared/read lease
                self.lease_duration_ms,
            ) {
                Ok(token) => {
                    _lease_guard = Some(LeaseGuard::new(
                        token,
                        fid.volume_id.0,
                        inode,
                        client_id.clone(),
                        &self.client,
                    ));
                    debug!("read: read lease acquired successfully");
                }
                Err(e) => {
                    warn!(
                        "read: read lease acquisition failed for inode={}: {}",
                        inode, e
                    );
                }
            }
        }

        // Use cached volume address (only queries Master as fallback)
        let addr = self.client.get_volume_addr(fid.volume_id.0).map_err(|e| {
            error!(
                "get_volume_addr failed: volume_id={}, error={}",
                fid.volume_id, e
            );
            std::io::Error::from_raw_os_error(libc::EIO)
        })?;

        // Use a closure to capture all return paths and ensure lease release
        let result = (|| -> std::io::Result<usize> {
            for chunk_idx in start_chunk..=prefetch_end_chunk {
                let chunk_offset = chunk_idx * chunk_size;
                if self.chunk_cache.get(inode, chunk_offset).is_none() {
                    let remaining = file_size.saturating_sub(chunk_offset);
                    let read_size = std::cmp::min(chunk_size, remaining);
                    match self.client.read_blob(
                        &addr,
                        fid.volume_id.0,
                        fid.file_key,
                        chunk_offset as i64,
                        read_size as i32,
                    ) {
                        Ok(ref data) => {
                            log::debug!(
                                "read_blob: inode={}, chunk_offset={}, data_len={}",
                                inode,
                                chunk_offset,
                                data.len()
                            );
                            let mtime = entry.mtime as u64;
                            self.chunk_cache
                                .put(inode, chunk_offset, data.clone(), mtime, 0);
                        }
                        Err(e) => {
                            if e.contains("needle not found") {
                                debug!(
                                    "read_blob: needle not found in volume, checking dirty chunks"
                                );
                                let is_dirty = {
                                    let key = (inode, chunk_idx);
                                    let shard = &self.dirty_shards[Self::dirty_shard_idx(&key)];
                                    let dirty_set = shard.read().unwrap();
                                    dirty_set.contains(&key)
                                };
                                if is_dirty {
                                    debug!(
                                        "read_blob: chunk {} is dirty, flushing first",
                                        chunk_idx
                                    );
                                    let _ = self.flush_dirty_chunks(inode, None);
                                    match self.client.read_blob(
                                        &addr,
                                        fid.volume_id.0,
                                        fid.file_key,
                                        chunk_offset as i64,
                                        read_size as i32,
                                    ) {
                                        Ok(data) => {
                                            let mtime = entry.mtime as u64;
                                            self.chunk_cache.put(
                                                inode,
                                                chunk_offset,
                                                data,
                                                mtime,
                                                0,
                                            );
                                        }
                                        Err(e2) => {
                                            error!("read_blob failed after flush: {}", e2);
                                            return Err(std::io::Error::from_raw_os_error(
                                                libc::EIO,
                                            ));
                                        }
                                    }
                                } else {
                                    debug!(
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

            log::debug!(
                "read: before copy loop, inode={}, end={}, offset={}",
                inode,
                end,
                offset
            );

            while current_offset < end {
                let chunk_data = self
                    .chunk_cache
                    .get(inode, current_offset)
                    .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;

                let chunk_start = (current_offset % self.chunk_cache.chunk_size()) as usize;
                let available_in_chunk = chunk_data.data.len().saturating_sub(chunk_start);
                let bytes_left_in_chunk = available_in_chunk.min((end - current_offset) as usize);

                if bytes_left_in_chunk == 0 {
                    log::debug!(
                        "read: bytes_left_in_chunk=0, breaking. chunk_data_len={}, chunk_start={}",
                        chunk_data.data.len(),
                        chunk_start
                    );
                    break;
                }

                let slice = &chunk_data.data[chunk_start..chunk_start + bytes_left_in_chunk];
                log::debug!(
                    "read: copying {} bytes from chunk_start={}, total_written={}",
                    bytes_left_in_chunk,
                    chunk_start,
                    total_written + bytes_left_in_chunk
                );
                w.write_all(slice)?;
                total_written += bytes_left_in_chunk;
                current_offset += bytes_left_in_chunk as u64;
            }

            log::debug!("read: returning total_written={}", total_written);
            Ok(total_written)
        })();

        // Lease is automatically released by LeaseGuard on drop
        result
    }

    fn write(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        r: &mut dyn ZeroCopyReader,
        size: u32,
        mut offset: u64,
        _lock_owner: Option<u64>,
        _delayed_write: bool,
        flags: u32,
        _fuse_flags: u32,
    ) -> std::io::Result<usize> {
        debug!("write: inode={}, size={}, offset={}", inode, size, offset);

        // Read data into buffer BEFORE acquiring any lock — I/O must not hold locks
        let mut buf = vec![0u8; size as usize];
        let read_len = r.read(&mut buf).unwrap_or(0);
        if read_len == 0 {
            return Ok(0);
        }
        buf.truncate(read_len);

        let is_append = (flags & FUSE_APPEND) != 0;
        let client_id = self.client.client_id();

        // Lock for metadata operations (append offset, FID assignment, size update)
        let meta_lock = self.get_write_lock(inode, u64::MAX);
        let _meta_guard = meta_lock.lock();

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        if is_append {
            let latest_entry = self
                .cache
                .get_inode(inode)
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;
            offset = latest_entry.size;
        }

        let chunk_size = self.chunk_cache.chunk_size();
        let mut lease_guard: Option<LeaseGuard> = None;

        if let Some(ref fid) = entry.fid {
            let end_offset = offset + read_len as u64;
            let start_chunk = self.chunk_cache.get_chunk_index(offset);
            let end_chunk = if end_offset == 0 {
                0
            } else {
                self.chunk_cache.get_chunk_index(end_offset - 1)
            };

            // Calculate stripe range for lease acquisition
            let stripe_start = offset / self.stripe_size;
            let stripe_end = if end_offset > 0 {
                (end_offset - 1) / self.stripe_size + 1
            } else {
                1
            };
            let stripe_count = stripe_end - stripe_start;

            // Acquire Volume Lease before writing
            debug!(
                "write: acquiring lease for inode={}, stripe_start={}, stripe_count={}, volume_id={}",
                inode, stripe_start, stripe_count, fid.volume_id.0
            );

            match self.client.acquire_lease(
                fid.volume_id.0,
                inode,
                stripe_start,
                stripe_count,
                &client_id,
                true, // exclusive write lease
                self.lease_duration_ms,
            ) {
                Ok(token) => {
                    lease_guard = Some(LeaseGuard::new(
                        token,
                        fid.volume_id.0,
                        inode,
                        client_id.clone(),
                        &self.client,
                    ));
                    debug!("write: lease acquired successfully");
                }
                Err(e) => {
                    warn!(
                        "write: lease acquisition failed for inode={}: {}, proceeding with caution",
                        inode, e
                    );
                }
            }

            // Drop metadata lock before per-chunk writes
            drop(_meta_guard);

            let new_size = offset + read_len as u64;

            // Write to chunk cache with per-chunk locks (no unsafe code)
            let mut data_offset = 0u64;
            let mut current_offset = offset;

            for chunk_idx in start_chunk..=end_chunk {
                let lock = self.get_write_lock(inode, chunk_idx);
                let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());

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

                self.mark_dirty(inode, chunk_idx);

                data_offset += bytes_to_write as u64;
                current_offset += bytes_to_write as u64;
            }

            // Re-acquire metadata lock and update size with latest value
            let _meta_guard = meta_lock.lock();
            if let Some(current_entry) = self.cache.get_inode(inode) {
                if new_size > current_entry.size {
                    self.cache.update_size(inode, new_size);
                }
            }

            // Synchronously flush dirty chunks to Volume Server while lease is held.
            // The LeaseGuard ensures the lease is released even if flush fails.
            let lease_ref = lease_guard.as_ref().map(|g| g.token());
            self.flush_dirty_chunks(inode, lease_ref).ok();
        } else {
            // First write: assign FID under metadata lock
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

            self.mark_dirty(inode, 0);

            // Acquire lease for first write too
            let stripe_start = 0;
            let stripe_count = 1;
            match self.client.acquire_lease(
                fid.volume_id.0,
                inode,
                stripe_start,
                stripe_count,
                &client_id,
                true,
                self.lease_duration_ms,
            ) {
                Ok(token) => {
                    lease_guard = Some(LeaseGuard::new(
                        token,
                        fid.volume_id.0,
                        inode,
                        client_id.clone(),
                        &self.client,
                    ));
                    debug!("write: first write lease acquired successfully");
                }
                Err(e) => {
                    warn!(
                        "write: first write lease acquisition failed for inode={}: {}",
                        inode, e
                    );
                }
            }

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
                content_size: new_size,
                disk_size: new_size,
                ttl: String::new(),
                symlink_target: String::new(),
                owner: String::new(),
                generation: entry.generation,
            };

            // Update entry inline to avoid sharing TCP stream across runtimes
            if let Err(e) = self.client.update_entry(&filer_entry, "", 0, false) {
                warn!("Failed to update entry on master: {}", e);
            }

            // Flush dirty chunks with lease for first write too
            let lease_ref = lease_guard.as_ref().map(|g| g.token());
            self.flush_dirty_chunks(inode, lease_ref).ok();
        }

        // Lease is automatically released by LeaseGuard on drop
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
        let _ = self.flush_dirty_chunks(inode, None);
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

        // Use local cache for directory listing
        // In PowerFS CRDT model, client writes update local cache, and delta sync
        // propagates changes to other clients' caches.
        let mut children = self.cache.list_children(inode);
        if children.is_empty() {
            // Try to get entries from the server, but don't block for too long
            // Use a short timeout to avoid blocking FUSE operations
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

        match self.client.delete_entry(olddir, old_str, entry.is_dir, "") {
            Ok(_) => {}
            Err(e) => {
                warn!("Failed to delete old entry on master during rename: {}", e);
            }
        }

        match self.client.create_entry(&filer_entry, "") {
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

        // Use powerfs-net protocol to create symlink on server
        let inode = match self.client.symlink(parent, name_str, link_str) {
            Ok(ino) => ino,
            Err(e) => {
                error!("symlink failed on server: {}", e);
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
        };

        let now = chrono::Utc::now().timestamp() as u64;
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
            cached_at: Instant::now(),
        };
        self.cache.insert(cached_entry.clone());
        Ok(self.create_fuse_entry(&cached_entry))
    }

    fn readlink(&self, _ctx: &Context, inode: Self::Inode) -> std::io::Result<Vec<u8>> {
        debug!("readlink: inode={}", inode);

        // First try to get from cache
        if let Some(target) = self.cache.get_symlink_target(inode) {
            return Ok(target.into_bytes());
        }

        // If not in cache, fetch from server via powerfs-net protocol
        match self.client.readlink(inode) {
            Ok(target) => {
                // Update cache with the symlink target
                self.cache.set_symlink_target(inode, target.clone());
                Ok(target.into_bytes())
            }
            Err(e) => {
                warn!("readlink failed for inode {}: {}", inode, e);
                Err(std::io::Error::from_raw_os_error(libc::ENOENT))
            }
        }
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

        // Use powerfs-net protocol to create hard link on server
        debug!(
            "link: sending NET_LINK for ino={}, newparent={}, name={}",
            inode, newparent, name_str
        );
        match self.client.link(inode, newparent, name_str) {
            Ok(_) => {
                debug!(
                    "link: NET_LINK succeeded for ino={}, name={}",
                    inode, name_str
                );
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
                    cached_at: Instant::now(),
                };

                self.cache.insert(new_entry.clone());
                Ok(self.create_fuse_entry(&new_entry))
            }
            Err(e) => {
                warn!("link failed on server: {}", e);
                Err(std::io::Error::from_raw_os_error(libc::EIO))
            }
        }
    }

    fn statfs(&self, _ctx: &Context, _inode: Self::Inode) -> std::io::Result<libc::statvfs64> {
        debug!("statfs");

        let stats = match self.client.statfs() {
            Ok(s) => s,
            Err(e) => {
                warn!("statfs failed: {}, using defaults", e);
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
        };

        let block_size: u64 = 4096;
        let total_blocks = if stats.total_size > 0 {
            stats.total_size / block_size
        } else {
            0
        };
        let free_blocks = if stats.free_size > 0 {
            stats.free_size / block_size
        } else {
            0
        };
        let bavail = free_blocks;

        let mut st: libc::statvfs64 = unsafe { std::mem::zeroed() };
        st.f_bsize = block_size as libc::c_ulong;
        st.f_frsize = block_size as libc::c_ulong;
        st.f_blocks = total_blocks;
        st.f_bfree = free_blocks;
        st.f_bavail = bavail;
        st.f_files = 10_000_000;
        st.f_ffree = 9_900_000;
        st.f_favail = 9_900_000;
        st.f_namemax = 255;

        info!(
            "statfs: total={}, used={}, free={}, volumes={}, blocks={}, bfree={}",
            stats.total_size,
            stats.used_size,
            stats.free_size,
            stats.volume_count,
            total_blocks,
            free_blocks
        );

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
        self.flush_dirty_chunks(inode, None)
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
