use crate::cache::{CachedEntry, MetadataCache, ROOT_INODE};
use crate::client::{PowerFuseClient, SyncFuseClient};
use fuse_backend_rs::api::filesystem::{
    Context, DirEntry, Entry, FileSystem, ZeroCopyReader, ZeroCopyWriter,
};
use fuse_backend_rs::api::server::Server;
use fuse_backend_rs::transport::{FuseChannel, FuseSession};
use log::{debug, error, info, warn};
use powerfs_common::error::{PowerFsError, Result};
use std::ffi::CStr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;

const TTL: Duration = Duration::from_secs(1);

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

        // Create gRPC client
        let grpc_client = PowerFuseClient::new(&self.master_addr, self.runtime_handle.clone());
        let sync_client = Arc::new(SyncFuseClient::new(grpc_client));

        // Create metadata cache
        let cache = Arc::new(MetadataCache::new());

        // Create FUSE filesystem
        let fs = PowerFsFs {
            client: sync_client,
            cache,
            collection: self.collection.clone(),
            replication: self.replication.clone(),
        };

        // Create FUSE session
        let mut session =
            FuseSession::new(Path::new(&self.mount_point), "powerfs", "powerfs", false).map_err(
                |e| PowerFsError::Internal(format!("failed to create fuse session: {}", e)),
            )?;

        session
            .mount()
            .map_err(|e| PowerFsError::Internal(format!("failed to mount fuse: {}", e)))?;

        info!("FUSE mounted at: {}", self.mount_point);

        // Create server and serve
        let server = Arc::new(Server::new(fs));

        let mut fuse_server = FuseServer {
            server: server.clone(),
            ch: session.new_channel().map_err(|e| {
                PowerFsError::Internal(format!("failed to create fuse channel: {}", e))
            })?,
        };

        // Spawn service loop in a separate thread
        let handle = std::thread::Builder::new()
            .name("fuse_server".to_string())
            .spawn(move || {
                info!("FUSE service thread started");
                let _ = fuse_server.svc_loop();
                warn!("FUSE service thread exited");
            })
            .map_err(|e| PowerFsError::Internal(format!("failed to spawn fuse thread: {}", e)))?;

        // Wait for Ctrl+C
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
    server: Arc<Server<PowerFsFs>>,
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

/// FUSE filesystem implementation backed by PowerFS Master/Volume servers
struct PowerFsFs {
    client: Arc<SyncFuseClient>,
    cache: Arc<MetadataCache>,
    collection: String,
    replication: String,
}

impl PowerFsFs {
    fn create_stat(&self, entry: &CachedEntry) -> libc::stat64 {
        let mut attr: libc::stat64 = unsafe { std::mem::zeroed() };
        attr.st_ino = entry.inode;
        attr.st_mode = if entry.is_symlink {
            entry.mode | 0o120000
        } else if entry.is_dir {
            entry.mode | 0o040000
        } else {
            entry.mode | 0o100000
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

    fn create_entry(&self, cached: &CachedEntry) -> Entry {
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
}

impl FileSystem for PowerFsFs {
    type Inode = u64;
    type Handle = u64;

    fn lookup(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<Entry> {
        let name_str = name.to_str().unwrap_or("");
        debug!("lookup: parent={}, name={}", parent, name_str);

        if let Some(entry) = self.lookup_in_cache(parent, name_str) {
            Ok(self.create_entry(&entry))
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ENOENT))
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

        self.cache.update_attr(inode, mode, size, uid, gid);

        if let Some(updated) = self.cache.get_inode(inode) {
            Ok((self.create_stat(&updated), TTL))
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ENOENT))
        }
    }

    fn mkdir(
        &self,
        _ctx: &Context,
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
        };
        self.cache.insert(entry.clone());
        Ok(self.create_entry(&entry))
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

        self.cache.remove(entry.inode);
        Ok(())
    }

    fn unlink(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        debug!("unlink: parent={}, name={}", parent, name_str);

        let entry = self
            .lookup_in_cache(parent, name_str)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        // Decrement nlink
        let should_delete = self.cache.dec_nlink(entry.inode);

        if should_delete {
            // Delete remote data if file has a FID
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
            self.cache.remove(entry.inode);
        }

        Ok(())
    }

    fn create(
        &self,
        _ctx: &Context,
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
            mode: args.mode & 0o7777,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
        };
        self.cache.insert(entry.clone());

        Ok((
            self.create_entry(&entry),
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

        if inode == ROOT_INODE || self.cache.get_inode(inode).is_some() {
            Ok((
                Some(inode),
                fuse_backend_rs::abi::fuse_abi::OpenOptions::empty(),
                None,
            ))
        } else {
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

        let locations = self.client.lookup_volume(fid.volume_id).map_err(|e| {
            error!("lookup_volume failed: {}", e);
            std::io::Error::from_raw_os_error(libc::EIO)
        })?;

        let loc = locations
            .first()
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;
        let addr = PowerFuseClient::location_to_grpc_addr(loc);

        let data = self
            .client
            .read_data(&addr, fid.volume_id.0, fid.file_key)
            .map_err(|e| {
                error!("read_data failed: {}", e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;

        let start = std::cmp::min(offset as usize, data.len());
        let end = std::cmp::min(start + size as usize, data.len());
        let slice = &data[start..end];
        w.write_all(slice)?;
        Ok(slice.len())
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

        let mut entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        // Read data from the FUSE reader
        let mut buf = vec![0u8; size as usize];
        let read_len = r.read(&mut buf).unwrap_or(0);
        if read_len == 0 {
            return Ok(0);
        }
        buf.truncate(read_len);

        // If file doesn't have a FID yet, assign one and write data
        if entry.fid.is_none() {
            let (fid, location) = self
                .client
                .assign_fid(&self.collection, &self.replication)
                .map_err(|e| {
                    error!("assign_fid failed: {}", e);
                    std::io::Error::from_raw_os_error(libc::EIO)
                })?;

            // Get volume server address
            let addr = if let Some(loc) = location {
                PowerFuseClient::location_to_grpc_addr(&loc)
            } else {
                // Lookup volume location
                let locations = self.client.lookup_volume(fid.volume_id).map_err(|e| {
                    error!("lookup_volume failed: {}", e);
                    std::io::Error::from_raw_os_error(libc::EIO)
                })?;
                let loc = locations
                    .first()
                    .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;
                PowerFuseClient::location_to_grpc_addr(loc)
            };

            self.client
                .write_data(&addr, fid.volume_id.0, fid.file_key, buf)
                .map_err(|e| {
                    error!("write_data failed: {}", e);
                    std::io::Error::from_raw_os_error(libc::EIO)
                })?;

            entry.fid = Some(fid);
            entry.size = read_len as u64;
            self.cache.update_size(inode, read_len as u64);
            self.cache
                .update_attr(inode, None, Some(read_len as u64), None, None);
        } else if let Some(fid) = &entry.fid {
            // File already has a FID, append/overwrite
            let locations = self.client.lookup_volume(fid.volume_id).map_err(|e| {
                error!("lookup_volume failed: {}", e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;
            let loc = locations
                .first()
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;
            let addr = PowerFuseClient::location_to_grpc_addr(loc);

            self.client
                .write_data(&addr, fid.volume_id.0, fid.file_key, buf)
                .map_err(|e| {
                    error!("write_data failed: {}", e);
                    std::io::Error::from_raw_os_error(libc::EIO)
                })?;

            let new_size = std::cmp::max(entry.size, offset + read_len as u64);
            self.cache.update_size(inode, new_size);
        }

        Ok(read_len)
    }

    fn release(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _flags: u32,
        _handle: Self::Handle,
        _flush: bool,
        _flock_release: bool,
        _lock_owner: Option<u64>,
    ) -> std::io::Result<()> {
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

        // Add "." entry
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

        // Add ".." entry
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

        // List children
        let children = self.cache.list_children(inode);
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

        self.cache
            .rename(olddir, old_str, newdir, new_str)
            .map_err(|e| {
                error!("rename failed: {}", e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;

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
        };
        self.cache.insert(entry.clone());
        Ok(self.create_entry(&entry))
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
        };

        self.cache.insert(new_entry.clone());

        Ok(self.create_entry(&new_entry))
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
        _inode: Self::Inode,
        _datasync: bool,
        _handle: Self::Handle,
    ) -> std::io::Result<()> {
        debug!("fsync: inode={}", _inode);
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
