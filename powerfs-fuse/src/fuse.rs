use bytes::Bytes;
use chrono::Utc;
use rand::Rng;
use fuse_backend_rs::api::filesystem::{
    Context, DirEntry, Entry, FileSystem, ZeroCopyReader, ZeroCopyWriter,
};
use libc;
use log::{debug, info};
use nix::mount;
use powerfs_common::{
    error::Result,
    types::{FileId, FileMetadata},
    utils::generate_file_id,
};
use powerfs_core::storage::StorageManager;
use std::collections::HashMap;
use std::ffi::CStr;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

struct PowerFsFs {
    storage_manager: Arc<StorageManager>,
    inode_map: RwLock<HashMap<u64, FileMetadata>>,
    path_map: RwLock<HashMap<String, FileId>>,
    next_inode: RwLock<u64>,
}

impl PowerFsFs {
    fn new(storage_manager: Arc<StorageManager>) -> Self {
        PowerFsFs {
            storage_manager,
            inode_map: RwLock::new(HashMap::new()),
            path_map: RwLock::new(HashMap::new()),
            next_inode: RwLock::new(2),
        }
    }

    fn allocate_inode(&self) -> u64 {
        let mut next = self.next_inode.write().unwrap();
        let inode = *next;
        *next += 1;
        inode
    }

    fn path_to_inode(&self, path: &str) -> Option<u64> {
        let path_map = self.path_map.read().unwrap();
        let inode_map = self.inode_map.read().unwrap();

        path_map.get(path).and_then(|file_id| {
            inode_map
                .iter()
                .find(|(_, meta)| &meta.file_id == file_id)
                .map(|(inode, _)| *inode)
        })
    }

    fn inode_to_metadata(&self, inode: u64) -> Option<FileMetadata> {
        self.inode_map.read().unwrap().get(&inode).cloned()
    }
}

fn create_dir_attr(inode: u64) -> libc::stat64 {
    let mut attr: libc::stat64 = unsafe { std::mem::zeroed() };
    attr.st_ino = inode;
    attr.st_mode = 0o040755;
    attr.st_nlink = 2;
    attr.st_uid = 0;
    attr.st_gid = 0;
    attr.st_rdev = 0;
    attr.st_size = 0;
    attr.st_blksize = 4096;
    attr.st_blocks = 0;
    let now = Utc::now().timestamp();
    attr.st_atime = now;
    attr.st_mtime = now;
    attr.st_ctime = now;
    attr
}

fn create_file_attr(inode: u64, meta: &FileMetadata) -> libc::stat64 {
    let mut attr: libc::stat64 = unsafe { std::mem::zeroed() };
    attr.st_ino = inode;
    attr.st_mode = meta.mode | 0o100000;
    attr.st_nlink = 1;
    attr.st_uid = meta.uid;
    attr.st_gid = meta.gid;
    attr.st_rdev = 0;
    attr.st_size = meta.size as i64;
    attr.st_blksize = 4096;
    attr.st_blocks = meta.size.div_ceil(4096) as i64;
    attr.st_atime = meta.atime.timestamp();
    attr.st_mtime = meta.mtime.timestamp();
    attr.st_ctime = meta.ctime.timestamp();
    attr
}

fn create_new_file_attr(inode: u64) -> libc::stat64 {
    let mut attr: libc::stat64 = unsafe { std::mem::zeroed() };
    attr.st_ino = inode;
    attr.st_mode = 0o100644;
    attr.st_nlink = 1;
    attr.st_uid = 0;
    attr.st_gid = 0;
    attr.st_rdev = 0;
    attr.st_size = 0;
    attr.st_blksize = 4096;
    attr.st_blocks = 0;
    let now = Utc::now().timestamp();
    attr.st_atime = now;
    attr.st_mtime = now;
    attr.st_ctime = now;
    attr
}

impl FileSystem for PowerFsFs {
    type Inode = u64;
    type Handle = u64;

    fn lookup(&self, _ctx: &Context, _parent: Self::Inode, name: &CStr) -> std::io::Result<Entry> {
        let name_str = name.to_str().unwrap_or("");
        let path = format!("/{}", name_str);

        if let Some(inode) = self.path_to_inode(&path) {
            debug!("Lookup path: {} -> inode: {}", path, inode);
            Ok(Entry {
                inode,
                generation: 0,
                attr: create_new_file_attr(inode),
                attr_flags: 0,
                attr_timeout: Duration::from_secs(1),
                entry_timeout: Duration::from_secs(1),
            })
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
        if inode == 1 {
            Ok((create_dir_attr(1), Duration::from_secs(1)))
        } else if let Some(meta) = self.inode_to_metadata(inode) {
            Ok((create_file_attr(inode, &meta), Duration::from_secs(1)))
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ENOENT))
        }
    }

    fn create(
        &self,
        _ctx: &Context,
        _parent: Self::Inode,
        name: &CStr,
        _args: fuse_backend_rs::abi::fuse_abi::CreateIn,
    ) -> std::io::Result<(
        Entry,
        Option<Self::Handle>,
        fuse_backend_rs::abi::fuse_abi::OpenOptions,
        Option<u32>,
    )> {
        let name_str = name.to_str().unwrap_or("");
        let path = format!("/{}", name_str);

        let mut path_map = self.path_map.write().unwrap();
        if path_map.contains_key(&path) {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        let inode = self.allocate_inode();
        let file_id = generate_file_id();

        let metadata = FileMetadata {
            file_id: file_id.clone(),
            name: name_str.to_string(),
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: Utc::now(),
            mtime: Utc::now(),
            ctime: Utc::now(),
            volume_ids: vec![],
            needle_ids: vec![],
        };

        path_map.insert(path, file_id);

        let mut inode_map = self.inode_map.write().unwrap();
        inode_map.insert(inode, metadata);

        debug!("Created file: {} with inode: {}", name_str, inode);

        Ok((
            Entry {
                inode,
                generation: 0,
                attr: create_new_file_attr(inode),
                attr_flags: 0,
                attr_timeout: Duration::from_secs(1),
                entry_timeout: Duration::from_secs(1),
            },
            None,
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
        if self.inode_to_metadata(inode).is_some() || inode == 1 {
            debug!("Opened inode: {}", inode);
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
        if let Some(meta) = self.inode_to_metadata(inode) {
            if !meta.needle_ids.is_empty() {
                if let Some(volume_id) = meta.volume_ids.first() {
                    if let Some(volume) = self.storage_manager.get_volume(volume_id) {
                        if let Some(needle_id) = meta.needle_ids.first() {
                            if let Ok(data) = volume.read_needle(needle_id) {
                                let start = std::cmp::min(offset as usize, data.len());
                                let end = std::cmp::min(start + size as usize, data.len());
                                let slice = &data[start..end];
                                w.write_all(slice)?;
                                return Ok(slice.len());
                            }
                        }
                    }
                }
            }
            Ok(0)
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ENOENT))
        }
    }

    fn write(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        _r: &mut dyn ZeroCopyReader,
        size: u32,
        offset: u64,
        _lock_owner: Option<u64>,
        _delayed_write: bool,
        _flags: u32,
        _fuse_flags: u32,
    ) -> std::io::Result<usize> {
        let mut inode_map = self.inode_map.write().unwrap();

        if let Some(meta) = inode_map.get_mut(&inode) {
            let data_len = size as u64;

            if meta.needle_ids.is_empty() {
                if let Some(volume_id) = self.storage_manager.find_available_volume() {
                    if let Some(volume) = self.storage_manager.get_volume(&volume_id) {
                        let mut buf = vec![0u8; size as usize];
                        let read_len = _r.read(&mut buf).unwrap_or(0);
                        if read_len > 0 {
                            let file_key = rand::thread_rng().gen::<u64>();
                            if let Ok(needle_info) =
                                volume.write_needle(file_key, Bytes::from(buf[..read_len].to_vec()))
                            {
                                meta.volume_ids.push(volume_id);
                                meta.needle_ids.push(needle_info.id);
                                meta.size = data_len;
                                meta.mtime = Utc::now();
                                return Ok(read_len);
                            }
                        }
                    }
                }
            }

            meta.size = std::cmp::max(meta.size, offset + data_len);
            meta.mtime = Utc::now();

            Ok(size as usize)
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ENOENT))
        }
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

    fn unlink(&self, _ctx: &Context, _parent: Self::Inode, name: &CStr) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        let path = format!("/{}", name_str);

        let mut path_map = self.path_map.write().unwrap();
        if let Some(file_id) = path_map.remove(&path) {
            let mut inode_map = self.inode_map.write().unwrap();
            inode_map.retain(|_, meta| meta.file_id != file_id);
            Ok(())
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ENOENT))
        }
    }

    fn readdir(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        _size: u32,
        _offset: u64,
        _add_entry: &mut dyn FnMut(DirEntry) -> std::io::Result<usize>,
    ) -> std::io::Result<()> {
        if inode != 1 {
            return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR));
        }
        Ok(())
    }
}

pub struct FuseClient {
    #[allow(dead_code)]
    fs: Arc<PowerFsFs>,
    mount_point: String,
}

impl FuseClient {
    pub fn new(storage_manager: Arc<StorageManager>, mount_point: &str) -> Self {
        FuseClient {
            fs: Arc::new(PowerFsFs::new(storage_manager)),
            mount_point: mount_point.to_string(),
        }
    }

    pub async fn mount(&self) -> Result<()> {
        let path = Path::new(&self.mount_point);
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        info!("Mounting PowerFS at: {}", self.mount_point);

        Ok(())
    }

    pub async fn unmount(&self) -> Result<()> {
        info!("Unmounting PowerFS from: {}", self.mount_point);
        let _ = mount::umount(self.mount_point.as_str());
        Ok(())
    }
}
