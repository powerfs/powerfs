use crate::proto::powerfs::metadata_notification::EventType;
use crate::proto::powerfs::{DirEntry, InodeEntry, PathIndexEntry};
use crate::proto::{Entry, MetadataNotification};
use log::{debug, info, warn};
use prost::Message;
use rocksdb::{IteratorMode, Options, DB};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct Lease {
    pub lease_id: String,
    pub path: String,
    pub client_id: String,
    pub expires_at: std::time::Instant,
    pub epoch: u64,
}

pub struct JobInfo {
    pub job_id: String,
    pub job_name: String,
    pub client_ids: HashSet<String>,
    pub start_time: u64,
    pub end_time: u64,
    pub is_active: bool,
}

pub struct DirectoryTree {
    db: DB,
    inode_counter: std::sync::atomic::AtomicU64,
    generation_counter: std::sync::atomic::AtomicU64,
    epoch: std::sync::atomic::AtomicU64,
    notifier: Arc<broadcast::Sender<MetadataNotification>>,
    subscribers: std::sync::RwLock<HashSet<String>>,
    pub leases: std::sync::RwLock<HashMap<String, Lease>>,
    path_lease_map: std::sync::RwLock<HashMap<String, HashSet<String>>>,
    jobs: std::sync::RwLock<HashMap<String, JobInfo>>,
    current_job_id: std::sync::RwLock<Option<String>>,
}

impl DirectoryTree {
    pub fn new(path: &Path) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = DB::open(&opts, path)?;

        let inode_counter = Self::load_inode_counter(&db);
        let generation_counter = Self::load_generation_counter(&db);
        let epoch = Self::load_and_increment_epoch(&db);
        let (notifier, _) = broadcast::channel(10000);

        Ok(DirectoryTree {
            db,
            inode_counter,
            generation_counter,
            epoch: std::sync::atomic::AtomicU64::new(epoch),
            notifier: Arc::new(notifier),
            subscribers: std::sync::RwLock::new(HashSet::new()),
            leases: std::sync::RwLock::new(HashMap::new()),
            path_lease_map: std::sync::RwLock::new(HashMap::new()),
            jobs: std::sync::RwLock::new(HashMap::new()),
            current_job_id: std::sync::RwLock::new(None),
        })
    }

    fn load_inode_counter(db: &DB) -> std::sync::atomic::AtomicU64 {
        if let Ok(Some(val)) = db.get(b"inode_counter") {
            if let Ok(s) = String::from_utf8(val) {
                if let Ok(counter) = s.parse::<u64>() {
                    return std::sync::atomic::AtomicU64::new(counter);
                }
            }
        }
        std::sync::atomic::AtomicU64::new(2)
    }

    fn load_generation_counter(db: &DB) -> std::sync::atomic::AtomicU64 {
        if let Ok(Some(val)) = db.get(b"generation_counter") {
            if let Ok(s) = String::from_utf8(val) {
                if let Ok(counter) = s.parse::<u64>() {
                    return std::sync::atomic::AtomicU64::new(counter);
                }
            }
        }
        std::sync::atomic::AtomicU64::new(1)
    }

    fn load_and_increment_epoch(db: &DB) -> u64 {
        let current = if let Ok(Some(val)) = db.get(b"epoch") {
            if let Ok(s) = String::from_utf8(val) {
                s.parse::<u64>().unwrap_or(0)
            } else {
                0
            }
        } else {
            0
        };
        let new_epoch = current + 1;
        let _ = db.put(b"epoch", new_epoch.to_string().as_bytes());
        debug!(
            "Master epoch loaded: {} -> {} (incremented on restart)",
            current, new_epoch
        );
        new_epoch
    }

    pub fn get_epoch(&self) -> u64 {
        self.epoch.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn allocate_generation(&self) -> u64 {
        let generation = self
            .generation_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _ = self
            .db
            .put(b"generation_counter", generation.to_string().as_bytes());
        generation
    }

    fn allocate_inode(&self) -> u64 {
        let inode = self
            .inode_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _ = self.db.put(b"inode_counter", inode.to_string().as_bytes());
        inode
    }

    fn inode_key(ino: u64) -> Vec<u8> {
        format!("inode:{}", ino).as_bytes().to_vec()
    }

    fn dir_key(parent_ino: u64, name: &str) -> Vec<u8> {
        format!("dir:{}:{}", parent_ino, name).as_bytes().to_vec()
    }

    fn dir_prefix(parent_ino: u64) -> Vec<u8> {
        format!("dir:{}:", parent_ino).as_bytes().to_vec()
    }

    fn path_key(path: &str) -> Vec<u8> {
        format!("path:{}", path).as_bytes().to_vec()
    }

    pub fn lookup(&self, parent_ino: u64, name: &str) -> Option<Entry> {
        let dir_key = Self::dir_key(parent_ino, name);
        if let Ok(Some(data)) = self.db.get(&dir_key) {
            let decode_result: Result<DirEntry, _> = prost::Message::decode(data.as_ref());
            if let Ok(dir_entry) = decode_result {
                return self
                    .get_entry_by_inode_internal(dir_entry.child_ino)
                    .map(|e| e.0);
            }
        }
        None
    }

    fn get_entry_by_inode_internal(&self, ino: u64) -> Option<(Entry, String)> {
        let inode_key = Self::inode_key(ino);
        if let Ok(Some(data)) = self.db.get(&inode_key) {
            if let Ok(inode_entry) = prost::Message::decode(data.as_ref()) {
                let path = self.get_path_by_inode(ino);
                return Some((self.inode_entry_to_entry(&inode_entry, &path), path));
            }
        }
        None
    }

    pub fn get_entry(&self, path: &str) -> Option<Entry> {
        let path_key = Self::path_key(path);
        if let Ok(Some(data)) = self.db.get(&path_key) {
            let decode_result: Result<PathIndexEntry, _> = prost::Message::decode(data.as_ref());
            if let Ok(path_index) = decode_result {
                return self
                    .get_entry_by_inode_internal(path_index.ino)
                    .map(|e| e.0);
            }
        }
        None
    }

    pub fn get_entry_by_inode(&self, ino: u64) -> Option<(Entry, String)> {
        self.get_entry_by_inode_internal(ino)
    }

    fn get_path_by_inode(&self, ino: u64) -> String {
        if ino == 1 {
            return "/".to_string();
        }

        let mut path = String::new();
        let mut current_ino = ino;

        loop {
            let inode_key = Self::inode_key(current_ino);
            if let Ok(Some(data)) = self.db.get(&inode_key) {
                let decode_result: Result<InodeEntry, _> = prost::Message::decode(data.as_ref());
                if let Ok(inode_entry) = decode_result {
                    let name = inode_entry.name;
                    if current_ino == 1 {
                        break;
                    }
                    path.insert_str(0, &format!("/{}", name));
                    current_ino = inode_entry.parent_ino;
                    if current_ino == 0 {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if path.is_empty() {
            "/".to_string()
        } else {
            path
        }
    }

    fn inode_entry_to_entry(&self, inode_entry: &InodeEntry, _path: &str) -> Entry {
        Entry {
            name: inode_entry.name.clone(),
            directory: if inode_entry.parent_ino == 0 {
                "/".to_string()
            } else {
                self.get_path_by_inode(inode_entry.parent_ino)
            },
            attributes: inode_entry.attributes.clone(),
            chunks: inode_entry.chunks.clone(),
            hard_link_id: inode_entry.hard_link_id.clone(),
            hard_link_counter: inode_entry.hard_link_counter,
            extended: inode_entry.extended.clone(),
            content_size: inode_entry.content_size,
            disk_size: inode_entry.disk_size,
            ttl: inode_entry.ttl.clone(),
            symlink_target: inode_entry.symlink_target.clone(),
            owner: inode_entry.owner.clone(),
            generation: inode_entry.generation,
        }
    }

    fn entry_to_inode_entry(&self, entry: &Entry, parent_ino: u64) -> InodeEntry {
        InodeEntry {
            ino: entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0),
            name: entry.name.clone(),
            parent_ino,
            attributes: entry.attributes.clone(),
            chunks: entry.chunks.clone(),
            symlink_target: entry.symlink_target.clone(),
            hard_link_id: entry.hard_link_id.clone(),
            hard_link_counter: entry.hard_link_counter,
            generation: entry.generation,
            extended: entry.extended.clone(),
            content_size: entry.content_size,
            disk_size: entry.disk_size,
            ttl: entry.ttl.clone(),
            owner: entry.owner.clone(),
            backend: Default::default(),
            s3_location: None,
            kv_location: None,
            stripe_config: None,
        }
    }

    fn get_parent_ino(&self, directory: &str) -> Option<u64> {
        if directory == "/" || directory.is_empty() {
            return Some(1);
        }

        let path_key = Self::path_key(directory);
        if let Ok(Some(data)) = self.db.get(&path_key) {
            let decode_result: Result<PathIndexEntry, _> = prost::Message::decode(data.as_ref());
            if let Ok(path_index) = decode_result {
                return Some(path_index.ino);
            }
        }
        None
    }

    pub fn create_directory(&self, path: &str) -> Result<u64, rocksdb::Error> {
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        let mut current_ino = 1;

        for part in parts {
            if self.lookup(current_ino, part).is_some() {
                let entry = self.lookup(current_ino, part).unwrap();
                current_ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
                continue;
            }

            let inode = self.allocate_inode();
            let generation = self.allocate_generation();

            let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
            let inode_entry = InodeEntry {
                ino: inode,
                name: part.to_string(),
                parent_ino: current_ino,
                attributes: Some(crate::proto::FuseAttributes {
                    ino: inode,
                    mode: 0o40755,
                    nlink: 2,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    size: 4096,
                    blksize: 4096,
                    blocks: 1,
                    atime: now,
                    mtime: now,
                    ctime: now,
                    crtime: now,
                    perm: 0o755,
                }),
                chunks: vec![],
                symlink_target: "".to_string(),
                hard_link_id: "".to_string(),
                hard_link_counter: 0,
                generation,
                extended: HashMap::new(),
                content_size: 4096,
                disk_size: 4096,
                ttl: "".to_string(),
                owner: String::new(),
                backend: Default::default(),
                s3_location: None,
                kv_location: None,
                stripe_config: None,
            };

            let mut inode_data = Vec::new();
            inode_entry
                .encode(&mut inode_data)
                .expect("failed to encode inode entry");
            self.db.put(Self::inode_key(inode), &inode_data)?;

            let dir_entry = DirEntry {
                parent_ino: current_ino,
                name: part.to_string(),
                child_ino: inode,
                child_type: 1,
                mode: 0o40755,
                size: 4096,
                mtime: now,
                nlink: 2,
            };

            let mut dir_data = Vec::new();
            dir_entry
                .encode(&mut dir_data)
                .expect("failed to encode dir entry");
            self.db.put(Self::dir_key(current_ino, part), &dir_data)?;

            let full_path = if current_ino == 1 {
                format!("/{}", part)
            } else {
                let parent_path = self.get_path_by_inode(current_ino);
                format!("{}/{}", parent_path, part)
            };

            let path_index = PathIndexEntry {
                ino: inode,
                parent_ino: current_ino,
                generation,
            };

            let mut path_data = Vec::new();
            path_index
                .encode(&mut path_data)
                .expect("failed to encode path index");
            self.db.put(Self::path_key(&full_path), &path_data)?;

            self.publish_notification(
                EventType::Create,
                &full_path,
                Some(self.inode_entry_to_entry(&inode_entry, &full_path)),
                "",
            );

            current_ino = inode;
        }

        Ok(current_ino)
    }

    pub fn create_entry(&self, mut entry: Entry, client_id: &str) -> Result<u64, rocksdb::Error> {
        let parent_ino = match self.get_parent_ino(&entry.directory) {
            Some(ino) => ino,
            None => return Ok(0),
        };

        // OR-Set 架构：客户端拥有 inode 分配权
        // 如果客户端提供了有效 inode（>= 100，避开系统保留段），则使用客户端 inode
        // 否则由 Master 分配（兼容旧客户端）
        let client_ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
        let inode = if client_ino >= 100 {
            // 使用客户端提供的 inode，并推进 Master 的 inode_counter 以避免未来冲突
            let current = self.inode_counter.load(std::sync::atomic::Ordering::SeqCst);
            if client_ino >= current {
                self.inode_counter
                    .store(client_ino + 1, std::sync::atomic::Ordering::SeqCst);
                let _ = self
                    .db
                    .put(b"inode_counter", (client_ino + 1).to_string().as_bytes());
            }
            client_ino
        } else {
            self.allocate_inode()
        };
        let generation = self.allocate_generation();

        if let Some(attrs) = &mut entry.attributes {
            attrs.ino = inode;
        }
        entry.generation = generation;

        let inode_entry = self.entry_to_inode_entry(&entry, parent_ino);

        let mut inode_data = Vec::new();
        inode_entry
            .encode(&mut inode_data)
            .expect("failed to encode inode entry");
        self.db.put(Self::inode_key(inode), &inode_data)?;

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let mode_val = entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0);
        let size_val = entry.attributes.as_ref().map(|a| a.size).unwrap_or(0);
        let nlink_val = entry.attributes.as_ref().map(|a| a.nlink).unwrap_or(1);

        let dir_entry = DirEntry {
            parent_ino,
            name: entry.name.clone(),
            child_ino: inode,
            child_type: if (mode_val & 0o40000) != 0 { 1 } else { 0 },
            mode: mode_val,
            size: size_val,
            mtime: now,
            nlink: nlink_val,
        };

        let mut dir_data = Vec::new();
        dir_entry
            .encode(&mut dir_data)
            .expect("failed to encode dir entry");
        self.db
            .put(Self::dir_key(parent_ino, &entry.name), &dir_data)?;

        let full_path = if parent_ino == 1 {
            format!("/{}", entry.name)
        } else {
            let parent_path = self.get_path_by_inode(parent_ino);
            format!("{}/{}", parent_path, entry.name)
        };

        let path_index = PathIndexEntry {
            ino: inode,
            parent_ino,
            generation,
        };

        let mut path_data = Vec::new();
        path_index
            .encode(&mut path_data)
            .expect("failed to encode path index");
        self.db.put(Self::path_key(&full_path), &path_data)?;

        self.publish_notification(EventType::Create, &full_path, Some(entry), client_id);

        Ok(inode)
    }

    pub fn update_entry(&self, mut entry: Entry, client_id: &str) -> Result<(), rocksdb::Error> {
        let ino = match entry.attributes.as_ref().map(|a| a.ino) {
            Some(ino) => ino,
            None => return Ok(()),
        };

        let received_mode = entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0);
        let received_file_type = received_mode & 0o170000;
        info!(
            "[DIR_TREE] update_entry: ino={}, received_mode={:o}, received_file_type={:o}, name={}, directory={}",
            ino, received_mode, received_file_type, entry.name, entry.directory
        );

        let generation = self.allocate_generation();
        entry.generation = generation;

        let inode_key = Self::inode_key(ino);
        let existing_data = self.db.get(&inode_key)?;
        if existing_data.is_none() {
            return Ok(());
        }

        let decode_result: Result<InodeEntry, _> =
            prost::Message::decode(existing_data.unwrap().as_ref());
        let existing_entry = match decode_result {
            Ok(e) => e,
            Err(_) => return Ok(()),
        };

        let existing_mode = existing_entry
            .attributes
            .as_ref()
            .map(|a| a.mode)
            .unwrap_or(0);
        let existing_file_type = existing_mode & 0o170000;
        info!(
            "[DIR_TREE] update_entry: ino={}, existing_mode={:o}, existing_file_type={:o}",
            ino, existing_mode, existing_file_type
        );

        let inode_entry = self.entry_to_inode_entry(&entry, existing_entry.parent_ino);

        let stored_mode = inode_entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0);
        let stored_file_type = stored_mode & 0o170000;
        info!(
            "[DIR_TREE] update_entry: ino={}, stored_mode={:o}, stored_file_type={:o}",
            ino, stored_mode, stored_file_type
        );

        let mut inode_data = Vec::new();
        inode_entry
            .encode(&mut inode_data)
            .expect("failed to encode inode entry");
        self.db.put(&inode_key, &inode_data)?;

        let dir_entry = DirEntry {
            parent_ino: existing_entry.parent_ino,
            name: entry.name.clone(),
            child_ino: ino,
            child_type: if (entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0) & 0o40000) != 0 {
                1
            } else {
                0
            },
            mode: entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0),
            size: entry.attributes.as_ref().map(|a| a.size).unwrap_or(0),
            mtime: entry.attributes.as_ref().map(|a| a.mtime).unwrap_or(0),
            nlink: entry.attributes.as_ref().map(|a| a.nlink).unwrap_or(1),
        };

        let mut dir_data = Vec::new();
        dir_entry
            .encode(&mut dir_data)
            .expect("failed to encode dir entry");
        self.db.put(
            Self::dir_key(existing_entry.parent_ino, &entry.name),
            &dir_data,
        )?;

        let path = self.get_path_by_inode(ino);
        self.publish_notification(EventType::Update, &path, Some(entry), client_id);

        Ok(())
    }

    pub fn delete_entry(&self, ino: u64, client_id: &str) -> Result<bool, rocksdb::Error> {
        let inode_key = Self::inode_key(ino);
        let existing_data = self.db.get(&inode_key)?;
        if existing_data.is_none() {
            return Ok(false);
        }

        let decode_result: Result<InodeEntry, _> =
            prost::Message::decode(existing_data.unwrap().as_ref());
        let inode_entry = match decode_result {
            Ok(e) => e,
            Err(_) => return Ok(false),
        };

        let path = self.get_path_by_inode(ino);
        let is_directory = inode_entry
            .attributes
            .as_ref()
            .map(|a| (a.mode & 0o40000) != 0)
            .unwrap_or(false);

        if is_directory {
            let mut to_delete = Vec::new();
            let mut stack = vec![ino];

            while let Some(current_ino) = stack.pop() {
                let dir_prefix = Self::dir_prefix(current_ino);
                let mut iter = self
                    .db
                    .iterator(IteratorMode::From(&dir_prefix, rocksdb::Direction::Forward));
                while let Some(Ok((key, value))) = iter.next() {
                    if !key.starts_with(&dir_prefix) {
                        break;
                    }
                    let dir_decode: Result<DirEntry, _> = prost::Message::decode(value.as_ref());
                    if let Ok(dir_entry) = dir_decode {
                        let child_ino = dir_entry.child_ino;
                        to_delete.push(child_ino);
                        let child_inode_key = Self::inode_key(child_ino);
                        if let Ok(Some(child_data)) = self.db.get(&child_inode_key) {
                            let child_decode: Result<InodeEntry, _> =
                                prost::Message::decode(child_data.as_ref());
                            if let Ok(child_inode) = child_decode {
                                if (child_inode.attributes.as_ref().map(|a| a.mode).unwrap_or(0)
                                    & 0o40000)
                                    != 0
                                {
                                    stack.push(child_ino);
                                }
                            }
                        }
                    }
                }
            }

            for child_ino in to_delete {
                let child_path = self.get_path_by_inode(child_ino);
                let child_inode_key = Self::inode_key(child_ino);

                let child_inode_data = self.db.get(&child_inode_key)?;

                self.db.delete(&child_inode_key)?;

                if let Some(data) = child_inode_data {
                    let child_decode: Result<InodeEntry, _> = prost::Message::decode(data.as_ref());
                    if let Ok(child_inode) = child_decode {
                        let dir_key = Self::dir_key(child_inode.parent_ino, &child_inode.name);
                        self.db.delete(&dir_key)?;
                    }
                }

                let path_key = Self::path_key(&child_path);
                self.db.delete(&path_key)?;

                self.publish_notification(EventType::Delete, &child_path, None, client_id);
            }
        }

        self.db.delete(&inode_key)?;

        let dir_key = Self::dir_key(inode_entry.parent_ino, &inode_entry.name);
        self.db.delete(&dir_key)?;

        let path_key = Self::path_key(&path);
        self.db.delete(&path_key)?;

        self.publish_notification(EventType::Delete, &path, None, client_id);
        Ok(true)
    }

    pub fn delete_entry_by_path(
        &self,
        path: &str,
        client_id: &str,
    ) -> Result<bool, rocksdb::Error> {
        let entry = self.get_entry(path);
        if entry.is_none() {
            return Ok(false);
        }
        let ino = entry
            .unwrap()
            .attributes
            .as_ref()
            .map(|a| a.ino)
            .unwrap_or(0);
        if ino == 0 {
            return Ok(false);
        }
        self.delete_entry(ino, client_id)
    }

    pub fn rename_entry(
        &self,
        old_parent_ino: u64,
        old_name: &str,
        new_parent_ino: u64,
        new_name: &str,
        client_id: &str,
    ) -> Result<bool, rocksdb::Error> {
        let old_dir_key = Self::dir_key(old_parent_ino, old_name);
        let old_data = self.db.get(&old_dir_key)?;
        if old_data.is_none() {
            return Ok(false);
        }

        let dir_decode: Result<DirEntry, _> = prost::Message::decode(old_data.unwrap().as_ref());
        let dir_entry = match dir_decode {
            Ok(e) => e,
            Err(_) => return Ok(false),
        };

        let ino = dir_entry.child_ino;

        let inode_key = Self::inode_key(ino);
        let inode_data = self.db.get(&inode_key)?;
        if inode_data.is_none() {
            return Ok(false);
        }

        let inode_decode: Result<InodeEntry, _> =
            prost::Message::decode(inode_data.unwrap().as_ref());
        let mut inode_entry = match inode_decode {
            Ok(e) => e,
            Err(_) => return Ok(false),
        };

        let old_path = self.get_path_by_inode(ino);

        let generation = self.allocate_generation();
        inode_entry.generation = generation;
        inode_entry.name = new_name.to_string();
        inode_entry.parent_ino = new_parent_ino;

        if let Some(attrs) = &mut inode_entry.attributes {
            attrs.ino = ino;
        }

        let mut new_inode_data = Vec::new();
        inode_entry
            .encode(&mut new_inode_data)
            .expect("failed to encode inode entry");
        self.db.put(&inode_key, &new_inode_data)?;

        self.db.delete(&old_dir_key)?;

        let new_dir_entry = DirEntry {
            parent_ino: new_parent_ino,
            name: new_name.to_string(),
            child_ino: ino,
            child_type: dir_entry.child_type,
            mode: dir_entry.mode,
            size: dir_entry.size,
            mtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            nlink: dir_entry.nlink,
        };

        let mut new_dir_data = Vec::new();
        new_dir_entry
            .encode(&mut new_dir_data)
            .expect("failed to encode dir entry");
        self.db
            .put(Self::dir_key(new_parent_ino, new_name), &new_dir_data)?;

        self.db.delete(Self::path_key(&old_path))?;

        let new_path = if new_parent_ino == 1 {
            format!("/{}", new_name)
        } else {
            let parent_path = self.get_path_by_inode(new_parent_ino);
            format!("{}/{}", parent_path, new_name)
        };

        let new_path_index = PathIndexEntry {
            ino,
            parent_ino: new_parent_ino,
            generation,
        };

        let mut new_path_data = Vec::new();
        new_path_index
            .encode(&mut new_path_data)
            .expect("failed to encode path index");
        self.db.put(Self::path_key(&new_path), &new_path_data)?;

        self.publish_notification(EventType::Delete, &old_path, None, client_id);
        self.publish_notification(
            EventType::Rename,
            &new_path,
            Some(self.inode_entry_to_entry(&inode_entry, &new_path)),
            client_id,
        );

        Ok(true)
    }

    pub fn list_entries(&self, parent_ino: u64, limit: u64, last_name: &str) -> Vec<Entry> {
        let dir_prefix = Self::dir_prefix(parent_ino);
        let mut entries = Vec::new();

        let mut iter = self
            .db
            .iterator(IteratorMode::From(&dir_prefix, rocksdb::Direction::Forward));
        let mut count = 0u64;

        while let Some(Ok((key, value))) = iter.next() {
            if !key.starts_with(&dir_prefix) {
                break;
            }

            let dir_decode: Result<DirEntry, _> = prost::Message::decode(value.as_ref());
            if let Ok(dir_entry) = dir_decode {
                let entry_name = dir_entry.name.clone();

                if !last_name.is_empty() && entry_name.as_str() <= last_name {
                    continue;
                }

                if let Some((entry, _)) = self.get_entry_by_inode_internal(dir_entry.child_ino) {
                    entries.push(entry);
                    count += 1;
                    if count >= limit {
                        break;
                    }
                }
            }
        }

        entries
    }

    pub fn init_root(&self) -> Result<(), rocksdb::Error> {
        if self.get_entry("/").is_none() {
            let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
            let root_inode = InodeEntry {
                ino: 1,
                name: "/".to_string(),
                parent_ino: 0,
                attributes: Some(crate::proto::FuseAttributes {
                    ino: 1,
                    mode: 0o40755,
                    nlink: 2,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    size: 4096,
                    blksize: 4096,
                    blocks: 1,
                    atime: now,
                    mtime: now,
                    ctime: now,
                    crtime: now,
                    perm: 0o755,
                }),
                chunks: vec![],
                symlink_target: "".to_string(),
                hard_link_id: "".to_string(),
                hard_link_counter: 0,
                generation: 1,
                extended: HashMap::new(),
                content_size: 4096,
                disk_size: 4096,
                ttl: "".to_string(),
                owner: String::new(),
                backend: Default::default(),
                s3_location: None,
                kv_location: None,
                stripe_config: None,
            };

            let mut inode_data = Vec::new();
            root_inode
                .encode(&mut inode_data)
                .expect("failed to encode root inode");
            self.db.put(Self::inode_key(1), &inode_data)?;

            let path_index = PathIndexEntry {
                ino: 1,
                parent_ino: 0,
                generation: 1,
            };

            let mut path_data = Vec::new();
            path_index
                .encode(&mut path_data)
                .expect("failed to encode path index");
            self.db.put(Self::path_key("/"), &path_data)?;

            let _ = self.inode_counter.compare_exchange(
                2,
                2,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            );
        }

        Ok(())
    }

    fn publish_notification(
        &self,
        event_type: EventType,
        path: &str,
        entry: Option<Entry>,
        client_id: &str,
    ) {
        let generation = entry.as_ref().map(|e| e.generation).unwrap_or(0);
        let epoch = self.get_epoch();
        let job_id = self
            .current_job_id
            .read()
            .unwrap()
            .clone()
            .unwrap_or_default();
        let notification = MetadataNotification {
            event_type: event_type as i32,
            path: path.to_string(),
            entry,
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            generation,
            old_path: String::new(),
            epoch,
            job_id,
            source_client_id: client_id.to_string(),
        };
        let _ = self.notifier.send(notification);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MetadataNotification> {
        self.notifier.subscribe()
    }

    pub fn add_subscriber(&self, path_prefix: &str) {
        let mut subscribers = self.subscribers.write().unwrap();
        subscribers.insert(path_prefix.to_string());
    }

    pub fn acquire_lease(&self, path: &str, client_id: &str, duration_ms: u64) -> String {
        // Opportunistic cleanup of expired leases to bound memory usage.
        self.cleanup_expired_leases();

        let lease_id = uuid::Uuid::new_v4().to_string();
        let expires_at = std::time::Instant::now() + std::time::Duration::from_millis(duration_ms);
        let epoch = self.get_epoch();

        let lease = Lease {
            lease_id: lease_id.clone(),
            path: path.to_string(),
            client_id: client_id.to_string(),
            expires_at,
            epoch,
        };

        {
            let mut leases = self.leases.write().unwrap();
            leases.insert(lease_id.clone(), lease);
        }

        {
            let mut path_lease_map = self.path_lease_map.write().unwrap();
            path_lease_map
                .entry(path.to_string())
                .or_default()
                .insert(lease_id.clone());
        }

        lease_id
    }

    pub fn release_lease(&self, lease_id: &str) -> bool {
        let lease = {
            let mut leases = self.leases.write().unwrap();
            leases.remove(lease_id)
        };

        if let Some(lease) = lease {
            let mut path_lease_map = self.path_lease_map.write().unwrap();
            if let Some(lease_ids) = path_lease_map.get_mut(&lease.path) {
                lease_ids.remove(lease_id);
                if lease_ids.is_empty() {
                    path_lease_map.remove(&lease.path);
                }
            }
            true
        } else {
            false
        }
    }

    pub fn renew_lease(&self, lease_id: &str, duration_ms: u64) -> Option<u64> {
        let mut leases = self.leases.write().unwrap();
        if let Some(lease) = leases.get_mut(lease_id) {
            lease.expires_at =
                std::time::Instant::now() + std::time::Duration::from_millis(duration_ms);
            let epoch = lease.epoch;
            debug!(
                "Renewed lease {}: new expiry in {}ms",
                lease_id, duration_ms
            );
            Some(epoch)
        } else {
            None
        }
    }

    pub fn has_active_lease(&self, path: &str) -> bool {
        let now = std::time::Instant::now();
        let current_epoch = self.get_epoch();

        if let Some(lease_ids) = self.path_lease_map.read().unwrap().get(path) {
            let leases = self.leases.read().unwrap();
            for lease_id in lease_ids {
                if let Some(lease) = leases.get(lease_id) {
                    if lease.epoch == current_epoch && lease.expires_at > now {
                        return true;
                    }
                }
            }
        }
        false
    }

    pub fn cleanup_expired_leases(&self) {
        let now = std::time::Instant::now();

        // Collect expired lease ids and their paths atomically under a single write lock
        // to avoid TOCTOU races with concurrent release_lease.
        let expired: Vec<(String, String)> = {
            let leases = self.leases.read().unwrap();
            leases
                .iter()
                .filter(|(_, lease)| lease.expires_at <= now)
                .map(|(id, lease)| (id.clone(), lease.path.clone()))
                .collect()
        };

        if expired.is_empty() {
            return;
        }

        let expired_count = expired.len();
        for (lease_id, path) in &expired {
            {
                let mut leases = self.leases.write().unwrap();
                leases.remove(lease_id);
            }
            let mut path_lease_map = self.path_lease_map.write().unwrap();
            if let Some(lease_ids) = path_lease_map.get_mut(path) {
                lease_ids.remove(lease_id);
                if lease_ids.is_empty() {
                    path_lease_map.remove(path);
                }
            }
        }

        debug!("Cleaned up {} expired leases", expired_count);
    }

    pub fn remove_subscriber(&self, path_prefix: &str) {
        let mut subscribers = self.subscribers.write().unwrap();
        subscribers.remove(path_prefix);
    }

    pub fn register_job_client(&self, job_id: &str, job_name: &str, client_id: &str) -> bool {
        let mut jobs = self.jobs.write().unwrap();
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        if let Some(job) = jobs.get_mut(job_id) {
            job.client_ids.insert(client_id.to_string());
            debug!(
                "Client {} joined job {} (total clients: {})",
                client_id,
                job_id,
                job.client_ids.len()
            );
        } else {
            let mut client_ids = HashSet::new();
            client_ids.insert(client_id.to_string());
            jobs.insert(
                job_id.to_string(),
                JobInfo {
                    job_id: job_id.to_string(),
                    job_name: job_name.to_string(),
                    client_ids,
                    start_time: now,
                    end_time: 0,
                    is_active: true,
                },
            );
            debug!("New job registered: {} ({})", job_id, job_name);
        }
        drop(jobs);
        *self.current_job_id.write().unwrap() = Some(job_id.to_string());
        true
    }

    pub fn deregister_job_client(&self, job_id: &str, client_id: &str) -> bool {
        let mut jobs = self.jobs.write().unwrap();
        if let Some(job) = jobs.get_mut(job_id) {
            job.client_ids.remove(client_id);
            debug!(
                "Client {} left job {} (remaining clients: {})",
                client_id,
                job_id,
                job.client_ids.len()
            );
            if job.client_ids.is_empty() {
                job.is_active = false;
                job.end_time = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
            }
            true
        } else {
            false
        }
    }

    pub fn complete_job(&self, job_id: &str) -> Option<u64> {
        let mut jobs = self.jobs.write().unwrap();
        if let Some(job) = jobs.get_mut(job_id) {
            job.is_active = false;
            job.end_time = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

            let client_count = job.client_ids.len() as u64;
            debug!("Job {} completed with {} clients", job_id, client_count);

            drop(jobs);
            self.publish_notification(EventType::JobComplete, "/", None, "");

            Some(client_count)
        } else {
            None
        }
    }

    pub fn get_job_info(&self, job_id: &str) -> Option<JobInfo> {
        let jobs = self.jobs.read().unwrap();
        jobs.get(job_id).map(|j| JobInfo {
            job_id: j.job_id.clone(),
            job_name: j.job_name.clone(),
            client_ids: j.client_ids.clone(),
            start_time: j.start_time,
            end_time: j.end_time,
            is_active: j.is_active,
        })
    }

    pub fn is_job_active(&self, job_id: &str) -> bool {
        let jobs = self.jobs.read().unwrap();
        jobs.get(job_id).is_some_and(|j| j.is_active)
    }

    pub fn push_delta(
        &self,
        client_id: &str,
        deltas: &[crate::proto::powerfs::DeltaOp],
    ) -> Result<crate::proto::powerfs::VectorClock, String> {
        debug!(
            "push_delta: client_id={}, count={}",
            client_id,
            deltas.len()
        );

        for delta in deltas {
            match &delta.op {
                Some(crate::proto::powerfs::delta_op::Op::Add(entry)) => {
                    let _parent_ino = entry.parent_ino;
                    let name = entry
                        .id
                        .as_ref()
                        .map(|id| id.name.clone())
                        .unwrap_or_default();
                    let mode = entry.mode;

                    let attrs = crate::proto::powerfs::FuseAttributes {
                        ino: entry.inode,
                        mode,
                        nlink: entry.nlink,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        size: entry.size,
                        blksize: 4096,
                        blocks: entry.size.div_ceil(512),
                        atime: entry.atime,
                        mtime: entry.mtime,
                        ctime: entry.ctime,
                        crtime: entry.ctime,
                        perm: mode & 0o777,
                    };

                    let entry_proto = crate::proto::powerfs::Entry {
                        name: name.clone(),
                        directory: "/".to_string(),
                        attributes: Some(attrs),
                        chunks: Vec::new(),
                        hard_link_id: String::new(),
                        hard_link_counter: 0,
                        extended: HashMap::new(),
                        content_size: entry.size,
                        disk_size: entry.size,
                        ttl: String::new(),
                        symlink_target: entry.symlink_target.clone(),
                        owner: String::new(),
                        generation: self.allocate_generation(),
                    };

                    if let Err(e) = self.create_entry(entry_proto, client_id) {
                        warn!("push_delta: create_entry failed: {}", e);
                    }
                }
                Some(crate::proto::powerfs::delta_op::Op::Remove(id)) => {
                    let parent_ino = 0;
                    let name = id.name.clone();
                    let ino = self
                        .lookup(parent_ino, &name)
                        .map(|e| e.attributes.as_ref().map(|a| a.ino).unwrap_or(0))
                        .unwrap_or(0);
                    if ino > 0 {
                        if let Err(e) = self.delete_entry(ino, client_id) {
                            warn!("push_delta: delete_entry failed: {}", e);
                        }
                    }
                }
                Some(crate::proto::powerfs::delta_op::Op::Rename(op)) => {
                    let old_name = op
                        .old_id
                        .as_ref()
                        .map(|id| id.name.clone())
                        .unwrap_or_default();
                    let new_entry = op.new_entry.as_ref();
                    let new_name = new_entry
                        .and_then(|e| e.id.as_ref())
                        .map(|id| id.name.clone())
                        .unwrap_or_default();
                    let old_parent_ino = 0;
                    let new_parent_ino = new_entry.map(|e| e.parent_ino).unwrap_or(0);

                    if let Err(e) = self.rename_entry(
                        old_parent_ino,
                        &old_name,
                        new_parent_ino,
                        &new_name,
                        client_id,
                    ) {
                        warn!("push_delta: rename_entry failed: {}", e);
                    }
                }
                Some(crate::proto::powerfs::delta_op::Op::SetAttr(op)) => {
                    let ino = op.inode;
                    if let Some((mut entry, _path)) = self.get_entry_by_inode_internal(ino) {
                        if let Some(attrs) = entry.attributes.as_mut() {
                            if op.size > 0 {
                                attrs.size = op.size;
                                attrs.blocks = op.size.div_ceil(512);
                            }
                            if op.mtime > 0 {
                                attrs.mtime = op.mtime;
                            }
                            if op.mode > 0 {
                                attrs.mode = op.mode;
                            }
                        }
                        entry.content_size = op.size.max(entry.content_size);
                        entry.disk_size = op.size.max(entry.disk_size);
                        entry.generation = self.allocate_generation();

                        if let Err(e) = self.update_entry(entry, client_id) {
                            warn!("push_delta: update_entry failed: {}", e);
                        }
                    }
                }
                None => {}
            }
        }

        Ok(crate::proto::powerfs::VectorClock {
            entries: Vec::new(),
        })
    }

    pub fn pull_delta(
        &self,
        client_id: &str,
    ) -> Result<
        (
            Vec<crate::proto::powerfs::DeltaOp>,
            crate::proto::powerfs::VectorClock,
        ),
        String,
    > {
        debug!("pull_delta: client_id={}", client_id);

        Ok((
            Vec::new(),
            crate::proto::powerfs::VectorClock {
                entries: Vec::new(),
            },
        ))
    }
}
