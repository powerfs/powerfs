use crate::proto::powerfs::metadata_notification::EventType;
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

    fn path_to_key(directory: &str, name: &str) -> Vec<u8> {
        if directory == "/" {
            format!("/{}", name).into_bytes()
        } else {
            format!("{}/{}", directory, name).into_bytes()
        }
    }

    fn path_prefix(directory: &str) -> Vec<u8> {
        if directory == "/" {
            b"/".to_vec()
        } else {
            format!("{}/", directory).into_bytes()
        }
    }

    pub fn lookup(&self, directory: &str, name: &str) -> Option<Entry> {
        let key = Self::path_to_key(directory, name);
        if let Ok(Some(data)) = self.db.get(&key) {
            if let Ok(entry) = prost::Message::decode(data.as_ref()) {
                return Some(entry);
            }
        }
        None
    }

    pub fn get_entry(&self, path: &str) -> Option<Entry> {
        if let Ok(Some(data)) = self.db.get(path.as_bytes()) {
            if let Ok(entry) = prost::Message::decode(data.as_ref()) {
                return Some(entry);
            }
        }
        None
    }

    fn inode_to_key(inode: u64) -> Vec<u8> {
        format!("inode:{}", inode).as_bytes().to_vec()
    }

    pub fn get_entry_by_inode(&self, inode: u64) -> Option<(Entry, String)> {
        let inode_key = Self::inode_to_key(inode);
        if let Ok(Some(path_bytes)) = self.db.get(&inode_key) {
            let path = String::from_utf8_lossy(&path_bytes).to_string();
            if let Some(entry) = self.get_entry(&path) {
                return Some((entry, path));
            }
        }
        None
    }

    pub fn create_directory(&self, path: &str) -> Result<u64, rocksdb::Error> {
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        let mut current_path = "/".to_string();

        for part in parts {
            let parent_path = current_path.clone();
            current_path = if current_path == "/" {
                format!("/{}", part)
            } else {
                format!("{}/{}", current_path, part)
            };

            if self.get_entry(&current_path).is_none() {
                let entry = Entry {
                    name: part.to_string(),
                    directory: parent_path,
                    attributes: Some(crate::proto::FuseAttributes {
                        ino: 0,
                        mode: 0o40755,
                        nlink: 2,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        size: 4096,
                        blksize: 4096,
                        blocks: 1,
                        atime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                        mtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                        ctime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                        crtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                        perm: 0o755,
                    }),
                    chunks: vec![],
                    hard_link_id: "".to_string(),
                    hard_link_counter: 0,
                    extended: HashMap::new(),
                    content_size: 4096,
                    disk_size: 4096,
                    ttl: "".to_string(),
                    symlink_target: "".to_string(),
                    owner: String::new(),
                    generation: self.allocate_generation(),
                };
                let _ = self.create_entry(entry, "");
            }
        }

        Ok(0)
    }

    pub fn create_entry(&self, mut entry: Entry, client_id: &str) -> Result<u64, rocksdb::Error> {
        let inode = self.allocate_inode();
        let generation = self.allocate_generation();

        if let Some(attrs) = &mut entry.attributes {
            attrs.ino = inode;
        }
        entry.generation = generation;

        let key = Self::path_to_key(&entry.directory, &entry.name);
        let path = String::from_utf8_lossy(&key).to_string();
        let mut data = Vec::new();
        entry.encode(&mut data).expect("failed to encode entry");

        self.db.put(&key, &data)?;
        self.db.put(Self::inode_to_key(inode), path.as_bytes())?;

        self.publish_notification(EventType::Create, &path, Some(entry), client_id);

        Ok(inode)
    }

    pub fn update_entry(&self, mut entry: Entry, client_id: &str) -> Result<(), rocksdb::Error> {
        let generation = self.allocate_generation();
        entry.generation = generation;

        let key = Self::path_to_key(&entry.directory, &entry.name);
        let path = String::from_utf8_lossy(&key).to_string();
        let mut data = Vec::new();
        entry.encode(&mut data).expect("failed to encode entry");

        self.db.put(&key, &data)?;

        self.publish_notification(EventType::Update, &path, Some(entry), client_id);

        Ok(())
    }

    pub fn delete_entry(&self, path: &str, client_id: &str) -> Result<bool, rocksdb::Error> {
        let exists = self.db.get(path.as_bytes())?.is_some();
        if exists {
            let entry_bytes = self.db.get(path.as_bytes())?;
            if let Some(bytes) = entry_bytes {
                let decode_result: Result<Entry, _> = prost::Message::decode(bytes.as_ref());
                if let Ok(entry) = decode_result {
                    if let Some(attr) = entry.attributes {
                        self.db.delete(Self::inode_to_key(attr.ino))?;
                        if (attr.mode & 0o40000) != 0 {
                            let mut to_delete = Vec::new();
                            let mut stack = vec![path.to_string()];

                            while let Some(dir_path) = stack.pop() {
                                let prefix = Self::path_prefix(&dir_path);
                                let mut iter = self.db.iterator(IteratorMode::From(
                                    &prefix,
                                    rocksdb::Direction::Forward,
                                ));
                                while let Some(Ok((key, value))) = iter.next() {
                                    if !key.starts_with(&prefix) {
                                        break;
                                    }
                                    let child_path = String::from_utf8_lossy(&key).to_string();
                                    if child_path != dir_path {
                                        to_delete.push(child_path.clone());
                                        let child_decode: Result<Entry, _> =
                                            prost::Message::decode(value.as_ref());
                                        if let Ok(child_entry) = child_decode {
                                            if let Some(child_attr) = child_entry.attributes {
                                                self.db.delete(Self::inode_to_key(child_attr.ino))?;
                                                if (child_attr.mode & 0o40000) != 0 {
                                                    stack.push(child_path);
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            for child_path in to_delete {
                                self.db.delete(child_path.as_bytes())?;
                                self.publish_notification(
                                    EventType::Delete,
                                    &child_path,
                                    None,
                                    client_id,
                                );
                            }
                        }
                    }
                }
            }

            self.db.delete(path.as_bytes())?;
            self.publish_notification(EventType::Delete, path, None, client_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn rename_entry(
        &self,
        old_path: &str,
        _old_directory: &str,
        _old_name: &str,
        new_directory: &str,
        new_name: &str,
        client_id: &str,
    ) -> Result<bool, rocksdb::Error> {
        info!(
            "rename_entry: old_path={}, new_directory={}, new_name={}",
            old_path, new_directory, new_name
        );
        let entry_bytes = self.db.get(old_path.as_bytes())?;
        if let Some(bytes) = entry_bytes {
            let decode_result: Result<Entry, _> = prost::Message::decode(bytes.as_ref());
            if let Ok(mut entry) = decode_result {
                info!(
                    "rename_entry: found entry name={}, directory={}",
                    entry.name, entry.directory
                );
                let generation = self.allocate_generation();
                entry.generation = generation;
                entry.name = new_name.to_string();
                entry.directory = new_directory.to_string();

                let new_key = Self::path_to_key(new_directory, new_name);
                let new_path = String::from_utf8_lossy(&new_key).to_string();
                info!("rename_entry: new_key={}", new_path);
                let mut data = Vec::new();
                entry.encode(&mut data).expect("failed to encode entry");

                self.db.delete(old_path.as_bytes())?;
                self.db.put(&new_key, &data)?;
                if let Some(ref attr) = entry.attributes {
                    self.db.put(Self::inode_to_key(attr.ino), new_path.as_bytes())?;
                }
                info!("rename_entry: db updated successfully");

                self.publish_notification(EventType::Delete, old_path, None, client_id);
                self.publish_notification(EventType::Rename, &new_path, Some(entry), client_id);

                Ok(true)
            } else {
                warn!("rename_entry: failed to decode entry");
                Ok(false)
            }
        } else {
            warn!("rename_entry: old_path not found in db");
            Ok(false)
        }
    }

    pub fn list_entries(&self, directory: &str, limit: u64, last_name: &str) -> Vec<Entry> {
        let prefix = Self::path_prefix(directory);
        let mut entries = Vec::new();

        let mut iter = self
            .db
            .iterator(IteratorMode::From(&prefix, rocksdb::Direction::Forward));
        let mut count = 0u64;

        while let Some(Ok((key, value))) = iter.next() {
            if !key.starts_with(&prefix) {
                break;
            }

            let path = String::from_utf8_lossy(&key);
            let prefix_str = String::from_utf8_lossy(&prefix);
            let entry_name = path.trim_start_matches(&*prefix_str);

            if entry_name.is_empty() {
                continue;
            }

            if !last_name.is_empty() && entry_name <= last_name {
                continue;
            }

            if let Ok(entry) = prost::Message::decode(value.as_ref()) {
                entries.push(entry);
                count += 1;
                if count >= limit {
                    break;
                }
            }
        }

        entries
    }

    pub fn init_root(&self) -> Result<(), rocksdb::Error> {
        if self.get_entry("/").is_none() {
            let root_entry = Entry {
                name: "/".to_string(),
                directory: "/".to_string(),
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
                    atime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                    mtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                    ctime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                    crtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                    perm: 0o755,
                }),
                chunks: vec![],
                hard_link_id: "".to_string(),
                hard_link_counter: 0,
                extended: HashMap::new(),
                content_size: 4096,
                disk_size: 4096,
                ttl: "".to_string(),
                symlink_target: "".to_string(),
                owner: String::new(),
                generation: 1,
            };

            let mut data = Vec::new();
            root_entry
                .encode(&mut data)
                .expect("failed to encode root entry");
            self.db.put(b"/", &data)?;

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
}
