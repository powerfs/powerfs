use crate::proto::powerfs::metadata_notification::EventType;
use crate::proto::{Entry, MetadataNotification};
use prost::Message;
use rocksdb::{IteratorMode, Options, DB};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct DirectoryTree {
    db: DB,
    inode_counter: std::sync::atomic::AtomicU64,
    notifier: Arc<broadcast::Sender<MetadataNotification>>,
    subscribers: std::sync::RwLock<HashSet<String>>,
}

impl DirectoryTree {
    pub fn new(path: &Path) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = DB::open(&opts, path)?;

        let inode_counter = Self::load_inode_counter(&db);
        let (notifier, _) = broadcast::channel(100);

        Ok(DirectoryTree {
            db,
            inode_counter,
            notifier: Arc::new(notifier),
            subscribers: std::sync::RwLock::new(HashSet::new()),
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
                };
                let _ = self.create_entry(entry);
            }
        }

        Ok(0)
    }

    pub fn create_entry(&self, mut entry: Entry) -> Result<u64, rocksdb::Error> {
        let inode = self.allocate_inode();

        if let Some(attrs) = &mut entry.attributes {
            attrs.ino = inode;
        }

        let key = Self::path_to_key(&entry.directory, &entry.name);
        let path = String::from_utf8_lossy(&key).to_string();
        let mut data = Vec::new();
        entry.encode(&mut data).expect("failed to encode entry");

        self.db.put(&key, &data)?;

        self.publish_notification(EventType::Create, &path, Some(entry));

        Ok(inode)
    }

    pub fn update_entry(&self, entry: &Entry) -> Result<(), rocksdb::Error> {
        let key = Self::path_to_key(&entry.directory, &entry.name);
        let path = String::from_utf8_lossy(&key).to_string();
        let mut data = Vec::new();
        entry.encode(&mut data).expect("failed to encode entry");

        self.db.put(&key, &data)?;

        self.publish_notification(EventType::Update, &path, Some(entry.clone()));

        Ok(())
    }

    pub fn delete_entry(&self, path: &str) -> Result<bool, rocksdb::Error> {
        let exists = self.db.get(path.as_bytes())?.is_some();
        if exists {
            self.db.delete(path.as_bytes())?;

            self.publish_notification(EventType::Delete, path, None);

            Ok(true)
        } else {
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

    fn publish_notification(&self, event_type: EventType, path: &str, entry: Option<Entry>) {
        let notification = MetadataNotification {
            event_type: event_type as i32,
            path: path.to_string(),
            entry,
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
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

    pub fn remove_subscriber(&self, path_prefix: &str) {
        let mut subscribers = self.subscribers.write().unwrap();
        subscribers.remove(path_prefix);
    }
}
