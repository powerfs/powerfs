use crate::proto::Entry;
use log::{info, warn};
use powerfs_orset::{
    ConflictRecord, ConflictResolution, DirEntry, DirORSet, EntryId, FileType, MergePolicy,
    VectorClock,
};
use rocksdb::DB;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

pub enum MetadataEvent {
    Create {
        client_id: String,
        client_id_num: u64,
        entry: Entry,
        parent_ino: u64,
        inode: u64,
    },
    Update {
        client_id: String,
        client_id_num: u64,
        entry: Entry,
        inode: u64,
    },
    Delete {
        client_id: String,
        client_id_num: u64,
        path: String,
        inode: u64,
        name: String,
    },
    Rename {
        client_id: String,
        client_id_num: u64,
        old_path: String,
        new_path: String,
        old_name: String,
        new_name: String,
        inode: u64,
    },
    SetPolicy {
        dir_ino: u64,
        policy: MergePolicy,
    },
}

pub struct MetadataManager {
    db: Arc<DB>,
    #[allow(dead_code)]
    orsets: Arc<RwLock<HashMap<u64, DirORSet>>>,
    #[allow(dead_code)]
    policies: Arc<RwLock<HashMap<u64, MergePolicy>>>,
    #[allow(dead_code)]
    client_counters: Arc<RwLock<HashMap<u64, u64>>>,
    event_tx: mpsc::Sender<MetadataEvent>,
}

impl MetadataManager {
    pub fn new(db: Arc<DB>) -> Self {
        let (event_tx, event_rx) = mpsc::channel(10000);

        let orsets = Arc::new(RwLock::new(HashMap::new()));
        let policies = Arc::new(RwLock::new(HashMap::new()));
        let client_counters = Arc::new(RwLock::new(HashMap::new()));

        let orsets_clone = orsets.clone();
        let policies_clone = policies.clone();
        let client_counters_clone = client_counters.clone();
        let db_clone = db.clone();

        tokio::spawn(Self::conflict_detector(
            db_clone,
            event_rx,
            orsets_clone,
            policies_clone,
            client_counters_clone,
        ));

        Self {
            db,
            orsets,
            policies,
            client_counters,
            event_tx,
        }
    }

    pub async fn conflict_detector(
        db: Arc<DB>,
        mut event_rx: mpsc::Receiver<MetadataEvent>,
        orsets: Arc<RwLock<HashMap<u64, DirORSet>>>,
        policies: Arc<RwLock<HashMap<u64, MergePolicy>>>,
        client_counters: Arc<RwLock<HashMap<u64, u64>>>,
    ) {
        info!("Conflict detector started");

        while let Some(event) = event_rx.recv().await {
            match event {
                MetadataEvent::Create {
                    client_id_num,
                    entry,
                    parent_ino,
                    inode,
                    ..
                } => {
                    info!(
                        "Received Create event: name={}, parent_ino={}, inode={}, client_id_num={}",
                        entry.name, parent_ino, inode, client_id_num
                    );
                    let dir_ino = parent_ino;
                    let entry_id = EntryId::new(entry.name.clone(), client_id_num, 0);
                    let mode = entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0);
                    let file_type = if (mode & 0o40000) != 0 {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };

                    let dir_entry = DirEntry {
                        id: entry_id,
                        inode,
                        file_type,
                        mode,
                        size: entry.attributes.as_ref().map(|a| a.size).unwrap_or(0),
                        mtime: entry.attributes.as_ref().map(|a| a.mtime).unwrap_or(0),
                        atime: entry.attributes.as_ref().map(|a| a.atime).unwrap_or(0),
                        ctime: entry.attributes.as_ref().map(|a| a.ctime).unwrap_or(0),
                        parent_ino: dir_ino,
                        chunks: Vec::new(),
                        symlink_target: None,
                    };

                    let policy = policies
                        .read()
                        .unwrap()
                        .get(&dir_ino)
                        .copied()
                        .unwrap_or_default();

                    let mut counters = client_counters.write().unwrap();
                    let counter = counters.entry(client_id_num).or_insert(0);
                    *counter += 1;
                    let mut vclock = VectorClock::new();
                    vclock.observe(client_id_num, *counter);

                    let mut orsets = orsets.write().unwrap();
                    let orset = orsets.entry(dir_ino).or_insert_with(|| {
                        let mut o = DirORSet::new(dir_ino);
                        o.policy = policy;
                        o
                    });
                    orset.policy = policy;

                    let delta = powerfs_orset::DeltaOp::Add {
                        entry: dir_entry,
                        vclock,
                    };
                    orset.apply_delta(&delta);

                    Self::persist_conflicts(&db, &orset.conflicts());
                }
                MetadataEvent::Update {
                    client_id_num,
                    entry,
                    inode,
                    ..
                } => {
                    let mut counters = client_counters.write().unwrap();
                    let counter = counters.entry(client_id_num).or_insert(0);
                    *counter += 1;
                    let mut vclock = VectorClock::new();
                    vclock.observe(client_id_num, *counter);

                    let mut orsets = orsets.write().unwrap();
                    for (_dir_ino, orset) in orsets.iter_mut() {
                        if orset.get_by_inode(inode).is_some() {
                            let delta = powerfs_orset::DeltaOp::SetAttr {
                                inode,
                                mode: None,
                                size: entry.attributes.as_ref().map(|a| a.size),
                                mtime: entry.attributes.as_ref().map(|a| a.mtime),
                                vclock,
                            };
                            orset.apply_delta(&delta);
                            Self::persist_conflicts(&db, &orset.conflicts());
                            break;
                        }
                    }
                }
                MetadataEvent::Delete {
                    client_id_num,
                    name,
                    ..
                } => {
                    let mut counters = client_counters.write().unwrap();
                    let counter = counters.entry(client_id_num).or_insert(0);
                    *counter += 1;
                    let mut vclock = VectorClock::new();
                    vclock.observe(client_id_num, *counter);

                    let mut orsets = orsets.write().unwrap();
                    for (_, orset) in orsets.iter_mut() {
                        let ids: Vec<_> = orset
                            .get_by_name(&name)
                            .iter()
                            .map(|e| e.id.clone())
                            .collect();
                        if !ids.is_empty() {
                            for id in ids {
                                let delta = powerfs_orset::DeltaOp::Remove {
                                    id,
                                    vclock: vclock.clone(),
                                };
                                orset.apply_delta(&delta);
                            }
                            Self::persist_conflicts(&db, &orset.conflicts());
                        }
                    }
                }
                MetadataEvent::Rename {
                    client_id_num,
                    old_name,
                    new_name,
                    inode,
                    ..
                } => {
                    let mut counters = client_counters.write().unwrap();
                    let counter = counters.entry(client_id_num).or_insert(0);
                    *counter += 1;
                    let mut vclock = VectorClock::new();
                    vclock.observe(client_id_num, *counter);

                    let mut orsets = orsets.write().unwrap();
                    for (dir_ino, orset) in orsets.iter_mut() {
                        let entries = orset.get_by_name(&old_name);
                        if !entries.is_empty() {
                            let new_entry = DirEntry {
                                id: EntryId::new(new_name.clone(), client_id_num, 0),
                                inode,
                                file_type: FileType::RegularFile,
                                mode: 0o644,
                                size: 0,
                                mtime: 0,
                                atime: 0,
                                ctime: 0,
                                parent_ino: *dir_ino,
                                chunks: Vec::new(),
                                symlink_target: None,
                            };
                            let delta = powerfs_orset::DeltaOp::Rename {
                                old_id: EntryId::new(old_name, client_id_num, 0),
                                new_entry,
                                vclock,
                            };
                            orset.apply_delta(&delta);
                            Self::persist_conflicts(&db, &orset.conflicts());
                            break;
                        }
                    }
                }
                MetadataEvent::SetPolicy { dir_ino, policy } => {
                    let mut policies = policies.write().unwrap();
                    policies.insert(dir_ino, policy);

                    let mut orsets = orsets.write().unwrap();
                    if let Some(orset) = orsets.get_mut(&dir_ino) {
                        orset.policy = policy;
                    }
                }
            }
        }
    }

    fn persist_conflicts(db: &DB, conflicts: &[ConflictRecord]) {
        for conflict in conflicts {
            if !conflict.resolved {
                let key_str = format!("conflict:{}", conflict.id);
                let key = key_str.as_bytes();
                if let Ok(data) = serde_json::to_vec(conflict) {
                    if let Err(e) = db.put(key, data) {
                        warn!("Failed to persist conflict: {}", e);
                    }
                }
            }
        }
    }

    pub fn send_event(&self, event: MetadataEvent) {
        if self.event_tx.try_send(event).is_err() {
            warn!("Metadata event channel is full, dropping event");
        }
    }

    pub fn get_conflicts(&self, _dir_ino: u64, unresolved_only: bool) -> Vec<ConflictRecord> {
        let mut conflicts = Vec::new();

        let prefix_str = "conflict:".to_string();
        let prefix = prefix_str.as_bytes();
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            prefix,
            rocksdb::Direction::Forward,
        ));

        for (_key, value) in iter.flatten() {
            if let Ok(conflict) = serde_json::from_slice::<ConflictRecord>(&value) {
                if unresolved_only && conflict.resolved {
                    continue;
                }
                conflicts.push(conflict);
            }
        }

        conflicts
    }

    pub fn resolve_conflict(
        &self,
        _dir_ino: u64,
        conflict_id: &str,
        resolution: ConflictResolution,
    ) {
        let key_str = format!("conflict:{}", conflict_id);
        let key = key_str.as_bytes();

        if let Ok(Some(data)) = self.db.get(key) {
            if let Ok(mut conflict) = serde_json::from_slice::<ConflictRecord>(&data) {
                conflict.resolved = true;
                conflict.resolution = Some(resolution);
                if let Ok(new_data) = serde_json::to_vec(&conflict) {
                    if let Err(e) = self.db.put(key, new_data) {
                        warn!("Failed to update conflict: {}", e);
                    }
                }
            }
        }
    }

    pub fn set_merge_policy(&self, dir_ino: u64, policy: MergePolicy) {
        self.send_event(MetadataEvent::SetPolicy { dir_ino, policy });
    }

    pub fn auto_resolve_conflicts(&self, _dir_ino: u64, _policy: MergePolicy) -> u64 {
        let mut resolved_count = 0;

        let prefix_str = "conflict:".to_string();
        let prefix = prefix_str.as_bytes();
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            prefix,
            rocksdb::Direction::Forward,
        ));

        for (key, value) in iter.flatten() {
            if let Ok(mut conflict) = serde_json::from_slice::<ConflictRecord>(&value) {
                if !conflict.resolved {
                    conflict.resolved = true;
                    conflict.resolution = Some(ConflictResolution::KeepLast);
                    if let Ok(new_data) = serde_json::to_vec(&conflict) {
                        if let Err(e) = self.db.put(key, new_data) {
                            warn!("Failed to auto-resolve conflict: {}", e);
                        } else {
                            resolved_count += 1;
                        }
                    }
                }
            }
        }

        resolved_count
    }
}
