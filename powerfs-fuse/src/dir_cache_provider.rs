use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;

use crossbeam::channel::{self, Receiver, Sender};

use powerfs_orset::{DirCacheProvider, DirORSet};

type DirCache = HashMap<u64, Arc<RwLock<DirORSet>>>;

enum ShardOp {
    Insert {
        dir_ino: u64,
        orset: Arc<RwLock<DirORSet>>,
    },
    Remove {
        dir_ino: u64,
    },
    Get {
        dir_ino: u64,
        reply: Sender<Option<Arc<RwLock<DirORSet>>>>,
    },
}

pub struct EnterpriseDirCache {
    shards: Arc<Vec<Arc<RwLock<DirCache>>>>,
    num_shards: usize,
    senders: Arc<Vec<Sender<ShardOp>>>,
    _threads: Vec<thread::JoinHandle<()>>,
}

impl Clone for EnterpriseDirCache {
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
            num_shards: self.num_shards,
            senders: self.senders.clone(),
            _threads: Vec::new(),
        }
    }
}

impl EnterpriseDirCache {
    pub fn new() -> Self {
        let num_shards = std::thread::available_parallelism()
            .map(|n| n.get() * 2)
            .unwrap_or(8);

        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(Arc::new(RwLock::new(HashMap::new())));
        }

        let shards_arc = Arc::new(shards);
        let mut senders = Vec::with_capacity(num_shards);
        let mut threads = Vec::with_capacity(num_shards);

        for i in 0..num_shards {
            let (sender, receiver) = channel::unbounded();
            let shard = shards_arc[i].clone();
            let thread = thread::spawn(move || {
                Self::shard_worker(receiver, shard);
            });
            senders.push(sender);
            threads.push(thread);
        }

        Self {
            shards: shards_arc,
            num_shards,
            senders: Arc::new(senders),
            _threads: threads,
        }
    }

    fn shard_worker(receiver: Receiver<ShardOp>, shard: Arc<RwLock<DirCache>>) {
        while let Ok(op) = receiver.recv() {
            match op {
                ShardOp::Insert { dir_ino, orset } => {
                    shard.write().unwrap().insert(dir_ino, orset);
                }
                ShardOp::Remove { dir_ino } => {
                    shard.write().unwrap().remove(&dir_ino);
                }
                ShardOp::Get { dir_ino, reply } => {
                    let result = shard.read().unwrap().get(&dir_ino).cloned();
                    let _ = reply.send(result);
                }
            }
        }
    }

    fn _shard_index(&self, dir_ino: u64) -> usize {
        (dir_ino as usize) % self.num_shards
    }

    fn get_shard(&self, dir_ino: u64) -> Arc<RwLock<DirCache>> {
        let idx = self._shard_index(dir_ino);
        self.shards[idx].clone()
    }
}

impl DirCacheProvider for EnterpriseDirCache {
    fn get(&self, dir_ino: u64) -> Option<Arc<RwLock<DirORSet>>> {
        let idx = self._shard_index(dir_ino);
        let (reply_sender, reply_receiver) = channel::bounded::<Option<Arc<RwLock<DirORSet>>>>(1);
        let _ = self.senders[idx].send(ShardOp::Get {
            dir_ino,
            reply: reply_sender,
        });
        reply_receiver.recv().unwrap_or(None)
    }

    fn insert(&self, dir_ino: u64, orset: Arc<RwLock<DirORSet>>) {
        let idx = self._shard_index(dir_ino);
        let _ = self.senders[idx].send(ShardOp::Insert { dir_ino, orset });
        let (read_sender, read_receiver) = channel::bounded::<Option<Arc<RwLock<DirORSet>>>>(1);
        let _ = self.senders[idx].send(ShardOp::Get {
            dir_ino,
            reply: read_sender,
        });
        let _ = read_receiver.recv();
    }

    fn remove(&self, dir_ino: u64) -> Option<Arc<RwLock<DirORSet>>> {
        let idx = self._shard_index(dir_ino);
        let _ = self.senders[idx].send(ShardOp::Remove { dir_ino });
        let (read_sender, read_receiver) = channel::bounded::<Option<Arc<RwLock<DirORSet>>>>(1);
        let _ = self.senders[idx].send(ShardOp::Get {
            dir_ino,
            reply: read_sender,
        });
        read_receiver.recv().unwrap_or(None)
    }

    fn ensure_dir_cache(&self, dir_ino: u64) -> Arc<RwLock<DirORSet>> {
        let shard = self.get_shard(dir_ino);
        {
            let cache = shard.read().unwrap();
            if let Some(orset_arc) = cache.get(&dir_ino) {
                return orset_arc.clone();
            }
        }
        let mut cache = shard.write().unwrap();
        cache
            .entry(dir_ino)
            .or_insert_with(|| Arc::new(RwLock::new(DirORSet::new(dir_ino))))
            .clone()
    }

    fn try_read(&self, dir_ino: u64) -> Result<Option<Arc<RwLock<DirORSet>>>, ()> {
        let shard = self.get_shard(dir_ino);
        let guard = shard.try_read().map_err(|_| ())?;
        Ok(guard.get(&dir_ino).cloned())
    }

    fn shard_index(&self, dir_ino: u64) -> usize {
        self._shard_index(dir_ino)
    }
}

impl Default for EnterpriseDirCache {
    fn default() -> Self {
        Self::new()
    }
}
