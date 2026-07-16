use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use powerfs_orset::{DirCacheProvider, DirORSet};

pub struct CommunityDirCache {
    cache: Arc<RwLock<HashMap<u64, Arc<RwLock<DirORSet>>>>>,
}

impl CommunityDirCache {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for CommunityDirCache {
    fn default() -> Self {
        Self::new()
    }
}

impl DirCacheProvider for CommunityDirCache {
    fn get(&self, dir_ino: u64) -> Option<Arc<RwLock<DirORSet>>> {
        let cache = self.cache.read().unwrap();
        cache.get(&dir_ino).cloned()
    }

    fn insert(&self, dir_ino: u64, orset: Arc<RwLock<DirORSet>>) {
        let mut cache = self.cache.write().unwrap();
        cache.insert(dir_ino, orset);
    }

    fn remove(&self, dir_ino: u64) -> Option<Arc<RwLock<DirORSet>>> {
        let mut cache = self.cache.write().unwrap();
        cache.remove(&dir_ino)
    }

    fn ensure_dir_cache(&self, dir_ino: u64) -> Arc<RwLock<DirORSet>> {
        let mut cache = self.cache.write().unwrap();
        cache
            .entry(dir_ino)
            .or_insert_with(|| Arc::new(RwLock::new(DirORSet::new(dir_ino))))
            .clone()
    }

    fn try_read(&self, dir_ino: u64) -> Result<Option<Arc<RwLock<DirORSet>>>, ()> {
        let cache = self.cache.try_read().map_err(|_| ())?;
        Ok(cache.get(&dir_ino).cloned())
    }

    fn shard_index(&self, _dir_ino: u64) -> usize {
        0
    }
}
