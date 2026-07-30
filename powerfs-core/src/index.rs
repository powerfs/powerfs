use lru::LruCache;
use powerfs_common::types::{NeedleId, NeedleInfo};
use std::collections::HashMap;
use std::sync::RwLock;

pub trait NeedleIndex: Send + Sync {
    fn get(&self, needle_id: &NeedleId) -> Option<NeedleInfo>;
    fn insert(&self, needle_id: NeedleId, info: NeedleInfo);
    fn remove(&self, needle_id: &NeedleId) -> Option<NeedleInfo>;
    fn contains(&self, needle_id: &NeedleId) -> bool;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn iter(&self) -> Vec<(NeedleId, NeedleInfo)>;
}

pub struct MemoryIndex {
    cache: RwLock<HashMap<NeedleId, NeedleInfo>>,
    lru: RwLock<LruCache<NeedleId, NeedleInfo>>,
}

impl MemoryIndex {
    pub fn new(capacity: usize) -> Self {
        MemoryIndex {
            cache: RwLock::new(HashMap::new()),
            lru: RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(capacity).unwrap(),
            )),
        }
    }
}

impl NeedleIndex for MemoryIndex {
    fn get(&self, needle_id: &NeedleId) -> Option<NeedleInfo> {
        let mut lru = self.lru.write().unwrap();
        let result = self.cache.read().unwrap().get(needle_id).cloned();
        if let Some(info) = &result {
            lru.put(needle_id.clone(), info.clone());
        }
        result
    }

    fn insert(&self, needle_id: NeedleId, info: NeedleInfo) {
        self.cache
            .write()
            .unwrap()
            .insert(needle_id.clone(), info.clone());
        self.lru.write().unwrap().put(needle_id, info);
    }

    fn remove(&self, needle_id: &NeedleId) -> Option<NeedleInfo> {
        let info = self.cache.write().unwrap().remove(needle_id);
        self.lru.write().unwrap().pop(needle_id);
        info
    }

    fn contains(&self, needle_id: &NeedleId) -> bool {
        self.cache.read().unwrap().contains_key(needle_id)
    }

    fn len(&self) -> usize {
        self.cache.read().unwrap().len()
    }

    fn iter(&self) -> Vec<(NeedleId, NeedleInfo)> {
        self.cache.read().unwrap().clone().into_iter().collect()
    }
}
