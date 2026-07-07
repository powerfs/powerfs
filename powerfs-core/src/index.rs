use lru::LruCache;
use powerfs_common::{
    error::{PowerFsError, Result},
    types::{NeedleId, NeedleInfo},
};
use serde_json;
use sled::Db;
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

pub struct PersistentIndex {
    db: Db,
    lru: RwLock<LruCache<NeedleId, NeedleInfo>>,
}

#[allow(clippy::result_large_err)]
impl PersistentIndex {
    pub fn new(path: &str) -> Result<Self> {
        let db =
            sled::open(path).map_err(|e| PowerFsError::Internal(format!("sled error: {}", e)))?;
        Ok(PersistentIndex {
            db,
            lru: RwLock::new(LruCache::new(std::num::NonZeroUsize::new(10000).unwrap())),
        })
    }

    fn key_from_id(needle_id: &NeedleId) -> Vec<u8> {
        needle_id.0.to_be_bytes().to_vec()
    }
}

impl NeedleIndex for PersistentIndex {
    fn get(&self, needle_id: &NeedleId) -> Option<NeedleInfo> {
        let mut lru = self.lru.write().unwrap();

        if let Some(info) = lru.get(needle_id) {
            return Some(info.clone());
        }

        let key = Self::key_from_id(needle_id);
        if let Ok(Some(data)) = self.db.get(&key) {
            match serde_json::from_slice::<NeedleInfo>(&data) {
                Ok(info) => {
                    lru.put(needle_id.clone(), info.clone());
                    Some(info)
                }
                Err(_) => None,
            }
        } else {
            None
        }
    }

    fn insert(&self, needle_id: NeedleId, info: NeedleInfo) {
        let key = Self::key_from_id(&needle_id);
        let data = serde_json::to_vec(&info).unwrap();
        let _ = self.db.insert(key, data);
        self.lru.write().unwrap().put(needle_id, info);
    }

    fn remove(&self, needle_id: &NeedleId) -> Option<NeedleInfo> {
        let key = Self::key_from_id(needle_id);
        if let Ok(Some(data)) = self.db.remove(&key) {
            match serde_json::from_slice::<NeedleInfo>(&data) {
                Ok(info) => {
                    self.lru.write().unwrap().pop(needle_id);
                    Some(info)
                }
                Err(_) => None,
            }
        } else {
            None
        }
    }

    fn contains(&self, needle_id: &NeedleId) -> bool {
        let key = Self::key_from_id(needle_id);
        self.db.contains_key(&key).unwrap_or(false)
    }

    fn len(&self) -> usize {
        self.db.len()
    }

    fn iter(&self) -> Vec<(NeedleId, NeedleInfo)> {
        let mut result = Vec::new();
        for (key, data) in self.db.iter().flatten() {
            if let Ok(info) = serde_json::from_slice::<NeedleInfo>(&data) {
                let needle_id = NeedleId(u64::from_be_bytes(
                    key.as_ref().try_into().unwrap_or([0; 8]),
                ));
                result.push((needle_id, info));
            }
        }
        result
    }
}
