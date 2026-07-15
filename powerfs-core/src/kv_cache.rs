use base64::engine::{general_purpose, Engine as _};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;

use crate::crdt::or_set::ReplicatedORSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum KVDtype {
    FP32,
    FP16,
    INT8,
    BF16,
    FP8,
}

impl KVDtype {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "fp32" => Some(Self::FP32),
            "fp16" => Some(Self::FP16),
            "bf16" => Some(Self::BF16),
            "fp8" => Some(Self::FP8),
            "int8" => Some(Self::INT8),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FP32 => "fp32",
            Self::FP16 => "fp16",
            Self::BF16 => "bf16",
            Self::FP8 => "fp8",
            Self::INT8 => "int8",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum PinMode {
    #[default]
    None,
    Soft,
    Hard,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KVNamespace {
    pub id: String,
    pub name: String,
    pub owner_id: String,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KVStoredValue {
    pub data: Vec<u8>,
    pub owner_id: String,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KVCRDTStats {
    pub key_count: usize,
    pub counter: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KVBlockMeta {
    pub block_id: u64,
    pub session_id: String,
    pub namespace_id: String,
    pub owner_id: String,
    pub layer_id: u32,
    pub num_tokens: u32,
    pub dtype: KVDtype,
    pub head_dim: u32,
    pub num_heads: u32,
    pub size_bytes: u64,
    pub created_at: u64,
    pub last_accessed: u64,
    pub ttl: Option<u64>,
    pub fid: String,
    pub block_index: u32,
    pub pin_mode: PinMode,
}

pub struct KVBlock {
    pub meta: KVBlockMeta,
    pub data: Vec<u8>,
}

pub type BatchPutRequest = (String, u32, u32, Vec<u8>, String, u32);

impl std::fmt::Debug for KVBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KVBlock")
            .field("meta", &self.meta)
            .field("data_len", &self.data.len())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct KVSession {
    pub session_id: String,
    pub namespace_id: String,
    pub owner_id: String,
    pub model_name: String,
    pub num_layers: u32,
    pub num_heads: u32,
    pub head_dim: u32,
    pub dtype: KVDtype,
    pub created_at: u64,
    pub last_accessed: u64,
    pub block_ids: Vec<u64>,
    pub ttl: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct KVCacheStats {
    pub total_blocks: u64,
    pub total_sessions: u64,
    pub used_memory_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

pub struct MemoryPool {
    block_size: usize,
    free_blocks: Mutex<Vec<Vec<u8>>>,
}

impl MemoryPool {
    pub fn new(block_size: usize, initial_blocks: usize) -> Self {
        let mut free_blocks = Vec::with_capacity(initial_blocks);
        for _ in 0..initial_blocks {
            free_blocks.push(vec![0u8; block_size]);
        }
        Self {
            block_size,
            free_blocks: Mutex::new(free_blocks),
        }
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn allocate(&self) -> Vec<u8> {
        let mut free = self.free_blocks.lock().unwrap();
        if let Some(buf) = free.pop() {
            buf
        } else {
            vec![0u8; self.block_size]
        }
    }

    pub fn deallocate(&self, buf: Vec<u8>) {
        let mut free = self.free_blocks.lock().unwrap();
        free.push(buf);
    }
}

unsafe impl Send for MemoryPool {}
unsafe impl Sync for MemoryPool {}

pub struct KVCacheEngine {
    max_memory_bytes: u64,
    block_size: usize,
    memory_pool: Arc<MemoryPool>,
    blocks: RwLock<HashMap<u64, KVBlock>>,
    sessions: RwLock<HashMap<String, KVSession>>,
    namespaces: RwLock<HashMap<String, KVNamespace>>,
    stats: Mutex<KVCacheStats>,
    next_block_id: AtomicU64,
    block_id_map: RwLock<HashMap<u64, String>>,
    db: Option<rocksdb::DB>,
    kv_store: ReplicatedORSet<String>,
    kv_value_cache: RwLock<HashMap<String, Vec<u8>>>,
    replica_id: String,
}

impl KVCacheEngine {
    pub fn new(max_memory_bytes: u64, block_size: usize) -> Self {
        let initial_blocks = (max_memory_bytes as usize / block_size / 10).max(1);
        let memory_pool = Arc::new(MemoryPool::new(block_size, initial_blocks));
        let replica_id = format!(
            "kv_engine_{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        Self {
            max_memory_bytes,
            block_size,
            memory_pool,
            blocks: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            namespaces: RwLock::new(HashMap::new()),
            stats: Mutex::new(KVCacheStats::default()),
            next_block_id: AtomicU64::new(1),
            block_id_map: RwLock::new(HashMap::new()),
            db: None,
            kv_store: ReplicatedORSet::new(&replica_id),
            kv_value_cache: RwLock::new(HashMap::new()),
            replica_id,
        }
    }

    pub fn new_with_db(
        max_memory_bytes: u64,
        block_size: usize,
        db_path: &str,
    ) -> Result<Self, String> {
        let initial_blocks = (max_memory_bytes as usize / block_size / 10).max(1);
        let memory_pool = Arc::new(MemoryPool::new(block_size, initial_blocks));

        let db = rocksdb::DB::open_default(db_path)
            .map_err(|e| format!("Failed to open rocksdb: {}", e))?;

        let replica_id = format!(
            "kv_engine_{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );

        let mut engine = Self {
            max_memory_bytes,
            block_size,
            memory_pool,
            blocks: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            namespaces: RwLock::new(HashMap::new()),
            stats: Mutex::new(KVCacheStats::default()),
            next_block_id: AtomicU64::new(1),
            block_id_map: RwLock::new(HashMap::new()),
            db: Some(db),
            kv_store: ReplicatedORSet::new(&replica_id),
            kv_value_cache: RwLock::new(HashMap::new()),
            replica_id,
        };

        engine.load_from_db()?;
        Ok(engine)
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn max_memory_bytes(&self) -> u64 {
        self.max_memory_bytes
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_session(
        &self,
        session_id: &str,
        namespace_id: &str,
        owner_id: &str,
        model_name: &str,
        num_layers: u32,
        num_heads: u32,
        head_dim: u32,
        dtype: KVDtype,
        ttl_seconds: u64,
    ) -> Result<(), String> {
        let mut sessions = self.sessions.write().unwrap();
        if sessions.contains_key(session_id) {
            return Err(format!("session {} already exists", session_id));
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let ttl = if ttl_seconds > 0 {
            Some(ttl_seconds)
        } else {
            None
        };

        let session = KVSession {
            session_id: session_id.to_string(),
            namespace_id: namespace_id.to_string(),
            owner_id: owner_id.to_string(),
            model_name: model_name.to_string(),
            num_layers,
            num_heads,
            head_dim,
            dtype,
            created_at: now,
            last_accessed: now,
            block_ids: Vec::new(),
            ttl,
        };

        sessions.insert(session_id.to_string(), session);

        let mut stats = self.stats.lock().unwrap();
        stats.total_sessions += 1;

        Ok(())
    }

    pub fn delete_session(&self, session_id: &str) -> Result<(), String> {
        let mut sessions = self.sessions.write().unwrap();
        let session = sessions
            .remove(session_id)
            .ok_or_else(|| format!("session {} not found", session_id))?;

        let mut blocks = self.blocks.write().unwrap();
        let mut block_id_map = self.block_id_map.write().unwrap();
        let mut stats = self.stats.lock().unwrap();

        for block_id in &session.block_ids {
            if let Some(block) = blocks.remove(block_id) {
                stats.used_memory_bytes = stats
                    .used_memory_bytes
                    .saturating_sub(block.meta.size_bytes);
                stats.total_blocks = stats.total_blocks.saturating_sub(1);
                self.memory_pool.deallocate(block.data);
            }
            block_id_map.remove(block_id);
        }

        stats.total_sessions = stats.total_sessions.saturating_sub(1);

        Ok(())
    }

    pub fn get_session(&self, session_id: &str) -> Option<KVSession> {
        let sessions = self.sessions.read().unwrap();
        sessions.get(session_id).cloned()
    }

    pub fn list_sessions(&self, limit: u32, prefix: &str) -> (Vec<String>, u64) {
        let sessions = self.sessions.read().unwrap();
        let mut ids: Vec<String> = sessions
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        ids.sort();
        let total = ids.len() as u64;
        ids.truncate(limit as usize);
        (ids, total)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_block(
        &self,
        session_id: &str,
        layer_id: u32,
        num_tokens: u32,
        data: &[u8],
        fid: &str,
        block_index: u32,
        pin_mode: PinMode,
    ) -> Result<u64, String> {
        {
            let sessions = self.sessions.read().unwrap();
            if !sessions.contains_key(session_id) {
                return Err(format!("session {} not found", session_id));
            }
        }

        let size_bytes = data.len() as u64;

        self.ensure_memory(size_bytes)?;

        let block_id = self.next_block_id.fetch_add(1, Ordering::SeqCst);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut buf = self.memory_pool.allocate();
        let copy_len = data.len().min(buf.len());
        buf[..copy_len].copy_from_slice(&data[..copy_len]);

        let session = {
            let sessions = self.sessions.read().unwrap();
            sessions
                .get(session_id)
                .ok_or_else(|| format!("session {} not found", session_id))?
                .clone()
        };

        let meta = KVBlockMeta {
            block_id,
            session_id: session_id.to_string(),
            namespace_id: session.namespace_id.clone(),
            owner_id: session.owner_id.clone(),
            layer_id,
            num_tokens,
            dtype: session.dtype,
            head_dim: session.head_dim,
            num_heads: session.num_heads,
            size_bytes,
            created_at: now,
            last_accessed: now,
            ttl: session.ttl,
            fid: fid.to_string(),
            block_index,
            pin_mode,
        };

        let block = KVBlock { meta, data: buf };

        self.save_block_to_db(block_id, &block)?;

        let mut blocks = self.blocks.write().unwrap();
        blocks.insert(block_id, block);

        let mut sessions = self.sessions.write().unwrap();
        if let Some(sess) = sessions.get_mut(session_id) {
            sess.block_ids.push(block_id);
            sess.last_accessed = now;
        }

        let mut stats = self.stats.lock().unwrap();
        stats.total_blocks += 1;
        stats.used_memory_bytes += size_bytes;

        let mut block_id_map = self.block_id_map.write().unwrap();
        block_id_map.insert(block_id, fid.to_string());

        Ok(block_id)
    }

    pub fn get_fid_by_block_id(&self, block_id: u64) -> Option<String> {
        let block_id_map = self.block_id_map.read().unwrap();
        block_id_map.get(&block_id).cloned()
    }

    pub fn set_fid_by_block_id(&self, block_id: u64, fid: &str) {
        let mut block_id_map = self.block_id_map.write().unwrap();
        block_id_map.insert(block_id, fid.to_string());
    }

    pub fn remove_block_id_mapping(&self, block_id: u64) {
        let mut block_id_map = self.block_id_map.write().unwrap();
        block_id_map.remove(&block_id);
    }

    pub fn restore_block_id_mapping(&self, block_id: u64, fid: &str) {
        let mut block_id_map = self.block_id_map.write().unwrap();
        block_id_map.insert(block_id, fid.to_string());
    }

    pub fn get_block_meta(&self, block_id: u64) -> Option<KVBlockMeta> {
        let blocks = self.blocks.read().unwrap();
        blocks.get(&block_id).map(|b| b.meta.clone())
    }

    pub fn get_session_by_block_id(&self, block_id: u64) -> Option<KVSession> {
        let blocks = self.blocks.read().unwrap();
        if let Some(block) = blocks.get(&block_id) {
            let sessions = self.sessions.read().unwrap();
            return sessions.get(&block.meta.session_id).cloned();
        }
        drop(blocks);

        let block_id_map = self.block_id_map.read().unwrap();
        if let Some(fid_str) = block_id_map.get(&block_id) {
            let sessions = self.sessions.read().unwrap();
            for (_, sess) in sessions.iter() {
                let expected_fid_prefix = format!("{},", sess.session_id.len() % 1000 + 1);
                if fid_str.starts_with(&expected_fid_prefix) {
                    return Some(sess.clone());
                }
            }
        }
        None
    }

    pub fn get_block(&self, block_id: u64) -> Option<KVBlockMeta> {
        let mut blocks = self.blocks.write().unwrap();
        let block = blocks.get_mut(&block_id)?;
        block.meta.last_accessed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let meta = block.meta.clone();

        let mut stats = self.stats.lock().unwrap();
        stats.hits += 1;

        Some(meta)
    }

    pub fn get_block_data(&self, block_id: u64) -> Option<(KVBlockMeta, Vec<u8>)> {
        let mut blocks = self.blocks.write().unwrap();
        let block = blocks.get_mut(&block_id)?;
        block.meta.last_accessed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let meta = block.meta.clone();
        let data = block.data[..meta.size_bytes as usize].to_vec();

        let mut stats = self.stats.lock().unwrap();
        stats.hits += 1;

        Some((meta, data))
    }

    pub fn get_session_blocks(&self, session_id: &str) -> Vec<KVBlockMeta> {
        let sessions = self.sessions.read().unwrap();
        let session = match sessions.get(session_id) {
            Some(s) => s,
            None => return Vec::new(),
        };

        let blocks = self.blocks.read().unwrap();
        let mut result = Vec::new();
        for bid in &session.block_ids {
            if let Some(block) = blocks.get(bid) {
                result.push(block.meta.clone());
            }
        }
        result
    }

    pub fn stats(&self) -> KVCacheStats {
        self.stats.lock().unwrap().clone()
    }

    fn ensure_memory(&self, needed_bytes: u64) -> Result<(), String> {
        let used = self.stats.lock().unwrap().used_memory_bytes;
        if used + needed_bytes <= self.max_memory_bytes {
            return Ok(());
        }

        self.evict_lru(needed_bytes)
    }

    pub fn evict_lru(&self, needed_bytes: u64) -> Result<(), String> {
        let mut blocks = self.blocks.write().unwrap();
        let mut sessions = self.sessions.write().unwrap();
        let mut block_id_map = self.block_id_map.write().unwrap();
        let mut stats = self.stats.lock().unwrap();

        let mut evicted_bytes: u64 = 0;

        while evicted_bytes < needed_bytes && !blocks.is_empty() {
            let mut oldest_id: Option<u64> = None;
            let mut oldest_time = u64::MAX;

            for (id, block) in blocks.iter() {
                if block.meta.last_accessed < oldest_time {
                    oldest_time = block.meta.last_accessed;
                    oldest_id = Some(*id);
                }
            }

            let oldest_id = match oldest_id {
                Some(id) => id,
                None => break,
            };

            let block = match blocks.remove(&oldest_id) {
                Some(b) => b,
                None => break,
            };

            let sid = block.meta.session_id.clone();
            evicted_bytes += block.meta.size_bytes;
            stats.used_memory_bytes = stats
                .used_memory_bytes
                .saturating_sub(block.meta.size_bytes);
            stats.total_blocks = stats.total_blocks.saturating_sub(1);
            stats.evictions += 1;

            self.memory_pool.deallocate(block.data);
            block_id_map.remove(&oldest_id);

            // Remove from session
            if let Some(sess) = sessions.get_mut(&sid) {
                sess.block_ids.retain(|&id| id != oldest_id);
            }
        }

        if evicted_bytes >= needed_bytes {
            Ok(())
        } else {
            Err(format!(
                "not enough memory: needed {} bytes, evicted {} bytes",
                needed_bytes, evicted_bytes
            ))
        }
    }

    pub fn cleanup_expired(&self) -> usize {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut expired_sessions = Vec::new();

        {
            let sessions = self.sessions.read().unwrap();
            for (id, sess) in sessions.iter() {
                if let Some(ttl) = sess.ttl {
                    if now.saturating_sub(sess.last_accessed) >= ttl {
                        expired_sessions.push(id.clone());
                    }
                }
            }
        }

        let mut count = 0;
        for sid in expired_sessions {
            if self.delete_session(&sid).is_ok() {
                count += 1;
            }
        }

        let mut expired_blocks = Vec::new();
        {
            let blocks = self.blocks.read().unwrap();
            for (id, block) in blocks.iter() {
                if let Some(ttl) = block.meta.ttl {
                    if now.saturating_sub(block.meta.last_accessed) > ttl {
                        expired_blocks.push(*id);
                    }
                }
            }
        }

        let mut blocks = self.blocks.write().unwrap();
        let mut sessions = self.sessions.write().unwrap();
        let mut block_id_map = self.block_id_map.write().unwrap();
        let mut stats = self.stats.lock().unwrap();

        for bid in expired_blocks {
            if let Some(block) = blocks.remove(&bid) {
                count += 1;
                stats.used_memory_bytes = stats
                    .used_memory_bytes
                    .saturating_sub(block.meta.size_bytes);
                stats.total_blocks = stats.total_blocks.saturating_sub(1);
                stats.evictions += 1;
                self.memory_pool.deallocate(block.data);
                block_id_map.remove(&bid);

                if let Some(sess) = sessions.get_mut(&block.meta.session_id) {
                    sess.block_ids.retain(|&id| id != bid);
                }
            }
        }

        count
    }

    pub fn batch_put(&self, requests: &[BatchPutRequest]) -> Vec<Result<u64, String>> {
        let mut results = Vec::with_capacity(requests.len());
        for (session_id, layer_id, num_tokens, data, fid, block_index) in requests {
            results.push(self.put_block(
                session_id,
                *layer_id,
                *num_tokens,
                data,
                fid,
                *block_index,
                PinMode::None,
            ));
        }
        results
    }

    pub fn batch_get(&self, block_ids: &[u64]) -> Vec<Option<(KVBlockMeta, Vec<u8>)>> {
        let mut results = Vec::with_capacity(block_ids.len());
        for bid in block_ids {
            results.push(self.get_block_data(*bid));
        }
        results
    }

    pub fn create_namespace(
        &self,
        namespace_id: &str,
        name: &str,
        owner_id: &str,
    ) -> Result<(), String> {
        let mut namespaces = self.namespaces.write().unwrap();
        if namespaces.contains_key(namespace_id) {
            return Err(format!("namespace {} already exists", namespace_id));
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let namespace = KVNamespace {
            id: namespace_id.to_string(),
            name: name.to_string(),
            owner_id: owner_id.to_string(),
            created_at: now,
            updated_at: now,
        };

        namespaces.insert(namespace_id.to_string(), namespace);
        self.save_namespace_to_db(namespace_id, &namespaces[namespace_id])?;

        Ok(())
    }

    pub fn get_namespace(&self, namespace_id: &str) -> Option<KVNamespace> {
        let namespaces = self.namespaces.read().unwrap();
        namespaces.get(namespace_id).cloned()
    }

    pub fn list_namespaces(&self, owner_id: &str) -> Vec<KVNamespace> {
        let namespaces = self.namespaces.read().unwrap();
        namespaces
            .values()
            .filter(|ns| ns.owner_id == owner_id)
            .cloned()
            .collect()
    }

    pub fn delete_namespace(&self, namespace_id: &str, owner_id: &str) -> Result<(), String> {
        let mut namespaces = self.namespaces.write().unwrap();
        let namespace = namespaces
            .get(namespace_id)
            .ok_or_else(|| format!("namespace {} not found", namespace_id))?;

        if namespace.owner_id != owner_id {
            return Err("permission denied".to_string());
        }

        namespaces.remove(namespace_id);
        self.delete_namespace_from_db(namespace_id)?;

        Ok(())
    }

    pub fn list_user_sessions(&self, owner_id: &str) -> Vec<KVSession> {
        let sessions = self.sessions.read().unwrap();
        sessions
            .values()
            .filter(|s| s.owner_id == owner_id)
            .cloned()
            .collect()
    }

    pub fn list_user_blocks(&self, owner_id: &str) -> Vec<KVBlockMeta> {
        let blocks = self.blocks.read().unwrap();
        blocks
            .values()
            .filter(|b| b.meta.owner_id == owner_id)
            .map(|b| b.meta.clone())
            .collect()
    }

    pub fn kv_put(
        &self,
        namespace_id: &str,
        key: &str,
        value: &[u8],
        owner_id: &str,
    ) -> Result<(), String> {
        let namespace = {
            let namespaces = self.namespaces.read().unwrap();
            namespaces.get(namespace_id).cloned()
        };

        if namespace.is_none() {
            return Err(format!("namespace {} not found", namespace_id));
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let kv_key = format!("kv:{}:{}", namespace_id, key);
        let kv_value = KVStoredValue {
            data: value.to_vec(),
            owner_id: owner_id.to_string(),
            created_at: now,
            updated_at: now,
        };

        self.kv_store.insert(kv_key.clone());
        let value_json = serde_json::to_string(&kv_value)
            .map_err(|e| format!("Failed to serialize value: {}", e))?;
        self.kv_value_cache
            .write()
            .unwrap()
            .insert(kv_key.clone(), value_json.as_bytes().to_vec());

        if let Some(ref db) = self.db {
            db.put(kv_key, value_json)
                .map_err(|e| format!("Failed to put key: {}", e))?;
        }

        Ok(())
    }

    pub fn kv_get(&self, namespace_id: &str, key: &str) -> Result<Option<KVStoredValue>, String> {
        let namespace = {
            let namespaces = self.namespaces.read().unwrap();
            namespaces.get(namespace_id).cloned()
        };

        if namespace.is_none() {
            return Err(format!("namespace {} not found", namespace_id));
        }

        let kv_key = format!("kv:{}:{}", namespace_id, key);

        if !self.kv_store.contains(&kv_key) {
            return Ok(None);
        }

        if let Some(value) = self.kv_value_cache.read().unwrap().get(&kv_key) {
            let value_str = String::from_utf8_lossy(value);
            let kv_value = serde_json::from_str(&value_str)
                .map_err(|e| format!("Failed to deserialize value: {}", e))?;
            return Ok(Some(kv_value));
        }

        if let Some(ref db) = self.db {
            if let Ok(Some(value)) = db.get(&kv_key) {
                let value_str = String::from_utf8_lossy(&value);
                let kv_value = serde_json::from_str(&value_str)
                    .map_err(|e| format!("Failed to deserialize value: {}", e))?;
                self.kv_value_cache
                    .write()
                    .unwrap()
                    .insert(kv_key, value.to_vec());
                return Ok(Some(kv_value));
            }
        }

        Ok(None)
    }

    pub fn kv_delete(&self, namespace_id: &str, key: &str) -> Result<bool, String> {
        let namespace = {
            let namespaces = self.namespaces.read().unwrap();
            namespaces.get(namespace_id).cloned()
        };

        if namespace.is_none() {
            return Err(format!("namespace {} not found", namespace_id));
        }

        let kv_key = format!("kv:{}:{}", namespace_id, key);

        if !self.kv_store.contains(&kv_key) {
            return Ok(false);
        }

        self.kv_store.remove(&kv_key);
        self.kv_value_cache.write().unwrap().remove(&kv_key);

        if let Some(ref db) = self.db {
            match db.delete(&kv_key) {
                Ok(()) => Ok(true),
                Err(e) => Err(format!("Failed to delete key: {}", e)),
            }
        } else {
            Ok(true)
        }
    }

    pub fn kv_exists(&self, namespace_id: &str, key: &str) -> Result<bool, String> {
        let namespace = {
            let namespaces = self.namespaces.read().unwrap();
            namespaces.get(namespace_id).cloned()
        };

        if namespace.is_none() {
            return Err(format!("namespace {} not found", namespace_id));
        }

        let kv_key = format!("kv:{}:{}", namespace_id, key);

        if self.kv_store.contains(&kv_key) {
            return Ok(true);
        }

        if let Some(ref db) = self.db {
            match db.get(&kv_key) {
                Ok(Some(_)) => Ok(true),
                Ok(None) => Ok(false),
                Err(e) => Err(format!("Failed to check existence: {}", e)),
            }
        } else {
            Ok(false)
        }
    }

    pub fn kv_list(&self, namespace_id: &str, prefix: Option<&str>) -> Result<Vec<String>, String> {
        let namespace = {
            let namespaces = self.namespaces.read().unwrap();
            namespaces.get(namespace_id).cloned()
        };

        if namespace.is_none() {
            return Err(format!("namespace {} not found", namespace_id));
        }

        let full_prefix = if let Some(p) = prefix {
            format!("kv:{}:{}", namespace_id, p)
        } else {
            format!("kv:{}:", namespace_id)
        };

        let mut keys = Vec::new();

        let orset_keys = self.kv_store.values();
        for key in orset_keys {
            if key.starts_with(&full_prefix) {
                if let Some(kv_key) = key.strip_prefix(&format!("kv:{}:", namespace_id)) {
                    keys.push(kv_key.to_string());
                }
            }
        }

        if keys.is_empty() {
            if let Some(ref db) = self.db {
                let prefix_bytes = full_prefix.as_bytes();
                for result in db.iterator(rocksdb::IteratorMode::From(
                    prefix_bytes,
                    rocksdb::Direction::Forward,
                )) {
                    match result {
                        Ok((key, _)) => {
                            let key_str = String::from_utf8_lossy(&key);
                            if key_str.starts_with(&full_prefix) {
                                let kv_key = key_str
                                    .strip_prefix(&format!("kv:{}:", namespace_id))
                                    .unwrap_or("");
                                keys.push(kv_key.to_string());
                            } else {
                                break;
                            }
                        }
                        Err(e) => return Err(format!("Failed to iterate: {}", e)),
                    }
                }
            }
        }

        Ok(keys)
    }

    pub fn kv_remove_by_regex(&self, namespace_id: &str, pattern: &str) -> Result<usize, String> {
        let namespace = {
            let namespaces = self.namespaces.read().unwrap();
            namespaces.get(namespace_id).cloned()
        };

        if namespace.is_none() {
            return Err(format!("namespace {} not found", namespace_id));
        }

        let prefix = format!("kv:{}:", namespace_id);
        let re = regex::Regex::new(pattern).map_err(|e| format!("Invalid regex: {}", e))?;

        let mut count = 0;
        let mut to_delete = Vec::new();

        if let Some(ref db) = self.db {
            let prefix_bytes = prefix.as_bytes();

            for result in db.iterator(rocksdb::IteratorMode::From(
                prefix_bytes,
                rocksdb::Direction::Forward,
            )) {
                match result {
                    Ok((key, _)) => {
                        let key_str = String::from_utf8_lossy(&key);
                        if key_str.starts_with(&prefix) {
                            let kv_key = key_str.strip_prefix(&prefix).unwrap_or("");
                            if re.is_match(kv_key) {
                                to_delete.push(key_str.to_string());
                            }
                        } else {
                            break;
                        }
                    }
                    Err(e) => return Err(format!("Failed to iterate: {}", e)),
                }
            }

            for key in &to_delete {
                if db.delete(key).is_ok() {
                    count += 1;
                }
            }
        }

        for key in to_delete {
            self.kv_store.remove(&key);
            self.kv_value_cache.write().unwrap().remove(&key);
        }

        Ok(count)
    }

    pub fn kv_remove_all(&self, namespace_id: &str) -> Result<usize, String> {
        let namespace = {
            let namespaces = self.namespaces.read().unwrap();
            namespaces.get(namespace_id).cloned()
        };

        if namespace.is_none() {
            return Err(format!("namespace {} not found", namespace_id));
        }

        let prefix = format!("kv:{}:", namespace_id);
        let mut count = 0;
        let mut to_delete = Vec::new();

        if let Some(ref db) = self.db {
            let prefix_bytes = prefix.as_bytes();

            for result in db.iterator(rocksdb::IteratorMode::From(
                prefix_bytes,
                rocksdb::Direction::Forward,
            )) {
                match result {
                    Ok((key, _)) => {
                        let key_str = String::from_utf8_lossy(&key);
                        if key_str.starts_with(&prefix) {
                            to_delete.push(key_str.to_string());
                        } else {
                            break;
                        }
                    }
                    Err(e) => return Err(format!("Failed to iterate: {}", e)),
                }
            }

            for key in &to_delete {
                if db.delete(key).is_ok() {
                    count += 1;
                }
            }
        }

        for key in to_delete {
            self.kv_store.remove(&key);
            self.kv_value_cache.write().unwrap().remove(&key);
        }

        Ok(count)
    }

    pub fn kv_get_replica_id(&self) -> &str {
        &self.replica_id
    }

    pub fn kv_snapshot(&self) -> Vec<String> {
        self.kv_store.values()
    }

    pub fn kv_merge(&self, other_snapshot: &[String]) {
        let mut other_or_set = crate::crdt::or_set::ORSet::new();
        for key in other_snapshot {
            other_or_set.insert_with_counter(key.clone(), "remote", 0);
        }
        self.kv_store.merge(&other_or_set);
    }

    pub fn kv_get_stats(&self) -> KVCRDTStats {
        KVCRDTStats {
            key_count: self.kv_store.len(),
            counter: self.kv_store.get_counter(),
        }
    }

    fn save_block_to_db(&self, block_id: u64, block: &KVBlock) -> Result<(), String> {
        if let Some(ref db) = self.db {
            let key = format!("block:{}", block_id);
            let meta_json = serde_json::to_string(&block.meta)
                .map_err(|e| format!("Failed to serialize block meta: {}", e))?;
            let data = format!(
                "{}|||{}",
                meta_json,
                general_purpose::STANDARD.encode(&block.data)
            );
            db.put(key, data)
                .map_err(|e| format!("Failed to save block to db: {}", e))?;
        }
        Ok(())
    }

    fn save_namespace_to_db(
        &self,
        namespace_id: &str,
        namespace: &KVNamespace,
    ) -> Result<(), String> {
        if let Some(ref db) = self.db {
            let key = format!("namespace:{}", namespace_id);
            let json = serde_json::to_string(namespace)
                .map_err(|e| format!("Failed to serialize namespace: {}", e))?;
            db.put(key, json)
                .map_err(|e| format!("Failed to save namespace to db: {}", e))?;
        }
        Ok(())
    }

    fn delete_namespace_from_db(&self, namespace_id: &str) -> Result<(), String> {
        if let Some(ref db) = self.db {
            let key = format!("namespace:{}", namespace_id);
            db.delete(key)
                .map_err(|e| format!("Failed to delete namespace from db: {}", e))?;
        }
        Ok(())
    }

    fn load_from_db(&mut self) -> Result<(), String> {
        if let Some(ref db) = self.db {
            let mut iter = db.iterator(rocksdb::IteratorMode::Start);

            while let Some(Ok((key, value))) = iter.next() {
                let key_str = String::from_utf8_lossy(&key);
                let value_str = String::from_utf8_lossy(&value);

                if key_str.starts_with("namespace:") {
                    if let Ok(namespace) = serde_json::from_str::<KVNamespace>(&value_str) {
                        self.namespaces
                            .write()
                            .unwrap()
                            .insert(namespace.id.clone(), namespace);
                    }
                } else if key_str.starts_with("block:") {
                    let mut parts = value_str.splitn(2, "|||");
                    if let (Some(meta_json), Some(data_base64)) = (parts.next(), parts.next()) {
                        if let Ok(meta) = serde_json::from_str::<KVBlockMeta>(meta_json) {
                            if let Ok(data) = general_purpose::STANDARD.decode(data_base64) {
                                let block = KVBlock {
                                    meta: meta.clone(),
                                    data,
                                };
                                self.blocks.write().unwrap().insert(meta.block_id, block);
                                self.block_id_map
                                    .write()
                                    .unwrap()
                                    .insert(meta.block_id, meta.fid);

                                let mut stats = self.stats.lock().unwrap();
                                stats.total_blocks += 1;
                                stats.used_memory_bytes += meta.size_bytes;

                                if meta.block_id >= self.next_block_id.load(Ordering::SeqCst) {
                                    self.next_block_id
                                        .store(meta.block_id + 1, Ordering::SeqCst);
                                }
                            }
                        }
                    }
                } else if key_str.starts_with("kv:") {
                    self.kv_store.insert(key_str.to_string());
                    self.kv_value_cache
                        .write()
                        .unwrap()
                        .insert(key_str.to_string(), value.to_vec());
                }
            }
        }
        Ok(())
    }
}
