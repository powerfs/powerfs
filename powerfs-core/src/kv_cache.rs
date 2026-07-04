use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KVDtype {
    FP32,
    FP16,
    BF16,
    FP8,
    INT8,
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

#[derive(Debug, Clone)]
pub struct KVBlockMeta {
    pub block_id: u64,
    pub session_id: String,
    pub layer_id: u32,
    pub num_tokens: u32,
    pub dtype: KVDtype,
    pub head_dim: u32,
    pub num_heads: u32,
    pub size_bytes: u64,
    pub created_at: Instant,
    pub last_accessed: Instant,
    pub ttl: Option<Duration>,
    pub fid: String,
    pub block_index: u32,
}

pub struct KVBlock {
    pub meta: KVBlockMeta,
    pub data: Vec<u8>,
}

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
    pub model_name: String,
    pub num_layers: u32,
    pub num_heads: u32,
    pub head_dim: u32,
    pub dtype: KVDtype,
    pub created_at: Instant,
    pub last_accessed: Instant,
    pub block_ids: Vec<u64>,
    pub ttl: Option<Duration>,
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
    stats: Mutex<KVCacheStats>,
    next_block_id: AtomicU64,
    block_id_map: RwLock<HashMap<u64, String>>,
}

impl KVCacheEngine {
    pub fn new(max_memory_bytes: u64, block_size: usize) -> Self {
        let initial_blocks = (max_memory_bytes as usize / block_size / 10).max(1);
        let memory_pool = Arc::new(MemoryPool::new(block_size, initial_blocks));
        Self {
            max_memory_bytes,
            block_size,
            memory_pool,
            blocks: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            stats: Mutex::new(KVCacheStats::default()),
            next_block_id: AtomicU64::new(1),
            block_id_map: RwLock::new(HashMap::new()),
        }
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

        let now = Instant::now();
        let ttl = if ttl_seconds > 0 {
            Some(Duration::from_secs(ttl_seconds))
        } else {
            None
        };

        let session = KVSession {
            session_id: session_id.to_string(),
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

    pub fn put_block(
        &self,
        session_id: &str,
        layer_id: u32,
        num_tokens: u32,
        data: &[u8],
        fid: &str,
        block_index: u32,
    ) -> Result<u64, String> {
        {
            let sessions = self.sessions.read().unwrap();
            if !sessions.contains_key(session_id) {
                return Err(format!("session {} not found", session_id));
            }
        }

        let size_bytes = data.len() as u64;

        // Check memory and evict if needed - must not hold any locks while evicting
        self.ensure_memory(size_bytes)?;

        let block_id = self.next_block_id.fetch_add(1, Ordering::SeqCst);
        let now = Instant::now();

        let mut buf = self.memory_pool.allocate();
        let copy_len = data.len().min(buf.len());
        buf[..copy_len].copy_from_slice(&data[..copy_len]);

        // Get session info without holding lock during eviction
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
        };

        let block = KVBlock { meta, data: buf };

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
        block.meta.last_accessed = Instant::now();
        let meta = block.meta.clone();

        let mut stats = self.stats.lock().unwrap();
        stats.hits += 1;

        Some(meta)
    }

    pub fn get_block_data(&self, block_id: u64) -> Option<(KVBlockMeta, Vec<u8>)> {
        let mut blocks = self.blocks.write().unwrap();
        let block = blocks.get_mut(&block_id)?;
        block.meta.last_accessed = Instant::now();
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
            // Find LRU block
            let mut oldest_id: Option<u64> = None;
            let mut oldest_time = Instant::now();

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
        let now = Instant::now();
        let mut expired_sessions = Vec::new();

        {
            let sessions = self.sessions.read().unwrap();
            for (id, sess) in sessions.iter() {
                if let Some(ttl) = sess.ttl {
                    if now.duration_since(sess.last_accessed) > ttl {
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

        // Also check individual blocks
        let mut expired_blocks = Vec::new();
        {
            let blocks = self.blocks.read().unwrap();
            for (id, block) in blocks.iter() {
                if let Some(ttl) = block.meta.ttl {
                    if now.duration_since(block.meta.last_accessed) > ttl {
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

    pub fn batch_put(&self, requests: &[(String, u32, u32, Vec<u8>, String, u32)]) -> Vec<Result<u64, String>> {
        let mut results = Vec::with_capacity(requests.len());
        for (session_id, layer_id, num_tokens, data, fid, block_index) in requests {
            results.push(self.put_block(session_id, *layer_id, *num_tokens, data, fid, *block_index));
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
}
