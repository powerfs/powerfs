use base64::engine::{general_purpose, Engine as _};
use std::collections::{HashMap, HashSet, VecDeque};
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

impl PinMode {
    /// Map the wire encoding (0=None, 1=Soft, 2=Hard); unknown values fall
    /// back to None rather than erroring.
    pub fn from_u32(value: u32) -> Self {
        match value {
            1 => PinMode::Soft,
            2 => PinMode::Hard,
            _ => PinMode::None,
        }
    }
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
    /// Monotonic per-key version (proposal time in millis); used to reject
    /// stale replicated writes. Defaults to 0 for entries written before
    /// versioning existed.
    #[serde(default)]
    pub version: u128,
}

/// A generic KV value too large for the raft command is stored as a volume
/// needle; the engine keeps this lightweight JSON reference (not the bytes)
/// under the normal ``kv:<ns>:<key>`` slot. The read path detects the marker
/// and fetches the needle by fid.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KVExternalRef {
    /// Marker discriminator; always ``powerfs-external-ref``.
    pub kind: String,
    /// SeaweedFS-style fid of the volume needle holding the value.
    pub fid: String,
    /// Value length in bytes.
    pub size: u64,
    pub owner_id: String,
    pub updated_at: u64,
    pub version: u128,
}

impl KVExternalRef {
    pub const KIND: &'static str = "powerfs-external-ref";

    pub fn new(fid: String, size: u64, owner_id: &str, updated_at: u64, version: u128) -> Self {
        Self {
            kind: Self::KIND.to_string(),
            fid,
            size,
            owner_id: owner_id.to_string(),
            updated_at,
            version,
        }
    }

    /// Detect an external-reference record from the raw slot bytes. The slot
    /// holds either serialized ``KVStoredValue`` JSON (inline value) or this
    /// marker JSON (value lives in a volume needle).
    pub fn parse_marker(data: &[u8]) -> Option<Self> {
        let candidate: KVExternalRef = serde_json::from_slice(data).ok()?;
        if candidate.kind == Self::KIND {
            Some(candidate)
        } else {
            None
        }
    }
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
    /// Whether ``data`` is resident in memory. Memory-pressure eviction keeps
    /// the block's logical identity (meta + block_id_map fid) and sets this to
    /// ``false`` so reads transparently re-fetch the volume needle.
    pub resident: bool,
    /// Local (non-replicated, non-persisted) read counter. Incremented on each
    /// successful read (resident hit or volume re-fetch); used for read-heat
    /// observability (Phase C). Memory eviction does not reset it.
    pub read_count: u64,
}

/// Minimal replicated block descriptor (metadata + fid), independent of the
/// master-side raft struct so core needs no dependency on master types.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicatedBlock {
    pub block_id: u64,
    pub session_id: String,
    pub layer_id: u32,
    pub num_tokens: u32,
    pub fid: String,
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
    /// Collection this session's blocks are stored in. Empty means "default".
    pub collection: String,
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

/// Per-block read-heat row (Phase C). Mirrors a proto BlockHeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeatView {
    pub block_id: u64,
    pub session_id: String,
    pub namespace_id: String,
    pub read_count: u64,
    pub resident: bool,
}

/// Per-session read-heat row; read_count is the sum over the session's blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionHeatView {
    pub session_id: String,
    pub namespace_id: String,
    pub read_count: u64,
    pub block_count: u64,
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
        // Never recycle an empty (or otherwise undersized) buffer; handing it
        // out would silently truncate the next stored block.
        if buf.is_empty() {
            return;
        }
        let mut free = self.free_blocks.lock().unwrap();
        free.push(buf);
    }
}

unsafe impl Send for MemoryPool {}
unsafe impl Sync for MemoryPool {}

/// One orphan-needle GC candidate held by the engine (core-side mirror of
/// the master crate's `raft_v2::GcFid`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GcEntry {
    /// SeaweedFS-style fid "volume_id,cookie,file_key".
    pub fid: String,
    /// Wall-clock millis at enqueue time, used to apply the grace period.
    pub enqueued_at: u128,
    /// Why it was enqueued: 0=overwrite, 1=delete, 2=propose-fail, 3=session-delete.
    pub reason: u8,
}

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
    /// block_id -> session_id reverse index; lets a node locate a session for
    /// a block it only knows by metadata/fid (e.g. followers, after failover)
    /// without holding the block bytes or relying on a fid-prefix heuristic.
    block_session_map: RwLock<HashMap<u64, String>>,
    /// Orphan-needle GC candidates, deduplicated by fid and persisted under
    /// the `gc:` DB prefix.
    gc_queue: RwLock<VecDeque<GcEntry>>,
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
            block_session_map: RwLock::new(HashMap::new()),
            gc_queue: RwLock::new(VecDeque::new()),
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
            block_session_map: RwLock::new(HashMap::new()),
            gc_queue: RwLock::new(VecDeque::new()),
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
        collection: &str,
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
            collection: collection.to_string(),
        };

        sessions.insert(session_id.to_string(), session);

        let mut stats = self.stats.lock().unwrap();
        stats.total_sessions += 1;

        Ok(())
    }

    pub fn delete_session(&self, session_id: &str) -> Result<(), String> {
        // Remove the session and release the sessions lock BEFORE acquiring
        // blocks/block_id_map. This avoids a lock-order inversion against
        // apply_save_blocks (which holds block_id_map and then takes
        // sessions), which could deadlock the state-machine apply task.
        let block_ids: Vec<u64> = {
            let mut sessions = self.sessions.write().unwrap();
            let session = sessions
                .remove(session_id)
                .ok_or_else(|| format!("session {} not found", session_id))?;
            session.block_ids
        };

        // Collect block fids BEFORE removing the mappings, so the needles can
        // be enqueued for GC. Single short-lived read lock.
        let gc_fids: Vec<String> = {
            let block_id_map = self.block_id_map.read().unwrap();
            block_ids
                .iter()
                .filter_map(|id| block_id_map.get(id).cloned())
                .collect()
        };

        {
            let mut blocks = self.blocks.write().unwrap();
            let mut block_id_map = self.block_id_map.write().unwrap();
            let mut block_session_map = self.block_session_map.write().unwrap();
            let mut stats = self.stats.lock().unwrap();

            for block_id in &block_ids {
                if let Some(block) = blocks.remove(block_id) {
                    // used_memory and the pool buffer were already released at
                    // memory-eviction time for non-resident blocks; account and
                    // return the buffer only when it is still resident, so the
                    // pool is never poisoned with a zero-length buffer.
                    if block.resident {
                        stats.used_memory_bytes = stats
                            .used_memory_bytes
                            .saturating_sub(block.meta.size_bytes);
                        self.memory_pool.deallocate(block.data);
                    }
                    stats.total_blocks = stats.total_blocks.saturating_sub(1);
                }
                block_id_map.remove(block_id);
                block_session_map.remove(block_id);
            }

            stats.total_sessions = stats.total_sessions.saturating_sub(1);
        }

        // Enqueue after all structural locks are released (no nesting with
        // blocks/sessions/map). Idempotent: a replay finds no mappings and an
        // already-present candidate is deduplicated.
        if !gc_fids.is_empty() {
            let now = Self::now_millis();
            let entries: Vec<GcEntry> = gc_fids
                .into_iter()
                .map(|fid| GcEntry {
                    fid,
                    enqueued_at: now,
                    reason: 3,
                })
                .collect();
            self.apply_gc_enqueue(&entries)?;
        }

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

        let block = KVBlock {
            meta,
            data: buf,
            resident: true,
            read_count: 0,
        };

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

        self.block_session_map
            .write()
            .unwrap()
            .insert(block_id, session_id.to_string());

        Ok(block_id)
    }

    /// Allocate a block id without storing any bytes/persist state. Used by
    /// the replicated put path: id is reserved first, bytes go to volume, and
    /// the id->fid mapping is installed only when the raft entry applies.
    pub fn alloc_block_id(&self) -> u64 {
        self.next_block_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Advance the block-id allocator past ``block_id`` so a newly promoted
    /// leader never reuses an id already replicated.
    pub fn advance_next_block_id(&self, block_id: u64) {
        self.next_block_id
            .fetch_max(block_id.saturating_add(1), Ordering::SeqCst);
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

        // Reliable reverse index (covers followers / post-failover).
        let session_id = self
            .block_session_map
            .read()
            .unwrap()
            .get(&block_id)
            .cloned();
        if let Some(sid) = session_id {
            return self.sessions.read().unwrap().get(&sid).cloned();
        }

        // Legacy heuristic fallback. Read the fid and release the map lock
        // BEFORE taking the sessions lock (global order: sessions -> map) to
        // avoid an ABBA inversion against evict_lru.
        let fid_for_check = {
            let block_id_map = self.block_id_map.read().unwrap();
            block_id_map.get(&block_id).cloned()
        };
        if let Some(fid_str) = fid_for_check {
            let sessions = self.sessions.read().unwrap();
            for sess in sessions.values() {
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
        // Memory-evicted block: bytes are not resident; signal the caller to
        // transparently re-fetch from the volume needle via block_id_map.
        if !block.resident {
            return None;
        }
        block.meta.last_accessed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Count this successful resident read for read-heat observability.
        block.read_count = block.read_count.saturating_add(1);
        let meta = block.meta.clone();
        // Clamp to the actual buffer length to avoid an out-of-bounds panic on
        // a corrupted/truncated block.
        let take = (meta.size_bytes as usize).min(block.data.len());
        let data = block.data[..take].to_vec();

        let mut stats = self.stats.lock().unwrap();
        stats.hits += 1;

        Some((meta, data))
    }

    /// Test-only: force a block's last-accessed timestamp so eviction tests can
    /// deterministically order cold/warm sessions. Returns false if absent.
    #[cfg(test)]
    pub(crate) fn set_block_last_accessed(&self, block_id: u64, ts: u64) -> bool {
        let mut blocks = self.blocks.write().unwrap();
        match blocks.get_mut(&block_id) {
            Some(b) => {
                b.meta.last_accessed = ts;
                true
            }
            None => false,
        }
    }

    pub fn get_session_blocks(&self, session_id: &str) -> Vec<KVBlockMeta> {
        // Snapshot the block ids and release the sessions lock BEFORE taking
        // the blocks lock, matching the global lock order (blocks -> sessions)
        // used by evict_lru/cleanup_expired and avoiding an ABBA deadlock.
        let block_ids: Vec<u64> = {
            let sessions = self.sessions.read().unwrap();
            match sessions.get(session_id) {
                Some(s) => s.block_ids.clone(),
                None => return Vec::new(),
            }
        };

        let blocks = self.blocks.read().unwrap();
        let mut result = Vec::new();
        for bid in block_ids {
            if let Some(block) = blocks.get(&bid) {
                result.push(block.meta.clone());
            }
        }
        result
    }

    pub fn stats(&self) -> KVCacheStats {
        self.stats.lock().unwrap().clone()
    }

    /// Count one successful read of a (possibly non-resident) block. Called by
    /// the master after a volume re-fetch succeeds. No-op if already removed.
    /// Uses only the blocks lock; no sessions/maps nesting.
    pub fn record_block_read(&self, block_id: u64) {
        let mut blocks = self.blocks.write().unwrap();
        if let Some(b) = blocks.get_mut(&block_id) {
            b.read_count = b.read_count.saturating_add(1);
        }
    }

    /// Build the read-heat snapshot in one blocks read-lock: per-block rows plus
    /// per-session rows aggregated from them (read_count summed over all the
    /// session's blocks, including non-resident). Sessions sort by reads desc;
    /// blocks sort by reads desc and are truncated to ``top_blocks`` (0=all).
    pub fn read_heat(&self, top_blocks: usize) -> (Vec<SessionHeatView>, Vec<BlockHeatView>) {
        use std::collections::HashMap;

        let blocks = self.blocks.read().unwrap();
        // (session_id, namespace_id) -> (summed reads, block count)
        let mut sess: HashMap<(String, String), (u64, u64)> = HashMap::new();
        let mut block_views: Vec<BlockHeatView> = Vec::with_capacity(blocks.len());

        for b in blocks.values() {
            block_views.push(BlockHeatView {
                block_id: b.meta.block_id,
                session_id: b.meta.session_id.clone(),
                namespace_id: b.meta.namespace_id.clone(),
                read_count: b.read_count,
                resident: b.resident,
            });
            let e = sess
                .entry((b.meta.session_id.clone(), b.meta.namespace_id.clone()))
                .or_insert((0, 0));
            e.0 = e.0.saturating_add(b.read_count);
            e.1 = e.1.saturating_add(1);
        }
        drop(blocks);

        let mut sessions: Vec<SessionHeatView> = sess
            .into_iter()
            .map(
                |((session_id, namespace_id), (read_count, block_count))| SessionHeatView {
                    session_id,
                    namespace_id,
                    read_count,
                    block_count,
                },
            )
            .collect();
        sessions.sort_by(|a, b| {
            b.read_count
                .cmp(&a.read_count)
                .then_with(|| a.session_id.cmp(&b.session_id))
        });

        block_views.sort_by(|a, b| {
            b.read_count
                .cmp(&a.read_count)
                .then_with(|| a.block_id.cmp(&b.block_id))
        });
        if top_blocks > 0 && block_views.len() > top_blocks {
            block_views.truncate(top_blocks);
        }

        (sessions, block_views)
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
        let mut stats = self.stats.lock().unwrap();

        let mut evicted_bytes: u64 = 0;

        while evicted_bytes < needed_bytes && !blocks.is_empty() {
            // Session-coordinated selection among RESIDENT blocks. Anchor a
            // target SESSION from the globally oldest eligible block: prefer
            // the oldest None block's session, otherwise the oldest Soft
            // block's session. Hard-pinned blocks never anchor a group.
            let mut none_time = u64::MAX;
            let mut none_sess: Option<String> = None;
            let mut soft_time = u64::MAX;
            let mut soft_sess: Option<String> = None;

            for block in blocks.values() {
                if !block.resident {
                    continue;
                }
                match block.meta.pin_mode {
                    PinMode::Hard => continue,
                    PinMode::None if block.meta.last_accessed < none_time => {
                        none_time = block.meta.last_accessed;
                        none_sess = Some(block.meta.session_id.clone());
                    }
                    PinMode::Soft if block.meta.last_accessed < soft_time => {
                        soft_time = block.meta.last_accessed;
                        soft_sess = Some(block.meta.session_id.clone());
                    }
                    _ => {}
                }
            }

            let (target_sess, target_tier) = match none_sess
                .map(|s| (s, PinMode::None))
                .or_else(|| soft_sess.map(|s| (s, PinMode::Soft)))
            {
                Some(v) => v,
                // No evictable resident block remains (only Hard / non-resident).
                None => break,
            };

            // Reclaim, as one group, every RESIDENT block in the target session
            // whose pin tier matches. In the None tier, Soft/Hard blocks in that
            // session are retained (global "all None before any Soft"); Hard is
            // never evicted in either tier.
            let group: Vec<u64> = blocks
                .iter()
                .filter(|(_, b)| {
                    b.resident && b.meta.pin_mode == target_tier && b.meta.session_id == target_sess
                })
                .map(|(id, _)| *id)
                .collect();

            for group_id in group {
                let mut block = match blocks.remove(&group_id) {
                    Some(b) => b,
                    None => continue,
                };

                // Memory-only eviction (same as Phase A): release the bytes but
                // keep the block's logical identity (meta + block_id_map fid +
                // session block id), so reads transparently re-fetch the volume
                // needle and a later delete_session still reclaims it.
                let freed = block.meta.size_bytes;
                self.memory_pool.deallocate(std::mem::take(&mut block.data));
                block.resident = false;
                blocks.insert(group_id, block);

                evicted_bytes += freed;
                stats.used_memory_bytes = stats.used_memory_bytes.saturating_sub(freed);
                stats.evictions += 1;
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

        let gc_candidates: Vec<(String, u8)> = {
            let mut blocks = self.blocks.write().unwrap();
            let mut sessions = self.sessions.write().unwrap();
            let mut block_id_map = self.block_id_map.write().unwrap();
            let mut block_session_map = self.block_session_map.write().unwrap();
            let mut stats = self.stats.lock().unwrap();

            let mut cands = Vec::new();
            for bid in expired_blocks {
                if let Some(mut block) = blocks.remove(&bid) {
                    count += 1;
                    // Expired block is genuinely dead: drop every index and
                    // record the fid so the GC worker reclaims its needle.
                    // Only release/account bytes that are still resident; a
                    // non-resident block already freed them at memory eviction.
                    if block.resident {
                        stats.used_memory_bytes = stats
                            .used_memory_bytes
                            .saturating_sub(block.meta.size_bytes);
                        self.memory_pool.deallocate(std::mem::take(&mut block.data));
                        stats.evictions += 1;
                    }
                    stats.total_blocks = stats.total_blocks.saturating_sub(1);
                    block_id_map.remove(&bid);
                    block_session_map.remove(&bid);
                    cands.push((block.meta.fid.clone(), 1u8));

                    if let Some(sess) = sessions.get_mut(&block.meta.session_id) {
                        sess.block_ids.retain(|&id| id != bid);
                    }
                }
            }
            cands
        };

        // Enqueue outside the structural locks so gc_queue never nests with
        // them; GC enqueue failure (DB error) is non-fatal.
        if !gc_candidates.is_empty() {
            let enqueued_at = Self::now_millis();
            let entries: Vec<GcEntry> = gc_candidates
                .iter()
                .map(|(fid, reason)| GcEntry {
                    fid: fid.clone(),
                    enqueued_at,
                    reason: *reason,
                })
                .collect();
            let _ = self.apply_gc_enqueue(&entries);
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

    /// List namespaces. An empty `owner_id` means "no filter" and returns
    /// every namespace (administrative view); a non-empty value filters to
    /// namespaces owned by that exact owner.
    pub fn list_namespaces(&self, owner_id: &str) -> Vec<KVNamespace> {
        let namespaces = self.namespaces.read().unwrap();
        namespaces
            .values()
            .filter(|ns| owner_id.is_empty() || ns.owner_id == owner_id)
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
            version: Self::now_millis(),
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

    // ==================================================================
    // Replication apply entry points (driven by raft apply_command).
    //
    // These are the ONLY mutators the replicated path uses. They are
    // idempotent (a leader applies optimistically and again on commit; nodes
    // replay on startup) and use per-key versions to prevent stale writes
    // from regressing newer values.
    // ==================================================================

    /// Current wall time in milliseconds (version clock).
    fn now_millis() -> u128 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    }

    /// Apply a generic KV upsert. ``is_inline`` selects between raw inline
    /// bytes and an external volume needle reference.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_kv_put(
        &self,
        namespace_id: &str,
        key: &str,
        is_inline: bool,
        inline_bytes: &[u8],
        fid: &str,
        size: u64,
        owner_id: &str,
        version: u128,
    ) -> Result<(), String> {
        // Namespace must exist; this naturally drops writes to a namespace
        // that was deleted before this entry applied.
        if self.namespaces.read().unwrap().get(namespace_id).is_none() {
            return Err(format!("namespace {} not found", namespace_id));
        }

        let kv_key = format!("kv:{}:{}", namespace_id, key);

        // Version guard: never let a stale write overwrite a newer value.
        if let Some(existing) = self.read_slot_version(&kv_key) {
            if existing > version {
                return Ok(());
            }
        }

        // Overwrite: if the current slot references a different external
        // needle, enqueue the old fid for GC BEFORE writing the new slot. On a
        // replay the slot already holds this command's fid, so it is a no-op
        // (idempotent, no duplicate candidate).
        if let Some(old_raw) = self.raw_for_key(&kv_key) {
            if let Some(old_marker) = KVExternalRef::parse_marker(&old_raw) {
                if old_marker.fid != fid {
                    self.apply_gc_enqueue(&[GcEntry {
                        fid: old_marker.fid,
                        enqueued_at: Self::now_millis(),
                        reason: 0,
                    }])?;
                }
            }
        }

        let now_secs = (version / 1000) as u64;
        let slot_bytes = if is_inline {
            let stored = KVStoredValue {
                data: inline_bytes.to_vec(),
                owner_id: owner_id.to_string(),
                created_at: now_secs,
                updated_at: now_secs,
                version,
            };
            serde_json::to_vec(&stored)
                .map_err(|e| format!("Failed to serialize stored value: {}", e))?
        } else {
            let marker = KVExternalRef::new(fid.to_string(), size, owner_id, now_secs, version);
            serde_json::to_vec(&marker)
                .map_err(|e| format!("Failed to serialize external ref: {}", e))?
        };

        self.kv_store.insert(kv_key.clone());
        self.kv_value_cache
            .write()
            .unwrap()
            .insert(kv_key.clone(), slot_bytes.clone());
        if let Some(ref db) = self.db {
            db.put(&kv_key, slot_bytes)
                .map_err(|e| format!("Failed to put key: {}", e))?;
        }
        Ok(())
    }

    /// Raw slot bytes from the in-memory cache, falling back to local DB.
    fn raw_for_key(&self, kv_key: &str) -> Option<Vec<u8>> {
        self.kv_value_cache
            .read()
            .unwrap()
            .get(kv_key)
            .cloned()
            .or_else(|| {
                self.db
                    .as_ref()
                    .and_then(|db| db.get(kv_key).ok().flatten())
            })
    }

    /// Read the version recorded in a slot (inline value or external marker).
    fn read_slot_version(&self, kv_key: &str) -> Option<u128> {
        let raw = self.raw_for_key(kv_key)?;

        if let Some(marker) = KVExternalRef::parse_marker(&raw) {
            return Some(marker.version);
        }
        serde_json::from_slice::<KVStoredValue>(&raw)
            .ok()
            .map(|v| v.version)
    }

    /// Apply deletion of an explicit set of keys (single key or a batch that
    /// the leader enumerated at proposal time).
    pub fn apply_kv_delete_keys(&self, namespace_id: &str, keys: &[String]) -> Result<(), String> {
        let mut gc_fids: Vec<String> = Vec::new();
        for key in keys {
            let kv_key = format!("kv:{}:{}", namespace_id, key);
            // Capture an external fid before the slot is removed.
            if let Some(raw) = self.raw_for_key(&kv_key) {
                if let Some(marker) = KVExternalRef::parse_marker(&raw) {
                    if !gc_fids.contains(&marker.fid) {
                        gc_fids.push(marker.fid);
                    }
                }
            }
            self.kv_store.remove(&kv_key);
            self.kv_value_cache.write().unwrap().remove(&kv_key);
            if let Some(ref db) = self.db {
                let _ = db.delete(&kv_key);
            }
        }
        if !gc_fids.is_empty() {
            let now = Self::now_millis();
            let entries: Vec<GcEntry> = gc_fids
                .into_iter()
                .map(|fid| GcEntry {
                    fid,
                    enqueued_at: now,
                    reason: 1,
                })
                .collect();
            self.apply_gc_enqueue(&entries)?;
        }
        Ok(())
    }

    /// Apply namespace creation. Upsert semantics (re-creating after replay)
    /// but never overwrite an existing namespace.
    pub fn apply_create_namespace(
        &self,
        namespace_id: &str,
        name: &str,
        owner_id: &str,
        version: u128,
    ) -> Result<(), String> {
        let mut namespaces = self.namespaces.write().unwrap();
        if namespaces.contains_key(namespace_id) {
            return Ok(());
        }
        let now_secs = (version / 1000) as u64;
        let namespace = KVNamespace {
            id: namespace_id.to_string(),
            name: name.to_string(),
            owner_id: owner_id.to_string(),
            created_at: now_secs,
            updated_at: now_secs,
        };
        namespaces.insert(namespace_id.to_string(), namespace.clone());
        drop(namespaces);
        self.save_namespace_to_db(namespace_id, &namespace)
    }

    /// Apply namespace deletion plus cleanup of its keys and session/block
    /// metadata.
    pub fn apply_delete_namespace(&self, namespace_id: &str) -> Result<(), String> {
        // Remove generic KV slots of this namespace.
        let prefix = format!("kv:{}:", namespace_id);
        let mut slot_keys: Vec<String> = Vec::new();
        if let Some(ref db) = self.db {
            for (k, _) in db.prefix_iterator(prefix.as_bytes()).flatten() {
                slot_keys.push(String::from_utf8_lossy(&k).to_string());
            }
        }
        let mut gc_fids: Vec<String> = Vec::new();
        for full in &slot_keys {
            // Capture an external fid before the slot is removed.
            if let Some(raw) = self.raw_for_key(full) {
                if let Some(marker) = KVExternalRef::parse_marker(&raw) {
                    if !gc_fids.contains(&marker.fid) {
                        gc_fids.push(marker.fid);
                    }
                }
            }
            self.kv_store.remove(full);
            self.kv_value_cache.write().unwrap().remove(full);
            if let Some(ref db) = self.db {
                let _ = db.delete(full);
            }
        }
        if !gc_fids.is_empty() {
            let now = Self::now_millis();
            let entries: Vec<GcEntry> = gc_fids
                .into_iter()
                .map(|fid| GcEntry {
                    fid,
                    enqueued_at: now,
                    reason: 1,
                })
                .collect();
            self.apply_gc_enqueue(&entries)?;
        }

        // Remove sessions (and their block mappings) bound to this namespace.
        let session_ids: Vec<String> = self
            .sessions
            .read()
            .unwrap()
            .values()
            .filter(|s| s.namespace_id == namespace_id)
            .map(|s| s.session_id.clone())
            .collect();
        for sid in session_ids {
            let _ = self.delete_session(&sid);
        }

        self.namespaces.write().unwrap().remove(namespace_id);
        self.delete_namespace_from_db(namespace_id)
    }

    /// Apply session creation (idempotent).
    #[allow(clippy::too_many_arguments)]
    pub fn apply_create_session(
        &self,
        session_id: &str,
        namespace_id: &str,
        owner_id: &str,
        model_name: &str,
        num_layers: u32,
        num_heads: u32,
        head_dim: u32,
        dtype: &str,
        ttl_seconds: u64,
        collection: &str,
    ) -> Result<(), String> {
        if self.sessions.read().unwrap().contains_key(session_id) {
            return Ok(());
        }

        // Bootstrapped default namespace: sessions without an explicit
        // namespace live here. Create it locally if missing so session
        // creation/replay never depends on a separate namespace command.
        if namespace_id == "default" && self.namespaces.read().unwrap().get("default").is_none() {
            let now_secs = Self::now_millis() as u64 / 1000;
            let ns = KVNamespace {
                id: "default".to_string(),
                name: "default".to_string(),
                owner_id: String::new(),
                created_at: now_secs,
                updated_at: now_secs,
            };
            self.namespaces
                .write()
                .unwrap()
                .insert("default".to_string(), ns.clone());
            let _ = self.save_namespace_to_db("default", &ns);
        }

        self.create_session(
            session_id,
            namespace_id,
            owner_id,
            model_name,
            num_layers,
            num_heads,
            head_dim,
            KVDtype::parse(dtype).unwrap_or(KVDtype::FP16),
            ttl_seconds,
            collection,
        )
    }

    /// Apply block metadata + fid mapping for one or more blocks. Metadata
    /// only: nodes that don't already hold the bytes resolve them on read via
    /// the fid. Idempotent across leader optimistic apply / commit / replay.
    pub fn apply_save_blocks(&self, blocks: &[ReplicatedBlock]) -> Result<(), String> {
        // Phase 1: block-id allocator + id maps. Each guard is temporary and
        // released before any sessions lock is taken, so no two of these
        // locks are ever held together.
        for b in blocks {
            self.advance_next_block_id(b.block_id);
            self.block_id_map
                .write()
                .unwrap()
                .insert(b.block_id, b.fid.clone());
            self.block_session_map
                .write()
                .unwrap()
                .insert(b.block_id, b.session_id.clone());
        }

        // Phase 2: append ids to sessions (sessions lock only).
        for b in blocks {
            let mut sessions = self.sessions.write().unwrap();
            if let Some(session) = sessions.get_mut(&b.session_id) {
                if !session.block_ids.contains(&b.block_id) {
                    session.block_ids.push(b.block_id);
                }
            }
        }
        Ok(())
    }

    /// Leader-only path: cache a block's bytes in the local memory cache and
    /// install its metadata maps after the bytes have already been written to
    /// a volume needle. Followers never call this — they only receive the
    /// metadata via `apply_save_blocks` and fetch bytes from the volume on
    /// read. Idempotent: if the block is already cached locally this is a
    /// no-op, so the raft commit replay (apply_save_blocks) cannot double
    /// count it.
    #[allow(clippy::too_many_arguments)]
    pub fn store_leader_block(
        &self,
        block_id: u64,
        session_id: &str,
        layer_id: u32,
        num_tokens: u32,
        data: &[u8],
        fid: &str,
        block_index: u32,
        pin_mode: PinMode,
    ) -> Result<(), String> {
        {
            let blocks = self.blocks.read().unwrap();
            if blocks.contains_key(&block_id) {
                return Ok(()); // already cached (replay) — keep stats intact
            }
        }

        {
            let sessions = self.sessions.read().unwrap();
            if !sessions.contains_key(session_id) {
                return Err(format!("session {} not found", session_id));
            }
        }

        let size_bytes = data.len() as u64;
        self.ensure_memory(size_bytes)?;

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
            namespace_id: session.namespace_id,
            owner_id: session.owner_id,
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

        let block = KVBlock {
            meta,
            data: buf,
            resident: true,
            read_count: 0,
        };
        self.save_block_to_db(block_id, &block)?;

        self.blocks.write().unwrap().insert(block_id, block);
        {
            let mut sessions = self.sessions.write().unwrap();
            if let Some(sess) = sessions.get_mut(session_id) {
                if !sess.block_ids.contains(&block_id) {
                    sess.block_ids.push(block_id);
                }
                sess.last_accessed = now;
            }
        }
        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_blocks += 1;
            stats.used_memory_bytes += size_bytes;
        }
        self.block_id_map
            .write()
            .unwrap()
            .insert(block_id, fid.to_string());
        self.block_session_map
            .write()
            .unwrap()
            .insert(block_id, session_id.to_string());

        Ok(())
    }

    /// Authoritatively reset ALL replicated KV state (generic KV slots,
    /// namespaces, sessions and block mappings), both in memory and in the
    /// local RocksDB. Used immediately before rebuilding the engine from the
    /// committed raft log when a node becomes leader, so any state left by a
    /// failed optimistic apply (a "phantom" entry that never committed) can
    /// never be served after a re-election. Bytes already written to volumes
    /// are left intact (they are content-addressed; unreferenced needles are
    /// harmless garbage).
    pub fn reset_replicated_state(&self) {
        let keys_to_delete: Vec<String> = {
            let mut keys = Vec::new();
            if let Some(ref db) = self.db {
                let mut iter = db.iterator(rocksdb::IteratorMode::Start);
                while let Some(Ok((k, _))) = iter.next() {
                    let ks = String::from_utf8_lossy(&k);
                    if ks.starts_with("kv:")
                        || ks.starts_with("namespace:")
                        || ks.starts_with("block:")
                        || ks.starts_with("gc:")
                    {
                        keys.push(ks.into_owned());
                    }
                }
            }
            keys
        };
        if let Some(ref db) = self.db {
            for k in &keys_to_delete {
                let _ = db.delete(k);
            }
        }

        self.sessions.write().unwrap().clear();
        self.blocks.write().unwrap().clear();
        self.namespaces.write().unwrap().clear();
        self.kv_value_cache.write().unwrap().clear();
        self.kv_store.clear();
        self.block_id_map.write().unwrap().clear();
        self.block_session_map.write().unwrap().clear();
        self.gc_queue.write().unwrap().clear();
        let mut stats = self.stats.lock().unwrap();
        stats.total_sessions = 0;
        stats.total_blocks = 0;
        stats.used_memory_bytes = 0;
    }

    /// Apply replicated orphan-needle GC candidates. Idempotent and
    /// deduplicated by fid; new candidates are persisted under `gc:<fid>`.
    pub fn apply_gc_enqueue(&self, entries: &[GcEntry]) -> Result<(), String> {
        let mut added: Vec<GcEntry> = Vec::new();
        {
            let mut queue = self.gc_queue.write().unwrap();
            for entry in entries {
                if queue.iter().any(|e| e.fid == entry.fid) {
                    continue;
                }
                queue.push_back(entry.clone());
                added.push(entry.clone());
            }
        }
        if let Some(ref db) = self.db {
            for entry in &added {
                let value = serde_json::to_vec(entry)
                    .map_err(|e| format!("Failed to serialize gc entry: {}", e))?;
                db.put(format!("gc:{}", entry.fid), value)
                    .map_err(|e| format!("Failed to persist gc entry: {}", e))?;
            }
        }
        Ok(())
    }

    /// Number of pending GC candidates (tests / observability).
    pub fn gc_queue_len(&self) -> usize {
        self.gc_queue.read().unwrap().len()
    }

    /// Snapshot every needle fid currently referenced by authoritative state:
    /// external markers in KV slots and block fids in block_id_map. Read-only,
    /// uses short locks that are released before parsing, so it never nests
    /// with structural locks (lock-order red line).
    pub fn snapshot_referenced_fids(&self) -> HashSet<String> {
        let mut referenced: HashSet<String> = HashSet::new();

        // Collect the full set of KV slot keys under the `kv:` prefix.
        let mut slot_keys: Vec<String> = self
            .kv_store
            .values()
            .into_iter()
            .filter(|k| k.starts_with("kv:"))
            .collect();
        if let Some(ref db) = self.db {
            for (k, _) in db
                .iterator(rocksdb::IteratorMode::From(
                    b"kv:",
                    rocksdb::Direction::Forward,
                ))
                .flatten()
            {
                let ks = String::from_utf8_lossy(&k).to_string();
                if !ks.starts_with("kv:") {
                    break;
                }
                if !slot_keys.contains(&ks) {
                    slot_keys.push(ks);
                }
            }
        }

        for full in slot_keys {
            if let Some(raw) = self.raw_for_key(&full) {
                if let Some(marker) = KVExternalRef::parse_marker(&raw) {
                    referenced.insert(marker.fid);
                }
            }
        }

        // Block fids are authoritative references too.
        for fid in self.block_id_map.read().unwrap().values() {
            referenced.insert(fid.clone());
        }

        referenced
    }

    /// Return candidates that have aged past the grace window WITHOUT removing
    /// them. A candidate is removed only after a confirmed delete via
    /// complete_gc, so a transient failure is retried on a later tick.
    pub fn take_due_entries(&self, now: u128, grace: u128) -> Vec<GcEntry> {
        let queue = self.gc_queue.read().unwrap();
        queue
            .iter()
            .filter(|e| now >= e.enqueued_at.saturating_add(grace))
            .cloned()
            .collect()
    }

    /// Remove a confirmed GC candidate from the queue and its `gc:<fid>` key.
    pub fn complete_gc(&self, fid: &str) {
        {
            let mut queue = self.gc_queue.write().unwrap();
            queue.retain(|e| e.fid != fid);
        }
        if let Some(ref db) = self.db {
            let _ = db.delete(format!("gc:{}", fid));
        }
    }

    /// Return the raw slot bytes for a generic KV key (None if absent).
    pub fn get_raw_slot(&self, namespace_id: &str, key: &str) -> Option<Vec<u8>> {
        let kv_key = format!("kv:{}:{}", namespace_id, key);
        self.kv_value_cache
            .read()
            .unwrap()
            .get(&kv_key)
            .cloned()
            .or_else(|| {
                self.db
                    .as_ref()
                    .and_then(|db| db.get(&kv_key).ok().flatten())
            })
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

    /// Enumerate every bare key in a namespace (for batch delete proposals).
    /// Sources both the in-memory ORSet and the local RocksDB slot prefix.
    pub fn enumerate_namespace_keys(&self, namespace_id: &str) -> Vec<String> {
        let prefix = format!("kv:{}:", namespace_id);
        let mut out: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

        for full in self.kv_store.values() {
            if let Some(bare) = full.strip_prefix(&prefix) {
                out.insert(bare.to_string());
            }
        }

        if let Some(ref db) = self.db {
            for (k, _) in db
                .iterator(rocksdb::IteratorMode::From(
                    prefix.as_bytes(),
                    rocksdb::Direction::Forward,
                ))
                .flatten()
            {
                let ks = String::from_utf8_lossy(&k);
                if let Some(bare) = ks.strip_prefix(&prefix) {
                    out.insert(bare.to_string());
                } else {
                    break;
                }
            }
        }

        out.into_iter().collect()
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

        let mut to_delete: std::collections::HashSet<String> = std::collections::HashSet::new();

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
                                to_delete.insert(key_str.to_string());
                            }
                        } else {
                            break;
                        }
                    }
                    Err(e) => return Err(format!("Failed to iterate: {}", e)),
                }
            }
        }

        // Also cover keys that exist only in the in-memory ORSet (e.g. an
        // engine constructed without a RocksDB handle).
        for full_key in self.kv_store.values() {
            if let Some(kv_key) = full_key.strip_prefix(&prefix) {
                if re.is_match(kv_key) {
                    to_delete.insert(full_key.clone());
                }
            }
        }

        let count = to_delete.len();
        self.purge_keys(to_delete);

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
        let mut to_delete: std::collections::HashSet<String> = std::collections::HashSet::new();

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
                            to_delete.insert(key_str.to_string());
                        } else {
                            break;
                        }
                    }
                    Err(e) => return Err(format!("Failed to iterate: {}", e)),
                }
            }
        }

        // Also cover keys that exist only in the in-memory ORSet.
        for full_key in self.kv_store.values() {
            if full_key.starts_with(&prefix) {
                to_delete.insert(full_key.clone());
            }
        }

        let count = to_delete.len();
        self.purge_keys(to_delete);

        Ok(count)
    }

    /// Delete the given full keys from RocksDB (if present), the replicated
    /// ORSet and the value cache. Best-effort on the DB layer; memory state
    /// is always cleared.
    fn purge_keys(&self, keys: std::collections::HashSet<String>) {
        if let Some(ref db) = self.db {
            for key in &keys {
                let _ = db.delete(key);
            }
        }

        let mut value_cache = self.kv_value_cache.write().unwrap();
        for key in keys {
            self.kv_store.remove(&key);
            value_cache.remove(&key);
        }
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
                                    resident: true,
                                    read_count: 0,
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
                } else if key_str.starts_with("gc:") {
                    if let Ok(entry) = serde_json::from_str::<GcEntry>(&value_str) {
                        let mut queue = self.gc_queue.write().unwrap();
                        if !queue.iter().any(|e| e.fid == entry.fid) {
                            queue.push_back(entry);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod replicate_tests {
    use super::*;

    fn engine() -> KVCacheEngine {
        KVCacheEngine::new(64 * 1024 * 1024, 4096)
    }

    #[test]
    fn apply_inline_twice_is_idempotent() {
        let e = engine();
        e.apply_create_namespace("ns", "n", "o", 1000).unwrap();
        e.apply_kv_put("ns", "k", true, b"v1", "", 0, "o", 2000)
            .unwrap();
        // Re-apply identical command (leader optimistic + commit replay).
        e.apply_kv_put("ns", "k", true, b"v1", "", 0, "o", 2000)
            .unwrap();

        let raw = e.get_raw_slot("ns", "k").unwrap();
        let v: KVStoredValue = serde_json::from_slice(&raw).unwrap();
        assert_eq!(v.data, b"v1");
        assert_eq!(v.version, 2000);
    }

    #[test]
    fn stale_version_does_not_regress() {
        let e = engine();
        e.apply_create_namespace("ns", "n", "o", 1000).unwrap();
        e.apply_kv_put("ns", "k", true, b"new", "", 0, "o", 5000)
            .unwrap();
        // Older version must be ignored.
        e.apply_kv_put("ns", "k", true, b"old", "", 0, "o", 3000)
            .unwrap();

        let raw = e.get_raw_slot("ns", "k").unwrap();
        let v: KVStoredValue = serde_json::from_slice(&raw).unwrap();
        assert_eq!(v.data, b"new");
        assert_eq!(v.version, 5000);
    }

    #[test]
    fn delete_makes_key_invisible() {
        let e = engine();
        e.apply_create_namespace("ns", "n", "o", 1000).unwrap();
        e.apply_kv_put("ns", "a", true, b"x", "", 0, "o", 2000)
            .unwrap();
        e.apply_kv_put("ns", "b", true, b"y", "", 0, "o", 2001)
            .unwrap();

        e.apply_kv_delete_keys("ns", &["a".to_string()]).unwrap();
        assert!(e.get_raw_slot("ns", "a").is_none());
        assert!(e.get_raw_slot("ns", "b").is_some());

        // Deleting an already-absent key is a no-op (idempotent replay).
        e.apply_kv_delete_keys("ns", &["a".to_string()]).unwrap();
        assert!(e.get_raw_slot("ns", "a").is_none());
    }

    #[test]
    fn external_marker_roundtrip() {
        let e = engine();
        e.apply_create_namespace("ns", "n", "o", 1000).unwrap();
        e.apply_kv_put("ns", "big", false, &[], "3,012345", 4096, "o", 7000)
            .unwrap();

        let raw = e.get_raw_slot("ns", "big").unwrap();
        let marker = KVExternalRef::parse_marker(&raw).expect("must be a marker");
        assert_eq!(marker.fid, "3,012345");
        assert_eq!(marker.size, 4096);
        assert_eq!(marker.version, 7000);

        // Version guard works for markers too: older inline write is dropped.
        e.apply_kv_put("ns", "big", true, b"stale", "", 0, "o", 6000)
            .unwrap();
        let raw2 = e.get_raw_slot("ns", "big").unwrap();
        assert!(KVExternalRef::parse_marker(&raw2).is_some());
    }

    #[test]
    fn create_namespace_is_idempotent() {
        let e = engine();
        e.apply_create_namespace("ns", "first", "o", 1000).unwrap();
        // Same id, different name must not overwrite.
        e.apply_create_namespace("ns", "second", "o", 2000).unwrap();
        let ns = e.get_namespace("ns").unwrap();
        assert_eq!(ns.name, "first");
    }

    #[test]
    fn leader_block_cache_is_idempotent_and_counts_once() {
        let e = engine();
        e.apply_create_session("sess", "default", "o", "gpt", 2, 4, 64, "fp16", 0, "")
            .unwrap();

        let data = vec![7u8; 1000];
        e.store_leader_block(50, "sess", 0, 8, &data, "1,abc", 0, PinMode::None)
            .unwrap();
        // Commit replay (apply_save_blocks) must not double count the cached
        // block when store is called again.
        e.store_leader_block(50, "sess", 0, 8, &data, "1,abc", 0, PinMode::None)
            .unwrap();

        let stats = e.stats.lock().unwrap();
        assert_eq!(stats.total_blocks, 1);
        assert_eq!(stats.used_memory_bytes, 1000);
        assert_eq!(e.get_fid_by_block_id(50).as_deref(), Some("1,abc"));
        assert_eq!(
            e.get_session_by_block_id(50)
                .map(|s| s.session_id)
                .as_deref(),
            Some("sess")
        );
    }

    #[test]
    fn gc_enqueue_deduplicates_by_fid() {
        let e = engine();
        let entries = vec![
            GcEntry {
                fid: "1,2,3".to_string(),
                enqueued_at: 100,
                reason: 0,
            },
            GcEntry {
                fid: "1,2,3".to_string(),
                enqueued_at: 200,
                reason: 1,
            },
            GcEntry {
                fid: "4,5,6".to_string(),
                enqueued_at: 300,
                reason: 2,
            },
        ];
        e.apply_gc_enqueue(&entries).unwrap();
        // Re-apply identical batch (replay).
        e.apply_gc_enqueue(&entries).unwrap();
        assert_eq!(e.gc_queue_len(), 2);
    }

    #[test]
    fn gc_queue_persists_and_reloads_from_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().to_str().unwrap();
        let entries = vec![GcEntry {
            fid: "7,8,9".to_string(),
            enqueued_at: 42,
            reason: 3,
        }];
        {
            let e = KVCacheEngine::new_with_db(64 * 1024 * 1024, 4096, db_path).unwrap();
            e.apply_gc_enqueue(&entries).unwrap();
            assert_eq!(e.gc_queue_len(), 1);
        }
        // Reopen: candidate must be restored from the `gc:` prefix.
        let e2 = KVCacheEngine::new_with_db(64 * 1024 * 1024, 4096, db_path).unwrap();
        assert_eq!(e2.gc_queue_len(), 1);
        let q = e2.gc_queue.read().unwrap();
        assert_eq!(q.front().unwrap(), &entries[0]);
    }

    #[test]
    fn reset_clears_gc_queue_and_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().to_str().unwrap();
        let e = KVCacheEngine::new_with_db(64 * 1024 * 1024, 4096, db_path).unwrap();
        e.apply_gc_enqueue(&[GcEntry {
            fid: "10,11,12".to_string(),
            enqueued_at: 1,
            reason: 0,
        }])
        .unwrap();
        assert_eq!(e.gc_queue_len(), 1);
        e.reset_replicated_state();
        assert_eq!(e.gc_queue_len(), 0);
        // No `gc:` keys remain in the DB.
        if let Some(ref db) = e.db {
            let mut iter = db.iterator(rocksdb::IteratorMode::Start);
            while let Some(Ok((k, _))) = iter.next() {
                assert!(!String::from_utf8_lossy(&k).starts_with("gc:"));
            }
        }
    }

    #[test]
    fn overwrite_enqueues_old_external_fid() {
        let e = engine();
        e.apply_create_namespace("ns", "n", "o", 1000).unwrap();
        e.apply_kv_put("ns", "k", false, b"", "1,2,3", 100, "o", 2000)
            .unwrap();
        e.apply_kv_put("ns", "k", false, b"", "4,5,6", 100, "o", 3000)
            .unwrap();
        assert_eq!(e.gc_queue_len(), 1);
        assert_eq!(e.gc_queue.read().unwrap().front().unwrap().fid, "1,2,3");
        // Re-apply the newer command: slot fid already matches, no new candidate.
        e.apply_kv_put("ns", "k", false, b"", "4,5,6", 100, "o", 3000)
            .unwrap();
        assert_eq!(e.gc_queue_len(), 1);
    }

    #[test]
    fn delete_enqueues_external_but_not_inline() {
        let e = engine();
        e.apply_create_namespace("ns", "n", "o", 1000).unwrap();
        e.apply_kv_put("ns", "big", false, b"", "7,8,9", 10, "o", 2000)
            .unwrap();
        e.apply_kv_put("ns", "small", true, b"x", "", 0, "o", 2000)
            .unwrap();
        e.apply_kv_delete_keys("ns", &["big".to_string(), "small".to_string()])
            .unwrap();
        assert_eq!(e.gc_queue_len(), 1);
        assert_eq!(e.gc_queue.read().unwrap().front().unwrap().fid, "7,8,9");
        // Re-apply delete: slots already gone, no new candidate.
        e.apply_kv_delete_keys("ns", &["big".to_string(), "small".to_string()])
            .unwrap();
        assert_eq!(e.gc_queue_len(), 1);
    }

    #[test]
    fn delete_session_enqueues_block_fids() {
        let e = engine();
        e.apply_create_session("sess", "default", "o", "gpt", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        let data = vec![1u8; 500];
        e.store_leader_block(1, "sess", 0, 8, &data, "10,0,1", 0, PinMode::None)
            .unwrap();
        e.store_leader_block(2, "sess", 0, 8, &data, "10,0,2", 0, PinMode::None)
            .unwrap();
        e.delete_session("sess").unwrap();
        let fids: HashSet<String> = e
            .gc_queue
            .read()
            .unwrap()
            .iter()
            .map(|x| x.fid.clone())
            .collect();
        assert_eq!(fids.len(), 2);
        assert!(fids.contains("10,0,1"));
        assert!(fids.contains("10,0,2"));
    }

    #[test]
    fn referenced_snapshot_includes_live_external_and_blocks() {
        let e = engine();
        e.apply_create_namespace("ns", "n", "o", 1000).unwrap();
        e.apply_kv_put("ns", "ext", false, b"", "3,0,1", 10, "o", 2000)
            .unwrap();
        e.apply_kv_put("ns", "inl", true, b"x", "", 0, "o", 2000)
            .unwrap();
        e.apply_create_session("sess", "default", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        let data = vec![1u8; 500];
        e.store_leader_block(1, "sess", 0, 8, &data, "3,0,9", 0, PinMode::None)
            .unwrap();
        let snap = e.snapshot_referenced_fids();
        assert!(snap.contains("3,0,1"));
        assert!(snap.contains("3,0,9"));
        // No inline-only key, no phantom.
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn due_entries_and_complete_gc_lifecycle() {
        let e = engine();
        e.apply_gc_enqueue(&[GcEntry {
            fid: "a".to_string(),
            enqueued_at: 9000,
            reason: 0,
        }])
        .unwrap();
        e.apply_gc_enqueue(&[GcEntry {
            fid: "b".to_string(),
            enqueued_at: 9500,
            reason: 0,
        }])
        .unwrap();
        // grace=1000: `a` due at now=10000, `b` not due until 10500.
        let due: Vec<String> = e
            .take_due_entries(10000, 1000)
            .iter()
            .map(|x| x.fid.clone())
            .collect();
        assert_eq!(due, vec!["a".to_string()]);
        // take_due_entries must not remove candidates.
        assert_eq!(e.gc_queue_len(), 2);
        e.complete_gc("a");
        assert_eq!(e.gc_queue_len(), 1);
        let due: Vec<String> = e
            .take_due_entries(10500, 1000)
            .iter()
            .map(|x| x.fid.clone())
            .collect();
        assert_eq!(due, vec!["b".to_string()]);
    }

    #[test]
    fn eviction_respects_pin_order() {
        let data = vec![1u8; 500];
        let resident = |e: &KVCacheEngine, id: u64| e.get_block_data(id).is_some();

        // Case 1: Hard stays resident; None is memory-evicted first, then Soft.
        // Memory eviction releases bytes but KEEPS the fid so the block can be
        // transparently re-fetched from the volume.
        let e = engine();
        e.apply_create_session("sess", "default", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        e.store_leader_block(1, "sess", 0, 8, &data, "1,0,1", 0, PinMode::Hard)
            .unwrap();
        e.store_leader_block(2, "sess", 0, 8, &data, "1,0,2", 0, PinMode::Soft)
            .unwrap();
        e.store_leader_block(3, "sess", 0, 8, &data, "1,0,3", 0, PinMode::None)
            .unwrap();
        e.evict_lru(1000).unwrap();
        assert!(resident(&e, 1), "Hard must stay resident");
        assert!(
            !resident(&e, 2) && e.get_fid_by_block_id(2).is_some(),
            "Soft memory-evicted but still fetchable via fid"
        );
        assert!(
            !resident(&e, 3) && e.get_fid_by_block_id(3).is_some(),
            "None memory-evicted but still fetchable via fid"
        );

        // Case 2: only Hard remains resident, no more bytes can be freed.
        assert!(e.evict_lru(500).is_err());

        // Case 3: None memory-evicted before Soft regardless of access time.
        let e2 = engine();
        e2.apply_create_session("sess2", "default", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        e2.store_leader_block(10, "sess2", 0, 8, &data, "1,0,10", 0, PinMode::Soft)
            .unwrap();
        e2.store_leader_block(11, "sess2", 0, 8, &data, "1,0,11", 0, PinMode::None)
            .unwrap();
        e2.evict_lru(500).unwrap();
        assert!(resident(&e2, 10));
        assert!(!resident(&e2, 11) && e2.get_fid_by_block_id(11).is_some());
    }

    #[test]
    fn evict_then_delete_session_does_not_poison_pool() {
        let data_a = vec![7u8; 300];
        let data_b = vec![9u8; 350];

        let e = engine();
        e.apply_create_session("sa", "default", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        e.store_leader_block(1, "sa", 0, 8, &data_a, "1,0,1", 0, PinMode::None)
            .unwrap();
        // Memory-evict A (buffer returned to pool), then delete the session.
        e.evict_lru(data_a.len() as u64).unwrap();
        e.delete_session("sa").unwrap();
        assert_eq!(e.stats().used_memory_bytes, 0, "used memory must be zero");

        // A subsequent store must not receive a poisoned zero-length buffer.
        e.apply_create_session("sb", "default", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        e.store_leader_block(2, "sb", 0, 8, &data_b, "1,0,2", 0, PinMode::None)
            .unwrap();
        let got = e.get_block_data(2).expect("block B readable");
        assert_eq!(got.1.len(), data_b.len());
        assert_eq!(got.1, data_b);
        assert_eq!(e.stats().used_memory_bytes, data_b.len() as u64);
        let _ = e.delete_session("sb");
    }

    #[test]
    fn session_group_eviction_evicts_cold_session_together() {
        let d = vec![5u8; 500];
        let e = engine();
        e.apply_create_session("sA", "default", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        e.apply_create_session("sB", "default", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        // Cold session A: a1 oldest, a2 newer. Warm session B: b1 in between.
        e.store_leader_block(10, "sA", 0, 8, &d, "1,0,10", 0, PinMode::None)
            .unwrap();
        e.store_leader_block(11, "sA", 0, 8, &d, "1,0,11", 1, PinMode::None)
            .unwrap();
        e.store_leader_block(20, "sB", 0, 8, &d, "1,0,20", 0, PinMode::None)
            .unwrap();
        e.set_block_last_accessed(10, 1);
        e.set_block_last_accessed(20, 50);
        e.set_block_last_accessed(11, 100);

        // Need exactly two blocks; single-block LRU would evict a1 then b1 and
        // leave a2. Group policy must evict A's a1,a2 together and keep b1.
        e.evict_lru(2 * d.len() as u64).unwrap();
        assert!(e.get_block_data(10).is_none(), "a1 non-resident");
        assert!(e.get_block_data(11).is_none(), "a2 non-resident");
        assert!(e.get_block_data(20).is_some(), "b1 still resident");
        // Identities retained so reads can re-fetch.
        assert!(e.get_fid_by_block_id(10).is_some());
        assert!(e.get_fid_by_block_id(11).is_some());
        assert_eq!(e.stats().evictions, 2);
        assert_eq!(e.stats().used_memory_bytes, d.len() as u64);
        let _ = e.delete_session("sA");
        let _ = e.delete_session("sB");
    }

    #[test]
    fn group_eviction_keeps_hard_and_none_before_soft() {
        let d = vec![5u8; 500];
        let e = engine();
        e.apply_create_session("sA", "default", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        e.store_leader_block(10, "sA", 0, 8, &d, "1,0,10", 0, PinMode::None)
            .unwrap();
        e.store_leader_block(11, "sA", 0, 8, &d, "1,0,11", 1, PinMode::Soft)
            .unwrap();
        e.store_leader_block(12, "sA", 0, 8, &d, "1,0,12", 2, PinMode::Hard)
            .unwrap();
        e.set_block_last_accessed(10, 1);
        e.set_block_last_accessed(11, 2);
        e.set_block_last_accessed(12, 3);

        // None tier: only the None block goes, even though all share a session.
        e.evict_lru(d.len() as u64).unwrap();
        assert!(e.get_block_data(10).is_none());
        assert!(e.get_block_data(11).is_some());
        assert!(e.get_block_data(12).is_some());
        assert_eq!(e.stats().used_memory_bytes, 2 * d.len() as u64);

        // Soft tier: the Soft block goes next; Hard still resident.
        e.evict_lru(d.len() as u64).unwrap();
        assert!(e.get_block_data(11).is_none());
        assert!(e.get_block_data(12).is_some());
        assert_eq!(e.stats().used_memory_bytes, d.len() as u64);

        // Only Hard remains: nothing evictable -> error, Hard untouched.
        assert!(e.evict_lru(d.len() as u64).is_err());
        assert!(e.get_block_data(12).is_some());
        assert_eq!(e.stats().evictions, 2);
        let _ = e.delete_session("sA");
        assert_eq!(e.stats().used_memory_bytes, 0);
    }

    #[test]
    fn group_evicted_blocks_refetch_and_delete_session_reclaims() {
        let d = vec![6u8; 400];
        let e = engine();
        e.apply_create_session("sC", "default", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        e.store_leader_block(30, "sC", 0, 8, &d, "1,0,30", 0, PinMode::None)
            .unwrap();
        e.store_leader_block(31, "sC", 0, 8, &d, "1,0,31", 1, PinMode::None)
            .unwrap();
        e.evict_lru(2 * d.len() as u64).unwrap();
        // Bytes gone but fid retained -> transparent re-fetch is reachable.
        assert!(e.get_block_data(30).is_none());
        assert!(e.get_block_data(31).is_none());
        assert!(e.get_fid_by_block_id(30).is_some());
        assert!(e.get_fid_by_block_id(31).is_some());
        assert_eq!(e.stats().evictions, 2);

        // Deleting the session of non-resident blocks must not double-count.
        e.delete_session("sC").unwrap();
        assert_eq!(e.stats().evictions, 2);
        assert_eq!(e.stats().used_memory_bytes, 0);
        assert!(e.get_fid_by_block_id(30).is_none());
    }

    #[test]
    fn block_read_count_resident_and_record() {
        let d = vec![1u8; 200];
        let e = engine();
        e.apply_create_session("s", "default", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        e.store_leader_block(1, "s", 0, 8, &d, "1,0,1", 0, PinMode::None)
            .unwrap();
        assert!(e.get_block_data(1).is_some()); // resident read -> 1
        e.record_block_read(1); // successful re-fetch -> 2
        e.record_block_read(999); // missing block -> no-op, no panic
        let (_, blocks) = e.read_heat(0);
        let b = blocks.iter().find(|x| x.block_id == 1).unwrap();
        assert_eq!(b.read_count, 2);
        assert!(b.resident);
        let _ = e.delete_session("s");
    }

    #[test]
    fn read_heat_aggregates_sessions_and_orders() {
        let d = vec![1u8; 200];
        let e = engine();
        e.apply_create_session("sA", "ns1", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        e.apply_create_session("sB", "ns1", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        e.store_leader_block(1, "sA", 0, 8, &d, "1,0,1", 0, PinMode::None)
            .unwrap();
        e.store_leader_block(2, "sA", 0, 8, &d, "1,0,2", 1, PinMode::None)
            .unwrap();
        e.store_leader_block(3, "sB", 0, 8, &d, "1,0,3", 0, PinMode::None)
            .unwrap();
        // reads: block1 x4, block2 x1, block3 x2
        for _ in 0..4 {
            e.record_block_read(1);
        }
        e.record_block_read(2);
        for _ in 0..2 {
            e.record_block_read(3);
        }

        let (sessions, blocks) = e.read_heat(0);
        // Sessions: sA = 4+1 = 5, then sB = 2.
        assert_eq!(sessions[0].session_id, "sA");
        assert_eq!(sessions[0].namespace_id, "ns1");
        assert_eq!(sessions[0].read_count, 5);
        assert_eq!(sessions[0].block_count, 2);
        assert_eq!(sessions[1].session_id, "sB");
        assert_eq!(sessions[1].read_count, 2);
        // Block order by reads: 1(4), 3(2), 2(1).
        assert_eq!(
            blocks.iter().map(|b| b.block_id).collect::<Vec<_>>(),
            vec![1, 3, 2]
        );

        let (_, top2) = e.read_heat(2);
        assert_eq!(
            top2.iter().map(|b| b.block_id).collect::<Vec<_>>(),
            vec![1, 3]
        );
        let _ = e.delete_session("sA");
        let _ = e.delete_session("sB");
    }

    #[test]
    fn eviction_preserves_read_count() {
        let d = vec![1u8; 200];
        let e = engine();
        e.apply_create_session("s", "default", "o", "m", 2, 4, 64, "fp16", 0, "")
            .unwrap();
        e.store_leader_block(1, "s", 0, 8, &d, "1,0,1", 0, PinMode::None)
            .unwrap();
        for _ in 0..3 {
            e.record_block_read(1); // count -> 3
        }
        e.evict_lru(d.len() as u64).unwrap();
        assert!(e.get_block_data(1).is_none()); // non-resident, not counted
        let (_, blocks) = e.read_heat(0);
        assert_eq!(blocks[0].read_count, 3); // retained across eviction
        assert!(!blocks[0].resident);

        e.record_block_read(1); // hot non-resident re-fetch -> 4
        let (_, blocks) = e.read_heat(0);
        assert_eq!(blocks[0].read_count, 4);
        let _ = e.delete_session("s");
    }
}
