use super::replica::{KVReplica, ReplicaConfig};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LayerId(pub u32);

#[derive(Debug, Clone)]
pub struct BlockMeta {
    pub block_id: BlockId,
    pub session_id: SessionId,
    pub layer_id: LayerId,
    pub dtype: BlockDtype,
    pub fid: u64,
    pub size: usize,
    pub offset: usize,
    pub checksum: Option<u64>,
    pub ttl: Option<Duration>,
    pub created_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BlockDtype {
    FP32,
    FP16,
    BF16,
    FP8,
    INT8,
    UINT4,
}

impl BlockDtype {
    pub fn size_of(&self) -> usize {
        match self {
            BlockDtype::FP32 => 4,
            BlockDtype::FP16 => 2,
            BlockDtype::BF16 => 2,
            BlockDtype::FP8 => 1,
            BlockDtype::INT8 => 1,
            BlockDtype::UINT4 => 1,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlockData {
    pub meta: BlockMeta,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ParallelLoadResult {
    pub block_id: BlockId,
    pub data: Option<BlockData>,
    pub error: Option<String>,
    pub latency_ms: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LoadPriority {
    High,
    Normal,
    Low,
}

#[derive(Debug, Clone)]
pub struct LoadRequest {
    pub block_ids: Vec<BlockId>,
    pub session_id: SessionId,
    pub priority: LoadPriority,
    pub max_parallelism: usize,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct BlockLoaderConfig {
    pub max_parallel_loads: usize,
    pub max_prefetch_blocks: usize,
    pub prefetch_threshold_ms: u64,
    pub cache_ttl: Duration,
    pub enable_prefetch: bool,
}

impl Default for BlockLoaderConfig {
    fn default() -> Self {
        Self {
            max_parallel_loads: 16,
            max_prefetch_blocks: 100,
            prefetch_threshold_ms: 50,
            cache_ttl: Duration::from_seconds(300),
            enable_prefetch: true,
        }
    }
}

pub struct BlockLoader {
    config: BlockLoaderConfig,
    replica: KVReplica<BlockId, BlockData>,
    local_cache: Arc<RwLock<HashMap<BlockId, (BlockData, Instant)>>>,
    loading_semaphore: Arc<Semaphore>,
    prefetch_queue: Arc<RwLock<HashSet<BlockId>>>,
    session_layers: Arc<RwLock<HashMap<SessionId, Vec<LayerId>>>>,
}

impl BlockLoader {
    pub fn new(replica_id: &str, config: BlockLoaderConfig) -> Self {
        Self {
            config,
            replica: KVReplica::new(replica_id),
            local_cache: Arc::new(RwLock::new(HashMap::new())),
            loading_semaphore: Arc::new(Semaphore::new(config.max_parallel_loads)),
            prefetch_queue: Arc::new(RwLock::new(HashSet::new())),
            session_layers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn load_parallel(&self, request: LoadRequest) -> Vec<ParallelLoadResult> {
        let start = Instant::now();
        let semaphore = Arc::clone(&self.loading_semaphore);
        let max_parallel = std::cmp::min(
            request.max_parallelism,
            self.config.max_parallel_loads,
        );

        let mut handles = Vec::with_capacity(request.block_ids.len());

        for &block_id in &request.block_ids {
            let permit = semaphore.acquire().await.unwrap();
            let replica = &self.replica;
            let local_cache = Arc::clone(&self.local_cache);
            let cache_ttl = self.config.cache_ttl;

            let handle = tokio::spawn(async move {
                let _permit = permit;
                Self::load_single_block(block_id, replica, local_cache, cache_ttl).await
            });
            handles.push(handle);
        }

        let mut results = Vec::with_capacity(handles.len());
        for (i, handle) in handles.into_iter().enumerate() {
            let block_id = request.block_ids[i].clone();
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(ParallelLoadResult {
                    block_id,
                    data: None,
                    error: Some(format!("Task join error: {}", e)),
                    latency_ms: start.elapsed().as_secs_f64() * 1000.0,
                }),
            }
        }

        if self.config.enable_prefetch && request.priority == LoadPriority::High {
            self.schedule_prefetch(&request).await;
        }

        results
    }

    async fn load_single_block(
        block_id: BlockId,
        replica: &KVReplica<BlockId, BlockData>,
        local_cache: Arc<RwLock<HashMap<BlockId, (BlockData, Instant)>>>,
        cache_ttl: Duration,
    ) -> ParallelLoadResult {
        let start = Instant::now();

        if let Some((data, created_at)) = local_cache.read().unwrap().get(&block_id) {
            if created_at.elapsed() < cache_ttl {
                let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                return ParallelLoadResult {
                    block_id,
                    data: Some(data.clone()),
                    error: None,
                    latency_ms,
                };
            }
        }

        match replica.get(&block_id) {
            Some(mut data_list) => {
                if let Some(data) = data_list.pop() {
                    let now = Instant::now();
                    local_cache.write().unwrap().insert(block_id.clone(), (data.clone(), now));

                    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                    ParallelLoadResult {
                        block_id,
                        data: Some(data),
                        error: None,
                        latency_ms,
                    }
                } else {
                    ParallelLoadResult {
                        block_id,
                        data: None,
                        error: Some("No data found in replica".to_string()),
                        latency_ms: start.elapsed().as_secs_f64() * 1000.0,
                    }
                }
            }
            None => ParallelLoadResult {
                block_id,
                data: None,
                error: Some("Block not found".to_string()),
                latency_ms: start.elapsed().as_secs_f64() * 1000.0,
            },
        }
    }

    async fn schedule_prefetch(&self, request: &LoadRequest) {
        let mut queue = self.prefetch_queue.write().unwrap();
        if queue.len() >= self.config.max_prefetch_blocks {
            return;
        }

        let next_blocks = self.predict_next_blocks(request);
        for block_id in next_blocks {
            if !queue.contains(&block_id) {
                queue.insert(block_id);
            }
        }
    }

    fn predict_next_blocks(&self, request: &LoadRequest) -> Vec<BlockId> {
        let mut next_blocks = Vec::new();
        
        let session_layers = self.session_layers.read().unwrap();
        if let Some(layers) = session_layers.get(&request.session_id) {
            for layer_id in layers {
                for offset in 0..8 {
                    let next_block_id = BlockId(format!(
                        "{}-layer{}-offset{}",
                        request.session_id.0, layer_id.0, offset
                    ));
                    if !request.block_ids.contains(&next_block_id) {
                        next_blocks.push(next_block_id);
                    }
                }
            }
        }

        next_blocks
    }

    pub fn store_block(&self, block: BlockData) {
        let now = Instant::now();
        
        self.replica.insert(block.meta.block_id.clone(), block.clone());
        
        let mut local_cache = self.local_cache.write().unwrap();
        local_cache.insert(block.meta.block_id, (block, now));
    }

    pub fn store_blocks(&self, blocks: Vec<BlockData>) {
        let now = Instant::now();
        
        for block in blocks {
            self.replica.insert(block.meta.block_id.clone(), block.clone());
            
            let mut local_cache = self.local_cache.write().unwrap();
            local_cache.insert(block.meta.block_id, (block, now));
        }
    }

    pub fn remove_block(&self, block_id: &BlockId) {
        let mut local_cache = self.local_cache.write().unwrap();
        local_cache.remove(block_id);
    }

    pub fn remove_session_blocks(&self, session_id: &SessionId) {
        let mut local_cache = self.local_cache.write().unwrap();
        local_cache.retain(|block_id, _| !block_id.0.starts_with(&session_id.0));
    }

    pub fn register_session_layers(&self, session_id: SessionId, layers: Vec<LayerId>) {
        let mut session_layers = self.session_layers.write().unwrap();
        session_layers.insert(session_id, layers);
    }

    pub fn unregister_session(&self, session_id: &SessionId) {
        let mut session_layers = self.session_layers.write().unwrap();
        session_layers.remove(session_id);
        self.remove_session_blocks(session_id);
    }

    pub fn cleanup_expired_cache(&self) {
        let mut local_cache = self.local_cache.write().unwrap();
        let now = Instant::now();
        local_cache.retain(|_, (_, created_at)| created_at.elapsed() < self.config.cache_ttl);
    }

    pub fn get_cache_stats(&self) -> CacheStats {
        let local_cache = self.local_cache.read().unwrap();
        let total_size: usize = local_cache.values().map(|(d, _)| d.data.len()).sum();
        let expired_count = local_cache
            .values()
            .filter(|(_, created_at)| created_at.elapsed() >= self.config.cache_ttl)
            .count();

        CacheStats {
            block_count: local_cache.len(),
            total_size_bytes: total_size,
            expired_count,
            loading_semaphore_available: self.loading_semaphore.available_permits(),
            prefetch_queue_size: self.prefetch_queue.read().unwrap().len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub block_count: usize,
    pub total_size_bytes: usize,
    pub expired_count: usize,
    pub loading_semaphore_available: usize,
    pub prefetch_queue_size: usize,
}

impl Default for CacheStats {
    fn default() -> Self {
        Self {
            block_count: 0,
            total_size_bytes: 0,
            expired_count: 0,
            loading_semaphore_available: 0,
            prefetch_queue_size: 0,
        }
    }
}
