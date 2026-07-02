use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StripePattern {
    #[default]
    RoundRobin,
    Raid0,
}

#[derive(Debug, Clone)]
pub struct StripeConfig {
    pub stripe_size: u64,
    pub num_stripes: u32,
    pub pattern: StripePattern,
}

impl Default for StripeConfig {
    fn default() -> Self {
        Self {
            stripe_size: 64 * 1024 * 1024,
            num_stripes: 4,
            pattern: StripePattern::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StripeInfo {
    pub config: StripeConfig,
    pub volume_ids: Vec<u32>,
    pub total_size: u64,
}

pub struct StripeEngine {
    config: StripeConfig,
}

impl StripeEngine {
    pub fn new(config: StripeConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &StripeConfig {
        &self.config
    }

    pub fn get_stripe_index(&self, offset: u64) -> u32 {
        let stripe_num = offset / self.config.stripe_size;
        (stripe_num % self.config.num_stripes as u64) as u32
    }

    pub fn get_stripe_offset(&self, offset: u64) -> u64 {
        offset % self.config.stripe_size
    }

    pub fn get_stripe_range(&self, offset: u64, length: u64) -> Vec<(u32, u64, u64)> {
        let mut ranges = Vec::new();
        let mut remaining = length;
        let mut current_offset = offset;

        while remaining > 0 {
            let stripe_idx = self.get_stripe_index(current_offset);
            let stripe_offset = self.get_stripe_offset(current_offset);
            let bytes_in_stripe = std::cmp::min(remaining, self.config.stripe_size - stripe_offset);

            ranges.push((stripe_idx, stripe_offset, bytes_in_stripe));

            current_offset += bytes_in_stripe;
            remaining -= bytes_in_stripe;
        }

        ranges
    }

    pub fn calculate_stripes_needed(&self, total_size: u64) -> u64 {
        if total_size == 0 {
            return 0;
        }
        total_size.div_ceil(self.config.stripe_size)
    }

    pub fn create_stripe_layout(&self, volume_ids: Vec<u32>, total_size: u64) -> StripeInfo {
        let num = std::cmp::min(volume_ids.len() as u32, self.config.num_stripes);
        StripeInfo {
            config: StripeConfig {
                num_stripes: num,
                ..self.config
            },
            volume_ids: volume_ids.into_iter().take(num as usize).collect(),
            total_size,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct LockRange {
    pub start: u64,
    pub end: u64,
    pub exclusive: bool,
    pub owner: u64,
}

pub struct LockManager {
    locks: std::sync::Mutex<std::collections::HashMap<u64, Vec<LockRange>>>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn try_lock(&self, inode: u64, range: LockRange) -> bool {
        let mut locks = self.locks.lock().unwrap();
        let ranges = locks.entry(inode).or_default();

        for existing in ranges.iter() {
            if ranges_overlap(existing, &range)
                && (existing.exclusive || range.exclusive)
                && existing.owner != range.owner
            {
                return false;
            }
        }

        ranges.push(range);
        true
    }

    pub fn unlock(&self, inode: u64, owner: u64) {
        let mut locks = self.locks.lock().unwrap();
        if let Some(ranges) = locks.get_mut(&inode) {
            ranges.retain(|r| r.owner != owner);
        }
    }

    pub fn unlock_range(&self, inode: u64, owner: u64, start: u64, end: u64) {
        let mut locks = self.locks.lock().unwrap();
        if let Some(ranges) = locks.get_mut(&inode) {
            ranges.retain(|r| !(r.owner == owner && r.start == start && r.end == end));
        }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

fn ranges_overlap(a: &LockRange, b: &LockRange) -> bool {
    a.start < b.end && b.start < a.end
}

pub struct QoSScheduler {
    tokens: std::sync::Mutex<u64>,
    rate_per_sec: u64,
    last_refill: std::sync::Mutex<std::time::Instant>,
}

impl QoSScheduler {
    pub fn new(rate_per_sec: u64) -> Self {
        Self {
            tokens: std::sync::Mutex::new(rate_per_sec),
            rate_per_sec,
            last_refill: std::sync::Mutex::new(std::time::Instant::now()),
        }
    }

    pub fn try_request(&self, bytes: u64) -> bool {
        let mut tokens = self.tokens.lock().unwrap();
        let mut last_refill = self.last_refill.lock().unwrap();

        let now = std::time::Instant::now();
        let elapsed = now.duration_since(*last_refill);
        if elapsed.as_secs() > 0 {
            *tokens = self.rate_per_sec;
            *last_refill = now;
        }

        if *tokens >= bytes {
            *tokens -= bytes;
            true
        } else {
            false
        }
    }

    pub fn set_rate(&self, rate_per_sec: u64) {
        let mut tokens = self.tokens.lock().unwrap();
        *tokens = rate_per_sec;
        *self.last_refill.lock().unwrap() = std::time::Instant::now();
    }
}

pub struct MetadataShard {
    pub shard_id: u32,
    pub range_start: u64,
    pub range_end: u64,
}

pub struct MetadataShardRouter {
    shards: Vec<MetadataShard>,
}

impl MetadataShardRouter {
    pub fn new(num_shards: u32, total_inodes: u64) -> Self {
        let mut shards = Vec::with_capacity(num_shards as usize);
        let range_size = total_inodes / num_shards as u64;

        for i in 0..num_shards {
            let start = i as u64 * range_size;
            let end = if i == num_shards - 1 {
                total_inodes
            } else {
                (i + 1) as u64 * range_size
            };
            shards.push(MetadataShard {
                shard_id: i,
                range_start: start,
                range_end: end,
            });
        }

        Self { shards }
    }

    pub fn get_shard(&self, inode: u64) -> u32 {
        for shard in &self.shards {
            if inode >= shard.range_start && inode < shard.range_end {
                return shard.shard_id;
            }
        }
        self.shards.last().map(|s| s.shard_id).unwrap_or(0)
    }

    pub fn get_shard_by_path_hash(&self, path: &str) -> u32 {
        let hash = fxhash(path);
        (hash % self.shards.len() as u64) as u32
    }

    pub fn num_shards(&self) -> u32 {
        self.shards.len() as u32
    }
}

fn fxhash(data: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in data.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub struct HPCConfig {
    pub stripe_config: StripeConfig,
    pub num_metadata_shards: u32,
    pub total_inodes: u64,
    pub io_rate_limit_per_sec: u64,
}

impl Default for HPCConfig {
    fn default() -> Self {
        Self {
            stripe_config: StripeConfig::default(),
            num_metadata_shards: 4,
            total_inodes: 10_000_000,
            io_rate_limit_per_sec: u64::MAX,
        }
    }
}

pub struct HPCEngine {
    pub config: HPCConfig,
    pub stripe_engine: Arc<StripeEngine>,
    pub lock_manager: Arc<LockManager>,
    pub qos: Arc<QoSScheduler>,
    pub shard_router: Arc<MetadataShardRouter>,
}

impl HPCEngine {
    pub fn new(config: HPCConfig) -> Self {
        let stripe_engine = Arc::new(StripeEngine::new(config.stripe_config.clone()));
        let lock_manager = Arc::new(LockManager::new());
        let qos = Arc::new(QoSScheduler::new(config.io_rate_limit_per_sec));
        let shard_router = Arc::new(MetadataShardRouter::new(
            config.num_metadata_shards,
            config.total_inodes,
        ));

        Self {
            config,
            stripe_engine,
            lock_manager,
            qos,
            shard_router,
        }
    }
}
