use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A single data point in a time series
#[derive(Debug, Clone, serde::Serialize)]
pub struct DataPoint {
    pub timestamp: i64,
    pub value: f64,
}

/// A time series with a fixed capacity (ring buffer)
#[derive(Debug, Clone)]
pub struct TimeSeries {
    points: Vec<DataPoint>,
    capacity: usize,
}

impl TimeSeries {
    pub fn new(capacity: usize) -> Self {
        Self {
            points: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, timestamp: i64, value: f64) {
        if self.points.len() >= self.capacity {
            self.points.remove(0);
        }
        self.points.push(DataPoint { timestamp, value });
    }

    pub fn points(&self) -> &[DataPoint] {
        &self.points
    }

    pub fn range(&self, start: i64, end: i64) -> Vec<DataPoint> {
        self.points
            .iter()
            .filter(|p| p.timestamp >= start && p.timestamp <= end)
            .cloned()
            .collect()
    }

    pub fn project(&self, future_ts: i64) -> Option<f64> {
        if self.points.len() < 2 {
            return None;
        }

        let first = &self.points[0];
        let last = &self.points[self.points.len() - 1];

        let time_span = (last.timestamp - first.timestamp) as f64;
        if time_span == 0.0 {
            return None;
        }

        let value_span = last.value - first.value;
        let slope = value_span / time_span;

        let elapsed = (future_ts - last.timestamp) as f64;
        Some(last.value + slope * elapsed)
    }

    pub fn latest(&self) -> Option<&DataPoint> {
        self.points.last()
    }

    pub fn len(&self) -> usize {
        self.points.len()
    }
}

#[cfg(feature = "redis")]
/// Redis sorted-set backend for time-series persistence
struct RedisBackend {
    client: redis::Client,
}

#[cfg(feature = "redis")]
impl RedisBackend {
    fn new(redis_url: &str) -> Option<Self> {
        match redis::Client::open(redis_url) {
            Ok(client) => Some(Self { client }),
            Err(e) => {
                log::warn!("TimeSeries Redis backend not available: {}", e);
                None
            }
        }
    }

    fn key(prefix: &str, id: &str) -> String {
        format!("powerfs:ts:{}:{}", prefix, id)
    }

    async fn add_point(&self, prefix: &str, id: &str, timestamp: i64, value: f64) {
        let key = Self::key(prefix, id);
        let member = serde_json::to_string(&value).unwrap_or_default();
        if let Ok(mut conn) = self.client.get_multiplexed_async_connection().await {
            let _: usize = redis::cmd("ZADD")
                .arg(&key)
                .arg(timestamp as f64)
                .arg(&member)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
            let _: bool = redis::cmd("EXPIRE")
                .arg(&key)
                .arg(604800i64)
                .query_async(&mut conn)
                .await
                .unwrap_or(false);
        }
    }

    async fn query_range(&self, prefix: &str, id: &str, start_ts: i64, end_ts: i64) -> Vec<DataPoint> {
        let key = Self::key(prefix, id);
        if let Ok(mut conn) = self.client.get_multiplexed_async_connection().await {
            let results: Vec<(String, f64)> = redis::cmd("ZRANGEBYSCORE")
                .arg(&key)
                .arg(start_ts)
                .arg(end_ts)
                .arg("WITHSCORES")
                .query_async(&mut conn)
                .await
                .unwrap_or_default();

            results
                .into_iter()
                .map(|(member, score)| {
                    let value: f64 = serde_json::from_str(&member).unwrap_or(0.0);
                    DataPoint {
                        timestamp: score as i64,
                        value,
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }
}

/// Time-series store for capacity and I/O metrics
pub struct TimeSeriesStore {
    volume_size: Arc<RwLock<HashMap<u64, TimeSeries>>>,
    volume_io: Arc<RwLock<HashMap<u64, TimeSeries>>>,
    disk_usage: Arc<RwLock<HashMap<String, TimeSeries>>>,
    capacity: usize,
    #[cfg(feature = "redis")]
    redis: Option<Arc<RedisBackend>>,
}

impl std::fmt::Debug for TimeSeriesStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeSeriesStore")
            .field("volume_size_entries", &self.volume_size.try_read().map(|m| m.len()).unwrap_or(0))
            .field("volume_io_entries", &self.volume_io.try_read().map(|m| m.len()).unwrap_or(0))
            .field("disk_usage_entries", &self.disk_usage.try_read().map(|m| m.len()).unwrap_or(0))
            .finish()
    }
}

impl Clone for TimeSeriesStore {
    fn clone(&self) -> Self {
        Self {
            volume_size: self.volume_size.clone(),
            volume_io: self.volume_io.clone(),
            disk_usage: self.disk_usage.clone(),
            capacity: self.capacity,
            #[cfg(feature = "redis")]
            redis: self.redis.clone(),
        }
    }
}

impl Default for TimeSeriesStore {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeSeriesStore {
    pub fn new() -> Self {
        Self {
            volume_size: Arc::new(RwLock::new(HashMap::new())),
            volume_io: Arc::new(RwLock::new(HashMap::new())),
            disk_usage: Arc::new(RwLock::new(HashMap::new())),
            capacity: 1440,
            #[cfg(feature = "redis")]
            redis: None,
        }
    }

    #[cfg(feature = "redis")]
    /// Create with Redis persistence backend
    pub fn with_redis(redis_url: &str) -> Self {
        let redis_backend = RedisBackend::new(redis_url).map(Arc::new);
        if redis_backend.is_some() {
            log::info!("TimeSeriesStore: Redis persistence enabled");
        }
        Self {
            volume_size: Arc::new(RwLock::new(HashMap::new())),
            volume_io: Arc::new(RwLock::new(HashMap::new())),
            disk_usage: Arc::new(RwLock::new(HashMap::new())),
            capacity: 1440,
            redis: redis_backend,
        }
    }

    #[cfg(feature = "redis")]
    /// Load historical data from Redis into in-memory ring buffers
    pub async fn load_from_redis(&self, minutes: i64) {
        if let Some(ref redis) = self.redis {
            log::info!("Loading time-series history from Redis (last {} minutes)", minutes);
            let volumes: Vec<u64> = {
                let store = self.volume_size.read().await;
                store.keys().copied().collect()
            };
            let end = chrono::Utc::now().timestamp();
            let start = end - minutes * 60;
            for vid in &volumes {
                let history = redis.query_range("volume_size", &vid.to_string(), start, end).await;
                if !history.is_empty() {
                    let mut series = TimeSeries::new(self.capacity);
                    for p in &history {
                        series.push(p.timestamp, p.value);
                    }
                    let mut store = self.volume_size.write().await;
                    store.insert(*vid, series);
                }
            }
            log::info!("Time-series history loaded from Redis");
        }
    }

    /// Record a volume size data point
    pub async fn record_volume_size(&self, volume_id: u64, timestamp: i64, used_bytes: f64) {
        #[cfg(feature = "redis")]
        if let Some(ref redis) = self.redis {
            let redis = redis.clone();
            let vid_str = volume_id.to_string();
            redis.add_point("volume_size", &vid_str, timestamp, used_bytes).await;
        }
        let mut store = self.volume_size.write().await;
        let series = store
            .entry(volume_id)
            .or_insert_with(|| TimeSeries::new(self.capacity));
        series.push(timestamp, used_bytes);
    }

    /// Record a volume I/O data point
    pub async fn record_volume_io(&self, volume_id: u64, timestamp: i64, ops_per_sec: f64) {
        #[cfg(feature = "redis")]
        if let Some(ref redis) = self.redis {
            let redis = redis.clone();
            let vid_str = volume_id.to_string();
            redis.add_point("volume_io", &vid_str, timestamp, ops_per_sec).await;
        }
        let mut store = self.volume_io.write().await;
        let series = store
            .entry(volume_id)
            .or_insert_with(|| TimeSeries::new(self.capacity));
        series.push(timestamp, ops_per_sec);
    }

    /// Record a disk usage data point
    pub async fn record_disk_usage(&self, node_id: &str, timestamp: i64, used_percent: f64) {
        #[cfg(feature = "redis")]
        if let Some(ref redis) = self.redis {
            let redis = redis.clone();
            let nid = node_id.to_string();
            redis.add_point("disk_usage", &nid, timestamp, used_percent).await;
        }
        let mut store = self.disk_usage.write().await;
        let series = store
            .entry(node_id.to_string())
            .or_insert_with(|| TimeSeries::new(self.capacity));
        series.push(timestamp, used_percent);
    }

    /// Get volume size history (merges in-memory buffer + Redis)
    pub async fn get_volume_size_history(&self, volume_id: u64, minutes: i64) -> Vec<DataPoint> {
        let store = self.volume_size.read().await;
        let now = chrono::Utc::now().timestamp();
        let start = now - minutes * 60;
        let points = if let Some(series) = store.get(&volume_id) {
            series.range(start, now)
        } else {
            Vec::new()
        };
        drop(store);

        if points.is_empty() {
            #[cfg(feature = "redis")]
            if let Some(ref redis) = self.redis {
                let history = redis
                    .query_range("volume_size", &volume_id.to_string(), start, now)
                    .await;
                if !history.is_empty() {
                    let mut series = TimeSeries::new(self.capacity);
                    for p in &history {
                        series.push(p.timestamp, p.value);
                    }
                    let mut write_store = self.volume_size.write().await;
                    write_store.insert(volume_id, series);
                }
                return history;
            }
        }

        points
    }

    /// Get disk usage history for a node
    pub async fn get_disk_history(&self, node_id: &str, minutes: i64) -> Vec<DataPoint> {
        let store = self.disk_usage.read().await;
        let now = chrono::Utc::now().timestamp();
        let start = now - minutes * 60;
        let points = if let Some(series) = store.get(node_id) {
            series.range(start, now)
        } else {
            Vec::new()
        };
        drop(store);

        if points.is_empty() {
            #[cfg(feature = "redis")]
            if let Some(ref redis) = self.redis {
                let history = redis
                    .query_range("disk_usage", node_id, start, now)
                    .await;
                if !history.is_empty() {
                    let mut series = TimeSeries::new(self.capacity);
                    for p in &history {
                        series.push(p.timestamp, p.value);
                    }
                    let mut write_store = self.disk_usage.write().await;
                    write_store.insert(node_id.to_string(), series);
                }
                return history;
            }
        }

        points
    }

    /// Project volume size growth
    pub async fn project_volume_size(&self, volume_id: u64, hours_ahead: i64) -> Option<f64> {
        let store = self.volume_size.read().await;
        if let Some(series) = store.get(&volume_id) {
            let future_ts = chrono::Utc::now().timestamp() + hours_ahead * 3600;
            series.project(future_ts)
        } else {
            None
        }
    }

    /// Get all tracked volume IDs
    pub async fn tracked_volumes(&self) -> Vec<u64> {
        let store = self.volume_size.read().await;
        store.keys().copied().collect()
    }

    /// Get all tracked node IDs
    pub async fn tracked_nodes(&self) -> Vec<String> {
        let store = self.disk_usage.read().await;
        store.keys().cloned().collect()
    }
}
