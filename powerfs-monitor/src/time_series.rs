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
    /// Data points in chronological order
    points: Vec<DataPoint>,
    /// Maximum number of points to retain
    capacity: usize,
}

impl TimeSeries {
    pub fn new(capacity: usize) -> Self {
        Self {
            points: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Add a data point, maintaining order and capacity
    pub fn push(&mut self, timestamp: i64, value: f64) {
        if self.points.len() >= self.capacity {
            self.points.remove(0);
        }
        self.points.push(DataPoint { timestamp, value });
    }

    /// Get all data points
    pub fn points(&self) -> &[DataPoint] {
        &self.points
    }

    /// Get data points within a time range
    pub fn range(&self, start: i64, end: i64) -> Vec<DataPoint> {
        self.points
            .iter()
            .filter(|p| p.timestamp >= start && p.timestamp <= end)
            .cloned()
            .collect()
    }

    /// Linear regression projection: estimate value at a future timestamp
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

    /// Get the latest value
    pub fn latest(&self) -> Option<&DataPoint> {
        self.points.last()
    }
}

/// Time-series store for capacity and I/O metrics
#[derive(Debug, Clone, Default)]
pub struct TimeSeriesStore {
    /// Volume size history: volume_id -> TimeSeries
    volume_size: Arc<RwLock<HashMap<u64, TimeSeries>>>,
    /// Volume I/O history: volume_id -> TimeSeries
    volume_io: Arc<RwLock<HashMap<u64, TimeSeries>>>,
    /// Disk usage history: node_id -> TimeSeries
    disk_usage: Arc<RwLock<HashMap<String, TimeSeries>>>,
    /// Default capacity for each series (24h at 1min intervals = 1440)
    capacity: usize,
}

impl TimeSeriesStore {
    pub fn new() -> Self {
        Self {
            volume_size: Arc::new(RwLock::new(HashMap::new())),
            volume_io: Arc::new(RwLock::new(HashMap::new())),
            disk_usage: Arc::new(RwLock::new(HashMap::new())),
            capacity: 1440,
        }
    }

    /// Record a volume size data point
    pub async fn record_volume_size(&self, volume_id: u64, timestamp: i64, used_bytes: f64) {
        let mut store = self.volume_size.write().await;
        let series = store
            .entry(volume_id)
            .or_insert_with(|| TimeSeries::new(self.capacity));
        series.push(timestamp, used_bytes);
    }

    /// Record a volume I/O data point (ops/sec derived from cumulative counters)
    pub async fn record_volume_io(&self, volume_id: u64, timestamp: i64, ops_per_sec: f64) {
        let mut store = self.volume_io.write().await;
        let series = store
            .entry(volume_id)
            .or_insert_with(|| TimeSeries::new(self.capacity));
        series.push(timestamp, ops_per_sec);
    }

    /// Record a disk usage data point
    pub async fn record_disk_usage(&self, node_id: &str, timestamp: i64, used_percent: f64) {
        let mut store = self.disk_usage.write().await;
        let series = store
            .entry(node_id.to_string())
            .or_insert_with(|| TimeSeries::new(self.capacity));
        series.push(timestamp, used_percent);
    }

    /// Get volume size history
    pub async fn get_volume_size_history(&self, volume_id: u64, minutes: i64) -> Vec<DataPoint> {
        let store = self.volume_size.read().await;
        if let Some(series) = store.get(&volume_id) {
            let now = chrono::Utc::now().timestamp();
            let start = now - minutes * 60;
            series.range(start, now)
        } else {
            Vec::new()
        }
    }

    /// Get disk usage history for a node
    pub async fn get_disk_history(&self, node_id: &str, minutes: i64) -> Vec<DataPoint> {
        let store = self.disk_usage.read().await;
        if let Some(series) = store.get(node_id) {
            let now = chrono::Utc::now().timestamp();
            let start = now - minutes * 60;
            series.range(start, now)
        } else {
            Vec::new()
        }
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
