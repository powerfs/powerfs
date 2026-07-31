use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

/// Ring buffer of recent latency samples for percentile computation
const MAX_SAMPLES: usize = 1000;

#[derive(Debug, Default, Clone)]
struct LatencySamples {
    reads: VecDeque<u64>,
    writes: VecDeque<u64>,
}

impl LatencySamples {
    fn push_read(&mut self, elapsed_us: u64) {
        if self.reads.len() >= MAX_SAMPLES {
            self.reads.pop_front();
        }
        self.reads.push_back(elapsed_us);
    }

    fn push_write(&mut self, elapsed_us: u64) {
        if self.writes.len() >= MAX_SAMPLES {
            self.writes.pop_front();
        }
        self.writes.push_back(elapsed_us);
    }

    fn compute_percentiles(&self) -> (u64, u64, u64, u64) {
        // Returns (read_p50, read_p99, write_p50, write_p99)
        let r50 = percentile(&self.reads, 0.50);
        let r99 = percentile(&self.reads, 0.99);
        let w50 = percentile(&self.writes, 0.50);
        let w99 = percentile(&self.writes, 0.99);
        (r50, r99, w50, w99)
    }
}

fn percentile(data: &VecDeque<u64>, p: f64) -> u64 {
    if data.is_empty() {
        return 0;
    }
    let mut sorted: Vec<u64> = data.iter().cloned().collect();
    sorted.sort_unstable();
    let idx = (p * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Per-volume I/O statistics collector.
/// Thread-safe, lock-free counters for hot-path read/write ops.
#[derive(Default)]
pub struct IoStatsCollector {
    volumes: RwLock<HashMap<u64, VolumeIoStats>>,
}

#[derive(Debug, Default)]
pub struct VolumeIoStats {
    pub read_ops: AtomicU64,
    pub write_ops: AtomicU64,
    pub read_bytes: AtomicU64,
    pub write_bytes: AtomicU64,
    /// Cumulative read latency in microseconds
    pub read_latency_us: AtomicU64,
    /// Cumulative write latency in microseconds
    pub write_latency_us: AtomicU64,
    /// Sample count for avg computation
    pub read_samples: AtomicU64,
    pub write_samples: AtomicU64,
    /// Recent latency samples for p50/p99 (protected by mutex)
    latency_samples: Mutex<LatencySamples>,
}

impl VolumeIoStats {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Clone for VolumeIoStats {
    fn clone(&self) -> Self {
        Self {
            read_ops: AtomicU64::new(self.read_ops.load(Ordering::Relaxed)),
            write_ops: AtomicU64::new(self.write_ops.load(Ordering::Relaxed)),
            read_bytes: AtomicU64::new(self.read_bytes.load(Ordering::Relaxed)),
            write_bytes: AtomicU64::new(self.write_bytes.load(Ordering::Relaxed)),
            read_latency_us: AtomicU64::new(self.read_latency_us.load(Ordering::Relaxed)),
            write_latency_us: AtomicU64::new(self.write_latency_us.load(Ordering::Relaxed)),
            read_samples: AtomicU64::new(self.read_samples.load(Ordering::Relaxed)),
            write_samples: AtomicU64::new(self.write_samples.load(Ordering::Relaxed)),
            latency_samples: Mutex::new(self.latency_samples.lock().unwrap().clone()),
        }
    }
}

impl IoStatsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ensure a volume entry exists.
    pub fn ensure_volume(&self, volume_id: u64) {
        let mut vols = self.volumes.write().unwrap();
        vols.entry(volume_id).or_default();
    }

    /// Record a read operation with its size and elapsed time.
    pub fn record_read(&self, volume_id: u64, bytes: u64, elapsed_us: u64) {
        self.ensure_volume(volume_id);
        let vols = self.volumes.read().unwrap();
        if let Some(stats) = vols.get(&volume_id) {
            stats.read_ops.fetch_add(1, Ordering::Relaxed);
            stats.read_bytes.fetch_add(bytes, Ordering::Relaxed);
            stats
                .read_latency_us
                .fetch_add(elapsed_us, Ordering::Relaxed);
            stats.read_samples.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut samples) = stats.latency_samples.lock() {
                samples.push_read(elapsed_us);
            }
        }
    }

    /// Record a write operation with its size and elapsed time.
    pub fn record_write(&self, volume_id: u64, bytes: u64, elapsed_us: u64) {
        self.ensure_volume(volume_id);
        let vols = self.volumes.read().unwrap();
        if let Some(stats) = vols.get(&volume_id) {
            stats.write_ops.fetch_add(1, Ordering::Relaxed);
            stats.write_bytes.fetch_add(bytes, Ordering::Relaxed);
            stats
                .write_latency_us
                .fetch_add(elapsed_us, Ordering::Relaxed);
            stats.write_samples.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut samples) = stats.latency_samples.lock() {
                samples.push_write(elapsed_us);
            }
        }
    }

    /// Snapshot all volume stats and return them with computed percentiles.
    pub fn snapshot(&self) -> HashMap<u64, VolumeIoSnapshot> {
        let vols = self.volumes.read().unwrap();
        vols.iter()
            .map(|(id, s)| {
                let read_ops = s.read_ops.load(Ordering::Relaxed);
                let write_ops = s.write_ops.load(Ordering::Relaxed);
                let read_bytes = s.read_bytes.load(Ordering::Relaxed);
                let write_bytes = s.write_bytes.load(Ordering::Relaxed);
                let read_latency = s.read_latency_us.load(Ordering::Relaxed);
                let write_latency = s.write_latency_us.load(Ordering::Relaxed);
                let read_samples = s.read_samples.load(Ordering::Relaxed);
                let write_samples = s.write_samples.load(Ordering::Relaxed);

                let read_avg = read_latency.checked_div(read_samples).unwrap_or(0);
                let write_avg = write_latency.checked_div(write_samples).unwrap_or(0);

                // Compute p50/p99 from recent latency samples
                let (read_p50, read_p99, write_p50, write_p99) = {
                    if let Ok(samples) = s.latency_samples.lock() {
                        samples.compute_percentiles()
                    } else {
                        (0, 0, 0, 0)
                    }
                };

                (
                    *id,
                    VolumeIoSnapshot {
                        volume_id: *id,
                        read_ops,
                        write_ops,
                        read_bytes,
                        write_bytes,
                        read_avg_latency_us: read_avg,
                        write_avg_latency_us: write_avg,
                        read_p50_us: read_p50,
                        read_p99_us: read_p99,
                        write_p50_us: write_p50,
                        write_p99_us: write_p99,
                    },
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct VolumeIoSnapshot {
    pub volume_id: u64,
    pub read_ops: u64,
    pub write_ops: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub read_avg_latency_us: u64,
    pub write_avg_latency_us: u64,
    pub read_p50_us: u64,
    pub read_p99_us: u64,
    pub write_p50_us: u64,
    pub write_p99_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_snapshot() {
        let collector = IoStatsCollector::new();
        let snapshot = collector.snapshot();
        assert!(snapshot.is_empty());
    }

    #[test]
    fn test_record_and_snapshot() {
        let collector = IoStatsCollector::new();
        collector.record_read(1, 1024, 100);
        collector.record_read(1, 2048, 200);
        collector.record_write(1, 512, 50);

        let snapshot = collector.snapshot();
        let vol = snapshot.get(&1).unwrap();
        assert_eq!(vol.read_ops, 2);
        assert_eq!(vol.write_ops, 1);
        assert_eq!(vol.read_bytes, 3072);
        assert_eq!(vol.write_bytes, 512);
        assert_eq!(vol.read_avg_latency_us, 150); // (100+200)/2
        assert_eq!(vol.write_avg_latency_us, 50);
        assert!(vol.read_p50_us > 0);
        assert!(vol.read_p99_us >= vol.read_p50_us);
        assert!(vol.write_p50_us > 0);
    }

    #[test]
    fn test_percentile_calculation() {
        let mut data = VecDeque::new();
        for i in 1..=100 {
            data.push_back(i * 1000); // 1000..100000
        }
        let p50 = percentile(&data, 0.50);
        let p99 = percentile(&data, 0.99);
        assert!(p50 >= 49000 && p50 <= 51000); // around 50000
        assert!(p99 >= 98000); // around 99000
    }

    #[test]
    fn test_empty_percentile() {
        let data: VecDeque<u64> = VecDeque::new();
        assert_eq!(percentile(&data, 0.50), 0);
    }
}
