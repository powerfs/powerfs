use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

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
    /// Sample count for p50/p99 approximation
    pub read_samples: AtomicU64,
    pub write_samples: AtomicU64,
    /// Last known p50/p99 (updated periodically from a snapshot)
    pub last_read_p50_us: AtomicU64,
    pub last_read_p99_us: AtomicU64,
    pub last_write_p50_us: AtomicU64,
    pub last_write_p99_us: AtomicU64,
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
            last_read_p50_us: AtomicU64::new(self.last_read_p50_us.load(Ordering::Relaxed)),
            last_read_p99_us: AtomicU64::new(self.last_read_p99_us.load(Ordering::Relaxed)),
            last_write_p50_us: AtomicU64::new(self.last_write_p50_us.load(Ordering::Relaxed)),
            last_write_p99_us: AtomicU64::new(self.last_write_p99_us.load(Ordering::Relaxed)),
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
        vols.entry(volume_id)
            .or_insert_with(VolumeIoStats::default);
    }

    /// Record a read operation with its size and elapsed time.
    pub fn record_read(&self, volume_id: u64, bytes: u64, elapsed_us: u64) {
        self.ensure_volume(volume_id);
        let vols = self.volumes.read().unwrap();
        if let Some(stats) = vols.get(&volume_id) {
            stats.read_ops.fetch_add(1, Ordering::Relaxed);
            stats.read_bytes.fetch_add(bytes, Ordering::Relaxed);
            stats.read_latency_us.fetch_add(elapsed_us, Ordering::Relaxed);
            stats.read_samples.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a write operation with its size and elapsed time.
    pub fn record_write(&self, volume_id: u64, bytes: u64, elapsed_us: u64) {
        self.ensure_volume(volume_id);
        let vols = self.volumes.read().unwrap();
        if let Some(stats) = vols.get(&volume_id) {
            stats.write_ops.fetch_add(1, Ordering::Relaxed);
            stats.write_bytes.fetch_add(bytes, Ordering::Relaxed);
            stats.write_latency_us.fetch_add(elapsed_us, Ordering::Relaxed);
            stats.write_samples.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Snapshot all volume stats and return them.
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

                let read_avg = if read_samples > 0 {
                    read_latency / read_samples
                } else {
                    0
                };
                let write_avg = if write_samples > 0 {
                    write_latency / write_samples
                } else {
                    0
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
                        read_p50_us: s.last_read_p50_us.load(Ordering::Relaxed),
                        read_p99_us: s.last_read_p99_us.load(Ordering::Relaxed),
                        write_p50_us: s.last_write_p50_us.load(Ordering::Relaxed),
                        write_p99_us: s.last_write_p99_us.load(Ordering::Relaxed),
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
