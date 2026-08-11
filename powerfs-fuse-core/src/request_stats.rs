//! Request statistics and stuck-request tracking.
//!
//! Provides real-time visibility into in-flight RPC requests for debugging
//! hangs and monitoring performance. Instrumented at the `ShardedRpcPool::submit`
//! chokepoint — every metadata RPC flows through it.
//!
//! # Counters
//!
//! - `total_submitted` / `total_completed` / `total_errors` — global
//! - Per-`MsgType` breakdown: submitted, completed, errors, timeouts,
//!   queue_fulls, circuit_opens, min/max/total latency
//!
//! # In-flight tracking
//!
//! Each `record_start` inserts an entry keyed by a monotonic ID into a
//! `DashMap`. `record_complete` removes it. The admin endpoint can enumerate
//! entries whose `age` exceeds a threshold to identify stuck requests.
//!
//! # Thread safety
//!
//! Counters use `AtomicU64` (lock-free). In-flight tracking uses `DashMap`
//! (sharded, low contention). `snapshot()` takes a consistent point-in-time
//! view for JSON serialization.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use serde::Serialize;

use crate::client_error::ClientError;

/// Per-request-type statistics.
#[derive(Debug, Default, Serialize, Clone)]
pub struct MsgTypeStats {
    pub submitted: u64,
    pub completed: u64,
    pub errors: u64,
    pub timeouts: u64,
    pub queue_fulls: u64,
    pub circuit_opens: u64,
    /// Minimum latency in microseconds (0 means no completed request yet).
    pub min_us: u64,
    /// Maximum latency in microseconds.
    pub max_us: u64,
    /// Sum of all latencies in microseconds (divide by completed for avg).
    pub total_us: u64,
}

/// In-flight request entry (for stuck detection).
#[derive(Debug, Clone, Serialize)]
pub struct InFlightEntry {
    pub msg_type: u16,
    pub msg_type_name: &'static str,
    pub shard_id: u64,
    /// Milliseconds since this request was submitted.
    pub age_ms: u128,
}

/// Point-in-time snapshot of all stats (for JSON serialization).
#[derive(Debug, Serialize)]
pub struct StatsSnapshot {
    pub total_submitted: u64,
    pub total_completed: u64,
    pub total_errors: u64,
    pub in_flight_count: usize,
    pub per_msg_type: HashMap<String, MsgTypeStats>,
    pub in_flight: Vec<InFlightEntry>,
    pub uptime_secs: u64,
}

/// Internal in-flight request record.
#[derive(Debug, Clone)]
struct InFlightRequest {
    msg_type: u16,
    shard_id: u64,
    started_at: Instant,
}

/// Request statistics tracker.
///
/// Thread-safe. Counters are lock-free (`AtomicU64`); in-flight tracking
/// uses `DashMap` for low-contention concurrent insert/remove.
pub struct RequestStats {
    // Global counters (lock-free)
    total_submitted: AtomicU64,
    total_completed: AtomicU64,
    total_errors: AtomicU64,

    // Per-msg_type stats. Keyed by msg_type value (u16).
    // Mutex is fine here — only touched on submit/complete, not on hot path.
    per_msg_type: std::sync::Mutex<HashMap<u16, MsgTypeStats>>,

    // In-flight requests, keyed by a monotonic tracking ID.
    in_flight: DashMap<u64, InFlightRequest>,
    next_id: AtomicU64,

    // Process start time for uptime calculation.
    started_at: Instant,
}

impl Default for RequestStats {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestStats {
    pub fn new() -> Self {
        Self {
            total_submitted: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            per_msg_type: std::sync::Mutex::new(HashMap::new()),
            in_flight: DashMap::new(),
            next_id: AtomicU64::new(1),
            started_at: Instant::now(),
        }
    }

    /// Record request start. Returns a tracking ID to pass to `record_complete`.
    ///
    /// Call this immediately before submitting the request to the RPC pool.
    pub fn record_start(&self, msg_type: u16, shard_id: u64) -> u64 {
        self.total_submitted.fetch_add(1, Ordering::Relaxed);

        {
            let mut per = self.per_msg_type.lock().unwrap();
            let entry = per.entry(msg_type).or_default();
            entry.submitted += 1;
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.in_flight.insert(
            id,
            InFlightRequest {
                msg_type,
                shard_id,
                started_at: Instant::now(),
            },
        );
        id
    }

    /// Record request completion (success or error).
    ///
    /// `id` is the value returned by `record_start`.
    /// `result` is `Ok(())` for success, `Err(&ClientError)` for failure.
    pub fn record_complete(&self, id: u64, result: Result<(), &ClientError>) {
        self.total_completed.fetch_add(1, Ordering::Relaxed);

        let req = self.in_flight.remove(&id).map(|(_, v)| v);

        let mut msg_type = 0u16;
        let mut duration_us = 0u64;
        let mut is_error = false;
        let mut is_timeout = false;
        let mut is_queue_full = false;
        let mut is_circuit_open = false;

        if let Some(req) = &req {
            msg_type = req.msg_type;
            duration_us = req.started_at.elapsed().as_micros() as u64;
        }

        if let Err(e) = result {
            is_error = true;
            self.total_errors.fetch_add(1, Ordering::Relaxed);
            match e {
                ClientError::Timeout(_) => is_timeout = true,
                ClientError::QueueFull(_) => is_queue_full = true,
                ClientError::CircuitOpen => is_circuit_open = true,
                _ => {}
            }
        }

        {
            let mut per = self.per_msg_type.lock().unwrap();
            let entry = per.entry(msg_type).or_default();
            entry.completed += 1;
            if is_error {
                entry.errors += 1;
            }
            if is_timeout {
                entry.timeouts += 1;
            }
            if is_queue_full {
                entry.queue_fulls += 1;
            }
            if is_circuit_open {
                entry.circuit_opens += 1;
            }
            if duration_us > 0 {
                if entry.min_us == 0 || duration_us < entry.min_us {
                    entry.min_us = duration_us;
                }
                if duration_us > entry.max_us {
                    entry.max_us = duration_us;
                }
                entry.total_us += duration_us;
            }
        }
    }

    /// Take a point-in-time snapshot of all statistics.
    pub fn snapshot(&self) -> StatsSnapshot {
        let in_flight_entries: Vec<InFlightEntry> = self
            .in_flight
            .iter()
            .map(|r| InFlightEntry {
                msg_type: r.msg_type,
                msg_type_name: msg_type_name(r.msg_type),
                shard_id: r.shard_id,
                age_ms: r.started_at.elapsed().as_millis(),
            })
            .collect();

        // Sort by age descending (oldest first — most likely stuck)
        let mut sorted = in_flight_entries;
        sorted.sort_by(|a, b| b.age_ms.cmp(&a.age_ms));

        let per_msg_type_raw = self.per_msg_type.lock().unwrap();
        let mut per_msg_type: HashMap<String, MsgTypeStats> = HashMap::new();
        for (k, v) in per_msg_type_raw.iter() {
            per_msg_type.insert(msg_type_name(*k).to_string(), v.clone());
        }

        StatsSnapshot {
            total_submitted: self.total_submitted.load(Ordering::Relaxed),
            total_completed: self.total_completed.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            in_flight_count: self.in_flight.len(),
            per_msg_type,
            in_flight: sorted,
            uptime_secs: self.started_at.elapsed().as_secs(),
        }
    }
}

/// Convert a `MsgType` value (u16) to a human-readable name.
pub fn msg_type_name(msg_type: u16) -> &'static str {
    match msg_type {
        0x0001 => "Ping",
        0x0002 => "Handshake",
        0x0010 => "Lookup",
        0x0011 => "GetAttr",
        0x0012 => "SetAttr",
        0x0013 => "Create",
        0x0014 => "Mkdir",
        0x0015 => "Unlink",
        0x0016 => "Rmdir",
        0x0017 => "Rename",
        0x0018 => "ReadDir",
        0x0019 => "Symlink",
        0x001A => "Readlink",
        0x001B => "Link",
        0x001C => "SetAttrData",
        0x001D => "SetAttrMeta",
        0x0030 => "PushDelta",
        0x0031 => "PullDelta",
        0x0032 => "Invalidate",
        0x0033 => "AllocInodeBatch",
        0x0034 => "UpdateInodeSizeChunks",
        0x0035 => "OpenCountInc",
        0x0036 => "OpenCountDec",
        0x0037 => "MigrateInlineAlloc",
        0x0038 => "SetXattr",
        0x0039 => "GetXattr",
        0x003a => "RemoveXattr",
        0x003b => "ListXattr",
        0x0040 => "StatFs",
        0x0050 => "Assign",
        0x0051 => "LookupVolume",
        0x0052 => "Heartbeat",
        0x0053 => "KeepConnected",
        0x0054 => "VolumeList",
        0x0060 => "CreateVolume",
        0x0061 => "DeleteVolume",
        0x0062 => "WriteNeedle",
        0x0063 => "ReadNeedle",
        0x0064 => "DeleteNeedle",
        0x0065 => "BatchWriteNeedle",
        0x0066 => "ReadNeedleBlob",
        0x0067 => "RangeLease",
        0x0068 => "VolumeStatus",
        _ => "Unknown",
    }
}

/// Shared request stats handle (convenience alias).
pub type SharedRequestStats = Arc<RequestStats>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_basic_counters() {
        let stats = RequestStats::new();

        let id1 = stats.record_start(0x0010, 0); // Lookup
        let id2 = stats.record_start(0x0013, 1); // Create

        let snap = stats.snapshot();
        assert_eq!(snap.total_submitted, 2);
        assert_eq!(snap.in_flight_count, 2);

        stats.record_complete(id1, Ok(()));
        stats.record_complete(id2, Err(&ClientError::Timeout(std::time::Duration::from_secs(5))));

        let snap = stats.snapshot();
        assert_eq!(snap.total_submitted, 2);
        assert_eq!(snap.total_completed, 2);
        assert_eq!(snap.total_errors, 1);
        assert_eq!(snap.in_flight_count, 0);

        let lookup = snap.per_msg_type.get("Lookup").unwrap();
        assert_eq!(lookup.submitted, 1);
        assert_eq!(lookup.completed, 1);
        assert_eq!(lookup.errors, 0);

        let create = snap.per_msg_type.get("Create").unwrap();
        assert_eq!(create.submitted, 1);
        assert_eq!(create.completed, 1);
        assert_eq!(create.errors, 1);
        assert_eq!(create.timeouts, 1);
    }

    #[test]
    fn test_in_flight_age() {
        let stats = RequestStats::new();
        let id = stats.record_start(0x0018, 5); // ReadDir

        std::thread::sleep(std::time::Duration::from_millis(50));

        let snap = stats.snapshot();
        assert_eq!(snap.in_flight.len(), 1);
        let entry = &snap.in_flight[0];
        assert_eq!(entry.msg_type_name, "ReadDir");
        assert_eq!(entry.shard_id, 5);
        assert!(entry.age_ms >= 40, "age_ms={} should be >= 40", entry.age_ms);

        stats.record_complete(id, Ok(()));
    }

    #[test]
    fn test_concurrent_access() {
        let stats = Arc::new(RequestStats::new());
        let mut handles = vec![];

        for t in 0..4 {
            let s = stats.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let id = s.record_start(0x0010, t * 100 + i);
                    s.record_complete(id, Ok(()));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let snap = stats.snapshot();
        assert_eq!(snap.total_submitted, 400);
        assert_eq!(snap.total_completed, 400);
        assert_eq!(snap.in_flight_count, 0);
    }

    #[test]
    fn test_latency_tracking() {
        let stats = RequestStats::new();

        let id1 = stats.record_start(0x0011, 0); // GetAttr
        std::thread::sleep(std::time::Duration::from_millis(10));
        stats.record_complete(id1, Ok(()));

        let id2 = stats.record_start(0x0011, 0);
        std::thread::sleep(std::time::Duration::from_millis(30));
        stats.record_complete(id2, Ok(()));

        let snap = stats.snapshot();
        let getattr = snap.per_msg_type.get("GetAttr").unwrap();
        assert_eq!(getattr.completed, 2);
        assert!(getattr.min_us > 0);
        assert!(getattr.max_us > getattr.min_us);
        assert!(getattr.total_us >= getattr.max_us);
    }

    #[test]
    fn test_msg_type_name() {
        assert_eq!(msg_type_name(0x0010), "Lookup");
        assert_eq!(msg_type_name(0x0013), "Create");
        assert_eq!(msg_type_name(0x0018), "ReadDir");
        assert_eq!(msg_type_name(0xFFFF), "Unknown");
    }
}
