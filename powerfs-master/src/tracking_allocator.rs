use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicU64, Ordering};
use tikv_jemallocator::Jemalloc;

pub struct AllocStats {
    pub alloc_bytes: AtomicU64,
    pub alloc_count: AtomicU64,
    pub dealloc_bytes: AtomicU64,
    pub dealloc_count: AtomicU64,
}

impl AllocStats {
    pub const fn new() -> Self {
        Self {
            alloc_bytes: AtomicU64::new(0),
            alloc_count: AtomicU64::new(0),
            dealloc_bytes: AtomicU64::new(0),
            dealloc_count: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> AllocSnapshot {
        AllocSnapshot {
            alloc_bytes: self.alloc_bytes.load(Ordering::Relaxed),
            alloc_count: self.alloc_count.load(Ordering::Relaxed),
            dealloc_bytes: self.dealloc_bytes.load(Ordering::Relaxed),
            dealloc_count: self.dealloc_count.load(Ordering::Relaxed),
        }
    }
}

impl Default for AllocStats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AllocSnapshot {
    pub alloc_bytes: u64,
    pub alloc_count: u64,
    pub dealloc_bytes: u64,
    pub dealloc_count: u64,
}

impl AllocSnapshot {
    pub fn live_bytes(&self) -> u64 {
        self.alloc_bytes.saturating_sub(self.dealloc_bytes)
    }

    pub fn live_count(&self) -> u64 {
        self.alloc_count.saturating_sub(self.dealloc_count)
    }
}

pub static ALLOC_STATS: AllocStats = AllocStats::new();

pub struct TrackingAllocator {
    pub inner: Jemalloc,
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc(layout);
        if !ptr.is_null() {
            ALLOC_STATS
                .alloc_bytes
                .fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_STATS.alloc_count.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ALLOC_STATS
            .dealloc_bytes
            .fetch_add(layout.size() as u64, Ordering::Relaxed);
        ALLOC_STATS.dealloc_count.fetch_add(1, Ordering::Relaxed);
        self.inner.dealloc(ptr, layout);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc_zeroed(layout);
        if !ptr.is_null() {
            ALLOC_STATS
                .alloc_bytes
                .fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_STATS.alloc_count.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_STATS
            .dealloc_bytes
            .fetch_add(layout.size() as u64, Ordering::Relaxed);
        ALLOC_STATS.dealloc_count.fetch_add(1, Ordering::Relaxed);
        let new_ptr = self.inner.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            ALLOC_STATS
                .alloc_bytes
                .fetch_add(new_size as u64, Ordering::Relaxed);
            ALLOC_STATS.alloc_count.fetch_add(1, Ordering::Relaxed);
        }
        new_ptr
    }
}

pub fn read_self_vm() -> Option<(u64, u64, u64)> {
    let content = std::fs::read_to_string("/proc/self/status").ok()?;
    let mut rss = None;
    let mut data = None;
    let mut peak = None;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            rss = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<u64>().ok());
        } else if let Some(rest) = line.strip_prefix("VmData:") {
            data = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<u64>().ok());
        } else if let Some(rest) = line.strip_prefix("VmPeak:") {
            peak = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<u64>().ok());
        }
    }
    Some((rss?, data.unwrap_or(0), peak.unwrap_or(0)))
}

pub fn read_jemalloc_stats() -> Option<(u64, u64, u64, u64)> {
    None
}
