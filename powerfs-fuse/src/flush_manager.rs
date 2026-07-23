use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel::{unbounded, Receiver, Sender};
use log::{debug, warn};

pub struct FlushConfig {
    pub per_file_threshold: usize,
    pub global_threshold: usize,
    pub max_dirty_age: Duration,
    pub backpressure_ratio: f64,
    pub worker_count: usize,
    pub scan_interval: Duration,
    pub max_retries: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for FlushConfig {
    fn default() -> Self {
        FlushConfig {
            per_file_threshold: 16 * 1024 * 1024,
            global_threshold: 256 * 1024 * 1024,
            max_dirty_age: Duration::from_secs(10),
            backpressure_ratio: 0.8,
            worker_count: num_cpus::get(),
            scan_interval: Duration::from_secs(2),
            max_retries: 5,
            initial_backoff_ms: 2000,
            max_backoff_ms: 60000,
        }
    }
}

pub struct DirtyInodeInfo {
    pub dirty_bytes: usize,
    pub first_dirty_at: Instant,
    pub retry_count: u32,
    pub last_retry_at: Option<Instant>,
}

pub enum FlushCommand {
    FlushInode(u64),
    Shutdown,
}

pub struct FlushManager {
    config: FlushConfig,
    inode_dirty: Arc<RwLock<HashMap<u64, DirtyInodeInfo>>>,
    global_dirty_bytes: Arc<AtomicUsize>,
    cache_max_bytes: usize,
    command_txs: Vec<Sender<FlushCommand>>,
    worker_handles: Mutex<Vec<thread::JoinHandle<()>>>,
    scan_handle: Mutex<Option<thread::JoinHandle<()>>>,
    running: Arc<AtomicBool>,
    flush_fn: Arc<dyn Fn(u64) -> Result<(), String> + Send + Sync + 'static>,
}

impl FlushManager {
    pub fn new<F>(config: FlushConfig, cache_max_bytes: usize, flush_fn: F) -> Arc<Self>
    where
        F: Fn(u64) -> Result<(), String> + Send + Sync + 'static,
    {
        let worker_count = config.worker_count.max(1);
        let mut command_txs = Vec::with_capacity(worker_count);
        let mut command_rxs = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let (tx, rx) = unbounded();
            command_txs.push(tx);
            command_rxs.push(rx);
        }

        let running = Arc::new(AtomicBool::new(true));

        let mgr = Arc::new(FlushManager {
            config,
            inode_dirty: Arc::new(RwLock::new(HashMap::new())),
            global_dirty_bytes: Arc::new(AtomicUsize::new(0)),
            cache_max_bytes,
            command_txs,
            worker_handles: Mutex::new(Vec::new()),
            scan_handle: Mutex::new(None),
            running,
            flush_fn: Arc::new(flush_fn),
        });

        mgr.start_workers(command_rxs);
        mgr.start_scan_loop();

        mgr
    }

    fn route_worker(&self, inode: u64) -> usize {
        (inode % self.config.worker_count.max(1) as u64) as usize
    }

    fn start_workers(self: &Arc<Self>, command_rxs: Vec<Receiver<FlushCommand>>) {
        let mut handles = self.worker_handles.lock().unwrap();

        for (i, rx) in command_rxs.into_iter().enumerate() {
            let mgr = Arc::clone(self);
            let handle = thread::Builder::new()
                .name(format!("flush-worker-{}", i))
                .spawn(move || {
                    debug!("flush_worker_{}: started", i);
                    while mgr.running.load(Ordering::SeqCst) {
                        match rx.recv_timeout(Duration::from_millis(500)) {
                            Ok(FlushCommand::FlushInode(inode)) => {
                                mgr.do_flush_inode(inode);
                            }
                            Ok(FlushCommand::Shutdown) => {
                                debug!("flush_worker_{}: shutdown", i);
                                break;
                            }
                            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                                continue;
                            }
                            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                                break;
                            }
                        }
                    }
                    debug!("flush_worker_{}: exited", i);
                })
                .expect("failed to spawn flush worker");
            handles.push(handle);
        }
    }

    fn start_scan_loop(self: &Arc<Self>) {
        let mgr = Arc::clone(self);
        let handle = thread::Builder::new()
            .name("flush-scanner".to_string())
            .spawn(move || {
                debug!("flush_scanner: started");
                while mgr.running.load(Ordering::SeqCst) {
                    thread::sleep(mgr.config.scan_interval);
                    if !mgr.running.load(Ordering::SeqCst) {
                        break;
                    }
                    mgr.scan_and_schedule();
                }
                debug!("flush_scanner: exited");
            })
            .expect("failed to spawn flush scanner");

        *self.scan_handle.lock().unwrap() = Some(handle);
    }

    fn scan_and_schedule(&self) {
        let global_dirty = self.global_dirty_bytes.load(Ordering::Relaxed);

        if global_dirty >= self.config.global_threshold {
            let oldest = self.find_oldest_dirty(self.config.worker_count.max(1));
            for inode in oldest {
                let worker_idx = self.route_worker(inode);
                let _ = self.command_txs[worker_idx]
                    .send_timeout(FlushCommand::FlushInode(inode), Duration::from_secs(2));
            }
        }

        let expired = self.find_expired_dirty(self.config.max_dirty_age);
        for inode in expired {
            let worker_idx = self.route_worker(inode);
            let _ = self.command_txs[worker_idx]
                .send_timeout(FlushCommand::FlushInode(inode), Duration::from_secs(2));
        }
    }

    pub fn track_dirty(&self, inode: u64, bytes: usize) -> usize {
        let mut dirty_map = self.inode_dirty.write().unwrap();
        let info = dirty_map.entry(inode).or_insert_with(|| DirtyInodeInfo {
            dirty_bytes: 0,
            first_dirty_at: Instant::now(),
            retry_count: 0,
            last_retry_at: None,
        });
        info.dirty_bytes += bytes;
        let current = info.dirty_bytes;
        drop(dirty_map);

        self.global_dirty_bytes.fetch_add(bytes, Ordering::Relaxed);

        if current >= self.config.per_file_threshold {
            let worker_idx = self.route_worker(inode);
            let _ = self.command_txs[worker_idx]
                .send_timeout(FlushCommand::FlushInode(inode), Duration::from_secs(2));
        }

        current
    }

    pub fn clear_dirty(&self, inode: u64) {
        let cleared = {
            let mut dirty_map = self.inode_dirty.write().unwrap();
            dirty_map
                .remove(&inode)
                .map(|info| info.dirty_bytes)
                .unwrap_or(0)
        };
        if cleared > 0 {
            self.global_dirty_bytes
                .fetch_sub(cleared, Ordering::Relaxed);
        }
    }

    pub fn global_dirty_bytes(&self) -> usize {
        self.global_dirty_bytes.load(Ordering::Relaxed)
    }

    pub fn inode_dirty_bytes(&self, inode: u64) -> usize {
        self.inode_dirty
            .read()
            .unwrap()
            .get(&inode)
            .map(|info| info.dirty_bytes)
            .unwrap_or(0)
    }

    pub fn is_backpressured(&self) -> bool {
        if self.cache_max_bytes == 0 {
            return false;
        }
        let dirty = self.global_dirty_bytes.load(Ordering::Relaxed) as f64;
        dirty >= (self.cache_max_bytes as f64 * self.config.backpressure_ratio)
    }

    pub fn notify_release(&self, inode: u64) {
        let dirty_bytes = self.inode_dirty_bytes(inode);
        if dirty_bytes > 0 {
            let worker_idx = self.route_worker(inode);
            let _ = self.command_txs[worker_idx]
                .send_timeout(FlushCommand::FlushInode(inode), Duration::from_secs(5));
        }
    }

    fn find_oldest_dirty(&self, count: usize) -> Vec<u64> {
        let dirty_map = self.inode_dirty.read().unwrap();
        let mut entries: Vec<(u64, Instant)> = dirty_map
            .iter()
            .map(|(inode, info)| (*inode, info.first_dirty_at))
            .collect();
        entries.sort_by_key(|(_, t)| *t);
        entries
            .into_iter()
            .take(count)
            .map(|(inode, _)| inode)
            .collect()
    }

    fn find_expired_dirty(&self, max_age: Duration) -> Vec<u64> {
        let now = Instant::now();
        let dirty_map = self.inode_dirty.read().unwrap();
        dirty_map
            .iter()
            .filter(|(_, info)| now.duration_since(info.first_dirty_at) >= max_age)
            .map(|(inode, _)| *inode)
            .collect()
    }

    fn do_flush_inode(&self, inode: u64) {
        let dirty_bytes = self.inode_dirty_bytes(inode);
        if dirty_bytes == 0 {
            return;
        }

        let (retry_count, last_retry_at) = {
            let dirty_map = self.inode_dirty.read().unwrap();
            if let Some(info) = dirty_map.get(&inode) {
                (info.retry_count, info.last_retry_at)
            } else {
                (0, None)
            }
        };

        if retry_count >= self.config.max_retries {
            warn!(
                "flush_manager: flush inode={} reached max retries ({}/{}), skipping",
                inode, retry_count, self.config.max_retries
            );
            return;
        }

        if let Some(last_at) = last_retry_at {
            let backoff = self.calculate_backoff(retry_count);
            if Instant::now().duration_since(last_at) < backoff {
                debug!(
                    "flush_manager: flush inode={} waiting for backoff {:?}, retry={}",
                    inode, backoff, retry_count
                );
                return;
            }
        }

        match (self.flush_fn)(inode) {
            Ok(_) => {
                self.clear_dirty(inode);
                debug!(
                    "flush_manager: flushed inode={}, bytes={}",
                    inode, dirty_bytes
                );
            }
            Err(e) => {
                warn!(
                    "flush_manager: flush inode={} failed (retry {}): {}",
                    inode, retry_count, e
                );
                self.increment_retry_count(inode);
            }
        }
    }

    fn calculate_backoff(&self, retry_count: u32) -> Duration {
        let base = self.config.initial_backoff_ms;
        let max = self.config.max_backoff_ms;
        let backoff = base * (2u64.pow(retry_count));
        Duration::from_millis(backoff.min(max))
    }

    fn increment_retry_count(&self, inode: u64) {
        let mut dirty_map = self.inode_dirty.write().unwrap();
        if let Some(info) = dirty_map.get_mut(&inode) {
            info.retry_count += 1;
            info.last_retry_at = Some(Instant::now());
        }
    }

    pub fn wait_for_backpressure_relief(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        while self.is_backpressured() {
            if start.elapsed() >= timeout {
                return !self.is_backpressured();
            }
            thread::sleep(Duration::from_millis(50));
        }
        true
    }

    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        for tx in &self.command_txs {
            let _ = tx.try_send(FlushCommand::Shutdown);
        }
    }
}

impl Drop for FlushManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}
