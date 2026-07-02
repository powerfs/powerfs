use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::protocol::*;

pub struct IoUringBackend {
    queue_depth: u32,
    next_unique: AtomicU64,
    pending: Mutex<Vec<KernelRequest>>,
    completed: Mutex<Vec<KernelResponse>>,
}

impl IoUringBackend {
    pub fn new(queue_depth: u32) -> Result<Self, String> {
        Ok(Self {
            queue_depth,
            next_unique: AtomicU64::new(1),
            pending: Mutex::new(Vec::new()),
            completed: Mutex::new(Vec::new()),
        })
    }

    pub fn queue_depth(&self) -> u32 {
        self.queue_depth
    }

    pub fn next_unique(&self) -> u64 {
        self.next_unique.fetch_add(1, Ordering::SeqCst)
    }

    pub fn submit_all(&self) -> Result<usize, String> {
        let mut pending = self.pending.lock().unwrap();
        let count = pending.len();
        pending.clear();
        Ok(count)
    }

    pub fn mock_complete(&self, resp: KernelResponse) {
        let mut completed = self.completed.lock().unwrap();
        completed.push(resp);
    }
}

impl KernelBackend for IoUringBackend {
    fn submit_request(&self, req: KernelRequest) -> Result<(), String> {
        let mut pending = self.pending.lock().unwrap();
        if pending.len() >= self.queue_depth as usize {
            return Err("queue full".to_string());
        }
        pending.push(req);
        Ok(())
    }

    fn poll_response(&self) -> Result<Option<KernelResponse>, String> {
        let mut completed = self.completed.lock().unwrap();
        Ok(completed.pop())
    }
}

pub struct AsyncIORing {
    inner: Arc<IoUringBackend>,
}

impl AsyncIORing {
    pub fn new(queue_depth: u32) -> Result<Self, String> {
        Ok(Self {
            inner: Arc::new(IoUringBackend::new(queue_depth)?),
        })
    }

    pub fn backend(&self) -> Arc<IoUringBackend> {
        self.inner.clone()
    }

    pub async fn submit_and_wait(&self, req: KernelRequest) -> Result<KernelResponse, String> {
        let unique = req.unique;
        self.inner.submit_request(req)?;
        self.inner.submit_all()?;

        loop {
            if let Some(resp) = self.inner.poll_response()? {
                if resp.unique == unique {
                    return Ok(resp);
                }
            }
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }
    }
}
