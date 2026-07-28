//! Request context for server-side request tracking
//!
//! Provides per-request context with trace_id, client info, timing,
//! and metadata for observability and debugging.

use std::net::SocketAddr;
use std::time::Instant;

use crate::protocol::{ClientType, MsgType, NetMessage};

/// Unique trace identifier for end-to-end request tracking
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TraceId(pub String);

impl TraceId {
    pub fn new() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let counter = rand_nanos();
        Self(format!("{:016x}{:016x}", nanos, counter))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

fn rand_nanos() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Client session information captured at handshake
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub client_id: u64,
    pub client_type: ClientType,
    pub address: SocketAddr,
}

/// RequestContext - per-request context for server-side processing
///
/// Created at the start of request processing, carries trace_id,
/// client info, timing, and allows middleware to attach metadata.
pub struct RequestContext {
    pub trace_id: TraceId,
    pub client: ClientInfo,
    pub msg_type: MsgType,
    pub seq: u32,
    pub start_time: Instant,
    pub latency_ms: Option<u64>,
    pub metadata: std::collections::HashMap<String, String>,
}

impl RequestContext {
    pub fn new(client: &ClientInfo, msg: &NetMessage) -> Self {
        let msg_type = msg.msg_type().unwrap_or(MsgType::Ping);
        Self {
            trace_id: TraceId::new(),
            client: client.clone(),
            msg_type,
            seq: msg.header.seq,
            start_time: Instant::now(),
            latency_ms: None,
            metadata: std::collections::HashMap::new(),
        }
    }

    pub fn with_trace_id(mut self, trace_id: TraceId) -> Self {
        self.trace_id = trace_id;
        self
    }

    pub fn trace_id(&self) -> &str {
        self.trace_id.as_str()
    }

    pub fn msg_type_name(&self) -> String {
        format!("{:?}", self.msg_type)
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.start_time.elapsed().as_millis() as u64
    }

    pub fn set_elapsed(&mut self) {
        self.latency_ms = Some(self.elapsed_ms());
    }

    pub fn latency(&self) -> Option<u64> {
        self.latency_ms
    }

    pub fn is_error(&self, status: u16) -> bool {
        status != crate::STATUS_OK
    }

    pub fn set_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }

    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }
}
