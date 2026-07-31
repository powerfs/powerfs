//! Resilient gRPC client for the Master service.
//!
//! Maintains connections to all configured master endpoints and
//! automatically fails over when the current endpoint is unavailable
//! or reports it is not the Raft leader.  The leader hint extracted
//! from "not leader" error messages is cached so subsequent calls
//! go directly to the known leader without probing.
//!
//! This module lives in the `powerfs-master` crate so that every
//! downstream client (monitor, filer, volume server, CLI, KV client)
//! can share the same leader-discovery logic instead of each
//! reimplementing its own.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;

use crate::proto::powerfs::master_service_client::MasterServiceClient;

pub struct ResilientMasterClient {
    /// All configured master gRPC endpoints (e.g. "http://172.30.0.11:9333").
    endpoints: Vec<String>,
    /// Lazily-created gRPC channels keyed by endpoint.
    channels: RwLock<HashMap<String, Channel>>,
    /// Index into `endpoints` for the current preferred endpoint.
    current: RwLock<usize>,
    /// Cached leader endpoint if known from a previous "not leader" hint.
    leader: RwLock<Option<String>>,
}

impl ResilientMasterClient {
    /// Create a new resilient client.  At least one endpoint must be
    /// provided; endpoints should already include the `http://` scheme.
    pub fn new(mut endpoints: Vec<String>) -> Result<Self, String> {
        // Normalise endpoints to include the http:// scheme.
        for ep in &mut endpoints {
            if !ep.starts_with("http://") && !ep.starts_with("https://") {
                *ep = format!("http://{}", ep);
            }
        }
        if endpoints.is_empty() {
            return Err("at least one master endpoint is required".to_string());
        }
        Ok(Self {
            endpoints,
            channels: RwLock::new(HashMap::new()),
            current: RwLock::new(0),
            leader: RwLock::new(None),
        })
    }

    /// Return a `MasterServiceClient` backed by the current preferred
    /// channel.  Channels are created lazily (via `connect_lazy`) so
    /// this method never blocks on a TCP handshake.
    pub async fn get_client(&self) -> MasterServiceClient<Channel> {
        let endpoint = self.current_endpoint().await;
        let channel = self.get_or_create_channel(&endpoint).await;
        MasterServiceClient::new(channel)
    }

    /// Mark the current endpoint as failed and advance to the next one.
    /// If a leader hint is cached, prefer that endpoint; otherwise round
    /// robin through the remaining endpoints.
    pub async fn failover(&self) {
        // Clear stale leader hint — it pointed to a non-working node.
        let mut leader = self.leader.write().await;
        let bad = self.current_endpoint().await;
        if let Some(ref hint) = *leader {
            if hint == &bad {
                *leader = None;
            }
        }
        drop(leader);

        // Advance to the next endpoint.
        let mut current = self.current.write().await;
        *current = (*current + 1) % self.endpoints.len();
    }

    /// Record the leader address reported by a "not leader" error.
    /// The address is expected in `host:port` form (no scheme).
    pub async fn set_leader_hint(&self, addr: &str) {
        let normalized = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        };

        // If the hinted address matches one of our endpoints, switch to it.
        if let Some(idx) = self.endpoints.iter().position(|e| e == &normalized) {
            let mut current = self.current.write().await;
            *current = idx;
        }
        let mut leader = self.leader.write().await;
        *leader = Some(normalized);
    }

    /// Execute a gRPC call with automatic failover.  The closure `f`
    /// receives a fresh `MasterServiceClient` and returns the gRPC
    /// result.  When the error is a transport failure or a "not leader"
    /// status, the client fails over to the next endpoint and retries
    /// once.
    pub async fn call<F, Fut, T>(&self, f: F) -> Result<T, Status>
    where
        F: Fn(MasterServiceClient<Channel>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, Status>> + Send,
        T: Send,
    {
        let client = self.get_client().await;
        match f(client).await {
            Ok(v) => Ok(v),
            Err(status) if is_retryable(&status) => {
                // Extract leader hint from the error message if present.
                if let Some(addr) = extract_leader_addr(status.message()) {
                    self.set_leader_hint(&addr).await;
                } else {
                    self.failover().await;
                }
                // Retry with the new endpoint.
                let client = self.get_client().await;
                f(client).await
            }
            Err(e) => Err(e),
        }
    }

    async fn current_endpoint(&self) -> String {
        let current = self.current.read().await;
        self.endpoints[*current].clone()
    }

    async fn get_or_create_channel(&self, endpoint: &str) -> Channel {
        // Fast path: channel already exists.
        {
            let channels = self.channels.read().await;
            if let Some(ch) = channels.get(endpoint) {
                return ch.clone();
            }
        }
        // Slow path: create a new lazy channel.  `connect_lazy` returns
        // immediately; the actual TCP connection is established on the
        // first RPC, which lets us survive transient network issues.
        let channel = Endpoint::from_shared(endpoint.to_string())
            .expect("invalid endpoint")
            .connect_lazy();
        let mut channels = self.channels.write().await;
        channels.insert(endpoint.to_string(), channel.clone());
        channel
    }
}

/// A transport error or "not leader" status is retryable.
fn is_retryable(status: &Status) -> bool {
    if status.code() == tonic::Code::Unavailable {
        return true;
    }
    let msg = status.message().to_lowercase();
    msg.contains("not leader") || msg.contains("transport error") || msg.contains("connection")
}

/// Try to extract a `host:port` leader address from a "not leader" message.
fn extract_leader_addr(msg: &str) -> Option<String> {
    // Messages look like: "not leader; current leader is 172.30.0.12:9333"
    let lower = msg.to_lowercase();
    if let Some(pos) = lower.find("leader is") {
        let rest = &msg[pos + "leader is".len()..].trim_start();
        // Take everything up to the next whitespace or end of string.
        let addr: String = rest.chars().take_while(|c| !c.is_whitespace()).collect();
        if !addr.is_empty() {
            return Some(addr);
        }
    }
    None
}

/// Convenience wrapper kept in `AppState` so existing handlers can keep
/// using `state.master_client` without caring about the internals.
pub type SharedMasterClient = Arc<ResilientMasterClient>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_retryable_not_leader() {
        let status = Status::internal("not leader; current leader is 172.30.0.12:9333");
        assert!(is_retryable(&status));
    }

    #[test]
    fn test_is_retryable_transport() {
        let status = Status::unavailable("transport error");
        assert!(is_retryable(&status));
    }

    #[test]
    fn test_is_retryable_non_retryable() {
        let status = Status::not_found("collection not found");
        assert!(!is_retryable(&status));
    }

    #[test]
    fn test_extract_leader_addr() {
        let addr = extract_leader_addr("not leader; current leader is 172.30.0.12:9333");
        assert_eq!(addr, Some("172.30.0.12:9333".to_string()));
    }

    #[test]
    fn test_extract_leader_addr_none() {
        let addr = extract_leader_addr("collection not found");
        assert_eq!(addr, None);
    }
}
