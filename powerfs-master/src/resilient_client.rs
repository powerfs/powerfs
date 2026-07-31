//! Resilient gRPC client for the Master service.
//!
//! Maintains connections to all configured master endpoints and
//! automatically discovers/follows the Raft leader.  The client
//! survives mid-call leader switches, network partitions, and master
//! restarts by retrying across every configured endpoint with
//! exponential backoff.
//!
//! **Usage**: callers only need `new()` + `call()`.  All leader
//! discovery, channel management, and failover logic is hidden
//! inside `call()`:
//!
//! ```ignore
//! let client = ResilientMasterClient::new(endpoints)?;
//! let resp = client.call(|mut c| async move {
//!     c.assign(Request::new(req)).await
//! }).await?;
//! ```
//!
//! This module lives in the `powerfs-master` crate so that every
//! downstream client (monitor, filer, S3, volume server, CLI, KV
//! client) can share the same leader-discovery logic instead of each
//! reimplementing its own.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;

use crate::proto::powerfs::master_service_client::MasterServiceClient;

/// Maximum number of retry attempts inside `call()`.
/// With N endpoints we allow N full rounds plus a small margin so
/// that a leader elected during the retry loop is still caught.
const MAX_RETRY_ATTEMPTS: usize = 5;

/// Initial backoff between retries; doubled each attempt (capped).
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(2);

pub struct ResilientMasterClient {
    /// All configured master gRPC endpoints (e.g. "http://172.30.0.11:9333").
    /// Immutable after construction.
    endpoints: Vec<String>,
    /// Lazily-created gRPC channels keyed by endpoint URL.
    channels: RwLock<HashMap<String, Channel>>,
    /// Index into `endpoints` used as round-robin fallback when no
    /// leader hint is cached.
    current: RwLock<usize>,
    /// Cached leader endpoint URL.  When set, `get_client()` prefers
    /// this over `current`.  Cleared by `failover()`.
    leader: RwLock<Option<String>>,
}

impl ResilientMasterClient {
    /// Create a new resilient client.  At least one endpoint must be
    /// provided.  Endpoints may be in `host:port` or
    /// `http://host:port` form — the scheme is added automatically.
    pub fn new(mut endpoints: Vec<String>) -> Result<Self, String> {
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

    /// Return a `MasterServiceClient` backed by the preferred channel.
    ///
    /// Preference order:
    /// 1. Cached leader hint (if set)
    /// 2. Current round-robin endpoint
    ///
    /// Channels are created lazily via `connect_lazy`, so this method
    /// never blocks on a TCP handshake.  Tonic channels auto-reconnect
    /// on transient failures, so cached channels remain usable across
    /// master restarts.
    pub async fn get_client(&self) -> MasterServiceClient<Channel> {
        let endpoint = self.preferred_endpoint().await;
        let channel = self.get_or_create_channel(&endpoint).await;
        MasterServiceClient::new(channel)
    }

    /// Mark the current endpoint as failed and advance to the next
    /// round-robin endpoint.  The cached leader hint is cleared
    /// unconditionally so the next `get_client()` call uses the
    /// round-robin position.
    pub async fn failover(&self) {
        // Unconditionally clear the leader hint — it pointed to a
        // non-working node (or we wouldn't be failing over).
        {
            let mut leader = self.leader.write().await;
            *leader = None;
        }
        let mut current = self.current.write().await;
        *current = (*current + 1) % self.endpoints.len();
    }

    /// Record the leader address reported by a "not leader" error.
    /// The address may be in `host:port` or `http://host:port` form.
    ///
    /// If the hinted address matches a configured endpoint, the
    /// round-robin index is synced so that `failover()` advances
    /// from the correct position.  If the hint is NOT in the
    /// configured list (e.g. dynamic cluster membership change),
    /// it is still cached and used by `get_client()` — the channel
    /// is created on demand.
    pub async fn set_leader_hint(&self, addr: &str) {
        let normalized = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        };

        // Sync the round-robin index if the hint is a known endpoint.
        if let Some(idx) = self.endpoints.iter().position(|e| e == &normalized) {
            let mut current = self.current.write().await;
            *current = idx;
        } else {
            log::warn!(
                "ResilientMasterClient: leader hint '{}' is not in configured endpoints {:?}; \
                 using it directly (channel will be created on demand)",
                normalized,
                self.endpoints
            );
        }

        let mut leader = self.leader.write().await;
        *leader = Some(normalized);
    }

    // ── public API ends here; internals below ──────────────────────

    /// Execute a gRPC call with automatic leader discovery, failover,
    /// and network reconnection.
    ///
    /// The closure `f` receives a fresh `MasterServiceClient` and
    /// returns the gRPC result.  When the error is a transport
    /// failure or a "not leader" status, the client:
    ///
    /// 1. Extracts the leader hint from the error message (if any)
    ///    and caches it, **or** fails over to the next endpoint.
    /// 2. Invalidates the stale channel for the failed endpoint so
    ///    that the next attempt creates a fresh connection.
    /// 3. Waits a short backoff (doubled each attempt) to let the
    ///    cluster stabilise during leader elections.
    /// 4. Retries with the new endpoint.
    ///
    /// This continues for up to `MAX_RETRY_ATTEMPTS` iterations,
    /// which is enough to try every endpoint at least once even in a
    /// 3-node cluster with a mid-loop leader change.
    pub async fn call<F, Fut, T>(&self, f: F) -> Result<T, Status>
    where
        F: Fn(MasterServiceClient<Channel>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, Status>> + Send,
        T: Send,
    {
        let mut backoff = INITIAL_BACKOFF;

        for attempt in 0..MAX_RETRY_ATTEMPTS {
            let used_endpoint = self.preferred_endpoint().await;
            let client = self.get_client().await;

            match f(client).await {
                Ok(v) => return Ok(v),
                Err(status) if is_retryable(&status) => {
                    let is_last = attempt + 1 >= MAX_RETRY_ATTEMPTS;
                    if is_last {
                        log::warn!(
                            "ResilientMasterClient: call failed after {} attempts: {}",
                            MAX_RETRY_ATTEMPTS,
                            status
                        );
                        return Err(status);
                    }

                    log::debug!(
                        "ResilientMasterClient: attempt {} failed on {}: {} — retrying",
                        attempt + 1,
                        used_endpoint,
                        status
                    );

                    // Drop the stale channel so the next attempt gets a
                    // fresh lazy connection (handles master restarts and
                    // dead TCP connections that tonic hasn't detected yet).
                    self.invalidate_channel(&used_endpoint).await;

                    // Follow the leader hint if present, otherwise round-robin.
                    if let Some(addr) = extract_leader_addr(status.message()) {
                        self.set_leader_hint(&addr).await;
                    } else {
                        self.failover().await;
                    }

                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
                Err(e) => return Err(e),
            }
        }

        // Unreachable — the loop always returns on the last attempt.
        unreachable!("ResilientMasterClient::call loop exhausted without returning")
    }

    /// The endpoint that `get_client()` will connect to next.
    async fn preferred_endpoint(&self) -> String {
        let leader = self.leader.read().await;
        if let Some(ref addr) = *leader {
            return addr.clone();
        }
        drop(leader);
        self.current_endpoint().await
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
        // Slow path: create a new lazy channel.  `connect_lazy`
        // returns immediately; the actual TCP connection is
        // established on the first RPC, and tonic auto-reconnects on
        // transient failures.
        let channel = Endpoint::from_shared(endpoint.to_string())
            .expect("invalid endpoint")
            .connect_lazy();
        let mut channels = self.channels.write().await;
        channels.insert(endpoint.to_string(), channel.clone());
        channel
    }

    /// Remove a (presumably broken) channel from the cache so the
    /// next `get_or_create_channel` creates a fresh one.
    async fn invalidate_channel(&self, endpoint: &str) {
        let mut channels = self.channels.write().await;
        channels.remove(endpoint);
    }
}

/// A transport error or "not leader" status is retryable.
fn is_retryable(status: &Status) -> bool {
    if status.code() == tonic::Code::Unavailable {
        return true;
    }
    let msg = status.message().to_lowercase();
    msg.contains("not leader")
        || msg.contains("transport error")
        || msg.contains("connection")
        || msg.contains("broken pipe")
        || msg.contains("reset")
}

/// Try to extract a `host:port` leader address from a "not leader" message.
/// Supports several message formats:
/// - "not leader; current leader is 172.30.0.12:9333"
/// - "not leader; leader is 172.30.0.12:9333"
/// - "not leader; leader: 172.30.0.12:9333"
fn extract_leader_addr(msg: &str) -> Option<String> {
    let lower = msg.to_lowercase();
    for marker in &["leader is", "leader:"] {
        if let Some(pos) = lower.find(marker) {
            let rest = &msg[pos + marker.len()..].trim_start();
            // Take everything up to the next whitespace, comma, or end.
            let addr: String = rest
                .chars()
                .take_while(|c| !c.is_whitespace() && *c != ',')
                .collect();
            // Validate it looks like host:port.
            if !addr.is_empty() && addr.contains(':') && !addr.contains(' ') {
                return Some(addr);
            }
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
    fn test_is_retryable_connection_reset() {
        let status = Status::unavailable("connection reset by peer");
        assert!(is_retryable(&status));
    }

    #[test]
    fn test_is_retryable_non_retryable() {
        let status = Status::not_found("collection not found");
        assert!(!is_retryable(&status));
    }

    #[test]
    fn test_extract_leader_addr_standard() {
        let addr = extract_leader_addr("not leader; current leader is 172.30.0.12:9333");
        assert_eq!(addr, Some("172.30.0.12:9333".to_string()));
    }

    #[test]
    fn test_extract_leader_addr_colon() {
        let addr = extract_leader_addr("not leader; leader: 172.30.0.12:9333");
        assert_eq!(addr, Some("172.30.0.12:9333".to_string()));
    }

    #[test]
    fn test_extract_leader_addr_none() {
        let addr = extract_leader_addr("collection not found");
        assert_eq!(addr, None);
    }

    #[test]
    fn test_extract_leader_addr_empty() {
        let addr = extract_leader_addr("not leader; current leader is ");
        assert_eq!(addr, None);
    }

    #[tokio::test]
    async fn test_new_normalises_endpoints() {
        let client = ResilientMasterClient::new(vec!["172.30.0.11:9333".to_string()]).unwrap();
        assert_eq!(client.endpoints[0], "http://172.30.0.11:9333");
    }

    #[tokio::test]
    async fn test_new_rejects_empty() {
        let result = ResilientMasterClient::new(vec![]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_leader_hint_known_endpoint() {
        let client = ResilientMasterClient::new(vec![
            "172.30.0.11:9333".to_string(),
            "172.30.0.12:9333".to_string(),
        ])
        .unwrap();

        // Initially current = 0.
        assert_eq!(*client.current.read().await, 0);

        client.set_leader_hint("172.30.0.12:9333").await;

        // current should be synced to index 1.
        assert_eq!(*client.current.read().await, 1);
        // leader hint should be set.
        assert_eq!(
            *client.leader.read().await,
            Some("http://172.30.0.12:9333".to_string())
        );
    }

    #[tokio::test]
    async fn test_set_leader_hint_unknown_endpoint() {
        let client = ResilientMasterClient::new(vec!["172.30.0.11:9333".to_string()]).unwrap();

        // Hint with an address NOT in the endpoints list.
        client.set_leader_hint("172.30.0.99:9333").await;

        // current should NOT change (still 0).
        assert_eq!(*client.current.read().await, 0);
        // leader hint should be set so get_client uses it.
        assert_eq!(
            *client.leader.read().await,
            Some("http://172.30.0.99:9333".to_string())
        );
        // preferred_endpoint should return the hint, not current.
        assert_eq!(client.preferred_endpoint().await, "http://172.30.0.99:9333");
    }

    #[tokio::test]
    async fn test_failover_clears_leader_and_advances() {
        let client = ResilientMasterClient::new(vec![
            "172.30.0.11:9333".to_string(),
            "172.30.0.12:9333".to_string(),
            "172.30.0.13:9333".to_string(),
        ])
        .unwrap();

        // Set a leader hint.
        client.set_leader_hint("172.30.0.12:9333").await;
        assert!(client.leader.read().await.is_some());

        // Failover should clear the hint and advance current.
        client.failover().await;
        assert!(client.leader.read().await.is_none());
        // current was 1 (synced by set_leader_hint), now 2.
        assert_eq!(*client.current.read().await, 2);
    }

    #[tokio::test]
    async fn test_preferred_endpoint_uses_leader_hint() {
        let client = ResilientMasterClient::new(vec![
            "172.30.0.11:9333".to_string(),
            "172.30.0.12:9333".to_string(),
        ])
        .unwrap();

        // No leader hint → use current (index 0).
        assert_eq!(client.preferred_endpoint().await, "http://172.30.0.11:9333");

        // Set leader hint to endpoint 1.
        client.set_leader_hint("172.30.0.12:9333").await;
        assert_eq!(client.preferred_endpoint().await, "http://172.30.0.12:9333");

        // Failover: clears leader hint, current was 1 (synced by
        // set_leader_hint), advances to (1+1)%2 = 0.
        client.failover().await;
        assert!(client.leader.read().await.is_none());
        assert_eq!(*client.current.read().await, 0);
        assert_eq!(client.preferred_endpoint().await, "http://172.30.0.11:9333");
    }

    #[tokio::test]
    async fn test_invalidate_channel_removes_entry() {
        let client = ResilientMasterClient::new(vec!["172.30.0.11:9333".to_string()]).unwrap();

        // Create a channel.
        let _ = client
            .get_or_create_channel("http://172.30.0.11:9333")
            .await;
        assert!(client
            .channels
            .read()
            .await
            .contains_key("http://172.30.0.11:9333"));

        // Invalidate it.
        client.invalidate_channel("http://172.30.0.11:9333").await;
        assert!(!client
            .channels
            .read()
            .await
            .contains_key("http://172.30.0.11:9333"));
    }

    #[tokio::test]
    async fn test_call_succeeds_first_attempt() {
        let client = ResilientMasterClient::new(vec!["172.30.0.11:9333".to_string()]).unwrap();

        // The closure returns Ok immediately — no gRPC call is made.
        let result: Result<i32, Status> = client.call(|_c| async move { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_call_returns_non_retryable_immediately() {
        let client = ResilientMasterClient::new(vec!["172.30.0.11:9333".to_string()]).unwrap();

        let result: Result<i32, Status> = client
            .call(|_c| async move { Err(Status::not_found("not found")) })
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }
}
