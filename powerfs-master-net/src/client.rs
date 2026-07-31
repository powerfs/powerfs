//! [`TlvMasterClient`] — reusable TLV client for the Master service.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};
use parking_lot::RwLock;
use powerfs_net::{
    ClientConfig, ClientType, FieldId, MsgType, NetMessage, NotificationHandler, PowerFsNetClient,
    TlvDecoder, TlvEncoder, STATUS_ERR_REDIRECT, STATUS_OK,
};

use crate::error::{MasterNetError, MasterNetResult};
use crate::types::{AssignResult, TopologyInfo, VolumeLocation, VolumeRoute};

/// Wrapper that turns an `Arc<dyn NotificationHandler>` into a `Box<dyn
/// NotificationHandler>` so it can be re-installed on every new
/// `PowerFsNetClient` after a reconnect/failover.
struct ArcNotificationHandler(Arc<dyn NotificationHandler + Send + Sync>);

impl NotificationHandler for ArcNotificationHandler {
    fn handle_notification(&self, msg: &NetMessage) {
        self.0.handle_notification(msg);
    }
}

/// Configuration for [`TlvMasterClient`].
#[derive(Debug, Clone)]
pub struct TlvMasterClientConfig {
    /// Client type sent in the TLV handshake.
    pub client_type: ClientType,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// Maximum retry attempts across all endpoints before giving up.
    pub max_retries: u32,
    /// Maximum leader-redirect hops per request.
    pub max_redirects: u32,
    /// Backoff between retries.
    pub retry_backoff: Duration,
}

impl Default for TlvMasterClientConfig {
    fn default() -> Self {
        Self {
            client_type: ClientType::Fuse,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            max_retries: 3,
            max_redirects: 5,
            retry_backoff: Duration::from_millis(200),
        }
    }
}

/// High-level TLV client for the PowerFS Master.
///
/// Manages a pool of master endpoints, automatically discovers the
/// Raft leader via `STATUS_ERR_REDIRECT`, and fails over to the next
/// endpoint on connection errors.  Callers use the typed methods
/// (`get_topology`, `assign`, `lookup_volume`) or the generic
/// `submit_request` without needing to know TLV encoding details.
pub struct TlvMasterClient {
    /// `(host, net_port)` pairs for every configured master.
    endpoints: Vec<(String, u16)>,
    /// Round-robin index into `endpoints` (fallback when no leader hint).
    current_idx: AtomicUsize,
    /// Cached leader address in `host:port` form.
    leader: RwLock<Option<String>>,
    /// Active network client — swapped atomically on redirect.
    net_client: Arc<RwLock<Arc<PowerFsNetClient>>>,
    /// Optional notification handler, re-installed on every reconnect so
    /// server-pushed `NOTIFY` frames (e.g. `TopologyChanged`) keep being
    /// delivered after a leader switch or endpoint failover.
    notification_handler: Arc<RwLock<Option<Arc<dyn NotificationHandler + Send + Sync>>>>,
    config: TlvMasterClientConfig,
}

impl TlvMasterClient {
    /// Create a new client with the given master endpoints.
    ///
    /// Each endpoint is a `(host, net_port)` tuple, e.g.
    /// `("172.30.0.11", 9334)`.  At least one endpoint is required.
    pub fn new(endpoints: Vec<(String, u16)>, config: TlvMasterClientConfig) -> Self {
        let first = endpoints
            .first()
            .cloned()
            .unwrap_or(("127.0.0.1".into(), 9334));
        let net_client = Self::build_net_client(&first.0, first.1, &config);

        Self {
            endpoints,
            current_idx: AtomicUsize::new(0),
            leader: RwLock::new(None),
            net_client: Arc::new(RwLock::new(Arc::new(net_client))),
            notification_handler: Arc::new(RwLock::new(None)),
            config,
        }
    }

    /// Install a notification handler to receive server-pushed `NOTIFY`
    /// frames (e.g. `TopologyChanged`).  The handler is preserved across
    /// reconnects and endpoint failovers.
    pub fn set_notification_handler(&self, handler: Arc<dyn NotificationHandler + Send + Sync>) {
        *self.notification_handler.write() = Some(handler.clone());
        let net_client = self.net_client.read().clone();
        net_client.set_notification_handler(Box::new(ArcNotificationHandler(handler)));
    }

    /// Re-apply the cached notification handler (if any) to a freshly
    /// built `PowerFsNetClient`.  Called after every reconnect/failover.
    fn apply_notification_handler(&self, net_client: &PowerFsNetClient) {
        if let Some(h) = self.notification_handler.read().clone() {
            net_client.set_notification_handler(Box::new(ArcNotificationHandler(h)));
        }
    }

    // ── public API ───────────────────────────────────────────────

    /// Current cached leader address (`host:port`), if known.
    pub fn current_leader(&self) -> Option<String> {
        self.leader.read().clone()
    }

    /// Update the cached leader hint without reconnecting.
    ///
    /// Useful when the caller learns about a leader change from an
    /// out-of-band source (e.g. a shard metadata response).  The next
    /// `submit_request()` will reconnect to this address if the current
    /// connection is stale.
    pub fn set_leader_hint(&self, addr: &str) {
        *self.leader.write() = Some(addr.to_string());
    }

    /// Clear the cached leader hint.
    ///
    /// Called by callers (e.g. FUSE `MasterClient::disconnect`) when
    /// they explicitly tear down the connection so that subsequent
    /// `current_leader()` calls return `None` and the next request
    /// falls back to the round-robin endpoint list.
    pub fn clear_leader(&self) {
        *self.leader.write() = None;
    }

    /// Whether the underlying TCP connection is alive.
    pub fn is_connected(&self) -> bool {
        self.net_client.read().is_connected()
    }

    /// Establish a TCP connection to the current (or first) endpoint.
    pub async fn connect(&self) -> MasterNetResult<()> {
        let net_client = self.net_client.read().clone();
        net_client.connect().await.map_err(|e| {
            MasterNetError::ConnectionFailed(format!("connect to first endpoint: {}", e))
        })?;
        let addr = self.current_endpoint_addr();
        *self.leader.write() = Some(addr.clone());
        info!("TlvMasterClient: connected to {}", addr);
        Ok(())
    }

    /// Send a raw TLV request with automatic leader-redirect and
    /// endpoint-failover handling.
    ///
    /// On `STATUS_ERR_REDIRECT` the leader address is extracted from
    /// the response body, the connection is switched to that address,
    /// and the request is retried (up to `max_redirects` times).
    ///
    /// On a transport error the client fails over to the next
    /// configured endpoint and retries (up to `max_retries` times).
    pub async fn submit_request(
        &self,
        msg_type: MsgType,
        body: &[u8],
    ) -> MasterNetResult<NetMessage> {
        let mut redirects = 0u32;
        let mut failures = 0u32;

        loop {
            // Ensure we have a live connection.
            if !self.is_connected() {
                if let Err(e) = self.connect_current().await {
                    failures += 1;
                    if failures > self.config.max_retries {
                        return Err(MasterNetError::AllEndpointsExhausted {
                            attempts: failures as usize,
                        });
                    }
                    self.advance_to_next_endpoint();
                    tokio::time::sleep(self.config.retry_backoff).await;
                    warn!(
                        "TlvMasterClient: connect failed ({}), advancing to next endpoint",
                        e
                    );
                    continue;
                }
            }

            let net_client = self.net_client.read().clone();
            let result = net_client.send_request(msg_type, body, &[]).await;

            match result {
                // ── Leader redirect ──
                Ok(resp) if resp.header.status == STATUS_ERR_REDIRECT => {
                    redirects += 1;
                    if redirects > self.config.max_redirects {
                        return Err(MasterNetError::RedirectFailed(format!(
                            "exceeded {} redirect hops",
                            self.config.max_redirects
                        )));
                    }
                    let leader_addr = extract_leader(&resp)?;
                    info!(
                        "TlvMasterClient: redirect {:?} → {} (hop {}/{})",
                        msg_type, leader_addr, redirects, self.config.max_redirects
                    );
                    self.reconnect_to(&leader_addr).await?;
                    continue; // retry on the new leader
                }

                // ── Success or non-redirect error ──
                Ok(resp) => return Ok(resp),

                // ── Transport error → failover ──
                Err(e) => {
                    failures += 1;
                    if failures > self.config.max_retries {
                        return Err(MasterNetError::AllEndpointsExhausted {
                            attempts: failures as usize,
                        });
                    }
                    warn!(
                        "TlvMasterClient: transport error on {:?} ({}), failing over",
                        msg_type, e
                    );
                    self.advance_to_next_endpoint();
                    tokio::time::sleep(self.config.retry_backoff).await;
                    continue;
                }
            }
        }
    }

    /// Fetch cluster topology (leader + volume routes) from Master.
    pub async fn get_topology(&self) -> MasterNetResult<TopologyInfo> {
        let resp = self.submit_request(MsgType::GetTopology, &[]).await?;

        if resp.header.status != STATUS_OK {
            return Err(MasterNetError::ServerError {
                status: resp.header.status,
                detail: "get_topology failed".into(),
            });
        }

        let mut dec = TlvDecoder::new(&resp.body);
        let leader = dec
            .next_string(FieldId::Owner)
            .map_err(|e| MasterNetError::DecodeError(e.to_string()))?;

        // Cache the leader hint.
        *self.leader.write() = Some(leader.clone());

        let volume_count = dec.next_u64(FieldId::Entries).unwrap_or(0) as usize;

        let mut volumes = Vec::with_capacity(volume_count);
        for _ in 0..volume_count {
            let volume_id = dec.next_u64(FieldId::VolumeId).unwrap_or(0);
            let addr = dec.next_string(FieldId::Owner).unwrap_or_default();
            let size = dec.next_u64(FieldId::Size).unwrap_or(0);
            volumes.push(VolumeRoute {
                volume_id,
                addr,
                size,
            });
        }

        Ok(TopologyInfo { leader, volumes })
    }

    /// Assign a new volume/file on Master.
    pub async fn assign(
        &self,
        collection: &str,
        replication: &str,
    ) -> MasterNetResult<AssignResult> {
        let mut enc = TlvEncoder::new();
        let _ = enc.add_string(FieldId::Name, collection);
        let _ = enc.add_string(FieldId::Backend, replication);
        let body = enc.into_bytes();

        let resp = self.submit_request(MsgType::Assign, &body).await?;

        if resp.header.status != STATUS_OK {
            return Err(MasterNetError::ServerError {
                status: resp.header.status,
                detail: "assign failed".into(),
            });
        }

        let mut dec = TlvDecoder::new(&resp.body);
        let volume_id = dec
            .next_u64(FieldId::VolumeId)
            .map_err(|e| MasterNetError::DecodeError(e.to_string()))?;
        let cookie = dec.next_u64(FieldId::Cookie).unwrap_or(0);
        let file_key = dec.next_u64(FieldId::FileKey).unwrap_or(0);
        let route_addr = dec.next_string(FieldId::Owner).unwrap_or_default();
        let replica_count = dec.next_u64(FieldId::Entries).unwrap_or(0) as usize;

        Ok(AssignResult {
            volume_id,
            cookie,
            file_key,
            route_addr,
            replica_count,
        })
    }

    /// Look up a volume's location by its ID.
    pub async fn lookup_volume(&self, volume_id: u64) -> MasterNetResult<Option<VolumeLocation>> {
        let mut enc = TlvEncoder::new();
        let _ = enc.add_string(FieldId::Name, &volume_id.to_string());
        let body = enc.into_bytes();

        let resp = self.submit_request(MsgType::LookupVolume, &body).await?;

        if resp.header.status == powerfs_net::STATUS_ERR_NOT_FOUND {
            return Ok(None);
        }
        if resp.header.status != STATUS_OK {
            return Err(MasterNetError::ServerError {
                status: resp.header.status,
                detail: "lookup_volume failed".into(),
            });
        }

        let mut dec = TlvDecoder::new(&resp.body);
        let _count = dec.next_u64(FieldId::Limit).unwrap_or(0);
        let url = dec.next_string(FieldId::Owner).unwrap_or_default();
        let data_center = dec.next_string(FieldId::Backend).unwrap_or_default();

        Ok(Some(VolumeLocation { url, data_center }))
    }

    // ── internals ────────────────────────────────────────────────

    fn build_net_client(host: &str, port: u16, config: &TlvMasterClientConfig) -> PowerFsNetClient {
        let net_cfg = ClientConfig {
            addr: host.to_string(),
            port,
            client_id: 0,
            client_type: config.client_type,
            connect_timeout: config.connect_timeout,
            request_timeout: config.request_timeout,
            max_retries: 1, // TlvMasterClient handles retries itself
            retry_delay: config.retry_backoff,
            heartbeat_interval: Duration::from_secs(30),
            max_inflight_requests: 256,
        };
        PowerFsNetClient::new(net_cfg)
    }

    fn current_endpoint_addr(&self) -> String {
        let idx = self.current_idx.load(Ordering::Relaxed) % self.endpoints.len();
        let (host, port) = &self.endpoints[idx];
        format!("{}:{}", host, port)
    }

    fn advance_to_next_endpoint(&self) {
        let len = self.endpoints.len();
        if len > 1 {
            // fetch_add returns the *old* value; the new index is old+1.
            let old = self.current_idx.fetch_add(1, Ordering::Relaxed);
            let new = (old + 1) % len;
            let (host, port) = &self.endpoints[new];
            let new_client = Self::build_net_client(host, *port, &self.config);
            self.apply_notification_handler(&new_client);
            *self.net_client.write() = Arc::new(new_client);
            info!(
                "TlvMasterClient: advanced to endpoint {}/{} ({}:{})",
                new + 1,
                len,
                host,
                port
            );
        }
    }

    async fn connect_current(&self) -> MasterNetResult<()> {
        let net_client = self.net_client.read().clone();
        net_client
            .connect()
            .await
            .map_err(|e| MasterNetError::ConnectionFailed(e.to_string()))
    }

    /// Switch the active connection to `addr` (`host:port`), update the
    /// leader cache, and establish the TCP connection.
    async fn reconnect_to(&self, addr: &str) -> MasterNetResult<()> {
        let (host, port) = parse_addr(addr);
        let new_client = Self::build_net_client(&host, port, &self.config);
        new_client.connect().await.map_err(|e| {
            MasterNetError::ConnectionFailed(format!("reconnect to {}: {}", addr, e))
        })?;

        self.apply_notification_handler(&new_client);
        *self.net_client.write() = Arc::new(new_client);
        *self.leader.write() = Some(addr.to_string());

        // Sync the round-robin index if this address is a known endpoint.
        if let Some(idx) = self
            .endpoints
            .iter()
            .position(|(h, p)| format!("{}:{}", h, p) == addr)
        {
            self.current_idx.store(idx, Ordering::Relaxed);
        }

        info!("TlvMasterClient: reconnected to leader {}", addr);
        Ok(())
    }
}

// ── helpers ─────────────────────────────────────────────────────

fn parse_addr(addr: &str) -> (String, u16) {
    match addr.rsplit_once(':') {
        Some((h, p)) => {
            let port = p.parse::<u16>().unwrap_or(9334);
            (h.to_string(), port)
        }
        None => (addr.to_string(), 9334),
    }
}

fn extract_leader(resp: &NetMessage) -> MasterNetResult<String> {
    let body = if !resp.body.is_empty() {
        &resp.body
    } else {
        &resp.data
    };
    let mut dec = TlvDecoder::new(body);
    let addr = dec
        .next_string(FieldId::Owner)
        .map_err(|e| MasterNetError::DecodeError(e.to_string()))?;
    if addr.is_empty() {
        return Err(MasterNetError::EmptyRedirect);
    }
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_addr_with_port() {
        let (h, p) = parse_addr("172.30.0.11:9334");
        assert_eq!(h, "172.30.0.11");
        assert_eq!(p, 9334);
    }

    #[test]
    fn test_parse_addr_no_port() {
        let (h, p) = parse_addr("172.30.0.11");
        assert_eq!(h, "172.30.0.11");
        assert_eq!(p, 9334); // default
    }

    #[test]
    fn test_new_requires_endpoints() {
        let client = TlvMasterClient::new(
            vec![("127.0.0.1".into(), 9334)],
            TlvMasterClientConfig::default(),
        );
        assert!(client.current_leader().is_none());
    }

    #[test]
    fn test_advance_to_next_endpoint_wraps() {
        let client = TlvMasterClient::new(
            vec![
                ("host1".into(), 9334),
                ("host2".into(), 9334),
                ("host3".into(), 9334),
            ],
            TlvMasterClientConfig::default(),
        );
        // Start at endpoint 0.
        assert_eq!(client.current_endpoint_addr(), "host1:9334");
        // Advance → endpoint 1.
        client.advance_to_next_endpoint();
        assert_eq!(client.current_endpoint_addr(), "host2:9334");
        // Advance → endpoint 2.
        client.advance_to_next_endpoint();
        assert_eq!(client.current_endpoint_addr(), "host3:9334");
        // Advance → wraps to endpoint 0.
        client.advance_to_next_endpoint();
        assert_eq!(client.current_endpoint_addr(), "host1:9334");
    }
}
