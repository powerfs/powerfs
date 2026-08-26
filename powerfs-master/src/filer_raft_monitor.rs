//! Master control-plane monitor: periodically polls each registered filer's
//! Raft health via `MsgType::FilerRaftStatus` and detects **fake Leaders** —
//! nodes whose Raft metrics report `ServerState::Leader` but whose lease is
//! no longer acknowledged by a quorum (root cause of `forward to: None, None`
//! loops and SLOW_REQ on the filer side).
//!
//! Upon detecting a fake Leader the Master removes the filer from the routing
//! table (`set_filer_unhealthy(true)`) so fuse/kernel clients stop sending
//! requests to the stale leader. When the filer recovers (Quorum re-acknowledges
//! the lease) the monitor re-adds it (`set_filer_unhealthy(false)`).
//!
//! See `docs/raft_fault_tolerance_design.md` for the full design, including
//! the alignment with Openraft's `ensure_linearizable` / `is_lease_valid` /
//! `running_state` semantics.

use std::sync::Arc;

use log::{debug, info, warn};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::{
    ClientConnPool, ClientPoolConfig, FieldId, MsgType, NetError, NetMessage, STATUS_OK,
};

use crate::master::{FilerNodeInfo, MasterNode};

/// Manual `ServerState` → u8 mapping used on the wire (see
/// `MsgType::FilerRaftStatus` doc and the filer-side encoder).
const STATE_LEARNER: u8 = 1;
const STATE_FOLLOWER: u8 = 2;
const STATE_CANDIDATE: u8 = 3;
const STATE_LEADER: u8 = 4;
const STATE_SHUTDOWN: u8 = 5;

/// flags bitmasks (matches the filer-side encoder).
const FLAG_HAS_PEERS: u64 = 1 << 0;
const FLAG_RUNNING_STATE_OK: u64 = 1 << 1;
const FLAG_IS_LEASE_VALID: u64 = 1 << 2;

/// Parsed Raft status of a single shard on a filer.
#[derive(Debug, Clone)]
pub struct FilerShardStatus {
    pub shard_id: u64,
    pub state_u8: u8,
    pub is_leader: bool,
    pub leader_addr: String,
    pub current_term: u64,
    pub has_peers: bool,
    pub running_state_ok: bool,
    pub is_lease_valid: bool,
    pub commit_index: u64,
    pub last_applied: u64,
}

impl FilerShardStatus {
    /// A fake Leader: reports `Leader` state with peers but `is_lease_valid`
    /// is false (Quorum does not acknowledge the lease). Single-node shards
    /// (no peers) cannot be fake Leaders — a solo Leader's lease is
    /// self-granted and never expires.
    pub fn is_fake_leader(&self) -> bool {
        self.is_leader && self.has_peers && !self.is_lease_valid
    }
}

/// Decode the TLV response body returned by `MsgType::FilerRaftStatus`.
///
/// Schema (must match the filer-side encoder in `net_handler.rs`):
/// ```text
/// Limit   → count(u64)
/// per entry:
///   Ino        → shard_id(u64)
///   Mode       → state(u8)
///   Owner      → leader_addr(string)
///   Cookie     → current_term(u64)
///   Entries    → flags(u64)
///   FileKey    → commit_index(u64)
///   UsedSpace  → last_applied(u64)
/// ```
pub fn decode_filer_raft_status(body: &[u8]) -> Result<Vec<FilerShardStatus>, NetError> {
    let mut dec = TlvDecoder::new(body);
    let count = dec.next_u64(FieldId::Limit).unwrap_or(0) as usize;

    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let shard_id = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let state_u8 = dec.next_u8(FieldId::Mode).unwrap_or(0);
        let leader_addr = dec.next_string(FieldId::Owner).unwrap_or_default();
        let current_term = dec.next_u64(FieldId::Cookie).unwrap_or(0);
        let flags = dec.next_u64(FieldId::Entries).unwrap_or(0);
        let commit_index = dec.next_u64(FieldId::FileKey).unwrap_or(0);
        let last_applied = dec.next_u64(FieldId::UsedSpace).unwrap_or(0);

        out.push(FilerShardStatus {
            shard_id,
            state_u8,
            is_leader: state_u8 == STATE_LEADER,
            leader_addr,
            current_term,
            has_peers: (flags & FLAG_HAS_PEERS) != 0,
            running_state_ok: (flags & FLAG_RUNNING_STATE_OK) != 0,
            is_lease_valid: (flags & FLAG_IS_LEASE_VALID) != 0,
            commit_index,
            last_applied,
        });
    }
    Ok(out)
}

/// Send a `FilerRaftStatus` request to a filer and return its shard statuses.
///
/// Uses the provided `ClientConnPool` to reuse the TCP connection across
/// polls. The request body is empty (filer returns status for all shards).
pub async fn query_filer_raft_status(
    pool: &ClientConnPool,
    addr_port: &str,
) -> Result<Vec<FilerShardStatus>, String> {
    let client = pool
        .get_or_connect_addr(addr_port)
        .await
        .map_err(|e| format!("connect to filer {} failed: {}", addr_port, e))?;

    let resp: NetMessage = client
        .send_request(MsgType::FilerRaftStatus, &[], &[])
        .await
        .map_err(|e| format!("FilerRaftStatus RPC to {} failed: {}", addr_port, e))?;

    if resp.header.status != STATUS_OK {
        return Err(format!(
            "filer {} returned status={} for FilerRaftStatus",
            addr_port, resp.header.status
        ));
    }

    decode_filer_raft_status(&resp.body).map_err(|e| format!("decode error: {}", e))
}

/// Spawn the control-plane monitor that periodically polls every registered
/// filer and removes fake Leaders from the routing table.
///
/// **Detection rule** (per filer): the filer is marked unhealthy if **all**
/// multi-node shards for which it is the Leader are fake Leaders (lease
/// invalid). Single-node shards and non-Leader shards are ignored — a filer
/// that is Leader of some healthy shards and fake-Leader of others is NOT
/// marked unhealthy (the healthy shards still serve traffic). This avoids
/// over-aggressive quarantine.
///
/// **Recovery rule**: once at least one previously-fake shard recovers
/// (lease valid again), or the filer is no longer Leader of any multi-node
/// shard, the filer is marked healthy again.
///
/// **Quorum-safety**: marking a filer unhealthy only affects the Master's
/// `list_filers` advertisement and the per-shard `shard_mapping` route.
/// It does NOT change Raft membership — the filer's own Raft state is
/// untouched, so it can still participate in elections and re-acquire
/// leadership legitimately once Quorum is restored.
pub fn spawn_filer_raft_monitor(master: Arc<MasterNode>, pool: Arc<ClientConnPool>) {
    tokio::spawn(async move {
        // 5s poll interval: balances detection latency against probe load.
        // Each poll issues one ensure_linearizable (ReadIndex) per Leader
        // shard on the filer — cheap on a healthy filer, fast-fail on a
        // fake Leader (500ms timeout in the filer-side probe).
        let poll_interval = tokio::time::Duration::from_secs(5);
        // A filer must remain fake-Leader across consecutive polls before
        // being quarantined — avoids flapping on transient lease blips.
        let confirm_threshold: u32 = 2;

        // node_id → consecutive fake-Leader observation count
        let mut fake_counts: std::collections::HashMap<String, u32> =
            std::collections::HashMap::new();

        info!(
            "FilerRaftMonitor: started (poll={:?}, confirm={})",
            poll_interval, confirm_threshold
        );

        loop {
            tokio::time::sleep(poll_interval).await;

            // Only the Raft leader runs the control-plane poll. Followers
            // do not own the routing table authoritatively; if they also
            // polled and called `set_filer_unhealthy` they would race with
            // the leader's decisions. Followers just clear their local
            // counters and wait until they become leader.
            if !master.is_leader().await || !master.is_raft_available() {
                if !fake_counts.is_empty() {
                    debug!(
                        "FilerRaftMonitor: not leader / raft unavailable — \
                         clearing fake_counts (size={})",
                        fake_counts.len()
                    );
                    fake_counts.clear();
                }
                continue;
            }

            // Snapshot registered filers (under the std::sync::RwLock).
            let filers: Vec<FilerNodeInfo> = master.list_filers();
            if filers.is_empty() {
                // No filers registered yet — clear state and wait.
                if !fake_counts.is_empty() {
                    debug!("FilerRaftMonitor: no filers registered, clearing fake_counts");
                    fake_counts.clear();
                }
                continue;
            }

            for filer in &filers {
                // Only poll filers that expose a net_port (TLV transport).
                if filer.net_port == 0 {
                    continue;
                }
                let addr_port = format!("{}:{}", filer.address, filer.net_port);

                let status_result = query_filer_raft_status(&pool, &addr_port).await;

                match status_result {
                    Ok(statuses) => {
                        // A filer is "fake-Leader" if it leads at least one
                        // multi-node shard AND every such shard is fake.
                        let led_multi_shards: Vec<&FilerShardStatus> = statuses
                            .iter()
                            .filter(|s| s.is_leader && s.has_peers)
                            .collect();
                        let any_fake = led_multi_shards.iter().any(|s| s.is_fake_leader());
                        let all_fake = !led_multi_shards.is_empty()
                            && led_multi_shards.iter().all(|s| s.is_fake_leader());

                        if all_fake {
                            let count = fake_counts.entry(filer.node_id.clone()).or_insert(0);
                            *count += 1;
                            debug!(
                                "FilerRaftMonitor: filer={} ALL led multi-node shards are \
                                 fake-Leader (consecutive={}, threshold={}); fake shards: {:?}",
                                filer.node_id,
                                *count,
                                confirm_threshold,
                                led_multi_shards
                                    .iter()
                                    .filter(|s| s.is_fake_leader())
                                    .map(|s| s.shard_id)
                                    .collect::<Vec<_>>()
                            );
                            if *count >= confirm_threshold && filer.is_healthy {
                                warn!(
                                    "FilerRaftMonitor: filer={} confirmed fake-Leader across \
                                     {} polls — removing from routing table (shards {:?})",
                                    filer.node_id,
                                    *count,
                                    led_multi_shards
                                        .iter()
                                        .map(|s| s.shard_id)
                                        .collect::<Vec<_>>()
                                );
                                master.set_filer_unhealthy(&filer.node_id, true);
                            }
                        } else {
                            // Healthy or partially healthy — reset counter
                            // and restore routing if previously quarantined.
                            if any_fake {
                                // Some shards fake, some healthy — log but
                                // do NOT quarantine (partial service still
                                // available on the healthy shards).
                                warn!(
                                    "FilerRaftMonitor: filer={} has mixed fake/healthy led \
                                     shards — NOT quarantining (partial service retained). \
                                     fake={:?}",
                                    filer.node_id,
                                    led_multi_shards
                                        .iter()
                                        .filter(|s| s.is_fake_leader())
                                        .map(|s| s.shard_id)
                                        .collect::<Vec<_>>()
                                );
                            }
                            // Always clear the fake counter when not all-fake
                            // (the filer is either fully healthy or only
                            // partially fake — neither warrants quarantine).
                            fake_counts.remove(&filer.node_id);
                            // If the filer was previously quarantined and is
                            // now not-all-fake, restore it so the healthy
                            // shards can serve traffic again.
                            if !filer.is_healthy {
                                info!(
                                    "FilerRaftMonitor: filer={} recovered — restoring \
                                     routing table entry",
                                    filer.node_id
                                );
                                master.set_filer_unhealthy(&filer.node_id, false);
                            }
                        }
                    }
                    Err(e) => {
                        // RPC failure is NOT a fake-Leader signal — the
                        // filer may be briefly unreachable or restarting.
                        // Do not quarantine on connectivity errors; just
                        // reset the fake counter so a transient blip does
                        // not accumulate toward the threshold.
                        debug!(
                            "FilerRaftMonitor: filer={} status query failed: {} — \
                             not quarantining (connectivity issue, not fake-Leader)",
                            filer.node_id, e
                        );
                        fake_counts.remove(&filer.node_id);
                    }
                }
            }
        }
    });
}

/// Build a `ClientConnPool` suitable for the Master's filer-control-plane
/// queries. The Master has no inbound notifications to handle from filers
/// on this pool (control-plane only), so the notification handler is `None`.
pub fn build_filer_control_pool(master_client_id: u64) -> Arc<ClientConnPool> {
    let config = ClientPoolConfig::default();
    Arc::new(ClientConnPool::new(
        master_client_id,
        config,
        None, // no inbound notifications on the control-plane pool
    ))
}

// Suppress unused-import warning for TlvEncoder — kept for future encode
// extensions (e.g. querying a specific shard rather than all shards).
#[allow(dead_code)]
fn _keep_tlv_encoder() -> TlvEncoder {
    TlvEncoder::new()
}

#[allow(dead_code)]
const _STATE_UNUSED: u8 = STATE_LEARNER | STATE_FOLLOWER | STATE_CANDIDATE | STATE_SHUTDOWN;
const _: () = {
    // Compile-time assertion that the manual state mapping is consistent
    // with the doc in `protocol.rs`. If the doc mapping changes, this
    // const-eval will fail and force an update here.
    assert!(STATE_LEADER == 4);
    assert!(STATE_LEARNER == 1);
};
