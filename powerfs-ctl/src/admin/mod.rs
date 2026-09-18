//! Admin client — talks to master's `/api/admin/*` HTTP endpoints on port
//! 9300 (raft membership + data-node maintenance/removal). Trait-based so
//! tests inject a MockAdminClient; the real `MasterAdminClient` lives in
//! `reqwest_client.rs`.
//!
//! The 5 `/api/admin/*` calls are raft-mutating → leader-only. Followers
//! return 503 with `{error, leader:{id,addr}}` where `addr` is raft gRPC
//! `ip:9335`. The reqwest impl retries once against the leader's HTTP port
//! (`ip:9300`). Callers discover the leader first via `leader_api_and_token`
//! (commands/mod.rs) — the retry only catches leader migration between
//! discovery and the call.
//!
//! Endpoints wrapped:
//!   GET    /api/admin/masters                  — membership snapshot
//!   POST   /api/admin/masters                  — add voter (id+addr)
//!   DELETE /api/admin/masters/{id}?force=      — remove voter
//!   POST   /api/admin/nodes/{name}/maintenance — toggle maintenance
//!   DELETE /api/admin/nodes/{name}?force=     — remove data node

pub mod reqwest_client;
pub use reqwest_client::MasterAdminClient;

use async_trait::async_trait;

/// `master_api` shape: `"host:9300"` (same as CertClient — plain HTTP on the
/// internal metrics port, no TLS). Each method takes the *leader's* address;
/// callers discover it via `leader_api_and_token`.
#[async_trait]
pub trait AdminClient: Send + Sync {
    /// `GET /api/admin/masters` — current membership + leader id.
    async fn list_masters(
        &self,
        master_api: &str,
        admin_token: &str,
    ) -> Result<MastersSnapshot, String>;
    /// `POST /api/admin/masters` — add a new voter (id + raft gRPC addr).
    async fn add_master(
        &self,
        master_api: &str,
        admin_token: &str,
        req: &AddMasterRequest,
    ) -> Result<(), String>;
    /// `DELETE /api/admin/masters/{id}?force=` — remove a voter.
    async fn remove_master(
        &self,
        master_api: &str,
        admin_token: &str,
        id: &str,
        force: bool,
    ) -> Result<(), String>;
    /// `POST /api/admin/nodes/{name}/maintenance` — toggle maintenance mode.
    async fn set_maintenance(
        &self,
        master_api: &str,
        admin_token: &str,
        name: &str,
        enabled: bool,
    ) -> Result<(), String>;
    /// `DELETE /api/admin/nodes/{name}?force=` — remove a data node.
    async fn remove_node(
        &self,
        master_api: &str,
        admin_token: &str,
        name: &str,
        force: bool,
    ) -> Result<(), String>;
}

// ---------------------------------------------------------------------------
// DTOs — mirror master-side types (powerfs-master/src/raft_v2.rs and
// admin_api.rs). Response types are Deserialize; request types are Serialize.
// ---------------------------------------------------------------------------

/// One raft member. Mirrors `raft_v2.rs:207` (Serialize on master →
/// Deserialize here). `role` is `"voter"` or `"learner"`.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct MemberInfo {
    pub id: String,
    pub addr: String,
    pub role: String,
}

/// Membership snapshot. Mirrors `raft_v2.rs:216`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct MastersSnapshot {
    /// This node's own raft id.
    pub local: String,
    /// Current leader's raft id (None during an election).
    pub leader: Option<String>,
    pub members: Vec<MemberInfo>,
}

/// Body for `POST /api/admin/masters`. Mirrors `admin_api.rs:240`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AddMasterRequest {
    pub id: u64,
    pub addr: String,
}

/// Body for `POST /api/admin/nodes/{name}/maintenance`. Mirrors
/// `admin_api.rs:248`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MaintenanceRequest {
    pub enabled: bool,
}

/// 503 body returned by followers on raft-mutating admin calls. Mirrors the
/// JSON shape produced by `AdminError::body()` in `admin_api.rs:54`:
/// `{ "error": "...", "leader": {"id":"..","addr":"ip:9335","role":"voter"} | null }`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct NotLeaderBody {
    /// Human-readable reason ("not the raft leader; retry against the current
    /// leader"). Not used by the ctl — we only need the leader hint.
    #[allow(dead_code)]
    pub error: String,
    pub leader: Option<MemberInfo>,
}

#[cfg(test)]
pub mod mock;
#[cfg(test)]
pub use mock::MockAdminClient;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_masters_snapshot() {
        let json = r#"{
            "local": "1",
            "leader": "1",
            "members": [
                {"id": "1", "addr": "172.30.0.11:9335", "role": "voter"},
                {"id": "2", "addr": "172.30.0.12:9335", "role": "voter"},
                {"id": "3", "addr": "172.30.0.13:9335", "role": "voter"}
            ]
        }"#;
        let snap: MastersSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snap.local, "1");
        assert_eq!(snap.leader.as_deref(), Some("1"));
        assert_eq!(snap.members.len(), 3);
        assert_eq!(snap.members[0].role, "voter");
    }

    #[test]
    fn deserialize_not_leader_body_with_hint() {
        let json = r#"{
            "error": "not the raft leader; retry against the current leader",
            "leader": {"id": "2", "addr": "172.30.0.12:9335", "role": "voter"}
        }"#;
        let nl: NotLeaderBody = serde_json::from_str(json).unwrap();
        assert!(nl.error.contains("not the raft leader"));
        let leader = nl.leader.unwrap();
        assert_eq!(leader.id, "2");
        assert_eq!(leader.addr, "172.30.0.12:9335");
    }

    #[test]
    fn deserialize_not_leader_body_no_leader() {
        // During an election the leader field is null.
        let json = r#"{"error": "no leader", "leader": null}"#;
        let nl: NotLeaderBody = serde_json::from_str(json).unwrap();
        assert!(nl.leader.is_none());
    }

    #[test]
    fn serialize_add_master_request() {
        let req = AddMasterRequest {
            id: 4,
            addr: "172.30.0.14:9335".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"id\":4"));
        assert!(json.contains("172.30.0.14:9335"));
    }

    #[test]
    fn serialize_maintenance_request() {
        let req = MaintenanceRequest { enabled: true };
        assert_eq!(serde_json::to_string(&req).unwrap(), "{\"enabled\":true}");
    }
}
