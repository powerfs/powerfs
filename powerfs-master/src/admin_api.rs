//! Cluster administration HTTP API (M5).
//!
//! Routes (nested under `/api/admin` by `metrics.rs`):
//! ```text
//! GET    /masters                       raft membership snapshot
//! POST   /masters                        add master {id, addr}: learner → voter
//! DELETE /masters/{id}?force=            remove a master voter
//! POST   /nodes/{name}/maintenance       data-node maintenance on/off
//! DELETE /nodes/{name}?force=            safety-checked data-node removal
//! ```
//! All routes require `Authorization: Bearer <admin_token>` (dev mode with
//! an empty configured token is permissive, same semantics as the cert API).
//! Raft-mutating calls are leader-only: followers answer 503 with a
//! `leader` hint so clients can retry against the right node.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use log::warn;
use serde::Deserialize;

use powerfs_common::error::PowerFsError;

use crate::master::MasterNode;
use crate::raft_v2::{
    guard_remove_master, MastersSnapshot, MemberGuardError, MemberInfo, RaftNodeV2,
};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Admin operation failures. `IntoResponse` maps them to status codes:
/// 401 / 503(+leader hint) / 404 / 400 / 409 / 500.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminError {
    Unauthorized,
    NotLeader {
        /// Current leader, if known.
        leader: Option<MemberInfo>,
    },
    NotFound(String),
    BadRequest(String),
    Conflict(String),
    Internal(String),
}

impl AdminError {
    fn body(&self) -> serde_json::Value {
        let (msg, leader) = match self {
            AdminError::Unauthorized => ("unauthorized".to_string(), None),
            AdminError::NotLeader { leader } => (
                "not the raft leader; retry against the current leader".to_string(),
                leader.clone(),
            ),
            AdminError::NotFound(m) => (m.clone(), None),
            AdminError::BadRequest(m) => (m.clone(), None),
            AdminError::Conflict(m) => (m.clone(), None),
            AdminError::Internal(m) => (m.clone(), None),
        };
        serde_json::json!({ "error": msg, "leader": leader })
    }
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        let status = match &self {
            AdminError::Unauthorized => StatusCode::UNAUTHORIZED,
            AdminError::NotLeader { .. } => StatusCode::SERVICE_UNAVAILABLE,
            AdminError::NotFound(_) => StatusCode::NOT_FOUND,
            AdminError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AdminError::Conflict(_) => StatusCode::CONFLICT,
            AdminError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(self.body())).into_response()
    }
}

// ---------------------------------------------------------------------------
// Cluster operation abstraction (real impl + test mock)
// ---------------------------------------------------------------------------

/// Everything the admin handlers need from the running master. Implemented
/// by [`AdminState`] in production and by a scripted mock in tests.
#[async_trait]
pub trait AdminOps: Send + Sync {
    /// Constant-time admin-token check; an empty configured token = dev
    /// mode (everything accepted).
    fn verify_token(&self, provided: &str) -> bool;

    /// Whether this process currently holds raft leadership.
    fn is_leader(&self) -> bool;

    /// Current raft membership snapshot.
    async fn list_masters(&self) -> Result<MastersSnapshot, AdminError>;

    /// Add a master: blocking learner join followed by promotion to voter.
    /// Idempotent for members that are already voters/learners.
    async fn add_master(&self, id: u64, addr: String) -> Result<(), AdminError>;

    /// Commit a new voter set (computed by the handler's removal guard).
    /// `retain_existing_learners=false` semantics, so dropping a learner id
    /// also removes it.
    async fn commit_voters(&self, voters: Vec<String>) -> Result<(), AdminError>;

    /// Toggle a data-node's maintenance flag.
    async fn set_maintenance(&self, node: &str, enabled: bool) -> Result<(), AdminError>;

    /// Safety-checked data-node removal.
    async fn remove_data_node(&self, node: &str, force: bool) -> Result<(), AdminError>;
}

/// Production [`AdminOps`] implementation: shared handles into the real
/// raft node and [`MasterNode`].
pub struct AdminState {
    admin_token: Option<String>,
    raft: Arc<RaftNodeV2>,
    master: Arc<MasterNode>,
}

impl AdminState {
    pub fn new(
        admin_token: Option<String>,
        raft: Arc<RaftNodeV2>,
        master: Arc<MasterNode>,
    ) -> Self {
        Self {
            admin_token,
            raft,
            master,
        }
    }
}

/// Constant-time token comparison, byte-for-byte identical to
/// [`crate::ca_manager::CaManager::verify_admin_token`]: no/empty configured
/// token means dev mode (accept anything).
fn token_matches(configured: Option<&str>, provided: &str) -> bool {
    match configured {
        Some(expected) if !expected.is_empty() => {
            let a = provided.as_bytes();
            let b = expected.as_bytes();
            if a.len() != b.len() {
                return false;
            }
            let mut diff: u8 = 0;
            for (x, y) in a.iter().zip(b.iter()) {
                diff |= x ^ y;
            }
            diff == 0
        }
        _ => true,
    }
}

/// Build the NotLeader error, attaching the current leader's id+addr from
/// the raft metrics snapshot when one is known.
fn not_leader_error(snapshot: &MastersSnapshot) -> AdminError {
    let leader = snapshot
        .leader
        .as_ref()
        .and_then(|id| snapshot.members.iter().find(|m| &m.id == id).cloned());
    AdminError::NotLeader { leader }
}

/// Classify a raw raft-layer error string. openraft returns
/// `ForwardToLeader` when a mutating call hits a follower; everything else
/// is a server-side failure.
fn raft_err_to_admin(e: String) -> AdminError {
    if e.to_lowercase().contains("leader") {
        warn!("admin API: raft call rejected (not leader): {e}");
        AdminError::NotLeader { leader: None }
    } else {
        AdminError::Internal(e)
    }
}

fn master_err_to_admin(e: PowerFsError, snapshot: &MastersSnapshot) -> AdminError {
    match e {
        PowerFsError::NotLeader => not_leader_error(snapshot),
        PowerFsError::InvalidRequest(m) => AdminError::Conflict(m),
        other => AdminError::Internal(other.to_string()),
    }
}

#[async_trait]
impl AdminOps for AdminState {
    fn verify_token(&self, provided: &str) -> bool {
        token_matches(self.admin_token.as_deref(), provided)
    }

    fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }

    async fn list_masters(&self) -> Result<MastersSnapshot, AdminError> {
        Ok(self.raft.list_members())
    }

    async fn add_master(&self, id: u64, addr: String) -> Result<(), AdminError> {
        self.raft
            .add_voter(id, addr)
            .await
            .map_err(raft_err_to_admin)
    }

    async fn commit_voters(&self, voters: Vec<String>) -> Result<(), AdminError> {
        self.raft
            .set_voters(&voters)
            .await
            .map_err(raft_err_to_admin)
    }

    async fn set_maintenance(&self, node: &str, enabled: bool) -> Result<(), AdminError> {
        let snapshot = self.raft.list_members();
        self.master
            .set_node_maintenance(node, enabled)
            .await
            .map_err(|e| master_err_to_admin(e, &snapshot))
    }

    async fn remove_data_node(&self, node: &str, force: bool) -> Result<(), AdminError> {
        let snapshot = self.raft.list_members();
        self.master
            .remove_data_node_checked(node, force)
            .await
            .map_err(|e| master_err_to_admin(e, &snapshot))
    }
}

// ---------------------------------------------------------------------------
// HTTP DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct AddMasterRequest {
    /// Raft node id (numeric; matches the new master's `raft_id`).
    pub id: u64,
    /// Raft gRPC address of the new master (`ip:9335`).
    pub addr: String,
}

#[derive(Debug, Deserialize)]
pub struct MaintenanceRequest {
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct ForceQuery {
    #[serde(default)]
    pub force: bool,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn require_auth(ops: &dyn AdminOps, headers: &HeaderMap) -> Result<(), AdminError> {
    let provided = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if ops.verify_token(provided) {
        Ok(())
    } else {
        Err(AdminError::Unauthorized)
    }
}

/// Leader gate for mutating raft operations. Returns a 503-with-hint when
/// this node is not currently the leader.
async fn require_leader(ops: &dyn AdminOps) -> Result<(), AdminError> {
    if ops.is_leader() {
        return Ok(());
    }
    let snapshot = ops
        .list_masters()
        .await
        .unwrap_or_else(|_| MastersSnapshot {
            local: String::new(),
            leader: None,
            members: Vec::new(),
        });
    Err(not_leader_error(&snapshot))
}

pub async fn list_masters(
    State(ops): State<Arc<dyn AdminOps>>,
    headers: HeaderMap,
) -> Result<Json<MastersSnapshot>, AdminError> {
    require_auth(ops.as_ref(), &headers)?;
    Ok(Json(ops.list_masters().await?))
}

pub async fn add_master(
    State(ops): State<Arc<dyn AdminOps>>,
    headers: HeaderMap,
    Json(req): Json<AddMasterRequest>,
) -> Result<StatusCode, AdminError> {
    require_auth(ops.as_ref(), &headers)?;
    if req.addr.trim().is_empty() {
        return Err(AdminError::BadRequest("addr must not be empty".into()));
    }
    require_leader(ops.as_ref()).await?;
    ops.add_master(req.id, req.addr).await?;
    Ok(StatusCode::OK)
}

pub async fn remove_master(
    State(ops): State<Arc<dyn AdminOps>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<ForceQuery>,
) -> Result<StatusCode, AdminError> {
    require_auth(ops.as_ref(), &headers)?;
    require_leader(ops.as_ref()).await?;

    let snapshot = ops.list_masters().await?;
    let voters: Vec<String> = snapshot
        .members
        .iter()
        .filter(|m| m.role == "voter")
        .map(|m| m.id.clone())
        .collect();
    let known: Vec<String> = snapshot.members.iter().map(|m| m.id.clone()).collect();

    let remaining = guard_remove_master(&voters, &known, &id, snapshot.leader.as_deref(), q.force)
        .map_err(|e| match e {
            MemberGuardError::UnknownMember(m) => AdminError::NotFound(m),
            MemberGuardError::LastVoter(m) => AdminError::BadRequest(m),
            MemberGuardError::LeaderRemoval(m) => AdminError::Conflict(m),
        })?;

    ops.commit_voters(remaining).await?;
    Ok(StatusCode::OK)
}

pub async fn set_maintenance(
    State(ops): State<Arc<dyn AdminOps>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(req): Json<MaintenanceRequest>,
) -> Result<StatusCode, AdminError> {
    require_auth(ops.as_ref(), &headers)?;
    require_leader(ops.as_ref()).await?;
    ops.set_maintenance(&name, req.enabled).await?;
    Ok(StatusCode::OK)
}

pub async fn remove_data_node(
    State(ops): State<Arc<dyn AdminOps>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(q): Query<ForceQuery>,
) -> Result<StatusCode, AdminError> {
    require_auth(ops.as_ref(), &headers)?;
    require_leader(ops.as_ref()).await?;
    ops.remove_data_node(&name, q.force).await?;
    Ok(StatusCode::OK)
}

/// All `/api/admin/*` routes. `metrics.rs` provides the concrete
/// `Arc<AdminState>` via `.with_state(...)`.
pub fn admin_router() -> Router<Arc<dyn AdminOps>> {
    Router::new()
        .route("/masters", get(list_masters).post(add_master))
        .route("/masters/:id", axum::routing::delete(remove_master))
        .route("/nodes/:name/maintenance", post(set_maintenance))
        .route("/nodes/:name", axum::routing::delete(remove_data_node))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::Mutex;
    use tower::ServiceExt;

    const TOKEN: &str = "admin-token";

    fn member(id: &str, addr: &str, role: &str) -> MemberInfo {
        MemberInfo {
            id: id.into(),
            addr: addr.into(),
            role: role.into(),
        }
    }

    /// Scripted AdminOps: returns a fixed snapshot/leadership flag and
    /// records calls; optional per-op errors drive the status mapping tests.
    struct MockOps {
        token: Option<String>,
        leader: bool,
        snapshot: Mutex<MastersSnapshot>,
        add_calls: Mutex<Vec<(u64, String)>>,
        commit_calls: Mutex<Vec<Vec<String>>>,
        maintenance_calls: Mutex<Vec<(String, bool)>>,
        remove_calls: Mutex<Vec<(String, bool)>>,
        add_err: Mutex<Option<AdminError>>,
        commit_err: Mutex<Option<AdminError>>,
        maintenance_err: Mutex<Option<AdminError>>,
        remove_err: Mutex<Option<AdminError>>,
    }

    impl Default for MockOps {
        fn default() -> Self {
            Self {
                token: None,
                leader: false,
                snapshot: Mutex::new(MastersSnapshot {
                    local: String::new(),
                    leader: None,
                    members: Vec::new(),
                }),
                add_calls: Mutex::new(Vec::new()),
                commit_calls: Mutex::new(Vec::new()),
                maintenance_calls: Mutex::new(Vec::new()),
                remove_calls: Mutex::new(Vec::new()),
                add_err: Mutex::new(None),
                commit_err: Mutex::new(None),
                maintenance_err: Mutex::new(None),
                remove_err: Mutex::new(None),
            }
        }
    }

    impl MockOps {
        fn ha(leader: bool) -> Arc<Self> {
            Arc::new(MockOps {
                token: Some(TOKEN.into()),
                leader,
                snapshot: Mutex::new(MastersSnapshot {
                    local: "1".into(),
                    leader: Some("1".into()),
                    members: vec![
                        member("1", "10.0.0.1:9335", "voter"),
                        member("2", "10.0.0.2:9335", "voter"),
                        member("3", "10.0.0.3:9335", "voter"),
                    ],
                }),
                ..Default::default()
            })
        }

        fn single(leader: bool) -> Arc<Self> {
            Arc::new(MockOps {
                token: Some(TOKEN.into()),
                leader,
                snapshot: Mutex::new(MastersSnapshot {
                    local: "1".into(),
                    leader: Some("1".into()),
                    members: vec![member("1", "10.0.0.1:9335", "voter")],
                }),
                ..Default::default()
            })
        }

        fn arced(self: &Arc<Self>) -> Arc<dyn AdminOps> {
            self.clone() as Arc<dyn AdminOps>
        }
    }

    #[async_trait]
    impl AdminOps for MockOps {
        fn verify_token(&self, provided: &str) -> bool {
            token_matches(self.token.as_deref(), provided)
        }
        fn is_leader(&self) -> bool {
            self.leader
        }
        async fn list_masters(&self) -> Result<MastersSnapshot, AdminError> {
            Ok(self.snapshot.lock().unwrap().clone())
        }
        async fn add_master(&self, id: u64, addr: String) -> Result<(), AdminError> {
            self.add_calls.lock().unwrap().push((id, addr));
            self.add_err
                .lock()
                .unwrap()
                .clone()
                .map(Err)
                .unwrap_or(Ok(()))
        }
        async fn commit_voters(&self, voters: Vec<String>) -> Result<(), AdminError> {
            self.commit_calls.lock().unwrap().push(voters);
            self.commit_err
                .lock()
                .unwrap()
                .clone()
                .map(Err)
                .unwrap_or(Ok(()))
        }
        async fn set_maintenance(&self, node: &str, enabled: bool) -> Result<(), AdminError> {
            self.maintenance_calls
                .lock()
                .unwrap()
                .push((node.into(), enabled));
            self.maintenance_err
                .lock()
                .unwrap()
                .clone()
                .map(Err)
                .unwrap_or(Ok(()))
        }
        async fn remove_data_node(&self, node: &str, force: bool) -> Result<(), AdminError> {
            self.remove_calls.lock().unwrap().push((node.into(), force));
            self.remove_err
                .lock()
                .unwrap()
                .clone()
                .map(Err)
                .unwrap_or(Ok(()))
        }
    }

    async fn status_of(resp: Response) -> (u16, String) {
        let status = resp.status().as_u16();
        let bytes = hyper_014::body::to_bytes(resp.into_body()).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    fn req(method: &str, uri: &str, token: Option<&str>, body: &str) -> Request<Body> {
        let mut b = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(t) = token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        b.body(Body::from(body.to_string())).unwrap()
    }

    // ---- auth ----

    #[tokio::test]
    async fn auth_required_on_every_route() {
        for (method, uri, body) in [
            ("GET", "/masters", ""),
            ("POST", "/masters", r#"{"id":4,"addr":"10.0.0.4:9335"}"#),
            ("DELETE", "/masters/2?force=true", ""),
            (
                "POST",
                "/nodes/volume-server-1/maintenance",
                r#"{"enabled":true}"#,
            ),
            ("DELETE", "/nodes/volume-server-1", ""),
        ] {
            let app = admin_router().with_state(MockOps::ha(true).arced());
            let (status, _) =
                status_of(app.oneshot(req(method, uri, None, body)).await.unwrap()).await;
            assert_eq!(status, 401, "{method} {uri} without token");

            let app = admin_router().with_state(MockOps::ha(true).arced());
            let (status, _) = status_of(
                app.oneshot(req(method, uri, Some("wrong"), body))
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(status, 401, "{method} {uri} bad token");
        }
    }

    #[tokio::test]
    async fn dev_mode_empty_token_is_permissive() {
        let mut ops = MockOps::ha(true);
        Arc::get_mut(&mut ops).unwrap().token = Some(String::new());
        let app = admin_router().with_state(ops.arced());
        let (status, _) =
            status_of(app.oneshot(req("GET", "/masters", None, "")).await.unwrap()).await;
        assert_eq!(status, 200);
    }

    // ---- masters ----

    #[tokio::test]
    async fn list_masters_returns_snapshot() {
        let app = admin_router().with_state(MockOps::ha(false).arced());
        let (status, body) = status_of(
            app.oneshot(req("GET", "/masters", Some(TOKEN), ""))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, 200);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["local"], "1");
        assert_eq!(v["leader"], "1");
        assert_eq!(v["members"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn add_master_happy_path() {
        let mock = MockOps::ha(true);
        let app = admin_router().with_state(mock.arced());
        let (status, _) = status_of(
            app.oneshot(req(
                "POST",
                "/masters",
                Some(TOKEN),
                r#"{"id":4,"addr":"10.0.0.4:9335"}"#,
            ))
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(
            *mock.add_calls.lock().unwrap(),
            vec![(4, "10.0.0.4:9335".to_string())]
        );
    }

    #[tokio::test]
    async fn add_master_on_follower_is_503_with_leader_hint() {
        let app = admin_router().with_state(MockOps::ha(false).arced());
        let (status, body) = status_of(
            app.oneshot(req(
                "POST",
                "/masters",
                Some(TOKEN),
                r#"{"id":4,"addr":"10.0.0.4:9335"}"#,
            ))
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(status, 503);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["leader"]["id"], "1");
        assert_eq!(v["leader"]["addr"], "10.0.0.1:9335");
    }

    #[tokio::test]
    async fn add_master_rejects_empty_addr_and_bad_body() {
        let app = admin_router().with_state(MockOps::ha(true).arced());
        let (status, _) = status_of(
            app.clone()
                .oneshot(req(
                    "POST",
                    "/masters",
                    Some(TOKEN),
                    r#"{"id":4,"addr":""}"#,
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, 400);

        let (status, _) = status_of(
            app.oneshot(req("POST", "/masters", Some(TOKEN), r#"{"id":"x"}"#))
                .await
                .unwrap(),
        )
        .await;
        assert!(status == 400 || status == 422, "got {status}");
    }

    #[tokio::test]
    async fn remove_follower_commits_surviving_voters() {
        let mock = MockOps::ha(true);
        let app = admin_router().with_state(mock.arced());
        let (status, body) = status_of(
            app.oneshot(req("DELETE", "/masters/2", Some(TOKEN), ""))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, 200, "body={body}");
        assert_eq!(
            *mock.commit_calls.lock().unwrap(),
            vec![vec!["1".to_string(), "3".to_string()]]
        );
    }

    #[tokio::test]
    async fn remove_leader_needs_force() {
        let mock = MockOps::ha(true);
        let app = admin_router().with_state(mock.arced());

        let (status, _) = status_of(
            app.clone()
                .oneshot(req("DELETE", "/masters/1", Some(TOKEN), ""))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, 409);
        assert!(mock.commit_calls.lock().unwrap().is_empty());

        let (status, _) = status_of(
            app.oneshot(req("DELETE", "/masters/1?force=true", Some(TOKEN), ""))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, 200);
    }

    #[tokio::test]
    async fn remove_unknown_is_404_and_last_voter_is_400() {
        let app = admin_router().with_state(MockOps::ha(true).arced());
        let (status, _) = status_of(
            app.clone()
                .oneshot(req("DELETE", "/masters/9", Some(TOKEN), ""))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, 404);

        let app = admin_router().with_state(MockOps::single(true).arced());
        let (status, _) = status_of(
            app.oneshot(req("DELETE", "/masters/1?force=true", Some(TOKEN), ""))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, 400);
    }

    #[tokio::test]
    async fn remove_on_follower_is_503() {
        let app = admin_router().with_state(MockOps::ha(false).arced());
        let (status, _) = status_of(
            app.oneshot(req("DELETE", "/masters/2", Some(TOKEN), ""))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, 503);
    }

    // ---- data nodes ----

    #[tokio::test]
    async fn maintenance_happy_path() {
        let mock = MockOps::ha(true);
        let app = admin_router().with_state(mock.arced());
        let (status, _) = status_of(
            app.oneshot(req(
                "POST",
                "/nodes/volume-server-2/maintenance",
                Some(TOKEN),
                r#"{"enabled":true}"#,
            ))
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(
            *mock.maintenance_calls.lock().unwrap(),
            vec![("volume-server-2".to_string(), true)]
        );
    }

    #[tokio::test]
    async fn maintenance_bad_body() {
        let app = admin_router().with_state(MockOps::ha(true).arced());
        let (status, _) = status_of(
            app.oneshot(req(
                "POST",
                "/nodes/volume-server-2/maintenance",
                Some(TOKEN),
                "{}",
            ))
            .await
            .unwrap(),
        )
        .await;
        assert!(status == 400 || status == 422, "got {status}");
    }

    #[tokio::test]
    async fn remove_data_node_passes_force_and_maps_conflict() {
        let mock = MockOps::ha(true);
        let app = admin_router().with_state(mock.arced());
        let (status, _) = status_of(
            app.oneshot(req(
                "DELETE",
                "/nodes/volume-server-2?force=true",
                Some(TOKEN),
                "",
            ))
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(
            *mock.remove_calls.lock().unwrap(),
            vec![("volume-server-2".to_string(), true)]
        );

        let mock2 = MockOps::ha(true);
        *mock2.remove_err.lock().unwrap() =
            Some(AdminError::Conflict("owns 3 volume route(s)".into()));
        let app2 = admin_router().with_state(mock2.arced());
        let (status, body) = status_of(
            app2.oneshot(req("DELETE", "/nodes/v1", Some(TOKEN), ""))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, 409);
        assert!(body.contains("volume route"));
    }

    #[tokio::test]
    async fn maintenance_on_follower_is_503() {
        let app = admin_router().with_state(MockOps::ha(false).arced());
        let (status, _) = status_of(
            app.oneshot(req(
                "POST",
                "/nodes/volume-server-2/maintenance",
                Some(TOKEN),
                r#"{"enabled":true}"#,
            ))
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(status, 503);
    }
}
