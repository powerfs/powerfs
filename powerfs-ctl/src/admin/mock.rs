//! In-memory `AdminClient` for testing handlers without touching the network.
//! Mirrors the `MockCertClient` pattern: per-method response slots + call
//! recording.

use super::{AddMasterRequest, AdminClient, MastersSnapshot, MemberInfo};
use async_trait::async_trait;
use std::sync::Mutex;

/// Call records for methods that take parameters (for assertions in tests).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveCall {
    pub id: String,
    pub force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceCall {
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveNodeCall {
    pub name: String,
    pub force: bool,
}

pub struct MockAdminClient {
    pub list_response: Mutex<Option<Result<MastersSnapshot, String>>>,
    pub add_response: Mutex<Option<Result<(), String>>>,
    pub add_calls: Mutex<Vec<AddMasterRequest>>,
    pub remove_response: Mutex<Option<Result<(), String>>>,
    pub remove_calls: Mutex<Vec<RemoveCall>>,
    pub maintenance_response: Mutex<Option<Result<(), String>>>,
    pub maintenance_calls: Mutex<Vec<MaintenanceCall>>,
    pub remove_node_response: Mutex<Option<Result<(), String>>>,
    pub remove_node_calls: Mutex<Vec<RemoveNodeCall>>,
}

impl MockAdminClient {
    pub fn new() -> Self {
        Self {
            list_response: Mutex::new(None),
            add_response: Mutex::new(None),
            add_calls: Mutex::new(Vec::new()),
            remove_response: Mutex::new(None),
            remove_calls: Mutex::new(Vec::new()),
            maintenance_response: Mutex::new(None),
            maintenance_calls: Mutex::new(Vec::new()),
            remove_node_response: Mutex::new(None),
            remove_node_calls: Mutex::new(Vec::new()),
        }
    }

    pub fn with_list(self, resp: Result<MastersSnapshot, String>) -> Self {
        *self.list_response.lock().unwrap() = Some(resp);
        self
    }
    pub fn with_add(self, resp: Result<(), String>) -> Self {
        *self.add_response.lock().unwrap() = Some(resp);
        self
    }
    pub fn with_remove(self, resp: Result<(), String>) -> Self {
        *self.remove_response.lock().unwrap() = Some(resp);
        self
    }
    pub fn with_maintenance(self, resp: Result<(), String>) -> Self {
        *self.maintenance_response.lock().unwrap() = Some(resp);
        self
    }
    pub fn with_remove_node(self, resp: Result<(), String>) -> Self {
        *self.remove_node_response.lock().unwrap() = Some(resp);
        self
    }

    pub fn add_calls(&self) -> Vec<AddMasterRequest> {
        self.add_calls.lock().unwrap().clone()
    }
    pub fn remove_calls(&self) -> Vec<RemoveCall> {
        self.remove_calls.lock().unwrap().clone()
    }
    pub fn maintenance_calls(&self) -> Vec<MaintenanceCall> {
        self.maintenance_calls.lock().unwrap().clone()
    }
    pub fn remove_node_calls(&self) -> Vec<RemoveNodeCall> {
        self.remove_node_calls.lock().unwrap().clone()
    }
}

impl Default for MockAdminClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AdminClient for MockAdminClient {
    async fn list_masters(
        &self,
        _master_api: &str,
        _admin_token: &str,
    ) -> Result<MastersSnapshot, String> {
        self.list_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Err("mock: list_masters not configured".into()))
    }

    async fn add_master(
        &self,
        _master_api: &str,
        _admin_token: &str,
        req: &AddMasterRequest,
    ) -> Result<(), String> {
        self.add_calls.lock().unwrap().push(req.clone());
        self.add_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Err("mock: add_master not configured".into()))
    }

    async fn remove_master(
        &self,
        _master_api: &str,
        _admin_token: &str,
        id: &str,
        force: bool,
    ) -> Result<(), String> {
        self.remove_calls.lock().unwrap().push(RemoveCall {
            id: id.into(),
            force,
        });
        self.remove_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Err("mock: remove_master not configured".into()))
    }

    async fn set_maintenance(
        &self,
        _master_api: &str,
        _admin_token: &str,
        name: &str,
        enabled: bool,
    ) -> Result<(), String> {
        self.maintenance_calls
            .lock()
            .unwrap()
            .push(MaintenanceCall {
                name: name.into(),
                enabled,
            });
        self.maintenance_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Err("mock: set_maintenance not configured".into()))
    }

    async fn remove_node(
        &self,
        _master_api: &str,
        _admin_token: &str,
        name: &str,
        force: bool,
    ) -> Result<(), String> {
        self.remove_node_calls.lock().unwrap().push(RemoveNodeCall {
            name: name.into(),
            force,
        });
        self.remove_node_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Err("mock: remove_node not configured".into()))
    }
}

/// Convenience: a 3-voter snapshot for tests that just need a healthy cluster.
pub fn three_voter_snapshot() -> MastersSnapshot {
    MastersSnapshot {
        local: "1".into(),
        leader: Some("1".into()),
        members: vec![
            MemberInfo {
                id: "1".into(),
                addr: "172.30.0.11:9335".into(),
                role: "voter".into(),
            },
            MemberInfo {
                id: "2".into(),
                addr: "172.30.0.12:9335".into(),
                role: "voter".into(),
            },
            MemberInfo {
                id: "3".into(),
                addr: "172.30.0.13:9335".into(),
                role: "voter".into(),
            },
        ],
    }
}
