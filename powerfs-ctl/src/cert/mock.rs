//! In-memory `CertClient` for testing handlers without touching the network.
//! Mirrors the `MockProbe` / `MockComposeDriver` pattern from M2.

use super::{
    CertClient, IssuedCert, RegistryEntry, RenewRequest, RevokeRequest, SignClientRequest,
};
use async_trait::async_trait;
use std::sync::Mutex;

pub struct MockCertClient {
    /// What `fetch_ca` returns. `None` = never called.
    pub ca_response: Mutex<Option<Result<String, String>>>,
    /// What `sign_client` returns.
    pub sign_response: Mutex<Option<Result<IssuedCert, String>>>,
    /// Every `sign_client` request received, in call order.
    pub sign_requests: Mutex<Vec<SignClientRequest>>,
    /// What `list_certs` returns.
    pub list_response: Mutex<Option<Result<Vec<RegistryEntry>, String>>>,
    /// What `renew` returns.
    pub renew_response: Mutex<Option<Result<IssuedCert, String>>>,
    /// Every `renew` request received, in call order.
    pub renew_requests: Mutex<Vec<RenewRequest>>,
    /// What `revoke` returns.
    pub revoke_response: Mutex<Option<Result<(), String>>>,
    /// Every `revoke` request received, in call order.
    pub revoke_requests: Mutex<Vec<RevokeRequest>>,
}

impl MockCertClient {
    pub fn new() -> Self {
        Self {
            ca_response: Mutex::new(None),
            sign_response: Mutex::new(None),
            sign_requests: Mutex::new(Vec::new()),
            list_response: Mutex::new(None),
            renew_response: Mutex::new(None),
            renew_requests: Mutex::new(Vec::new()),
            revoke_response: Mutex::new(None),
            revoke_requests: Mutex::new(Vec::new()),
        }
    }

    /// `fetch_ca` will return this PEM (or this Err).
    pub fn with_ca(self, resp: Result<String, String>) -> Self {
        *self.ca_response.lock().unwrap() = Some(resp);
        self
    }

    /// `sign_client` will return this IssuedCert (or this Err).
    pub fn with_sign(self, resp: Result<IssuedCert, String>) -> Self {
        *self.sign_response.lock().unwrap() = Some(resp);
        self
    }

    /// `list_certs` will return this vec (or this Err).
    pub fn with_list(self, resp: Result<Vec<RegistryEntry>, String>) -> Self {
        *self.list_response.lock().unwrap() = Some(resp);
        self
    }

    /// `renew` will return this IssuedCert (or this Err).
    pub fn with_renew(self, resp: Result<IssuedCert, String>) -> Self {
        *self.renew_response.lock().unwrap() = Some(resp);
        self
    }

    /// `revoke` will return this Ok/Err.
    pub fn with_revoke(self, resp: Result<(), String>) -> Self {
        *self.revoke_response.lock().unwrap() = Some(resp);
        self
    }

    pub fn sign_calls(&self) -> Vec<SignClientRequest> {
        self.sign_requests.lock().unwrap().clone()
    }

    pub fn renew_calls(&self) -> Vec<RenewRequest> {
        self.renew_requests.lock().unwrap().clone()
    }

    pub fn revoke_calls(&self) -> Vec<RevokeRequest> {
        self.revoke_requests.lock().unwrap().clone()
    }
}

impl Default for MockCertClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CertClient for MockCertClient {
    async fn fetch_ca(&self, _master_api: &str, _admin_token: &str) -> Result<String, String> {
        self.ca_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Err("mock: fetch_ca not configured".into()))
    }

    async fn sign_client(
        &self,
        _master_api: &str,
        _admin_token: &str,
        req: &SignClientRequest,
    ) -> Result<IssuedCert, String> {
        self.sign_requests.lock().unwrap().push(req.clone());
        self.sign_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Err("mock: sign_client not configured".into()))
    }

    async fn list_certs(
        &self,
        _master_api: &str,
        _admin_token: &str,
    ) -> Result<Vec<RegistryEntry>, String> {
        self.list_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Err("mock: list_certs not configured".into()))
    }

    async fn renew(
        &self,
        _master_api: &str,
        _admin_token: &str,
        req: &RenewRequest,
    ) -> Result<IssuedCert, String> {
        self.renew_requests.lock().unwrap().push(req.clone());
        self.renew_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Err("mock: renew not configured".into()))
    }

    async fn revoke(
        &self,
        _master_api: &str,
        _admin_token: &str,
        req: &RevokeRequest,
    ) -> Result<(), String> {
        self.revoke_requests.lock().unwrap().push(req.clone());
        self.revoke_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Err("mock: revoke not configured".into()))
    }
}
