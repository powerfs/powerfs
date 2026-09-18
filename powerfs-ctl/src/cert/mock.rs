//! In-memory `CertClient` for testing handlers without touching the network.
//! Mirrors the `MockProbe` / `MockComposeDriver` pattern from M2.

use super::{CertClient, IssuedCert, SignClientRequest};
use async_trait::async_trait;
use std::sync::Mutex;

pub struct MockCertClient {
    /// What `fetch_ca` returns. `None` = never called.
    pub ca_response: Mutex<Option<Result<String, String>>>,
    /// What `sign_client` returns.
    pub sign_response: Mutex<Option<Result<IssuedCert, String>>>,
    /// Every `sign_client` request received, in call order.
    pub sign_requests: Mutex<Vec<SignClientRequest>>,
}

impl MockCertClient {
    pub fn new() -> Self {
        Self {
            ca_response: Mutex::new(None),
            sign_response: Mutex::new(None),
            sign_requests: Mutex::new(Vec::new()),
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

    pub fn sign_calls(&self) -> Vec<SignClientRequest> {
        self.sign_requests.lock().unwrap().clone()
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
}
