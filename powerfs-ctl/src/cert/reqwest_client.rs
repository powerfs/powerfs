//! Real `CertClient` backed by reqwest — talks to master's `/api/cert/*`
//! HTTP endpoints (master acts as cluster CA). Mirrors `ReqwestProbe`
//! patterns from M2 (plain HTTP on metrics port, no TLS).

use super::{
    CertClient, IssuedCert, RegistryEntry, RenewRequest, RevokeRequest, SignClientRequest,
};
use async_trait::async_trait;
use std::time::Duration;

pub struct MasterCertClient {
    http: reqwest::Client,
}

impl MasterCertClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client build"),
        }
    }
}

impl Default for MasterCertClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CertClient for MasterCertClient {
    async fn fetch_ca(&self, master_api: &str, admin_token: &str) -> Result<String, String> {
        let url = format!("http://{}/api/cert/ca", master_api);
        let resp = self
            .http
            .get(&url)
            .bearer_auth(admin_token)
            .send()
            .await
            .map_err(|e| format!("GET {}: {}", url, e))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("GET {} -> HTTP {}", url, status));
        }
        resp.text()
            .await
            .map_err(|e| format!("read {} body: {}", url, e))
    }

    async fn sign_client(
        &self,
        master_api: &str,
        admin_token: &str,
        req: &SignClientRequest,
    ) -> Result<IssuedCert, String> {
        let url = format!("http://{}/api/cert/sign-client", master_api);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(admin_token)
            .json(req)
            .send()
            .await
            .map_err(|e| format!("POST {}: {}", url, e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("POST {} -> HTTP {}: {}", url, status, body));
        }
        resp.json::<IssuedCert>()
            .await
            .map_err(|e| format!("decode {} response: {}", url, e))
    }

    async fn list_certs(
        &self,
        master_api: &str,
        admin_token: &str,
    ) -> Result<Vec<RegistryEntry>, String> {
        let url = format!("http://{}/api/cert/list", master_api);
        let resp = self
            .http
            .get(&url)
            .bearer_auth(admin_token)
            .send()
            .await
            .map_err(|e| format!("GET {}: {}", url, e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("GET {} -> HTTP {}: {}", url, status, body));
        }
        resp.json::<Vec<RegistryEntry>>()
            .await
            .map_err(|e| format!("decode {} response: {}", url, e))
    }

    async fn renew(
        &self,
        master_api: &str,
        admin_token: &str,
        req: &RenewRequest,
    ) -> Result<IssuedCert, String> {
        let url = format!("http://{}/api/cert/renew", master_api);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(admin_token)
            .json(req)
            .send()
            .await
            .map_err(|e| format!("POST {}: {}", url, e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("POST {} -> HTTP {}: {}", url, status, body));
        }
        resp.json::<IssuedCert>()
            .await
            .map_err(|e| format!("decode {} response: {}", url, e))
    }

    async fn revoke(
        &self,
        master_api: &str,
        admin_token: &str,
        req: &RevokeRequest,
    ) -> Result<(), String> {
        let url = format!("http://{}/api/cert/revoke", master_api);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(admin_token)
            .json(req)
            .send()
            .await
            .map_err(|e| format!("POST {}: {}", url, e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("POST {} -> HTTP {}: {}", url, status, body));
        }
        Ok(())
    }
}
