//! Real `AdminClient` backed by reqwest — talks to master's `/api/admin/*`
//! HTTP endpoints (port 9300, same as CertClient). All admin calls are
//! raft-mutating → leader-only; followers 503 with a leader hint. This impl
//! retries once against the hinted leader's HTTP address (raft gRPC
//! `ip:9335` → `ip:9300`).

use super::{AddMasterRequest, AdminClient, MaintenanceRequest, MastersSnapshot, NotLeaderBody};
use async_trait::async_trait;
use reqwest::StatusCode;
use std::time::Duration;

pub struct MasterAdminClient {
    http: reqwest::Client,
}

impl MasterAdminClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client build"),
        }
    }

    /// Build and send a request. `body` is JSON-encoded for POST; None for
    /// GET/DELETE. Returns the raw response (caller checks status + body).
    async fn send(
        &self,
        url: &str,
        admin_token: &str,
        method: reqwest::Method,
        body: Option<&str>,
    ) -> Result<reqwest::Response, String> {
        let mut rb = self
            .http
            .request(method.clone(), url)
            .bearer_auth(admin_token);
        if let Some(b) = body {
            rb = rb
                .header("content-type", "application/json")
                .body(b.to_string());
        }
        rb.send()
            .await
            .map_err(|e| format!("{} {}: {}", method, url, e))
    }

    /// Execute a request; on 503, parse the leader hint from the body (raft
    /// gRPC `ip:9335` → HTTP `ip:9300`) and retry once. All other statuses
    /// (including non-503 errors) are returned as-is for the caller to handle.
    async fn call(
        &self,
        master_api: &str,
        admin_token: &str,
        path: &str,
        method: reqwest::Method,
        body: Option<&str>,
    ) -> Result<reqwest::Response, String> {
        let url = format!("http://{}{}", master_api, path);
        let resp = self.send(&url, admin_token, method.clone(), body).await?;

        if resp.status() != StatusCode::SERVICE_UNAVAILABLE {
            return Ok(resp);
        }

        // 503 — parse leader hint and retry once.
        let text = resp.text().await.unwrap_or_default();
        let new_api = parse_leader_hint(&text)
            .ok_or_else(|| format!("HTTP 503 from {} but no leader hint in body: {}", url, text))?;
        let new_url = format!("http://{}{}", new_api, path);
        self.send(&new_url, admin_token, method, body).await
    }
}

impl Default for MasterAdminClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract the leader's HTTP address from a 503 response body. The master
/// returns `{"leader": {"addr": "ip:9335", ...}}` where `addr` is the raft
/// gRPC listen address. We swap the port to 9300 (HTTP metrics port) and
/// return `"ip:9300"`.
fn parse_leader_hint(body: &str) -> Option<String> {
    let nl: NotLeaderBody = serde_json::from_str(body).ok()?;
    let leader = nl.leader?;
    Some(swap_grpc_to_http(&leader.addr))
}

/// `"ip:9335"` → `"ip:9300"`. Falls back to appending `:9300` if no port
/// separator is found (shouldn't happen in practice but avoids a panic).
fn swap_grpc_to_http(addr: &str) -> String {
    match addr.rfind(':') {
        Some(idx) => format!("{}:9300", &addr[..idx]),
        None => format!("{}:9300", addr),
    }
}

#[async_trait]
impl AdminClient for MasterAdminClient {
    async fn list_masters(
        &self,
        master_api: &str,
        admin_token: &str,
    ) -> Result<MastersSnapshot, String> {
        let resp = self
            .call(
                master_api,
                admin_token,
                "/api/admin/masters",
                reqwest::Method::GET,
                None,
            )
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!(
                "GET /api/admin/masters -> HTTP {}: {}",
                status, body
            ));
        }
        resp.json::<MastersSnapshot>()
            .await
            .map_err(|e| format!("decode list response: {}", e))
    }

    async fn add_master(
        &self,
        master_api: &str,
        admin_token: &str,
        req: &AddMasterRequest,
    ) -> Result<(), String> {
        let body = serde_json::to_string(req).map_err(|e| format!("encode body: {}", e))?;
        let resp = self
            .call(
                master_api,
                admin_token,
                "/api/admin/masters",
                reqwest::Method::POST,
                Some(&body),
            )
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(format!(
            "POST /api/admin/masters -> HTTP {}: {}",
            status, body
        ))
    }

    async fn remove_master(
        &self,
        master_api: &str,
        admin_token: &str,
        id: &str,
        force: bool,
    ) -> Result<(), String> {
        let path = format!("/api/admin/masters/{}?force={}", id, force);
        let resp = self
            .call(
                master_api,
                admin_token,
                &path,
                reqwest::Method::DELETE,
                None,
            )
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(format!("DELETE {} -> HTTP {}: {}", path, status, body))
    }

    async fn set_maintenance(
        &self,
        master_api: &str,
        admin_token: &str,
        name: &str,
        enabled: bool,
    ) -> Result<(), String> {
        let path = format!("/api/admin/nodes/{}/maintenance", name);
        let req = MaintenanceRequest { enabled };
        let body = serde_json::to_string(&req).map_err(|e| format!("encode body: {}", e))?;
        let resp = self
            .call(
                master_api,
                admin_token,
                &path,
                reqwest::Method::POST,
                Some(&body),
            )
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(format!("POST {} -> HTTP {}: {}", path, status, body))
    }

    async fn remove_node(
        &self,
        master_api: &str,
        admin_token: &str,
        name: &str,
        force: bool,
    ) -> Result<(), String> {
        let path = format!("/api/admin/nodes/{}?force={}", name, force);
        let resp = self
            .call(
                master_api,
                admin_token,
                &path,
                reqwest::Method::DELETE,
                None,
            )
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(format!("DELETE {} -> HTTP {}: {}", path, status, body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swap_grpc_to_http_replaces_port() {
        assert_eq!(swap_grpc_to_http("172.30.0.12:9335"), "172.30.0.12:9300");
        assert_eq!(swap_grpc_to_http("10.0.0.5:9335"), "10.0.0.5:9300");
    }

    #[test]
    fn swap_grpc_to_http_no_port() {
        assert_eq!(swap_grpc_to_http("172.30.0.12"), "172.30.0.12:9300");
    }

    #[test]
    fn parse_leader_hint_extracts_http_addr() {
        let body = r#"{"error":"not the raft leader","leader":{"id":"2","addr":"172.30.0.12:9335","role":"voter"}}"#;
        let api = parse_leader_hint(body).unwrap();
        assert_eq!(api, "172.30.0.12:9300");
    }

    #[test]
    fn parse_leader_hint_null_leader_returns_none() {
        let body = r#"{"error":"no leader","leader":null}"#;
        assert!(parse_leader_hint(body).is_none());
    }

    #[test]
    fn parse_leader_hint_garbage_returns_none() {
        assert!(parse_leader_hint("not json").is_none());
    }
}
