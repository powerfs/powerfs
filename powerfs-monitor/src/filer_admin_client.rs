//! Filer admin HTTP client — Monitor 作为唯一入口，前端不直连 filer。
//!
//! 设计原则（见 docs/filer-redesign-plan.md）：前端只跟 Monitor 交互。
//! Filer 进程的所有 `/admin/*` HTTP 接口由 Monitor 通过本模块代理调用，
//! reqwest → `http://{address}:{http_port}/admin/*`。
//!
//! 响应体用 `serde_json::Value` 透传给前端，不在 Monitor 里强类型化 filer
//! 内部数据结构（避免 filer 类型变更牵动 monitor 编译）。仅在需要拼接的
//! `/api/filer/nodes`（合并 gRPC ListFilers + 心跳）用强类型 `FilerNode`。

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Filer admin 调用错误。前端据此显示具体原因（节点不可达 vs 超时 vs 业务错）。
#[derive(Debug)]
pub enum FilerAdminError {
    /// node_id 在 master 注册表里找不到 — 节点未注册或已下线
    NodeNotFound(String),
    /// 节点注册了但 http_port=0 或 address 为空 — 配置不完整
    NoHttpEndpoint(String),
    /// reqwest 连接失败 / 超时 — filer 进程不可达
    Unreachable(String, String),
    /// filer 返回非 2xx — 业务错误（如 Raft 不可用）
    HttpStatus(reqwest::StatusCode, String),
    /// 响应体反序列化失败 — filer 版本不兼容
    Decode(String),
}

impl std::fmt::Display for FilerAdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeNotFound(id) => write!(f, "filer 节点 {} 未在 master 注册", id),
            Self::NoHttpEndpoint(id) => write!(f, "filer 节点 {} 未配置 http_port", id),
            Self::Unreachable(id, e) => write!(f, "filer 节点 {} 不可达: {}", id, e),
            Self::HttpStatus(code, body) => write!(f, "filer 返回 {} : {}", code, body),
            Self::Decode(e) => write!(f, "filer 响应解析失败: {}", e),
        }
    }
}

impl std::error::Error for FilerAdminError {}

/// 单个 filer 节点的定位信息 — 从 gRPC ListFilers 拿到。
/// 用于构造 `http://{address}:{http_port}/admin/*` URL。
#[derive(Debug, Clone)]
pub struct FilerEndpoint {
    pub node_id: String,
    pub address: String,
    pub http_port: u32,
}

/// 前端展示用的 filer 节点 — 合并 master 注册视角 + 心跳视角。
#[derive(Debug, Clone, Serialize)]
pub struct FilerNode {
    pub node_id: String,
    pub address: String,
    pub http_port: u32,
    pub grpc_port: u32,
    /// master 注册视角的静态健康（gRPC ListFilers.is_healthy）
    pub is_registered: bool,
    pub registered_healthy: bool,
    pub leader_count: u64,
    pub total_shards: u64,
    /// 心跳视角的真实健康（来自 metric_store，受 NODE_HEARTBEAT_TIMEOUT_SECS 控制）
    pub heartbeat_status: String,
    /// 距离上次心跳的秒数（前端展示 "3s 前"）
    pub last_seen_ago_secs: u64,
    pub cpu_usage: f64,
    pub mem_usage: f64,
    pub disk_usage: f64,
    pub uptime: u64,
}

/// Filer admin HTTP client — 共享 reqwest::Client（连接池复用）。
#[derive(Clone)]
pub struct FilerAdminClient {
    http: reqwest::Client,
}

impl FilerAdminClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .connect_timeout(Duration::from_secs(2))
                .build()
                .expect("failed to build reqwest client for filer admin"),
        }
    }

    fn url(&self, ep: &FilerEndpoint, path: &str) -> Result<String, FilerAdminError> {
        if ep.address.is_empty() || ep.http_port == 0 {
            return Err(FilerAdminError::NoHttpEndpoint(ep.node_id.clone()));
        }
        Ok(format!("http://{}:{}{}", ep.address, ep.http_port, path))
    }

    /// GET 透传 — 响应体原样转给前端（status / shards / balancer/status / balancer/config）
    pub async fn get_json(
        &self,
        ep: &FilerEndpoint,
        path: &str,
    ) -> Result<serde_json::Value, FilerAdminError> {
        let url = self.url(ep, path)?;
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| FilerAdminError::Unreachable(ep.node_id.clone(), e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(FilerAdminError::HttpStatus(status, body));
        }
        resp.json::<serde_json::Value>()
            .await
            .map_err(|e| FilerAdminError::Decode(e.to_string()))
    }

    /// POST 透传 — balancer/start / balancer/stop / balancer/trigger
    pub async fn post_json(
        &self,
        ep: &FilerEndpoint,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, FilerAdminError> {
        let url = self.url(ep, path)?;
        let mut req = self.http.post(&url);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| FilerAdminError::Unreachable(ep.node_id.clone(), e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(FilerAdminError::HttpStatus(status, body));
        }
        // POST 可能返回空 body（如 balancer/trigger），用空对象兜底
        resp.json::<serde_json::Value>()
            .await
            .or(Ok(serde_json::json!({})))
    }

    /// PUT 透传 — balancer/config
    pub async fn put_json(
        &self,
        ep: &FilerEndpoint,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, FilerAdminError> {
        let url = self.url(ep, path)?;
        let resp = self
            .http
            .put(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| FilerAdminError::Unreachable(ep.node_id.clone(), e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(FilerAdminError::HttpStatus(status, body));
        }
        resp.json::<serde_json::Value>()
            .await
            .or(Ok(serde_json::json!({})))
    }

    /// DELETE 透传 — admin/buckets/:name (删除 bucket)
    pub async fn delete_json(
        &self,
        ep: &FilerEndpoint,
        path: &str,
    ) -> Result<serde_json::Value, FilerAdminError> {
        let url = self.url(ep, path)?;
        let resp = self
            .http
            .delete(&url)
            .send()
            .await
            .map_err(|e| FilerAdminError::Unreachable(ep.node_id.clone(), e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(FilerAdminError::HttpStatus(status, body));
        }
        resp.json::<serde_json::Value>()
            .await
            .or(Ok(serde_json::json!({})))
    }
}

impl Default for FilerAdminClient {
    fn default() -> Self {
        Self::new()
    }
}

/// 路径参数 — axum 从 URL 提取
#[derive(Debug, Deserialize)]
pub struct FilerNodeIdParam {
    pub node_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ShardIdParam {
    pub shard_id: String,
}
