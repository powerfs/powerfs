//! Debug config poller: background task that fetches debug config from master
//! every 2 seconds and applies it locally via `dynamic_log`.
//!
//! ## 使用方式
//!
//! 在各服务 main.rs 中启动：
//! ```ignore
//! let poller = DebugConfigPoller::new(
//!     "fuse-1".to_string(),          // node_id
//!     master_endpoints.clone(),      // Vec<(String, u16)>
//! );
//! poller.start();
//! ```
//!
//! poller 内部 spawn 一个 tokio task，每 2s 调 master `GetDebugConfig`，
//! 拉到配置后与上次对比，变化时本地应用 `set_log_level` / `set_target_filter`
//! / `set_flag`，并 log 一条 info。

use log::{info, warn};
use std::time::Duration;

use powerfs_net::serialize::{self, DebugConfig};
use powerfs_net::{ClientConfig, ClientType, NetError, PowerFsNetClient, CHANNEL_META};

/// 轮询间隔（秒）
const POLL_INTERVAL_SECS: u64 = 2;

/// Debug config poller: 后台轮询 master 获取调试配置并本地应用。
pub struct DebugConfigPoller {
    node_id: String,
    master_endpoints: Vec<(String, u16)>,
}

impl DebugConfigPoller {
    pub fn new(node_id: String, master_endpoints: Vec<(String, u16)>) -> Self {
        Self {
            node_id,
            master_endpoints,
        }
    }

    /// 启动后台轮询 task。非阻塞，立即返回。
    pub fn start(self) {
        if self.master_endpoints.is_empty() {
            warn!(
                "DEBUG_CONFIG_POLLER: no master endpoints, polling disabled for node='{}'",
                self.node_id
            );
            return;
        }

        info!(
            "DEBUG_CONFIG_POLLER: starting for node='{}', masters={:?}",
            self.node_id, self.master_endpoints
        );

        tokio::spawn(async move {
            let mut last_config: Option<DebugConfig> = None;
            let mut endpoint_idx = 0usize;

            loop {
                tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;

                // 尝试当前 endpoint，失败则轮询下一个
                let mut fetched: Option<DebugConfig> = None;
                for attempt in 0..self.master_endpoints.len() {
                    let idx = (endpoint_idx + attempt) % self.master_endpoints.len();
                    let (host, port) = &self.master_endpoints[idx];
                    match fetch_config(host, *port, &self.node_id).await {
                        Ok(config) => {
                            fetched = Some(config);
                            endpoint_idx = idx; // 记住成功的 endpoint
                            break;
                        }
                        Err(e) => {
                            warn!(
                                "DEBUG_CONFIG_POLLER: fetch from {}:{} failed (attempt {}): {}",
                                host,
                                port,
                                attempt + 1,
                                e
                            );
                        }
                    }
                }

                let config = match fetched {
                    Some(c) => c,
                    None => continue,
                };

                // 与上次对比，变化时才应用
                let changed = match &last_config {
                    None => true,
                    Some(prev) => prev != &config,
                };

                if changed {
                    apply_config(&config, &self.node_id);
                    last_config = Some(config);
                }
            }
        });
    }
}

/// 从 master 拉取调试配置
async fn fetch_config(
    master_host: &str,
    master_net_port: u16,
    node_id: &str,
) -> Result<DebugConfig, NetError> {
    let config = ClientConfig {
        addr: master_host.to_string(),
        port: master_net_port,
        client_type: ClientType::Admin,
        channel: CHANNEL_META,
        request_timeout: Duration::from_secs(3),
        ..Default::default()
    };
    let client = PowerFsNetClient::new(config);
    client.connect().await?;

    let body = serialize::encode_get_debug_config_req(node_id)
        .map_err(|e| NetError::Protocol(format!("encode GetDebugConfig req: {}", e)))?;

    let resp = client
        .send_request(powerfs_net::MsgType::GetDebugConfig, &body, &[])
        .await?;

    serialize::decode_get_debug_config_resp(&resp.body)
        .map_err(|e| NetError::Protocol(format!("decode GetDebugConfig resp: {}", e)))
}

/// 本地应用调试配置.
///
/// 被两处调用:
///   1. (legacy poller path) `DebugConfigPoller::start` 每 2s 拉取配置后;
///   2. (push model) 各端 NotificationHandler 收到 Master 推送的
///      `DebugConfigChanged(0x008A)` NOTIFY 后, 反序列化 body 直接调用本函数.
/// 两条路径共享同一份应用逻辑, 不需要区分 node_id (纯日志/标记).
pub fn apply_config(config: &powerfs_net::serialize::DebugConfig, node_id: &str) {
    let mut changes = Vec::new();

    if let Some(level) = &config.log_level {
        if let Err(e) = crate::dynamic_log::set_log_level(level) {
            warn!(
                "DEBUG_CONFIG_POLLER: set_log_level('{}') failed: {}",
                level, e
            );
        } else {
            changes.push(format!("level={}", level));
        }
    }

    if let Some(filter) = &config.target_filter {
        crate::dynamic_log::set_target_filter(filter);
        changes.push(format!("target_filter='{}'", filter));
    }

    for (name, on) in &config.flags {
        crate::dynamic_log::set_flag(name, *on);
        changes.push(format!("{}={}", name, on));
    }

    if !changes.is_empty() {
        info!(
            "DEBUG_CONFIG_POLLER: applied config for node='{}': {}",
            node_id,
            changes.join(", ")
        );
    }
}
