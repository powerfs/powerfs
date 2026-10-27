//! 存储后端配置：Filer 元数据端点、master 分配端点与写入策略。

use std::time::Duration;

use rustfs_ecstore::api::error::{Error, Result};

/// master 端点列表，逗号分隔，形如 `host:port,host:port`。
pub const ENV_MASTER_ENDPOINTS: &str = "POWERFS_MASTER_ENDPOINTS";
/// Filer gRPC 端点列表，逗号分隔，形如 `host:port,host:port`。
pub const ENV_FILER_ENDPOINTS: &str = "POWERFS_FILER_ENDPOINTS";
/// 写入时使用的 collection 名称。
pub const ENV_S3_COLLECTION: &str = "POWERFS_S3_COLLECTION";
/// 写入时使用的 replication 策略。
pub const ENV_S3_REPLICATION: &str = "POWERFS_S3_REPLICATION";
/// Filer 写请求使用的客户端标识；缺省为进程级 UUID v4。
pub const ENV_CLIENT_ID: &str = "POWERFS_S3_CLIENT_ID";
/// Filer/volume RPC 超时（毫秒）。
pub const ENV_RPC_TIMEOUT_MS: &str = "POWERFS_RPC_TIMEOUT_MS";

const DEFAULT_COLLECTION: &str = "default";
const DEFAULT_REPLICATION: &str = "001";
const DEFAULT_RPC_TIMEOUT_MS: u64 = 5000;

/// PowerFS 存储后端配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreConfig {
    /// master gRPC 端点（`host:port`，不带 scheme）。
    pub master_endpoints: Vec<String>,
    /// Filer gRPC 端点（`host:port`，不带 scheme）。
    pub filer_endpoints: Vec<String>,
    /// 分配 volume 时使用的 collection。
    pub collection: String,
    /// 分配 volume 时使用的 replication 策略。
    pub replication: String,
    /// Filer 写请求的 `client_id`。
    pub client_id: String,
    /// Filer/volume RPC 超时。
    pub rpc_timeout: Duration,
}

impl StoreConfig {
    /// 从环境变量读取配置。
    ///
    /// - `POWERFS_MASTER_ENDPOINTS`：必填，逗号分隔；
    /// - `POWERFS_FILER_ENDPOINTS`：必填，逗号分隔；
    /// - `POWERFS_S3_COLLECTION`：默认 `default`；
    /// - `POWERFS_S3_REPLICATION`：默认 `001`；
    /// - `POWERFS_S3_CLIENT_ID`：默认进程级 UUID v4；
    /// - `POWERFS_RPC_TIMEOUT_MS`：默认 `5000`。
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            master_endpoints: require_endpoints(ENV_MASTER_ENDPOINTS)?,
            filer_endpoints: require_endpoints(ENV_FILER_ENDPOINTS)?,
            collection: read_or_default(ENV_S3_COLLECTION, DEFAULT_COLLECTION),
            replication: read_or_default(ENV_S3_REPLICATION, DEFAULT_REPLICATION),
            client_id: read_or_else(ENV_CLIENT_ID, || uuid::Uuid::new_v4().to_string()),
            rpc_timeout: Duration::from_millis(read_u64_or_default(
                ENV_RPC_TIMEOUT_MS,
                DEFAULT_RPC_TIMEOUT_MS,
            )),
        })
    }
}

/// 读取必填的端点列表；缺失或为空时报错，避免启动后才发现配置不全。
fn require_endpoints(key: &str) -> Result<Vec<String>> {
    let raw = std::env::var(key).unwrap_or_default();
    let endpoints = parse_endpoints(&raw);
    if endpoints.is_empty() {
        return Err(Error::other(format!(
            "环境变量 {key} 未设置或为空，无法确定端点"
        )));
    }
    Ok(endpoints)
}

/// 解析逗号分隔的端点列表，去掉空白项。
fn parse_endpoints(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn read_or_default(key: &str, default: &str) -> String {
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => default.to_string(),
    }
}

fn read_or_else(key: &str, fallback: impl FnOnce() -> String) -> String {
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => fallback(),
    }
}

fn read_u64_or_default(key: &str, default: u64) -> u64 {
    match std::env::var(key) {
        Ok(value) => value.trim().parse().unwrap_or(default),
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_endpoints_splits_and_trims_blank_items() {
        let endpoints = parse_endpoints(" 10.0.0.1:9000 , ,10.0.0.2:9000 ");
        assert_eq!(endpoints, vec!["10.0.0.1:9000", "10.0.0.2:9000"]);
        assert!(parse_endpoints("  ,, ").is_empty());
    }

    #[test]
    fn duration_default_is_five_seconds() {
        assert_eq!(DEFAULT_RPC_TIMEOUT_MS, 5000);
    }
}
