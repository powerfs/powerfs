//! Master Client - Volume → Master TLV 心跳客户端
//!
//! 基于 [`TlvMasterClient`] 长连接实现。`TlvMasterClient` 内部使用
//! `PowerFsNetClient` 维护与 master 的持久 TCP 连接, 支持自动重连、
//! leader 重定向和端点故障转移。
//!
//! 心跳间隔由调用方 (main.rs) 控制 (默认 5 秒), 本模块仅提供
//! `start_heartbeat` (初始化) 和 `send_heartbeat` (发送 TLV 请求)。
//! 连接生命周期由 `TlvMasterClient` 管理 — 首次 `send_heartbeat` 时
//! 按需建立连接, 后续复用同一条连接。

use log::{debug, info};
use powerfs_common::types::NodeId;
use powerfs_master::proto::VolumeShortInfo;
use powerfs_master_net::{TlvMasterClient, TlvMasterClientConfig};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::{FieldId, MsgType, Transport, STATUS_OK};
use std::sync::Arc;
use std::time::Duration;

pub struct MasterClient {
    node_id: NodeId,
    http_port: u32,
    net_port: u32,
    ip: String,
    /// Registration token for node authentication. Sent in every Heartbeat
    /// TLV body (FieldId::RegistrationToken) so the master can verify the
    /// node is authorized to join.  None/empty = dev mode.
    registration_token: Option<String>,
    /// Client certificate PEM content for production node authentication.
    /// Sent via TLV FieldId::ClientCert(0xD4) in every Heartbeat body so
    /// the master can validate it against the CA.  Empty in dev mode.
    client_cert_pem: String,
    /// Long-lived TLV client — manages connection, redirect, failover.
    tlv_client: Arc<TlvMasterClient>,
}

pub struct NewMasterClientParams<'a> {
    /// master_addresses 配置项 (ip:http_port 格式)
    pub master_addresses: &'a [&'a str],
    /// Master 的 powerfs-net 端口
    pub master_net_port: u16,
    pub node_id: NodeId,
    pub http_port: u32,
    pub net_port: u32,
    pub ip: &'a str,
    /// Registration token for master authentication. None = dev mode.
    pub registration_token: Option<&'a str>,
    /// Client certificate PEM content for production authentication.
    /// Empty = dev mode (no cert, master without CA configured).
    pub client_cert_pem: &'a str,
    /// Optional transport (e.g. RDMA). None = TCP.
    pub transport: Option<Arc<dyn Transport>>,
}

impl MasterClient {
    pub fn new(params: NewMasterClientParams<'_>) -> Self {
        // 从 master_addresses ("ip:http_port") 提取 IP, 拼接 master_net_port
        let endpoints: Vec<(String, u16)> = params
            .master_addresses
            .iter()
            .map(|addr| {
                let ip = addr.rfind(':').map(|pos| &addr[..pos]).unwrap_or(addr);
                (ip.to_string(), params.master_net_port)
            })
            .collect();

        let tlv_config = TlvMasterClientConfig {
            client_type: powerfs_net::ClientType::Volume,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            max_retries: 3,
            max_redirects: 5,
            retry_backoff: Duration::from_millis(100),
            client_cert_pem: if params.client_cert_pem.is_empty() {
                None
            } else {
                Some(params.client_cert_pem.to_string())
            },
            transport: params.transport.clone(),
        };

        let tlv_client = Arc::new(TlvMasterClient::new(endpoints, tlv_config));

        MasterClient {
            node_id: params.node_id,
            http_port: params.http_port,
            net_port: params.net_port,
            ip: params.ip.to_string(),
            registration_token: params.registration_token.map(|s| s.to_string()),
            client_cert_pem: params.client_cert_pem.to_string(),
            tlv_client,
        }
    }

    /// 初始化心跳。长连接模式下无需显式建立连接 — 连接在首次
    /// `send_heartbeat` 时按需建立 (lazy connect), 由 `TlvMasterClient`
    /// 自动管理重连。此方法仅记录日志。
    pub async fn start_heartbeat(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "VOLUME_HEARTBEAT: long-lived TLV heartbeat initialized (lazy connect on first send)"
        );
        Ok(())
    }

    /// 通过 TLV MsgType::Heartbeat 向 Master 发送心跳。
    ///
    /// 使用 `TlvMasterClient::submit_request` 发送请求, 它自动处理:
    ///   - 连接建立 (首次调用时 lazy connect)
    ///   - 连接维持 (后续调用复用同一条 TCP 连接)
    ///   - 自动重连 (连接断开时 `PowerFsNetClient` 内部重连)
    ///   - leader 重定向 (STATUS_ERR_REDIRECT → 切换到 leader 重试)
    ///   - 端点故障转移 (传输错误 → 切换到下一个 master)
    pub async fn send_heartbeat(
        &self,
        volumes: Vec<VolumeShortInfo>,
        cpu_usage: f32,
        memory_usage: f32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 构建 Heartbeat TLV 请求 body
        let mut enc = TlvEncoder::new();
        let _ = enc.add_string(FieldId::ClientId, &self.node_id.0);
        let _ = enc.add_string(FieldId::Owner, &self.ip);
        let _ = enc.add_u64(FieldId::Blksize, self.http_port as u64);
        let _ = enc.add_u64(FieldId::NetPort, self.net_port as u64);
        let _ = enc.add_u64(FieldId::Entries, volumes.len() as u64);

        // P5: Node load metrics — scaled to basis points (0-10000 = 0.00%-100.00%).
        let cpu_bps = (cpu_usage.clamp(0.0, 1.0) * 10000.0) as u64;
        let mem_bps = (memory_usage.clamp(0.0, 1.0) * 10000.0) as u64;
        let _ = enc.add_u64(FieldId::CpuUsage, cpu_bps);
        let _ = enc.add_u64(FieldId::MemoryUsage, mem_bps);

        // Registration token for node authentication.
        if let Some(token) = &self.registration_token {
            if !token.is_empty() {
                let _ = enc.add_string(FieldId::RegistrationToken, token);
            }
        }
        // Client certificate (PEM) for production node authentication.
        if !self.client_cert_pem.is_empty() {
            let _ = enc.add_string(FieldId::ClientCert, &self.client_cert_pem);
        }

        for vol in volumes {
            let _ = enc.add_u64(FieldId::Ino, vol.volume_id);
            let _ = enc.add_u64(FieldId::Size, vol.size);
            let _ = enc.add_u64(FieldId::Mode, vol.read_only as u64);
            let _ = enc.add_string(FieldId::Name, &vol.collection);
            let _ = enc.add_u64(FieldId::UsedSpace, vol.used);
            let _ = enc.add_u64(FieldId::FileCount, vol.file_count);
        }
        let body = enc.into_bytes();

        debug!(
            "VOLUME_HEARTBEAT: sending via long-lived connection (volumes={})",
            body.len()
        );

        // TlvMasterClient 自动处理连接、重定向、故障转移
        let reply = self
            .tlv_client
            .submit_request(MsgType::Heartbeat, &body)
            .await
            .map_err(|e| format!("heartbeat submit_request failed: {}", e))?;

        if reply.header.status != STATUS_OK {
            return Err(format!("Heartbeat failed: status={:#06x}", reply.header.status).into());
        }

        // 解析成功响应 (leader + volume_size_limit)
        let mut dec = TlvDecoder::new(&reply.body);
        let leader = dec.next_string(FieldId::Owner).unwrap_or_default();
        let volume_size_limit = dec.next_u64(FieldId::Size).unwrap_or(0);

        debug!(
            "VOLUME_HEARTBEAT: heartbeat ok, leader={}, volume_size_limit={}",
            leader, volume_size_limit
        );

        Ok(())
    }
}
