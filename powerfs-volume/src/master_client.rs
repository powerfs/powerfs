//! Master Client - Volume → Master TLV 心跳客户端
//!
//! Phase A2: 从 gRPC SendHeartbeat 流式心跳迁移到 TLV MsgType::Heartbeat
//! 请求-响应模式。每次心跳建立短连接发送 TLV 请求, 处理 REDIRECT 重定向
//! 到 leader。移除了 gRPC 依赖 (MasterServiceClient, Channel, tonic) 和
//! 死代码 grow() 函数。
//!
//! 心跳间隔由调用方 (main.rs) 控制 (默认 5 秒), 本模块仅提供
//! start_heartbeat (初始化标志) 和 send_heartbeat (发送 TLV 请求)。

use log::{debug, info, warn};
use powerfs_common::types::NodeId;
use powerfs_master::proto::VolumeShortInfo;
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::{FieldId, STATUS_ERR_REDIRECT, STATUS_OK};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct MasterClient {
    /// Master 的 TLV 地址列表 (ip:master_net_port)
    master_net_addresses: Vec<String>,
    current_master_index: Arc<AtomicUsize>,
    node_id: NodeId,
    http_port: u32,
    net_port: u32,
    ip: String,
    heartbeat_running: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct NewMasterClientParams<'a> {
    /// master_addresses 配置项 (ip:http_port 格式)
    pub master_addresses: &'a [&'a str],
    /// Master 的 powerfs-net 端口
    pub master_net_port: u16,
    pub node_id: NodeId,
    pub http_port: u32,
    pub net_port: u32,
    pub ip: &'a str,
}

impl MasterClient {
    pub fn new(params: NewMasterClientParams<'_>) -> Self {
        // 从 master_addresses ("ip:http_port") 提取 IP, 拼接 master_net_port
        let master_net_addresses: Vec<String> = params
            .master_addresses
            .iter()
            .map(|addr| {
                let ip = addr.rfind(':').map(|pos| &addr[..pos]).unwrap_or(addr);
                format!("{}:{}", ip, params.master_net_port)
            })
            .collect();

        MasterClient {
            master_net_addresses,
            current_master_index: Arc::new(AtomicUsize::new(0)),
            node_id: params.node_id,
            http_port: params.http_port,
            net_port: params.net_port,
            ip: params.ip.to_string(),
            heartbeat_running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn current_master(&self) -> String {
        let idx = self.current_master_index.load(Ordering::Relaxed);
        self.master_net_addresses
            .get(idx)
            .cloned()
            .unwrap_or_else(|| self.master_net_addresses[0].clone())
    }

    fn next_master(&self) {
        let current = self.current_master_index.load(Ordering::Relaxed);
        let next = (current + 1) % self.master_net_addresses.len();
        self.current_master_index.store(next, Ordering::Relaxed);
    }

    /// 初始化心跳。TLV 模式下无需建立持久流, 仅标记运行状态。
    /// 实际心跳由 main.rs 循环调用 send_heartbeat 发送。
    pub async fn start_heartbeat(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self
            .heartbeat_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            info!("VOLUME_HEARTBEAT: heartbeat already running, skipping");
            return Ok(());
        }
        info!("VOLUME_HEARTBEAT: TLV heartbeat initialized (master_net={:?})",
              self.master_net_addresses);
        Ok(())
    }

    /// 通过 TLV MsgType::Heartbeat 向 Master 发送心跳。
    ///
    /// 流程:
    ///   1. 连接当前 master (ip:master_net_port)
    ///   2. powerfs-net 握手
    ///   3. 发送 Heartbeat TLV 请求 (node_id, ip, ports, volumes)
    ///   4. 读取响应:
    ///      - STATUS_OK: 心跳成功, 记录 leader 地址
    ///      - STATUS_ERR_REDIRECT: 切换到 leader 地址, 重试 (最多 MAX_REDIRECTS 次)
    ///      - 其他错误: 切换到下一个 master, 由调用方重试
    pub async fn send_heartbeat(
        &self,
        volumes: Vec<VolumeShortInfo>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const MAX_REDIRECTS: usize = 5;
        let mut current_addr = self.current_master();

        for depth in 0..MAX_REDIRECTS {
            debug!(
                "VOLUME_HEARTBEAT: sending to {} (depth={}, volumes={})",
                current_addr,
                depth,
                volumes.len()
            );

            match self.send_heartbeat_once(&current_addr, &volumes).await {
                Ok(leader) => {
                    // 心跳成功; 如果 leader 指向不同的 master, 更新索引
                    if !leader.is_empty() && leader != current_addr {
                        if let Some(idx) = self
                            .master_net_addresses
                            .iter()
                            .position(|a| {
                                // 按主机匹配 (leader 可能带不同端口)
                                let leader_host =
                                    leader.split(':').next().unwrap_or(&leader);
                                let addr_host = a.split(':').next().unwrap_or(a);
                                leader_host == addr_host
                            })
                        {
                            let current = self.current_master_index.load(Ordering::Relaxed);
                            if idx != current {
                                info!(
                                    "VOLUME_HEARTBEAT: switching to leader master: {} (index {})",
                                    leader, idx
                                );
                                self.current_master_index.store(idx, Ordering::Relaxed);
                            }
                        }
                    }
                    return Ok(());
                }
                Err(HeartbeatError::Redirect { leader }) => {
                    if leader.is_empty() {
                        return Err("redirected to empty leader address".into());
                    }
                    if leader == current_addr {
                        return Err(format!(
                            "redirect loop: master {} points to itself",
                            current_addr
                        )
                        .into());
                    }
                    warn!(
                        "VOLUME_HEARTBEAT: redirected to leader: {} (depth={})",
                        leader, depth
                    );
                    current_addr = leader;
                    continue;
                }
                Err(HeartbeatError::Connect(e)) => {
                    warn!(
                        "VOLUME_HEARTBEAT: connect to {} failed: {}; trying next master",
                        current_addr, e
                    );
                    self.next_master();
                    current_addr = self.current_master();
                    // 重置 depth 让新 master 有完整重试机会
                    continue;
                }
                Err(HeartbeatError::Other(e)) => {
                    warn!(
                        "VOLUME_HEARTBEAT: heartbeat to {} failed: {}; trying next master",
                        current_addr, e
                    );
                    self.next_master();
                    current_addr = self.current_master();
                    continue;
                }
            }
        }

        Err(format!(
            "exceeded {} redirects while sending heartbeat",
            MAX_REDIRECTS
        )
        .into())
    }

    /// 向指定 master 地址发送一次 Heartbeat TLV 请求。
    async fn send_heartbeat_once(
        &self,
        master_addr: &str,
        volumes: &[VolumeShortInfo],
    ) -> Result<String, HeartbeatError> {
        // 构建 Heartbeat TLV 请求
        let mut enc = TlvEncoder::new();
        let _ = enc.add_string(FieldId::ClientId, &self.node_id.0);
        let _ = enc.add_string(FieldId::Owner, &self.ip);
        let _ = enc.add_u64(FieldId::Blksize, self.http_port as u64);
        let _ = enc.add_u64(FieldId::NetPort, self.net_port as u64);
        let _ = enc.add_u64(FieldId::Entries, volumes.len() as u64);

        for vol in volumes {
            let _ = enc.add_u64(FieldId::Ino, vol.volume_id);
            let _ = enc.add_u64(FieldId::Size, vol.size);
            let _ = enc.add_u64(FieldId::Mode, vol.read_only as u64);
            let _ = enc.add_string(FieldId::Name, &vol.collection);
            let _ = enc.add_u64(FieldId::UsedSpace, vol.used);
            let _ = enc.add_u64(FieldId::FileCount, vol.file_count);
        }
        let body = enc.into_bytes();

        // 统一 RPC 客户端 (Layer A): connect → handshake → send → read
        let client_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
        let reply = powerfs_net::call_once(
            master_addr,
            powerfs_net::ClientType::Volume,
            client_id,
            powerfs_net::MsgType::Heartbeat,
            &body,
        )
        .await
        .map_err(|e| HeartbeatError::Connect(format!("transport to {}: {}", master_addr, e)))?;

        // 处理 REDIRECT
        if reply.status == STATUS_ERR_REDIRECT {
            let mut dec = TlvDecoder::new(&reply.body);
            let leader = dec.next_string(FieldId::Owner).unwrap_or_default();
            return Err(HeartbeatError::Redirect { leader });
        }

        if reply.status != STATUS_OK {
            return Err(HeartbeatError::Other(format!(
                "Heartbeat failed: status={:#06x}",
                reply.status
            )));
        }

        // 解析成功响应 (leader + volume_size_limit)
        let mut dec = TlvDecoder::new(&reply.body);
        let leader = dec.next_string(FieldId::Owner).unwrap_or_default();
        let volume_size_limit = dec.next_u64(FieldId::Size).unwrap_or(0);

        debug!(
            "VOLUME_HEARTBEAT: heartbeat ok, leader={}, volume_size_limit={}",
            leader, volume_size_limit
        );

        Ok(leader)
    }
}

/// 心跳请求错误类型, 区分重定向和连接错误以便上层处理。
enum HeartbeatError {
    /// Master 返回 REDIRECT, 需要切换到 leader 地址重试
    Redirect { leader: String },
    /// 连接失败, 需要切换到下一个 master
    Connect(String),
    /// 其他错误
    Other(String),
}
