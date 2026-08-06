//! 抽象客户端连接与全局连接注册表
//!
//! 设计参考: BeeGFS StreamConn + 内核端 powerfs_net_server_conn
//!
//! 每个客户端连接对应一个 [`ClientConn`], 统一管理:
//! - 连接状态 (Active/Suspended/Closing/Closed)
//! - holder 身份 (UUID, 替代分散的 client_id_map)
//! - 持有的 lease (inode + token, 快速断连清理)
//! - 可配置属性 (优先级/限速/并发)
//! - 统计信息 (请求数/错误数/字节数)
//!
//! [`ConnRegistry`] 是全局连接注册表, 提供增删改查:
//! - register / unregister
//! - get / get_by_holder
//! - disconnect (主动断开)
//! - set_config (动态配置)
//! - list (管理/监控)

use crate::protocol::{ClientType, NetMessage};
use dashmap::DashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};

/// 连接状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
    /// 活跃: 正常收发
    Active,
    /// 挂起: 暂停收发 (限流/熔断)
    Suspended,
    /// 关闭中: 停止接收, 发送剩余响应
    Closing,
    /// 已关闭: 资源待清理
    Closed,
}

/// 客户端配置 (可动态修改)
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// 请求优先级 (0=最高)
    pub priority: u8,
    /// 速率限制 (req/s, 0=不限)
    pub rate_limit: u32,
    /// 最大并发请求
    pub max_concurrent: u16,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            priority: 8,
            rate_limit: 0,
            max_concurrent: 64,
        }
    }
}

/// 客户端统计
#[derive(Debug, Clone)]
pub struct ClientStats {
    pub request_count: u64,
    pub error_count: u64,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub connected_at: Instant,
    pub last_activity: Instant,
}

impl Default for ClientStats {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            request_count: 0,
            error_count: 0,
            bytes_sent: 0,
            bytes_recv: 0,
            connected_at: now,
            last_activity: now,
        }
    }
}

/// 出站帧通道 (server → client TCP 写入)
///
/// Worker 响应帧和 server 主动推送的通知帧都通过此通道发送,
/// 由 IoLoop 的 write_task 单独消费, 避免多任务竞争 write_half.
pub type OutboundTx = mpsc::UnboundedSender<Vec<u8>>;

/// 关闭句柄 (封装底层连接的关闭操作)
///
/// IoLoop 在 manage() 中创建, 通过 mpsc 通知读取循环退出.
#[derive(Clone, Debug)]
pub struct CloseHandle {
    shutdown_tx: mpsc::Sender<()>,
}

impl CloseHandle {
    pub fn new(shutdown_tx: mpsc::Sender<()>) -> Self {
        Self { shutdown_tx }
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(()).await;
    }
}

/// 抽象客户端连接
///
/// 每个客户端一个, 统一管理连接状态和资源.
/// 支持: disconnect() / set_config() / get_stats() / 查询 held_leases
#[derive(Debug)]
pub struct ClientConn {
    /// 客户端 ID (握手时分配)
    pub id: u64,
    /// Holder UUID (lease 持有者标识, 替代 client_id_map)
    pub holder_uuid: RwLock<Option<String>>,
    /// 客户端地址
    pub addr: SocketAddr,
    /// 客户端类型 (Fuse/Kernel/Admin)
    pub client_type: ClientType,

    /// 连接状态
    pub state: RwLock<ConnState>,
    /// 客户端配置 (可动态修改)
    pub config: RwLock<ClientConfig>,
    /// 客户端统计
    pub stats: RwLock<ClientStats>,

    /// 持有的 inode lease 列表 (快速断连清理)
    pub held_leases: RwLock<HashSet<u64>>,
    /// 持有的 lease token 列表
    pub held_tokens: RwLock<HashSet<String>>,

    /// 出站帧通道 (响应帧 + 通知帧), IoLoop write_task 消费
    pub outbound_tx: OutboundTx,
    /// 关闭句柄 (用于主动断开底层 TCP 连接)
    pub close_handle: RwLock<Option<CloseHandle>>,
}

impl ClientConn {
    pub fn new(
        id: u64,
        addr: SocketAddr,
        client_type: ClientType,
        outbound_tx: OutboundTx,
    ) -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            id,
            holder_uuid: RwLock::new(None),
            addr,
            client_type,
            state: RwLock::new(ConnState::Active),
            config: RwLock::new(ClientConfig::default()),
            stats: RwLock::new(ClientStats {
                connected_at: now,
                last_activity: now,
                ..Default::default()
            }),
            held_leases: RwLock::new(HashSet::new()),
            held_tokens: RwLock::new(HashSet::new()),
            outbound_tx,
            close_handle: RwLock::new(None),
        })
    }

    /// 设置 holder UUID (握手后调用)
    pub async fn set_holder(&self, holder: String) {
        *self.holder_uuid.write().await = Some(holder);
    }

    /// 获取 holder UUID (lease 校验时用)
    pub async fn holder(&self) -> Option<String> {
        self.holder_uuid.read().await.clone()
    }

    /// 添加持有的 lease
    pub async fn add_lease(&self, inode: u64, token: String) {
        self.held_leases.write().await.insert(inode);
        self.held_tokens.write().await.insert(token);
    }

    /// 移除持有的 lease
    pub async fn remove_lease(&self, inode: u64, token: &str) {
        self.held_leases.write().await.remove(&inode);
        self.held_tokens.write().await.remove(token);
    }

    /// 获取持有的 inode 列表 (断连清理时用)
    pub async fn held_inodes(&self) -> Vec<u64> {
        self.held_leases.read().await.iter().copied().collect()
    }

    /// 设置关闭句柄 (IoLoop.manage() 调用)
    pub async fn set_close_handle(&self, handle: CloseHandle) {
        *self.close_handle.write().await = Some(handle);
    }

    /// 主动断开连接
    pub async fn disconnect(&self) {
        {
            let mut state = self.state.write().await;
            *state = ConnState::Closing;
        }
        if let Some(handle) = self.close_handle.read().await.as_ref() {
            handle.shutdown().await;
        }
    }

    /// 更新活动时间
    pub async fn touch(&self) {
        self.stats.write().await.last_activity = Instant::now();
    }

    /// 发送响应帧 (Worker 调用)
    ///
    /// 将 NetMessage 序列化为 wire frame, 推送到 write_task 的出站通道.
    /// 非阻塞: 通道满或关闭时返回 false, 由调用方决定如何处理.
    pub fn send_response(&self, msg: &NetMessage) -> bool {
        self.outbound_tx.send(msg.to_frame()).is_ok()
    }

    /// 推送通知消息 (server → client, 用于 Invalidate 等)
    ///
    /// 与 send_response 共用 outbound_tx, 由 write_task 统一写入 TCP.
    pub fn notify(&self, msg: &NetMessage) -> bool {
        self.outbound_tx.send(msg.to_frame()).is_ok()
    }
}

/// 客户端信息摘要 (用于 list() 返回)
#[derive(Debug, Clone)]
pub struct ClientConnInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub client_type: ClientType,
    pub state: ConnState,
    pub holder: Option<String>,
    pub request_count: u64,
    pub error_count: u64,
}

/// 全局连接注册表 (替代分散的 client_id_map + sessions)
///
/// 线程安全, 使用 DashMap 支持高并发读写.
/// 提供: register / unregister / get / disconnect / set_config / list
pub struct ConnRegistry {
    /// client_id → ClientConn
    conns: DashMap<u64, Arc<ClientConn>>,
    /// holder_uuid → client_id (lease 校验时用)
    by_holder: DashMap<String, u64>,
}

impl ConnRegistry {
    pub fn new() -> Self {
        Self {
            conns: DashMap::new(),
            by_holder: DashMap::new(),
        }
    }

    /// 注册新连接
    pub async fn register(&self, conn: Arc<ClientConn>) {
        let id = conn.id;
        if let Some(holder) = conn.holder_uuid.read().await.as_ref() {
            self.by_holder.insert(holder.clone(), id);
        }
        self.conns.insert(id, conn);
    }

    /// 注销连接 (断连时调用)
    ///
    /// 返回被移除的 ClientConn (供 on_disconnect 清理 lease)
    pub async fn unregister(&self, id: u64) -> Option<Arc<ClientConn>> {
        let conn = self.conns.remove(&id).map(|(_, c)| c)?;
        if let Some(holder) = conn.holder_uuid.read().await.as_ref() {
            self.by_holder.remove(holder);
        }
        Some(conn)
    }

    /// 获取连接
    pub fn get(&self, id: u64) -> Option<Arc<ClientConn>> {
        self.conns.get(&id).map(|r| r.value().clone())
    }

    /// 通过 holder UUID 获取连接
    pub fn get_by_holder(&self, holder: &str) -> Option<Arc<ClientConn>> {
        let id = *self.by_holder.get(holder)?;
        self.get(id)
    }

    /// 主动断开指定连接
    pub async fn disconnect(&self, id: u64) -> bool {
        if let Some(conn) = self.get(id) {
            conn.disconnect().await;
            true
        } else {
            false
        }
    }

    /// 设置客户端配置
    pub async fn set_config(&self, id: u64, config: ClientConfig) -> bool {
        if let Some(conn) = self.get(id) {
            *conn.config.write().await = config;
            true
        } else {
            false
        }
    }

    /// 列出所有连接信息 (管理/监控)
    pub async fn list(&self) -> Vec<ClientConnInfo> {
        let mut result = Vec::new();
        for entry in self.conns.iter() {
            let conn = entry.value();
            let stats = conn.stats.read().await;
            result.push(ClientConnInfo {
                id: conn.id,
                addr: conn.addr,
                client_type: conn.client_type,
                state: *conn.state.read().await,
                holder: conn.holder_uuid.read().await.clone(),
                request_count: stats.request_count,
                error_count: stats.error_count,
            });
        }
        result
    }

    /// 活跃连接数
    pub fn count(&self) -> usize {
        self.conns.len()
    }
}

impl Default for ConnRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_conn(id: u64) -> Arc<ClientConn> {
        let (tx, _rx) = mpsc::unbounded_channel::<Vec<u8>>();
        ClientConn::new(
            id,
            "127.0.0.1:1234".parse().unwrap(),
            ClientType::Kernel,
            tx,
        )
    }

    #[tokio::test]
    async fn test_client_conn_lifecycle() {
        let conn = make_conn(42);

        // 初始状态
        assert_eq!(*conn.state.read().await, ConnState::Active);
        assert!(conn.holder().await.is_none());

        // 设置 holder
        conn.set_holder("uuid-abc".to_string()).await;
        assert_eq!(conn.holder().await.as_deref(), Some("uuid-abc"));

        // lease 操作
        conn.add_lease(100, "token1".to_string()).await;
        conn.add_lease(200, "token2".to_string()).await;
        assert_eq!(conn.held_inodes().await.len(), 2);

        conn.remove_lease(100, "token1").await;
        assert_eq!(conn.held_inodes().await.len(), 1);
        assert_eq!(conn.held_inodes().await[0], 200);
    }

    #[tokio::test]
    async fn test_conn_registry_register_unregister() {
        let registry = ConnRegistry::new();

        let conn = make_conn(1);
        conn.set_holder("holder-1".to_string()).await;
        registry.register(conn.clone()).await;

        assert_eq!(registry.count(), 1);
        assert!(registry.get(1).is_some());
        assert!(registry.get_by_holder("holder-1").is_some());

        let removed = registry.unregister(1).await;
        assert!(removed.is_some());
        assert_eq!(registry.count(), 0);
        assert!(registry.get(1).is_none());
        assert!(registry.get_by_holder("holder-1").is_none());
    }

    #[tokio::test]
    async fn test_conn_registry_disconnect() {
        let registry = ConnRegistry::new();
        let conn = make_conn(2);
        registry.register(conn.clone()).await;

        // 主动断开
        let ok = registry.disconnect(2).await;
        assert!(ok);
        assert_eq!(*conn.state.read().await, ConnState::Closing);
    }

    #[tokio::test]
    async fn test_conn_registry_set_config() {
        let registry = ConnRegistry::new();
        let conn = make_conn(3);
        registry.register(conn.clone()).await;

        let ok = registry
            .set_config(3, ClientConfig {
                priority: 1,
                rate_limit: 100,
                max_concurrent: 10,
            })
            .await;
        assert!(ok);

        let cfg = conn.config.read().await;
        assert_eq!(cfg.priority, 1);
        assert_eq!(cfg.rate_limit, 100);
        assert_eq!(cfg.max_concurrent, 10);
    }

    #[tokio::test]
    async fn test_conn_registry_list() {
        let registry = ConnRegistry::new();
        for i in 1..=5 {
            let conn = make_conn(i);
            conn.set_holder(format!("holder-{}", i)).await;
            registry.register(conn).await;
        }

        let list = registry.list().await;
        assert_eq!(list.len(), 5);
    }
}
