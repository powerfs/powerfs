//! 抽象客户端连接与全局连接注册表
//!
//! 设计参考: BeeGFS StreamConn + 内核端 powerfs_net_server_conn
//!
//! 每个客户端连接对应一个 [`ClientConn`], 统一管理:
//! - 连接状态 (Active/Suspended/Closing/Closed)
//! - holder 身份 (UUID, 替代分散的 client_id_map)
//! - 持有的 lease (inode + token, 快速断连清理)
//! - 可配置策略 (优先级/限速/并发)
//! - 速率限制 (Token Bucket, 每连接独立)
//! - 统计信息 (请求数/错误数/字节数)
//! - 通知推送 (Server→Client Invalidate 等)
//!
//! [`ConnRegistry`] 是全局连接注册表, 提供增删改查:
//! - register / unregister
//! - get / get_by_holder
//! - disconnect (主动断开)
//! - set_policy (动态策略)
//! - notify / broadcast (通知推送)
//! - metrics_snapshot / health_check (聚合监控)
//! - list (管理/监控)

use crate::protocol::{ClientType, NetMessage};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};

// ---------------------------------------------------------------------------
// RateLimiter — 令牌桶限流器 (从 server_connection.rs 合并而来)
// ---------------------------------------------------------------------------

/// Simple token bucket rate limiter for per-client rate limiting.
///
/// Each `ClientConn` owns one instance so rate limiting is enforced at the
/// connection level without consulting a separate `ServerConnectionManager`.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    max_tokens: u64,
    refill_rate: f64,
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(max_tokens: u64, refill_rate_per_sec: f64) -> Self {
        Self {
            max_tokens,
            refill_rate: refill_rate_per_sec,
            tokens: max_tokens as f64,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token. Returns true if allowed.
    pub fn try_acquire(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        let refill = elapsed.as_secs_f64() * self.refill_rate;
        self.tokens = (self.tokens + refill).min(self.max_tokens as f64);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    pub fn available_tokens(&self) -> u64 {
        self.tokens as u64
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        // 1000 tokens max, 100 tokens/sec refill (10 req/s sustained)
        Self::new(1000, 100.0)
    }
}

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

/// 客户端策略 (可动态修改)
///
/// 表示服务端对单个客户端连接施加的 QoS 策略，
/// 与 [`crate::client::ClientConfig`]（客户端连接配置）语义不同：
/// - `ClientConfig` 描述"如何连接"（地址、超时、重试）
/// - `ClientPolicy` 描述"如何限流"（优先级、速率、并发上限）
#[derive(Debug, Clone)]
pub struct ClientPolicy {
    /// 请求优先级 (0=最高)
    pub priority: u8,
    /// 速率限制 (req/s, 0=不限)
    pub rate_limit: u32,
    /// 最大并发请求
    pub max_concurrent: u16,
}

impl Default for ClientPolicy {
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
    /// 通路类型: 0=data, 1=meta (握手登记, 收帧校验)
    pub channel: u8,
    /// 客户端协议 features (握手时协商, 决定编码格式等)
    pub features: u32,
    /// route_hash (握手时从 client_id 计算, 收帧校验防错乱)
    pub route_hash: u8,

    /// 连接状态
    pub state: RwLock<ConnState>,
    /// 客户端策略 (可动态修改)
    pub policy: RwLock<ClientPolicy>,
    /// 客户端统计
    pub stats: RwLock<ClientStats>,

    /// 持有的 inode lease 列表 (快速断连清理)
    pub held_leases: RwLock<HashSet<u64>>,
    /// 持有的 lease token 列表
    pub held_tokens: RwLock<HashSet<String>>,

    /// 每连接速率限制器 (Token Bucket)
    pub rate_limiter: RwLock<RateLimiter>,

    /// 出站帧通道 (响应帧 + 通知帧), IoLoop write_task 消费
    pub outbound_tx: OutboundTx,
    /// 关闭句柄 (用于主动断开底层 TCP 连接)
    pub close_handle: RwLock<Option<CloseHandle>>,
}

/// 计算 route_hash: 复用 `protocol::calc_route_hash` (与内核 pfs_route_hash 一一致).
/// 服务端收帧时校验 route_hash 是否匹配本连接, 防止帧串到错误连接.
fn calc_route_hash(client_id: u64, channel: u8) -> u8 {
    crate::protocol::calc_route_hash(client_id, channel)
}

impl ClientConn {
    pub fn new(
        id: u64,
        addr: SocketAddr,
        client_type: ClientType,
        channel: u8,
        features: u32,
        outbound_tx: OutboundTx,
    ) -> Arc<Self> {
        let now = Instant::now();
        let route_hash = calc_route_hash(id, channel);
        Arc::new(Self {
            id,
            holder_uuid: RwLock::new(None),
            addr,
            client_type,
            channel,
            features,
            route_hash,
            state: RwLock::new(ConnState::Active),
            policy: RwLock::new(ClientPolicy::default()),
            stats: RwLock::new(ClientStats {
                connected_at: now,
                last_activity: now,
                ..Default::default()
            }),
            held_leases: RwLock::new(HashSet::new()),
            held_tokens: RwLock::new(HashSet::new()),
            rate_limiter: RwLock::new(RateLimiter::default()),
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

    /// 检查速率限制 (Token Bucket)
    ///
    /// 返回 true 表示允许请求通过，false 表示被限流。
    pub async fn check_rate_limit(&self) -> bool {
        self.rate_limiter.write().await.try_acquire()
    }

    /// 记录请求完成 (更新统计)
    pub async fn record_request(&self, success: bool) {
        let mut stats = self.stats.write().await;
        stats.request_count += 1;
        if !success {
            stats.error_count += 1;
        }
        stats.last_activity = Instant::now();
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
///
/// 双通道支持: 同一 client_id 可有 data(0) 和 meta(1) 两个通道连接,
/// 内部按 (client_id, channel) 二级索引存储, 避免通道间互相覆盖.
pub struct ConnRegistry {
    /// client_id → (channel → ClientConn)
    /// 双通道: data(CHANNEL_DATA=0) + meta(CHANNEL_META=1) 独立存储
    conns: DashMap<u64, HashMap<u8, Arc<ClientConn>>>,
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

    /// 注册新连接 (按 channel 独立存储)
    ///
    /// 同一 client_id 的 data 和 meta 通道连接分别存储,
    /// 互不覆盖. 重复注册同 channel 的连接会覆盖旧连接
    /// (重连场景: 新连接替换旧连接).
    pub async fn register(&self, conn: Arc<ClientConn>) {
        let id = conn.id;
        let channel = conn.channel;
        if let Some(holder) = conn.holder_uuid.read().await.as_ref() {
            self.by_holder.insert(holder.clone(), id);
        }
        // 按 (client_id, channel) 存储, 两个通道互不干扰
        let mut entry = self.conns.entry(id).or_insert_with(HashMap::new);
        if entry.insert(channel, conn.clone()).is_some() {
            log::debug!(
                "ConnRegistry: register replaced old conn for client_id={} channel={} (reconnect)",
                id,
                channel
            );
            // 旧连接的 holder 不清除 (by_holder 按 client_id 索引, 新连接已覆盖)
        } else {
            log::debug!(
                "ConnRegistry: register new conn client_id={} channel={}",
                id,
                channel
            );
        }
    }

    /// 注销连接 (断连时调用)
    ///
    /// 按 (client_id, channel) 精确移除, 不影响同 client_id 的其他通道.
    /// 当 client_id 的所有通道都注销后, 清理 by_holder 索引.
    ///
    /// `check_conn` 用于身份校验: 当客户端重连后, 旧连接的 cleanup 可能
    /// 误删新连接 (同 channel). 传入 Some(&conn) 可确保只移除指定连接.
    ///
    /// 返回被移除的 ClientConn (供 on_disconnect 清理 lease)
    pub async fn unregister(
        &self,
        id: u64,
        check_conn: Option<&Arc<ClientConn>>,
    ) -> Option<Arc<ClientConn>> {
        let channel = check_conn.map(|c| c.channel);

        // 身份校验 + 精确移除
        let removed = {
            let mut entry = self.conns.get_mut(&id)?;
            let inner = entry.value_mut();
            // 如果有 check_conn, 校验同一 channel 的连接是否是同一个
            if let (Some(check), Some(ch)) = (check_conn, channel) {
                if let Some(existing) = inner.get(&ch) {
                    if !Arc::ptr_eq(existing, check) {
                        // 不同连接 (client 重连后旧连接 cleanup 误触),
                        // 不移除新连接
                        return None;
                    }
                }
            }
            // 按 channel 移除 (如果有 channel 信息), 否则移除第一个
            if let Some(ch) = channel {
                inner.remove(&ch)
            } else {
                // 向后兼容: 无 channel 信息时移除第一个
                let key = inner.keys().next().copied();
                key.and_then(|k| inner.remove(&k))
            }
        };

        // 如果该 client_id 的所有通道都已注销, 清理外层 entry 和 holder
        let should_clean_holder = {
            if let Some(entry) = self.conns.get(&id) {
                entry.value().is_empty()
            } else {
                true
            }
        };
        if should_clean_holder {
            self.conns.remove(&id);
        }

        if let Some(ref conn) = removed {
            if should_clean_holder {
                if let Some(holder) = conn.holder_uuid.read().await.as_ref() {
                    self.by_holder.remove(holder);
                }
            }
        }

        removed
    }

    /// 获取连接 (返回任意通道的连接, 优先 meta 通道)
    ///
    /// 用于通知推送等不区分通道的场景. 如需特定通道, 使用 [`get_by_channel`].
    pub fn get(&self, id: u64) -> Option<Arc<ClientConn>> {
        let entry = self.conns.get(&id)?;
        let inner = entry.value();
        // 优先返回 meta 通道 (通知推送通常走 meta)
        inner
            .get(&1)
            .or_else(|| inner.values().next())
            .cloned()
    }

    /// 获取指定通道的连接
    pub fn get_by_channel(&self, id: u64, channel: u8) -> Option<Arc<ClientConn>> {
        let entry = self.conns.get(&id)?;
        entry.value().get(&channel).cloned()
    }

    /// 通过 holder UUID 获取连接
    pub fn get_by_holder(&self, holder: &str) -> Option<Arc<ClientConn>> {
        let id = *self.by_holder.get(holder)?;
        self.get(id)
    }

    /// 主动断开指定 client_id 的所有通道连接
    pub async fn disconnect(&self, id: u64) -> bool {
        let entry = match self.conns.get(&id) {
            Some(e) => e,
            None => return false,
        };
        let conns: Vec<Arc<ClientConn>> = entry.value().values().cloned().collect();
        drop(entry);
        for conn in conns {
            conn.disconnect().await;
        }
        true
    }

    /// 设置客户端策略 (应用到所有通道)
    pub async fn set_policy(&self, id: u64, policy: ClientPolicy) -> bool {
        let entry = match self.conns.get(&id) {
            Some(e) => e,
            None => return false,
        };
        let conns: Vec<Arc<ClientConn>> = entry.value().values().cloned().collect();
        drop(entry);
        if conns.is_empty() {
            return false;
        }
        for conn in conns {
            *conn.policy.write().await = policy.clone();
        }
        true
    }

    /// 列出所有连接信息 (管理/监控, 含所有通道)
    pub async fn list(&self) -> Vec<ClientConnInfo> {
        let mut result = Vec::new();
        for entry in self.conns.iter() {
            for conn in entry.value().values() {
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
        }
        result
    }

    /// 活跃客户端数 (按 client_id 计数, 非通道数)
    pub fn count(&self) -> usize {
        self.conns.len()
    }

    /// 活跃连接数 (含所有通道)
    pub fn conn_count(&self) -> usize {
        self.conns.iter().map(|e| e.value().len()).sum()
    }

    // ========================================================================
    // 通知推送 (Server→Client)
    // ========================================================================

    /// 向指定客户端推送通知消息
    ///
    /// 优先通过 meta 通道发送 (通知通常走 meta 通道).
    /// 返回 true 表示已排队，false 表示客户端不存在或通道关闭。
    pub fn notify(&self, client_id: u64, msg: &NetMessage) -> bool {
        if let Some(conn) = self.get(client_id) {
            conn.notify(msg)
        } else {
            false
        }
    }

    /// 向所有活跃客户端广播通知 (每客户端仅发一次, 优先 meta 通道)
    ///
    /// 返回成功接收的客户端数量。
    pub fn broadcast(&self, msg: &NetMessage) -> usize {
        let mut count = 0;
        for entry in self.conns.iter() {
            let inner = entry.value();
            // 优先 meta 通道, 回退到任意通道
            let conn = inner.get(&1).or_else(|| inner.values().next());
            if let Some(conn) = conn {
                if conn.notify(msg) {
                    count += 1;
                }
            }
        }
        count
    }

    // ========================================================================
    // 聚合监控 (替代 ServerConnectionManager 的 metrics/health 方法)
    // ========================================================================

    /// 获取聚合指标快照 (所有通道连接的统计汇总)
    pub async fn metrics_snapshot(&self) -> ConnMetricsSnapshot {
        let mut snapshot = ConnMetricsSnapshot::default();
        for entry in self.conns.iter() {
            for conn in entry.value().values() {
                let stats = conn.stats.read().await;
                snapshot.total_requests += stats.request_count;
                snapshot.total_errors += stats.error_count;
                snapshot.total_bytes_sent += stats.bytes_sent;
                snapshot.total_bytes_recv += stats.bytes_recv;
                snapshot.total_sessions += 1;
                if *conn.state.read().await == ConnState::Active {
                    snapshot.active_sessions += 1;
                }
            }
        }
        snapshot
    }

    /// 健康检查 (用于监控系统)
    pub async fn health_check(&self) -> ConnHealthStatus {
        let snapshot = self.metrics_snapshot().await;
        ConnHealthStatus {
            healthy: true,
            active_sessions: snapshot.active_sessions,
            total_sessions: snapshot.total_sessions,
        }
    }

    /// 列出所有活跃客户端 ID
    pub fn list_client_ids(&self) -> Vec<u64> {
        self.conns.iter().map(|e| *e.key()).collect()
    }

    /// 获取指定连接的统计信息 (优先 meta 通道)
    pub async fn get_stats(&self, client_id: u64) -> Option<ClientStats> {
        let conn = self.get(client_id)?;
        let stats = conn.stats.read().await;
        Some(stats.clone())
    }
}

/// 聚合连接指标快照 (跨所有连接)
#[derive(Debug, Default, Clone)]
pub struct ConnMetricsSnapshot {
    pub total_requests: u64,
    pub total_errors: u64,
    pub total_bytes_sent: u64,
    pub total_bytes_recv: u64,
    pub active_sessions: usize,
    pub total_sessions: usize,
}

/// 连接健康状态
#[derive(Debug, Clone)]
pub struct ConnHealthStatus {
    pub healthy: bool,
    pub active_sessions: usize,
    pub total_sessions: usize,
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
            0,
            0,
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

        let removed = registry.unregister(1, None).await;
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
            .set_policy(
                3,
                ClientPolicy {
                    priority: 1,
                    rate_limit: 100,
                    max_concurrent: 10,
                },
            )
            .await;
        assert!(ok);

        let cfg = conn.policy.read().await;
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

    /// 辅助函数: 创建指定 channel 的连接
    fn make_conn_channel(id: u64, channel: u8) -> Arc<ClientConn> {
        let (tx, _rx) = mpsc::unbounded_channel::<Vec<u8>>();
        ClientConn::new(
            id,
            "127.0.0.1:1234".parse().unwrap(),
            ClientType::Fuse,
            channel,
            0,
            tx,
        )
    }

    /// 双通道场景: 同一 client_id 的 data(0) + meta(1) 通道连接共存,
    /// 一个通道断连不影响另一个通道.
    #[tokio::test]
    async fn test_conn_registry_dual_channel() {
        let registry = ConnRegistry::new();

        // 注册 data 通道连接
        let data_conn = make_conn_channel(100, 0);
        data_conn.set_holder("holder-dual".to_string()).await;
        registry.register(data_conn.clone()).await;

        // 注册 meta 通道连接 (同一 client_id, 不同 channel)
        let meta_conn = make_conn_channel(100, 1);
        registry.register(meta_conn.clone()).await;

        // 两个通道都应可获取
        assert_eq!(registry.count(), 1); // 1 个 client_id
        assert_eq!(registry.conn_count(), 2); // 2 个通道连接
        assert!(registry.get_by_channel(100, 0).is_some()); // data
        assert!(registry.get_by_channel(100, 1).is_some()); // meta
        assert!(registry.get(100).is_some()); // 任意通道

        // data 通道断连: 注销 data 通道, meta 通道应不受影响
        let removed = registry.unregister(100, Some(&data_conn)).await;
        assert!(removed.is_some());
        assert_eq!(registry.count(), 1); // client_id 仍存在
        assert_eq!(registry.conn_count(), 1); // 只剩 1 个通道
        assert!(registry.get_by_channel(100, 0).is_none()); // data 已注销
        assert!(registry.get_by_channel(100, 1).is_some()); // meta 仍在
        assert!(registry.get(100).is_some()); // 仍可获取 (meta)

        // meta 通道断连: 注销 meta 通道, client_id 应完全清除
        let removed = registry.unregister(100, Some(&meta_conn)).await;
        assert!(removed.is_some());
        assert_eq!(registry.count(), 0); // client_id 已清除
        assert_eq!(registry.conn_count(), 0);
        assert!(registry.get(100).is_none());
        assert!(registry.get_by_holder("holder-dual").is_none());
    }

    /// 重连场景: 同通道新连接替换旧连接
    #[tokio::test]
    async fn test_conn_registry_reconnect_same_channel() {
        let registry = ConnRegistry::new();

        // 注册 meta 通道连接
        let old_meta = make_conn_channel(200, 1);
        old_meta.set_holder("holder-reconn".to_string()).await;
        registry.register(old_meta.clone()).await;

        // 重连: 新 meta 通道连接替换旧连接
        let new_meta = make_conn_channel(200, 1);
        registry.register(new_meta.clone()).await;

        // 注册表应只有 1 个通道连接 (新连接替换了旧连接)
        assert_eq!(registry.conn_count(), 1);
        let current = registry.get_by_channel(200, 1).unwrap();
        assert!(Arc::ptr_eq(&current, &new_meta));

        // 旧连接的 unregister 不应影响新连接 (身份校验)
        let removed = registry.unregister(200, Some(&old_meta)).await;
        assert!(removed.is_none()); // 旧连接已不在注册表中, 返回 None
        assert!(registry.get_by_channel(200, 1).is_some()); // 新连接仍在

        // 新连接的 unregister 正常工作
        let removed = registry.unregister(200, Some(&new_meta)).await;
        assert!(removed.is_some());
        assert!(registry.get(200).is_none());
    }
}
