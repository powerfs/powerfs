use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::circuit_breaker::CircuitBreaker;
use crate::net_client::PowerFuseNetClient;
use powerfs_net as net;

/// MetaShard 信息
#[derive(Debug, Clone)]
pub struct ShardInfo {
    /// 分片 ID
    pub shard_id: u64,
    /// Leader 地址
    pub leader_addr: String,
    /// Followers 地址
    pub follower_addrs: Vec<String>,
    /// 分片哈希
    pub shard_hash: u64,
}

impl ShardInfo {
    pub fn new(shard_id: u64, leader_addr: String) -> Self {
        Self {
            shard_id,
            leader_addr,
            follower_addrs: Vec::new(),
            shard_hash: shard_id,
        }
    }

    pub fn with_followers(mut self, followers: Vec<String>) -> Self {
        self.follower_addrs = followers;
        self
    }

    pub fn with_hash(mut self, hash: u64) -> Self {
        self.shard_hash = hash;
        self
    }
}

/// Volume 信息
#[derive(Debug, Clone)]
pub struct VolumeInfo {
    /// Volume ID
    pub volume_id: u64,
    /// Volume 路径
    pub volume_path: String,
    /// Volume 地址
    pub addr: String,
    /// 是否已挂载
    pub mounted: bool,
}

impl VolumeInfo {
    pub fn new(volume_id: u64, volume_path: String, addr: String) -> Self {
        Self {
            volume_id,
            volume_path,
            addr,
            mounted: true,
        }
    }
}

/// 集群拓扑结构
#[derive(Debug, Clone, Default)]
pub struct ClusterTopology {
    /// MetaShard 列表
    pub shards: HashMap<u64, ShardInfo>,
    /// Volume 列表
    pub volumes: HashMap<u64, VolumeInfo>,
    /// 拓扑版本号
    pub version: u64,
    /// 更新时间
    pub updated_at: Option<Instant>,
}

impl ClusterTopology {
    pub fn new() -> Self {
        Self {
            shards: HashMap::new(),
            volumes: HashMap::new(),
            version: 0,
            updated_at: None,
        }
    }

    pub fn get_shard_leader(&self, shard_id: u64) -> Option<&str> {
        self.shards.get(&shard_id).map(|s| s.leader_addr.as_str())
    }

    pub fn get_volume(&self, volume_id: u64) -> Option<&VolumeInfo> {
        self.volumes.get(&volume_id)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn volume_count(&self) -> usize {
        self.volumes.len()
    }
}

/// 拓扑更新监听器
pub trait TopologyUpdateListener: Send + Sync {
    fn on_topology_update(&self, old: &ClusterTopology, new: &ClusterTopology);
}

/// 空实现的监听器 (默认)
pub struct NoopTopologyListener;

impl TopologyUpdateListener for NoopTopologyListener {
    fn on_topology_update(&self, _old: &ClusterTopology, _new: &ClusterTopology) {
        // 不做任何事情
    }
}

/// 有状态的计数器监听器 (用于测试)
pub struct CountingTopologyListener {
    update_count: Mutex<u64>,
}

impl CountingTopologyListener {
    pub fn new() -> Self {
        Self {
            update_count: Mutex::new(0),
        }
    }

    pub fn update_count(&self) -> u64 {
        *self.update_count.lock().unwrap()
    }
}

impl Default for CountingTopologyListener {
    fn default() -> Self {
        Self::new()
    }
}

impl TopologyUpdateListener for CountingTopologyListener {
    fn on_topology_update(&self, _old: &ClusterTopology, _new: &ClusterTopology) {
        let mut count = self.update_count.lock().unwrap();
        *count += 1;
    }
}

/// 集群拓扑管理器
pub struct ClusterTopologyManager {
    topology: RwLock<ClusterTopology>,
    listeners: Mutex<Vec<Arc<dyn TopologyUpdateListener>>>,
    breaker: CircuitBreaker,
}

impl ClusterTopologyManager {
    pub fn new() -> Self {
        Self {
            topology: RwLock::new(ClusterTopology::new()),
            listeners: Mutex::new(Vec::new()),
            breaker: CircuitBreaker::default(),
        }
    }

    /// 获取当前拓扑
    pub fn get_topology(&self) -> ClusterTopology {
        self.topology.read().unwrap().clone()
    }

    /// 获取特定分片的 Leader 地址
    pub fn get_shard_leader(&self, shard_id: u64) -> Option<String> {
        let topology = self.topology.read().unwrap();
        topology.get_shard_leader(shard_id).map(|s| s.to_string())
    }

    /// 获取特定 Volume 信息
    pub fn get_volume(&self, volume_id: u64) -> Option<VolumeInfo> {
        let topology = self.topology.read().unwrap();
        topology.get_volume(volume_id).cloned()
    }

    /// 更新拓扑
    pub fn update_topology(&self, new_topology: ClusterTopology) {
        let old = {
            let mut topology = self.topology.write().unwrap();
            let old = topology.clone();
            *topology = new_topology;
            old
        };

        // 通知所有监听器
        let listeners = self.listeners.lock().unwrap();
        for listener in listeners.iter() {
            listener.on_topology_update(&old, &self.topology.read().unwrap());
        }
    }

    /// 添加监听器
    pub fn add_listener(&self, listener: Arc<dyn TopologyUpdateListener>) {
        let mut listeners = self.listeners.lock().unwrap();
        listeners.push(listener);
    }

    /// 获取熔断器
    pub fn circuit_breaker(&self) -> &CircuitBreaker {
        &self.breaker
    }

    /// 检查是否可以进行拓扑请求
    pub fn can_request(&self) -> bool {
        self.breaker.is_available()
    }

    /// 记录成功的拓扑请求
    pub fn record_success(&self) {
        self.breaker.record_success();
    }

    /// 记录失败的拓扑请求
    pub fn record_failure(&self) {
        self.breaker.record_failure();
    }
}

impl Default for ClusterTopologyManager {
    fn default() -> Self {
        Self::new()
    }
}

/// MasterClient 配置
#[derive(Debug, Clone)]
pub struct MasterClientConfig {
    /// Master 节点地址列表
    pub master_addrs: Vec<String>,
    /// 请求超时
    pub request_timeout: std::time::Duration,
    /// 重试次数
    pub max_retries: u32,
    /// 熔断器配置
    pub circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig,
}

impl Default for MasterClientConfig {
    fn default() -> Self {
        Self {
            master_addrs: vec!["127.0.0.1:9333".to_string()],
            request_timeout: std::time::Duration::from_secs(5),
            max_retries: 3,
            circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig::default(),
        }
    }
}

/// MasterClient 状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterClientState {
    /// 未连接
    Disconnected,
    /// 已连接
    Connected,
    /// 重连中
    Reconnecting,
}

/// MasterClient - 与 Master 服务通信的客户端
pub struct MasterClient {
    config: MasterClientConfig,
    state: Mutex<MasterClientState>,
    topology_manager: Arc<ClusterTopologyManager>,
    current_leader: Mutex<Option<String>>,
    /// 网络客户端 (可选，用于真实网络发送)
    net_client: Option<Arc<PowerFuseNetClient>>,
}

impl MasterClient {
    pub fn new(config: MasterClientConfig, topology_manager: Arc<ClusterTopologyManager>) -> Self {
        Self {
            config,
            state: Mutex::new(MasterClientState::Disconnected),
            topology_manager,
            current_leader: Mutex::new(None),
            net_client: None,
        }
    }

    /// 设置网络客户端
    pub fn set_net_client(&mut self, client: Arc<PowerFuseNetClient>) {
        self.net_client = Some(client);
    }

    /// 获取网络客户端引用
    pub fn net_client(&self) -> Option<&Arc<PowerFuseNetClient>> {
        self.net_client.as_ref()
    }

    /// 获取当前状态
    pub fn state(&self) -> MasterClientState {
        *self.state.lock().unwrap()
    }

    /// 获取拓扑管理器
    pub fn topology_manager(&self) -> &Arc<ClusterTopologyManager> {
        &self.topology_manager
    }

    /// 获取当前 Leader 地址
    pub fn current_leader(&self) -> Option<String> {
        self.current_leader.lock().unwrap().clone()
    }

    /// 设置当前 Leader
    pub fn set_leader(&self, addr: String) {
        let mut leader = self.current_leader.lock().unwrap();
        *leader = Some(addr);
        *self.state.lock().unwrap() = MasterClientState::Connected;
    }

    /// 连接到 Master
    pub async fn connect(&self) -> Result<(), MasterClientError> {
        if !self.topology_manager.can_request() {
            return Err(MasterClientError::CircuitOpen);
        }

        // 如果有网络客户端，发送真实的连接请求
        if let Some(net_client) = &self.net_client {
            match net_client.master_client().is_connected() {
                true => {
                    let master_addr = self
                        .config
                        .master_addrs
                        .first()
                        .ok_or(MasterClientError::NoMasterAddress)?
                        .clone();
                    self.set_leader(master_addr);
                    self.topology_manager.record_success();

                    log::info!(
                        "MasterClient: Connected to {} via powerfs-net",
                        self.current_leader().unwrap()
                    );
                    Ok(())
                }
                false => Err(MasterClientError::ConnectionFailed(
                    "Master client not connected".to_string(),
                )),
            }
        } else {
            // 无网络客户端时使用模拟实现
            let master_addr = self
                .config
                .master_addrs
                .first()
                .ok_or(MasterClientError::NoMasterAddress)?
                .clone();

            self.set_leader(master_addr);
            self.topology_manager.record_success();

            log::info!(
                "MasterClient: Connected to {} (mock)",
                self.current_leader().unwrap()
            );
            Ok(())
        }
    }

    /// 获取拓扑信息
    pub async fn fetch_topology(&self) -> Result<ClusterTopology, MasterClientError> {
        if !self.topology_manager.can_request() {
            return Err(MasterClientError::CircuitOpen);
        }

        let _leader = self
            .current_leader()
            .ok_or(MasterClientError::NotConnected)?;

        // 如果有网络客户端，发送真实的 GetTopology 请求
        if let Some(net_client) = &self.net_client {
            let body = vec![];
            match net_client
                .master_client()
                .send_request(net::MsgType::GetTopology, &body, &[])
                .await
            {
                Ok(response) if response.is_ok() => {
                    self.topology_manager.record_success();
                    Ok(ClusterTopology::new())
                }
                Ok(response) => {
                    self.topology_manager.record_failure();
                    Err(MasterClientError::ConnectionFailed(format!(
                        "Server error: {}",
                        response.header.status
                    )))
                }
                Err(e) => {
                    self.topology_manager.record_failure();
                    Err(MasterClientError::ConnectionFailed(format!(
                        "Network error: {}",
                        e
                    )))
                }
            }
        } else {
            // 无网络客户端时使用模拟实现
            let topology = ClusterTopology::new();
            self.topology_manager.record_success();
            Ok(topology)
        }
    }

    /// 更新本地拓扑
    pub fn update_topology(&self, topology: ClusterTopology) {
        self.topology_manager.update_topology(topology);
    }

    /// 断开连接
    pub fn disconnect(&self) {
        *self.state.lock().unwrap() = MasterClientState::Disconnected;
        *self.current_leader.lock().unwrap() = None;
        log::info!("MasterClient: Disconnected");
    }
}

impl Drop for MasterClient {
    fn drop(&mut self) {
        self.disconnect();
    }
}

/// MasterClient 错误
#[derive(Debug, thiserror::Error)]
pub enum MasterClientError {
    #[error("Not connected to master")]
    NotConnected,

    #[error("Circuit breaker is open")]
    CircuitOpen,

    #[error("No master address configured")]
    NoMasterAddress,

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Request timeout")]
    Timeout,

    #[error("Leader changed: old={old}, new={new}")]
    LeaderChanged { old: String, new: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_topology_operations() {
        let mut topology = ClusterTopology::new();

        let shard = ShardInfo::new(1, "127.0.0.1:9334".to_string())
            .with_followers(vec!["127.0.0.1:9335".to_string()]);
        topology.shards.insert(1, shard);

        let volume = VolumeInfo::new(1, "/data/vol1".to_string(), "127.0.0.1:9344".to_string());
        topology.volumes.insert(1, volume);

        topology.version = 1;

        assert_eq!(topology.shard_count(), 1);
        assert_eq!(topology.volume_count(), 1);
        assert_eq!(topology.get_shard_leader(1), Some("127.0.0.1:9334"));
        assert_eq!(topology.get_volume(1).unwrap().volume_path, "/data/vol1");
        assert!(topology.get_shard_leader(2).is_none());
    }

    #[test]
    fn test_topology_manager() {
        let manager = ClusterTopologyManager::new();

        // 初始状态
        assert_eq!(manager.get_topology().version, 0);
        assert!(manager.get_shard_leader(1).is_none());

        // 更新拓扑
        let mut topology = ClusterTopology::new();
        topology
            .shards
            .insert(1, ShardInfo::new(1, "10.0.0.1:9334".to_string()));
        topology.volumes.insert(
            1,
            VolumeInfo::new(1, "/vol".to_string(), "10.0.0.1:9344".to_string()),
        );
        topology.version = 1;

        manager.update_topology(topology.clone());

        let current = manager.get_topology();
        assert_eq!(current.version, 1);
        assert_eq!(current.shard_count(), 1);
        assert_eq!(
            manager.get_shard_leader(1),
            Some("10.0.0.1:9334".to_string())
        );
    }

    #[test]
    fn test_topology_listener_notification() {
        let manager = ClusterTopologyManager::new();
        let listener = Arc::new(CountingTopologyListener::new());
        manager.add_listener(listener.clone());

        // 初始不应触发
        assert_eq!(listener.update_count(), 0);

        // 第一次更新
        let topology1 = ClusterTopology::new();
        manager.update_topology(topology1);
        assert_eq!(listener.update_count(), 1);

        // 第二次更新
        let topology2 = ClusterTopology {
            version: 1,
            ..Default::default()
        };
        manager.update_topology(topology2);
        assert_eq!(listener.update_count(), 2);
    }

    #[test]
    fn test_master_client_state() {
        let manager = Arc::new(ClusterTopologyManager::new());
        let client = MasterClient::new(MasterClientConfig::default(), manager);

        assert_eq!(client.state(), MasterClientState::Disconnected);
        assert!(client.current_leader().is_none());

        client.set_leader("127.0.0.1:9333".to_string());
        assert_eq!(client.state(), MasterClientState::Connected);
        assert_eq!(client.current_leader(), Some("127.0.0.1:9333".to_string()));

        client.disconnect();
        assert_eq!(client.state(), MasterClientState::Disconnected);
        assert!(client.current_leader().is_none());
    }

    #[test]
    fn test_master_client_config() {
        let config = MasterClientConfig::default();
        assert_eq!(config.master_addrs.len(), 1);
        assert_eq!(config.master_addrs[0], "127.0.0.1:9333");
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_breaker_integration() {
        let manager = Arc::new(ClusterTopologyManager::new());

        // 初始可用
        assert!(manager.can_request());

        // 模拟失败
        for _ in 0..5 {
            manager.record_failure();
        }

        // 现在不可用
        assert!(!manager.can_request());
    }
}
