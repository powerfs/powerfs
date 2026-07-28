# PowerFS 分布式通信架构设计文档

> **版本**: v3.0  
> **日期**: 2026-07-28  
> **状态**: 设计评审中  
> **v3.0 变更**: VolumeClient 简化为统一数据队列 + 多传输通道；新增内核文件系统适配层

---

## 目录

1. [架构总览](#1-架构总览)
2. [MetaShardClient 设计](#2-metashardclient-设计)
3. [VolumeClient 设计](#3-volumeclient-设计) (v3.0 简化)
4. [请求生命周期](#4-请求生命周期)
5. [Exactly-Once 保证](#5-exactly-once-保证)
6. [Raft 日志集成](#6-raft-日志集成)
7. [熔断器与健康管理](#7-熔断器与健康管理)
8. [协议扩展](#8-协议扩展)
9. [FUSE 层集成](#9-fuse-层集成)
10. [内核文件系统适配层](#10-内核文件系统适配层) (v3.0 新增)
11. [分阶段实施计划](#11-分阶段实施计划)

---

## 1. 架构总览

### 1.1 分层架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                          FUSE Callbacks                               │
│     (lookup, create, read, write, mkdir, unlink, statfs...)          │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  MasterClient   │  │ MetaShardClient │  │  VolumeClient   │
│  (集群状态权威)  │  │  (元数据客户端)  │  │  (数据客户端)    │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ QueryChannel    │  │ DataChannel     │  │ LeaseChannel    │
│ WatchChannel    │  │ ControlChannel  │  │ MgmtChannel     │
│                 │  │                 │  │ DataQueue +     │
│                 │  │                 │  │ ChannelPool     │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ ClusterTopology │  │                 │  │                 │
│   Manager       │  │                 │  │                 │
│ (子组件)        │  │                 │  │                 │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ powerfs-net     │  │ powerfs-net     │  │ powerfs-net     │
│ TCP + 帧编解码   │  │ TCP + 帧编解码   │  │ TCP + 帧编解码   │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

### 1.2 架构说明

**核心设计**: FUSE 层直接对接三个独立的客户端，每个客户端内部包含：
- 多个请求队列（mpsc channel）或数据流
- 独立的传输通道（powerfs-net 协议栈）
- 各自的状态管理和高可用策略

**MasterClient** (集群状态权威):
- `QueryChannel`: 同步请求-响应通道，用于获取拓扑、分配 Volume、集群级 Statfs 等查询操作。
- `WatchChannel`: 异步推送通道，用于接收 Master 主动下发的拓扑变更通知。
- `ClusterTopologyManager`: 作为子组件，利用上述通道维护本地拓扑缓存并向其他客户端（MetaShardClient, VolumeClient）分发变更。

**MetaShardClient** (Filer 客户端):
- `DataChannel`: 元数据操作（lookup, create, mkdir, unlink 等）
- `ControlChannel`: 控制请求（心跳、Leader 查询、配置变更）
- 每个 shard 一个连接，双通道调度。动态管理（通过 MasterClient 获取）。

**VolumeClient** (v3.0 简化设计):
- `LeaseChannel`: 独占通道，处理 Lease 获取/续约/释放（最高优先级，永不被读写阻塞）
- `MgmtChannel`: 独占通道，处理 StatFs/心跳/Volume 状态查询
- `DataQueue`: **统一数据入口**，读写请求共享一个队列
- `Channel Pool`: N 条底层 TCP 通道，由 DataQueue 按策略分发。动态管理（通过 MasterClient 获取）。

### 1.3 v2.0 → v3.0 设计对比

| 维度 | v2.0（原方案） | v3.0（推荐方案） |
|------|--------------|----------------|
| Volume 通道数 | 4 (Write/Read/Lease/Mgmt) | 2 独占 + 1 统一队列 + N 通道池 |
| 读写隔离 | 写/读 各一个队列 | 共享 DataQueue，通道池分发 |
| 调度复杂度 | 高（4 通道优先级调度） | 低（3 队列 + 通道选择） |
| 带宽利用率 | 低（读写互不共享连接） | 高（读写共享通道池） |
| 内核态实现 | 复杂（4 等待队列） | 简洁（3 等待队列 + 通道数组） |
| 扩展性 | 难（通道数固定） | 易（动态调整通道数） |

### 1.4 对比现有架构

| 现有架构 | 新架构 |
|---------|--------|
| SyncFuseNetClient 单连接 | MasterClient + MetaShardClient + VolumeClient 三客户端 |
| 单一请求通道 | 每客户端多通道 |
| 硬编码服务地址 | MasterClient 动态发现 + 拓扑管理 |
| 无分片路由 | MetaShardClient 内置 ShardRouter |
| 无状态追踪 | 每请求完整生命周期追踪 |
| 无幂等保证 | RequestId + 服务端幂等索引 |
| 无熔断器 | CircuitBreaker 三态转换 |
| 无 Lease | VolumeClient 内置 Lease 管理 |

---

## 2. MasterClient 设计

### 2.1 设计理念

`MasterClient` 是 FUSE 客户端与集群 Master 节点之间的桥梁，是**集群状态的权威来源**。它负责：
- **系统引导 (Bootstrap)**：客户端启动时，通过它获取初始的集群拓扑。
- **动态更新 (Dynamic Update)**：通过 Watch 机制实时感知拓扑变更（如 Leader 切换、节点上下线）。
- **集群级服务 (Cluster-level Services)**：提供 Volume 分配、集群级 Statfs、挂载认证等集群级别的查询与操作接口。
- **高可用 (HA)**：处理 Master 本身的高可用性（Raft 集群 Leader 切换）。

### 2.2 结构体定义

```rust
/// Master 客户端
pub struct MasterClient {
    /// Master 地址列表 (用于 HA)
    master_addrs: Vec<String>,
    /// 当前连接的 Master 地址
    current_leader: Arc<RwLock<Option<String>>>,

    // ===== 两个通道 =====
    /// 查询通道 (同步请求-响应): 获取拓扑、分配 Volume、认证等
    query_tx: mpsc::Sender<MasterQueryRequest>,
    query_rx: Mutex<mpsc::Receiver<MasterQueryRequest>>,

    /// 监听通道 (异步接收): 接收拓扑变更推送
    watch_tx: mpsc::Sender<MasterWatchRequest>,
    watch_rx: Mutex<mpsc::Receiver<MasterWatchRequest>>,

    // ===== 传输层 =====
    transport: Arc<PowerFsNetClient>,
    runtime: Arc<tokio::runtime::Runtime>,

    // ===== 子组件 =====
    topology_manager: Arc<ClusterTopologyManager>,
}

/// Master 查询请求
pub struct MasterQueryRequest {
    pub kind: MasterQueryKind,
    pub payload: Vec<u8>,
    pub reply_tx: Option<oneshot::Sender<NetResult<NetMessage>>>,
}

#[derive(Clone)]
pub enum MasterQueryKind {
    GetTopology,        // 获取全量拓扑
    AssignVolume,       // 为新文件分配 Volume
    ClusterStatFs,      // 集群级 StatFs
    Authenticate,       // 挂载认证
    GetConfiguration,   // 获取集群配置
}

/// Master 监听请求
pub struct MasterWatchRequest {
    pub kind: MasterWatchKind,
}

#[derive(Clone)]
pub enum MasterWatchKind {
    SubscribeTopology,  // 订阅拓扑变更
    Unsubscribe,        // 取消订阅
}
```

### 2.3 核心 API

```rust
impl MasterClient {
    /// 获取集群拓扑 (QueryChannel)
    pub async fn get_cluster_topology(&self) -> NetResult<ClusterTopology> { ... }

    /// 为新文件分配 Volume (QueryChannel)
    pub async fn assign_volume(&self, file_size: u64, replication: u32) -> NetResult<VolumeAssignment> { ... }

    /// 集群级 StatFs (QueryChannel)
    pub async fn cluster_statfs(&self) -> NetResult<FsStats> { ... }

    /// 挂载认证 (QueryChannel)
    pub async fn authenticate(&self, client_id: &str) -> NetResult<AuthToken> { ... }

    /// 启动拓扑管理器 (返回 ClusterTopologyManager 实例)
    pub async fn start_topology_manager(&self) -> Arc<ClusterTopologyManager> {
        self.topology_manager.clone()
    }
}
```

### 2.4 高可用 (HA) 逻辑

Master 本身是 Raft 集群，`MasterClient` 必须处理 Master Leader 的切换：
-   **多地址配置**：初始化时传入所有 Master 节点的地址 (例如，`master1:9333,master2:9333,master3:9333`)。
-   **Leader 探测**：连接后，通过 `RaftLeadership` 检查当前节点是否为 Leader。如果不是，获取 Leader 地址并重新连接。
-   **自动重连**：与 Master 的连接断开时，`MasterClient` 会自动按优先级列表循环尝试重连。
-   **状态恢复**：重连成功后，立即拉取最新拓扑并恢复 Watch 机制。

### 2.5 ClusterTopologyManager (MasterClient 子组件)

`ClusterTopologyManager` 现在是 `MasterClient` 的一个子组件，负责维护本地拓扑缓存并向 `MetaShardClient` 和 `VolumeClient` 分发变更。

```rust
pub struct ClusterTopologyManager {
    master_client: Arc<MasterClient>, // 通过 MasterClient 通信
    
    // 拓扑缓存
    topology: Arc<RwLock<ClusterTopology>>,
    
    // 事件回调
    shard_listeners: Arc<Vec<Box<dyn Fn(ShardEvent) + Send + Sync>>>,
    volume_listeners: Arc<Vec<Box<dyn Fn(VolumeEvent) + Send + Sync>>>,
}

impl ClusterTopologyManager {
    /// 引导初始化 (由 MasterClient 在启动时调用)
    async fn bootstrap(&self) {
        // 通过 MasterClient 的 QueryChannel 获取初始拓扑
        let topology = self.master_client.get_cluster_topology().await.unwrap();
        self.update_cache(topology).await;
        self.notify_subscribers().await;
    }
    
    /// 启动后台监听
    async fn start_watcher(&self) {
        // 1. 请求 Master 推送
        self.master_client.subscribe_topology().await;
        
        // 2. 后台循环处理推送事件
        loop {
            let event = self.master_client.recv_topology_event().await;
            match event {
                MasterEvent::TopologyChanged(new_topology) => {
                    self.update_cache(new_topology).await;
                    self.notify_subscribers().await;
                }
            }
        }
    }

    /// 订阅分片变更
    pub fn subscribe_shard_events(&self, listener: Box<dyn Fn(ShardEvent) + Send + Sync>) {
        self.shard_listeners.write().unwrap().push(listener);
    }

    /// 订阅卷变更
    pub fn subscribe_volume_events(&self, listener: Box<dyn Fn(VolumeEvent) + Send + Sync>) {
        self.volume_listeners.write().unwrap().push(listener);
    }
}
```

---

## 3. MetaShardClient 设计

### 3.1 动态发现与初始化

`MetaShardClient` 不再静态配置所有 `ShardConnection`，而是依赖 `ClusterTopologyManager` 动态创建和管理。

-   **初始引导 (Bootstrap)**：通过 `ClusterTopologyManager` 从 Master 获取当前活跃的 Filer 分片列表及其 Leader 地址，自动初始化 `ShardConnection`。
-   **动态更新**：订阅 `ClusterTopologyManager` 的事件，当 Master 通知分片变更（如新 Leader 选举、节点上线）时，自动更新或重建 `ShardConnection`。

### 3.2 结构体定义

```rust
/// Filer 元数据客户端
pub struct MetaShardClient {
    /// 客户端身份 (持久化)
    identity: Arc<ClientIdentity>,
    /// 路由
    router: Arc<ShardRouter>,
    /// 每个 shard 的连接 (每个连接有双通道)
    shard_connections: Arc<RwLock<HashMap<ShardId, ShardConnection>>>,
    /// 熔断器 (per-shard)
    breakers: Arc<RwLock<HashMap<ShardId, CircuitBreaker>>>,
    /// Runtime
    runtime: Arc<tokio::runtime::Runtime>,
}

/// 单个 Shard 的连接 (双通道)
pub struct ShardConnection {
    shard_id: ShardId,
    leader_addr: String,

    // ===== 两个请求队列 =====
    /// 数据通道: 元数据操作 (lookup, create, mkdir, unlink, rename...)
    data_tx: mpsc::Sender<MetaRequest>,
    data_rx: Mutex<mpsc::Receiver<MetaRequest>>,

    /// 控制通道: 心跳、Leader查询、配置变更 (不被数据通道阻塞)
    control_tx: mpsc::Sender<ControlRequest>,
    control_rx: Mutex<mpsc::Receiver<ControlRequest>>,

    // ===== 状态管理 =====
    /// 待处理请求表
    pending: Arc<RwLock<HashMap<RequestId, RequestContext>>>,
    /// 最后心跳时间
    last_heartbeat: Arc<AtomicInstant>,

    // ===== 传输层 =====
    transport: Arc<PowerFsNetClient>,
}

/// 分片路由器
pub struct ShardRouter {
    /// shard_id → leader 信息缓存
    leader_cache: Arc<RwLock<HashMap<ShardId, LeaderInfo>>>,
    /// Filer 地址列表 (故障转移)
    filer_addresses: Vec<String>,
    /// 缓存 TTL
    cache_ttl: Duration,
}

struct LeaderInfo {
    addr: String,
    last_updated: Instant,
    version: u64,   // leader 变更版本号
}
```

### 3.3 请求分类与通道分配

#### DataChannel - 元数据通道

| 操作 | MsgType | 描述 |
|------|---------|------|
| lookup | Lookup | 查找 inode |
| create | Create | 创建文件 |
| mkdir | CreateDirectory | 创建目录 |
| unlink | DeleteFile | 删除文件 |
| rmdir | DeleteDirectory | 删除目录 |
| rename | Rename | 重命名 |
| symlink | CreateSymlink | 创建符号链接 |
| link | CreateHardLink | 创建硬链接 |
| setattr | SetAttr | 设置属性 |
| readdir | ListDirectory | 列出目录 |
| readlink | Readlink | 读取符号链接 |
| delta sync | PushDelta, PullDelta, Invalidate | 一致性协议 |

#### ControlChannel - 控制通道

| 操作 | MsgType | 描述 |
|------|---------|------|
| 心跳 | Heartbeat | 保活 |
| Leader 查询 | QueryLeader | 查询 shard leader |
| 主动通知 | NotifyLeaderChange | Leader 变更通知 |
| 缓存刷新 | FlushCaches | 通知刷新缓存 |
| 配置变更 | ConfigReload | 通知重新加载配置 |

### 3.4 通道调度逻辑

```rust
impl ShardConnection {
    /// 运行通道调度器 (在独立任务中运行)
    async fn run_scheduler(&self) {
        loop {
            // 优先级调度: 控制通道优先于数据通道
            tokio::select! {
                // 控制通道 - 优先处理
                result = self.control_rx.as_mut().unwrap().recv() => {
                    if let Some(req) = result {
                        self.handle_control_request(req).await;
                    }
                }
                // 数据通道 - 串行处理 (Raft 提交顺序要求)
                result = self.data_rx.as_mut().unwrap().recv() => {
                    if let Some(req) = result {
                        self.handle_data_request(req).await;
                    }
                }
                // 连接断开
                _ = self.transport.closed() => {
                    error!("Shard {} connection closed", self.shard_id);
                    self.on_connection_lost().await;
                    break;
                }
            }
        }
    }
}
```

### 3.5 对外 API

```rust
impl MetaShardClient {
    /// 查找 inode (DataChannel)
    pub async fn lookup(&self, parent: u64, name: &str) -> NetResult<FilerEntry> { ... }
    /// 创建文件 (DataChannel)
    pub async fn create(&self, parent: u64, name: &str, mode: u32) -> NetResult<FilerEntry> { ... }
    /// 创建目录 (DataChannel)
    pub async fn mkdir(&self, parent: u64, name: &str, mode: u32) -> NetResult<FilerEntry> { ... }
    /// 删除文件 (DataChannel)
    pub async fn unlink(&self, parent: u64, name: &str) -> NetResult<()> { ... }
    /// 心跳 (ControlChannel)
    pub async fn heartbeat(&self, shard_id: ShardId) -> NetResult<()> { ... }
    /// 查询 Leader (ControlChannel)
    pub async fn query_leader(&self, shard_id: ShardId) -> NetResult<String> { ... }
}
```

---

## 4. VolumeClient 设计

### 4.1 设计理念 (v3.0)

**核心变更**:
- ✅ 读写共享统一 `DataQueue` 入口
- ✅ 底层由 N 条 `Channel`（TCP 连接）组成通道池
- ✅ Lease 和 Mgmt 保持独立独占通道，保证高优先级请求不被数据请求阻塞
- ✅ 支持内核态 C 实现的简单映射
- ✅ **动态管理**: 通过 `ClusterTopologyManager` 动态发现和添加 Volume，而不是静态配置。

### 4.2 动态发现与初始化

与 `MetaShardClient` 类似，`VolumeClient` 也依赖 `ClusterTopologyManager`：
-   启动时获取所有 Volume 节点的列表和位置。
-   监听 Master 的 Volume 变更事件（如 Volume 故障迁移、新 Volume 添加）。

### 4.3 结构体定义

```rust
/// Volume 数据客户端
pub struct VolumeClient {
    identity: Arc<ClientIdentity>,
    router: Arc<VolumeRouter>,
    /// volume_id → VolumeConnection 映射
    connections: Arc<RwLock<HashMap<VolumeId, VolumeConnection>>>,
    /// 熔断器 (per-volume)
    breakers: Arc<RwLock<HashMap<VolumeId, CircuitBreaker>>>,
    /// 活跃 Lease
    active_leases: Arc<RwLock<HashMap<String, LeaseInfo>>>,
    /// 全局通道数
    default_channels: usize,
    runtime: Arc<tokio::runtime::Runtime>,
}

/// 单个 Volume 的连接 (v3.0: 3 入口 + N 通道池)
pub struct VolumeConnection {
    volume_id: VolumeId,
    addr: String,

    // ===== 三个入口 (严格优先级) =====
    /// Lease 独占通道 (最高优先级)
    lease_tx: mpsc::Sender<LeaseRequest>,
    lease_rx: Mutex<mpsc::Receiver<LeaseRequest>>,

    /// 管理独占通道 (第二优先级)
    mgmt_tx: mpsc::Sender<MgmtRequest>,
    mgmt_rx: Mutex<mpsc::Receiver<MgmtRequest>>,

    /// 统一数据入口 (读写共享)
    data_tx: mpsc::Sender<DataRequest>,
    data_rx: Mutex<mpsc::Receiver<DataRequest>>,

    // ===== 通道池 (N 条 TCP 连接) =====
    channels: Vec<Channel>,
    selector: ChannelSelector,

    // ===== 状态管理 =====
    pending: Arc<RwLock<HashMap<RequestId, RequestContext>>>,
    last_heartbeat: Arc<AtomicInstant>,

    // ===== 传输层 =====
    // (每个 Channel 内部持有独立 PowerFsNetClient)
}

/// 单条传输通道
pub struct Channel {
    id: usize,
    transport: Arc<PowerFsNetClient>,
    inflight: Arc<AtomicUsize>,   // 当前在途请求数
    max_inflight: usize,
}

/// 通道选择器
pub enum ChannelSelector {
    /// 轮询
    RoundRobin { next: AtomicUsize },
    /// 最少在途 (推荐默认)
    LeastInflight,
    /// Stripe 亲和 + 最少在途
    StickyShard,
}

impl ChannelSelector {
    pub fn pick(&self, channels: &[Channel], req: &DataRequest) -> &Channel { ... }
}
```

### 4.4 三种请求类型

```rust
/// 数据请求 (读写共用 DataQueue)
pub struct DataRequest {
    pub ctx: RequestContext,
    pub kind: DataOp,
    pub volume_id: VolumeId,
    pub file_key: u64,
    pub offset: u64,
    pub data: Option<Vec<u8>>,    // 写时有值
    pub lease_token: Option<String>,
    pub reply_tx: Option<oneshot::Sender<NetResult<NetMessage>>>,
}

#[derive(Clone, Copy)]
pub enum DataOp {
    Read,
    Write,
    Delete,
    BatchWrite,
}

/// Lease 请求 (LeaseChannel 独占)
pub struct LeaseRequest {
    pub ctx: RequestContext,
    pub op: LeaseOp,
    pub reply_tx: Option<oneshot::Sender<NetResult<NetMessage>>>,
}

#[derive(Clone)]
pub enum LeaseOp {
    Acquire { inode: u64, stripe_start: u64, stripe_count: u64, exclusive: bool },
    Renew { token: String },
    Release { token: String },
    Query { token: String },
}

/// 管理请求 (MgmtChannel 独占)
pub struct MgmtRequest {
    pub ctx: RequestContext,
    pub op: MgmtOp,
    pub reply_tx: Option<oneshot::Sender<NetResult<NetMessage>>>,
}

#[derive(Clone)]
pub enum MgmtOp {
    StatFs,
    VolumeStatus,
    Heartbeat,
    HealthCheck,
}
```

### 4.5 通道调度逻辑

```rust
impl VolumeConnection {
    /// 运行通道调度器
    async fn run_scheduler(&self) {
        loop {
            // 按优先级调度: Lease > Mgmt > Data
            tokio::select! {
                // 1. Lease 通道 - 最高优先级 (永不被阻塞)
                result = self.lease_rx.as_mut().unwrap().recv() => {
                    if let Some(req) = result {
                        self.handle_lease_request(req).await;
                    }
                }
                // 2. 管理通道 - 第二优先级
                result = self.mgmt_rx.as_mut().unwrap().recv() => {
                    if let Some(req) = result {
                        self.handle_mgmt_request(req).await;
                    }
                }
                // 3. 统一数据通道 - 第三优先级 (读写共享)
                result = self.data_rx.as_mut().unwrap().recv() => {
                    if let Some(req) = result {
                        self.handle_data_request(req).await;
                    }
                }
                // 连接断开
                _ = self.channels_dead() => {
                    error!("Volume {} all channels dead", self.volume_id);
                    break;
                }
            }
        }
    }

    /// 处理数据请求 (统一入口 + 通道选择分发)
    async fn handle_data_request(&self, req: DataRequest) {
        let ctx = req.ctx.clone();
        ctx.mark_sent();

        // 选择通道
        let channel = self.selector.pick(&self.channels, &req);

        // 加入 pending 表
        self.pending.write().await.insert(ctx.request_id.clone(), ctx.clone());
        channel.inflight.fetch_add(1, Ordering::Relaxed);

        // 发送
        match channel.transport.send_request(&ctx).await {
            Ok(response) => {
                ctx.mark_complete(response.clone());
                self.pending.write().await.remove(&ctx.request_id);
                channel.inflight.fetch_sub(1, Ordering::Relaxed);

                if let Some(tx) = req.reply_tx {
                    let _ = tx.send(Ok(response));
                }
            }
            Err(NetError::ConnectionLost) => {
                warn!("Channel {} lost, {} inflight pending",
                      channel.id, channel.inflight.load(Ordering::Relaxed));
                channel.dead = true;
                self.breaker.record_failure();
            }
            Err(e) => {
                ctx.mark_failed(e.into());
                self.pending.write().await.remove(&ctx.request_id);
                channel.inflight.fetch_sub(1, Ordering::Relaxed);
                self.breaker.record_failure();

                if let Some(tx) = req.reply_tx {
                    let _ = tx.send(Err(e));
                }
            }
        }
    }
}
```

### 4.6 请求分类与通道分配

#### LeaseChannel - Lease 通道 (最高优先级，独占)

| 操作 | MsgType | 描述 |
|------|---------|------|
| 获取 Lease | RangeLease (0x0060) | 获取范围锁 |
| 续约 | LeaseRenew (0x0061) | 续约 |
| 释放 | LeaseRelease (0x0062) | 主动释放 |
| 查询 | LeaseQuery (0x0063) | 查询 Lease 状态 |

#### MgmtChannel - 管理通道 (第二优先级，独占)

| 操作 | MsgType | 描述 |
|------|---------|------|
| 文件系统统计 | StatFs (0x0070) | 真实空间使用信息 |
| Volume 状态 | VolumeStatus (0x0071) | Volume 健康状态 |
| 心跳 | Heartbeat | 保活 |
| 健康检查 | VolumeHealth (0x0072) | 深度健康检查 |

#### DataQueue + ChannelPool - 数据通道 (第三优先级，共享)

| 操作 | MsgType | 描述 |
|------|---------|------|
| 读数据 | ReadNeedle | 单条读取 |
| 大对象读 | ReadNeedleBlob | 大块数据读取 |
| 写数据 | WriteNeedle | 单条写入 |
| 批量写 | BatchWriteNeedle | 批量写入 |
| 删除 | DeleteNeedle | 删除数据 |

### 4.7 通道选择策略

#### StickyShard + LeastInflight (推荐默认)

```rust
impl ChannelSelector {
    pub fn pick(&self, channels: &[Channel], req: &DataRequest) -> &Channel {
        match self {
            // 1. StickyShard: 相同 stripe 的请求走同一通道 (TCP 亲和性)
            ChannelSelector::StickyShard => {
                let idx = (req.file_key >> 26) as usize % channels.len();
                let ch = &channels[idx];
                // 如果命中通道在途过多，退化到 LeastInflight
                if ch.inflight.load(Ordering::Relaxed) < ch.max_inflight / 2 {
                    return ch;
                }
                channels.iter().min_by_key(|c| c.inflight.load(Ordering::Relaxed)).unwrap()
            }
            // 2. LeastInflight: 选在途最少的通道
            ChannelSelector::LeastInflight => {
                channels.iter().min_by_key(|c| c.inflight.load(Ordering::Relaxed)).unwrap()
            }
            // 3. RoundRobin: 轮询
            ChannelSelector::RoundRobin { next } => {
                let n = channels.len();
                let idx = next.fetch_add(1, Ordering::Relaxed) % n;
                &channels[idx]
            }
        }
    }
}
```

### 4.8 Lease 管理

```rust
impl VolumeClient {
    /// 获取 Lease (走 LeaseChannel)
    pub async fn acquire_lease(
        &self,
        volume_id: VolumeId,
        inode: u64,
        stripe_start: u64,
        stripe_count: u64,
        exclusive: bool,
    ) -> NetResult<LeaseToken> {
        let conn = self.get_or_create_connection(volume_id).await?;

        let ctx = RequestContext::new(RequestKind::Lease, MsgType::RangeLease, ...);

        let (reply_tx, rx) = oneshot::channel();
        let request = LeaseRequest {
            ctx,
            op: LeaseOp::Acquire { inode, stripe_start, stripe_count, exclusive },
            reply_tx: Some(reply_tx),
        };

        conn.lease_tx.send(request).await.map_err(|_| NetError::ChannelClosed)?;

        // 等待响应
        let response = rx.await.map_err(|_| NetError::Timeout)??;

        // 解析 Lease Token
        let token = parse_lease_response(&response)?;

        // 记录活跃 Lease
        self.active_leases.write().await.insert(token.clone(), LeaseInfo {
            token: token.clone(),
            inode,
            stripe_start,
            stripe_count,
            acquired_at: Instant::now(),
            expire_at: Instant::now() + Duration::from_secs(30),
        });

        Ok(token)
    }

    /// 释放 Lease
    pub async fn release_lease(&self, token: &str) -> NetResult<()> { ... }

    /// 续约 (后台定时任务)
    async fn renew_expired_leases(&self) { ... }
}
```

### 4.9 StatFs 实现

```rust
impl VolumeClient {
    /// 获取真实的文件系统统计信息 (走 MgmtChannel)
    pub async fn statfs(&self) -> NetResult<FsStats> {
        let volumes = self.router.list_volumes().await;

        let mut total_size: u64 = 0;
        let mut used_size: u64 = 0;
        let mut free_size: u64 = 0;
        let mut file_count: u64 = 0;

        for volume in &volumes {
            let conn = self.get_or_create_connection(volume.id).await?;
            // 每个 Volume 走 MgmtChannel 查询
            let stats = conn.query_statfs().await?;
            total_size += stats.total;
            used_size += stats.used;
            free_size += stats.free;
            file_count += stats.file_count;
        }

        Ok(FsStats { total_size, used_size, free_size, file_count })
    }
}
```

### 4.10 对外 API

```rust
impl VolumeClient {
    /// 读数据 (走 DataQueue → 通道池)
    pub async fn read(
        &self, volume_id: VolumeId, file_key: u64, offset: i64, size: i32,
    ) -> NetResult<Vec<u8>> {
        let ctx = RequestContext::new(RequestKind::DataRead, MsgType::ReadNeedle, ...);
        let request = DataRequest {
            ctx, volume_id, file_key,
            kind: DataOp::Read,
            offset: offset as u64,
            data: None,
            lease_token: None,
            reply_tx: Some(reply_tx),
        };
        let conn = self.get_or_create_connection(volume_id).await?;
        conn.data_tx.send(request).await.map_err(|_| NetError::ChannelClosed)?;
        let response = rx.await.map_err(|_| NetError::Timeout)??;
        Ok(response.data)
    }

    /// 写数据 (走 DataQueue → 通道池)
    pub async fn write(
        &self, volume_id: VolumeId, file_key: u64, data: &[u8], lease_token: &str,
    ) -> NetResult<()> {
        self.verify_lease(lease_token)?;
        let ctx = RequestContext::new(RequestKind::DataWrite, MsgType::WriteNeedle, ...);
        let request = DataRequest {
            ctx, volume_id, file_key,
            kind: DataOp::Write,
            offset: 0,
            data: Some(data.to_vec()),
            lease_token: Some(lease_token.to_string()),
            reply_tx: Some(reply_tx),
        };
        let conn = self.get_or_create_connection(volume_id).await?;
        conn.data_tx.send(request).await.map_err(|_| NetError::ChannelClosed)?;
        let _ = rx.await.map_err(|_| NetError::Timeout)??;
        Ok(())
    }

    /// 删除数据 (走 DataQueue → 通道池)
    pub async fn delete(...) -> NetResult<()> { ... }

    /// 获取 Lease (走 LeaseChannel)
    pub async fn acquire_lease(...) -> NetResult<LeaseToken> { ... }

    /// 获取 StatFs (走 MgmtChannel)
    pub async fn statfs(&self) -> NetResult<FsStats> { ... }

    /// Volume 状态 (走 MgmtChannel)
    pub async fn volume_status(&self, volume_id: VolumeId) -> NetResult<VolumeStatus> { ... }
}
```

---

## 5. 请求生命周期

### 5.1 请求状态机

```
    Init ──send──▶ Sent ──response──▶ Complete
      │              │  │  │
      │              │  │  └──redirect──▶ Resent ──new leader──▶ Complete
      │              │  │
      │              │  └──timeout────▶ Wait ──retry──▶ Sent
      │              │
      │              └──fail──▶ Failed
      │
      └──circuit open──▶ Cancelled
```

### 5.2 RequestContext

```rust
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// 全局唯一请求 ID
    pub request_id: RequestId,
    /// 请求类型 (决定通道和重试策略)
    pub kind: RequestKind,
    /// 协议消息类型
    pub msg_type: MsgType,

    /// 路由信息
    pub target_shard: Option<ShardId>,
    pub target_volume: Option<VolumeId>,
    pub target_addr: String,

    /// 负载
    pub body: Vec<u8>,
    pub data: Vec<u8>,

    /// 状态追踪
    pub state: Arc<Mutex<RequestState>>,
    pub created_at: Instant,
    pub sent_at: Option<Instant>,

    /// 重试控制
    pub retry_count: Arc<AtomicU32>,
    pub max_retries: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    // MetaShard
    MetaRead,           // lookup, getattr, readdir
    MetaWrite,          // create, mkdir, unlink, rename
    MetaControl,        // heartbeat, query leader
    // Volume
    DataRead,           // read needle
    DataWrite,          // write needle
    Lease,              // range lease
    VolumeMgmt,         // statfs, status
}

impl RequestKind {
    pub fn channel(&self) -> ChannelType {
        match self {
            Self::MetaRead | Self::MetaWrite => ChannelType::MetaData,
            Self::MetaControl => ChannelType::MetaControl,
            // v3.0: 读写统一走 VolumeData 通道池
            Self::DataRead | Self::DataWrite => ChannelType::VolumeData,
            Self::Lease => ChannelType::VolumeLease,
            Self::VolumeMgmt => ChannelType::VolumeMgmt,
        }
    }

    pub fn max_retries(&self) -> u32 {
        match self {
            Self::Lease => 5,
            Self::DataRead | Self::MetaRead => 3,
            Self::DataWrite | Self::MetaWrite => 2,
            Self::MetaControl | Self::VolumeMgmt => 1,
        }
    }
}
```

---

## 6. Exactly-Once 保证

### 6.1 设计原则

**核心思想：客户端是“无状态”的，所有状态的权威来源于服务端。**
因此，我们**不采用** Client-side RequestJournal（客户端持久化日志）来保证可靠性。所有可靠性依赖服务端的机制来保证。

### 6.2 保证层级

```
Layer 1: Server-side RaftIdempotencyIndex (Filer) / Volume IdempotencyLog (Volume)
  - RequestId 嵌入服务端持久化日志
  - Leader 切换后可查询已处理请求
  - 重放/重连返回幂等结果
  - 这是唯一的幂等保证

Layer 2: Raft Consensus (Filer) / RocksDB WAL (Volume)
  - 线性一致，已提交日志不丢失
  - 写入原子性

Layer 3: Lease (Volume)
  - 短租约 (如 30 秒)，过期自动失效
  - 客户端重启后，旧 Lease 失效，需重新获取
  - 保证并发写入的强一致性
```

### 6.3 客户端重连策略

客户端需要区分两种重连场景，处理策略不同：

#### 场景 A: 网络超时重连 (连接断开但进程存活)
- **特征**：内存中 `VolumeClient` / `MetaShardClient` 实例还在。
- **处理**：
  1. 检测到底层 TCP 连接断开。
  2. 尝试重连服务端。
  3. 重连成功后，**遍历内存中的 `PendingTable`**，将状态为 `Timeout` 或 `Wait` 的请求重新发送。
  4. 服务端通过 `RaftIdempotencyIndex` / `Volume IdempotencyLog` 返回幂等结果。
  5. 这是内存级别的临时状态恢复。

#### 场景 B: 进程崩溃重连 (进程被杀后重启)
- **特征**：客户端进程死掉后重启，所有内存状态丢失。
- **处理**：
  1. 进程启动，`VolumeClient` 和 `MetaShardClient` **从零初始化**。
  2. **立即清空所有本地状态**（包括 `PendingTable`、本地缓存、`ActiveLeases` 列表）。
  3. 连接服务端，后续所有操作都是“全新”的。
  4. 对于元数据操作（如 `mkdir`），直接发送，由服务端状态决定最终结果。
  5. 对于数据操作，**必须重新获取 Lease**。旧的 Lease 因客户端消失和 TTL 过期，最终会失效。如果写入时旧 Lease 还在，会失败，客户端重试获取新 Lease 即可。

### 6.4 场景分析

#### 场景 1: Leader 切换 (网络超时重连)

```
T0: Client → Leader(A): Request(id=X, op=CreateDir)
T1: Leader(A) 提交 Raft (index=100), 挂了
T2: Raft 选举 Leader(B), 从日志恢复 (index=100)
T3: Client 超时，检测到连接断开
T4: Client 重连成功，将内存中 PendingTable 的 Request(id=X) 重新发送给 Leader(B)
T5: Leader(B) 查 RaftIdempotencyIndex: id=X 已处理 → 返回原结果
```

#### 场景 2: Volume 重启 (进程崩溃重连)

```
T0: Client → Volume: Write(id=X, lease=token)
T1: Client 进程崩溃
T2: Volume 重启, WAL 回滚未完成写入, 清理过期 Lease
T3: Client 重启, 清空所有状态
T4: Client 连接 Volume, 尝试 Write(id=X, lease=token_new)
T5: Volume 验证 token_new, 写入成功
(注: 如果 T3 时 Volume 未重启，且 token 未过期，T4 的 Write 仍会成功。如果 token 过期，Client 需先 AcquireLease)
```

---

## 7. Raft 日志集成

### 7.1 扩展 ShardCommand

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandMeta {
    pub request_id: String,     // 幂等 ID
    pub client_id: String,     // 客户端 ID
    pub seq: u64,              // 序列号
    pub timestamp_ms: u64,     // 时间戳
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShardCommand {
    CreateFile {
        meta: CommandMeta,      // 新增
        parent_inode: u64,
        name: String,
        inode: u64,
    },
    // ... 所有命令添加 meta 字段 ...
}
```

### 7.2 RaftIdempotencyIndex

```rust
/// Raft 日志幂等索引 (独立 ColumnFamily)
pub struct RaftIdempotencyIndex {
    entries: HashMap<String, IdempotentEntry>,
    max_size: usize,
}

struct IdempotentEntry {
    raft_index: u64,
    result: IdempotentResult,
    created_at: Instant,
}

enum IdempotentResult {
    Success { response_data: Vec<u8> },
    Failure { error_code: u16, error_msg: String },
}

impl RaftIdempotencyIndex {
    pub fn check(&self, request_id: &str) -> Option<&IdempotentResult> {
        self.entries.get(request_id).map(|e| &e.result)
    }

    pub fn record(&mut self, request_id: &str, index: u64, result: IdempotentResult) { ... }

    /// 从 Raft 日志重建 (Leader 启动时调用)
    pub fn rebuild_from_log(&mut self, entries: &[RaftEntry]) { ... }
}
```

### 7.3 Filer 请求处理流程

```rust
impl FilerNetHandler {
    async fn handle_request(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        // 1. 提取 RequestId
        let request_id = msg.get_request_id()?;

        // 2. 幂等检查
        if let Some(result) = self.idempotency_index.check(&request_id) {
            return self.build_idempotent_response(msg, result);
        }

        // 3. 解析命令 (含 meta)
        let command = self.parse_command(msg, &request_id)?;

        // 4. Propose 到 Raft
        let index = self.raft_manager.propose(shard_id, command.serialize()).await?;

        // 5. 等待 apply
        let result = self.wait_for_apply(shard_id, index).await?;

        // 6. 记录幂等索引
        self.idempotency_index.record(&request_id, index, IdempotentResult::Success { ... });

        Ok(result)
    }
}
```

---

## 8. 熔断器与健康管理

### 8.1 CircuitBreaker

```rust
pub struct CircuitBreaker {
    state: RwLock<CircuitState>,
    config: CircuitBreakerConfig,
}

struct CircuitState {
    health: ConnectionHealth,
    recent_failures: VecDeque<Instant>,
    recent_successes: VecDeque<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionHealth {
    Healthy,
    Degraded { failure_rate: f64 },
    CircuitOpen { until: Instant },
}

impl CircuitBreaker {
    pub fn can_send(&self) -> bool {
        let state = self.state.read().unwrap();
        match state.health {
            ConnectionHealth::CircuitOpen { until } => Instant::now() >= until,
            _ => true,
        }
    }

    pub fn record_success(&self) { ... }
    pub fn record_failure(&self) { ... }
    pub fn force_open(&self) { ... }
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u32,
    pub open_duration: Duration,
    pub success_threshold: u32,
    pub window_duration: Duration,
}
```

### 8.2 健康监控

```rust
impl MetaShardClient {
    pub async fn start_health_monitor(&self) { ... }
}

impl VolumeClient {
    /// 启动健康监控 (后台任务)
    pub async fn start_health_monitor(&self) {
        // 1. 检查 Volume 连接 (MgmtChannel 心跳)
        // 2. 续约 Lease (LeaseChannel)
        // 3. 检查通道池健康度
        // 4. 触发熔断/恢复
    }
}
```

---

## 9. 协议扩展

### 9.1 新增 TLV 字段

```rust
#[derive(Debug, Clone, Copy)]
pub enum FieldId {
    // 请求幂等
    RequestId = 0x0050,
    RequestKind = 0x0051,
    ClientAckSeq = 0x0052,

    // Lease
    LeaseToken = 0x0060,
    LeaseEpoch = 0x0061,
    LeaseOp = 0x0062,

    // 路由
    RedirectTarget = 0x0070,
    RedirectReason = 0x0071,

    // v3.0: 通道选择
    ChannelId = 0x0080,
    StripeHash = 0x0081,
}
```

### 9.2 编解码辅助

```rust
impl TlvEncoder {
    pub fn add_request_id(&mut self, id: &RequestId) { ... }
    pub fn add_lease_token(&mut self, token: &str) { ... }
    pub fn add_channel_id(&mut self, id: u8) { ... }
    pub fn add_stripe_hash(&mut self, hash: u64) { ... }
}

impl TlvDecoder {
    pub fn get_request_id(&self) -> Option<RequestId> { ... }
    pub fn get_lease_token(&self) -> Option<String> { ... }
    pub fn get_channel_id(&self) -> Option<u8> { ... }
    pub fn get_stripe_hash(&self) -> Option<u64> { ... }
}
```

---

## 10. FUSE 层集成

### 10.1 PowerFsFs 重构

```rust
pub struct PowerFsFs {
    // ===== 两个核心客户端 =====
    meta_client: Arc<MetaShardClient>,    // Filer 客户端
    volume_client: Arc<VolumeClient>,      // Volume 客户端 (v3.0)

    // ===== 缓存层 =====
    cache: Arc<MetadataCache>,
    chunk_cache: Arc<ChunkCache>,

    // ===== 其他状态 =====
    collection: String,
    replication: String,
}
```

### 10.2 回调示例

```rust
impl FileSystem for PowerFsFs {
    fn lookup(&self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_str().unwrap_or("");
        match self.meta_client.lookup(parent, name_str) {
            Ok(entry) => {
                let attr = self.build_attr(&entry);
                reply.entry(&TTL, &attr, 0);
            }
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn write(&self, _req: &Request, inode: u64, fh: u64, offset: i64, data: &[u8], reply: ReplyWritten) {
        // 1. 获取文件信息
        let entry = match self.cache.get_inode(inode) {
            Some(e) => e,
            None => { reply.error(libc::ENOENT); return; }
        };

        // 2. 获取 Lease (VolumeClient - LeaseChannel)
        let lease_token = match self.volume_client.acquire_lease(
            entry.volume_id, entry.stripe_start, entry.stripe_count, true
        ) {
            Ok(token) => token,
            Err(_) => { reply.error(libc::EIO); return; }
        };

        // 3. 写数据 (VolumeClient - DataQueue → 通道池)
        match self.volume_client.write(
            entry.volume_id, entry.file_key, data, &lease_token
        ) {
            Ok(_) => {
                // 4. 更新元数据 (MetaShardClient - DataChannel)
                let _ = self.meta_client.setattr(inode, Some(offset + data.len() as u64), None, None, None);
                reply.written(data.len() as u32);
            }
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn read(&self, _req: &Request, inode: u64, fh: u64, offset: i64, size: u32, reply: ReplyData) {
        let entry = match self.cache.get_inode(inode) {
            Some(e) => e,
            None => { reply.error(libc::ENOENT); return; }
        };

        // 读数据 (VolumeClient - DataQueue → 通道池)
        match self.volume_client.read(
            entry.volume_id, entry.file_key, offset, size as i32
        ) {
            Ok(data) => reply.data(&data),
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn statfs(&self, _req: &Request, _inode: u64, reply: ReplyStatfs) {
        match self.volume_client.statfs() {
            Ok(stats) => {
                let mut st: libc::statvfs64 = unsafe { std::mem::zeroed() };
                st.f_bsize = 4096;
                st.f_frsize = 4096;
                st.f_blocks = stats.total_size / 4096;
                st.f_bfree = stats.free_size / 4096;
                st.f_bavail = stats.free_size / 4096;
                st.f_files = stats.file_count;
                reply.statfs(&st);
            }
            Err(_) => reply.error(libc::EIO),
        }
    }
}
```

---

## 11. 动态拓扑管理: ClusterTopologyManager

> **v3.0 新增核心组件** — 作为客户端动态发现和适应集群变化的引导层。

### 11.1 设计理念

分布式文件系统的客户端不应该硬编码服务端地址。`ClusterTopologyManager` 作为 FUSE 客户端与 Master 之间的桥梁，提供：
-   **引导 (Bootstrap)**：启动时从 Master 获取最新拓扑快照。
-   **动态发现**：自动管理与 Filer 和 Volume 的连接。
-   **事件驱动**：实时响应用于 Master 推送的拓扑变更通知（如 Leader 切换、节点故障）。
-   **Master HA**：处理 Master 本身的高可用性（故障切换）。

### 11.2 核心结构体

```rust
pub struct ClusterTopologyManager {
    /// Master 客户端 (支持 Raft HA)
    master_client: Arc<MasterClient>,

    /// 本地拓扑缓存
    topology: Arc<RwLock<ClusterTopology>>,

    /// 订阅者: 监听分片变更 (供 MetaShardClient 使用)
    shard_subscribers: Arc<Vec<Box<dyn Fn(ShardEvent) + Send + Sync>>>,
    /// 订阅者: 监听卷变更 (供 VolumeClient 使用)
    volume_subscribers: Arc<Vec<Box<dyn Fn(VolumeEvent) + Send + Sync>>>,

    /// 运行时
    runtime: Arc<tokio::runtime::Runtime>,
}

struct ClusterTopology {
    shards: HashMap<ShardId, ShardInfo>,
    volumes: HashMap<VolumeId, VolumeInfo>,
}

struct ShardInfo {
    id: ShardId,
    leader: String,
    version: u64, // Leader 变更的版本号
    last_updated: Instant,
}

struct VolumeInfo {
    id: VolumeId,
    addr: String,
    port: u16,
    status: VolumeStatus,
}
```

### 11.3 初始化流程 (Bootstrap)

```
1. [FUSE 启动] 
2.   └─> 创建 ClusterTopologyManager 实例
3.   └─> manager.bootstrap(master_addr)
        ├─> master_client.connect_to_master()
        ├─> topology = master_client.get_cluster_topology()
        ├─> manager.update_cache(topology)
        ├─> manager.notify_shard_subscribers(topology.shards)
        │   └─> MetaShardClient 收到通知，为每个 Shard 创建 ShardConnection
        ├─> manager.notify_volume_subscribers(topology.volumes)
        │   └─> VolumeClient 收到通知，为每个 Volume 创建 VolumeConnection
        └─> manager.start_watcher()  // 启动后台监听任务
4. [FUSE 启动完成，开始服务用户请求]
```

### 11.4 动态更新机制

#### 方案：Master Watch + 定期 Poll

-   **Watch 机制 (推送)**：Manager 与 Master 保持一个长连接。当 Master 检测到集群状态变更（如 Raft 选举）时，主动通过此连接推送 `TopologyChangeEvent`。这是最实时的方式。

-   **Poll 机制 (兜底)**：为了防止长连接断开导致漏接事件，Manager 定期（如每 5 秒）向 Master 请求一次最新的拓扑快照 (`PollTopology`)，与本地缓存比对差异。

#### 处理变更

```rust
impl ClusterTopologyManager {
    async fn handle_master_event(&self, event: MasterEvent) {
        match event {
            MasterEvent::ShardLeaderChanged { id, new_addr, new_version } => {
                // 更新缓存
                self.topology.write().await.shards.insert(id, ShardInfo { ... });
                // 通知订阅者
                for sub in self.shard_subscribers.iter() {
                    sub(ShardEvent::LeaderChanged { id, new_addr, new_version });
                }
            }
            MasterEvent::VolumeDown { id } => {
                // 通知订阅者
                for sub in self.volume_subscribers.iter() {
                    sub(VolumeEvent::NodeDown { id });
                }
            }
        }
    }
}
```

### 11.5 Master 高可用

Master 本身也是一个 Raft 集群。`ClusterTopologyManager` 必须能处理 Master Leader 的切换：
-   **多地址配置**：FUSE 客户端启动时配置多个 Master 地址 (例如，`master1:9333,master2:9333,master3:9333`)。
-   **`MasterClient` 逻辑**：
    1.  启动时，依次尝试连接所有 Master 地址。
    2.  一旦成功连接到一个 Master，通过 `RaftLeadership` 检查它是否是 Leader。如果不是，获取 Leader 地址并重新连接。
    3.  连接断开时，`MasterClient` 会自动重新尝试连接，按优先级列表循环尝试。
    4.  重连成功后，立即拉取最新拓扑并恢复 Watch 机制。

---

## 12. 内核文件系统适配层

> **v3.0 新增章节** — 确保通信架构在用户态（FUSE）和内核态（powerfs_mod）完全对齐。

### 12.1 设计目标

1. **协议兼容**：用户态和内核态使用相同的二进制协议（powerfs-net wire format）
2. **语义一致**：相同的请求类型、状态机、重试策略、熔断行为
3. **结构映射**：用户态的 `MetaShardClient` / `VolumeClient` 可一比一映射到内核态 C 实现
4. **性能优先**：内核态实现需零拷贝、最少锁、最小内存占用

### 12.2 用户态 ↔ 内核态 结构映射

#### MetaShardClient 映射

| 用户态 (Rust) | 内核态 (C) |
|---------------|-----------|
| `MetaShardClient` | `struct powerfs_meta_client` |
| `ShardConnection.data_tx` | `wait_queue_head_t data_wq` + `list_head data_queue` |
| `ShardConnection.control_tx` | `wait_queue_head_t ctrl_wq` + `list_head ctrl_queue` |
| `mpsc::Receiver` | `wait_event_interruptible_timeout` |
| `Arc<PowerFsNetClient>` | `struct socket *sock` |
| `pending: HashMap<RequestId, Context>` | `struct rb_root pending` + `spinlock_t` |
| `CircuitBreaker` | `atomic_t failures` + `jiffies open_until` |
| `tokio::runtime::Runtime` | `struct task_struct *worker` |

#### VolumeClient 映射 (v3.0)

| 用户态 (Rust) | 内核态 (C) |
|---------------|-----------|
| `VolumeClient` | `struct powerfs_volume_client` |
| `LeaseChannel` (mpsc) | `lease_wq` (wait_queue_head_t) + 独立 kthread |
| `MgmtChannel` (mpsc) | `mgmt_wq` (wait_queue_head_t) + 独立 kthread |
| `DataQueue` (mpsc) | `data_wq` (wait_queue_head_t) + `list_head data_queue` |
| `ChannelPool (Vec<Channel>)` | `struct powerfs_channel channels[N]` |
| `ChannelSelector` | `atomic_t next_idx` + hash 函数 |
| `Channel.inflight` | `atomic_t inflight` |
| `Channel.transport` | `struct socket *sock` |

### 12.3 内核态核心结构

```c
/* powerfs_transport_client.h — 内核态客户端抽象 */

/* 请求类型 (与用户态 RequestKind 对齐) */
enum powerfs_req_kind {
    POWERFS_REQ_META_READ,    /* lookup, readdir */
    POWERFS_REQ_META_WRITE,   /* create, mkdir, unlink */
    POWERFS_REQ_META_CONTROL, /* heartbeat, query leader */
    POWERFS_REQ_DATA_READ,   /* volume read */
    POWERFS_REQ_DATA_WRITE,  /* volume write */
    POWERFS_REQ_LEASE,       /* range lease */
    POWERFS_REQ_VOL_MGMT,    /* statfs, volume status */
};

/* 通用请求头 (嵌入到具体请求结构) */
struct powerfs_req_hdr {
    u64 request_id;           /* 全局唯一 ID, 与用户态一致 */
    enum powerfs_req_kind kind;
    u16 msg_type;             /* 协议 MsgType */
    int state;                /* INIT/SENT/COMPLETE/FAILED */
    atomic_t refcnt;
    struct completion done;   /* 完成通知 */
    void (*on_complete)(struct powerfs_req_hdr *req, int result);
};

/* === Meta 请求 === */
struct powerfs_meta_req {
    struct powerfs_req_hdr hdr;
    u64 parent_ino;
    char name[256];
    u64 mode, uid, gid;
    /* ... 其他元数据字段 ... */
};

/* === 数据请求 (读写共用, v3.0 核心) === */
struct powerfs_data_req {
    struct powerfs_req_hdr hdr;
    u64 volume_id;
    u64 file_key;
    loff_t offset;
    size_t size;
    int op;                    /* READ / WRITE / DELETE */
    char __user *user_buf;     /* 用户态缓冲区 (read 时) */
    void *kernel_buf;          /* 内核缓冲区 (write 时) */
    size_t buf_len;
    char lease_token[128];     /* Lease Token, 可选 */
    u64 stripe_hash;           /* 用于 StickyShard 选择通道 */
};

/* === Lease 请求 === */
struct powerfs_lease_req {
    struct powerfs_req_hdr hdr;
    int op;                    /* ACQUIRE / RENEW / RELEASE / QUERY */
    u64 inode;
    u64 stripe_start;
    u64 stripe_count;
    bool exclusive;
    char token[128];
};

/* === 管理请求 === */
struct powerfs_mgmt_req {
    struct powerfs_req_hdr hdr;
    int op;                    /* STATFS / STATUS / HEALTH */
    struct kstatfs *stats;
};
```

```c
/* powerfs_volume_client.h — 内核态 VolumeClient */

/* 单条传输通道 (一个 TCP 连接) */
struct powerfs_channel {
    int id;
    struct socket *sock;       /* 内核 TCP socket */
    atomic_t inflight;         /* 在途请求数 */
    unsigned long last_active; /* jiffies */
    bool dead;
    spinlock_t lock;
};

/* VolumeClient (v3.0 内核态) */
struct powerfs_volume_client {
    int volume_id;
    char addr[64];
    u16 port;

    /* === 三个入口 (严格优先级) === */
    wait_queue_head_t lease_wq;
    wait_queue_head_t mgmt_wq;
    wait_queue_head_t data_wq;

    spinlock_t lease_lock;
    spinlock_t mgmt_lock;
    spinlock_t data_lock;

    struct list_head lease_queue;
    struct list_head mgmt_queue;
    struct list_head data_queue;

    /* === 通道池 === */
    struct powerfs_channel *channels;
    int num_channels;
    atomic_t next_idx;         /* RoundRobin 索引 */

    /* === 状态 === */
    atomic_t inflight_total;
    atomic_t circuit_state;    /* 0=NORMAL, 1=DEGRADED, 2=OPEN */
    unsigned long circuit_open_until;

    /* === 等待请求表 (rbtree, 便于按 request_id 查找) === */
    struct rb_root pending;
    spinlock_t pending_lock;

    /* === Worker 线程 === */
    struct task_struct *worker;
    struct workqueue_struct *tx_wq;
};
```

### 12.4 内核态调度核心

```c
/* powerfs_volume_client.c — 主调度循环 */

static int powerfs_volume_client_worker(void *data)
{
    struct powerfs_volume_client *vc = data;

    while (!kthread_should_stop()) {
        /* 按优先级等待: lease > mgmt > data */
        int ret = wait_event_interruptible_timeout(
            vc->data_wq,
            !list_empty(&vc->lease_queue) ||
            !list_empty(&vc->mgmt_queue) ||
            !list_empty(&vc->data_queue),
            msecs_to_jiffies(100));

        if (ret == -ERESTARTSYS)
            continue;

        if (signal_pending(current))
            break;

        /* 1. 最高优先级: Lease */
        powerfs_process_lease(vc);

        /* 2. 次高优先级: Mgmt */
        powerfs_process_mgmt(vc);

        /* 3. 最后: DataQueue (读写共享 + 通道池分发) */
        powerfs_process_data(vc);

        /* 4. 检查超时 */
        powerfs_check_timeouts(vc);

        /* 5. 健康检查 */
        powerfs_check_channels(vc);
    }

    return 0;
}

/* 数据请求处理 — 统一入口 + 通道选择 */
static void powerfs_process_data(struct powerfs_volume_client *vc)
{
    unsigned long flags;
    struct powerfs_data_req *req;
    struct powerfs_channel *ch;

    spin_lock_irqsave(&vc->data_lock, flags);
    while (!list_empty(&vc->data_queue)) {
        req = list_first_entry(&vc->data_queue, struct powerfs_data_req, hdr.list);
        list_del_init(&req->hdr.list);
        spin_unlock_irqrestore(&vc->data_lock, flags);

        /* 选择通道: StickyShard + LeastInflight */
        ch = powerfs_select_channel(vc, req);
        if (!ch) {
            req->hdr.state = POWERFS_STATE_FAILED;
            complete(&req->hdr.done);
            continue;
        }

        atomic_inc(&ch->inflight);
        atomic_inc(&vc->inflight_total);
        req->hdr.state = POWERFS_STATE_SENT;
        req->hdr.private = ch;

        /* 提交到发送 workqueue */
        queue_work(vc->tx_wq, &req->work);

        spin_lock_irqsave(&vc->data_lock, flags);
    }
    spin_unlock_irqrestore(&vc->data_lock, flags);
}

/* 通道选择算法 (与用户态 ChannelSelector::StickyShard 对齐) */
static struct powerfs_channel *
powerfs_select_channel(struct powerfs_volume_client *vc,
                        struct powerfs_data_req *req)
{
    struct powerfs_channel *best = NULL, *candidate;
    int best_inflight = INT_MAX;
    int i, idx;

    if (unlikely(vc->circuit_state == 2))  /* CircuitOpen */
        return NULL;

    /* StickyShard: stripe_hash 映射到通道 */
    idx = hash_long(req->stripe_hash, ilog2(vc->num_channels));
    candidate = &vc->channels[idx];
    if (!candidate->dead &&
        atomic_read(&candidate->inflight) < vc->num_channels) {
        return candidate;
    }

    /* 退化: LeastInflight */
    for (i = 0; i < vc->num_channels; i++) {
        candidate = &vc->channels[i];
        if (candidate->dead)
            continue;
        int inf = atomic_read(&candidate->inflight);
        if (inf < best_inflight) {
            best_inflight = inf;
            best = candidate;
        }
    }
    return best;
}
```

### 12.5 内核态注意事项

#### 内存管理
```c
/* 所有请求结构从 kmem_cache 预分配 */
static struct kmem_cache *powerfs_data_req_cache;
static struct kmem_cache *powerfs_lease_req_cache;
static struct kmem_cache *powerfs_mgmt_req_cache;

static int powerfs_init_caches(void)
{
    powerfs_data_req_cache = kmem_cache_create("powerfs_data_req",
        sizeof(struct powerfs_data_req), 0,
        SLAB_HWCACHE_ALIGN | SLAB_PANIC, NULL);
    /* ... 其他 cache ... */
}
```

#### 零拷贝数据传输
```c
/* Write: 用户态 → 内核页 → TCP sendmsg */
static int powerfs_volume_write(struct powerfs_volume_client *vc,
                                u64 file_key, void __user *user_buf,
                                size_t len)
{
    struct page **pages;
    int npages = (len + PAGE_SIZE - 1) / PAGE_SIZE;

    /* 1. 从用户空间取页 */
    pages = kvmalloc_array(npages, sizeof(struct page *), GFP_KERNEL);
    get_user_pages_unlocked(user_buf, npages, FOLL_WRITE, pages);

    /* 2. 直接用 sendmsg 发送 (零拷贝到内核 TCP 栈) */
    /* 注意: powerfs-net 协议需支持 sendmsg 的 MSG_ZEROCOPY */
    powerfs_net_send_pages(vc->channels[0].sock, pages, npages);

    /* 3. 释放 */
    put_user_pages(pages, npages);
    kvfree(pages);
    return 0;
}
```

#### 背压机制
```c
/* 当所有通道 inflight 达到上限时, 调用方被睡眠 */
int powerfs_volume_submit_data(struct powerfs_volume_client *vc,
                               struct powerfs_data_req *req)
{
    /* 等待可用通道 */
    wait_event_interruptible_timeout(
        vc->data_wq,
        !powerfs_all_channels_full(vc),
        msecs_to_jiffies(5000));

    /* 入队 */
    list_add_tail(&req->hdr.list, &vc->data_queue);
    wake_up(&vc->data_wq);
    return 0;
}
```

### 12.6 用户态 ↔ 内核态 接口一致性

为使两套实现行为一致，定义统一的**传输抽象层**：

```
              powerfs_transport_abstract (共享定义)
              ┌─────────────────────────────────┐
              │  - RequestId, RequestKind,      │
              │     MsgType, LeaseToken 定义    │
              │  - TLV 编解码 (wire format)     │
              │  - 状态机转换规则                │
              │  - 错误码映射 (NetError ↔ errno) │
              └─────────────────────────────────┘
                 │                    │
                 ▼                    ▼
        powerfs-net (Rust)      powerfs-net-kernel (C)
        用户态实现                内核态实现
        (tokio TCP)              (kernel sock_create_kern)
```

**关键要求**:
1. wire format 完全一致（可通过抓包验证）
2. 相同的超时、重试、错误码语义
3. 相同的通道选择算法
4. 相同的熔断器状态机

### 12.7 内核态实施要点

| 优先级 | 要点 | 工时 |
|--------|------|------|
| P0 | 定义 `powerfs_req_hdr` 通用头 + kmem_cache 预分配 | 2h |
| P0 | 实现 `powerfs_volume_client` 结构与三个等待队列 | 4h |
| P0 | 实现通道池与 `powerfs_select_channel` 选择算法 | 4h |
| P1 | 实现 `powerfs_volume_client_worker` 主调度循环 | 6h |
| P1 | 实现 Lease/Mgmt 独立处理路径 | 4h |
| P1 | 实现熔断与健康监控 | 4h |
| P2 | 实现零拷贝数据路径 (get_user_pages + sendmsg) | 6h |
| P2 | 与 powerfs_net.c (现有内核 TCP 栈) 集成 | 4h |
| P2 | 与 VFS read/write/page_cache 集成 | 8h |
| P3 | 测试与用户态 wire-format 兼容 | 4h |

---

## 13. 分阶段实施计划

### Phase 1: 基础类型与协议扩展 + 拓扑管理 (4天)

| 任务 | 文件 | 工时 |
|------|------|------|
| RequestId, ClientIdentity | `powerfs-fuse-core/src/request_id.rs` | 4h |
| RequestState, RequestKind | `powerfs-fuse-core/src/request_state.rs` | 4h |
| RequestContext | `powerfs-fuse-core/src/request_context.rs` | 6h |
| 新 TLV 字段和 MsgType | `powerfs-net/src/protocol.rs` | 8h |
| 编解码辅助 (含 ChannelId, StripeHash) | `powerfs-net/src/tlv.rs` | 6h |
| **ClusterTopologyManager 核心结构** | `powerfs-fuse-core/src/topology_manager.rs` | 6h |
| **MasterClient (HA 逻辑)** | `powerfs-fuse-core/src/master_client.rs` | 4h |

### Phase 2: MetaShardClient 实现 (5天)

| 任务 | 文件 | 工时 |
|------|------|------|
| ShardConnection (双通道) | `powerfs-fuse-core/src/meta_shard_client.rs` | 12h |
| ShardRouter (分片路由) | `powerfs-fuse-core/src/shard_router.rs` | 8h |
| CircuitBreaker | `powerfs-fuse-core/src/circuit_breaker.rs` | 8h |
| ~~RequestJournal~~ (已移除) | - | - |
| **MetaShardClient 与 TopologyManager 集成** | `powerfs-fuse-core/src/meta_shard_client.rs` | 6h |
| 通道调度与请求处理 | `powerfs-fuse-core/src/meta_shard_client.rs` | 12h |

### Phase 3: VolumeClient 实现 (v3.0, 4天)

| 任务 | 文件 | 工时 |
|------|------|------|
| VolumeConnection (3 入口 + N 通道池) | `powerfs-fuse-core/src/volume_client.rs` | 12h |
| ChannelPool + ChannelSelector | `powerfs-fuse-core/src/channel_pool.rs` | 8h |
| LeaseManager | `powerfs-fuse-core/src/lease_manager.rs` | 8h |
| VolumeRouter | `powerfs-fuse-core/src/volume_router.rs` | 4h |
| **VolumeClient 与 TopologyManager 集成** | `powerfs-fuse-core/src/volume_client.rs` | 4h |

### Phase 4: 服务端改造 (4天)

| 任务 | 文件 | 工时 |
|------|------|------|
| ShardCommand 添加 CommandMeta | `powerfs-filer/src/raft_group_manager.rs` | 4h |
| RaftIdempotencyIndex | `powerfs-filer/src/idempotency_index.rs` | 8h |
| Filer 请求处理流程改造 | `powerfs-filer/src/net_handler.rs` | 12h |
| Volume IdempotencyLog | `powerfs-volume/src/volume_server.rs` | 8h |
| Volume Lease 验证 | `powerfs-volume/src/lease_handler.rs` | 4h |
| **Master 接口扩展 (下发拓扑, Watch)** | `powerfs-master/src/master_server.rs` | 6h |

### Phase 5: FUSE 层集成 (3天)

| 任务 | 文件 | 工时 |
|------|------|------|
| PowerFsFs 重构 | `powerfs-fuse/src/fuse.rs` | 8h |
| SyncFuseNetClient 移除 | `powerfs-fuse-core/src/net_client.rs` | 4h |
| **FUSE 启动流程集成 (Bootstrap)** | `powerfs-fuse/src/main.rs` | 6h |
| 测试 | 测试文件 | 8h |

### Phase 6: 内核文件系统适配 (v3.0 新增, 5天)

| 任务 | 文件 | 工时 |
|------|------|------|
| 通用请求头 + kmem_cache | `kernel/powerfs_mod/powerfs_transport_client.h` | 4h |
| VolumeClient C 结构 | `kernel/powerfs_mod/powerfs_volume_client.c` | 8h |
| 通道池 + 选择算法 | `kernel/powerfs_mod/powerfs_volume_client.c` | 8h |
| 主调度循环 | `kernel/powerfs_mod/powerfs_volume_client.c` | 6h |
| 与现有 powerfs_net.c 集成 | `kernel/powerfs_mod/powerfs_net.c` | 6h |
| 与 VFS 读写路径集成 | `kernel/powerfs_mod/powerfs_fs.c` | 8h |
| **内核态 ClusterTopologyManager (简化版)** | `kernel/powerfs_mod/powerfs_topology.c` | 4h |
| Wire-format 兼容测试 | `kernel/tests/` | 4h |

### Phase 7: 故障注入验证 (3天)

| 测试 | 描述 | 工时 |
|------|------|------|
| Leader 切换测试 | kill leader, verify no data loss | 8h |
| Volume 重启测试 | kill volume, verify exactly-once | 8h |
| 网络分区测试 | 模拟网络中断 | 4h |
| 客户端崩溃测试 | kill fuse client, verify recovery | 4h |
| 内核模块 reload 测试 | 内核态 mount / umount / 热切换 | 4h |
| 24h 稳定性测试 | soak test | 12h |
