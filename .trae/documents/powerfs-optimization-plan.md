# PowerFS 优化方案规划

> 版本：v1.0
> 创建日期：2026-07-20
> 状态：方案讨论阶段，待评审

## 目录

1. [编译时间戳版本跟踪](#1-编译时间戳版本跟踪)
2. [节点状态细粒度模型](#2-节点状态细粒度模型)
3. [Client 智能重连策略](#3-client-智能重连策略)
4. [集群管理接口](#4-集群管理接口)
5. [卷批量预分配 + 智能调度](#5-卷批量预分配--智能调度)
6. [实施优先级与依赖关系](#6-实施优先级与依赖关系)

---

## 1. 编译时间戳版本跟踪

### 1.1 背景

当前代码仅包含 Cargo 包版本号（如 `0.1.0`），缺少编译时间戳和 Git commit 信息，不利于线上问题排查和版本追踪。需要在二进制启动时输出完整的版本元信息。

### 1.2 现状

- `Cargo.toml` 中仅有 `version = "0.1.0"`
- 二进制启动日志中无编译时间、commit hash、构建机器等信息
- 多个组件（master、volume、monitor、fuse）各自独立，版本难以统一追踪

### 1.3 方案设计

#### 1.3.1 在 `powerfs-common` 中添加 `build_info` 模块

**新建 `powerfs-common/build.rs`**：

```rust
use std::process::Command;

fn main() {
    // Git commit hash
    let commit = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_COMMIT={}", commit);

    // Git branch
    let branch = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_BRANCH={}", branch);

    // Build timestamp (UTC)
    let now = chrono::Utc::now();
    println!("cargo:rustc-env=BUILD_TIME={}", now.to_rfc3339());

    // Build hostname
    let hostname = std::env::var("HOSTNAME")
        .unwrap_or_else(|_| std::env::var("COMPUTERNAME").unwrap_or_default());
    println!("cargo:rustc-env=BUILD_HOST={}", hostname);

    // Rustc version
    let rustc = Command::new("rustc")
        .args(["--version"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=RUSTC_VERSION={}", rustc);

    println!("cargo:rerun-if-changed=../.git/HEAD");
}
```

**新建 `powerfs-common/src/build_info.rs`**：

```rust
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct BuildInfo {
    pub version: &'static str,
    pub git_commit: &'static str,
    pub git_branch: &'static str,
    pub build_time: &'static str,
    pub build_host: &'static str,
    pub rustc_version: &'static str,
    pub crate_name: &'static str,
}

impl BuildInfo {
    pub fn current() -> Self {
        BuildInfo {
            version: env!("CARGO_PKG_VERSION"),
            git_commit: env!("GIT_COMMIT"),
            git_branch: env!("GIT_BRANCH"),
            build_time: env!("BUILD_TIME"),
            build_host: env!("BUILD_HOST"),
            rustc_version: env!("RUSTC_VERSION"),
            crate_name: env!("CARGO_PKG_NAME"),
        }
    }

    pub fn log_startup(&self) {
        log::info!("====== PowerFS Build Info ======");
        log::info!("  Component:    {}", self.crate_name);
        log::info!("  Version:      {}", self.version);
        log::info!("  Git Commit:   {} ({})", self.git_commit, self.git_branch);
        log::info!("  Build Time:   {}", self.build_time);
        log::info!("  Build Host:   {}", self.build_host);
        log::info!("  Rustc:        {}", self.rustc_version);
        log::info!("================================");
    }
}
```

#### 1.3.2 各组件在 `main()` 启动时调用

```rust
// powerfs-master/src/main.rs
use powerfs_common::build_info::BuildInfo;

fn main() {
    // 初始化日志...
    BuildInfo::current().log_startup();
    // 启动服务...
}
```

#### 1.3.3 通过 gRPC 暴露版本信息（可选）

```protobuf
message GetVersionResponse {
    string version = 1;
    string git_commit = 2;
    string git_branch = 3;
    string build_time = 4;
    string build_host = 5;
    string rustc_version = 6;
    string component = 7;
}

service ManagementService {
    rpc GetVersion(GetVersionRequest) returns (GetVersionResponse);
}
```

### 1.4 影响范围

| 文件 | 改动 |
|------|------|
| `powerfs-common/build.rs` | 新建 |
| `powerfs-common/src/build_info.rs` | 新建 |
| `powerfs-common/src/lib.rs` | 添加 `pub mod build_info;` |
| `powerfs-common/Cargo.toml` | 添加 `chrono` build-dependency |
| 各组件 `main.rs` | 启动时调用 `BuildInfo::current().log_startup()` |

### 1.5 风险

- `build.rs` 中调用 `git` 命令在非 git 仓库会失败，已用 `unwrap_or` 兜底
- 编译时间戳使用编译机时区，建议统一 UTC

---

## 2. 节点状态细粒度模型

### 2.1 背景

当前节点状态只有 `Healthy / Degraded / Unavailable` 三态，无法准确描述网络降级、磁盘压力、CPU 压力等多种故障场景。需要更细粒度的状态模型，支撑后续的智能调度和数据迁移。

### 2.2 现状

**[types.rs](file:///home/portion/powerfs/powerfs-common/src/types.rs#L465-L470)** 当前定义：

```rust
pub enum NodeState {
    Healthy,
    Degraded,
    Unavailable,
}
```

**问题**：
- 无法区分网络故障和磁盘故障
- 缺少 `Init`、`Ready`、`SoftError`、`FailSlow` 等中间态
- 无故障类型分类，无法指导调度

### 2.3 方案设计

#### 2.3.1 新状态模型

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    /// 节点刚启动，尚未完成初始化
    Init,
    /// 初始化完成，已注册到 master，等待心跳确认
    Ready,
    /// 完全健康
    Healthy,
    /// 软错误：可恢复，性能略有下降，但仍可服务
    SoftError {
        error_type: SoftErrorType,
        since: u64,  // unix timestamp
    },
    /// 故障慢节点：网络降级或资源压力导致响应慢，但仍可服务（降级模式）
    FailSlow {
        degrade_type: DegradeType,
        severity: u8,  // 1-100，100 最严重
        since: u64,
    },
    /// 降级状态：拒绝写入，只读
    Degraded,
    /// 完全故障，不可服务
    Fault,
    /// 主动下线，运维中
    Maintenance,
    /// 失联，超过心跳阈值
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoftErrorType {
    MemoryPressure,   // 内存压力
    DiskAlmostFull,   // 磁盘接近满
    CpuPressure,      // CPU 压力
    TooManyOpenFiles, // 文件句柄过多
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DegradeType {
    NetworkDegrade,   // 网络降级（高延迟、丢包）
    NetworkError,     // 网络错误（部分连接失败）
    MemoryError,      // 内存不足
    CpuError,         // CPU 过载
    DiskError,        // 磁盘 IO 慢或错误
    LatencySpike,     // 延迟尖峰
}
```

#### 2.3.2 状态转换图

```
                    ┌─────────┐
                    │  Init   │
                    └────┬────┘
                         │ register OK
                         ▼
                    ┌─────────┐
        ┌──────────▶│  Ready  │──────────┐
        │           └────┬────┘          │
        │                │ heartbeat OK  │
        │                ▼               │
        │           ┌─────────┐          │
        │           │ Healthy │◀─────────┘
        │           └────┬────┘
        │                │ soft issue detected
        │                ▼
        │           ┌─────────────┐
        │           │  SoftError  │
        │           └─────┬───────┘
        │                 │ recovered / worsened
        │                 ▼
        │           ┌─────────────┐
        │           │  FailSlow   │
        │           └─────┬───────┘
        │                 │ worsened
        │                 ▼
        │           ┌─────────────┐
        │           │  Degraded   │ (read-only)
        │           └─────┬───────┘
        │                 │ worsened
        │                 ▼
        │           ┌─────────────┐
        │           │   Fault     │
        │           └─────┬───────┘
        │                 │ heartbeat timeout
        │                 ▼
        │           ┌─────────────┐
        │           │ Unavailable │
        │           └─────┬───────┘
        │                 │ heartbeat recovered
        └─────────────────┘
                          │
                          │ manual operation
                          ▼
                    ┌──────────────┐
                    │ Maintenance  │
                    └──────────────┘
```

#### 2.3.3 调度策略矩阵

| 节点状态 | 新建卷 | 写入 | 读取 | 迁移源 | 迁移目标 |
|---------|--------|------|------|--------|---------|
| Init | ❌ | ❌ | ❌ | ❌ | ❌ |
| Ready | ✅ | ✅ | ✅ | ❌ | ✅ |
| Healthy | ✅ | ✅ | ✅ | ❌ | ✅ |
| SoftError | ✅ (降权) | ✅ | ✅ | ❌ | ⚠️ (降权) |
| FailSlow | ⚠️ (降权) | ⚠️ | ✅ | ❌ | ❌ |
| Degraded | ❌ | ❌ | ✅ | ✅ | ❌ |
| Fault | ❌ | ❌ | ❌ | ✅ | ❌ |
| Unavailable | ❌ | ❌ | ❌ | ✅ | ❌ |
| Maintenance | ❌ | ❌ | ❌ | ✅ | ❌ |

#### 2.3.4 心跳上报字段扩展

```protobuf
message HeartbeatRequest {
    string node_id = 1;
    // ... existing fields
    
    // 新增：节点自评估状态
    NodeState self_reported_state = 100;
    SoftErrorType soft_error_type = 101;
    DegradeType degrade_type = 102;
    uint32 degrade_severity = 103;
    
    // 新增：详细资源指标
    ResourceMetrics metrics = 110;
}

message ResourceMetrics {
    double cpu_usage = 1;            // 0.0 - 1.0
    double memory_usage = 2;
    double disk_usage = 3;
    double disk_io_latency_ms = 4;   // P99
    double network_latency_ms = 5;   // 到 master 的延迟
    uint32 active_connections = 6;
    uint32 open_file_handles = 7;
    double load_avg_1m = 8;
}
```

### 2.4 影响范围

| 文件 | 改动 |
|------|------|
| `powerfs-common/src/types.rs` | 扩展 `NodeState` 枚举 |
| `powerfs-master/proto/master.proto` | 扩展 `HeartbeatRequest` |
| `powerfs-master/src/topology.rs` | 节点状态机转换逻辑 |
| `powerfs-master/src/master.rs` | 心跳处理、状态判断 |
| `powerfs-volume/src/heartbeat.rs` | 上报资源指标 |
| `powerfs-master/src/volume_assigner.rs` | 根据状态过滤可用节点 |
| `powerfs-fuse-core/src/client.rs` | 根据状态选择 master |

### 2.5 风险

- 状态转换逻辑复杂，需要充分测试
- 历史状态数据需要持久化（避免重启后状态丢失）
- 兼容旧客户端的心跳协议

---

## 3. Client 智能重连策略

### 3.1 背景

当 master 集群 3 个节点都健康，但某个 client 连不上 leader 时，当前实现会不断轮询重连，可能造成故障放大。需要更智能的重连策略，区分瞬时故障和持续故障。

### 3.2 现状

**[client.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/client.rs#L122-L148)** 的 `try_connect_to_master`：

- 简单轮询所有 master 地址
- 遇到 "not leader" 错误会尝试切换到 leader
- 无退避机制，可能高频重试
- 不区分节点状态，对所有节点一视同仁

### 3.3 方案设计

#### 3.3.1 连接管理器结构

```rust
pub struct MasterConnectionManager {
    master_addresses: Vec<String>,
    /// 每个节点的当前状态（从集群广播获取）
    node_states: Arc<RwLock<HashMap<String, NodeHealthState>>>,
    /// 每个节点的退避状态
    backoff_state: Arc<RwLock<HashMap<String, BackoffState>>>,
    /// 当前 leader 缓存
    current_leader: Arc<RwLock<Option<String>>>,
    /// 配置
    config: ConnectionConfig,
}

#[derive(Debug, Clone)]
pub struct NodeHealthState {
    pub state: NodeState,
    pub last_heartbeat: Instant,
    pub consecutive_failures: u32,
    pub last_failure_time: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct BackoffState {
    pub current_delay: Duration,
    pub next_retry_at: Instant,
    pub failure_count: u32,
}

pub struct ConnectionConfig {
    pub initial_backoff: Duration,    // 默认 100ms
    pub max_backoff: Duration,        // 默认 30s
    pub backoff_multiplier: f64,      // 默认 2.0
    pub jitter_factor: f64,           // 默认 0.1
    pub circuit_breaker_threshold: u32, // 默认 10 次失败后熔断
    pub circuit_breaker_duration: Duration, // 默认 60s
    pub health_check_interval: Duration,   // 默认 5s
}
```

#### 3.3.2 智能选路策略

```rust
impl MasterConnectionManager {
    /// 选择最佳连接目标
    async fn select_best_master(&self) -> Result<String, ConnectionError> {
        let states = self.node_states.read().unwrap();
        let backoffs = self.backoff_state.read().unwrap();
        let now = Instant::now();

        // 1. 优先尝试当前 leader
        if let Some(leader) = self.current_leader.read().unwrap().as_ref() {
            if self.is_node_available(leader, &states, &backoffs, now) {
                return Ok(leader.clone());
            }
        }

        // 2. 按"健康度"排序所有节点
        let mut candidates: Vec<_> = self.master_addresses
            .iter()
            .filter(|addr| self.is_node_available(addr, &states, &backoffs, now))
            .map(|addr| {
                let health_score = self.calculate_health_score(addr, &states);
                let backoff_score = self.calculate_backoff_score(addr, &backoffs, now);
                (addr, health_score + backoff_score)
            })
            .collect();

        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        candidates.first()
            .map(|(addr, _)| addr.to_string())
            .ok_or(ConnectionError::NoAvailableMaster)
    }

    fn is_node_available(
        &self,
        addr: &str,
        states: &HashMap<String, NodeHealthState>,
        backoffs: &HashMap<String, BackoffState>,
        now: Instant,
    ) -> bool {
        // 跳过熔断中的节点
        if let Some(backoff) = backoffs.get(addr) {
            if now < backoff.next_retry_at {
                return false;
            }
        }
        // 跳过明确不可用的节点
        if let Some(state) = states.get(addr) {
            match state.state {
                NodeState::Fault | NodeState::Unavailable | NodeState::Maintenance => return false,
                _ => {}
            }
        }
        true
    }

    /// 记录连接成功
    pub async fn record_success(&self, addr: &str) {
        let mut backoffs = self.backoff_state.write().unwrap();
        if let Some(state) = backoffs.get_mut(addr) {
            state.current_delay = self.config.initial_backoff;
            state.failure_count = 0;
            state.next_retry_at = Instant::now();
        }
    }

    /// 记录连接失败
    pub async fn record_failure(&self, addr: &str, error: &str) {
        let mut backoffs = self.backoff_state.write().unwrap();
        let state = backoffs.entry(addr.to_string()).or_insert_with(|| BackoffState {
            current_delay: self.config.initial_backoff,
            next_retry_at: Instant::now(),
            failure_count: 0,
        });

        state.failure_count += 1;
        
        // 指数退避 + 抖动
        let jitter = rand::random::<f64>() * self.config.jitter_factor;
        let delay_secs = state.current_delay.as_secs_f64() 
            * self.config.backoff_multiplier 
            * (1.0 + jitter);
        state.current_delay = Duration::from_secs_f64(
            delay_secs.min(self.config.max_backoff.as_secs_f64())
        );
        state.next_retry_at = Instant::now() + state.current_delay;

        // 熔断判定
        if state.failure_count >= self.config.circuit_breaker_threshold {
            state.next_retry_at = Instant::now() + self.config.circuit_breaker_duration;
            log::warn!(
                "Master {} circuit breaker opened after {} failures",
                addr, state.failure_count
            );
        }

        // 更新节点状态
        let mut states = self.node_states.write().unwrap();
        let health = states.entry(addr.to_string()).or_insert_with(|| NodeHealthState {
            state: NodeState::Healthy,
            last_heartbeat: Instant::now(),
            consecutive_failures: 0,
            last_failure_time: None,
        });
        health.consecutive_failures += 1;
        health.last_failure_time = Some(Instant::now());

        // 连续失败达到阈值，标记为 FailSlow / Unavailable
        if health.consecutive_failures >= self.config.circuit_breaker_threshold {
            health.state = NodeState::Unavailable;
        } else if health.consecutive_failures >= 3 {
            health.state = NodeState::FailSlow {
                degrade_type: DegradeType::NetworkError,
                severity: 50,
                since: 0,
            };
        }
    }
}
```

#### 3.3.3 错误分类重试

```rust
fn should_retry(error: &tonic::Status) -> RetryPolicy {
    match error.code() {
        // 瞬时错误：立即重试
        tonic::Code::Unavailable 
        | tonic::Code::DeadlineExceeded 
        | tonic::Code::Aborted => RetryPolicy::RetryWithBackoff,
        
        // 语义错误：不重试
        tonic::Code::NotFound 
        | tonic::Code::AlreadyExists 
        | tonic::Code::InvalidArgument 
        | tonic::Code::FailedPrecondition => RetryPolicy::NoRetry,
        
        // Leader 相关：切换 leader 后重试
        tonic::Code::FailedPrecondition if error.message().contains("not leader") 
            => RetryPolicy::SwitchLeader,
        
        // 其他内部错误：有限重试
        _ => RetryPolicy::RetryWithBackoff,
    }
}
```

#### 3.3.4 网络降级探测

```rust
/// 主动探测 master 节点的网络质量
async fn probe_master_health(&self, addr: &str) -> NetworkQuality {
    let start = Instant::now();
    
    // 发送轻量级 ping
    match tokio::time::timeout(
        Duration::from_millis(500),
        self.ping_master(addr)
    ).await {
        Ok(Ok(_)) => {
            let latency = start.elapsed();
            if latency < Duration::from_millis(50) {
                NetworkQuality::Excellent
            } else if latency < Duration::from_millis(200) {
                NetworkQuality::Good
            } else if latency < Duration::from_millis(500) {
                NetworkQuality::Degraded
            } else {
                NetworkQuality::Poor
            }
        }
        Ok(Err(_)) => NetworkQuality::Error,
        Err(_) => NetworkQuality::Timeout,
    }
}
```

### 3.4 影响范围

| 文件 | 改动 |
|------|------|
| `powerfs-fuse-core/src/connection_manager.rs` | 新建：连接管理器 |
| `powerfs-fuse-core/src/client.rs` | 重构 `try_connect_to_master`，使用连接管理器 |
| `powerfs-fuse-core/src/lib.rs` | 导出 `MasterConnectionManager` |

### 3.5 风险

- 退避策略可能导致请求延迟增大，需要合理配置
- 熔断期间请求会失败，上层需要捕获并降级
- 状态同步存在延迟，可能决策不准

---

## 4. 集群管理接口

### 4.1 背景

当前缺少统一的管理接口查询节点状态、卷状态、执行运维操作。需要扩展管理 gRPC 接口，支撑 Monitor UI 展示和运维操作。

### 4.2 现状

- 仅有 `/cluster/status` 等 HTTP 接口（基础）
- 缺少节点状态查询、卷迁移、节点下线等管理操作
- Monitor UI 数据来源分散

### 4.3 方案设计

#### 4.3.1 管理 gRPC 服务定义

```protobuf
service ClusterManagement {
    // === 节点管理 ===
    rpc ListNodes(ListNodesRequest) returns (ListNodesResponse);
    rpc GetNodeState(GetNodeStateRequest) returns (NodeStateResponse);
    rpc SetNodeMaintenance(SetNodeMaintenanceRequest) returns (SetNodeMaintenanceResponse);
    rpc DrainNode(DrainNodeRequest) returns (DrainNodeResponse);
    
    // === 卷管理 ===
    rpc ListVolumes(ListVolumesRequest) returns (ListVolumesResponse);
    rpc GetVolumeState(GetVolumeStateRequest) returns (VolumeStateResponse);
    rpc MigrateVolume(MigrateVolumeRequest) returns (MigrateVolumeResponse);
    rpc BalanceVolumes(BalanceVolumesRequest) returns (BalanceVolumesResponse);
    rpc GetVolumeAssignment(GetVolumeAssignmentRequest) returns (GetVolumeAssignmentResponse);
    
    // === 集群管理 ===
    rpc GetClusterStatus(GetClusterStatusRequest) returns (ClusterStatusResponse);
    rpc GetClusterMetrics(GetClusterMetricsRequest) returns (ClusterMetricsResponse);
    rpc GetVersion(GetVersionRequest) returns (GetVersionResponse);
    
    // === 健康检查 ===
    rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
    rpc GetNodeMetrics(GetNodeMetricsRequest) returns (NodeMetricsResponse);
}

message NodeStateResponse {
    string node_id = 1;
    NodeState state = 2;
    SoftErrorType soft_error_type = 3;
    DegradeType degrade_type = 4;
    uint32 degrade_severity = 5;
    ResourceMetrics metrics = 6;
    uint64 last_heartbeat_unix = 7;
    uint32 consecutive_failures = 8;
    uint32 active_volumes = 9;
    uint64 total_space = 10;
    uint64 used_space = 11;
}

message MigrateVolumeRequest {
    uint32 volume_id = 1;
    string source_node_id = 2;
    string target_node_id = 3;
    bool force = 4;  // 强制迁移（即使源节点健康）
}

message MigrateVolumeResponse {
    bool success = 1;
    string migration_id = 2;
    string error = 3;
    uint64 estimated_duration_secs = 4;
}
```

#### 4.3.2 RESTful HTTP 网关

为方便运维工具和 UI 调用，在 master 上提供 HTTP 网关转发到 gRPC：

| HTTP 端点 | gRPC 方法 | 用途 |
|-----------|-----------|------|
| `GET /api/v1/nodes` | ListNodes | 列出所有节点 |
| `GET /api/v1/nodes/{id}` | GetNodeState | 查询单个节点状态 |
| `POST /api/v1/nodes/{id}/maintenance` | SetNodeMaintenance | 进入维护模式 |
| `POST /api/v1/nodes/{id}/drain` | DrainNode | 排空节点 |
| `GET /api/v1/volumes` | ListVolumes | 列出所有卷 |
| `POST /api/v1/volumes/{id}/migrate` | MigrateVolume | 迁移卷 |
| `POST /api/v1/cluster/balance` | BalanceVolumes | 平衡卷 |
| `GET /api/v1/cluster/status` | GetClusterStatus | 集群总览 |
| `GET /api/v1/version` | GetVersion | 版本信息 |

#### 4.3.3 运维操作示例

**节点下线流程**：
1. 调用 `SetNodeMaintenance(node_id, true)` 标记维护模式
2. 调用 `DrainNode(node_id)` 触发卷迁移
3. 等待所有卷迁移完成
4. 节点安全下线

**卷迁移流程**：
1. 调用 `MigrateVolume(volume_id, source, target)`
2. master 创建迁移任务，返回 `migration_id`
3. 客户端轮询迁移状态
4. 迁移完成后，更新 volume layout

### 4.4 影响范围

| 文件 | 改动 |
|------|------|
| `powerfs-master/proto/master.proto` | 新增 `ClusterManagement` 服务 |
| `powerfs-master/src/management_service.rs` | 新建：实现管理服务 |
| `powerfs-master/src/http_gateway.rs` | 新建：HTTP 网关 |
| `powerfs-master/src/master.rs` | 注册管理服务 |
| `powerfs-monitor/` | 调用新接口展示数据 |

### 4.5 风险

- 管理操作可能影响在线业务，需要权限校验
- 卷迁移期间数据一致性保障
- HTTP 网关需要鉴权机制

---

## 5. 卷批量预分配 + 智能调度

### 5.1 背景

当前 `volume_grow` RPC 实现存在严重效率问题：每次请求都可能创建多个卷然后丢弃，最多重试 `count * 10` 次。`RoundRobinAssigner` 和 `ConsistentHashAssigner` 实际算法相同，且无法指定目标节点。

### 5.2 现状

**[server.rs:581-638](file:///home/portion/powerfs/powerfs-master/src/server.rs#L581-L638)** 的 `volume_grow`：

```rust
while new_volume_ids.len() < req.count as usize && created_count < max_retries {
    match self.master.create_new_volume(&req.replication, &req.collection).await {
        Ok((fid, nodes)) => {
            created_count += 1;
            // 如果主节点不匹配，就丢弃这个卷！
            if !req.data_node.is_empty() && primary_node.id.0 != req.data_node {
                continue;  // ← 问题：卷已创建但被丢弃
            }
            new_volume_ids.push(fid.volume_id.0);
        }
    }
}
```

**问题**：
- `max_retries = req.count * 10`，最多创建 10 倍于需要的卷
- 不匹配的卷被创建后直接丢弃，造成资源浪费
- 每个请求触发多次 Raft 提交
- 不考虑节点负载、容量、状态

**[volume_assigner.rs](file:///home/portion/powerfs/powerfs-master/src/volume_assigner.rs)**：
- `ConsistentHashAssigner` 与 `RoundRobinAssigner` 实现完全相同（伪一致性哈希）
- 不支持指定目标节点

### 5.3 方案设计

采用**综合方案**：批量预分配池 + 智能调度器

#### 5.3.1 总体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Volume Allocation Manager                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────────┐    ┌──────────────────┐                       │
│  │  VolumePool      │    │  VolumeAssigner  │                       │
│  │  (预分配池)      │    │  (智能调度器)    │                       │
│  │                  │    │                  │                       │
│  │  Pool A (rep=3)  │    │  - 节点状态感知  │                       │
│  │  Pool B (rep=2)  │    │  - 容量感知      │                       │
│  │  Pool C (node=X) │    │  - 故障域隔离    │                       │
│  │                  │    │  - 负载均衡      │                       │
│  └────────┬─────────┘    └────────┬─────────┘                       │
│           │                       │                                 │
│           └───────────┬───────────┘                                 │
│                       ▼                                             │
│           ┌───────────────────────┐                                 │
│           │   Pool Replenisher    │  (后台异步补充)                  │
│           └───────────────────────┘                                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

#### 5.3.2 预分配池设计

```rust
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PoolKey {
    pub collection: String,
    pub replica_count: u8,
    pub disk_type: DiskType,
    pub preferred_node: Option<NodeId>,  // 指定节点池
}

pub struct VolumePool {
    pools: RwLock<HashMap<PoolKey, Vec<PooledVolume>>>,
    config: PoolConfig,
    /// 已使用但未确认的卷（防止重复分配）
    reserved: RwLock<HashMap<VolumeId, Instant>>,
}

pub struct PooledVolume {
    pub volume_id: VolumeId,
    pub nodes: Vec<DataNodeInfo>,
    pub collection: String,
    pub replica_count: u8,
    pub disk_type: DiskType,
    pub created_at: Instant,
    pub state: PooledVolumeState,
    pub file_key: u64,  // 下一个可用的 file key
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PooledVolumeState {
    Available,   // 池中可用
    Reserved,    // 已分配，等待确认
    Used,        // 已使用
    Expired,     // 过期，待清理
}

pub struct PoolConfig {
    pub min_pool_size_per_key: usize,   // 每个池最小容量，默认 10
    pub max_pool_size_per_key: usize,   // 每个池最大容量，默认 100
    pub replenish_threshold: f64,       // 补充阈值，默认 0.3（30%）
    pub volume_ttl: Duration,           // 池中卷的过期时间，默认 1 小时
    pub cleanup_interval: Duration,     // 清理间隔，默认 5 分钟
    pub replenish_batch_size: usize,    // 单次补充数量，默认 20
}
```

#### 5.3.3 智能调度器设计

```rust
pub struct SmartVolumeAssigner {
    master: Arc<MasterNode>,
    rack_awareness_enabled: bool,
    data_center_awareness_enabled: bool,
}

impl VolumeAssigner for SmartVolumeAssigner {
    fn assign(
        &self,
        volume_id: u32,
        nodes: &[DataNodeInfo],
        replica_count: usize,
    ) -> Vec<DataNodeInfo> {
        self.assign_internal(volume_id, nodes, replica_count, None)
    }
}

impl SmartVolumeAssigner {
    /// 智能分配：综合考虑节点状态、容量、负载、故障域
    fn assign_internal(
        &self,
        volume_id: u32,
        nodes: &[DataNodeInfo],
        replica_count: usize,
        preferred_node: Option<&NodeId>,
    ) -> Vec<DataNodeInfo> {
        // 1. 过滤不可用节点（状态、维护模式等）
        let candidates: Vec<_> = nodes.iter()
            .filter(|n| self.is_node_assignable(n))
            .collect();

        if candidates.len() < replica_count {
            return Vec::new();
        }

        // 2. 按综合评分排序
        let mut scored: Vec<_> = candidates.iter()
            .map(|n| (n, self.calculate_score(n)))
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        // 3. 故障域隔离：避免副本分配到同一 rack/DC
        let mut selected = Vec::with_capacity(replica_count);
        let mut used_racks = HashSet::new();
        let mut used_dcs = HashSet::new();

        // 3.1 优先选择 preferred_node
        if let Some(pref) = preferred_node {
            if let Some((node, _)) = scored.iter().find(|(n, _)| n.id == *pref) {
                selected.push((*node).clone());
                used_racks.insert(node.rack_id.0.clone());
                used_dcs.insert(node.data_center_id.0.clone());
            }
        }

        // 3.2 按评分选择其他副本，跳过同 rack/DC
        for (node, _) in &scored {
            if selected.len() >= replica_count {
                break;
            }
            if selected.iter().any(|n| n.id == node.id) {
                continue;
            }
            // rack 感知：同 rack 已有副本，跳过（除非没有其他选择）
            if self.rack_awareness_enabled 
                && used_racks.contains(&node.rack_id.0) 
                && scored.len() - selected.len() > replica_count - selected.len() {
                continue;
            }
            used_racks.insert(node.rack_id.0.clone());
            used_dcs.insert(node.data_center_id.0.clone());
            selected.push((*node).clone());
        }

        // 3.3 如果故障域约束太严格，降级为不考虑故障域
        if selected.len() < replica_count {
            for (node, _) in &scored {
                if selected.len() >= replica_count {
                    break;
                }
                if selected.iter().any(|n| n.id == node.id) {
                    continue;
                }
                selected.push((*node).clone());
            }
        }

        selected
    }

    /// 计算节点评分（0.0 - 1.0，越高越好）
    fn calculate_score(&self, node: &DataNodeInfo) -> f64 {
        let mut score = 1.0;

        // 1. 状态评分
        match node.state {
            NodeState::Healthy => score *= 1.0,
            NodeState::Ready => score *= 0.9,
            NodeState::SoftError { .. } => score *= 0.6,
            NodeState::FailSlow { severity, .. } => score *= 1.0 - (severity as f64 / 100.0) * 0.5,
            _ => return 0.0,
        }

        // 2. 容量评分（剩余容量比例）
        if node.total_space > 0 {
            let free_ratio = 1.0 - (node.used_space as f64 / node.total_space as f64);
            score *= 0.5 + 0.5 * free_ratio;  // 容量占比 50% 权重
        }

        // 3. 负载评分（卷数量越少分越高）
        let volume_load = if node.volume_count > 0 {
            1.0 / (1.0 + node.volume_count as f64 * 0.01)
        } else {
            1.0
        };
        score *= 0.7 + 0.3 * volume_load;

        // 4. 维护模式直接拒绝
        if node.maintenance_mode {
            return 0.0;
        }

        score
    }

    fn is_node_assignable(&self, node: &DataNodeInfo) -> bool {
        match node.state {
            NodeState::Init | NodeState::Degraded | NodeState::Fault 
            | NodeState::Unavailable | NodeState::Maintenance => false,
            _ => !node.maintenance_mode,
        }
    }
}
```

#### 5.3.4 `volume_grow` 重构

```rust
async fn volume_grow(
    &self,
    request: Request<VolumeGrowRequest>,
) -> Result<Response<VolumeGrowResponse>, Status> {
    if !self.master.is_leader().await {
        return Err(Status::failed_precondition("not leader"));
    }

    let req = request.into_inner();
    let preferred_node = if req.data_node.is_empty() {
        None
    } else {
        Some(NodeId(req.data_node.clone()))
    };

    // 1. 尝试从预分配池获取
    let pool_key = PoolKey {
        collection: req.collection.clone(),
        replica_count: parse_replica_count(&req.replication),
        disk_type: DiskType::default(),
        preferred_node: preferred_node.clone(),
    };

    let mut new_volume_ids = Vec::with_capacity(req.count as usize);
    let mut locations = Vec::new();

    // 从池中批量获取
    if let Ok(pooled) = self.master.volume_pool.try_acquire(&pool_key, req.count as usize) {
        for vol in pooled {
            new_volume_ids.push(vol.volume_id.0);
            for node in &vol.nodes {
                locations.push(Location {
                    url: node.url(),
                    public_url: node.public_url.clone(),
                    grpc_port: node.grpc_port,
                    data_center: node.data_center_id.to_string(),
                });
            }
        }
    }

    // 2. 池中不足，直接创建（智能分配器指定 preferred_node）
    while (new_volume_ids.len() as u32) < req.count {
        match self.master.create_new_volume_with_preference(
            &req.replication,
            &req.collection,
            preferred_node.as_ref(),
        ).await {
            Ok((fid, nodes)) => {
                new_volume_ids.push(fid.volume_id.0);
                for node in &nodes {
                    locations.push(Location {
                        url: node.url(),
                        public_url: node.public_url.clone(),
                        grpc_port: node.grpc_port,
                        data_center: node.data_center_id.to_string(),
                    });
                }
            }
            Err(e) => {
                return Ok(Response::new(VolumeGrowResponse {
                    new_volume_ids,
                    locations,
                    error: e.to_string(),
                }));
            }
        }
    }

    // 3. 触发池补充（异步）
    self.master.volume_pool.trigger_replenish(&pool_key);

    Ok(Response::new(VolumeGrowResponse {
        new_volume_ids,
        locations,
        error: String::new(),
    }))
}
```

#### 5.3.5 后台池补充逻辑

```rust
impl VolumePool {
    /// 后台补充任务
    pub async fn replenish_loop(self: Arc<Self>, master: Arc<MasterNode>) {
        let mut cleanup_tick = tokio::time::interval(self.config.cleanup_interval);
        let mut replenish_tick = tokio::time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                _ = cleanup_tick.tick() => {
                    self.cleanup_expired().await;
                }
                _ = replenish_tick.tick() => {
                    self.replenish_if_needed(&master).await;
                }
            }
        }
    }

    async fn replenish_if_needed(&self, master: &MasterNode) {
        let pools = self.pools.read().unwrap();
        for (key, volumes) in pools.iter() {
            let available = volumes.iter()
                .filter(|v| v.state == PooledVolumeState::Available)
                .count();
            
            let threshold = (self.config.min_pool_size_per_key as f64 
                * self.config.replenish_threshold) as usize;
            
            if available < threshold {
                let to_create = self.config.replenish_batch_size
                    .min(self.config.max_pool_size_per_key - volumes.len());
                
                drop(pools);  // 释放锁
                
                for _ in 0..to_create {
                    if let Ok((fid, nodes)) = master.create_new_volume_with_preference(
                        &replica_to_string(key.replica_count),
                        &key.collection,
                        key.preferred_node.as_ref(),
                    ).await {
                        self.add_to_pool(key.clone(), fid.volume_id, nodes);
                    }
                }
                
                log::info!(
                    "Replenished pool {:?} with {} volumes",
                    key, to_create
                );
            }
        }
    }

    async fn cleanup_expired(&self) {
        let mut pools = self.pools.write().unwrap();
        let now = Instant::now();
        
        for volumes in pools.values_mut() {
            volumes.retain(|v| {
                if v.state == PooledVolumeState::Available 
                    && now.duration_since(v.created_at) > self.config.volume_ttl {
                    log::debug!("Expiring pooled volume {}", v.volume_id.0);
                    false
                } else {
                    true
                }
            });
        }
    }
}
```

### 5.4 性能对比

| 指标 | 当前实现 | 新方案 |
|------|---------|--------|
| 单次 volume_grow 延迟 | 100ms - 数秒（多次 Raft 提交） | <10ms（池中直接获取） |
| 创建卷数量 | 请求量 × 最多 10 倍 | 请求量 + 后台预分配 |
| 节点选择准确性 | 随机（取模） | 智能评分 |
| 故障域隔离 | 无 | rack/DC 感知 |
| 节点状态感知 | 无 | 11 种状态感知 |

### 5.5 影响范围

| 文件 | 改动 |
|------|------|
| `powerfs-master/src/volume_pool.rs` | 新建：预分配池 |
| `powerfs-master/src/volume_assigner.rs` | 新增 `SmartVolumeAssigner` |
| `powerfs-master/src/master.rs` | 新增 `create_new_volume_with_preference`，集成 pool |
| `powerfs-master/src/server.rs` | 重构 `volume_grow` |
| `powerfs-master/proto/master.proto` | 扩展 `VolumeGrowRequest`（可选字段） |

### 5.6 风险

- 预分配的卷占用存储空间，需要合理设置池大小
- 池补充与实时请求可能竞争 Raft 提交
- 故障域约束在小集群下可能无法满足，需要降级策略
- 池中卷过期清理需要确认无引用

---

## 6. 实施优先级与依赖关系

### 6.1 优先级排序

| 优先级 | 模块 | 原因 |
|--------|------|------|
| P0 | 编译时间戳 | 改动最小，价值最高，便于后续版本追踪 |
| P0 | volume_grow 重构（智能调度） | 直接解决当前性能问题 |
| P1 | 节点状态细粒度 | 智能调度的基础 |
| P1 | Client 智能重连 | 避免故障放大 |
| P2 | 卷批量预分配池 | 在智能调度基础上进一步优化 |
| P2 | 集群管理接口 | 在状态模型稳定后实施 |

### 6.2 依赖关系

```
┌────────────────┐     ┌────────────────────┐
│ 1. 编译时间戳  │     │ 2. 节点状态细粒度  │
│ (无依赖)       │     │ (无依赖)           │
└────────────────┘     └─────────┬──────────┘
                                 │
                                 ▼
                       ┌────────────────────┐
                       │ 4. 集群管理接口    │
                       │ (依赖 2)           │
                       └────────────────────┘
                                 │
                                 ▼
                       ┌────────────────────┐
┌────────────────┐     │ 5. 智能调度器      │
│ 3. Client 重连 │     │ (依赖 2)           │
│ (依赖 2)       │     └─────────┬──────────┘
└────────────────┘               │
                                 ▼
                       ┌────────────────────┐
                       │ 5. 批量预分配池    │
                       │ (依赖 5 智能调度)  │
                       └────────────────────┘
```

### 6.3 建议实施顺序

1. **第一阶段**（立即可做）：
   - 编译时间戳（独立模块，1 天）
   - 节点状态细粒度模型定义（独立，1 天）

2. **第二阶段**（依赖第一阶段）：
   - 智能调度器（依赖状态模型，2-3 天）
   - volume_grow 重构（依赖智能调度器，1 天）
   - Client 智能重连（依赖状态模型，2-3 天）

3. **第三阶段**（依赖第二阶段）：
   - 批量预分配池（依赖智能调度器，3-4 天）
   - 集群管理接口（依赖状态模型，2-3 天）

### 6.4 测试计划

每个模块需要配套测试：

1. **单元测试**：状态转换、评分算法、退避策略
2. **集成测试**：模拟节点故障、网络分区、卷迁移
3. **基准测试**：volume_grow 延迟对比、连接恢复时间
4. **混沌测试**：随机杀死节点、注入网络延迟

---

## 附录：相关文件索引

- [client.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/client.rs) - FUSE 客户端 gRPC 封装
- [types.rs](file:///home/portion/powerfs/powerfs-common/src/types.rs) - 公共类型定义
- [volume_assigner.rs](file:///home/portion/powerfs/powerfs-master/src/volume_assigner.rs) - 卷分配器
- [volume_router.rs](file:///home/portion/powerfs/powerfs-master/src/volume_router.rs) - 卷路由
- [master.rs](file:///home/portion/powerfs/powerfs-master/src/master.rs) - master 核心逻辑
- [server.rs](file:///home/portion/powerfs/powerfs-master/src/server.rs) - gRPC 服务实现
- [master.proto](file:///home/portion/powerfs/powerfs-master/proto/master