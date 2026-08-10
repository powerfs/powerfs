# 资源分配策略松耦合方案

> 状态：方案规划，待定是否实施
> 日期：2026-08-10
> 目标：将元数据和数据分配策略从服务中解耦为独立 crate，提供统一接口规范，支持策略扩展和管理接口，为后续 AI Agent 接入负载均衡奠定基础。

## 1. 背景与动机

### 1.1 当前问题

资源分配逻辑分散在 4 个服务 crate 中，耦合程度不一：

| 分配场景 | 所在位置 | 接口形态 | 耦合问题 |
|---------|---------|---------|---------|
| Volume→DataNode | `powerfs-master/src/volume_assigner.rs` | `VolumeAssigner` trait + 3 个实现 | 依赖 `powerfs_common::DataNodeInfo`，但已是 trait 化，耦合较低 |
| Inode→Shard 路由 | `powerfs-filer/src/shard_strategy.rs` | `ShardStrategy::calculate_shard(inode)` | 硬耦合 filer 的 `ShardId` 和 `raft_group_manager` |
| File→Volume/Needle | `powerfs-filer/src/net_handler.rs` | `alloc_for_new_file()` / `alloc_for_stripe_file()` | 直接访问 `self.zones`、`zone_client::alloc_needle_id`，最深耦合 |
| Offset→Volume 定位 | `powerfs-layout/src/placement.rs` | `Placement::locate(offset)` | **已独立**，`powerfs-layout` 无服务依赖 |
| 可靠性状态机 | `powerfs-layout/src/reliability.rs` | `Reliability` + `ReliabilityState` | **已独立** |

### 1.2 现有策略清单

**Master 侧**（`volume_assigner.rs`）：
- `RoundRobinAssigner` — 按 `volume_id % nodes.len()` 轮询
- `ConsistentHashAssigner` — 当前实现与 RoundRobin 相同（占位）
- `SmartVolumeAssigner` — 节点状态过滤 + 容量/负载评分 + rack/DC 故障域隔离
- `AssignContext` — rack_awareness、DC_awareness、preferred_node

**Filer 侧**（`shard_strategy.rs` + `net_handler.rs`）：
- `ShardStrategy::calculate_shard` — `inode / inode_per_shard % shard_count`
- `alloc_for_new_file` — 遍历 zones 取第一个可用 volume
- `alloc_for_stripe_file` — 跨所有 zone 收集 volume，按 node 分组，round-robin 跨节点选 volume（anti-affinity by node）
- `alloc_inode_batch` — shard 内原子计数器分配 inode 范围

**Layout 侧**（`powerfs-layout`，已独立）：
- `Placement` 四态：Inline / Flat / Stripe / WideStripe
- `Placement::locate(offset)` — 计算 (volume_idx, volume_offset)
- `Reliability` 状态机：SingleReplica → PendingReplicated → Replicated → EC

### 1.3 设计目标

1. **策略可插拔**：分配策略独立为 crate，服务通过 trait 调用，支持运行时切换
2. **状态分离**：分配器无状态，每次调用接收集群快照，不持有服务内部状态
3. **管理接口**：状态查询（只读）+ 管理接口（写操作），供监控和 AI Agent 接入
4. **负载感知**：分配决策考虑节点/volume 负载，优先分配空闲资源
5. **迁移支持**：数据迁移（不含元数据迁移），负载自适应限速
6. **资源伸缩**：支持 Shard 和 Volume 的动态增减，加 Shard 无需元数据迁移，减 Volume 通过 drain + 数据迁移完成

## 2. 架构设计

### 2.1 六模块划分

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ 1.静态配置    │────→│ 2.动态状态    │────→│ 3.动态请求    │
│ (启动加载)    │     │ (heartbeat   │     │ (客户端发起)  │
│              │     │  聚合快照)    │     │              │
└──────────────┘     └──────┬───────┘     └──────┬───────┘
                            │                    │
                            ▼                    ▼
                     ┌──────────────┐     ┌──────────────┐
                     │ 5.状态查询    │     │ 4.动态分配    │
                     │ (只读视图)    │     │   输出        │
                     │              │     │ (分配器决策)  │
                     └──────┬───────┘     └──────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │ 6.管理接口    │
                     │ (写操作/迁移) │
                     └──────────────┘
```

### 2.2 新 Crate：`powerfs-allocator`

依赖关系：
```
powerfs-allocator
  ├── powerfs-common   (基础类型)
  └── powerfs-layout   (Placement/Reliability 类型)

powerfs-master  ──→ powerfs-allocator  (VolumeAssigner)
powerfs-filer   ──→ powerfs-allocator  (VolumePlacement/ShardRouting/InodeBatch)
powerfs-monitor ──→ powerfs-allocator  (StatusQuery)
```

## 3. 接口规范

### 3.1 模块 1：静态配置状态

启动时加载，变更需走管理接口。

```rust
/// 集群静态配置 — 由配置文件 + Master 注册信息合并
#[derive(Clone, Debug)]
pub struct ClusterStaticConfig {
    pub zones: Vec<ZoneConfig>,
    pub shard_count: usize,
    pub inode_per_shard: u64,
    pub default_replication: ReplicationConfig,
    pub placement: PlacementPolicyConfig,
    pub rebalance: RebalancePolicy,
    pub migration: MigrationPolicy,
    pub volume_default_size: u64,
}

pub struct ZoneConfig {
    pub zone_id: u32,
    pub name: String,
    pub node_ids: Vec<String>,
}

pub struct PlacementPolicyConfig {
    pub strategy: String,             // "round_robin" | "anti_affinity" | "least_loaded"
    pub rack_aware: bool,
    pub cross_zone_replication: bool,
}

/// 迁移限速 + 负载自适应策略
pub struct MigrationPolicy {
    pub max_concurrent_migrations: u32,
    pub max_bandwidth_mbps: u64,
    /// 集群平均 load_score 超过此值时静默暂停迁移
    pub load_pause_threshold: f64,    // 默认 0.7
    /// 负载下降到此值后自动恢复迁移
    pub load_resume_threshold: f64,   // 默认 0.4
    /// 迁移扫描最小间隔（秒）
    pub scan_interval_secs: u64,      // 默认 60
}

pub struct RebalancePolicy {
    pub volume_full_threshold: f64,   // 0.85 — 超过触发迁移
    pub near_full_exclude_ratio: f64, // 0.90 — 超过则不分配（即使空闲）
    pub load_imbalance_threshold: f64,// 2.0
    pub cold_data_threshold_hours: u64, // 24
    pub min_migration_chunk_count: u32, // 10
}
```

**关键决策**：`near_full_exclude_ratio` 表示 volume 使用率超过此值后，即使该 volume 看起来"空闲"（load_score 低），分配器也不向其分配新数据。已满的返回 `ENOSPACE`。

### 3.2 模块 2：动态状态

heartbeat 聚合快照，非实时，10-30s 更新。

```rust
/// 集群运行时快照 — Master 聚合所有 heartbeat 生成
#[derive(Clone, Debug)]
pub struct ClusterSnapshot {
    pub version: u64,                 // 单调递增，用于缓存失效
    pub timestamp: std::time::Instant,
    pub config_version: u64,          // 关联的静态配置版本

    pub volumes: Vec<VolumeRuntime>,
    pub nodes: Vec<NodeRuntime>,
    pub shards: Vec<ShardRuntime>,
    pub cluster_avg_load: f64,        // 集群平均负载（供迁移限速判断）
}

pub struct VolumeRuntime {
    pub volume_id: u64,
    pub node_id: String,
    pub zone_id: u32,
    pub total_size: u64,
    pub used_size: u64,
    pub state: VolumeState,
    pub load: VolumeLoad,
    /// 冷数据 needle 数量（由 volume server 统计上报）
    pub cold_needle_count: u64,
    /// 热数据 needle 数量
    pub hot_needle_count: u64,
}

pub struct VolumeLoad {
    pub iops: u64,
    pub bandwidth_mbps: u64,
    pub write_latency_p99_us: u64,
    pub active_connections: u32,
}

pub struct NodeRuntime {
    pub node_id: String,
    pub state: NodeState,             // Healthy | Degraded | Maintenance | Down
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub disk_usage: f32,
    pub load_score: f64,              // 0.0-1.0 综合评分
    pub in_maintenance: bool,         // 维护模式：排除分配
}

pub struct ShardRuntime {
    pub shard_id: u64,
    pub leader_node: String,
    pub follower_nodes: Vec<String>,
    pub qps: u64,
    pub raft_backlog: u32,
    pub open_inode_count: u64,
    pub active_lease_count: u64,      // 活跃 lease 数（迁移避让判断）
}
```

**设计要点**：
- 快照由 Master 维护，Filer 调用分配器时传入 `&ClusterSnapshot` 引用
- 快照带 `version`，分配器可缓存评分结果，版本不变则复用
- 接受 10-30s 滞后，不做实时查询，适合大规模集群
- 已满的 volume 在快照中标记 `state = Full`，分配器直接返回 `ENOSPACE`

### 3.3 模块 3 + 4：动态请求与分配输出

请求和输出配对，分配器是无状态函数：`(snapshot, request) → decision`。

```rust
// ============ 请求 ============

pub enum AllocationRequest {
    SingleFile(SingleFileReq),
    StripeFile(StripeFileReq),
    InodeBatch(InodeBatchReq),
    VolumeAssign(VolumeAssignReq),
}

pub struct SingleFileReq {
    pub collection: String,
    pub replication: String,
    pub file_size_hint: Option<u64>,
}

pub struct StripeFileReq {
    pub collection: String,
    pub stripe_count: u32,
    pub stripe_size: u64,
}

pub struct InodeBatchReq {
    pub count: u32,
    pub client_id: String,
}

// ============ 输出 ============

pub enum AllocationDecision {
    SingleFile(SingleFileDecision),
    StripeFile(StripeFileDecision),
    InodeBatch(InodeBatchDecision),
    VolumeAssign(VolumeAssignDecision),
}

pub struct SingleFileDecision {
    pub volume_id: u64,
    pub zone_id: u32,
    pub node_id: String,
    pub suggested_needle_id: u64,     // 建议值，服务侧原子确认
    pub score: f64,                   // 评分（调试/监控用）
    pub alternatives: Vec<u64>,       // 备选 volume_id
}

pub struct StripeFileDecision {
    pub placements: Vec<SingleFileDecision>,
    pub used_zones: Vec<u32>,         // 验证 anti-affinity
}

pub struct InodeBatchDecision {
    pub start_inode: u64,
    pub end_inode: u64,
    pub shard_id: u64,
}

// ============ 分配 trait ============

/// 无状态分配器：输入快照+请求，输出决策
pub trait Allocator: Send + Sync {
    fn allocate(
        &self,
        snapshot: &ClusterSnapshot,
        request: &AllocationRequest,
    ) -> Result<AllocationDecision, AllocError>;
}

pub enum AllocError {
    NoSpace,                          // 所有 volume 都满了 → ENOSPACE
    NoHealthyNode,                    // 没有健康节点
    AntiAffinityFailed,               // 无法满足跨 zone 要求
    StrategyError(String),
}
```

**评分算法**（核心策略，可插拔替换）：

```rust
/// 默认评分函数：空间 + 负载复合评分
fn score_volume(vol: &VolumeRuntime, node: &NodeRuntime, config: &RebalancePolicy) -> Option<f64> {
    let usage_ratio = vol.used_size as f64 / vol.total_size as f64;
    let space_ratio = 1.0 - usage_ratio;

    // 硬排除：使用率超过 near_full_exclude_ratio → 不分配（即使空闲）
    if usage_ratio >= config.near_full_exclude_ratio {
        return None;
    }

    // 软惩罚：使用率超过 volume_full_threshold → 评分降权
    let space_score = if usage_ratio > config.volume_full_threshold {
        space_ratio * 0.3
    } else {
        space_ratio
    };

    let load_penalty = node.load_score;  // 0=空闲, 1=饱和
    Some(space_score * 0.6 + (1.0 - load_penalty) * 0.4)
}
```

### 3.4 模块 5：状态查询（只读）

供 Monitor 面板、管理界面、AI Agent 读取。

```rust
pub trait StatusQuery: Send + Sync {
    /// 集群整体概览
    fn cluster_overview(&self) -> ClusterOverview;

    /// 节点负载分布（判断忙闲不均）
    fn node_load_distribution(&self) -> Vec<NodeLoadReport>;

    /// Volume 使用详情
    fn volume_details(&self) -> Vec<VolumeDetail>;

    /// 分配统计（策略效果评估）
    fn allocation_stats(&self) -> AllocationStats;

    /// 迁移任务状态
    fn migration_tasks(&self) -> Vec<MigrationTaskStatus>;

    /// 当前生效配置
    fn current_config(&self) -> ClusterStaticConfig;
}

pub struct ClusterOverview {
    pub total_capacity: u64,
    pub used_capacity: u64,
    pub free_capacity: u64,
    pub node_count: u32,
    pub healthy_nodes: u32,
    pub volume_count: u32,
    pub avg_load: f64,
    pub max_load: f64,
    pub min_load: f64,
    pub imbalance_ratio: f64,         // max/min，>2.0 说明不均
}

pub struct AllocationStats {
    pub total_requests: u64,
    pub successful: u64,
    pub failed: u64,
    pub no_space_count: u64,
    pub avg_decision_latency_us: u64,
}

pub struct MigrationTaskStatus {
    pub task_id: String,
    pub action_type: MigrationType,   // ColdData | HotData | VolumeGrow
    pub state: MigrationState,        // Pending | Running | PausedByLoad | Completed | Failed
    pub progress: f32,
    pub bytes_migrated: u64,
    pub bytes_total: u64,
    pub pause_reason: Option<String>, // "cluster load 0.82 > threshold 0.7"
}
```

### 3.5 模块 6：管理接口（写操作）

供管理员、AI Agent 下发，支持 dry-run。

```rust
pub trait ManagementApi: Send + Sync {

    // ===== 配置管理 =====

    /// 切换分配策略
    fn set_placement_strategy(&self, strategy: &str) -> Result<()>;

    /// 更新迁移策略
    fn update_migration_policy(&self, policy: MigrationPolicy) -> Result<()>;

    /// 更新均衡策略
    fn update_rebalance_policy(&self, policy: RebalancePolicy) -> Result<()>;

    /// 设置节点维护模式（排除分配 + 排干数据）
    fn set_node_maintenance(&self, node_id: &str, enabled: bool) -> Result<()>;

    // ===== 迁移控制 =====

    /// 手动触发均衡检查（dry_run=true 只返回建议不执行）
    fn trigger_rebalance_check(&self, dry_run: bool) -> Result<Vec<RebalanceAction>>;

    /// 执行迁移决策（dry_run=true 只校验不执行）
    fn execute_migrations(
        &self,
        actions: Vec<RebalanceAction>,
        dry_run: bool,
    ) -> Result<MigrationExecutionResult>;

    /// 暂停所有迁移
    fn pause_all_migrations(&self) -> Result<()>;

    /// 恢复迁移
    fn resume_migrations(&self) -> Result<()>;

    /// 取消特定迁移任务
    fn cancel_migration(&self, task_id: &str) -> Result<()>;

    // ===== 覆盖操作（运维场景） =====

    /// 手动 pin volume 到指定 node
    fn pin_volume_to_node(&self, volume_id: u64, node_id: &str) -> Result<()>;
    fn unpin_volume(&self, volume_id: u64) -> Result<()>;
}

pub struct MigrationExecutionResult {
    pub accepted_task_ids: Vec<String>,
    pub rejected: Vec<MigrationRejection>,
}

pub struct MigrationRejection {
    pub action: RebalanceAction,
    pub reason: RejectionReason,      // HasActiveLease | VolumeFull | NodeInMaintenance
}

pub enum RebalanceAction {
    /// 迁移冷数据（volume 快满 → 迁冷数据到空闲 volume）
    MigrateColdData {
        from_volume: u64,
        to_volume: u64,
        needle_ids: Vec<u64>,
    },
    /// 迁移热数据（忙节点 → 空闲节点）
    MigrateHotData {
        from_node: String,
        to_node: String,
        volume_ids: Vec<u64>,
    },
    /// 请求 Master 创建新 volume
    RequestVolumeGrow {
        zone_id: u32,
        size: u64,
    },
}
```

**dry-run 设计**：
- `trigger_rebalance_check(dry_run=true)` — 返回系统建议的 `RebalanceAction` 列表，不执行
- `execute_migrations(actions, dry_run=true)` — 校验每个 action 是否可执行（目标 volume 有空间、needle 无活跃 lease 等），返回哪些会被接受、哪些会被拒绝，但不实际执行
- 管理员/AI Agent 可先用 dry-run 预览，确认后再用 `dry_run=false` 执行

## 4. 四个约束的落地

### 4.1 约束 1：heartbeat 非实时，支持大规模

```
Volume Server ──heartbeat(10s)──→ Master
Filer         ──heartbeat(15s)──→ Master
                                  │
                    Master 聚合 → ClusterSnapshot
                                  │
                    Filer 分配时引用快照（零拷贝）
```

- 快照 `version` 单调递增，分配器缓存评分结果，version 不变则复用
- 大规模集群（100+ 节点）：heartbeat 用增量上报（只报变化的 volume）
- 已满的 volume 在快照中 `state = Full`，分配器直接返回 `ENOSPACE`
- 接近满的 volume（使用率 > `near_full_exclude_ratio`）被评分函数硬排除，不分配

### 4.2 约束 2：迁移避让正在使用的数据

```rust
/// 迁移前检查（由 Filer/Volume Server 执行）
fn can_migrate_needle(volume_id: u64, needle_id: u64) -> bool {
    // 1. 检查 needle 对应 inode 是否有活跃 lease
    let inode = lookup_inode_by_needle(volume_id, needle_id);
    if has_active_lease(inode) {
        return false;  // 有客户端正在写
    }
    // 2. 检查 open_count
    if get_open_count(inode) > 0 {
        return false;  // 有客户端打开着
    }
    // 3. 检查 lease grace period
    if within_lease_grace_period(inode) {
        return false;
    }
    true
}
```

LoadBalancer 生成 `MigrateColdData` 时，`needle_ids` 只包含 `can_migrate == true` 的 needle。无法迁移的标记为 `skipped`，下轮再试。

**迁移范围**：仅数据迁移（needle 从 volume A 复制到 volume B，更新 inode 的 chunks 列表）。不涉及元数据迁移（shard 合并/分裂等暂不支持）。

### 4.3 约束 3：冷热数据管理在 Filer/Volume Server 侧

```
Volume Server 维护:
  - needle_access_count: HashMap<needle_id, AccessStats>
  - 定期标记: access_count < threshold → cold
  - heartbeat 上报: cold_needle_count, hot_needle_count
  - 查询接口: get_cold_needles(limit) → Vec<needle_id>

Filer 维护:
  - inode_atime: 最后访问时间
  - inode_open_count: 当前打开数
  - 查询接口: get_open_count(inode) → u32

LoadBalancer 只调用:
  - volume.get_cold_needles() → Vec<needle_id>   (volume server 实现)
  - filer.get_open_count(inode) → u32             (filer 实现)
```

分配器/LoadBalancer 不自己判断冷热，只消费服务侧的标记结果。

### 4.4 约束 4：限速 + 负载自适应静默停止

```rust
/// 迁移调度器 — 周期运行，负载感知
impl MigrationScheduler {
    fn tick(&mut self, snapshot: &ClusterSnapshot) {
        // 1. 负载过高 → 静默暂停所有 Running 任务
        if snapshot.cluster_avg_load > self.policy.load_pause_threshold {
            for task in &mut self.active_tasks {
                if task.state == MigrationState::Running {
                    task.state = MigrationState::PausedByLoad;
                    task.pause_reason = format!(
                        "cluster load {:.2} > threshold {:.2}",
                        snapshot.cluster_avg_load, self.policy.load_pause_threshold
                    );
                    // 静默：不报错，不告警，只记录状态
                }
            }
            return;  // 不启动新任务
        }

        // 2. 负载恢复 → 自动恢复 PausedByLoad 任务
        if snapshot.cluster_avg_load < self.policy.load_resume_threshold {
            for task in &mut self.active_tasks {
                if task.state == MigrationState::PausedByLoad {
                    task.state = MigrationState::Running;
                    task.pause_reason = None;
                }
            }
        }

        // 3. 限速检查：活跃任务数未达上限才启动新任务
        let running = self.active_tasks.iter()
            .filter(|t| t.state == MigrationState::Running)
            .count();
        if running >= self.policy.max_concurrent_migrations as usize {
            return;
        }

        // 4. 启动新任务
        let new_actions = self.compute_rebalance(snapshot);
        for action in new_actions {
            self.start_task(action);
        }
    }
}
```

**滞回设计**：暂停阈值 0.7，恢复阈值 0.4，避免在阈值附近频繁切换。

## 5. 分步实施计划

### 5.1 可行性评估

| 步骤 | 内容 | 可行性 | 理由 |
|------|------|--------|------|
| P1 | 创建 `powerfs-allocator` crate，定义 trait + 类型 | 高 | 纯新增，不影响现有代码 |
| P2 | 迁移 `ShardStrategy` 为 `ShardRoutingStrategy`，引入 ShardMap 替换取模 | 高 | 逻辑简单，启动时生成等价映射表，后续支持 range 分裂 |
| P3 | 迁移 master `VolumeAssigner` 到 allocator crate | 中 | 已 trait 化，需移动 `DataNodeInfo` 依赖 |
| P4 | 提取 filer `alloc_for_*` 决策逻辑到 allocator | 中 | 需拆分"决策"和"执行"，但逻辑清晰 |
| P5 | 扩展 heartbeat 携带负载指标 | 中 | 需改 master + volume + filer 三端 |
| P6 | 实现 `AllocatorStatus` 接口 + monitor 接入 | 高 | 只读，不碰分配路径 |
| P7 | 实现 `LoadBalancer` + `MigrationScheduler`（含 Volume drain 迁移） | 低 | 需服务侧支持迁移执行，工作量最大 |
| P8 | 实现 Shard 伸缩（add_shard range 分裂） | 中 | 依赖 P2 ShardMap，加 shard 无需迁移 |
| P9 | 实现 Volume 伸缩管理接口（create/drain/remove） | 中 | 依赖 P7 迁移能力 |

### 5.2 推荐分步实施

**阶段一（低风险，纯整理）**：
- P1：创建 crate + 定义 trait
- P2：迁移 ShardStrategy + 引入 ShardMap（启动时生成等价映射表，行为不变）
- P3：迁移 VolumeAssigner
- P6：实现 StatusQuery（基于现有 heartbeat 数据）

**阶段二（中风险，策略外移 + 伸缩基础）**：
- P4：提取 filer alloc 决策逻辑
- P5：扩展 heartbeat 负载指标
- P8：实现 Shard add（range 分裂，加 shard 无需迁移）

**阶段三（高风险，迁移能力 + 完整伸缩）**：
- P7：实现 LoadBalancer + 迁移调度
- P9：实现 Volume drain + 迁移 + 移除
- 需要先在 volume server 实现冷热数据标记
- 需要在 filer 实现 needle→inode 反查 + lease 检查

### 5.3 现有策略模块化整理（可立即执行）

不创建新 crate，仅整理现有代码，便于策略调整：

1. **Master 侧**（`volume_assigner.rs`）：
   - 已有 trait + 3 实现，结构良好
   - 建议：将 `AssignContext` 扩展为包含负载信息的 `AssignContextV2`
   - 建议：`SmartVolumeAssigner` 的评分函数提取为可配置

2. **Filer 侧**（`net_handler.rs`）：
   - `alloc_for_new_file` / `alloc_for_stripe_file` 内联在 `NetHandler` 中
   - 建议：提取为独立的 `FilerAllocator` struct，接收 `&zones` 作为参数
   - 建议：评分逻辑提取为独立函数，便于后续替换

3. **Filer 侧**（`shard_strategy.rs`）：
   - 已是独立 struct，但硬依赖 `crate::raft_group_manager::ShardId`
   - 建议：定义 `powerfs-common::ShardId`，消除 filer 依赖
   - 建议：引入 ShardMap 替换取模（启动时按 `shard_count` 生成等价映射表，行为与取模一致，后续支持 range 分裂实现加 shard）

4. **Layout 侧**（`powerfs-layout`）：
   - 已完全独立，无需改动

## 6. AI Agent 接入路径

```
AI Agent
  │
  ├─ 读: StatusQuery::cluster_overview()       → 整体健康
  ├─ 读: StatusQuery::node_load_distribution() → 找忙闲不均
  ├─ 读: StatusQuery::migration_tasks()        → 迁移进度
  │
  ├─ 分析（外部模型）
  │   → 发现 node_3 持续高负载，node_1 空闲
  │   → 调整阈值: load_imbalance 2.0 → 1.5
  │
  ├─ 写: ManagementApi::update_rebalance_policy()  → 更敏感触发
  ├─ 写: ManagementApi::trigger_rebalance_check(dry_run=true) → 预览
  ├─ 写: ManagementApi::execute_migrations(actions, dry_run=true) → 校验
  │
  └─ 确认后: ManagementApi::execute_migrations(actions, dry_run=false) → 执行
```

AI Agent 不接触分配路径（模块 3/4），只通过状态查询（模块 5）读取、管理接口（模块 6）写入。保证分配性能不受 AI 影响。

## 7. 资源伸缩（扩展性）

扩展性与分配紧密相关：新增资源需被分配器感知，下线资源需安全排干。分数据层和元数据层讨论。

### 7.1 统一资源状态机

所有可分配资源（Shard / Volume / Node）遵循统一生命周期：

```
Creating → Active → Draining → Removed
              ↑         │
              └─────────┘
            (恢复，仅 Volume/Node)
```

| 状态 | 分配器行为 | LoadBalancer 行为 |
|------|-----------|------------------|
| Creating | 排除（未就绪） | 不涉及 |
| Active | 可分配 | 可作为迁移目标 |
| Draining | 排除（正在下线） | 数据迁出源 |
| Removed | 不在快照中 | 不涉及 |

### 7.2 Volume 伸缩（数据层）— 已由 snapshot + 状态机覆盖

**加 Volume**：无需特殊处理。
```
Volume Server 创建 volume → 注册 Master → 出现在 ClusterSnapshot
→ 评分函数自动选中（新 volume 通常 used_size=0，评分最高）
```

**减 Volume**：
```
1. Master 标记 volume.state = Draining
2. 评分函数排除 Draining volume（score_volume 返回 None）
3. LoadBalancer 生成 MigrateColdData { from: draining_vol, to: active_vol }
4. 所有 needle 迁移完成后，volume.state = Empty
5. Master 标记 Deleted，Volume Server 删除文件
```

**空间不足自动扩容**：
```
分配器检测所有 Active volume 使用率 > 80%
→ 生成 RebalanceAction::RequestVolumeGrow { zone_id, size }
→ Master 在负载最低的 node 上创建新 volume
→ 新 volume 出现在下一次 snapshot
```

**结论**：Volume 伸缩无需额外接口，现有设计已覆盖。

### 7.3 Shard 伸缩（元数据层）— 需要从取模改为映射表

#### 问题

当前路由算法：
```rust
let shard_key = inode / self.inode_per_shard;
ShardId(shard_key % shard_count)
```

这是取模运算。改 `shard_count`（如 3→4）会导致 **75% 的 inode 路由全变**，这些 inode 的元数据已经在原 shard 的 Raft 组中，无法移动。

#### 方案：Range 映射表替代取模

将 `calculate_shard(inode)` 从纯函数改为**查表**：

```rust
/// Range-based shard mapping（替代 modulo-based calculate_shard）
pub struct ShardMap {
    /// 按 range_start 排序，二分查找
    /// 每个 inode range 映射到唯一 shard
    entries: RwLock<Vec<ShardMapEntry>>,
}

struct ShardMapEntry {
    range_start: u64,           // inclusive
    range_end: u64,             // exclusive
    shard_id: ShardId,
    state: ShardState,          // Active | Draining
}

impl ShardMap {
    fn route(&self, inode: u64) -> ShardId {
        let entries = self.entries.read().unwrap();
        // 二分查找：找到 range_start <= inode < range_end 的 entry
        let idx = entries.partition_point(|e| e.range_start <= inode) - 1;
        entries[idx].shard_id
    }
}
```

#### 加 Shard（无需元数据迁移）

```
当前: shard_count=3, inode空间 [0, u64::MAX) 分成 3 段

加 shard_4:
1. 选择一个 Active range（如 shard_1 的 [0, 1M)）
2. 在该 range 内选一个 split_point（如 500K）
   - shard_1 保留 [0, 500K)，已有 inode 不动
   - shard_4 获得 [500K, 1M)，只接收新分配的 inode
3. alloc_inode_batch 从 shard_4 的 range 分配
4. 更新 ShardMap，广播给所有 Filer

结果:
   shard_1: [0, 500K)      ← 旧 inode 留在这
   shard_4: [500K, 1M)     ← 新 inode 去这
   shard_2: [1M, 2M)       ← 不变
   shard_3: [2M, u64::MAX) ← 不变
```

**关键**：已有 inode 的路由完全不变，只有新分配的 inode 可能去新 shard。无需迁移任何元数据。

#### 减 Shard（需元数据迁移，当前 defer）

```
1. Master 标记 shard_2.state = Draining
2. ShardMap 路由时 Draining shard 仍能查到（已有 inode 还在那）
3. alloc_inode_batch 不再从 Draining shard 的 range 分配
4. 后续（元数据迁移就绪后）：
   - 将 shard_2 的 inode 逐个迁移到其他 Active shard
   - 迁移完成后删除 ShardMap 中 shard_2 的 entry
5. 当前阶段：减 shard 暂不支持，只标记 Draining 阻止新分配
```

#### 更新后的 ShardRoutingStrategy trait

```rust
pub trait ShardRoutingStrategy: Send + Sync {
    /// 路由 inode 到 shard（查表，非计算）
    fn route(&self, inode: u64) -> ShardId;

    /// 获取所有 Active shard（分配器可用的）
    fn active_shards(&self) -> Vec<ShardId>;

    /// 获取所有 Draining shard（正在下线的）
    fn draining_shards(&self) -> Vec<ShardId>;

    /// 加 shard：在指定 range 内分裂，新 range 给新 shard
    /// 只影响后续 inode 分配，已有 inode 路由不变
    fn add_shard(
        &mut self,
        new_shard_id: ShardId,
        split_from: ShardId,
        split_point: u64,
    ) -> Result<(), ShardError>;

    /// 标记 shard 为 Draining（停止新分配，等待元数据迁移）
    fn drain_shard(&mut self, shard_id: ShardId) -> Result<(), ShardError>;

    /// 移除已排干的 shard（所有 inode 必须已迁移走）
    fn remove_shard(&mut self, shard_id: ShardId) -> Result<(), ShardError>;

    /// 获取 shard 的 inode range
    fn shard_range(&self, shard_id: ShardId) -> Option<(u64, u64)>;
}

pub enum ShardError {
    ShardNotFound(ShardId),
    ShardNotDraining(ShardId),     // remove_shard 时 shard 未排干
    ShardAlreadyExists(ShardId),
    InvalidSplitPoint(u64),
    NoActiveShardToSplit,          // 没有 Active shard 可分裂
}
```

#### 兼容现有 split-create

现有 split-create 流程不变：
- inode 记录存在 `calculate_shard(inode)` 对应的 shard
- dir_entry 存在 `calculate_shard(parent_inode)` 对应的 shard

唯一变化：`calculate_shard` 从取模变为查表。对调用方透明。

### 7.4 伸缩操作接入管理接口

扩展 `ManagementApi`：

```rust
pub trait ManagementApi: Send + Sync {
    // ... 已有接口 ...

    // ===== Shard 伸缩 =====

    /// 加 shard（dry_run=true 只返回分裂建议不执行）
    fn add_shard(
        &self,
        split_from: Option<ShardId>,  // None = 自动选最忙的 shard 分裂
        dry_run: bool,
    ) -> Result<ShardSplitPlan, ManageError>;

    /// 标记 shard 为 Draining
    fn drain_shard(&self, shard_id: ShardId) -> Result<(), ManageError>;

    /// 移除已排干的 shard
    fn remove_shard(&self, shard_id: ShardId) -> Result<(), ManageError>;

    // ===== Volume 伸缩 =====

    /// 手动创建新 volume（通常由 RequestVolumeGrow 自动触发）
    fn create_volume(
        &self,
        zone_id: u32,
        node_id: Option<String>,    // None = 自动选负载最低的 node
        size: u64,
    ) -> Result<u64, ManageError>;  // 返回 volume_id

    /// 标记 volume 为 Draining（触发数据迁移）
    fn drain_volume(&self, volume_id: u64) -> Result<(), ManageError>;

    /// 移除已排干的 volume
    fn remove_volume(&self, volume_id: u64) -> Result<(), ManageError>;
}

/// Shard 分裂计划（dry_run 返回）
pub struct ShardSplitPlan {
    pub split_from: ShardId,
    pub split_point: u64,
    pub new_shard_id: ShardId,
    pub new_range: (u64, u64),
    pub affected_future_allocations: u64,  // 预估影响的新 inode 数
}
```

### 7.5 自动伸缩触发条件

```
自动加 Shard:
  - 条件: 所有 Active shard 的 raft_backlog > 阈值（元数据压力大）
  - 或: 最忙 shard QPS / 最闲 shard QPS > imbalance_threshold
  - 动作: 分裂最忙 shard

自动加 Volume:
  - 条件: 所有 Active volume 使用率 > volume_full_threshold
  - 或: 集群剩余空间 < 10%
  - 动作: 在负载最低的 node 创建新 volume

自动减 Volume:
  - 条件: volume 使用率 < 5% 且持续 > 1 小时
  - 或: 管理员手动 drain
  - 动作: 标记 Draining → 迁移数据 → 移除

自动减 Shard:
  - 仅手动触发（元数据迁移成本高，不自动）
```

### 7.6 实施优先级

| 操作 | 可行性 | 何时可做 |
|------|--------|---------|
| 加 Volume（自动） | 高 | 已支持，snapshot 自动感知 |
| 减 Volume（drain + 迁移） | 中 | 依赖 LoadBalancer P7 |
| 加 Shard（range 分裂） | 中 | 依赖 ShardMap 替换取模（P2 阶段） |
| 减 Shard（drain + 元数据迁移） | 低 | 依赖元数据迁移能力，当前 defer |

**近期可行**：加 Volume（已支持）+ 加 Shard（改 ShardMap）。减操作等迁移能力就绪。

## 8. 待定问题

1. **快照一致性**：已确认不需要回退机制。已满返回 ENOSPACE，接近满（> near_full_exclude_ratio）不分配。
2. **元数据迁移**：已确认暂不支持，仅做数据迁移。
3. **管理接口鉴权**：已确认需要 dry-run 模式。完整鉴权方案待定（可能复用 RBAC）。
4. **needle_id 所有权**：分配器给 `suggested_needle_id`，服务侧原子 CAS 确认。并发冲突时用 `alternatives` 重试。
5. **策略热更新**：是否支持运行时切换策略（不重启），还是需要重启服务。待定。
6. **Shard 减容**：当前 defer，依赖元数据迁移能力。加 shard 可通过 ShardMap range 分裂实现，无需迁移。
7. **ShardMap 初始迁移**：从取模改为映射表需要一次性切换。可启动时根据 `shard_count` 生成初始映射表，行为与取模一致，后续分裂时才产生差异。

## 9. 实施进展

### 9.1 Allocator crate（P1-P9，已完成）

| 阶段 | 内容 | 状态 | commit |
|------|------|------|--------|
| P1 | 创建 `powerfs-allocator` crate，6 模块 trait 定义 | ✅ | 39a589ff |
| P2 | ShardMap range 映射替代取模，filer ShardStrategy 迁移 | ✅ | e0a384b4 |
| P3 | master VolumeAssigner 迁移到 allocator crate | ✅ | bbbca17a |
| P4 | filer `alloc_for_*` 决策逻辑提取为 FilerAllocator | ✅ | 9d7663dc |
| P5 | heartbeat 扩展 CPU/内存负载指标 | ✅ | a3884f41 |
| P6 | SnapshotStatusQuery + master snapshot builder | ✅ | 5e195285 |
| P7 | LoadBalancer + MigrationScheduler | ✅ | d5924e7d |
| P8 | Shard 伸缩（range 分裂） | ✅ | c80bed0f |
| P9 | VolumeManager + VolumeControl 伸缩管理 | ✅ | a3635c4f |

### 9.2 Master 集成（进行中）

**已完成：RebalanceEngine 接入 master 后台 tick loop**

- `powerfs-master/src/allocator_integration.rs`：
  - `LoggingExecutor`：实现 `MigrationExecutor`，记录每个迁移动作并立即完成（无数据移动）。
  - `RebalanceEngine`：持有 `LoadBalancer` + `MigrationScheduler`，提供 `run_tick(master)` 和 `tasks()`。
  - `spawn_rebalance_loop`：周期后台任务，leader-only，每 `scan_interval_secs`（默认 60s）执行一次 tick。
- `MigrationScheduler::complete_task`：补充执行器完成回调（success/failure → Completed/Failed），释放 source slot。
- `MasterNode`：新增 `rebalance_engine` 字段、`migration_tasks()` 访问器，`start()` 中构造引擎并启动 loop。
- 测试：allocator 77 个（+4 complete_task），master 96 个（+2 integration），clippy 0 警告。

**设计决策**：
- 周期 tick loop（非 heartbeat 回调）：`build_cluster_snapshot` 是聚合视图，适合周期性消费，不适合在每次 heartbeat 触发（高频）。
- `LoggingExecutor` 而非真实迁移：真实数据迁移需要 volume server 冷数据追踪 + filer needle→inode 反查（计划 §5.2 阶段三前提），属于高风险大改动，留作后续里程碑。
- sync→async 边界：allocator 全 sync，master 是 tokio async。`run_tick` 为 sync，由 `spawn_blocking` 调用；`is_leader().await` 在 async loop 中检查。

**已完成：VolumeState::Draining 变体 + MasterVolumeControl**

- `powerfs_common::types::VolumeState`：新增 `Draining` 变体（Creating/Available/Full/ReadOnly/**Draining**/Deleting）。
- master.rs 3 处穷举 match 更新：Raft string↔VolumeState 双向映射、`map_volume_state`（Draining→VolumeRuntimeState::Draining）。
- provider_impl.rs Display 映射、volume/server.rs 状态映射 + `read_only` 字段（Draining 也视为只读）。
- `MasterVolumeControl`：实现 `VolumeControl` trait，通过 `block_in_place`+`Handle::block_on` 桥接 sync→async，调用 master 的 `create_new_volume_with_preference`/`update_volume_state(Draining)`/`delete_volume`。
- `map_powerfs_error`：PowerFsError→ManageError 映射（NotLeader→InvalidState, VolumeNotFound→ResourceNotFound 等）+ 3 个单元测试。
- 测试：allocator 77、common 68（+Draining 变体）、master 99（+3 错误映射+1 map_volume_state），clippy 0 警告。

**已完成：MasterManagementApi 实现 ManagementApi trait**

- `MasterManagementApi`：实现完整 `ManagementApi` trait（15 个方法）。
  - **可用方法**（9 个）：`update_migration_policy`、`update_rebalance_policy`（策略更新，通过共享 Arc<RwLock>）；`trigger_rebalance_check`（dry_run + 执行）、`execute_migrations`、`pause_all_migrations`、`resume_migrations`、`cancel_migration`（迁移控制，委托 MigrationScheduler）；`create_volume`、`drain_volume`、`remove_volume`（卷伸缩，通过 VolumeManager + MasterVolumeControl）。
  - **NYI 方法**（6 个）：`set_placement_strategy`（需策略注册表）、`set_node_maintenance`（需 Raft 命令）、`pin_volume_to_node`/`unpin_volume`（需新 feature）、`add_shard`/`drain_shard`/`remove_shard`（需 filer 连接）。返回 `ManageError::InvalidState("not yet implemented: ...")`。
- `RebalanceEngine`：暴露 `migration_policy()`/`rebalance_policy()` 访问器，供 ManagementApi 更新策略。
- `MasterNode`：新增 `management_api` 字段 + `management_api()` 访问器，`start()` 中构造。
- 测试：master 99 测试 PASS，clippy 0 警告，rustfmt clean。

**已完成：ManagementApi gRPC handler 接入**

- `master.proto`：新增 10 个 allocator 管理 RPC + 对应消息类型：
  - `TriggerRebalanceCheck`（dry_run + 执行）、`PauseAllMigrations`/`ResumeMigrations`/`CancelMigration`（迁移控制）、`GetMigrationTasks`（任务状态查询）。
  - `CreateVolumeManaged`/`DrainVolumeManaged`/`RemoveVolumeManaged`（卷伸缩）。
  - `UpdateMigrationPolicy`/`UpdateRebalancePolicy`（策略热更新）。
- `server.rs`：10 个 gRPC handler 实现，每个委托 `MasterManagementApi` 对应方法，返回结构化 `success`/`error` 响应。
- 转换辅助函数：`rebalance_action_to_proto`（RebalanceAction→RebalanceActionInfo）、`migration_task_to_proto`（MigrationTaskStatus→MigrationTaskInfo）。
- 未初始化优雅降级：engine/API 未就绪时返回 `success=false` + 描述性错误（如 Raft follower 上），不崩溃。
- 测试：master 99 PASS，allocator 77 PASS，clippy 0 警告，rustfmt clean。

**已完成：Shard 伸缩 filer 连接**

- Filer 侧 proto：新增 `AddShard`/`DrainShard`/`RemoveShard` RPC（FilerMetaService + PosixMetaService 双服务）+ 消息类型。
- Filer 侧 handler：`grpc_service.rs` + `posix_service.rs` 实现 3 个 handler，委托 `ShardStrategy` 的 `add_shard_auto`/`drain_shard`/`remove_shard`。
- Filer 侧 `ShardStrategy::remove_shard`：新增方法，委托 `ShardMap::remove_shard`（合并 range 到前一个 Active 条目）+ 递减 shard_count。
- Master 侧 proto 编译：`build.rs` 新增 filer.proto 编译（filer_proto 子目录避免 package=powerfs 冲突）。
- Master 侧 `filer_proto` 模块 + `filer_client` 模块：`FilerManagementClient` 连接第一个健康 filer，调用 shard scaling RPC。
- `MasterManagementApi`：`add_shard`/`drain_shard`/`remove_shard` 从 NYI stub 改为通过 `FilerManagementClient`（`block_in_place`+`Handle::block_on` 桥接）调用 filer gRPC。
- 测试：master 99 PASS，filer 74 PASS，clippy 0 新增警告，fmt clean。

**已完成：真实 MigrationExecutor 实现**

- `MasterMigrationExecutor`：替换 `LoggingExecutor`，执行端到端数据迁移。
  - `cold_needles`：通过 volume client `ListNeedles` RPC 枚举冷 needle。
  - `start_migration`：spawn 异步任务执行 `run_migration`，完成后通过 completion channel 通知 scheduler。
  - `cancel_migration`：协作式取消（`AtomicBool` 标志），任务在各 needle 拷贝间检查取消标志。
  - `run_cold_data_migration`：filer `FindInodesByVolume` 反查 needle→inode → 读源 needle → 写目标 volume（auto-assign file_key）→ filer `UpdateInodeSizeChunks` 更新 chunk 映射 → 删除源 needle。
- Volume 侧：`ListNeedles` RPC + `write_needle` 支持 `file_key=0` 自动分配。
- Filer 侧：`FindInodesByVolume` RPC（跨 shard needle→inode 反查）+ `UpdateInodeSizeChunks` RPC（Raft 复制 chunk 列表交换）。
- Master 侧：`volume_client` 新增 `list_needles`/`write_needle_return_key`；`filer_client` 新增 `find_inodes_by_volume`/`update_inode_size_chunks`。
- `RebalanceEngine::new_with_master`：用 `MasterMigrationExecutor` 构造引擎；`master::start()` 改用此构造器。
- `run_tick` 完成队列 drain 同时支持 sync（LoggingExecutor）和 async（MasterMigrationExecutor）完成。
- 测试：master 99、filer 74、volume 18、allocator 77 全部 PASS，clippy 0 新增警告。

**已完成：Volume pinning（pin_volume_to_node / unpin_volume）**

- `RaftCommand::PinVolume { volume_id, node_id }` / `UnpinVolume { volume_id }`：新增 Raft 命令变体，serde 自动序列化，所有 master 副本一致。
- `MasterNode::volume_pins: RwLock<HashMap<VolumeId, String>>`：Raft 复制的 pin 注册表。
- `apply_pin_volume` / `apply_unpin_volume`：apply_command match 新增分支，更新内存 pin 注册表。
- `MasterNode::pin_volume_to_node` / `unpin_volume`：leader-only async 方法，propose Raft 命令。
- `ClusterSnapshot::pinned_volumes: HashMap<u64, String>`：快照新增字段，`build_cluster_snapshot` 从 pin 注册表注入。
- LoadBalancer 尊重 pin：
  - `over_threshold_active_volumes`：跳过 pinned volume（不作为冷数据迁移源）。
  - `compute_hot_data_action`：`volume_ids` 排除 pinned volume（不作为热数据迁移源），仅包含 busiest 节点上的非 pinned Active volume。
- `MasterManagementApi::pin_volume_to_node` / `unpin_volume`：从 NYI stub 改为 `block_in_place`+`Handle::block_on` 桥接，调用 master 的 Raft-proposed 方法。
- gRPC：`PinVolume` / `UnpinVolume` RPC + handler（master.proto + server.rs），复用 `VolumeManageResponse`。
- 测试：allocator 79（+2 pin 行为测试），master 99，clippy 0 警告，fmt clean。

**待完成**：
- `set_placement_strategy`：需要运行时策略注册表（低优先级）。

**已完成：set_node_maintenance Raft 命令**

- `RaftCommand::SetNodeMaintenance { node_id, enabled }`：新增 Raft 命令变体，serde_json 自动序列化。
- `apply_set_node_maintenance`：在 apply_command match 中路由，通过 `topology.get_node_mut` 设置 `maintenance_mode` 标志。
- `MasterNode::set_node_maintenance`：leader-only async 方法，遵循 `update_volume_state` 的 propose 模式。
- `MasterManagementApi::set_node_maintenance`：从 NYI stub 改为 `block_in_place`+`Handle::block_on` 桥接，调用 master 的 Raft-proposed 方法。
- `SetNodeMaintenance` gRPC RPC + handler：供运维/AI Agent 通过 gRPC 设置节点维护模式。
- 效果：`maintenance_mode=true` 时，集群快照构建器将节点映射为 `NodeRuntimeState::Maintenance`，分配器在所有分配决策中排除该节点。
- 测试：master 99 PASS，clippy 0 警告，rustfmt clean。
