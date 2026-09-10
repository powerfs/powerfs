# Filer 管理重新设计方案

> **核心设计原则**：前端只跟 Monitor 交互。Filer admin 不能靠 nginx 转发，Monitor 必须作为唯一入口。

## 一、现状问题诊断

### 1.1 架构层面：违反"前端只跟 Monitor 交互"原则（最严重）

当前 Filer 页面的所有 API 调用（`powerfs-monitor-frontend/src/services/api.ts:877-892`）：

```ts
// Note: Filer admin APIs are proxied via nginx (/api/filer/* -> filer:8888/admin/*)
const response = await api.get('/filer/status')
const response = await api.get('/filer/shards')
```

问题：
- 前端实际跟 filer 进程的 `/admin/*` 通信，靠 nginx 反代
- Monitor 后端完全没有 `/api/filer/*` 路由
- 违反设计原则：前端必须只跟 Monitor 交互
- 副作用：认证绕过（admin 接口不走 Monitor 的 JWT 中间件）、单点失败（filer 挂了前端直接 502）、多 filer 时只能命中一个、CORS 配置脆弱

### 1.2 数据层面：没数据 + 不匹配新架构

**没数据的原因**：nginx 反代到 filer 的 `/admin/status`，但 filer 进程可能没起来 / 端口不对 / 反代规则缺失 → 前端拿不到任何 FilerStatus，页面显示全 0。

**架构不匹配**：
- FilerStatus 只反映**单个 filer 实例**的视角（`shard_count/leader_count/total_inodes`），但新架构是多 filer 集群
- 用户关心的是"集群里几个 filer？哪个健康？哪个挂了？shard 分布是否均衡？" —— 单实例视角答不上来
- `is_healthy` 字段是 master 注册视角的静态值，不反映心跳超时

### 1.3 管理便捷性：功能割裂、操作分散

- Filer 列表、Shard 详情、Balancer 控制分散在不同 API 调用，没有统一视图
- Bucket 列表只是字符串数组，不能管理（创建/删除/配额）
- 没有运维操作（重启 balancer、手动触发 rebalance、查看某个 filer 的 shard 分布）
- CRDT 冲突已经降级为健康指示器，但占着页面主视觉位置

## 二、新架构下的 Filer 角色定位

基于代码调研（`powerfs-filer/proto/filer.proto`、`powerfs-filer/src/meta_shard_manager.rs`、`powerfs-filer/src/shard_scheduler.rs`）：

| 维度 | 实际能力 |
|---|---|
| 集群形态 | 多个 filer 节点组成 Raft group，每个节点持有部分 shard 的 leader |
| 元数据分片 | 按 inode 范围切分 ShardDetail（shard_id / inode_range / term / commit_index / qps） |
| 负载均衡 | ShardScheduler 自动迁移 shard leader，可 start/stop/trigger + 配置阈值 |
| 健康维度 | master 视角 is_healthy + 心跳超时机制 + Raft term 滞后检测 |
| Bucket 管理 | `/admin/status` 返回 buckets 列表，但 filer 没有 bucket CRUD admin API |

**结论**：Filer 页面从"单实例状态查看"重构为"多 filer 集群运维中心"，包含 4 个能力域：节点管理 / Shard 分布 / Balancer 调度 / 健康监控。

## 三、设计方案：Monitor-side Filer Admin Bridge

### 3.1 核心原则

Monitor 作为 Filer admin 唯一入口：所有 `/api/filer/*` 请求由 Monitor 处理，Monitor 内部通过 HTTP（reqwest，已有依赖）调用目标 filer 的 `/admin/*`，绝不让前端直连 filer。

### 3.2 后端：Monitor 新增 Filer Admin Bridge

#### 3.2.1 Filer 注册表（已有，复用）

Monitor 通过 gRPC `ListFilers` 已经拿到 filer 注册信息（`powerfs-monitor/src/main.rs:609-640`）：

```rust
FilerNodeInfo {
    node_id: String,
    address: String,
    grpc_port: u32,
    http_port: u32,      // 关键：admin HTTP 端口
    is_healthy: bool,    // master 视角静态值
    leader_count: u64,
    total_shards: u64,
}
```

加上 `metric_store` 里的心跳数据（`last_seen` / `cpu_usage` / `disk_usage`）→ 真实健康状态。

#### 3.2.2 新增 Monitor HTTP 路由（替换 nginx 反代）

| Monitor 路由 | 方法 | 说明 | 后端动作 |
|---|---|---|---|
| `/api/filer/nodes` | GET | 新增：集群 filer 节点列表（合并 master 注册 + 心跳） | gRPC ListFilers + metric_store.get_nodes(filer) |
| `/api/filer/nodes/:node_id/status` | GET | 单节点详细状态 | reqwest → `http://{address}:{http_port}/admin/status` |
| `/api/filer/nodes/:node_id/shards` | GET | 单节点 shard 列表 | reqwest → `/admin/shards` |
| `/api/filer/nodes/:node_id/shards/:shard_id` | GET | 单 shard 详情 | reqwest → `/admin/shards/:id` |
| `/api/filer/nodes/:node_id/balancer/status` | GET | balancer 状态 | reqwest → `/admin/balancer/status` |
| `/api/filer/nodes/:node_id/balancer/start` | POST | 启动 balancer | reqwest → `/admin/balancer/start` |
| `/api/filer/nodes/:node_id/balancer/stop` | POST | 停止 balancer | reqwest → `/admin/balancer/stop` |
| `/api/filer/nodes/:node_id/balancer/trigger` | POST | 手动触发 rebalance | reqwest → `/admin/balancer/trigger` |
| `/api/filer/nodes/:node_id/balancer/config` | GET/PUT | balancer 配置 | reqwest → `/admin/balancer/config` |
| `/api/filer/cluster/status` | GET | 新增：聚合所有 filer 的集群级状态 | 遍历所有 filer 调 `/admin/status` 聚合 |
| `/api/filer/cluster/shards` | GET | 新增：集群全局 shard 视图（按 shard_id 聚合多 filer 副本） | 遍历所有 filer 调 `/admin/shards` 合并 |

#### 3.2.3 关键设计点

- **节点选择策略**：`/api/filer/nodes/:node_id/*` 路由里，Monitor 根据 `node_id` 查注册表拿到 `address:http_port`，再发起 reqwest 请求。filer 挂了 → reqwest 超时/连接拒绝 → Monitor 返回 503 + 错误详情。
- **集群聚合查询并发**：`/api/filer/cluster/*` 用 `futures::future::join_all` 并发请求所有 filer，单节点失败不影响其他节点结果（partial success 语义）。
- **超时控制**：每个 reqwest 请求设 3s 超时，避免单个 filer 慢拖垮整个页面。
- **认证一致性**：所有 `/api/filer/*` 走 Monitor 的 JWT 中间件（admin 操作 require admin），修复当前 admin 接口无认证的安全问题。
- **缓存**（Phase C 可选）：集群状态查询结果缓存 5s，避免前端轮询打爆 filer。

#### 3.2.4 新增 `filer_admin_client.rs` 模块

封装 reqwest 调用，统一处理超时/错误/重试。

### 3.3 前端：Filer 页面重构

#### 3.3.1 页面信息架构（4 个 Tab）

```
Filer 管理
├─ Tab 1: 节点管理 (Cluster Nodes)         ← 新增，最重要的视图
│   ├─ KPI 行: 节点总数 / 健康 / 异常 / Leader 分布
│   ├─ Filer 节点表 (node_id / address / 心跳状态 / leader_count / total_shards / 操作)
│   └─ 操作: 查看详情 / 重启 balancer / 触发 rebalance
│
├─ Tab 2: Shard 分布 (Shard Topology)      ← 重构,集群全局视角
│   ├─ KPI: shard 总数 / leader 分布均匀度 / 落后副本数
│   ├─ Shard 表 (shard_id / inode_range / leader_node / term / commit_index / qps)
│   ├─ Leader 分布柱状图 (每节点 leader 数)
│   └─ 异常 shard 高亮 (term 落后 / commit_index 滞后)
│
├─ Tab 3: Balancer 调度 (Load Balancer)    ← 重构, 按节点维度
│   ├─ 集群 balancer 总览 (running 节点数 / 总迁移数 / 成功率)
│   ├─ 每节点 balancer 状态卡 (start/stop/trigger 按钮)
│   ├─ Balancer 配置编辑 (阈值 / 策略)
│   └─ 迁移历史 (最近 N 次迁移记录)
│
└─ Tab 4: Bucket & 健康 (Buckets & Health) ← 精简, 整合
    ├─ Bucket 列表 (从 cluster status 聚合)
    ├─ CRDT 冲突健康指示器 (已做, 移到这里)
    └─ Raft 健康度 (term 滞后检测 / commit_index 落后检测)
```

#### 3.3.2 用户最关心的状态（基于运维场景）

| 关心的状态 | 数据来源 | 展示方式 |
|---|---|---|
| 集群有几个 filer？都活着吗？ | gRPC ListFilers + 心跳超时 | 节点表 status 列（green/red） |
| shard leader 分布均匀吗？ | 集群 shard 聚合 | 柱状图 + 不均匀度 KPI |
| 有没有 shard 落后？ | shard 的 term / commit_index 对比 | 红色高亮异常 shard |
| balancer 在跑吗？效果如何？ | SchedulerStatus | running 状态 + 迁移成功率 |
| 某个 filer 负载高吗？ | 心跳 cpu/mem/disk + shard qps | 节点表负载列 |
| bucket 有哪些？ | cluster status 聚合 | bucket 列表 |

#### 3.3.3 管理便捷性改进

- 批量操作：Balancer 的 start/stop/trigger 支持"应用到所有节点"按钮（Monitor 遍历调用）
- 快捷诊断：节点异常时一键"诊断" → Monitor 并发调该节点所有 admin 接口，返回汇总报告
- 配置编辑：Balancer config 用表单编辑（不用手拼 JSON）
- 操作确认：所有 POST 操作弹确认框，显示影响范围

### 3.4 API 契约定义（前端 ↔ Monitor）

#### 新增/修改的 API 函数（api.ts）

```ts
// 节点管理
getFilerNodes(): Promise<FilerNode[]>              // 新增
getFilerNodeStatus(nodeId): Promise<FilerStatus>   // 改: 加 nodeId 参数

// Shard (集群视角)
getClusterShards(): Promise<ClusterShard[]>        // 新增: 按 shard_id 聚合
getFilerNodeShards(nodeId): Promise<ShardDetail[]> // 改: 加 nodeId

// Balancer (按节点)
getFilerNodeBalancerStatus(nodeId): Promise<SchedulerStatus>
startFilerNodeBalancer(nodeId): Promise<void>
stopFilerNodeBalancer(nodeId): Promise<void>
triggerFilerNodeBalancer(nodeId): Promise<void>
getFilerNodeBalancerConfig(nodeId): Promise<SchedulerConfig>
updateFilerNodeBalancerConfig(nodeId, config): Promise<void>

// 批量操作
startAllFilerBalancers(): Promise<BatchResult>     // 新增
stopAllFilerBalancers(): Promise<BatchResult>      // 新增
```

#### 类型定义（types/index.ts）

```ts
export interface FilerNode {
  node_id: string
  address: string
  http_port: number
  grpc_port: number
  // 真实健康状态 (来自 metric_store 心跳, 不是 master 静态值)
  heartbeat_status: 'online' | 'offline'
  last_seen_ago_secs: number
  cpu_usage: number
  mem_usage: number
  disk_usage: number
  // master 注册信息
  is_registered: boolean
  leader_count: number
  total_shards: number
}

export interface ClusterShard {
  shard_id: number
  inode_range_start: number
  inode_range_end: number
  // 多副本: 同一 shard_id 在不同 filer 上的状态
  replicas: Array<{
    node_id: string
    is_leader: boolean
    term: number
    commit_index: number
    applied_index: number
    inode_count: number
    write_qps: number
    read_qps: number
  }>
  // 集群级健康判定
  is_healthy: boolean       // term 一致 + commit_index 落后 < 阈值
  lag_reason?: string      // 不健康时的原因
}
```

## 四、实施路径（分 Phase）

### Phase A: Monitor Bridge 基础（P0，必须先做）
1. 新增 `filer_admin_client.rs`（reqwest 封装）
2. 实现 `/api/filer/nodes`（合并 gRPC ListFilers + 心跳）
3. 实现 `/api/filer/nodes/:node_id/{status,shards,shards/:id,balancer/*}`（代理）
4. 删除前端 nginx 反代依赖，所有 API 改走 Monitor
5. 验收：Filer 页面能显示节点列表 + 单节点 status

### Phase B: 前端页面重构（P1）
1. 拆 4 Tab 结构（节点管理 / Shard 分布 / Balancer / Bucket&健康）
2. 节点管理 Tab：Filer 节点表 + 心跳状态 + 负载
3. Shard 分布 Tab：集群 shard 视图 + leader 柱状图
4. 验收：页面有真实数据，不再是全 0

### Phase C: 集群聚合 + 高级功能（P2）
1. `/api/filer/cluster/status` + `/api/filer/cluster/shards` 聚合端点
2. Balancer 批量操作（start/stop all）
3. Shard 异常检测（term/commit_index 滞后高亮）
4. Balancer 配置表单编辑
5. 验收：能完整管理 filer 集群

### Phase D: 健康度评分（P3，可选）
1. Filer 集群健康度评分（综合心跳 + Raft 一致性 + 负载均衡度）
2. Dashboard 顶部 filer 健康徽标
3. 验收：Dashboard 一眼看出 filer 集群状态

## 五、决策记录（已确认）

1. **Balancer 批量操作**：✅ 需要。Phase C 实现 `start_all_filer_balancers` / `stop_all_filer_balancers` / `trigger_all_filer_balancers`，Monitor 内部遍历所有 filer 并发调用，返回 `BatchResult { success: [], failed: [] }`。
2. **Bucket 管理**：✅ 已实施（filer + Monitor bridge）。方案：
   - filer 新增 `GET /admin/buckets`（列表，JSON）
   - filer 新增 `POST /admin/buckets`（创建，参数：bucket_name + 可选 collection + 可选 quota）
   - filer 新增 `DELETE /admin/buckets/:name`（删除，带级联确认参数）
   - filer 新增 `PUT /admin/buckets/:name/quota`（设置配额，size_limit=0 表示无限制）
   - Monitor bridge 透传这 4 个新接口到 `/api/filer/buckets*`，所有写操作后失效 cluster/status 缓存
3. **缓存策略**（建议方案，Phase A 不引入，Phase C 视负载情况启用）：
   - **单节点查询（status/shards/balancer/status）**：不缓存，要实时性
   - **集群聚合查询（cluster/status, cluster/shards）**：缓存 5s，避免前端轮询打爆 filer
   - **前端轮询频率**：节点列表 10s、Shard 分布 15s、Balancer 状态 5s
   - **缓存失效**：所有写操作（balancer start/stop/trigger/config PUT、bucket CUD）后立即失效相关缓存
   - **实现方式**：Monitor 内存 `RwLock<HashMap<cache_key, (Value, Instant)>>`，简单 TTL 过期，不引入 Redis
4. **认证要求**：✅ 所有 `/api/filer/*`（包括读操作）都要求 admin 权限。理由：filer admin 接口暴露集群元数据拓扑（shard 分布、inode 数量），属于敏感运维信息，不应让普通用户访问。
