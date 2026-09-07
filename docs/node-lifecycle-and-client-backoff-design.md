# 数据节点生命周期管理 + 客户端连接退避 + Volume transport 选择设计

## 背景与问题

RDMA 验证（#76）过程中，临时 `docker rm -f` 一个 volume 节点后触发了两个关联缺陷：

1. **filer 连接风暴 / 137 被杀**：死节点地址残留在 master 拓扑里，filer 的 scrubber
   每轮对其 `get_or_connect`。`ClientConnPool` 只有"成功缓存"，**没有失败负缓存、
   没有 backoff**，失败立即返回、下一个 inode 立刻重试，形成 ~15–70 次/秒热重连；
   每次 RDMA 连接新建 MR 池（`rdma_buf_num=128 × rdma_buf_size=64KB` ≈ 8MB 起步），
   分配速度远超 Drop 释放速度，内存高速增长后被系统压力杀掉（非 cgroup OOM）。
2. **master 对 volume 节点增删没有统一闭环**：
   - `last_heartbeat` 只写不读，**无 liveness 检测**，死节点永不摘除
     （现存僵尸节点 volume-server-2/3）。
   - `NodeState::Unavailable`（心跳超时）/`Maintenance` 枚举已存在但无人置位；
     维护模式只影响 allocator 新分配，**不进入下发给 filer 的拓扑**。
   - `apply_remove_node` 只删 node、**不删其名下 volume 路由**；且未接到
     gRPC admin API / CLI（CLI `cluster-remove` 的 `RemoveNodeRequest{u64}` 是
     Raft 成员管理，不是数据节点）。
   - filer zone 构建（`register_filer_zone` → `list_volume_routes()`）返回
     **全部** volume 路由，不校验所属节点健康/维护状态。

## 目标

- master 统一管理数据节点生命周期：心跳注册（已有）→ 健康/超时/维护/摘除。
- 下发给 filer 的拓扑只包含可服务节点（排除超时/维护/摘除）。
- 提供数据节点 add/remove 的 gRPC admin API + CLI（字符串 node_id，连带 volume）。
- 客户端对不可达地址做负缓存 + 指数退避，拓扑陈旧时也不风暴、不爆内存（最后防线）。

## 设计

### A. master 节点生命周期管理

#### A1. 心跳过期检测（liveness watcher）

新增 leader 侧后台任务（复用现有 `tokio::spawn` tick 模式，仅 leader 执行）：

- 周期 `POWERFS_NODE_LIVENESS_INTERVAL`（默认 10s）。
- 扫描 `topology` 中所有数据节点，依据 `last_heartbeat`：
  - 超过 `POWERFS_NODE_OFFLINE_TIMEOUT`（默认 30s）未心跳 → 提议
    `RaftCommand::SetNodeState { node_id, state: Unavailable }`（幂等：已是
    Unavailable 则不重复提议）。
  - 心跳恢复时 `apply_heartbeat` 已会把节点置回 Healthy（现有逻辑），自动康复。
- 不自动删除节点（防网络抖动误删数据路由）；摘除走显式 remove（A3）。

新增 Raft 命令（仿 `SetNodeMaintenance`）：

```rust
RaftCommand::SetNodeState { node_id: String, state: NodeState }
// apply: topology.get_node_mut(nid).state = state; state_since = now
```

#### A2. 拓扑/zone 下发过滤

新增 helper：根据 `topology` 计算"可服务节点集合"（`state.is_readable()` 且
`maintenance_mode == false`；即排除 Unavailable/Fault/Maintenance）。

- 新增 `Master::list_servable_volume_routes()`：在 `list_volume_routes()` 基础上
  过滤掉所属 node 不可服务的路由。
- `register_filer_zone()` 的初始选取（`select_volumes_node_anti_affinity`）和
  重注册更新（route_map）都改用 servable 路由。这样 filer 周期重注册（~60s）后
  zone 中不再含死/维护节点地址，scrubber/allocator 自然不再连。
- allocator `build_cluster_snapshot()` 已通过 `map_node_state` 映射
  Maintenance/Down，保持不变（双保险）。

#### A3. 数据节点 remove/add 闭环 + API + CLI

- **Raft apply 增强**：`apply_remove_node(node_id)` 在 `topology.remove_node()`
  之外，连带从 `volume_routes` 删除该 node 名下所有路由（并清理 zone_registry 中
  引用这些 volume_id 的条目）。
- **gRPC admin API**：在 MasterService（management/admin，已要求 admin_token）
  新增 `RemoveDataNode { node_id: String }` / `AddDataNode {...}`（若 add 未暴露），
  内部调用 `master.remove_node(&NodeId)` / `add_node(...)`（Raft propose）。
- **CLI**：新增
  - `powerfs-cli node remove <node_id>`（字符串，数据节点；区别于 raft 成员的
    `cluster remove <u64>`）
  - `powerfs-cli node list`（复用 topology/status，显示 state/maintenance/last-heartbeat）
  - `powerfs-cli node maintenance <node_id> <true|false>`（包装现有 SetNodeMaintenance）
- remove 前置校验：节点仍 Healthy 且有 volume 时，提示先 drain（或提供
  `--force`）；默认拒绝摘除在线有数据节点，避免误删。

状态机：

```
                 心跳注册                心跳>30s
  (不存在) ───────────────▶ Healthy ◀──────────────▶ Unavailable
                              │  ▲                          │
            maintenance=true  │  │ maintenance=false        │ 心跳恢复(自动 Healthy)
                              ▼  │                          │
                          Maintenance                       │ remove
                              │                             ▼
                              └──────────────▶ (拓扑/路由删除) Removed
```

### B. 客户端连接失败负缓存 + 指数退避（powerfs-net ClientConnPool）

在 [client_pool.rs](../powerfs-net/src/client_pool.rs) 增加 per-addr 失败冷却：

- 新增 `failures: DashMap<String /*pool key*/, FailedConn>`，
  `FailedConn { next_allowed_at: Instant, attempts: u32 }`。
- `get_or_connect`：
  1. 命中已连接缓存 → 复用（不变）。
  2. key 在 `failures` 且 `Instant::now() < next_allowed_at` → 直接返回
     `NetError::Unavailable("addr in backoff")`，**不新建连接、不分配 RDMA 资源**。
  3. 否则尝试连接；失败 → 记录/更新 failures：
     `delay = min(base * 2^attempts, max)`，base=500ms，max=30s，
     `next_allowed_at = now + delay`。
  4. 连接成功 → 清除该 key 的 failures。
- 效果：死地址第 1 次失败后，后续请求在冷却窗内快速失败（微秒级），不再建连；
  节点恢复后下一次冷却窗结束即可重连成功并清负缓存。
- 退避仅针对"连接建立失败"，不影响已连接链路上的请求超时语义。

### C. 内核 volume 路径 transport 选择（RDMA 优先 + TCP fallback）

#### 现状

内核 volume 数据/meta 连接被**写死 TCP**，两处 override：

- `powerfs_net_data.c:541`（动态路由发现 `pfs_ensure_volume_conn`）
- `powerfs_net_conn.c:2757`（静态 pool init）

注释解释："Volume OSD connections ALWAYS use TCP regardless of mount -o
transport=xxx"。原因是：如果内核继承 `g_pool.transport_type=RDMA` 去连一个只
监听 TCP 的 volume listener，会 immediate EOF / errors=1 / ret=-107。

但这个"修复"是**矫枉过正**：它假设 volume 永远只有 TCP listener，即使 volume
配置了 `transport=rdma` 也不走 RDMA。**内核已有完整的 RDMA transport**（filer
meta channel 用了 `powerfs_rdma_ops`），且 `powerfs_conn_connect_one()` 已有
AUTO fallback 逻辑（conn.c:1954-2008）：先试 RDMA，`init_conn`/`connect` 失败
则回退 TCP。volume 路径只是没有走到这个分支。

#### 修复

两处 volume TCP-forced 改为：**直接继承 mount 的 `g_pool.transport_type`**。

```c
/* Volume connections inherit mount transport type:
 *   tcp  → TCP only
 *   rdma → RDMA only (fails hard, no TCP fallback)
 *   auto → RDMA first, TCP fallback on failure */
conn->transport = powerfs_transport_pick_ops(g_pool.transport_type);
conn->transport_type = g_pool.transport_type;
```

`powerfs_conn_connect_one` 已有各模式的正确处理：
- `transport=rdma`：RDMA 失败直接返回错误（**不 fallback**）
- `transport=auto`：RDMA 失败后回退 TCP
- `transport=tcp`：走 TCP socket 路径

- `powerfs_net_data.c:533-542`（`pfs_ensure_volume_conn`）
- `powerfs_net_conn.c:2750-2757`（静态 pool init volume 分支）

#### 效果

| volume 配置 | mount transport | 内核 volume 实际路径 |
|-------------|----------------|---------------------|
| tcp | tcp | TCP（不变） |
| tcp | rdma | RDMA 失败 → **报错**（无 fallback，符合预期） |
| tcp | auto | RDMA 快速失败 → TCP fallback（~ms 级，rdma_cm REJECTED） |
| rdma | tcp | TCP（mount 显式选 TCP） |
| rdma | rdma | RDMA 连接成功；**数据帧失败**（#79：Rust server recv buf 64KB < kernel 2MB） |
| rdma | auto | 同上（RDMA 连接成功，数据帧失败） |

- `transport=rdma` 不通就报错（不 fallback），只有 `transport=auto` 才 fallback。
- RDMA 失败是快速失败（rdma_cm REJECTED / ECONNREFUSED，非超时）。
- 内核→filer RDMA 正常（小帧 ≤64KB）；内核→volume RDMA 连接成功但数据帧
  传输因 buffer 不匹配失败（#79），待修复。

#### 非目标（记录为后续特性）

- **Volume 双监听（同时 RDMA + TCP）**：当前 volume 只绑一种 transport。
  AUTO 模式通过"尝试 RDMA → 失败回退 TCP"实现兼容，不需要双监听。未来如需
  "同一 volume 同时服务 RDMA 和 TCP 客户端"（如混合内核客户端 + Rust filer
  客户端），需 volume server 双 listener 支持。记录为后续特性。
- 路由元数据携带 per-volume transport 字段（当前靠 AUTO 探测，不需要）。

### 配置项（均有默认值，可 env 覆盖）

| Env | 默认 | 说明 |
|-----|------|------|
| `POWERFS_NODE_LIVENESS_INTERVAL` | 10s | liveness 扫描周期 |
| `POWERFS_NODE_OFFLINE_TIMEOUT` | 30s | 心跳超时判 Unavailable |
| `POWERFS_CONN_BACKOFF_BASE_MS` | 500 | 客户端连接退避基数 |
| `POWERFS_CONN_BACKOFF_MAX_MS` | 30000 | 客户端连接退避上限 |

## 涉及文件

- `powerfs-master/src/raft_v2.rs`：新增 `RaftCommand::SetNodeState`
- `powerfs-master/src/master.rs`：liveness watcher、`apply_set_node_state`、
  `list_servable_volume_routes`、`register_filer_zone` 过滤、
  `apply_remove_node` 连带 volume、节点 add/remove 管理方法
- `powerfs-master/proto/master.proto` + `server.rs`：RemoveDataNode/AddDataNode/
  node list/maintenance admin RPC
- `powerfs-cli/src/commands/`：新增 `node` 子命令（list/remove/maintenance）
- `powerfs-net/src/client_pool.rs`：失败负缓存 + 指数退避
- `kernel/powerfs_mod/powerfs_net_data.c`：volume TCP-forced → AUTO（RDMA 优先）
- `kernel/powerfs_mod/powerfs_net_conn.c`：volume 静态 init TCP-forced → AUTO

## 验证

- 单元：退避窗口内不重复建连；servable 路由过滤掉 Unavailable/Maintenance；
  SetNodeState/RemoveNode Raft apply 正确。
- 集群（host RDMA 环境）：
  1. 起一个临时 volume 节点 → `node list` 可见 Healthy。
  2. `docker rm -f` 该节点 → 30s 内 `node list` 显示 Unavailable；filer 日志
     不再出现对死地址的高频重连（退避生效），zone 重注册后 total_volumes 回落。
  3. filer 内存稳定（docker stats），无 137。
  4. `node remove <id>` → 节点及其 volume 路由从拓扑消失；重复 remove 幂等。
  5. 回归：正常读写/fio、scrubber EC 不受影响；维护节点 `node maintenance`
     后不接收新分配。
  6. volume transport：mount transport=rdma → volume TCP listener 的连接走
     AUTO（RDMA 快速失败 → TCP fallback），dmesg 显示 `auto: rdma connect
     ... failed, falling back to TCP`；volume RDMA listener 的连接走 RDMA。

## 非目标

- 不做死节点上数据的自动迁移/重构建（drain/rebalance 已有独立引擎，后续对接）。
- 不改 #76 的 Rust RDMA 客户端帧 bug（独立问题）。
- **Volume 双监听（同时 RDMA + TCP）**为后续特性，当前 AUTO fallback 足够。
- 不在路由元数据中加 per-volume transport 字段（AUTO 探测即可）。
