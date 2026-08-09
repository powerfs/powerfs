# 配置一致性修复方案 (Config Consistency Fix Plan)

> 状态：进行中
> 起因：FUSE 客户端创建文件时出现 0 字节文件、跨客户端读 I/O error、子目录创建失败。
> 根因：master 已经能下发全局 `shard_count` 与 filer 列表，但客户端未消费，
> 导致 `calculate_shard_id(inode) = (inode / 1_000_000) % 256`，而 Filer 实际按 `% 3`
> 切分，路由错位 → inode not found / EIO。

## 1. 现状盘点

| 环节 | 文件 | 现状 |
|------|------|------|
| Master `handle_get_topology` | `powerfs-master/src/net_handler.rs:504` | ✅ 已返回 filer 列表 + `TotalShards` |
| Master `handle_register_filer` | `powerfs-master/src/net_handler.rs:662` | ✅ 已校验 `shard_count` + `Force` + `STATUS_ERR_BAD_REQUEST` |
| master-net `get_topology()` | `powerfs-master-net/src/client.rs:247` | ❌ 只解析 `leader` + `volumes`，丢掉 filer 列表/total_shards |
| fuse `ClusterTopology` | `powerfs-fuse-core/src/topology.rs:71` | ❌ 只有空的 `shards` 字段，无 `shard_count` |
| fuse `fetch_topology()` | `powerfs-fuse-core/src/topology.rs:391` | ❌ 不填 `shards`，只填 `volumes` |
| fuse `setup_default_routes()` | `powerfs-fuse-core/src/meta_shard_client.rs:552` | ❌ fallback 时硬编码 256 分片 |
| fuse `calculate_shard_id()` | `powerfs-fuse-core/src/meta_shard_client.rs:603` | ❌ 用 `shard_router.len()`=256，应为 `% shard_count` |
| Filer `register_filer()` 响应处理 | `powerfs-filer/src/zone_client.rs:41` | ❌ `BAD_REQUEST` 当普通错误，仅 warn 不退出 |
| Filer 注册循环 | `powerfs-filer/src/main.rs:494` | ❌ 不区分 `BAD_REQUEST`，未透传 `--force` |

## 2. 健康状态模型（设计原则）

按用户决策：监控无关，filer/volume 都注册到 master，关键组件缺失即 unhealthy，
个别非关键缺失为 degraded。

| 状态 | 判定 | 行为 |
|------|------|------|
| **healthy** | Master 有 leader + 所有 filer `shard_count` 一致 + 每分片有 leader + 每 zone 有 volume | 正常服务 |
| **degraded** | Master 有 leader + 个别 filer/volume 离线但 Raft 多数派可用 | 降级服务 |
| **unhealthy** | 无 Master leader / `shard_count` 不一致 / 某分片无 leader | 禁止新写入，启动时拒绝安装 |

> 三级状态在 `GetTopology` 中下发由 monitor 展示属于扩展性内容，本方案只落地
> 启动期一致性门禁（Step 4/5），运行期状态机留作后续。

## 3. 实施步骤

### Step 1 · master-net 解析全局信息

**改动文件**：
- `powerfs-master-net/src/types.rs`
- `powerfs-master-net/src/client.rs`

**改动内容**：
- 新增 `FilerRoute { filer_id, advertise_addr, net_port, is_healthy, shard_ids }`
- `TopologyInfo` 增加 `filers: Vec<FilerRoute>` 与 `total_shards: u64`
- `get_topology()` 解析 `FilerListEntries / FilerAddress / NetPort / IsDir / ShardIdList / TotalShards`

### Step 2 · fuse 拓扑填充真实分片

**改动文件**：
- `powerfs-fuse-core/src/topology.rs`

**改动内容**：
- `ClusterTopology` 增加 `shard_count: usize`（master 全局值，独立于 `shards.len()`）
- `shard_count()` 优先返回 `shard_count`，回退到 `shards.len()`
- `fetch_topology()` 把 `TopologyInfo.filers` + `total_shards` 转成 `ClusterTopology`：
  - 每个 `shard_id` → `ShardInfo { leader_addr }`
  - `shard_count = total_shards`

### Step 3 · 消除 256 硬编码

**改动文件**：
- `powerfs-fuse-core/src/meta_shard_client.rs`

**改动内容**：
- `sync_shard_router()` 从 topology 读取真实分片
- `setup_default_routes()` 仅在拓扑完全为空时用 default，且用 `1` 而非 `256`
- `calculate_shard_id()` 优先用 `topology.shard_count()`，保证 `% 3`

### Step 4 · Filer 启动门禁

**改动文件**：
- `powerfs-filer/src/zone_client.rs`
- `powerfs-filer/src/main.rs`（含 FilerConfig）

**改动内容**：
- `register_filer()` 增加 `force: bool` 参数，发送 `FieldId::Force`
- 区分 `STATUS_ERR_BAD_REQUEST`：返回带 detail 的错误
- 注册循环中：`BAD_REQUEST` 且非 force → `error!` + `process::exit(1)`
- `BAD_REQUEST` 且 force → `warn` 继续
- `FilerConfig` / CLI 增加 `force` 字段

### Step 5 · Fuse 挂载门禁

**改动文件**：
- `powerfs-fuse-core/src/fuse_client_facade.rs`
- `powerfs-fuse/src/main.rs`（含 FuseConfig）
- `config/fuse-*.toml`

**改动内容**：
- `fetch_topology` 后校验 `total_shards > 0` 且至少 1 个 healthy filer
- 否则拒绝挂载，除非 `--force`
- `fuse.toml` 的 `filer_addresses` 降级为可选（优先取 topology），只保留 `master_addresses` 必填

## 4. 验证矩阵

- cargo check / clippy / pnpm build
- 大文件读写（> inline 阈值）
- 跨客户端读
- 子目录创建
- Filer 启动门禁：用错误 `shard_count` 启动应失败退出，加 `--force` 应启动
- Fuse 挂载门禁：master 不可达应拒绝挂载

## 5. 扩展性（后续）

- Master 在 `GetTopology` 中下发 `cluster_health`（healthy/degraded/unhealthy）
- Monitor 前端展示三级状态
- Filer/Volume 数量变化时的 hash 路由重平衡策略
