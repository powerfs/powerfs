# raft-rs → openraft 迁移方案

> 状态：**方案已确认，待实施**
> 日期：2026-08-12
> 分支：`allocator-decoupling`
> 目标：将 PowerFS 的共识层从 `raft-rs 0.7` 迁移到 `openraft 0.10.0-alpha.33`，统一 Master/Filer 两套 Raft 部署，消除手写 `Ready` 循环与 `eraftpb` protobuf 转发，改用 openraft 的异步自驱动模型 + `RaftNetwork`/`RaftLogStorage`/`RaftStateMachine` trait 体系。
>
> **已确认决策（2026-08-12）：**
> - **版本**：`openraft 0.10.0-alpha.33`（接受 alpha 风险，精确锁版本）
> - **Filer 多组**：采用 openraft 官方 `multiraft` crate
> - **旧数据**：全量重建（放弃 RocksDB 里的旧 eraftpb 日志/快照，从业务层重建元数据，不做在线转换层）
> - **节奏**：Master 先行 + 直接替换（不用 feature flag 双轨，靠分支隔离）

---

## 1. 背景与动机

### 1.1 当前痛点

PowerFS 现有两套独立的 raft-rs 部署（共约 **4200 行** Raft 相关代码）：

| 部署 | crate | 拓扑 | 核心文件 | 行数 |
|---|---|---|---|---|
| Master 元数据 | `powerfs-master` | 单 Raft 组 | `raft_storage.rs` / `raft_node.rs` / `raft_server.rs` | ~1169 + 831 + 217 |
| Filer 分片元数据 | `powerfs-filer`（+ `powerfs-common`） | 多 Raft 组（每分片一组） | `raft_group_manager.rs` / `powerfs-common/src/raft/storage.rs` | ~1539 + 682 |

主要问题：

1. **手写 `Ready` 循环**：`RaftNode::process_ready`（[raft_node.rs:273-472](file:///home/portion/powerfs/powerfs-master/src/raft_node.rs)）和 `RaftGroup::process_ready`（[raft_group_manager.rs:463-676](file:///home/portion/powerfs/powerfs-filer/src/raft_group_manager.rs)）各自实现了 ~200 行的 `Ready` 处理（send_messages / apply_snapshot / commit_entries / persist hardstate / advance），两份逻辑高度重复且易出 bug。
2. **`eraftpb` protobuf 透传**：节点间通过 gRPC `RaftMessageStream`（流式 `eraftpb::Message`）转发，需要 protobuf 序列化、与 `prost` 共存、且 `eraftpb` 类型与业务类型割裂。
3. **`Storage` trait 单体化**：raft-rs 的 `Storage` trait 把日志读取、快照、成员信息塞进一个 trait，外加自写的 `RaftStorageExt`（append/set_hard_state/apply_snapshot/compact/load_from_db/save_to_db），职责不清。
4. **Multi-raft 手写**：Filer 的 `RaftGroupManager` 手写多组调度（每分片一个 `RawNode`），缺统一的组管理抽象。
5. **快照路径脆弱**：`apply_snapshot`/`create_snapshot` 的 RocksDB CF 操作与内存缓存一致性靠人工维护，已有历史 bug（见 `docs/raft-storage-bugs-fixes.md`）。

### 1.2 为什么选 openraft 0.10

- **异步自驱动**：`Raft<C>` 内部跑自己的 tick / 复制 / 选举任务，业务侧只需 `raft.client_write(entry)`，彻底删除 `process_ready` 全套代码。
- **trait 拆分清晰**：v0.10 把存储拆成 `RaftLogStorage`（日志）+ `RaftStateMachine`（状态机+快照）+ `RaftLogReader`（只读读日志）+ `RaftSnapshotBuilder`，职责分明。
- **`RaftNetwork` trait**：节点间 RPC 由 trait 方法 `append_entries` / `vote` / `install_snapshot` 表达，可自然映射到 gRPC，替代裸 `Message` 流。
- **原生 Multi-raft**：openraft 工作区自带 `multiraft` crate（`openraft/multiraft/`），Filer 可直接复用。
- **类型安全**：`RaftTypeConfig` 关联类型让 Entry / NodeId / SnapshotData 全部强类型，消除 `eraftpb` 的 `Vec<u8>` 黑盒。
- **Leader lease / linearizable read**：内置 `ReadPolicy`、leader-lease 支持，免手写 read-index。

### 1.3 风险

- **alpha 版本**：`0.10.0-alpha.33` 仍是 alpha，API 可能在 stable 前调整。需要精确锁版本，并评估是否先上 `0.9.x` stable。
- **edition 2024**：openraft 要求 Rust ≥ 1.85（本地 1.97.1 ✓，CI runner stable ✓）。
- **迁移面广**：两套部署、~4200 行、gRPC proto、测试全部要改。

---

## 2. 当前 raft-rs 集成现状

### 2.1 Master（单组）

- **存储** [raft_storage.rs](file:///home/portion/powerfs/powerfs-master/src/raft_storage.rs)：`RocksDbStorage` 实现 `raft::Storage`，RocksDB CF：`raft_log` / `raft_state` / `raft_snapshot`。维护内存 `entries: VecDeque<Entry>` 缓存 + `hard_state` / `conf_state` / `snapshot_meta`。
- **节点** [raft_node.rs](file:///home/portion/powerfs/powerfs-master/src/raft_node.rs)：`RaftNode` 包装 `raft::RawNode`，`process_ready` 处理 `Ready`（消息/soft state/snapshot/entries/hardstate/committed/advance），`handle_propose` 调 `node.propose` 后立即取 `Ready`。`add_peer`/`remove_peer`/`transfer_leader` 用 `ConfChange`。
- **传输** [raft_server.rs](file:///home/portion/powerfs/powerfs-master/src/raft_server.rs)：gRPC `RaftService`（[master.proto:638-704](file:///home/portion/powerfs/powerfs-master/proto/master.proto)）：`Propose` / `RaftMessageStream`（流 `eraftpb::Message`）/ `SendRaftMessage` / `AddNode` / `RemoveNode` / `TransferLeader` / `GetClusterInfo`。

### 2.2 Filer（多组）

- **存储** [powerfs-common/src/raft/storage.rs](file:///home/portion/powerfs/powerfs-common/src/raft/storage.rs)：`RocksDbRaftStorage` 实现 `raft::Storage` + 自定义 `RaftStorageExt`（`append_entries` / `set_hard_state` / `apply_snapshot_entry` / `compact_log_entries` / `load_from_db` / `save_to_db`）。同样三 CF。
- **多组管理** [raft_group_manager.rs](file:///home/portion/powerfs/powerfs-filer/src/raft_group_manager.rs)：`RaftGroupManager` 管理每分片的 `RaftGroup`，每个 group 一个 `RawNode`，`process_ready` 与 Master 同构。`add_peer`/`remove_peer`/`transfer_leader`。
- **传输** [filer.proto](file:///home/portion/powerfs/powerfs-filer/proto/filer.proto)：`FilerMetaService` / `PosixMetaService` 含 `SendRaftMessage` 等。

### 2.3 共用基线

- `eraftpb::{Entry, Message, Snapshot, HardState, SnapshotMetadata, ConfState}` 作为 raft-rs 的数据载体。
- `ConfChange` / `ConfChangeSingle` / `ConfChangeType` 处理成员变更。
- 测试 `powerfs-master/tests/cluster.rs` / `raft_integration_test.rs` 直接构造 raft-rs 类型。

---

## 3. 目标架构（openraft 0.10）

### 3.1 核心类型映射

| raft-rs 0.7 | openraft 0.10 | 迁移动作 |
|---|---|---|
| `raft::RawNode` | `openraft::Raft<C>` | 删除 `tick`/`Ready` 循环，改 `client_write` |
| `raft::Storage`（单体 trait） | `RaftLogStorage` + `RaftStateMachine` | 拆分 RocksDbStorage 为两个 impl |
| `RaftStorageExt`（自写） | `RaftLogStorage::append` / `truncate` 等 | 删除 Ext，方法并入 `RaftLogStorage` |
| `eraftpb::Entry` | `openraft::Entry<C>`（含 `EntryPayload`） | 重定义业务 Entry payload |
| `eraftpb::Message` 流 | `RaftNetwork::{append_entries,vote,install_snapshot}` | gRPC RPC 化 |
| `eraftpb::Snapshot` | `openraft::Snapshot<C>` + `SnapshotMeta` | 重写快照数据格式 |
| `eraftpb::HardState` / `ConfState` | openraft 内部管理（`RaftState` / `StoredMembership`） | 不再手写持久化 |
| `ConfChange` / `ConfChangeV2` | `raft.change_membership(ChangeMembers)` | 简化为单一 API |
| `RawNode::is_leader()`（查 `State`） | `raft.metrics().await.current_leader` | 改异步 metrics |
| 手写 read-index | `raft.ensure_linearizable(ReadPolicy)` | 内置 |

### 3.2 `RaftTypeConfig` 定义（每部署一个）

示意（非最终代码）：

```rust
openraft::declare_raft_types!(pub MasterRaftTypeConfig:
    D = MasterRequest,
    R = MasterResponse,
    NodeId = NodeId,            // 复用现有 NodeId (String)
    Node = BasicNode,
    Entry = openraft::Entry<MasterRaftTypeConfig>,
    SnapshotData = Cursor<Vec<u8>>,
    AsyncRuntime = openraft_rt_tokio::TokioRuntime,
);
```

Master 与 Filer 各一份（Filer 多组时可共用同一 TypeConfig，组 ID 区分）。

### 3.3 存储拆分

- `RaftLogStorage`：仅管日志 CF（`raft_log`）+ 元数据 CF（`raft_log_meta`：vote / last_purged）。方法 `append` / `truncate` / `purge` / `save_vote` / `get_log_state` 等。
- `RaftStateMachine`：管 `raft_state_meta` CF（last_applied_log / last_membership）+ `raft_state_data` CF（业务状态）+ 快照目录（`snapshot_dir`，文件名按 log_id 序排序）。实现 `apply` / `applied_state` / `build_snapshot` / `install_snapshot` / `get_current_snapshot`。
- **不复用旧 RocksDB CF 数据格式**（决策 6.6 已确认全量重建）：旧 `eraftpb` entry 与 openraft `Entry<C>` 序列化不兼容，新存储首次启动清理旧 CF。CF 命名沿用 powerfs-master 风格（`raft_*` 前缀）。
- **存储实现策略（2026-08-12 确认）**：在 `powerfs-raft` 内重新实现 `RocksLogStore<C>` + `RocksStateMachine<C>`，泛型于 `C: RaftTypeConfig`（同一份代码服务 Master + Filer）。参考 `openraft/examples/log-rocks`（log 层范本）与 `openraft/examples/sm-rocks`（state machine 范本），但不直接 path-dep（避免与 crates.io openraft 版本冲突）。总代码量约 700 行。
- 旧 `RocksDbStorage` / `RocksDbRaftStorage` 的内存 `entries` 缓存删除——openraft 直接走 RocksDB，避免双写。

### 3.4 传输层

`RaftNetwork` trait 的三个方法映射到 gRPC：

| openraft trait 方法 | 新 gRPC RPC |
|---|---|
| `append_entries(REQ) -> Result<AppendEntriesResponse>` | `RaftAppendEntries` |
| `install_snapshot(REQ) -> Result<InstallSnapshotResponse>` | `RaftInstallSnapshot` |
| `vote(REQ) -> Result<VoteResponse>` | `RaftVote` |

- 旧 `RaftMessageStream` / `SendRaftMessage` 删除。
- `RaftNetworkFactory` 实现持有对端地址表（从 master `GetTopology` 拿），按 `NodeId` 路由。
- proto 中 openraft 的请求/响应用 `serde` + `bytes` 序列化承载（openraft 类型开 `serde` feature），或手写 proto 映射。**待决策（见 §6.1）**。

### 3.5 多组（Filer）

两个候选：

- **方案 A**：用 openraft 的 `multiraft` crate（`openraft/multiraft/`），原生多组。
- **方案 B**：保留现有 `RaftGroupManager` 结构，每组一个 `Raft<FilerRaftTypeConfig>`，手动驱动。

**待决策（见 §6.2）**。

---

## 4. 架构差异分析

### 4.1 控制反转（最大变化）

raft-rs：应用主循环驱动 `tick` → `ready()` → 处理 `Ready` → `advance()`。
openraft：`Raft::new(...)` 启动内部 task，应用被动接收 `RaftNetwork` RPC、被动实现 `RaftStateMachine::apply`。

影响：`RaftNode::process_ready`（~200 行）和 `RaftGroup::process_ready`（~200 行）**整体删除**；`tick` 线程删除；`handle_propose` 改为 `raft.client_write(req).await`。

### 4.2 持久化职责转移

raft-rs：应用负责在 `Ready` 里持久化 `HardState` / `Entries` / `Snapshot`。
openraft：框架在 trait 方法里调用持久化（`RaftLogStorage::append` 等），应用只需保证 trait 方法本身持久化即可。`HardState` 不再是应用关心的概念（openraft 内部管 `RaftState`）。

### 4.3 成员变更简化

raft-rs：`ConfChange` / `ConfChangeSingle` / `ConfChangeType` 三层结构，需 `propose_conf_change` + 解析。
openraft：`raft.change_membership(ChangeMembers::AddVoters(ids) | RemoveVoters(...) | Replace(...))`，一行。

### 4.4 传输层从"裸消息流"变"类型化 RPC"

旧：节点间流式 `eraftpb::Message`，应用自己解码后喂给 `RawNode::step`。
新：openraft 直接调你的 `RaftNetwork::append_entries`，你在 impl 里发 gRPC 到对端、拿到响应回传。框架不再要求 `step`。

---

## 5. 迁移策略（分阶段）

总原则：**先存储、再传输、后生命周期；Master 先行，Filer 跟进；保留旧路径做灰度回滚。**

### 阶段 0：准备（不动现有代码）

- [ ] 在 workspace `Cargo.toml` 增加 `openraft = "0.10.0-alpha.33"`（或 path/git 依赖，**待决策 §6.4**）。
- [ ] 建 `powerfs-raft` 新 crate（或在 `powerfs-common` 内开 `raft_v2` 模块），承载 `RaftTypeConfig` / `RaftLogStorage` / `RaftStateMachine` / `RaftNetwork` 的共享实现，供 Master 与 Filer 复用。
- [ ] 决定 openraft 依赖来源：crates.io / git tag / vendored（`openraft/` 目录已克隆）。
- [ ] 评估 `multiraft` crate 成熟度。

### 阶段 1：存储层迁移（可独立验证）

- [ ] 在 `powerfs-raft` 实现 `RaftLogStorage` + `RaftStateMachine`，复用现有 3 个 RocksDB CF 的**命名结构**（`raft_log` / `raft_state` / `raft_snapshot`），但数据格式按 openraft 重定义（已确认全量重建，**不读旧 eraftpb 数据**，无需转换层）。
- [ ] 提供启动时清理旧 CF 数据的初始化路径（首次启动发现旧 eraftpb entry 即清空重建）。
- [ ] 单元测试：用 openraft 官方 `StoreBuilder`（`openraft::testing`）跑存储一致性套件。
- [ ] **此阶段不接入运行时**，仅保证存储 trait 正确。

### 阶段 2：传输层 gRPC 化

- [ ] 在 `master.proto` / `filer.proto` 新增 `RaftAppendEntries` / `RaftVote` / `RaftInstallSnapshot` RPC。
- [ ] 实现 `RaftNetwork` + `RaftNetworkFactory`，复用现有 master 地址表。
- [ ] 旧 `RaftService` 暂时保留（灰度期共存）。

### 阶段 3：Master 节点切换

- [ ] 新增 `powerfs-master/src/raft_v2.rs`，用 `Raft<MasterRaftTypeConfig>` 替换 `RaftNode`。
- [ ] 业务 `propose` 调用点（zone/node 命令、`raft_storage` 写入）改走 `raft.client_write`。
- [ ] `add_peer` / `remove_peer` / `transfer_leader` 改 `change_membership`。
- [ ] 删除 `raft_node.rs` / `raft_server.rs` 旧逻辑（已确认直接替换，不留 feature flag 双轨，回滚靠 git revert 分支）。
- [ ] 跑 `coherence_phase2/3_test` / `coherence_failover_test` / `master_outage_e2e_test` 验证。

### 阶段 4：Filer 多组切换

- [ ] 采用 openraft 官方 `multiraft` crate（已确认），替换 `RaftGroupManager` / `RaftGroup`。先在阶段 0 评估 multiraft alpha API 成熟度，若 API 缺失则临时回退手写多 `Raft` 实例。
- [ ] `shard_store` / `meta_shard_manager` 的 `propose` 点改 `client_write`。
- [ ] 跑 `inode_lease_test` / `integration_test` / `mock_server_test`。

### 阶段 5：清理与移除

- [ ] 删除 `eraftpb` 依赖、旧 `RaftService` proto、`raft-rs` workspace 依赖。
- [ ] 删除 `powerfs-common/src/raft/storage.rs`（`RocksDbRaftStorage`）与 `RaftStorageExt`。
- [ ] 删除 `docs/raft-storage-bugs-fixes.md`（已无意义）。
- [ ] 更新 CI：移除 `protobuf-codec` feature。
- [ ] 更新 docker-compose 测试拓扑。

---

## 6. 待决策点

> 6.1 / 6.2 / 6.3 / 6.4 / 6.5 / 6.6 全部已于 2026-08-12 确认。

### 6.1 openraft RPC 的传输编码

- **选项 A**：openraft 类型开 `serde` feature，gRPC 请求体用 `serde_json` / `bincode` 序列化 `Vec<u8>` 承载。简单，但跨语言不友好（Monitor/CLI 无法直接解析）。
- **选项 B**：手写 proto 映射 openraft 的 `AppendEntriesRequest` / `VoteRequest` 等字段。类型清晰，但 openraft alpha 期字段会变，维护成本高。
- ✅ **已确认**：采用 B（proto 映射），直接复用参考示例 `openraft/examples/raft-kv-memstore-grpc/` 的 `proto/raft.proto` + `pb_impl/` 转换代码（NodeId 由 `uint64` 改 `string` 以适配 powerfs）。阶段 2 落地。

### 6.2 Filer 多组实现

- **选项 A**：openraft `multiraft` crate。原生、少写代码，但 alpha crate 成熟度未知。
- **选项 B**：`RaftGroupManager` 内每分片一个 `Raft<C>` 实例，手动管理。可控，但保留了多组调度的手写逻辑。
- ✅ **已确认**：采用 A（multiraft crate）。阶段 0 先评估其 API 成熟度，若关键 API 缺失则临时回退 B。

### 6.3 版本选择：0.10-alpha vs 0.9 stable

- **选项 A**：直接上 `0.10.0-alpha.33`（trait 拆分 + RT 解耦，架构最干净）。
- **选项 B**：先上 `0.9.x` stable（API 已稳定，但仍是单体 `RaftStorage` trait，迁移收益小），稳定后再升 0.10。
- ✅ **已确认**：采用 A（`0.10.0-alpha.33`），接受 alpha 风险并精确锁版本。

### 6.4 依赖来源

- **选项 A**：crates.io（`openraft = "0.10.0-alpha.33"`）。
- **选项 B**：git（`openraft = { git = "...", tag = "v0.10.0-alpha.33" }`）。
- **选项 C**：vendored path（已克隆的 `openraft/` 目录，`path = "openraft/openraft"`）。
- ✅ **已确认**：采用 A（crates.io）。`openraft` / `openraft-multi` / `openraft-rt-tokio` 均在 crates.io 发布 `0.10.0-alpha.33`，已验证可拉取编译。本地 `openraft/` 克隆仅作参考阅读，已 gitignore，不参与编译（避免嵌套工作区继承冲突）。

### 6.5 灰度与回滚

- **选项 A**：feature flag `legacy-raft` / `openraft-v2` 双轨并存，运行时选。
- **选项 B**：直接替换，靠分支隔离。
- ✅ **已确认**：采用 B（直接替换，分支隔离），不用 feature flag 双轨。回滚靠 git revert。

### 6.6 旧数据兼容

- 现有 RocksDB CF 数据格式是否需要迁移脚本？
- openraft 的 `LogId` / `Membership` 结构与 `eraftpb` 的 `Entry`/`ConfState` 不同，旧日志 entry 能否被新 `RaftLogStorage` 读出？
- ✅ **已确认**：全量重建。放弃旧 eraftpb 日志/快照数据，新 `RaftLogStorage` 首次启动清理旧 CF，从业务层重建元数据（Master 元数据可从 DataNode 心跳重建，Filer 分片元数据从 shard_store 业务数据重建）。不做在线转换层。

---

## 7. 测试策略

1. **存储一致性**：openraft 提供 `openraft::testing::StoreBuilder`，让 `RaftLogStorage` impl 跑官方套件（`Suite`），覆盖 append/truncate/snapshot/compaction。
2. **网络一致性**：openraft `NetworkBuilder` 套件，验证 `RaftNetwork` 实现。
3. **端到端**：复用现有 `coherence_phase0-3_test`、`master_outage_e2e_test`、`inode_lease_test`、`integration_test`，只换底层。
4. **故障注入**：复用 `coherence_failover_test`（leader 切换）、`concurrent_consistency`（并发）。
5. **新增**：openraft 版本的 leader lease / linearizable read 单测（旧代码没有）。
6. **回滚验证**：feature flag 切回 legacy-raft，确认旧路径仍工作。

---

## 8. 工作量预估（不含实施，仅用于排期讨论）

| 阶段 | 模块 | 影响文件数 | 复杂度 |
|---|---|---|---|
| 0 | 依赖与新 crate | 2-3 | 低 |
| 1 | 存储 trait 迁移 | 3 新增 + 测试 | 中 |
| 2 | gRPC 传输 | 2 proto + 2 net_handler | 中 |
| 3 | Master 节点 | 5-8 | 高 |
| 4 | Filer 多组 | 6-10 | 高 |
| 5 | 清理 | 10+ 删除 | 低 |

> 实施顺序建议：0 → 1 → 2 → 3 → (Master 验证通过) → 4 → 5。每阶段独立可验证、可回滚。

---

## 9. 开放问题

1. ✅ 已确认：接受 `0.10.0-alpha.33` alpha 风险（§6.3 → A）。
2. ✅ 已确认：Filer 多组用 multiraft crate（§6.2 → A）。
3. ✅ 已确认：旧数据全量重建，不做在线迁移（§6.6）。
4. ✅ 已确认：不用 feature flag，直接在分支上替换（§6.5 → B）。
5. ✅ 已确认：openraft 依赖走 crates.io（§6.4 → A，已验证 0.10.0-alpha.33 可拉取编译）。
6. ✅ 已确认：Master 先、Filer 后（§6.5 节奏 → Master 先行）。
7. ✅ **阶段 1 完成（2026-08-12）**：`powerfs-raft` 内实现 `RocksLogStore<C>` + `RocksStateMachine<C>`，通过 `openraft::testing::Suite` 官方存储一致性套件（`1 passed; 0 failed`，8.6s）。CF 布局：`raft_log` / `raft_log_meta` / `raft_state_meta` / `raft_state_data`。Normal entry payload 存储留给阶段 3/4 业务特定实现。
8. ✅ **阶段 2 完成（2026-08-12）**：gRPC 传输层落地。新增 `proto/raft.proto`（NodeId=string，Entry.app_data=bytes）+ `pb_impl.rs`（泛型于 `C: RaftTypeConfig` 的 proto ↔ openraft 类型转换）+ `network.rs`（`RaftNetworkV2` 客户端，实现 Vote/AppendEntries/StreamAppend/FullSnapshot RPC）+ `grpc.rs`（`RaftService` 服务端，处理入站 RPC 并转发给 `Raft<C, RocksStateMachine>`）。端到端集成测试 `tests/grpc_cluster.rs` 启动 3 节点集群，验证 Vote/AppendEntries/StreamAppend RPC：领导者选举、成员变更（add_learner + change_membership）、10 条日志复制全部通过（0.43s）。
9. ✅ **阶段 3 完成（2026-08-12）**：Master 节点接入 openraft。`RocksStateMachine::apply()` 增强：存储 Normal entry payload 到 `raft_state_data` CF + 通过 channel 通知 applied index。新增 `raft_v2.rs`（`RaftNodeV2` 封装 `Raft<MasterTypeConfig, RocksStateMachine>`，提供 propose/add_learner/change_membership/transfer_leader/scan_applied_entries API）。`master.rs` 完全替换 `RaftNode` → `RaftNodeV2`：初始化、apply 循环（u64 index → read_applied_entry → 反序列化 RaftCommand）、propose（双重 apply：本地立即 + Raft 持久化）、transfer_leader（async）、get_cluster_info。废弃 TLV raft 消息转发器和 `raft_message_stream` gRPC（openraft 用自己的 RaftService gRPC）。`cargo check`/`clippy`/`fmt` 干净，workspace 全部测试通过（0 failures）。
10. ✅ **阶段 4 完成（2026-08-12）**：Filer 多组接入 openraft。扩展 `raft.proto` 新增 `group_id` 字段支持多组路由（VoteRequest/AppendEntriesRequest/SnapshotRequestMeta）。实现多组服务端路由 `MultiRaftRouter<C>` + `MultiRaftServiceImpl<C>`（按 group_id 分发入站 RPC）。实现多组客户端网络层 `MultiGroupRouter` + `MultiNetworkFactory`（共享 gRPC 连接池，按 group_id 携带出站 RPC）。创建 `RaftGroupManagerV2`（替代旧 `RaftGroupManager`，管理每 shard 的 `Raft<FilerTypeConfig, RocksStateMachine>` 实例 + 共享 gRPC 服务 + apply 通知 channel）。适配 `MetaShardManager`（create_shard 使用新 apply 循环：apply_rx → read_applied_entry → ShardCommand::deserialize → shard_store.apply_command）、`ShardScheduler`、`main.rs`。废弃 `net_handler.rs` 的 TLV `handle_raft_message`。`cargo check`/`clippy`/`fmt` 干净，workspace 全部测试通过（0 failures）。
11. ✅ **阶段 5 完成（2026-08-12）**：清理旧 raft-rs 代码。删除旧文件：`powerfs-filer/src/raft_group_manager.rs`、`powerfs-master/src/raft_node.rs`、`raft_storage.rs`、`raft_server.rs`、`raft_client.rs`、`powerfs-common/src/raft/mod.rs`、`powerfs-common/src/raft/storage.rs`。共享类型（Peer/ApplyEntry/RaftCommand/RaftVolumeShortInfo）迁移到 [raft_group_manager_v2.rs](file:///home/portion/powerfs/powerfs-filer/src/raft_group_manager_v2.rs) 和 [raft_v2.rs](file:///home/portion/powerfs/powerfs-master/src/raft_v2.rs)。移除所有 Cargo.toml 中的 `raft` 和 `protobuf` 依赖（workspace + powerfs-master + powerfs-filer + powerfs-common）。修复 [powerfs-common/src/error.rs](file:///home/portion/powerfs/powerfs-common/src/error.rs) 残留的 `raft` 导入和 `Raft(Box<raft::Error>)` 错误变体。`cargo build --workspace` 通过（仅 3 个无关的 dead code/unused variable 警告），`cargo test --workspace --lib --bins --tests` 全部通过（0 failures）。

---

## 10. 实施进度

| 阶段 | 状态 | 完成日期 | 备注 |
|---|---|---|---|
| 0 | ✅ 完成 | 2026-08-12 | 依赖接入 + TypeConfig + crate 骨架 |
| 1 | ✅ 完成 | 2026-08-12 | RocksLogStore + RocksStateMachine + Suite 全绿 |
| 2 | ✅ 完成 | 2026-08-12 | gRPC 传输层（proto + pb_impl + network + grpc）+ 3 节点端到端测试通过 |
| 3 | ✅ 完成 | 2026-08-12 | Master 接入（RocksStateMachine apply 增强 + raft_v2.rs + master.rs 全替换）|
| 4 | ✅ 完成 | 2026-08-12 | Filer 多组接入（raft.proto group_id + MultiRaftRouter/MultiRaftServiceImpl + MultiGroupRouter/MultiNetworkFactory + RaftGroupManagerV2 + MetaShardManager/ShardScheduler/main.rs 适配 + TLV RaftMessage 废弃）|
| 5 | ✅ 完成 | 2026-08-12 | 清理旧 raft-rs 代码（删除 7 个旧文件 + 移除 raft/protobuf 依赖 + 修复 error.rs 残留引用 + 类型迁移到 v2 文件）|

---

## 附：参考资源

- openraft 源码：[openraft/](file:///home/portion/powerfs/openraft/)（已克隆，gitignored）
- rocksdb 示例：[openraft/examples/raft-kv-rocksdb/src/store.rs](file:///home/portion/powerfs/openraft/examples/raft-kv-rocksdb/src/store.rs)
- gRPC 示例：[openraft/examples/raft-kv-memstore-grpc/](file:///home/portion/powerfs/openraft/examples/raft-kv-memstore-grpc/)
- multiraft：[openraft/multiraft/](file:///home/portion/powerfs/openraft/multiraft/)
- openraft 升级文档：[openraft/openraft/src/docs/upgrade_guide/upgrade.md](file:///home/portion/powerfs/openraft/openraft/src/docs/upgrade_guide/upgrade.md)
- 当前实现（v2）：[raft_v2.rs](file:///home/portion/powerfs/powerfs-master/src/raft_v2.rs)（Master）/ [raft_group_manager_v2.rs](file:///home/portion/powerfs/powerfs-filer/src/raft_group_manager_v2.rs)（Filer）/ [powerfs-raft/src/store/](file:///home/portion/powerfs/powerfs-raft/src/store/)（存储层）/ [powerfs-raft/src/grpc.rs](file:///home/portion/powerfs/powerfs-raft/src/grpc.rs)（gRPC 服务端）/ [powerfs-raft/src/network.rs](file:///home/portion/powerfs/powerfs-raft/src/network.rs)（gRPC 客户端）/ [powerfs-raft/src/multi.rs](file:///home/portion/powerfs/powerfs-raft/src/multi.rs) + [multi_network.rs](file:///home/portion/powerfs/powerfs-raft/src/multi_network.rs)（多组路由）
