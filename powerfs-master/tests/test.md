# PowerFS 全面测试方案

## 概述

本文档对照 [设计文档](/workspace/design.md) 中的 5 个阶段规划，基于对全部 **69 个源文件** 的逐方法审计，制定完整的分层测试方案。当前项目处于 **Phase 1**（Rust 重构 SeaweedFS 核心基座）。

### 当前测试覆盖现状

| Crate | 公开方法数 | 已测试 | 未测试 | 覆盖率 |
|-------|-----------|--------|--------|--------|
| powerfs-common | 78 | 76 | 2 | ~97% |
| powerfs-core | 44 | 43 | 1 (PersistentIndex) | ~98% |
| powerfs-master | 104 | ~10 | ~94 | ~10% |
| powerfs-volume | 16 | 0 | 16 | **0%** |
| powerfs-fuse | 19 | 0 | 19 | **0%** |
| powerfs-server | 5 子命令 | 0 | 5 | **0%** |
| powerfs-cli | 24 | 0 | 24 | **0%** |
| **总计** | **~290** | **~129** | **~161** | **~44%** |

> 注：`powerfs-common` 和 `powerfs-core` 虽覆盖率较高，但 `PersistentIndex`（7 方法）完全未测试，错误 From 实现（8 个）未测试，`RocksDbBackend::compact/flush/open/open_with_options` 未测试。

---

## 一、测试金字塔总览

```
                    ┌──────────────────┐
                    │    E2E 测试       │  全链路：启停集群 → 挂载 → 读写 → 卸载
                    │   (~25 用例)      │
                    ├──────────────────┤
                    │    集成测试       │  gRPC 端到端、多节点 Raft、FUSE 文件操作
                    │   (~90 用例)      │
                    ├──────────────────┤
                    │    单元测试       │  每个 public 函数/方法、每个错误路径
                    │   (~350+ 用例)    │
                    └──────────────────┘
```

### 测试策略原则

| 维度 | 策略 |
|------|------|
| 覆盖率目标 | 行覆盖率 > 85%，分支覆盖率 > 75% |
| 测试独立性 | 每个测试独立创建 tempdir，测试后自动清理 |
| 确定性 | 不依赖 wall-clock 时间；通过 `tokio::time::pause` 控制异步时间 |
| 分层执行 | 单元测试 < 5s / 集成测试 < 30s / E2E < 120s |
| CI 集成 | 每次 push/PR 触发全量 `cargo test --all`；main 分支合并触发 benchmark |

---

## 二、各 Crate 逐方法测试方案

### 2.1 powerfs-common（基础类型与工具库）

**当前状态**：5 个测试文件，约 157 个测试用例，覆盖率 ~97%。
**残留缺口**：8 个 `From` trait 实现 + `RocksDbBackend` 部分方法未测试。

#### 2.1.1 error.rs — 缺失的 From 实现测试（8 个缺口）

| # | 缺失项 | 测试用例 | 说明 |
|---|--------|----------|------|
| 1 | `From<serde_json::Error>` | `test_error_from_serde_json` | 构造 serde_json::Error，验证转换为 `PowerFsError::SerdeJson` |
| 2 | `From<tonic::transport::Error>` | `test_error_from_tonic_transport` | 构造 transport::Error，验证转换为 `PowerFsError::TonicTransport` |
| 3 | `From<tonic::Status>` | `test_error_from_tonic_status` | 构造 tonic::Status，验证转换为 `PowerFsError::TonicStatus` |
| 4 | `From<prost::DecodeError>` | `test_error_from_prost_decode` | 构造 DecodeError，验证转换为 `PowerFsError::ProstDecode` |
| 5 | `From<prost::EncodeError>` | `test_error_from_prost_encode` | 构造 EncodeError，验证转换为 `PowerFsError::ProstEncode` |
| 6 | `From<uuid::Error>` | `test_error_from_uuid_parse` | 构造 uuid::Error，验证转换为 `PowerFsError::UuidParse` |
| 7 | `From<std::net::AddrParseError>` | `test_error_from_addr_parse` | 构造 AddrParseError，验证转换为 `PowerFsError::AddrParse` |
| 8 | `From<raft::Error>` | `test_error_from_raft` | 构造 raft::Error，验证转换为 `PowerFsError::Raft` |

#### 2.1.2 storage/rocksdb.rs — 缺失方法测试（4 个缺口）

| # | 缺失项 | 测试用例 | 说明 |
|---|--------|----------|------|
| 1 | `RocksDbBackend::open()` | `test_rocksdb_open_default` | 默认参数打开，验证可读写 |
| 2 | `RocksDbBackend::open_with_options()` | `test_rocksdb_open_with_options` | 自定义 Options 打开 |
| 3 | `RocksDbBackend::compact()` | `test_rocksdb_compact` | 写入数据后 compact，验证数据完整性 |
| 4 | `RocksDbBackend::flush()` | `test_rocksdb_flush` | 写入后 flush，关闭重开验证持久化 |

#### 2.1.3 storage.rs — StorageBackend trait 方法

| # | 缺失项 | 测试用例 | 说明 |
|---|--------|----------|------|
| 1 | `StorageBackend::len()` | `test_rocksdb_len` | 验证 len 返回正确键值对数量 |
| 2 | `StorageBackend::is_empty()` | `test_rocksdb_is_empty` | 空数据库返回 true，插入后返回 false |

#### 2.1.4 types.rs — 边界场景补充

| # | 缺失项 | 测试用例 | 说明 |
|---|--------|----------|------|
| 1 | `ReplicaPlacement` 边界 | `test_replica_placement_max_copies` | 副本数 > 999 的解析 |
| 2 | `VolumeState` 状态转换 | `test_volume_state_transitions` | 验证各状态间合法/非法转换 |
| 3 | `Topology` 大规模 | `test_topology_large_scale` | 100+ 节点增删查性能 |
| 4 | `DataNodeInfo::url()` | `test_data_node_url_with_https` | https + 自定义端口组合 |
| 5 | `Fid` 非法格式 | `test_fid_from_string_malformed` | 空字符串、非数字、缺失字段 |

#### 2.1.5 新增测试文件

| 文件 | 用例数 | 内容 |
|------|--------|------|
| `serde_test.rs` | ~12 | 所有 `#[derive(Serialize, Deserialize)]` 类型的 JSON 往返序列化/反序列化：`VolumeInfo`、`DataNodeInfo`、`Topology`、`RaftConfig`、`ClusterConfig`、`NeedleInfo`、`FileMetadata` |

---

### 2.2 powerfs-core（核心存储引擎）

**当前状态**：4 个测试文件，约 74 个测试用例，覆盖率 ~98%。
**残留缺口**：`PersistentIndex` 全部 7 个方法零覆盖。

#### 2.2.1 PersistentIndex 测试（最关键的缺口）

| # | 缺失项 | 测试用例 | 说明 |
|---|--------|----------|------|
| 1 | `PersistentIndex::new()` | `test_persistent_index_new` | 创建索引，验证 sled 数据库初始化 |
| 2 | `PersistentIndex::key_from_id()` | `test_persistent_index_key_format` | 验证 key 格式一致性 |
| 3 | `PersistentIndex::insert()` | `test_persistent_index_insert_and_get` | 插入后查询 |
| 4 | `PersistentIndex::get()` | `test_persistent_index_get_nonexistent` | 查询不存在的 key |
| 5 | `PersistentIndex::remove()` | `test_persistent_index_remove` | 删除后查询返回 None |
| 6 | `PersistentIndex::contains()` | `test_persistent_index_contains` | 存在/不存在 |
| 7 | `PersistentIndex::len()` | `test_persistent_index_len` | 空索引 len=0，插入后递增 |
| 8 | 持久化恢复 | `test_persistent_index_reopen` | 写入数据 → 关闭 → 重新打开 → 数据可读 |
| 9 | LRU 缓存一致性 | `test_persistent_index_lru_consistency` | LRU 缓存与 sled 数据一致 |
| 10 | 并发安全 | `test_persistent_index_concurrent` | 多线程并发读写 |

#### 2.2.2 needle.rs — 边界补充

| # | 测试用例 | 说明 |
|---|----------|------|
| 1 | `test_needle_corruption_detect` | 随机翻转 1 bit，校验和验证失败 |
| 2 | `test_needle_max_data_size` | 最大允许数据大小的 Needle |
| 3 | `test_needle_from_bytes_truncated` | 截断的字节数组 |
| 4 | `test_needle_from_bytes_empty` | 空字节数组 |
| 5 | `test_needle_from_bytes_invalid_size_field` | 篡改 size 字段导致越界 |

#### 2.2.3 volume.rs — 并发与异常补充

| # | 测试用例 | 说明 |
|---|----------|------|
| 1 | `test_volume_concurrent_write_read` | 4 线程同时写入不同 file_key，验证数据隔离 |
| 2 | `test_volume_write_after_full` | 写满卷后继续写入，应返回错误 |
| 3 | `test_volume_write_after_read_only` | set_read_only() 后写入拒绝 |
| 4 | `test_volume_write_after_deleting` | set_deleting() 后所有操作拒绝 |
| 5 | `test_volume_reopen_data_integrity` | 写入 → 关闭 → 重新打开 Volume → 读取验证 |
| 6 | `test_volume_file_size_boundary` | 创建接近磁盘大小的卷，边界写入 |

#### 2.2.4 storage.rs (StorageManager) — 边界补充

| # | 测试用例 | 说明 |
|---|----------|------|
| 1 | `test_storage_manager_many_volumes` | 创建 100 个卷，验证性能和正确性 |
| 2 | `test_find_available_all_full` | 所有卷满时返回 None |
| 3 | `test_concurrent_create_volume` | 并发创建卷，ID 不冲突 |
| 4 | `test_load_volumes_corrupted_dir` | 目录含非法子目录时的恢复行为 |

#### 2.2.5 新增测试文件

| 文件 | 用例数 | 内容 |
|------|--------|------|
| `concurrent_test.rs` | ~6 | 多线程混合读写 stress test（持续 30s）、死锁检测、4 读 + 4 写线程竞态 |

---

### 2.3 powerfs-master（Master 节点与 Raft 共识）— **覆盖率仅 10%**

**当前状态**：2 个集成测试文件，仅测试了 Raft 集群的基础选举/复制/快照/故障转移。MasterNode 的 37 个 public 方法、RaftServiceServer 的 13 个方法、MasterGrpcServer 的 10 个方法全部未测试。

#### 2.3.1 单元测试 — MasterNode 核心方法（master.rs）

##### 集群拓扑管理

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 1 | `test_master_new` | `new()` | 创建 MasterNode，验证初始状态 |
| 2 | `test_master_id_and_address` | `id()`, `address()` | 验证 ID 和地址 |
| 3 | `test_master_leader_state` | `is_leader()`, `get_leader()`, `set_leader()` | Leader 状态 get/set |
| 4 | `test_master_add_node` | `add_node()` | 添加新节点到拓扑 |
| 5 | `test_master_add_node_duplicate` | `add_node()` | 重复添加同一节点 |
| 6 | `test_master_remove_node` | `remove_node()` | 移除节点 |
| 7 | `test_master_remove_node_nonexistent` | `remove_node()` | 移除不存在的节点 |
| 8 | `test_master_list_nodes` | `list_nodes()` | 列出所有节点 |
| 9 | `test_master_get_node` | `get_node()` | 查询单个节点 |
| 10 | `test_master_get_node_nonexistent` | `get_node()` | 查询不存在的节点 |

##### 卷管理

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 11 | `test_master_assign_volume_basic` | `assign_volume()` | 基础卷分配 |
| 12 | `test_master_assign_volume_with_collection` | `assign_volume()` | 指定 collection |
| 13 | `test_master_assign_volume_with_replication` | `assign_volume()` | 指定 replica_placement |
| 14 | `test_master_assign_volume_with_disk_type` | `assign_volume()` | 指定 disk_type |
| 15 | `test_master_assign_volume_no_capacity` | `assign_volume()` | 所有节点满载时分配失败 |
| 16 | `test_master_assign_volume_concurrent` | `assign_volume()` | 并发多客户端分配不重复 |
| 17 | `test_master_get_volume` | `get_volume()` | 查询已分配卷 |
| 18 | `test_master_get_volume_nonexistent` | `get_volume()` | 查询不存在的卷 |
| 19 | `test_master_list_volumes` | `list_volumes()` | 列出所有卷 |
| 20 | `test_master_update_volume_state` | `update_volume_state()` | 更新卷状态（Available→ReadOnly） |
| 21 | `test_master_update_volume_state_invalid` | `update_volume_state()` | 非法状态转换被拒绝 |
| 22 | `test_master_get_node_volumes` | `get_node_volumes()` | 获取节点上所有卷 |

##### 命令提议与应用

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 23 | `test_master_propose_command` | `propose_command()` | 通过 Raft 提议命令 |
| 24 | `test_master_apply_add_node` | `apply_command()` → `apply_add_node()` | 应用 AddNode 命令 |
| 25 | `test_master_apply_remove_node` | `apply_command()` → `apply_remove_node()` | 应用 RemoveNode 命令 |
| 26 | `test_master_apply_assign_volume` | `apply_command()` → `apply_assign_volume()` | 应用 AssignVolume 命令 |
| 27 | `test_master_apply_update_volume_state` | `apply_command()` → `apply_update_volume_state()` | 应用 UpdateVolumeState 命令 |
| 28 | `test_master_apply_update_node_volumes` | `apply_command()` → `apply_update_node_volumes()` | 应用 UpdateNodeVolumes 命令 |
| 29 | `test_master_apply_heartbeat` | `apply_command()` → `apply_heartbeat()` | 应用 Heartbeat 命令 |

##### 心跳处理

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 30 | `test_master_handle_heartbeat_normal` | `handle_heartbeat()` | 正常心跳更新节点信息 |
| 31 | `test_master_handle_heartbeat_new_node` | `handle_heartbeat()` | 新节点首次心跳注册 |
| 32 | `test_master_handle_heartbeat_timeout` | `handle_heartbeat()` | 心跳超时标记节点离线 |

##### 客户端管理

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 33 | `test_master_add_remove_client` | `add_client()`, `remove_client()` | 添加/移除客户端 |
| 34 | `test_master_lookup_volume` | `lookup_volume()` | 查找卷位置 |
| 35 | `test_master_lookup_volume_nonexistent` | `lookup_volume()` | 查找不存在的卷 |

##### 机架感知

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 36 | `test_master_select_nodes_by_rack` | `select_nodes_by_rack()` | 机架感知节点选择 |
| 37 | `test_master_select_nodes_cross_dc` | `select_nodes_by_rack()` | 跨数据中心节点选择 |
| 38 | `test_master_volume_layout_key` | `get_volume_layout_key()` | 卷布局键生成 |

##### ClientManager

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 39 | `test_client_manager_new` | `ClientManager::new()` | 创建 |
| 40 | `test_client_manager_add_remove` | `add_client()`, `remove_client()` | 添加/移除客户端 |
| 41 | `test_client_manager_broadcast` | `broadcast()` | 广播消息到所有客户端 |

#### 2.3.2 单元测试 — RaftNode 缺失方法（raft_node.rs）

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 1 | `test_raft_node_new_with_config` | `new_with_config()` | 自定义配置创建 |
| 2 | `test_raft_node_is_follower` | `is_follower()` | 初始为 Follower |
| 3 | `test_raft_node_is_candidate` | `is_candidate()` | 选举期间为 Candidate |
| 4 | `test_raft_node_term` | `term()` | Term 查询 |
| 5 | `test_raft_node_leader_id` | `leader_id()` | Leader ID 查询 |
| 6 | `test_raft_node_commit_index` | `commit_index()` | Commit 索引 |
| 7 | `test_raft_node_last_applied_index` | `last_applied_index()` | 已应用索引 |
| 8 | `test_raft_node_get_peers` | `get_peers()` | Peer 列表 |
| 9 | `test_raft_node_get_peer_address` | `get_peer_address()` | Peer 地址 |
| 10 | `test_raft_node_add_peer` | `add_peer()` | 添加 Peer |
| 11 | `test_raft_node_remove_peer` | `remove_peer()` | 移除 Peer |
| 12 | `test_raft_node_transfer_leader` | `transfer_leader()` | Leader 转移 |
| 13 | `test_raft_node_send_message` | `send_message()` | 发送 Raft 消息 |
| 14 | `test_raft_node_handle_propose` | `handle_propose()` | 处理提案请求 |
| 15 | `test_raft_node_take_apply_rx` | `take_apply_rx()` | 获取 apply 通道 |
| 16 | `test_raft_node_process_ready` | `process_ready()` | 处理就绪状态 |

#### 2.3.3 单元测试 — RocksDbStorage 缺失方法（raft_storage.rs）

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 1 | `test_raft_storage_new_with_single_node` | `new_with_single_node()` | 单节点存储初始化 |
| 2 | `test_raft_storage_save_load_state` | `save_state()`, `load_state()` | 持久化状态保存/加载往返 |
| 3 | `test_raft_storage_apply_snapshot` | `apply_snapshot()` | 快照应用 |
| 4 | `test_raft_storage_compact_log` | `compact_log()` | 日志压缩后数据完整性 |
| 5 | `test_raft_storage_entries` | `Storage::entries()` | 日志条目查询 |
| 6 | `test_raft_storage_term` | `Storage::term()` | Term 查询 |
| 7 | `test_raft_storage_first_index` | `Storage::first_index()` | 首索引 |
| 8 | `test_raft_storage_last_index` | `Storage::last_index()` | 末索引 |
| 9 | `test_raft_storage_snapshot` | `Storage::snapshot()` | 快照查询 |
| 10 | `test_raft_command_deserialize` | `RaftCommand::deserialize()` | 反序列化各变体 |
| 11 | `test_raft_command_roundtrip` | `serialize()` + `deserialize()` | 序列化往返（6 种命令变体） |

#### 2.3.4 集成测试 — Raft 共识层扩展

扩展现有 `raft_integration_test.rs` 和 `cluster.rs`：

| # | 测试用例 | 分类 | 说明 |
|---|----------|------|------|
| 1 | `test_leader_downgrade_on_higher_term` | Leader 选举 | Leader 收到更高 term 的心跳自动降级 |
| 2 | `test_single_node_cluster_stable` | Leader 选举 | 单节点集群稳定运行 |
| 3 | `test_seven_node_cluster_election` | Leader 选举 | 7 节点集群选举 |
| 4 | `test_large_log_replication` | 日志复制 | 10MB 大日志条目复制 |
| 5 | `test_out_of_order_log_rejection` | 日志复制 | 乱序日志被拒绝 |
| 6 | `test_log_conflict_resolution` | 日志复制 | Term 不一致时日志冲突解决 |
| 7 | `test_follower_lag_catch_up` | 日志复制 | Follower 落后多轮日志追赶 |
| 8 | `test_log_continuity_after_leader_change` | 日志复制 | Leader 变更后日志连续 |
| 9 | `test_snapshot_install_to_lagging_follower` | 快照 | 快照安装到落后 Follower |
| 10 | `test_snapshot_concurrent_propose` | 快照 | 快照创建期间并发 propose |
| 11 | `test_compact_after_snapshot` | 快照 | Compact 后日志索引正确 |
| 12 | `test_single_node_join` | 成员变更 | 单节点加入集群 |
| 13 | `test_single_node_remove` | 成员变更 | 单节点移除 |
| 14 | `test_batch_membership_change` | 成员变更 | 批量成员变更 |
| 15 | `test_replication_during_membership_change` | 成员变更 | 成员变更期间日志复制 |
| 16 | `test_new_node_snapshot_join` | 成员变更 | 新节点通过快照安装加入 |
| 17 | `test_remove_leader_self` | 成员变更 | 移除 Leader 自身后重新选举 |
| 18 | `test_symmetric_partition_recovery` | 容错 | 对称网络分区恢复 |
| 19 | `test_brain_split_prevention` | 容错 | 分区后不产生双 Leader |
| 20 | `test_crash_restart_data_recovery` | 容错 | 节点 crash 后重启数据恢复 |
| 21 | `test_slow_follower_no_drag` | 容错 | 慢 Follower 不拖慢整体提交 |

#### 2.3.5 集成测试 — gRPC 服务层

##### MasterService（server.rs）

| # | 测试用例 | RPC | 说明 |
|---|----------|-----|------|
| 1 | `test_grpc_send_heartbeat_normal` | `SendHeartbeat` | 正常心跳双向流 |
| 2 | `test_grpc_send_heartbeat_new_node` | `SendHeartbeat` | 新节点首次心跳 |
| 3 | `test_grpc_lookup_volume_found` | `LookupVolume` | 查找存在的卷 |
| 4 | `test_grpc_lookup_volume_not_found` | `LookupVolume` | 查找不存在的卷 |
| 5 | `test_grpc_assign_basic` | `Assign` | 分配新 FID |
| 6 | `test_grpc_assign_no_capacity` | `Assign` | 无容量时分配失败 |
| 7 | `test_grpc_volume_list` | `VolumeList` | 列出所有卷 |
| 8 | `test_grpc_keep_connected` | `KeepConnected` | 双向流连接保持 |
| 9 | `test_grpc_ping` | `Ping` | Ping 延迟测试 |
| 10 | `test_grpc_volume_grow` | `VolumeGrow` | 批量卷增长 |
| 11 | `test_grpc_assign_leader_forward` | `Assign` | 非 Leader 节点转发 |

##### RaftService（raft_server.rs）

| # | 测试用例 | RPC | 说明 |
|---|----------|-----|------|
| 12 | `test_grpc_propose_and_apply` | `Propose` | Propose 后确认 apply |
| 13 | `test_grpc_raft_message_stream` | `RaftMessageStream` | 双向 Raft 消息流 |
| 14 | `test_grpc_get_cluster_info` | `GetClusterInfo` | 集群信息查询 |
| 15 | `test_grpc_add_node` | `AddNode` | 添加 Raft 节点 |
| 16 | `test_grpc_remove_node` | `RemoveNode` | 移除 Raft 节点 |
| 17 | `test_grpc_transfer_leader` | `TransferLeader` | Leader 转移后确认 |

##### RaftGrpcClient

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 18 | `test_raft_grpc_client_connect` | `RaftGrpcClient::connect()` | 连接对端 Raft 节点 |
| 19 | `test_raft_grpc_client_send_message` | `RaftGrpcClient::send_message()` | 发送 Raft 消息 |
| 20 | `test_raft_grpc_client_address` | `RaftGrpcClient::address()` | 地址查询 |

---

### 2.4 powerfs-volume（Volume 数据服务器）— **覆盖率 0%**

**当前状态**：无任何测试。16 个 public 方法全部未覆盖。

#### 2.4.1 单元测试 — VolumeServer（server.rs）

| # | 测试用例 | RPC | 说明 |
|---|----------|-----|------|
| 1 | `test_volume_server_create_volume` | `CreateVolume` | 正常创建卷 |
| 2 | `test_volume_server_create_volume_duplicate` | `CreateVolume` | 重复创建 |
| 3 | `test_volume_server_create_volume_oversized` | `CreateVolume` | 超大卷 |
| 4 | `test_volume_server_create_volume_zero_size` | `CreateVolume` | 零大小卷拒绝 |
| 5 | `test_volume_server_delete_volume` | `DeleteVolume` | 正常删除 |
| 6 | `test_volume_server_delete_volume_nonexistent` | `DeleteVolume` | 删除不存在的卷 |
| 7 | `test_volume_server_delete_volume_with_data` | `DeleteVolume` | 删除含数据的卷 |
| 8 | `test_volume_server_list_volumes` | `ListVolumes` | 空列表/多卷列表 |
| 9 | `test_volume_server_get_node_info` | `GetNodeInfo` | 节点信息查询 |
| 10 | `test_volume_server_write_needle` | `WriteNeedle` | 正常写入 |
| 11 | `test_volume_server_write_needle_volume_nonexistent` | `WriteNeedle` | 卷不存在 |
| 12 | `test_volume_server_write_needle_volume_read_only` | `WriteNeedle` | 卷只读时拒绝写入 |
| 13 | `test_volume_server_write_needle_volume_full` | `WriteNeedle` | 卷满时拒绝写入 |
| 14 | `test_volume_server_write_needle_large_data` | `WriteNeedle` | 大数据（10MB）写入 |
| 15 | `test_volume_server_read_needle` | `ReadNeedle` | 正常读取 |
| 16 | `test_volume_server_read_needle_nonexistent` | `ReadNeedle` | 读取不存在 |
| 17 | `test_volume_server_read_needle_after_delete` | `ReadNeedle` | 删除后读取 |
| 18 | `test_volume_server_delete_needle` | `DeleteNeedle` | 正常删除 |
| 19 | `test_volume_server_delete_needle_nonexistent` | `DeleteNeedle` | 删除不存在 |
| 20 | `test_volume_server_delete_needle_twice` | `DeleteNeedle` | 二次删除幂等 |

#### 2.4.2 单元测试 — MasterClient（master_client.rs）

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 21 | `test_master_client_new` | `MasterClient::new()` | 创建客户端 |
| 22 | `test_master_client_start_heartbeat` | `start_heartbeat()` | 启动心跳 |
| 23 | `test_master_client_send_heartbeat` | `send_heartbeat()` | 发送心跳消息 |
| 24 | `test_master_client_grow` | `grow()` | 请求卷增长 |
| 25 | `test_master_client_heartbeat_reconnect` | `start_heartbeat()` | 心跳断线重连 |

#### 2.4.3 集成测试

| # | 测试用例 | 说明 |
|---|----------|------|
| 26 | `test_volume_integration_full_flow` | 启动 VolumeServer → 连接 Master → 心跳注册 → 分配卷 → 写入读取 → 删除卷 |
| 27 | `test_volume_concurrent_write_read` | 并发写入不同卷 |
| 28 | `test_volume_concurrent_same_volume` | 并发读写同一卷 |

---

### 2.5 powerfs-fuse（FUSE 客户端）— **覆盖率 0%**

**当前状态**：无任何测试。19 个方法全部未覆盖。且 `mount()` 实现不完整（只创建目录未实际启动 FUSE 会话）。

#### 2.5.1 前置修复项

- `FuseClient::mount()` 需调用 `fuse_backend_rs` 的实际挂载 API（`FuseSession::new().mount()`）
- `FuseClient::unmount()` 需要正确的 umount 流程

#### 2.5.2 单元测试 — PowerFsFs 内部方法

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 1 | `test_fuse_fs_new` | `PowerFsFs::new()` | 创建文件系统实例 |
| 2 | `test_fuse_allocate_inode` | `allocate_inode()` | 分配 inode 递增 |
| 3 | `test_fuse_path_to_inode` | `path_to_inode()` | 路径→inode 映射 |
| 4 | `test_fuse_path_to_inode_nonexistent` | `path_to_inode()` | 不存在路径 |
| 5 | `test_fuse_inode_to_metadata` | `inode_to_metadata()` | inode→元数据映射 |
| 6 | `test_fuse_create_dir_attr` | `create_dir_attr()` | 目录属性创建 |
| 7 | `test_fuse_create_file_attr` | `create_file_attr()` | 文件属性创建 |
| 8 | `test_fuse_create_new_file_attr` | `create_new_file_attr()` | 新文件属性创建 |

#### 2.5.3 单元测试 — FileSystem trait 实现

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 9 | `test_fuse_lookup_root` | `lookup()` | 查找根目录 |
| 10 | `test_fuse_lookup_existing` | `lookup()` | 查找存在文件 |
| 11 | `test_fuse_lookup_nonexistent` | `lookup()` | 查找不存在文件 |
| 12 | `test_fuse_getattr_file` | `getattr()` | 文件属性 |
| 13 | `test_fuse_getattr_dir` | `getattr()` | 目录属性 |
| 14 | `test_fuse_getattr_nonexistent` | `getattr()` | 不存在 inode |
| 15 | `test_fuse_create_file` | `create()` | 创建新文件 |
| 16 | `test_fuse_create_existing` | `create()` | 创建已存在文件 |
| 17 | `test_fuse_open_read` | `open()` | 以读模式打开 |
| 18 | `test_fuse_open_write` | `open()` | 以写模式打开 |
| 19 | `test_fuse_open_nonexistent` | `open()` | 打开不存在文件 |
| 20 | `test_fuse_read_empty_file` | `read()` | 读空文件 |
| 21 | `test_fuse_read_with_content` | `read()` | 读有内容文件 |
| 22 | `test_fuse_read_beyond_eof` | `read()` | 读超出文件范围 |
| 23 | `test_fuse_write_create` | `write()` | 写入创建 |
| 24 | `test_fuse_write_append` | `write()` | 追加写入 |
| 25 | `test_fuse_release` | `release()` | 释放文件句柄 |
| 26 | `test_fuse_unlink` | `unlink()` | 删除文件 |
| 27 | `test_fuse_unlink_nonexistent` | `unlink()` | 删除不存在文件 |
| 28 | `test_fuse_readdir_empty` | `readdir()` | 读空目录 |
| 29 | `test_fuse_readdir_with_files` | `readdir()` | 读含文件目录 |
| 30 | `test_fuse_readdir_nested` | `readdir()` | 读嵌套目录 |

#### 2.5.4 集成测试 — 挂载/卸载

| # | 测试用例 | 说明 |
|---|----------|------|
| 31 | `test_fuse_mount_empty_dir` | 挂载到空目录成功 |
| 32 | `test_fuse_mount_unmount` | 挂载→卸载后目录恢复 |
| 33 | `test_fuse_integration_write_read` | 挂载→创建文件→写入→读取→卸载 |
| 34 | `test_fuse_integration_large_file` | 大文件（100MB）读写完整性 |

---

### 2.6 powerfs-server（统一入口）— **覆盖率 0%**

| # | 测试用例 | 说明 |
|---|----------|------|
| 1 | `test_server_cli_master_defaults` | Master 子命令默认参数 |
| 2 | `test_server_cli_master_custom` | Master 自定义端口和目录 |
| 3 | `test_server_cli_volume` | Volume 子命令解析 |
| 4 | `test_server_cli_filer` | Filer 子命令解析 |
| 5 | `test_server_cli_fuse` | Fuse 子命令解析 |
| 6 | `test_server_cli_mount` | Mount 子命令解析 |
| 7 | `test_server_cli_help` | `--help` 输出完整性 |
| 8 | `test_server_cli_version` | `--version` 输出 |
| 9 | `test_server_cli_invalid_command` | 非法命令报错 |
| 10 | `test_server_cli_missing_required_arg` | 必需参数缺失报错 |
| 11 | `test_server_startup_master` | 启动 Master 进程 |
| 12 | `test_server_startup_volume` | 启动 Volume 进程连接 Master |
| 13 | `test_server_startup_graceful_shutdown` | 启动后 SIGTERM 优雅关闭 |

---

### 2.7 powerfs-cli（CLI 管理工具）— **覆盖率 0%**

#### 2.7.1 gRPC 客户端测试

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 1 | `test_cli_master_client_connect` | `MasterClient::connect()` | 连接 Master |
| 2 | `test_cli_master_client_connect_fail` | `MasterClient::connect()` | 连接失败处理 |
| 3 | `test_cli_master_client_service` | `MasterClient::service()` | 获取 MasterService 客户端 |
| 4 | `test_cli_master_client_raft_service` | `MasterClient::raft_service()` | 获取 RaftService 客户端 |
| 5 | `test_cli_volume_client_connect` | `VolumeServerClient::connect()` | 连接 Volume 服务器 |
| 6 | `test_cli_volume_client_create_volume` | `VolumeServerClient::create_volume()` | 创建卷 |
| 7 | `test_cli_volume_client_write_read_needle` | `write_needle()` + `read_needle()` | 写入→读取往返 |
| 8 | `test_cli_volume_client_delete_needle` | `delete_needle()` | 删除 Needle |

#### 2.7.2 CLI 命令集成测试

| # | 测试用例 | 命令 | 说明 |
|---|----------|------|------|
| 9 | `test_cli_status` | `status` | 显示集群状态 |
| 10 | `test_cli_status_detailed` | `status --detailed` | 详细状态 |
| 11 | `test_cli_assign` | `assign` | 分配 FID |
| 12 | `test_cli_assign_with_replication` | `assign --replication 002` | 指定副本策略分配 |
| 13 | `test_cli_lookup_by_volume_id` | `lookup --volume-id 1` | 按 volume_id 查找 |
| 14 | `test_cli_lookup_by_fid` | `lookup --fid 1,0,0` | 按 FID 查找 |
| 15 | `test_cli_lookup_nonexistent` | `lookup --volume-id 999` | 查找不存在 |
| 16 | `test_cli_volume_list` | `volume-list` | 列出所有卷 |
| 17 | `test_cli_heartbeat` | `heartbeat` | 模拟心跳 |
| 18 | `test_cli_grow` | `grow` | 请求卷增长 |
| 19 | `test_cli_write_read_consistency` | `write` + `read` | 写入→读取数据一致 |
| 20 | `test_cli_read_nonexistent` | `read` | 读取不存在文件 |
| 21 | `test_cli_cluster_add` | `cluster-add` | 添加集群节点 |
| 22 | `test_cli_cluster_remove` | `cluster-remove` | 移除集群节点 |
| 23 | `test_cli_cluster_transfer` | `cluster-transfer` | 转移 Leader |

#### 2.7.3 CLI 输出与错误处理测试

| # | 测试用例 | 说明 |
|---|----------|------|
| 24 | `test_cli_error_connection_refused` | Master 不可达时报错 |
| 25 | `test_cli_error_invalid_volume_id` | 非法 volume_id |
| 26 | `test_cli_output_format` | 输出格式正确性 |
| 27 | `test_cli_exit_code_success` | 成功时退出码 0 |
| 28 | `test_cli_exit_code_error` | 失败时退出码非 0 |

---

## 三、端到端（E2E）集成测试

### 3.1 单节点端到端测试

| # | 测试用例 | 流程 |
|---|----------|------|
| 1 | `e2e_single_node_full_flow` | 启动 1 Master + 1 Volume → 挂载 FUSE → 创建文件 → 写入 → 读取 → 校验 → 删除 → 卸载 |
| 2 | `e2e_single_node_large_file` | 单节点 1GB 文件写入/读取完整性（校验和验证） |
| 3 | `e2e_single_node_many_small_files` | 10000 个 4KB 小文件创建/读取/删除 |
| 4 | `e2e_single_node_directory_ops` | 目录创建/嵌套/遍历/删除 |
| 5 | `e2e_single_node_restart_recovery` | 写入 → 关闭 Master+Volume → 重启 → 数据可读 |
| 6 | `e2e_single_node_filer_api` | Filer HTTP API 文件上传/下载/列表 |

### 3.2 多节点集群端到端测试

| # | 测试用例 | 流程 |
|---|----------|------|
| 7 | `e2e_cluster_3node_basic` | 3 Master（Raft）+ 3 Volume → 分配卷 → 写入 → 读取 |
| 8 | `e2e_cluster_master_failover` | 3 Master → 写入数据 → kill Leader → 新 Leader 选举 → 继续读写 |
| 9 | `e2e_cluster_volume_failover` | 3 Volume → 写入数据到 Vol1 → kill Vol1 → 其他 Volume 可正常服务 |
| 10 | `e2e_cluster_node_add_remove` | 动态加入/移除 Volume 节点，负载自动重分配 |
| 11 | `e2e_cluster_cross_dc_rack` | 多机架/多数据中心拓扑 → 验证机架感知分配 |
| 12 | `e2e_cluster_concurrent_clients` | 10 个并发客户端同时挂载读写 |
| 13 | `e2e_cluster_network_partition` | 模拟网络分区 → 恢复后数据一致性 |

### 3.3 E2E 测试基础设施设计

> **E2E 测试方式调整**：Phase 1 优先使用 in-process 集成测试（如当前 `raft_integration_test.rs` 的方式），真实进程启动的 E2E 测试推到 Phase 4 之后。

#### 3.3.1 Phase 1：in-process 集成测试（推荐）

基于现有 `raft_integration_test.rs` 模式，使用 `tokio::test` 在同一进程内启动多个节点：

```rust
// tests/in_process_cluster.rs — in-process 集成测试工具类
use tokio::sync::mpsc;
use powerfs_master::raft_node::RaftNode;
use powerfs_master::master::MasterNode;

pub struct InProcessCluster {
    nodes: Vec<RaftNode>,
    master: MasterNode,
    message_channels: Vec<mpsc::Sender<OutgoingMessage>>,
}

impl InProcessCluster {
    /// 创建 in-process 集群（同一进程内）
    pub async fn new(num_nodes: u8) -> Self;

    /// 等待 Leader 选出
    pub async fn wait_for_leader(&self) -> u64;

    /// 通过 MasterNode 直接调用 API
    pub fn master(&self) -> &MasterNode;

    /// 模拟节点故障（停止消息处理）
    pub async fn pause_node(&mut self, node_id: u64);

    /// 恢复节点
    pub async fn resume_node(&mut self, node_id: u64);

    /// 清理
    pub async fn shutdown(&mut self);
}
```

#### 3.3.2 Phase 4+：真实进程 E2E 测试（环境依赖强）

```rust
// tests/e2e_cluster.rs — 真实进程 E2E 测试工具类
use std::process::{Child, Command};
use std::time::Duration;

pub struct E2ECluster {
    masters: Vec<Child>,
    volumes: Vec<Child>,
    filer: Option<Child>,
    mount_point: tempfile::TempDir,
    master_ports: Vec<u16>,
    volume_ports: Vec<u16>,
}

impl E2ECluster {
    pub fn new(num_masters: u8, num_volumes: u8, with_fuse: bool) -> Self;
    pub fn wait_ready(&self, timeout: Duration) -> Result<()>;
    pub fn write_file(&self, path: &str, data: &[u8]) -> Result<()>;
    pub fn read_file(&self, path: &str) -> Result<Vec<u8>>;
    pub fn cli(&self, args: &[&str]) -> Result<String>;
    pub fn kill_master(&mut self, index: usize);
    pub fn kill_volume(&mut self, index: usize);
    pub fn partition(&self, group_a: &[usize], group_b: &[usize]);
    pub fn heal_partition(&self);
    pub fn restart_master(&mut self, index: usize) -> Result<()>;
    pub fn shutdown(&mut self);
}

impl Drop for E2ECluster {
    fn drop(&mut self) {
        self.shutdown();
    }
}
```

---

## 四、性能与基准测试（Benchmark）

### 4.1 微基准测试（criterion）

> **Benchmark 目标值策略调整**：当前目标值为经验值，建议先跑一轮基线测试，再根据实际结果设定合理目标值。

| # | Benchmark | 被测组件 | 指标 | 基线值（待填充） | 目标值 |
|---|-----------|----------|------|------------------|--------|
| 1 | `bench_needle_serialize` | Needle | 序列化吞吐 | - | > 500 MB/s |
| 2 | `bench_needle_deserialize` | Needle | 反序列化吞吐 | - | > 500 MB/s |
| 3 | `bench_needle_checksum` | BLAKE3 | 校验和吞吐 | - | > 2 GB/s |
| 4 | `bench_memory_index_insert` | MemoryIndex | 插入 ops/s | - | > 1M ops/s |
| 5 | `bench_memory_index_lookup` | MemoryIndex | 查询 ops/s | - | > 5M ops/s |
| 6 | `bench_persistent_index_insert` | PersistentIndex | 持久化插入 ops/s | - | > 10K ops/s |
| 7 | `bench_persistent_index_lookup` | PersistentIndex | 持久化查询 ops/s | - | > 50K ops/s |
| 8 | `bench_rocksdb_put` | RocksDbBackend | 写入 ops/s | - | > 50K ops/s |
| 9 | `bench_rocksdb_get` | RocksDbBackend | 读取 ops/s | - | > 200K ops/s |
| 10 | `bench_volume_write_4k` | Volume | 4KB IOPS | - | > 10K IOPS |
| 11 | `bench_volume_read_4k` | Volume | 4KB 读取 IOPS | - | > 50K IOPS |
| 12 | `bench_volume_write_1m` | Volume | 1MB 写入带宽 | - | > 500 MB/s |
| 13 | `bench_volume_read_1m` | Volume | 1MB 读取带宽 | - | > 1 GB/s |
| 14 | `bench_raft_propose` | RaftNode | 提案吞吐 | - | > 5K ops/s |

#### 4.1.1 基线测试流程

1. 在基准环境（Ubuntu 22.04, 32GB RAM, NVMe SSD）上运行：
   ```bash
   cargo bench -p powerfs-core -- --save-baseline main
   ```
2. 将结果填入上表「基线值」列
3. 根据基线值调整目标值，确保目标具有挑战性但可实现

### 4.2 系统级性能测试

| # | 场景 | 配置 | 关键指标 |
|---|------|------|----------|
| 1 | 小文件吞吐 | 100K 文件 × 4KB, 10 并发 | IOPS, p50/p99 延迟 |
| 2 | 大文件吞吐 | 100 文件 × 1GB, 4 并发 | 聚合带宽 (GB/s) |
| 3 | 混合负载 | 70% 读 + 30% 写, 50 并发 | 平均延迟、抖动 |
| 4 | 元数据压力 | 100K 目录 × 100 文件 | lookup/create/unlink 延迟 |
| 5 | FUSE 额外延迟 | 对比原生 FS | 额外延迟开销 (μs) |
| 6 | Raft 共识延迟 | 3/5/7 节点 | propose → commit p99 |

### 4.3 性能回归 CI 集成

- 每个 PR 运行关键 benchmark，与 main 基线对比
- 性能退化 > 10% 时 CI 发出 Warning
- 性能提升 > 20% 时高亮展示

---

## 五、压力与稳定性测试

### 5.1 压力测试

| # | 测试 | 说明 | 持续时间 |
|---|------|------|----------|
| 1 | `stress_concurrent_writes` | 50 并发持续写入 4KB 文件 | 10 min |
| 2 | `stress_concurrent_reads` | 100 并发持续读取 | 10 min |
| 3 | `stress_mixed_workload` | 80% 读 + 20% 写混合 | 30 min |
| 4 | `stress_volume_fill` | 持续写入直到所有卷满，验证错误处理 | - |
| 5 | `stress_master_failover_loop` | 循环 kill/restart Master（每 30s 切换） | 5 min |
| 6 | `stress_memory_leak` | 长时间运行，监控 RSS 内存增长 | 1 hour |

### 5.2 稳定性测试（Soak Test）

| # | 测试 | 说明 |
|---|------|------|
| 1 | `soak_24h_baseline` | 24 小时持续低负载运行，检测内存泄漏、FD 泄漏、磁盘空间增长 |
| 2 | `soak_crash_restart_loop` | 随机 kill 任意节点 + 重启，循环 100 次，验证数据不丢失 |

---

## 六、安全测试

| # | 测试用例 | 攻击向量 | 说明 |
|---|----------|----------|------|
| 1 | `test_security_invalid_fid` | 输入验证 | 非法 FID 格式（空字符串、SQL 注入字符、超长字符串） |
| 2 | `test_security_path_traversal` | 路径穿越 | `../../etc/passwd` 被正确拒绝 |
| 3 | `test_security_oversized_request` | 资源耗尽 | 超大数据包被拒绝（Needle > max size） |
| 4 | `test_security_checksum_tamper` | 数据篡改 | 篡改 Needle 校验和后读取被拒绝 |
| 5 | `test_security_special_chars` | 注入 | 文件名含 `\0`、`/`、换行符等特殊字符 |
| 6 | `test_security_disk_full` | 资源限制 | 磁盘满时优雅降级，不崩溃 |
| 7 | `test_security_fd_exhaustion` | 资源限制 | 大量文件打开时正确管理 FD |
| 8 | `test_security_large_concurrent_connections` | 资源限制 | 高并发连接不导致 OOM |

---

## 七、兼容性与回归测试

### 7.1 数据格式兼容性

| # | 测试 | 说明 |
|---|------|------|
| 1 | `test_compat_needle_binary_format` | Needle 二进制格式（header[12] + data[N] + footer[8]）版本稳定性 |
| 2 | `test_compat_rocksdb_key_schema` | RocksDB 键前缀向后兼容 |
| 3 | `test_compat_volume_file_structure` | Volume 数据文件结构不变 |
| 4 | `test_compat_proto_backward` | Proto 消息字段增删向后兼容 |

### 7.2 平台兼容性

| # | 平台 | 验证内容 |
|---|------|----------|
| 1 | Linux x86_64 (Ubuntu 22.04) | 主要 CI 平台，全量测试 |
| 2 | Linux x86_64 (Ubuntu 20.04) | 编译 + 单元测试 |
| 3 | Linux aarch64 | 交叉编译 + 基本功能（CI 可选） |
| 4 | macOS (开发环境) | 编译通过 + 非 FUSE 测试通过 |

---

## 八、测试基础设施建设

### 8.1 测试工具链

> **工具链引入策略调整**：不要一次性引入所有工具，按阶段逐步引入，避免复杂度爆炸。

| 工具 | 用途 | 状态 | 引入阶段 | 说明 |
|------|------|------|----------|------|
| `cargo test` | 单元测试 + 集成测试 | 已使用 | Phase 0 | Rust 内置 |
| `tempfile` | 临时目录管理 | 已使用 | Phase 0 | 已在项目中使用 |
| `tokio::test` | 异步测试 | 已使用 | Phase 0 | 已在项目中使用 |
| `mockall` | Mock 框架（Mock gRPC service、Raft Storage） | **优先引入** | P0 单元测试阶段 | 仅用于 gRPC mock，其他工具按需引入 |
| `criterion` | 微基准测试 | 待添加 | P2 Benchmark 阶段 | 先跑基线再定目标值 |
| `proptest` | 属性模糊测试（Needle 序列化往返、路径解析） | 待添加 | P2 安全测试阶段 | 在安全/压力测试阶段引入 |
| `rstest` | 参数化测试 | 待添加 | 按需引入 | 当测试用例出现大量重复模式时引入 |
| `tracing-test` | 测试中结构化日志 | 待添加 | 按需引入 | 当调试复杂异步测试时引入 |

### 8.2 CI/CD 流水线增强

当前 `.github/workflows/rust.yml` 已有：fmt → clippy → build → test。需增强：

```yaml
# 增强后的 CI 流水线
jobs:
  # 快速检查（每次 push）
  quick:
    runs-on: ubuntu-latest
    steps:
      - fmt-check       # cargo fmt --all -- --check
      - clippy          # cargo clippy --all-targets --all-features -- -D warnings
      - unit-test       # cargo test --lib --all

  # 完整测试（PR）
  full:
    runs-on: ubuntu-latest
    needs: quick
    steps:
      - integration-test  # cargo test --test '*' --all
      - doc-test          # cargo test --doc --all

  # 端到端（合并到 main）
  e2e:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - e2e-cluster-test  # 启动真实进程，运行端到端测试

  # 覆盖率（合并到 main）
  coverage:
    runs-on: ubuntu-latest
    steps:
      - tarpaulin         # cargo tarpaulin --all --out Xml
      - codecov-upload    # 上传到 codecov

  # 性能回归（合并到 main）
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - critcmp           # 与基线对比 benchmark 结果
```

### 8.3 测试辅助 Crate 规划

创建 `powerfs-test-utils`（workspace 内部 crate，`[dev-dependencies]` 引用）：

```
powerfs-test-utils/
├── Cargo.toml
└── src/
    ├── lib.rs           # 导出所有模块
    ├── cluster.rs       # 多节点 Raft 测试集群（从 powerfs-master/tests/cluster.rs 迁移）
    ├── data_gen.rs      # 随机测试数据生成器
    ├── assertions.rs    # 自定义断言宏
    ├── e2e_cluster.rs   # E2E 测试集群管理（启动真实进程）
    └── chaos.rs         # 故障注入工具（kill 进程、iptables 分区、磁盘满模拟）
```

---

## 九、各阶段测试重点对照

对照 design.md 的 5 个阶段：

| 阶段 | 测试重点 | 优先级 |
|------|----------|--------|
| **Phase 0** 项目初始化 | CI/CD 搭建、覆盖率基础设施、代码规范检查 | P0 ✓ |
| **Phase 1** Rust 重写核心基座 | **本方案覆盖的全部内容**：单元测试全覆盖、Raft 集成测试、Volume gRPC 测试、FUSE 文件系统测试、E2E 全链路测试 | **P0（当前）** |
| Phase 2 引入 BeeGFS 并行能力 | 分布式分片元数据测试、文件条带化读写测试、POSIX 文件锁/原子操作测试、HPC 作业 QoS 隔离测试 | P1 |
| Phase 3 Linux 内核客户端 | 内核模块测试框架（kunit）、VFS 注册/操作测试、FUSE vs Kernel Client 延迟对比 benchmark | P1 |
| Phase 4 KV Cache 引擎 | KV 寻址正确性、TTL/LRU 淘汰逻辑、会话隔离、GPU Direct 数据通路、KV 性能压测（QPS/p99） | P1 |
| Phase 5 生产级优化 | SPDK/RDMA 性能测试、EC 纠删码测试、冷热分层测试、监控告警测试、对标 Lustre/BeeGFS/CubeFS Benchmark | P2 |

---

## 十、当前阶段（Phase 1）测试执行计划

> **执行策略调整**：本方案框架和用例设计优秀，但需按「先修阻塞 → 补齐可测模块 → 随功能推进逐步测试」的节奏执行。不可测模块（FUSE mount 未实现、Volume gRPC 未启动）的测试推到对应功能实现阶段再做。

### P0 前置：阻塞项修复（必须先行）

以下问题是测试的前提条件，必须先修复，否则大量测试写出来也跑不通：

| 优先级 | 任务 | 文件 | 工时估算 | 状态 |
|--------|------|------|----------|------|
| P0-1 | 修复 `write_needle` 中 `spawn_blocking().unwrap()` 吞错误 | `powerfs-volume/src/server.rs:98` | 0.5d | ⏳ |
| P0-2 | 修复 `read_needle` 中 `spawn_blocking().unwrap()` 吞错误 | `powerfs-volume/src/server.rs:133` | 0.5d | ⏳ |
| P0-3 | 修复 `delete_needle` 中 `spawn_blocking().unwrap()` 吞错误 | `powerfs-volume/src/server.rs:163` | 0.5d | ⏳ |
| P0-4 | 启动 Volume gRPC 服务器 | `powerfs-volume/src/main.rs` | 1d | ⏳ |
| P0-5 | 实现 FUSE mount/unmount | `powerfs-fuse/src/fuse.rs:374` | 1d | ⏳ |
| P0-6 | 实现 Filer 子命令 | `powerfs-server/src/main.rs` | 1d | ⏳ |

### P0：立即执行（当前可测模块）

| 优先级 | 任务 | 用例数 | 工时估算 | 状态 |
|--------|------|--------|----------|------|
| 1 | powerfs-core — PersistentIndex 全部 7 方法 | 10 | 1d | ⏳ |
| 2 | powerfs-master 单元测试 — MasterNode 全部 37 方法 | 41 | 3d | ⏳ |
| 3 | powerfs-master 单元测试 — RaftNode 缺失方法（16 个） | 16 | 1.5d | ⏳ |
| 4 | powerfs-master 单元测试 — RocksDbStorage 缺失方法（11 个） | 11 | 1d | ⏳ |
| 5 | powerfs-common — 补齐 8 个 From 实现 + 4 个 RocksDB 方法 | 16 | 0.5d | ⏳ |
| 6 | powerfs-master — Raft 共识层扩展（21 个） | 21 | 2d | ⏳ |

### P1：功能实现后跟进（依赖 P0 前置修复）

| 优先级 | 任务 | 用例数 | 工时估算 | 依赖项 |
|--------|------|--------|----------|--------|
| 7 | powerfs-volume — VolumeServer gRPC 全部 9 RPC | 28 | 2d | P0-1~P0-4 |
| 8 | powerfs-volume — MasterClient 测试 | 5 | 0.5d | P0-4 |
| 9 | powerfs-fuse — 全部 19 方法测试 | 34 | 2d | P0-5 |
| 10 | powerfs-master 集成测试 — gRPC 服务层（20 个） | 20 | 2d | P0-1~P0-4 |
| 11 | powerfs-server — CLI 参数解析与启动测试 | 13 | 1d | P0-4~P0-6 |
| 12 | powerfs-cli — gRPC 客户端 + 11 命令测试 | 28 | 2d | P0-4 |

### P2：进阶测试（Phase 1 后期 / Phase 2 前期）

| 优先级 | 任务 | 用例数 | 工时估算 | 说明 |
|--------|------|--------|----------|------|
| 13 | in-process 集成测试框架扩展 | - | 1d | 基于现有 `raft_integration_test.rs` 模式 |
| 14 | Benchmark 框架搭建（criterion）+ 基线测试 | 14 | 1.5d | 先跑基线再定目标值 |
| 15 | 压力测试 + 安全测试 | 14 | 1.5d | 需要 FUSE/Volume 功能完整 |

### P3：E2E 全链路测试（Phase 4 之后）

| 优先级 | 任务 | 用例数 | 工时估算 | 说明 |
|--------|------|--------|----------|------|
| 16 | E2E 测试框架搭建（真实进程启动） | 13 | 2d | 环境依赖强，延后到功能稳定后 |
| 17 | E2E 全链路测试 | 13 | 2d | 需要所有核心功能完整 |

---

## 十一、测试用例总计

| 类别 | Crate / 模块 | 现有 | 新增 | 总计 |
|------|-------------|------|------|------|
| 单元 | powerfs-common | 157 | 28 | 185 |
| 单元 | powerfs-core | 74 | 33 | 107 |
| 单元 | powerfs-master (MasterNode) | 0 | 41 | 41 |
| 单元 | powerfs-master (RaftNode) | 0 | 16 | 16 |
| 单元 | powerfs-master (RocksDbStorage) | 0 | 11 | 11 |
| 单元 | powerfs-volume | 0 | 28 | 28 |
| 单元 | powerfs-fuse | 0 | 34 | 34 |
| 单元 | powerfs-server | 0 | 13 | 13 |
| 单元 | powerfs-cli | 0 | 28 | 28 |
| 集成 | Raft 共识层 | 6 | 21 | 27 |
| 集成 | gRPC 服务层 | 0 | 20 | 20 |
| E2E | 单节点 | 0 | 6 | 6 |
| E2E | 多节点集群 | 0 | 7 | 7 |
| 性能 | Benchmark | 0 | 14 | 14 |
| 压力 | Stress / Soak | 0 | 8 | 8 |
| 安全 | Security | 0 | 8 | 8 |
| 兼容 | Compatibility | 0 | 4 | 4 |
| **合计** | | **237** | **320** | **557** |

---

## 十二、附录

### A. 测试命名规范

```
# 单元测试 — 函数名_场景[_边界]
test_<function>_<scenario>              # test_volume_write_needle_success
test_<function>_<scenario>_<edge>       # test_volume_write_needle_volume_full

# 集成测试 — 模块_操作_预期
test_<module>_<operation>_<expected>    # test_master_assign_volume_success
test_<module>_<operation>_<error>       # test_master_assign_volume_no_capacity

# E2E 测试
e2e_<scenario>                          # e2e_single_node_full_flow

# 性能测试
bench_<component>_<operation>           # bench_needle_serialize

# 安全测试
test_security_<vector>                  # test_security_path_traversal

# 压力测试
stress_<workload>                       # stress_concurrent_writes
soak_<duration>_<workload>              # soak_24h_baseline
```

### B. 当前代码中已知待修复问题（P0 前置任务）

> 以下问题是测试的前提条件，必须先修复。详细修复任务见「十、当前阶段（Phase 1）测试执行计划」中的「P0 前置」部分。

| # | 文件 | 行号 | 问题 | 影响测试 | 修复方案 |
|---|------|------|------|----------|----------|
| 1 | `powerfs-fuse/src/fuse.rs` | 374 | `mount()` 只创建目录，未启动 FUSE 会话 | FUSE 测试阻塞 | 调用 `fuse_backend_rs` 的实际挂载 API：`FuseSession::new().mount(mount_point)` |
| 2 | `powerfs-volume/src/main.rs` | - | `run_volume()` 未启动 gRPC 服务器 | Volume E2E 测试阻塞 | 启动 tonic gRPC 服务器，注册 VolumeServer 服务 |
| 3 | `powerfs-server/src/main.rs` | - | `Filer` 子命令仅占位（只打印日志） | Filer 测试阻塞 | 实现 Filer HTTP 服务器，支持文件上传/下载/列表 API |
| 4 | `powerfs-volume/src/server.rs` | 98 | `write_needle` 使用 `spawn_blocking().unwrap()` 吞错误 | 错误路径不可测 | 将 `spawn_blocking().unwrap()` 改为 `spawn_blocking().await`，正确传播错误 |
| 5 | `powerfs-volume/src/server.rs` | 133 | `read_needle` 同上 | 错误路径不可测 | 同上 |
| 6 | `powerfs-volume/src/server.rs` | 163 | `delete_needle` 同上 | 错误路径不可测 | 同上 |

### C. 运行测试命令

```bash
# 全量测试
cargo test --all --verbose

# 仅单元测试（快）
cargo test --lib --all

# 仅集成测试
cargo test --test '*' --all

# 特定 crate
cargo test -p powerfs-core
cargo test -p powerfs-master --test raft_integration_test

# 带输出
cargo test -- --nocapture

# 覆盖率
cargo tarpaulin --all --out Html --output-dir ./coverage

# Benchmark
cargo bench -p powerfs-core
```

---

> 本文档基于对全部源代码的逐方法审计生成。随项目阶段演进持续更新。
