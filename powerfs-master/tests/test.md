# PowerFS Master Raft 多节点集成测试

## 测试环境

测试位于 `powerfs-master/tests/` 目录下，包含 Raft 分布式共识的全面测试。

## 测试文件结构

```
tests/
├── raft_integration_test.rs    # 主测试入口，综合测试
├── cluster.rs                  # 多节点集群测试基础设施
├── leader_election.rs          # Leader 选举测试
├── log_replication.rs          # 日志复制测试
└── fault_tolerance.rs          # 容错测试
```

## 运行测试

### 编译测试

```bash
cd /home/portion/powerfs
source proxy.sh
cargo build -p powerfs-master --test raft_integration_test --offline
```

### 运行所有测试

```bash
cargo test -p powerfs-master --test raft_integration_test
```

### 运行单个测试

```bash
# Leader 选举测试
cargo test -p powerfs-master --test raft_integration_test test_single_leader_election

# 日志复制测试
cargo test -p powerfs-master --test raft_integration_test test_basic_log_replication

# 故障转移测试
cargo test -p powerfs-master --test raft_integration_test test_leader_failure
```

### 运行测试并显示详细输出

```bash
cargo test -p powerfs-master --test raft_integration_test -- --nocapture
```

## 测试用例列表

### 集群启动测试

| 测试名 | 描述 |
|--------|------|
| `test_cluster_startup` | 验证 3 节点集群能正常启动 |
| `test_cluster_with_adjusted_timing` | 验证调整选举超时参数后集群能正常工作 |

### Leader 选举测试

| 测试名 | 描述 |
|--------|------|
| `test_single_leader_election` | 验证集群启动后只有一个 Leader |
| `test_leader_reelection_after_failure` | 验证 Leader 故障后能重新选举 |
| `test_election_with_quorum` | 验证部分节点下线后仍能选举 Leader |
| `test_no_leader_without_quorum` | 验证失去多数节点后无法选举 Leader |
| `test_leader_stability` | 验证 Leader 在稳定集群中不会频繁切换 |

### 日志复制测试

| 测试名 | 描述 |
|--------|------|
| `test_basic_log_replication` | 验证基本日志复制功能 |
| `test_command_application` | 验证命令被正确应用到状态机 |
| `test_concurrent_log_replication` | 验证并发提议能正确处理 |
| `test_follower_catch_up` | 验证落后的 Follower 能追赶 |
| `test_log_compaction` | 验证日志压缩（快照）功能 |

### 读写一致性测试

| 测试名 | 描述 |
|--------|------|
| `test_propose_command` | 验证通过 Raft 提议命令 |
| `test_read_after_write_consistency` | 验证写入后读取的一致性 |
| `test_concurrent_proposals` | 验证并发提议的正确性 |

### 容错测试

| 测试名 | 描述 |
|--------|------|
| `test_follower_failure` | 验证 Follower 故障后集群继续工作 |
| `test_leader_failure` | 验证 Leader 故障后集群重新选举并继续工作 |
| `test_partition_recovery` | 验证网络分区恢复后集群正常工作 |
| `test_multiple_failures` | 验证多个节点故障后集群继续工作（5节点集群，2节点下线） |
| `test_majority_loss` | 验证失去多数节点后集群无法继续 |
| `test_commit_persistence_after_leader_change` | 验证 Leader 变更后已提交命令不丢失 |
| `test_rapid_leader_changes` | 验证快速连续 Leader 变换的稳定性 |

## 测试基础设施

### RaftTestCluster

多节点测试集群，支持：

- 创建指定数量的节点（默认 3 节点）
- 启动/停止所有节点
- 等待 Leader 选举
- 向集群提议命令
- 查询节点状态（Leader、last_index、applied_index）

```rust
// 创建 3 节点集群
let cluster = RaftTestCluster::new(3).await;
cluster.start_all().await;

// 等待 Leader
let leader = cluster.wait_for_leader(Duration::from_secs(5)).await;

// 提议命令
let cmd = RaftCommand::AddNode { ... };
cluster.propose(&leader, cmd).await;

// 查询状态
let leaders = cluster.get_all_leaders().await;
let indices = cluster.get_all_last_indices().await;
```

### ClusterBuilder

支持自定义配置：

```rust
let cluster = RaftTestCluster::builder()
    .num_nodes(5)              // 5 节点集群
    .tick_ms(50)               // 50ms tick 间隔
    .election_timeout_ms(200) // 200ms 选举超时
    .build()
    .await;
```

## 测试注意事项

1. **每个测试创建独立集群**：测试之间不共享状态，每个测试创建自己的临时目录和节点

2. **异步测试**：所有测试使用 `#[tokio::test]`，需要等待 Leader 选举和日志复制

3. **临时目录自动清理**：每个节点的 RocksDB 数据存储在临时目录，测试结束后自动清理

4. **超时设置**：测试默认超时 60 秒，可通过 `--timeout` 参数调整

## 添加新测试

在对应模块文件中添加新测试函数：

```rust
#[tokio::test]
async fn test_new_feature() {
    // 创建集群
    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    // 等待 Leader
    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // 测试逻辑...

    // 清理
    cluster.shutdown().await;
}
```

## 调试测试

### 启用日志输出

测试代码中已移除 `env_logger` 依赖。如需日志输出，可添加：

```rust
// 在 Cargo.toml 的 [dev-dependencies] 中添加
env_logger = "0.10"

// 在测试函数开头添加
let _ = env_logger::builder()
    .filter_level(log::LevelFilter::Debug)
    .try_init();
```

### 单步调试

```bash
# 使用 rust-gdb 或 rust-lldb
rust-gdb --args ./target/debug/raft_integration_test test_single_leader_election
```

## 测试依赖

| 依赖 | 版本 | 用途 |
|------|------|------|
| `tempfile` | 3.8 | 创建临时目录存储 RocksDB 数据 |

## 预期结果

所有测试应通过。如果测试失败，可能原因：

1. **Leader 选举超时**：检查 tick_ms 和 election_timeout_ms 参数
2. **网络问题**：测试在本地运行，无真实网络通信
3. **资源竞争**：某些测试需要更多等待时间

## 后续改进

1. 添加真实 gRPC 通信测试（启动真实服务端）
2. 添加持久化恢复测试（重启节点后状态恢复）
3. 添加性能基准测试（吞吐量、延迟）