# FUSE Coherence v2 - 遗漏功能修复计划

## Context

对照 `design/fuse-coherence-v2.md` 方案文档检查发现4个已描述但未实现的功能。这些功能影响文件系统一致性的关键路径，需按优先级一次性修复并补充单元测试。

**当前测试基线**: 71个测试通过（44 Phase 0-3 + 27 failover）

## 修改文件清单

| 文件 | 修改内容 |
|------|----------|
| `powerfs-master/proto/master.proto` | 新增LeaseRenew消息、JOB_COMPLETE枚举、job_id字段 |
| `powerfs-master/src/directory_tree.rs` | renew_lease方法、complete_job发通知、current_job_id字段 |
| `powerfs-master/src/server.rs` | renew_lease gRPC handler |
| `powerfs-fuse/src/client.rs` | renew_lease异步+同步方法 |
| `powerfs-fuse/src/fuser_fs.rs` | LeaseInfo结构体、lease_renewal_loop、lookup generation验证、job_id传递 |
| `powerfs-fuse/src/cache.rs` | path_generations字段、clear_all方法 |
| `powerfs-master/tests/coherence_failover_test.rs` | 续租测试 |
| `powerfs-master/tests/coherence_phase3_test.rs` | 作业完成通知、job_id测试 |
| `powerfs-fuse/src/cache.rs` tests模块 | generation验证、clear_all测试 |

## 实施顺序

按依赖关系排序，功能2→3→4→1（避免对`handle_metadata_notification`和`metadata_subscription_loop`的重复修改）。

### 步骤1: 功能2 - 作业结束批量失效 (P1)

1. **Proto**: `master.proto` EventType枚举添加 `JOB_COMPLETE = 4`
2. **Master**: `directory_tree.rs` 的 `complete_job()` 中调用 `publish_notification(EventType::JobComplete, "/", None)`
3. **FUSE**: `fuser_fs.rs` 的 `handle_metadata_notification` 添加 `JobComplete` 分支，调用 `cache.clear_all()`
4. **Cache**: `cache.rs` 新增 `clear_all()` 方法 - 清空inode_cache、path_map、dir_cache，重新初始化根目录(inode 1)
5. **测试**: 
   - `coherence_phase3_test.rs`: `test_complete_job_publishes_notification`、`test_complete_nonexistent_job_no_notification`
   - `cache.rs` tests: `test_clear_all_empties_and_reinitializes`

### 步骤2: 功能3 - Lookup时Generation验证 (P2)

1. **Cache**: `cache.rs` 的 `MetadataCache` 添加 `path_generations: RwLock<HashMap<String, u64>>` 字段
2. **Cache**: 新增 `update_path_generation()` 和 `get_path_generation()` 方法
3. **Cache**: `clear_all()` 中清空 `path_generations`
4. **FUSE**: `handle_metadata_notification` 开头添加 `cache.update_path_generation(&notification.path, notification.generation)`
5. **FUSE**: `lookup()` 缓存命中时比较 `entry.generation` 与 `cache.get_path_generation(&lookup_path)`，过期则跳过缓存
6. **测试**: `cache.rs` tests: `test_path_generation_update_and_get`、`test_path_generation_stale_detection`

### 步骤3: 功能4 - 作业级租约共享 (P2)

1. **Proto**: `MetadataNotification` 添加 `string job_id = 8`
2. **Master**: `DirectoryTree` 添加 `current_job_id: RwLock<Option<String>>` 字段
3. **Master**: `register_job_client()` 末尾设置 `current_job_id`
4. **Master**: `publish_notification()` 读取 `current_job_id` 并填入通知
5. **FUSE**: `PowerFsFuserFs` 添加 `job_id: String` 字段，`new()` 接受参数
6. **FUSE**: `handle_metadata_notification` 添加 `job_id: &str` 参数，lease过滤逻辑中如果 `notification.job_id == job_id` 则不跳过invalidation
7. **FUSE**: `metadata_subscription_loop` 添加 `job_id` 参数并传递
8. **FUSE**: `FuserApp::run()` 从环境变量获取 `job_id` 并传入
9. **测试**: `coherence_phase3_test.rs`: `test_register_job_sets_current_job_id`、`test_notification_includes_job_id`、`test_notification_without_job_has_empty_job_id`

### 步骤4: 功能1 - 租约自动续租 (P0)

1. **Proto**: 新增 `LeaseRenewRequest { string lease_id = 1; uint64 duration_ms = 2; }` 和 `LeaseRenewResponse { bool success = 1; string error = 2; uint64 epoch = 3; }`，添加 `rpc RenewLease`
2. **Master**: `directory_tree.rs` 新增 `renew_lease(lease_id, duration_ms) -> Option<u64>` - 找到lease并更新 `expires_at`
3. **Server**: `server.rs` 新增 `renew_lease` gRPC handler
4. **Client**: `client.rs` 新增 `renew_lease()` 异步方法返回 `Result<(bool, u64), String>` 和 `SyncFuseClient` 同步包装器
5. **FUSE**: 新增 `LeaseInfo { lease_id, path, duration_ms, acquired_at }` 结构体
6. **FUSE**: `leases` 字段类型从 `HashMap<u64, Vec<String>>` 改为 `HashMap<u64, Vec<LeaseInfo>>`
7. **FUSE**: `open()` 中duration从300000改为30000（30秒），存储LeaseInfo
8. **FUSE**: `release()` 中pop LeaseInfo并释放lease_id
9. **FUSE**: 新增 `lease_renewal_loop()` 异步函数 - 每5秒检查，剩余时间<1/3时续租
10. **FUSE**: `FuserApp::run()` 中spawn续租循环
11. **测试**: `coherence_failover_test.rs`: `test_renew_lease_updates_expiry`、`test_renew_nonexistent_lease`、`test_renew_lease_preserves_epoch`

## 关键设计决策

1. **续租检查间隔5秒**: 30秒租约在剩余10秒（1/3）时续租，5秒间隔保证至少2次检查机会
2. **current_job_id简化设计**: 使用单一 `RwLock<Option<String>>` 而非client_id→job_id映射，满足单作业场景（HPC主要用例）
3. **clear_all后重建根目录**: 必须重新初始化inode 1，否则FUSE内核无法工作
4. **LeaseInfo存储path**: 续租需要path信息，但renew_lease RPC只需lease_id（Master端通过lease_id查找path）

## 验证方案

```bash
# 1. 编译
cargo check --all

# 2. 格式和clippy
cargo fmt --all
cargo clippy -p powerfs-master -p powerfs-fuse --tests -- -D warnings

# 3. 运行所有coherence测试
cargo test --package powerfs-fuse --test coherence_phase0_test --test coherence_phase1_test
cargo test --package powerfs-master --test coherence_phase2_test --test coherence_phase3_test --test coherence_failover_test

# 4. 确认无回归
cargo test --all
```

预期结果: 现有71个测试 + 新增约15个测试 = ~86个测试全部通过
