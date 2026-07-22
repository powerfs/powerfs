# PowerFS 开发进度记录

## 已完成工作

### 1. 核心设计文档
- [FileLayout 与 Stripe 设计文档](docs/file_layout_stripe_design.md)
- [无锁 Flush 设计文档](docs/lock_free_flush_design.md)

### 2. Volume 预创建机制
- 通过配置文件设置 `initial_volume_count`
- 支持启动时预创建指定数量的 Volume
- 参数优先级：CLI > 配置文件 > 默认值

### 3. 无锁 Flush 架构
- 按 inode 哈希路由到固定 worker 线程
- Worker 内部单线程串行处理，消除 per-inode 锁竞争
- 每个 worker 维护独立 gRPC 连接池

### 4. FUSE 权限修复
- 根目录使用 `libc::getuid()/getgid()` 初始化
- 文件/目录创建时从 FUSE Request 获取客户端 uid/gid
- 移除 DefaultPermissions 挂载选项

### 5. Rename 优化
- 同分片 rename 通过分片写锁实现原子操作
- 跨分片 rename 通过创建新条目+删除旧条目两步操作

### 6. DirORSet::rename_entry 单元测试
- `test_dir_orset_rename_entry_basic` - 基本重命名功能
- `test_dir_orset_rename_entry_nonexistent` - 重命名不存在条目
- `test_dir_orset_rename_entry_inode_preserved` - inode 保持不变
- `test_dir_orset_rename_entry_mtime_updated` - mtime 更新
- `test_dir_orset_rename_entry_tombstone_added` - 添加 tombstone
- `test_dir_orset_rename_entry_delta_log` - delta 日志记录

### 7. 文件级租约机制
- [powerfs-fuse/src/fuser_fs.rs](powerfs-fuse/src/fuser_fs.rs)
  - `open()` 时以写模式打开文件获取租约（30秒过期）
  - `release()` 时释放租约
  - 租约管理：`leases: Arc<RwLock<HashMap<u64, String>>>`

### 8. 租约续期机制
- [powerfs-fuse-enterprise/src/fuser_fs.rs](powerfs-fuse-enterprise/src/fuser_fs.rs)
  - 后台续期线程：每 10 秒遍历所有活跃租约调用 `renew_lease()`
  - 续期周期：10秒（小于租约有效期 30 秒，确保租约不失效）
  - 续期成功/失败都记录日志，方便调试
  - 线程停止信号：`lease_renewer_running` 原子标志

### 9. 代码质量检查
- ✅ `cargo fmt` 格式化完成
- ✅ `cargo clippy` 无警告
- ✅ concurrent_consistency 测试全部通过

### 10. FUSE O_APPEND 并发修复
- [powerfs-fuse-enterprise/src/fuser_fs.rs](powerfs-fuse-enterprise/src/fuser_fs.rs)
  - 问题：内核在 getattr 和 write 之间有竞态窗口，多线程可能拿到相同文件大小并写到同一 offset 导致覆盖丢数据
  - 解决方案：
    1. 检测 `O_APPEND` 标志，动态计算写入偏移（`current_file_size(inode)`）
    2. 实现 inode 级别写锁（`write_locks: Arc<RwLock<HashMap<u64, Arc<Mutex<()>>>>>`）
    3. 获取文件大小和写入操作在同一临界区内完成，保证原子性

### 11. 并发读写 size 追踪修复
- [powerfs-fuse-enterprise/src/data_manager.rs](powerfs-fuse-enterprise/src/data_manager.rs)
  - 问题：多线程并发 read 时偶发返回 0 字节导致 UnexpectedEof
  - 根本原因：文件创建/打开后 file_sizes 缓存未初始化，导致 `current_file_size()` 返回 0
  - 解决方案：
    1. `create()` 时初始化 file_sizes 为 0
    2. `open()` 时同步元数据中的文件大小到 DataManager
    3. `current_file_size()` 综合考虑 chunk_cache 中的大小和 write_buffer 中的最大偏移
    4. `read()` 方法增加从 write_buffer 读取的逻辑，支持读取尚未刷新到 chunk_cache 的数据

### 12. 租约失效通知机制
- [powerfs-fuse-enterprise/src/fuser_fs.rs](powerfs-fuse-enterprise/src/fuser_fs.rs)
  - 问题：当租约被 Master 抢占或过期时，客户端需要及时感知并处理
  - 解决方案：
    1. 新增 `lease_epochs` 追踪每个 inode 的租约 epoch
    2. 新增 `invalidated_inodes` 集合记录已失效的 inode
    3. 租约续期失败时标记 inode 为失效
    4. `write()` 方法开头检查租约是否失效，失效时返回 EIO 错误
    5. `handle_lease_invalidation()` 方法处理失效后的清理（清除租约、锁、缓冲、元数据缓存）

### 13. 多客户端一致性测试
- [powerfs-master/tests/multi_client_consistency_test.rs](powerfs-master/tests/multi_client_consistency_test.rs)
  - 测试用例：
    1. `test_single_client_lease_acquisition` - 单个客户端获取租约
    2. `test_second_client_cannot_acquire_existing_lease` - 第二个客户端无法获取已有租约
    3. `test_second_client_can_acquire_after_first_releases` - 第一个释放后第二个可获取
    4. `test_concurrent_lease_acquisition_single_winner` - 并发获取只有一个胜出
    5. `test_renew_lease_extends_expiration` - 续期延长有效期
    6. `test_renew_lease_fails_for_released_lease` - 续期已释放的租约失败
    7. `test_lease_expires_after_timeout` - 租约超时过期
    8. `test_client_can_acquire_expired_lease` - 客户端可获取过期租约
    9. `test_multiple_files_independent_leases` - 多文件独立租约
    10. `test_lease_release_allows_new_acquisition` - 释放后允许重新获取
  - 修复：`acquire_lease` 方法缺少租约互斥检查，使用写锁保护检查和插入操作
  - 测试结果：全部 10 个测试用例通过

### 14. 冲突检测增强
- [powerfs-orset/src/lib.rs](powerfs-orset/src/lib.rs)
  - 新增冲突类型：
    - `RenameDelete`: 重命名后删除冲突（客户端 A 重命名文件，客户端 B 同时删除原文件）
    - `CreateDelete`: 创建后删除冲突（客户端 A 创建文件，客户端 B 同时删除同名文件）
  - 增强的冲突检测场景：
    - `detect_rename_conflict`: 检测跨客户端 rename 冲突、rename + delete 冲突
    - `detect_remove_conflict`: 检测 create + delete 冲突
  - 更新 `ConflictStats` 和 `ConflictStatsFull` 结构，添加新冲突类型的统计字段
- [powerfs-master/proto/master.proto](powerfs-master/proto/master.proto)
  - 在 `ConflictType` enum 中添加 `RENAME_DELETE = 5` 和 `CREATE_DELETE = 6`
- [powerfs-master/src/server.rs](powerfs-master/src/server.rs)
  - 更新冲突类型转换逻辑，支持新的冲突类型

### 9. IO500 测试验证
- 测试环境：3个 Volume Server + 3个 Master + Redis + FUSE 客户端
- 测试配置：blockSize=1GB, n=1000, segmentCount=100
- 测试结果：所有阶段成功完成

| 测试阶段 | 结果 |
|----------|------|
| ior-easy-write | 0.42 GiB/s |
| ior-easy-read | 0.84 GiB/s |
| ior-hard-write | 0.14 GiB/s |
| ior-hard-read | 0.72 GiB/s |
| ior-rnd4K-easy-read | 1.04 GiB/s |
| mdtest-easy-write | 2.41 kIOPS |
| mdtest-easy-stat | 5.29 kIOPS |
| mdtest-easy-delete | 4.24 kIOPS |
| mdtest-hard-write | 0.20 kIOPS |
| mdtest-hard-stat | 3.86 kIOPS |
| mdtest-hard-delete | 2.69 kIOPS |
| find | 1.72 kIOPS |

- **验证结论**：PowerFS 企业版基本功能正确，支持 POSIX API，元数据和数据读写正常
- **发现问题**：rmdir 返回 "Directory not empty"（可能是 OR-Set 延迟删除问题）

---

## 后续待办事项

### 优先级：高

| # | 任务 | 说明 | 状态 |
|---|------|------|------|
| 1 | 租约续期机制 | 实现后台租约续期线程，防止写文件期间租约过期 | ✅ 已完成 |
| 2 | FUSE O_APPEND 并发修复 | 内核在 getattr 和 write 之间有窗口，多线程可能拿到相同文件大小并写到同一 offset 导致覆盖丢数据 | ✅ 已完成 |
| 3 | 并发读写 size 追踪 | 多线程并发 read 时偶发返回 0 字节导致 UnexpectedEof | ✅ 已完成 |
| 4 | 租约失效通知 | Master 通知客户端租约被抢占时的处理逻辑 | ✅ 已完成 |
| 5 | 多客户端一致性测试 | 验证租约机制能有效防止并发写冲突 | ✅ 已完成 |

### 优先级：中

| # | 任务 | 说明 | 状态 |
|---|------|------|------|
| 6 | 冲突检测增强 | 更多场景的冲突检测（如跨客户端 rename 冲突） | ✅ 已完成 |
| 7 | 冲突自动解决策略 | 实现更多自动解决策略（如按内容哈希合并） | 待实现 |
| 8 | 元数据同步优化 | 优化 delta sync 的效率和可靠性 | 待实现 |

### 优先级：低

| # | 任务 | 说明 | 状态 |
|---|------|------|------|
| 9 | 性能基准测试 | 对比无锁 Flush 前后的性能差异 | 待实现 |
| 10 | 代码文档完善 | 补充关键模块的文档注释 | 待实现 |

---

## 推荐下一步

优先实现 **租约续期机制**，因为当前租约只有 30 秒有效期，如果文件打开时间超过 30 秒，租约会过期，可能导致并发写冲突。需要在后台启动一个线程，定期为活跃的租约续期。

---

## 提交记录

| 仓库 | 提交 | 说明 |
|------|------|------|
| powerfs | `101df36a` | Implement file-level lease mechanism |
| powerfs | `b9c2e911` | Fix FUSE uid/gid permissions |
| powerfs-fuse-enterprise | `84370c3` | Fix FUSE uid/gid permissions |
