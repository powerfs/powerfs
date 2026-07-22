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

### 8. 代码质量检查
- ✅ `cargo fmt` 格式化完成
- ✅ `cargo clippy` 无警告
- ✅ concurrent_consistency 测试全部通过

---

## 后续待办事项

### 优先级：高

| # | 任务 | 说明 | 状态 |
|---|------|------|------|
| 1 | 租约续期机制 | 实现后台租约续期线程，防止写文件期间租约过期 | 待实现 |
| 2 | FUSE O_APPEND 并发修复 | 内核在 getattr 和 write 之间有窗口，多线程可能拿到相同文件大小并写到同一 offset 导致覆盖丢数据 | 待实现 |
| 3 | 并发读写 size 追踪 | 多线程并发 read 时偶发返回 0 字节导致 UnexpectedEof | 待实现 |
| 4 | 租约失效通知 | Master 通知客户端租约被抢占时的处理逻辑 | 待实现 |
| 5 | 多客户端一致性测试 | 验证租约机制能有效防止并发写冲突 | 待实现 |

### 优先级：中

| # | 任务 | 说明 | 状态 |
|---|------|------|------|
| 6 | 冲突检测增强 | 更多场景的冲突检测（如跨客户端 rename 冲突） | 待实现 |
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
