# PowerFS 锁机制模块化与优化方案

> 状态: 讨论中
> 分支: `lock-optimization` (基于 master `27e5e817`)
> 约束: 本文档不入库（docs/ 已在 .gitignore）

## 一、目标

1. **模块化**: 现有锁机制散落 5 个文件、无统一接口,先解耦成独立 crate,接口清晰后便于后续试验
2. **性能评估**: 基于模块化后的代码建立性能基线,量化当前锁机制的真实瓶颈
3. **优化实施**: 基于基线数据提出优化方案(Lockify 异步元数据 / SeqDLM Early Grant 等),按优先级实施

## 二、现状诊断

### 2.1 锁机制散落分布(无统一接口层)

| 位置 | 职责 | 边界清晰度 |
|---|---|---|
| `powerfs-fuse/src/cache.rs` | FUSE 客户端: EntryState 状态机 + HoldState(Pinned=租约持有) + open_count 引用计数 | 中等(状态机清晰,但租约逻辑和缓存强耦合) |
| `powerfs-fuse/src/invalidate_handler.rs` | FUSE 客户端: 处理 Filer 推送的 Invalidate 通知 + pinned/lease 保护 | 差(与 cache.rs 双向依赖) |
| `powerfs-filer/src/inode_lease_manager.rs` | Filer 端: per-inode 独占租约(admission control) | 清晰(已是独立模块) |
| `powerfs-fuse-core/src/meta_shard_client.rs` | FUSE 客户端: 发送 Lease/Invalidate 消息 | 差(业务+协议混在一起) |
| `powerfs-fuse/src/fuse.rs` | open/release/setattr 中散落锁交互 | 差(锁逻辑嵌入业务路径) |

### 2.2 三个关键问题

**问题 1: 客户端"锁"和"缓存"强耦合**
`cache.rs` 同时管: 元数据缓存 + 租约状态(pinned) + 脏页标记(Dirty)。租约应该独立成模块,缓存只管缓存。

**问题 2: 客户端和服务端租约语义不对称**
- 客户端: `HoldState::Pinned` = "我有打开句柄,绕过 TTL"(隐式租约)
- 服务端: `InodeLeaseManager` = "显式 acquire/release,带 token 和 grace period"(显式租约)
- 两者**没有协议对应关系**。客户端的 pinned 不调服务端 acquire,服务端的 lease 也不通知客户端

**问题 3: Early Revoke 半成品**
`invalidate_handler.rs` 已有"pinned inode 保护"逻辑(租约期内跳过 invalidate),但**没有主动让锁机制**——收到冲突通知时不会主动 unpin+flush,只是被动等待 TTL 过期。这正是 Early Revoke 要补的。

## 三、架构设计

### 3.1 三层接口 + 双客户端形态 + 双语言

```
┌──────────────────────────────────────────────────────────────┐
│           powerfs-lock (接口层, Rust trait)                   │
│                                                              │
│  trait LockManager {                                         │
│      async fn acquire(&mut self, req: LockRequest)           │
│          -> Result<LockGrant, LockError>;                    │
│      async fn release(&mut self, inode: u64, token: u64);     │
│      async fn renew(&mut self, inode: u64, token: u64)       │
│          -> Result<LeaseInfo, LockError>;                     │
│      fn register_handler(&self, h: Arc<dyn LockEventHandler>);│
│  }                                                           │
│                                                              │
│  trait LockEventHandler {                                     │
│      fn on_revoke(&self, inode: u64, token: u64);   // Early Revoke│
│      fn on_invalidate(&self, inode: u64, range: Option<Range>);│
│  }                                                           │
│                                                              │
│  struct LockRequest { inode, mode, range, timeout }          │
│  struct LockGrant { inode, token, sn, lease_ms }             │
│  enum LockMode { Shared, Exclusive, Range(Range) }           │
└────────┬──────────────────────────┬──────────────────────────┘
         │                          │
         │ trait 对客户端形态中立    │
         │                          │
┌────────▼─────────────┐  ┌────────▼──────────────────┐
│ powerfs-lock-fuse    │  │ powerfs-kernel (C)       │
│ (用户态, Rust)        │  │ (内核态, C 实现)         │
│                      │  │                          │
│ - ClientLeaseState   │  │ - powerfs_lock.h 接口    │
│   (从 cache.rs 拆出) │  │ - tlk_codec.c TLV 编解码 │
│ - tokio runtime      │  │ - lock_client.c 状态机   │
│ - FUSE_NOTIFY_INVAL  │  │ - invalidate_inode_pages2│
│ - 适配 LockEventHandler│ │ - 适配回调              │
└────────┬─────────────┘  └────────┬─────────────────┘
         │                       │
         └──────────┬────────────┘
                    │
         ┌──────────▼──────────────┐
         │ powerfs-lock-net (协议)  │
         │ Rust TLV 编解码          │
         └──────────┬──────────────┘
                    │
         ┌──────────▼──────────────┐
         │     powerfs-filer        │
         │  (服务端 LockManager)    │
         │  - InodeLeaseManager    │
         │  - SN 分配 (预留)        │
         │  - Raft 补日志 (预留)    │
         └─────────────────────────┘
```

### 3.2 关键决策

**决策 1: 协议规范文档化(不是代码共享)**

内核是 C,不能链接 Rust crate。协议层是规范文档,不是共享代码:
- `docs/lock-protocol.md` 定义所有消息的字段布局、字节序、TLV 编码规则
- Rust 端 `powerfs-lock-net` 实现编解码
- C 端 `powerfs-kernel` 独立实现编解码(纯 C,几百行)
- 协议变更时同步更新文档,两端各自实现

**决策 2: SN 分配策略(优化阶段决定)**

模块化阶段不实现 SN 分配,只在接口层预留 `sn: u64` 字段。优化方案讨论后二选一:
- 方案 A (同步): SN 分配先走 Raft 多数派再授予。安全但 Early Grant 收益被 Raft 延迟吃掉
- 方案 B (异步, 推荐): Leader 本地 `AtomicU64::fetch_add` 分配 SN,立即 Grant,后台批量异步补 Raft 日志(10ms 窗口)。真正实现 Early Grant,Leader 切换时未提交的锁操作回滚重做

**决策 3: 锁模式简化为 3 种**

不要 PR/PW/EX/CW 四种(DLM 历史包袱),简化为:
- `Shared` (读,多客户端并发读)
- `Exclusive` (写,排他)
- `Range` (范围写,支持 flock 和 OFD 语义)

**决策 4: 内核客户端特殊约束**

- 不能阻塞内核: 持锁等待用 `wait_event_interruptible`(可中断睡眠),超时返回 `-EAGAIN`
- 页缓存失效与锁强绑定: 释放前必须先 `filemap_fdatawrite_range` 刷脏 + `filemap_fdatawait_range` 等待,再 release
- Lease 存储位置: `inode->i_private` 挂 `KernelLeaseState { token, sn, expire_at }`

## 四、实施步骤

| 步骤 | 内容 | 依赖 | 优先级 |
|---|---|---|---|
| 1 | `powerfs-lock` crate (Rust trait + 类型) | 无 | 高 |
| 2 | `powerfs-lock-net` crate (Rust TLV 编解码) | 1 | 高 |
| 3 | `docs/lock-protocol.md` 协议规范(不入库) | 2 | 高 |
| 4 | `powerfs-lock-filer` (迁入 InodeLeaseManager + 实现 trait) | 1 | 中 |
| 5 | `powerfs-lock-fuse` (从 cache.rs 拆 ClientLeaseState) | 1, 2 | 中 |
| 6 | 改造 fuse.rs 业务路径调 LockManager trait | 5 | 中 |
| 7 | 性能基线测试(现有 vs 模块化后) | 1-6 | 中 |
| 8 | 基于基线提优化方案并讨论 | 7 | 低 |
| 9 | `powerfs-kernel` C 骨架(优化方案确定后) | 3, 8 | 低 |

### 4.1 步骤 5 的拆解风险

从 `cache.rs` 拆 `ClientLeaseState` 是最痛的一步,因为 cache 和 lease 双向依赖。两种策略:

- **激进拆解**: 彻底解开双向依赖,`cache.rs` 只保留纯缓存(EntryState 的 Clean/Dirty/Flushing),lease 独立成 `ClientLeaseState`
- **保守适配器**: 保留 cache.rs 现状,只在外面包一层 `LockManager` 适配器。接口层建好后步骤 6 仍可推进

建议: 步骤 1-4 先做(低风险),步骤 5 单独评估。如果双向依赖解不开,用保守适配器方案。

## 五、优化方向(性能基线后决定)

### 5.1 元数据路径延迟优化(对标 Lockify)

- 本地自宣告所有权: 新建 inode 无历史归属,当前节点本地直接设置自身为 owner,无需同步 RPC
- 异步所有权回执同步: 本地创建完成后后台异步通知父目录 owner
- 适用场景: 低冲突元数据负载(creat/mkdir/mknod)
- 预期收益: ≈6x

### 5.1.1 目录级 Shared Lease（客户端目录内容缓存一致性）

**问题**: `create` 路径的 `entry_exists(parent, name)` 在缓存 MISS 时发 lookup RPC,
导致 `cp N files` 到同一目录变成 2N RPC,IOPS 减半。单客户端场景下此 RPC 完全冗余。

**方案**: 对父目录 acquire **Shared lease**,持有期间信任本地 dentry 缓存,跳过 lookup RPC。
- `opendir`/`readdir` 时 acquire Shared lease(lockify self-declare,后台同步)
- `entry_exists` / `lookup` 在持有目录 lease 时只查缓存,不发 RPC
- `create`/`mkdir`/`unlink`/`rmdir` 修改目录后本地 invalidate dentry 缓存(保持 lease)
- `releasedir` 释放目录 lease
- 其他客户端修改同一目录 → lockify CAS 冲突 → 本地 lease 失效 → 重新发 RPC

**服务端**: 零改动。`InodeLeaseManager::acquire(inode, ...)` 已不区分文件/目录。

**与文件 inode lease 的关系**: 正交。文件 lease 是写入路径 Exclusive,目录 lease 是读取路径
Shared,复用同一个 `FuseLockManager`。

**详细设计**: 见 `shard-routing-no-forward-principle.md` §7。

**预期收益**: 单客户端 `cp 1000 files` IOPS 从 ~12 提升到 ~22(减少一半 RPC)。

### 5.2 高冲突写吞吐优化(对标 SeqDLM)

- Early Grant 预授予锁: 收到旧客户端锁撤销应答后不等脏页落地即提前授予下一个排队锁
- Early Revoke 预撤销锁: 当前持有者未释放时提前向排队节点发送撤销预通知
- SN 全局序列号兜底 IO 顺序,后台异步完成旧节点脏页回写
- 适用场景: 多节点同文件重叠写、高频率锁抢占
- 预期收益: ≈8-10x

### 5.3 Raft 强一致兜底(融合优化)

- Leader 本地分配 SN(乐观),后台批量异步补 Raft 日志
- Leader 切换时新 Leader 从 Raft 日志恢复已提交 SN,未提交的锁操作回滚重做
- 锁变更、租约变更、SN 序列号均写入 Raft 日志,落地即强一致

## 六、专家视角问题审视(基于代码调研)

### 6.1 现状全景: 三套 lease 实现,两套互斥模式

| 位置 | 实现 | 复用 powerfs-lease? | 持久化 | Raft 关联 |
|---|---|---|---|---|
| `powerfs-lease/` | 通用原语: `MemoryLeaseStore<K>` + `LeaseManager` trait + `LeaseGuard` | 是(本身) | 有 `LeasePersistence` trait | 无 |
| `powerfs-volume/src/range_lease.rs` | `StripeKey` + `RangeLeaseManager` | ✅ 复用 | ✅ 用 persistence.rs | 无(volume 无状态) |
| `powerfs-filer/src/inode_lease_manager.rs` | 独立 `InodeLeaseManager`(`RwLock<HashMap>`) | ❌ 重复造轮子 | ❌ 无持久化 | ❌ 不跟随 Raft 切换 |

**关键发现**: filer 和 volume 的 lease **互斥运行**,由客户端配置 `lease_mode = "inode" | "range"` 决定([fuse_client_facade.rs:820](file:///home/portion/powerfs/powerfs-fuse-core/src/fuse_client_facade.rs#L820))。不是两套同时工作,而是二选一。这简化了设计——不需要跨服务锁协调。

### 6.2 七个架构级问题(按严重性排序)

**问题 P1(一致性风险): Filer lease 不持久化,Leader 切换丢锁**

[inode_lease_manager.rs:71-78](file:///home/portion/powerfs/powerfs-filer/src/inode_lease_manager.rs#L71-L78) 注释明确写:
> "Does NOT replicate across Raft; if the Filer leader changes, lease state is lost. Clients retry acquire on the new leader."

问题: Leader 切换瞬间,新 Leader 不知道旧 Leader 授予了哪些 lease。如果客户端 A 持有写 lease 正在写,Leader 切换后客户端 B 能立即获取同一 inode 的 lease——**双写冲突,数据损坏**。注释说"由 Raft `UpdateInodeSizeChunks` 保证最终一致性",但这是事后补救,不是事前防护。写过程中的中间状态可能已经损坏数据。

**问题 P2(技术债): Filer lease 重复造轮子,与 volume lease 接口不一致**

`InodeLeaseManager` 自己写 `RwLock<HashMap>` + token + grace period,完全没复用 `powerfs-lease`。导致:
- 两套 acquire/release/renew API 签名不同
- 客户端 `is_inode_lease_mode()` 分支判断散落 6 处([fuse.rs:5743](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L5743), [fuse.rs:5806](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L5806), [provider_adapter.rs:1212](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs#L1212) 等)
- 无持久化(volume 有,filer 没有)

**问题 P3(性能): 每个 4K write 触发 lease 检查,即便有缓存也增加延迟**

[fuse.rs:4771-4774](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L4771-L4774) 注释:
> "不再在 write 路径显式 acquire/release lease... 首次 write 时 ensure_lease 获取 lease 并缓存,后续 write 复用缓存"

但 `ensure_lease` 仍每个 write 调用,内部做缓存检查(`get_valid_lease_token`)。虽然是快速路径,但每次 write 都要走一次函数调用 + 锁检查。对于顺序大写(1MB chunk = 256 次 4K write),这是 256 次冗余检查。

**问题 P4(死锁风险): lease 获取无全局顺序,跨 inode 可能死锁**

客户端 A: acquire(inode1) → acquire(inode2)
客户端 B: acquire(inode2) → acquire(inode1)

当前 lease 是 per-inode 独立的,没有全局排序。虽然实际场景跨 inode 同时持锁罕见(如 rename),但 rename(dir_a/file, dir_b/file) 会同时涉及两个目录的 lease。如果两个客户端反向 rename,可能死锁。

**问题 P5(可用性): grace period 固定 5 秒,不适配网络环境**

[inode_lease_manager.rs:30](file:///home/portion/powerfs/powerfs-filer/src/inode_lease_manager.rs#L30) `DEFAULT_GRACE_PERIOD_MS = 5000`。低延迟网络(同机架)5 秒太长(写阻塞 5 秒),高延迟网络(跨机房)5 秒可能不够(客户端还活着但续期还没到)。应该动态调整。

**问题 P6(泄漏风险): LeaseGuard Drop 是"尽力而为",网络分区时可能泄漏**

[guard.rs:97-149](file:///home/portion/powerfs/powerfs-lease/src/guard.rs#L97-L149) Drop 通过 weak manager 异步释放。如果网络分区,release RPC 失败,lease 泄漏直到 TTL 过期。TTL 期间其他客户端被阻塞。grace period 之后才能获取,加起来可能阻塞 5-35 秒。

**问题 P7(可观测性): 无锁队列监控,无法定位锁冲突热点**

没有指标暴露: 当前有多少锁等待、平均等待时间、锁持有时长分布、冲突频率。优化时无法定位瓶颈,只能靠日志猜测。

### 6.3 优化方法(按问题对应)

| 问题 | 优化方法 | 阶段 |
|---|---|---|
| P1 Leader 切换丢锁 | lease 状态写 Raft 日志,新 Leader 回放恢复。模块化时预留接口,优化阶段实现 | 优化阶段 |
| P2 重复造轮子 | filer lease 重写为复用 `powerfs-lease`(用 `InodeKey`),统一底层。**模块化阶段就做** | 模块化 |
| P3 write 路径冗余检查 | open 时获取 lease 并绑定到 file handle,write 直接用 handle 关联的 lease,不再每次检查。Early Grant 后 write 路径零锁开销 | 优化阶段 |
| P4 跨 inode 死锁 | lease 获取按 inode 排序(全局哈希序),或用 try-lock + 超时回退 | 优化阶段 |
| P5 固定 grace period | 基于 P99 续期 RTT 动态调整: `grace = max(5s, 3 * p99_renew_rtt)` | 优化阶段 |
| P6 网络分区泄漏 | Fencer token 机制: lease 携带 epoch,Leader 切换时 epoch++,旧 epoch 的 lease 自动失效 | 优化阶段 |
| P7 可观测性差 | 暴露 Prometheus 指标: `lock_wait_count`, `lock_wait_p99_ms`, `lock_hold_avg_ms`, `lock_conflict_rate` | 模块化 |

## 七、彻底重写方案(一次到位)

基于专家视角审视,**模块化阶段就重写 filer lease**(不是保守适配器),因为:
1. 技术债不解决,优化阶段还要再改一遍
2. 复用 `powerfs-lease` 后,P1(持久化)和 P2(接口不一致)一次性解决
3. `powerfs-lease` 已有 `LeasePersistence` trait,filer 接 Raft 持久化只需实现这个 trait

### 7.1 统一 lease 模型

```
                    powerfs-lease (通用原语)
                    - MemoryLeaseStore<K>
                    - LeasePersistence trait
                    - LeaseGuard (RAII)
                         │
            ┌────────────┴────────────┐
            │                         │
     InodeKey (新)              StripeKey (现有)
     group_id = inode           group_id = inode
     conflicts = same inode     conflicts = range overlap
            │                         │
   ┌────────▼────────┐    ┌──────────▼──────────┐
   │ FilerLeaseStore │    │ VolumeLeaseStore    │
   │ (新, 复用)      │    │ (现有, 已复用)       │
   │                 │    │                     │
   │ Persistence:    │    │ Persistence:        │
   │   Raft log       │    │   local RocksDB     │
   │ (新, P1 修复)   │    │   (现有)            │
   └────────┬────────┘    └──────────┬──────────┘
            │                         │
            └────────────┬────────────┘
                         │
              powerfs-lock (统一接口)
              trait LockManager {
                  acquire(inode, mode, range)
                  release(inode, token)
                  renew(inode, token)
                  on_revoke(handler)  // Early Revoke 回调
              }
                         │
         ┌───────────────┴───────────────┐
         │                               │
   FUSE 客户端                       内核客户端 (C)
   (Rust, 调 trait)                  (独立实现,
    inode mode → FilerLeaseStore       同一协议)
    range mode → VolumeLeaseStore
```

### 7.2 模块化阶段必做(一次到位)

| 任务 | 解决问题 | 风险 |
|---|---|---|
| 新建 `powerfs-lock` crate(trait + 类型) | P2 接口统一 | 低 |
| 新建 `powerfs-lock-net` crate(TLV 编解码) | 协议层独立 | 低 |
| **重写 filer lease 复用 `powerfs-lease`** | P2 重复造轮子 | 中(回归测试) |
| **filer lease 接 Raft 持久化** | P1 Leader 切换丢锁 | 中(需选主后加载) |
| 从 cache.rs 拆 `ClientLeaseState` | lease/缓存解耦 | 高(双向依赖) |
| 改造 fuse.rs 用 LockManager trait | 接口统一 | 中 |
| 暴露 Prometheus 锁指标 | P7 可观测性 | 低 |

### 7.3 优化阶段(模块化后,基于基线数据)

| 任务 | 解决问题 | 前置条件 |
|---|---|---|
| Early Grant + Early Revoke | P3 write 路径 + 高冲突吞吐 | 基线数据确认高冲突是瓶颈 |
| Lockify 异步元数据 | 元数据延迟 | 基线数据确认元数据是瓶颈 |
| lease 绑定 file handle | P3 write 路径冗余 | Early Grant 完成后 |
| 跨 inode 获取排序 | P4 死锁 | 出现死锁报告或压力测试触发 |
| 动态 grace period | P5 固定超时 | 监控数据积累 |
| Fencer token(epoch) | P6 网络分区泄漏 | Raft 持久化已完成 |
| SN 分配(Leader 乐观 + Raft 兜底) | Early Grant 的有序性保证 | Early Grant 完成后 |

## 八、高可用性设计(故障隔离 + 独立通道)

### 8.1 故障客户端的危害模式

| 危害类型 | 表现 | 现状影响 |
|---|---|---|
| 频繁宕机 | lease 反复获取/释放,触发 grace period | 其他客户端阻塞 5-35 秒 |
| 慢客户端 | 持有 lease 但响应慢,Early Revoke ACK 迟迟不来 | 锁切换卡顿,影响所有人 |
| Lease churn | 高频获取/释放(buggy 客户端循环 open/close) | 消耗服务端 CPU + 日志带宽 |
| 续期失败累积 | 网络抖动导致续期频繁失败 | 误触发 grace period |
| 死循环持锁 | 客户端 hang 住不释放 | 直到 TTL + grace 才能回收 |

### 8.2 三层防御机制

```
┌─────────────────────────────────────────────────────────┐
│  Layer 1: 客户端健康评分 (ClientHealthScore)            │
│  - 故障次数、续期成功率、lease 持有时长 P99、churn 率   │
│  - 分数: 0-100, < 30 触发 Layer 2/3                     │
└────────┬────────────────────────────────────────────────┘
         │
┌────────▼─────────────────────────────────────────────────┐
│  Layer 2: 自适应限流 (AdaptiveThrottle)                  │
│  - 低分客户端 lease 时长自动缩短 (30s → 5s → 1s)        │
│  - 高频 churn 客户端 acquire 限速 (令牌桶)              │
│  - 非阻塞,只是变慢,不直接拒绝                           │
└────────┬────────────────────────────────────────────────┘
         │
┌────────▼─────────────────────────────────────────────────┐
│  Layer 3: 强制隔离 (Quarantine)                         │
│  - 分数 < 10 且持续 N 次 → 加入隔离池                   │
│  - 隔离期内 lease 请求直接拒绝 (LockError::Quarantined) │
│  - 隔离期可配置 (默认 60s), 期满后分数恢复到 50 重试    │
│  - 类似 GFS 的 client revocation 机制                   │
└─────────────────────────────────────────────────────────┘
```

### 8.3 关键实现要点

**1. 强制 lease 回收(不等 TTL)**

慢客户端不响应 Early Revoke ACK 时,服务端主动判定故障:
- 发出 Revoke 后 2 秒无 ACK → 标记客户端 "unresponsive"
- 强制回收 lease + 触发 Layer 1 扣分
- 新 Leader 直接授予下一个排队者(配合 SN 兜底)

**2. Fencer token 防僵尸客户端**

lease 携带 `epoch`,客户端宕机后重启必须先申请新 epoch:
- 客户端启动时向 Filer 注册获取 `client_epoch`
- lease 请求携带 epoch,旧 epoch 的请求直接拒绝
- 防止僵尸进程(假死)继续写数据

**3. 黑名单机制**

连续 3 次进入隔离池 → 永久黑名单(需管理员解除)。防恶意客户端。

### 8.4 锁消息独立网络通道

**现状问题**: powerfs-net 现有 `CHANNEL_DATA` / `CHANNEL_META` 两个逻辑通道,锁消息混在 CHANNEL_DATA 里:
- Head-of-line blocking: 高负载时 IO 消息挤占锁消息
- 限流误伤: 连接限流时锁消息也被限
- 故障扩散: IO 消息处理慢,锁消息跟着卡

**方案选择**:

| 方案 | 隔离程度 | 复杂度 | 连接数 | 适用场景 |
|---|---|---|---|---|
| A. 逻辑通道 | 中(共享 TCP,独立队列) | 低 | 不变 | 默认 |
| B. 独立 TCP 连接 | 高(完全隔离) | 中 | ×2 | 高负载集群 |
| C. 优先级队列 | 低(共享一切,仅调度) | 高 | 不变 | 轻量级 |

**推荐方案 A + 可选 B**:

```
powerfs-net 现有:
  CHANNEL_DATA  (IO 读写)
  CHANNEL_META  (元数据)

新增:
  CHANNEL_LOCK  (锁消息: acquire/grant/revoke/release/renew/ack)

特性:
  - 独立接收队列 + 独立处理线程池 (不被 IO 阻塞)
  - 独立限流配置 (不受 IO 限流影响)
  - 同一 TCP 连接 (避免连接数爆炸)
  - 配置选项: lock_dedicated_connection=true 时启用方案 B
```

### 8.5 锁消息优先级分层

CHANNEL_LOCK 内部再分优先级:

```
P0 (最高): LockRevokeAck     // ACK 慢了影响锁切换
P0:        LockRevoke        // Early Revoke 通知
P1:        LockGrant         // 授予响应
P2:        LockAcquire       // 新请求
P3:        LeaseRenew        // 续期(可容忍延迟)
P3:        LockRelease       // 释放(可容忍延迟)
```

### 8.6 服务端独立处理线程池

锁消息走独立线程池,不与 IO/元数据处理竞争:
- 防止大 write 阻塞 IO 线程池时,锁消息也跟着卡
- 线程池大小可配置(默认 4,与 IO 线程池解耦)
- 锁消息处理绝不调用阻塞操作(如刷盘),保持快速
- 心跳(KeepConnected)走 CHANNEL_LOCK,避免 IO 拥塞误判客户端宕机

## 九、约束

1. **文档不入库**: 除 README.md 外,设计文档(docs/)不入 git。docs/ 已在 .gitignore
2. **服务端单一**: powerfs-filer 用户态,扩展 DLM 调度,不新建独立服务
3. **客户端双形态**: 同一套服务端同时服务 FUSE(用户态 Rust)和内核态(C)客户端
4. **自研不兼容**: 内核客户端是自研模块,不兼容 GFS2/OCFS2 的 fs/dlm
5. **协议文档化**: 双语言通过 docs/lock-protocol.md 字节级规范协同,不共享代码
6. **一次到位**: 模块化阶段就重写 filer lease 复用 powerfs-lease + 接 Raft 持久化,不留技术债给优化阶段
7. **lease 模式二选一**: inode lease 和 range lease 互斥运行(由客户端配置决定),不需要跨服务锁协调
8. **故障隔离必备**: 模块化阶段必须包含客户端健康评分 + 自适应限流 + 强制隔离三层防御,应对大规模客户端频繁宕机
9. **锁消息独立通道**: 新增 CHANNEL_LOCK 逻辑通道 + 独立处理线程池,锁消息不被 IO 拥塞阻塞
