# PowerFS Data Consistency Design

## 1. 设计背景与核心矛盾

PowerFS 采用**分层一致性模型**来平衡数据正确性与系统性能：

- **强一致路径 (Data Lease Lock)**：保护文件数据相关元数据（size、chunks），通过 Volume 级别的 Stripe (64MB) Lease 锁实现线性强一致。
- **最终一致路径 (Meta CRDT)**：保护文件属性相关元数据（mode、uid/gid、时间戳），通过 CRDT (Conflict-free Replicated Data Type) 的 Delta Sync 实现最终一致。

### 核心矛盾

| 维度 | 强一致 (Lease) | 最终一致 (CRDT) |
|------|---------------|----------------|
| **保护对象** | `inode.size`, `inode.chunks`, `content_size` | `inode.mode`, `uid`, `gid`, `mtime`, `atime`, `ctime`, `nlink` |
| **一致性** | 线性强一致 - 所有客户端立即可见 | 最终一致 - 容忍秒级延迟 |
| **机制** | Volume Stripe Lease 锁 | CRDT Delta Sync + Invalidation |
| **冲突** | 独占互斥 - 写入者阻塞直到获取 Lease | 自动合并 - 并发修改通过 CRDT Merge 解决 |
| **延迟** | 较高（需要 Lease 往返） | 较低（本地修改 + 异步同步） |
| **适用** | 文件数据读写、截断 | chmod/chown/utimes 等属性变更 |

---

## 2. 分层一致性模型

### 2.1 第一层：强一致性路径 (Data Lease Lock)

**适用数据**：`inode.size`, `inode.chunks`, `inode.content_size`, `inode.disk_size`

**一致性要求**：线性强一致。任何写入必须对所有客户端立即可见。

**机制**：Volume Server 端的 Stripe 级独占 Lease 锁。

**完整流程**：

```
FUSE Client (Writer)                     Volume Server                 Filer (Meta)
    |-- 1. Acquire Lease(stripe) -------->|                             |
    |   <-- Lease Granted (token) --------|                             |
    |                                    |                             |
    |-- 2. Write Needle(data) ---------->|                             |
    |   <-- Write OK -------------------|                             |
    |                                    |                             |
    |-- 3. SetAttr(size, chunks) --------------------------------->   |
    |                                    |   Raft: UpdateEntry(      |
    |                                    |     size, chunks)          |
    |                                    |   <-- Commit OK ------------|
    |                                    |                             |
    |-- 4. Release Lease ---------------->|                             |
    |   (triggers Invalidate)            |                             |
    |                                    |                             |
    |-- 5. Invalidate(other clients) ------------------------------->  |
    |                                    |   Broadcast Invalidate     |
```

**关键设计**：
- **Lease 粒度**：Stripe (64MB)。同一文件不同 Stripe 可以并行写（不同 Lease）。
- **Lease 类型**：独占 Lease（写）vs 只读 Lease（读，可多个读者共享）。
- **生命周期**：Acquire → Renew (30s 心跳) → Release。超时未释放则自动回收。
- **冲突处理**：Lease 已被占用时返回 `Error::Busy`，客户端需等待或重试（指数退避）。
- **原子性**：`SetAttr(size, chunks)` 必须在 Release Lease 之前完成，确保数据和元数据一致。

### 2.2 第二层：最终一致性路径 (Meta CRDT)

**适用数据**：`inode.mode`, `uid`, `gid`, `mtime`, `atime`, `ctime`, `nlink`

**一致性要求**：最终一致。允许秒级延迟（POSIX 标准通常允许）。

**机制**：CRDT Delta Sync + Invalidation 通知。

**完整流程**：

```
FUSE Client (chmod)                       Filer (Meta)
    |-- 1. Local modify mode             |
    |-- 2. PushDelta(SetMode) --------->|
    |                                    |   CRDT merge_setattr(
    |                                    |     mode)
    |                                    |   Raft: ApplyDelta(...)
    |                                    |   <-- Commit OK
    |                                    |
    |-- 3. Invalidate(all subscribers) ->|
    |                                    |   Broadcast to all
    |                                    |   subscribed clients
```

**关键设计**：
- **CRDT 类型**：State-based CRDT，每个属性变更表示为一个 Delta。
- **合并策略**：
  - **Mode/Uid/Gid**: LWW (Last Write Wins)，时间戳较新的覆盖较旧的。
  - **Mtime/Ctime**: Max 策略，取最大值。
  - **Atime**: Max 策略，取最大值。
  - **Nlink**: Counter 策略，支持并发 +1/-1，最终结果正确。
- **Invalidation**：Delta 合并完成后，Filer 向所有订阅该 Inode 的客户端广播 Invalidate 消息。
- **Client Refresh**：收到 Invalidate 后，客户端主动触发 getattr 拉取最新元数据。

---

## 3. 核心机制设计

### 3.1 Volume Lease Lock

**位置**：`powerfs-volume/src/lease_manager.rs` (新建)

```rust
pub struct VolumeLeaseManager {
    /// Stripe -> LeaseInfo
    leases: RwLock<HashMap<StripeId, LeaseInfo>>,
    /// Lease timeout duration
    timeout: Duration,
}

pub struct LeaseInfo {
    pub stripe_id: StripeId,
    pub holder: String,       // client_id
    pub token: u64,
    pub acquired_at: Instant,
    pub last_renewed_at: Instant,
    pub lease_type: LeaseType, // Exclusive / Shared
}

pub enum LeaseType {
    Exclusive,  // Write lease - only one holder
    Shared,     // Read lease - multiple holders allowed
}
```

**RPC 接口**：
- `AcquireLease(stripe_id, lease_type, client_id) -> (token, expires_at)`
- `RenewLease(stripe_id, token) -> expires_at`
- `ReleaseLease(stripe_id, token) -> ()`
- `GetLeaseInfo(stripe_id) -> Option<LeaseInfo>`

### 3.2 Meta CRDT Delta

**位置**：`powerfs-filer/src/crdt_delta.rs` (新建)

```rust
pub enum MetaDelta {
    SetMode { inode: u64, mode: u32, timestamp: u64 },
    SetUid  { inode: u64, uid: u32, timestamp: u64 },
    SetGid  { inode: u64, gid: u32, timestamp: u64 },
    SetMtime { inode: u64, mtime: u64, timestamp: u64 },
    SetAtime { inode: u64, atime: u64, timestamp: u64 },
    SetCtime { inode: u64, ctime: u64, timestamp: u64 },
    IncNlink { inode: u64, delta: i32 },
    DecNlink { inode: u64, delta: i32 },
}
```

### 3.3 InodeNotifier (Invalidation 广播)

**位置**：`powerfs-filer/src/inode_notifier.rs` (新建)

```rust
pub struct InodeNotifier {
    /// inode -> set of subscribed client_ids
    subscribers: RwLock<HashMap<u64, HashSet<String>>>,
    /// client_id -> NetConnection handle for sending push messages
    connections: RwLock<HashMap<String, ClientHandle>>,
}

impl InodeNotifier {
    pub fn subscribe(&self, inode: u64, client_id: String, conn: ClientHandle);
    pub fn unsubscribe(&self, inode: u64, client_id: &str);
    pub fn notify(&self, inode: u64, version: u64);
    pub fn disconnect_client(&self, client_id: &str);
}
```

**Invalidate 消息格式**：
```
MsgType = Invalidate (0x0032)
FrameFlags = NOTIFY (one-way, no response expected)
Body:
  - FieldId::Inode   -> u64
  - FieldId::Version -> u64
  - FieldId::Owner   -> inode path (for debugging)
```

### 3.4 客户端通知处理

**位置**：`powerfs-fuse-core/src/invalidate_handler.rs` (新建)

```rust
pub struct InvalidateHandler {
    cache: Arc<MetadataCache>,
    client_id: String,
}

impl InvalidateHandler {
    /// Called by PowerFsNetClient's background reader thread
    pub fn on_invalidate(&self, inode: u64, version: u64) {
        // 1. Check if we have this inode cached
        // 2. Compare version: if remote version > local version, clear cache
        // 3. If remote version <= local, ignore (our cache is newer)
        self.cache.invalidate_if_older(inode, version);
    }
}
```

### 3.5 版本校验机制

在 `MetadataCache` 中添加 version 字段（已完成部分）：

```rust
pub struct CachedEntry {
    // ... existing fields ...
    pub version: u64,  // Added
}

impl MetadataCache {
    /// Invalidate only if remote version is newer than cached version
    pub fn invalidate_if_older(&self, inode: u64, remote_version: u64) {
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(&inode) {
            if remote_version > entry.version {
                entry.size = 0;  // Mark as stale
                entry.version = remote_version;
            }
        }
    }
}
```

---

## 4. 数据流向示例

### 4.1 文件写入 (强一致路径)

```
Client A (Writer)                    Volume Server              Filer
    |-- Acquire Lease (Stripe 0) -->|                          |
    |   (exclusive)                  |                          |
    |                                |                          |
    |-- Write Needle (data) ------->|                          |
    |   <-- Write OK ---------------|                          |
    |                                |                          |
    |-- SetAttr(size=1024) --------------------------------->  |
    |                                |   Raft: size=1024        |
    |                                |   version++               |
    |   <-- SetAttr OK ----------------------------------------|
    |                                |                          |
    |-- Release Lease ------------->|                          |
    |   (triggers invalidation)     |                          |
    |                                |                          |
    |                          +----+----+                     |
    |                          | Invalidate (inode=N)           |
    |                          | Broadcast to all clients       |
    |                          +----+----+                     |
    |                                |                          |
```

### 4.2 文件读取 (Lease 保护)

```
Client B (Reader)                    Volume Server              Filer
    |-- Acquire Lease (Stripe 0) -->|                          |
    |   (shared/read)               |                          |
    |   <-- Lease Granted ----------|                          |
    |                                |                          |
    |                                |   (Atomic read of        |
    |                                |    size + chunks)        |
    |                                |                          |
    |-- Read Needle (data) -------->|                          |
    |   <-- Read OK ---------------|                          |
    |                                |                          |
    |-- Release Lease ------------->|                          |
```

### 4.3 文件权限修改 (最终一致路径)

```
Client A (chmod 644)                Filer
    |-- PushDelta(SetMode) -------->|
    |   mode=0o644                  |   CRDT merge
    |   timestamp=T1                |   mode = LWW(T1)
    |                                |
    |                                |   Raft commit
    |                                |   version++
    |                                |
    |   <-- Delta Accepted ---------|
    |                                |
    |                                |   Invalidate broadcast
    |                                |   (all subscribers)
    |
Client B (stat)                     Filer
    |-- (receives Invalidate)       |
    |-- Clear local cache           |
    |-- getattr(path) ------------>|
    |   <-- Returns mode=0o644 -----|
```

### 4.4 并发 chmod (CRDT 自动合并)

```
Client A (chmod 644, T1)            Filer                    Client B (chmod 755, T2)
    |-- PushDelta(SetMode) -------->|                         |-- PushDelta(SetMode) -------->|
    |   mode=0o644                  |   CRDT merge (LWW)      |   mode=0o755                  |
    |   ts=T1                       |   mode = max(           |   ts=T2                      |
    |                                |     (0o644, T1),       |                                |
    |                                |     (0o755, T2))      |                                |
    |                                |   = 0o755 (T2 is later)|                                |
    |                                |                         |                                |
```

---

## 5. 实施路线图

### Phase 1: Lease 机制基础实现

| 步骤 | 任务 | 模块 | 状态 |
|------|------|------|------|
| 1.1 | 实现 `RangeLeaseManager` (acquire/release/renew) | `powerfs-volume` | ✅ 已完成 |
| 1.2 | 添加 `AcquireLease` / `ReleaseLease` RPC 消息类型处理 | `powerfs-volume` NetHandler | ✅ 已完成 |
| 1.3 | 在 `VolumeClient` 中实现 `acquire_lease`/`release_lease_remote` | `powerfs-fuse-core` | ✅ 已完成 |
| 1.4 | 在 `FuseClientFacade`/`SyncFuseClientFacade` 中添加 Lease 接口 | `powerfs-fuse-core` | ✅ 已完成 |
| 1.5 | 重构 FUSE write 路径：Acquire Lease → Write → Flush → Release Lease | `powerfs-fuse` | ✅ 已完成 |
| 1.6 | 重构 FUSE read 路径：Acquire Read Lease → Read → Release Lease | `powerfs-fuse` | ✅ 已完成（需修复lease释放BUG） |
| 1.7 | 修复 read 路径 lease 未释放问题 | `powerfs-fuse` | ✅ 已完成 |
| 1.8 | 添加 Lease 单元测试和集成测试 | `powerfs-volume` | ✅ 已完成 |
| 1.9 | Lease 续租心跳自动化 | `powerfs-fuse-core` | ✅ 已完成 |

### Phase 2: SetAttr 路径拆分

| 步骤 | 任务 | 模块 | 状态 |
|------|------|------|------|
| 2.1 | 拆分 `SetAttr` 为 `SetAttrData` (强一致) 和 `SetAttrMeta` (最终一致) | `powerfs-net` | ✅ 已完成 |
| 2.2 | 实现 `MetaDelta` CRDT 结构和合并逻辑 | `powerfs-filer` | ✅ 已完成 |
| 2.3 | 在 Filer `MetaShardManager` 中分别处理两条路径 | `powerfs-filer` | ✅ 已完成 |
| 2.4 | 实现 `SetAttrData` 的 Raft 强一致提交 | `powerfs-filer` | ✅ 已完成 |
| 2.5 | 实现 `SetAttrMeta` 的 CRDT Delta 提交 | `powerfs-filer` | ✅ 已完成 |
| 2.6 | 添加 CRDT 合并单元测试 (16 tests) | `powerfs-filer` | ✅ 已完成 |

### Phase 3: Invalidation 通知机制

| 步骤 | 任务 | 模块 | 状态 |
|------|------|------|------|
| 3.1 | 添加 Server→Client 推送通道到 `ServerConnectionManager` | `powerfs-net` | ✅ 已完成 |
| 3.2 | 实现 `InodeNotifier` (订阅表 + 广播) | `powerfs-filer` | ✅ 已完成 |
| 3.3 | 在 Filer 的 NetHandler 中接入 `InodeNotifier` | `powerfs-filer` | ✅ 已完成 |
| 3.4 | 在 `PowerFsNetClient` 中添加通知接收处理 | `powerfs-net` | ✅ 已完成 |
| 3.5 | 实现 `InvalidateHandler` 处理服务端推送 | `powerfs-fuse` | ✅ 已完成 |
| 3.6 | 在 `MetadataCache` 中实现 version 比对 | `powerfs-fuse` | ✅ 已完成 |
| 3.7 | 添加端到端 Invalidation 测试 | 测试 | ✅ 已完成 |

### Phase 4: 优化与完善

| 步骤 | 任务 | 模块 | 状态 |
|------|------|------|------|
| 4.1 | Lease 续租心跳自动化 | `powerfs-fuse-core` | ✅ 已完成 |
| 4.2 | Lease 超时回收与故障转移 | `powerfs-volume` | ✅ 已完成 |
| 4.3 | CRDT Delta 定期压缩与清理 | `powerfs-filer` | ✅ 已完成 |
| 4.4 | Invalidation 确认机制（可选） | `powerfs-net` | 待实现 |
| 4.5 | 缓存 TTL 兜底策略（1-2秒） | `powerfs-fuse` | ✅ 已完成 |
| 4.6 | CRDT 后台维护任务（Tombstone/Delta Log 压缩） | `powerfs-filer` | ✅ 已完成 |

---

## 6. 现有代码差距分析

| 模块 | 现有实现 | 差距 / 待完善 |
|------|----------|--------------|
| **powerfs-volume** | ✅ `RangeLeaseManager` 已实现（acquire/release/renew/validate），支持 holder 管理和故障转移 | ✅ 已完善过期 Lease 自动清理的后台任务 |
| **powerfs-fuse-core** | ✅ `VolumeClient.acquire_lease()`/`release_lease_remote()` 已实现，有续租心跳线程 | ✅ write/read 路径已接入 Lease |
| **powerfs-fuse** | ✅ `FuseClientFacade`/`SyncFuseClientFacade` Lease 接口已就绪，写入前后获取/释放 Lease | ✅ write/read 路径已接入 Lease，TTL 兜底策略已实现 |
| **powerfs-filer** | ✅ `MetaShardManager` 已拆分 `setattr_data` / `setattr_meta`。有 `InodeNotifier`。 | ✅ CRDT compact 方法已实现并接入后台维护任务（Tombstone/Delta Log 压缩） |
| **powerfs-net** | ✅ Server→Client Push 支持已添加，有 `NotificationHandler` 机制 | ⚠️ 需添加 Invalidation 确认机制（可选） |
| **crdt_orset** | ✅ CRDT Delta 定义和合并逻辑已完善，支持批量应用和压缩 | ✅ 已接入后台维护任务定期清理 |
| **MetadataCache** | ✅ 有 `version` 字段和版本比对逻辑，支持 Invalidation，有 TTL 兜底 | ✅ 已完善 |

**当前重点任务**：
1. ~~端到端 Invalidation 测试 (Phase 3.7)~~ ✅ 已完成
2. ~~CRDT compact 后台任务接入 Filer (Phase 4.3 补充)~~ ✅ 已完成
3. 在真实 FUSE 挂载环境下验证 Lease 锁机制
4. Phase 4.4 Invalidation 确认机制（可选）

---

## 7. 关键约束与原则

1. **数据操作强一致**：size/chunks 更新必须在 Lease 保护下原子完成。
2. **属性操作最终一致**：mode/uid/gid 等属性通过 CRDT 自动合并，容忍秒级延迟。
3. **Invalidation 可靠性**：通知丢失时通过 TTL (1-2s) 兜底，确保最终一致。
4. **版本号单调递增**：每次修改（数据或属性）都递增 version 字段，客户端据此判断缓存新鲜度。
5. **Lease 故障安全**：客户端崩溃时 Lease 自动超时回收，不会永久阻塞。
6. **CRDT 无冲突**：所有并发修改都能被 CRDT 正确合并，不需要锁。
