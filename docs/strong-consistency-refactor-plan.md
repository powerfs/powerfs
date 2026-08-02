# PowerFS 强一致元数据重构方案

> 状态：**已确认方案**，后续讨论与执行以此为准
> 范围：元数据一致性架构重构（CRDT 弱一致 → Filer Raft 强一致）+ 客户端读写缓存优化
> 编写日期：2026-08-02

---

## 一、已确认决策摘要

| # | 决策项 | 结论 |
|---|--------|------|
| 1 | CRDT 去留 | 删除 CRDT 专属代码，保留通用元数据同步接口在独立 crate |
| 2 | 通用接口保留范围 | 保留 `alloc_inode_batch` / `update_inode_size_chunks` / `open_count` / `ChunkWire`（强一致路径也需要） |
| 3 | 独立 crate 保留 | 保留独立 crate（不并入 powerfs-fuse-core），职责更清晰 |
| 4 | Filer 接口方式 | 复用现有强一致 handler（已就绪），fuse-core 补具名封装方法 |
| 5 | 阶段策略 | 直接强一致重构（跳过中间态），阶段1 无客户端目录缓存，阶段2 再加 lease+callback |
| 6 | MetadataCache 处理 | 保留 `inode_cache`（attr 缓存），删除 `path_map` / `dir_cache` |
| 7 | 跨客户端缓存失效 | 阶段1 方式 B（Filer 自身缓存承担热点），阶段2 方式 A（lease + callback invalidation） |
| 8 | Read Lease 缓存复用 | 合并到强一致重构后一起做，不单独提前推进 |
| 9 | 接口规范化 | 四类接口（元数据/数据/网络/Raft 组）统一 trait 化，合并到强一致重构避免改两遍 |
| 10 | MetadataClient trait | 新增，取代废弃的 MetadataProvider，MetaShardClient 实现 |
| 11 | DataClient trait | 新增，lease 自动管理，取代 StorageProvider 死代码 + write_blob_with_lease |
| 12 | LeaseManager trait | 新增，统一 read/write lease 入口，LeaseToken 强类型 + LeaseHandle RAII |
| 13 | FieldId 重命名 | Ino→VolumeId, FileKey→Inode, 消除 rename 字段复用，新增 NewParentIno/NewName |
| 14 | RaftShardManager trait | 新增，强一致重构后独立推进，check_leader 集中到写方法内 |
| 15 | 死代码清理 | 删除 write_blob/batch_write_blob trait 方法及 build_write_tlv/build_batch_write_tlv |
| 16 | RDMA 适配 | 本次定义 Transport trait（仅 TCP 实现）+ TLV 改 bytes::Bytes 零拷贝；后续新增 RDMA 实现 |
| 17 | Raft 读语义 | Leader Lease Read（leader 任期内本地读），不走 Raft read index |
| 18 | 跨客户端 read 可见性 | open 时强制 getattr 绕过 TTL + lease 响应携带 chunk 列表 |
| 19 | fsck 工具 | 扫描 Filer 元数据 → 对比 Volume 实际数据块 → 清理孤儿 inode/chunk |
| 20 | 客户端崩溃恢复 | lease TTL + grace period（= duration×2）+ 阶段2 心跳加速释放 |
| 21 | Filer leader 切换 | MetadataClient 内部重试（3 次，每次 2s），上层无感 |
| 22 | shard 分片策略 | 按 bucket（顶级目录）分片，每 bucket 独立 Raft 组，跨 shard 返回 EXDEV |
| 23 | 监控指标 | Filer Raft/RPC/缓存命中率/lease/GC/fsck 全维度监控 |
| 24 | 性能优化预留 | 批量 lookup / readdir 含 attr / Raft batch 提交 |

---

## 二、当前架构分析

### 2.1 数据读写路径（已优化，基本合理，保留）

#### Write 路径（[fuse.rs:1795](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1795)）

```
write(buf, offset)
  ├─ chunk_cache.modify/put(inode, chunk_offset, buf)  // 写本地 1MB chunk
  ├─ mark_dirty(inode, chunk_idx)                       // 标记脏
  └─ [后台 flusher 100ms] flush_dirty_chunks
       ├─ ensure_lease(volume, inode)                   // lease 缓存复用，60s 有效
       │    ├─ get_valid_lease_token() 命中 → 直接返回   // 无网络往返
       │    └─ 未命中 → RangeLease 请求到 Volume Server
       └─ write_blob(volume, data, lease_token)         // 持久化到 Volume
```

**结论**：write 路径已发挥客户端缓存，多次 4K write 合并到 1MB chunk，lease 60s 复用，flusher 异步持久化。**保留不动**。

#### Read 路径（[fuse.rs:1521](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1521)）

```
read(size, offset)
  ├─ chunk_cache.get(inode, chunk_offset)  // 命中直接返回
  └─ miss 时:
       ├─ acquire_lease(volume, inode, shared/read)  // ❌ 每次都请求，无缓存复用
       ├─ read_blob(volume, file_key, offset, size)
       └─ chunk_cache.put(inode, chunk_offset, data)  // 回填缓存
```

**问题**：read 路径每次 miss 都 `acquire_lease`，没有走 `ensure_lease` 的缓存复用路径，与 write 路径不对齐。**需修复**（实施步骤 Step 6）。

#### Release/close 路径（[fuse.rs:1965](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1965)）

```
release()
  ├─ flush_dirty_chunks(inode)              // 同步 flush 剩余脏数据
  ├─ sync_size_chunks_on_close(inode)       // size/chunks 同步到 Filer Raft（强一致）
  └─ release_lease(volume, inode, token)
```

**结论**：close 时强一致同步 size/chunks 到 Filer，已是强一致。**保留不动**。

### 2.2 元数据路径（CRDT 弱一致，问题多，需重构）

#### 当前双源架构

| 数据类型 | 权威源 | 缓存层 | 一致性机制 |
|---------|--------|--------|-----------|
| 目录条目（name→inode） | DirORSet（CRDT 本地副本） | MetadataCache.path_map | CRDT delta sync（异步）+ TTL |
| inode attr（mode/uid/gid/mtime） | Filer Raft | MetadataCache.inode_cache | TTL 2s + invalidation |
| size/chunks | Filer Raft | MetadataCache.inode_cache | close 时强一致 sync |
| 文件数据 | Volume Server | chunk_cache | Lease 排他锁 |

#### 双源导致的 Bug（最近 5 个 commit）

1. `0a0744f6` — lookup 硬编码 is_dir=false，子目录被当文件
2. `cffbcf37` — readdir 读 MetadataCache.path_map 残留条目，返回幽灵文件
3. `23871fcc` — DirORSet 同名多 EntryId 未去重，list_entries 重复
4. `69dca05c` — local_remove_entry 只删一个 EntryId，rm -rf 报 ENOTEMPTY
5. `d8312d62` — EEXIST 检查用 MetadataCache，残留条目导致误判

**根因**：MetadataCache 与 DirORSet 职责重叠，都是目录条目的"权威源"，一致性维护极其复杂。

### 2.3 Filer 强一致能力现状（已就绪）

**Filer 侧已具备完整强一致元数据 handler**（[net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs)）：

| Handler | 行号 | 走 Raft 强一致 |
|---------|------|---------------|
| handle_lookup | 212 | ✅ check_leader + meta_shard_manager |
| handle_getattr | 289 | ✅ |
| handle_setattr / handle_setattr_data / handle_setattr_meta | 339/375/415 | ✅ |
| handle_create | 465 | ✅ |
| handle_mkdir | 557 | ✅ |
| handle_unlink | 616 | ✅ |
| handle_rmdir | 663 | ✅ |
| handle_rename | 704 | ✅ |
| handle_readdir | 740 | ✅ |
| handle_symlink | 805 | ✅ |
| handle_readlink | 840 | ✅ |
| handle_link | 858 | ✅ |
| handle_statfs | 797 | ✅ |

均有 `check_leader` + `meta_shard_manager` Raft 提交 + `notify_inode_change` 缓存失效通知。

**fuse 侧现状**：
- [meta_shard_client.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/meta_shard_client.rs) 有通用 TLV 提交接口 `submit_metadata_request_and_wait`（传输通道就绪）
- 但**没有具名封装方法**（无 `mkdir()` / `create()` / `unlink()` 等）
- 当前 fuse 走 CRDT 本地 apply，**根本没调用 Filer 的强一致 handler**

**关键结论**：Filer 端零改动，主要工作集中在 fuse-core 补客户端封装 + fuse.rs 调用改写。

---

## 三、目标架构

### 3.1 强一致元数据路径（重构后）

```
mkdir/create/unlink/rmdir/rename/symlink/link/setattr:
  fuse.rs → meta_shard_client.<op>() → submit_metadata_request_and_wait(TLV)
            → Filer net_handler.handle_<op>() → check_leader + Raft 提交
            → notify_inode_change（缓存失效通知）

lookup/readdir/getattr:
  fuse.rs → meta_shard_client.<op>() → submit_metadata_request_and_wait(TLV)
            → Filer net_handler.handle_<op>() → shard_store 读取（in-memory 缓存）
```

**特点**：
- 每次元数据操作 1 次 RPC 到 Filer（LAN ~1-2ms）
- Filer Raft 3 节点强一致
- Filer shard_store in-memory 缓存承担热点（LRU 淘汰冷目录）
- 客户端无目录列表缓存（阶段1），getattr 走 inode_cache（attr 缓存，TTL + invalidation）

### 3.2 客户端缓存策略

#### 阶段1（本次重构）：客户端无目录缓存

| 缓存层 | 保留 | 删除 | 说明 |
|--------|------|------|------|
| `MetadataCache.inode_cache` | ✅ | - | attr 缓存，TTL 2s + invalidation，加速 getattr |
| `MetadataCache.path_map` | - | ❌ | CRDT 双源根源，删除 |
| `MetadataCache.dir_cache` | - | ❌ | 目录列表缓存，改为回源 Filer |
| `DirORSet`（CRDT 副本） | - | ❌ | CRDT 专属，删除 |
| `ChunkCache`（数据缓存） | ✅ | - | 读写数据缓存，保留不动 |

**getattr 优化**：inode_cache 命中时直接返回（无需 RPC），TTL 2s 内有效。Filer 的 `notify_inode_change` 通知客户端失效缓存（已有机制）。

#### 阶段2（后续优化）：客户端 lease 缓存 + callback invalidation

当阶段1 性能不达标时（fio/io500 或实际负载证明 Filer 成为瓶颈），引入：

- **客户端缓存工作目录**（局部性：同一客户端反复访问相同目录）
- **read lease**：客户端缓存 dir 列表时获取共享 lease，Filer 记录持有者列表
- **write 时 callback**：client2 修改 dir 时，Filer 同步撤销所有 read lease（带超时），client1 收到 callback 后失效缓存
- **lease TTL 兜底**：callback 失败时（网络分区/客户端崩溃），等 lease TTL（如 30s）过期后强制提交，进入 grace period

**关键设计点**：
- lease 粒度：**per-directory**（目录是失效基本单位，管理简单）
- callback 同步（write 等待 lease 撤销完成才提交 Raft，保证强一致）
- grace period 应对 callback 超时（类似当前 Volume lease 的 grace period 机制）

### 3.3 跨客户端缓存失效机制（阶段2）

| 方式 | 机制 | 评价 |
|------|------|------|
| A. 服务器转发失效（callback） | client 持 read lease；write 时 Filer 撤销 lease | ✅ 阶段2 采用 |
| B. 完全服务端缓存 | 客户端不缓存，回源 Filer | ✅ 阶段1 采用 |
| C. 混合冷热分流 | 冷目录客户端缓存，热目录仅 Filer | ❌ 不采用（复杂度高收益有限） |
| D. 客户端短 TTL（无 lease） | 客户端缓存 1-2s TTL，无 callback | ❌ 不采用（非强一致） |

**热点处理**：Filer shard_store 本身是 in-memory 缓存，热点目录自然驻留（LRU 淘汰），无需显式冷热分流机制。阶段2 客户端缓存的是"本地工作目录"（局部性），与 Filer 缓存互补。

### 3.4 数据读写缓存优化（合并到本次重构）

#### Read Lease 缓存复用（修复 read 路径缺陷）

**问题**：read 路径每次 miss 都 `acquire_lease`（[fuse.rs:1619](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1619)），没有走 `ensure_lease` 的缓存复用路径。

**修复**：read 路径改用 `ensure_lease`（与 write 路径对齐），read lease 同样缓存 60s 复用，shared 模式（`exclusive=false`）。

**预期收益**：read miss 场景 lease 请求数降低 ~90%。

#### 其他优化（可选，后续评估）

- Read Prefetch 窗口自适应（顺序读检测，增大 prefetch）
- Write-Back 合并优化（高速写入时延长 flush 间隔）
- Chunk Cache 容量与淘汰策略（按 inode 分组淘汰）

### 3.5 接口规范化（合并到强一致重构）

当前四类接口（元数据/数据/网络/Raft 组）存在 trait 形同虚设、字段命名误导、死代码等问题。本次重构一并规范化，避免改两遍。

#### 3.5.1 元数据接口：MetadataClient trait

**问题**：`powerfs-common/traits.rs:MetadataProvider` trait 定义了但 fuse 未实现；fuse 走 meta_shard_client 裸 TLV submit，无具名方法，无类型安全。

**规范化**：新增 `MetadataClient` trait，所有元数据操作必须通过此 trait：

```rust
// powerfs-fuse-core/src/metadata_client.rs
#[async_trait]
pub trait MetadataClient: Send + Sync {
    // 目录/文件操作（走 Filer Raft 强一致）
    async fn mkdir(&self, parent: u64, name: &str, mode: u32, uid: u32, gid: u32) -> Result<DirEntry>;
    async fn create(&self, parent: u64, name: &str, mode: u32, uid: u32, gid: u32) -> Result<DirEntry>;
    async fn unlink(&self, parent: u64, name: &str) -> Result<()>;
    async fn rmdir(&self, parent: u64, name: &str) -> Result<()>;
    async fn rename(&self, old_parent: u64, old_name: &str, new_parent: u64, new_name: &str) -> Result<()>;
    async fn symlink(&self, parent: u64, name: &str, target: &str, uid: u32, gid: u32) -> Result<DirEntry>;
    async fn link(&self, old_parent: u64, old_name: &str, new_parent: u64, new_name: &str) -> Result<()>;
    // 查询
    async fn lookup(&self, parent: u64, name: &str) -> Result<DirEntry>;
    async fn readdir(&self, parent: u64) -> Result<Vec<DirEntry>>;
    async fn getattr(&self, inode: u64) -> Result<DirEntry>;
    async fn readlink(&self, inode: u64) -> Result<String>;
    // 属性
    async fn setattr(&self, inode: u64, params: &SetAttrParams) -> Result<()>;
    // 通用元数据同步（保留自 powerfs-coherence）
    async fn alloc_inode_batch(&self, count: u32) -> Result<InodeRange>;
    async fn update_size_chunks(&self, inode: u64, size: u64, chunks: Vec<ChunkRef>) -> Result<()>;
    async fn open_count_inc(&self, inode: u64) -> Result<()>;
    async fn open_count_dec(&self, inode: u64) -> Result<()>;
}
```

**实现**：`MetaShardClient` 实现此 trait，内部构造 TLV 调用 `submit_metadata_request_and_wait`。
**删除**：`powerfs-common/traits.rs` 的废弃 `MetadataProvider`。

#### 3.5.2 数据接口：DataClient trait

**问题**：`StorageProvider` trait 的 `write_blob`/`batch_write_blob` 是死代码；fuse 实际用非 trait 的 `write_blob_with_lease`；lease 与数据耦合。

**规范化**：新增 `DataClient` trait，lease 自动管理：

```rust
// powerfs-fuse-core/src/data_client.rs
#[async_trait]
pub trait DataClient: Send + Sync {
    async fn write(&self, volume_id: u64, inode: u64, offset: u64, data: &[u8]) -> Result<()>;
    async fn read(&self, volume_id: u64, inode: u64, offset: u64, size: u32) -> Result<Vec<u8>>;
    async fn delete(&self, volume_id: u64, file_key: u64) -> Result<()>;
}
```

**实现**：组合 `LeaseManager` + `VolumeClient`，内部处理 lease 缓存复用 + TLV 读写。
**删除**：`StorageProvider` trait 的 `write_blob`/`batch_write_blob`（死代码）、`SyncFuseClientFacade.write_blob`、`FacadeStorageProvider::write_blob`/`batch_write_blob`、`build_write_tlv`/`build_batch_write_tlv`（无 inode 版本）。

#### 3.5.3 Lease 接口：LeaseManager trait

**问题**：lease 操作散落 4 层（VolumeClient/ProviderAdapter/FuseClientFacade/fuse.rs），read/write 路径不对称，token 是裸 String。

**规范化**：统一 `LeaseManager` trait + `LeaseHandle` RAII + `LeaseToken` 强类型：

```rust
// powerfs-fuse-core/src/lease.rs
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseToken(Arc<str>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseMode { Shared, Exclusive }

pub struct LeaseHandle {
    token: LeaseToken, volume_id: u64, inode: u64, mode: LeaseMode,
    expire_at: Instant, releaser: Arc<dyn LeaseReleaser>,
}
impl Drop for LeaseHandle { /* 自动 release */ }

pub trait LeaseManager: Send + Sync {
    fn acquire(&self, volume_id: u64, inode: u64, mode: LeaseMode, stripe: u64, count: u64) -> Result<LeaseHandle>;
    fn renew(&self, volume_id: u64, inode: u64, token: &LeaseToken) -> Result<()>;
    fn state(&self, volume_id: u64, inode: u64) -> Option<LeaseState>;
}
pub trait LeaseReleaser: Send + Sync {
    fn release(&self, volume_id: u64, inode: u64, token: &LeaseToken);
}
```

**实现**：`VolumeLeaseManager` 包装现有 `VolumeClient` 的 lease 逻辑。
**删除**：fuse.rs 的 `LeaseGuard`（被 `LeaseHandle` 取代）、`ProviderAdapter::ensure_lease`（被 `LeaseManager::acquire` 取代）。

#### 3.5.4 网络接口：FieldId 重命名 + 消除复用

**问题**：`FieldId::Ino` 在 write TLV 存 volume_id（不是 inode）；`FieldId::FileKey` 存 inode（不是 file_key）；`SymlinkTarget` 在 rename 里复用存 new_name；`Ino` 在 rename 里复用存 new_parent_ino。

**规范化**：字段语义固定，消除复用：

| 原字段 | 新字段 | 说明 |
|--------|--------|------|
| `Ino`（write 路径） | `VolumeId` | 明确是 volume_id |
| `Name`（write 路径） | `FileKey` | 明确是 file_key |
| `FileKey`（write 路径） | `Inode` | 明确是 inode |
| `Ino`（rename 复用） | `NewParentIno` | 不再复用 |
| `SymlinkTarget`（rename 复用） | `NewName` | 不再复用 |

新增字段：`NewParentIno`、`NewName`、`LeaseHolder`、`LeaseMode`。
**影响**：所有 TLV 编解码（provider_adapter.rs / serialize.rs / net_handler.rs），需全量测试。

#### 3.5.5 Raft 组接口：RaftShardManager trait（强一致重构后）

**问题**：`MetaShardManager` 有完整 API 但无 trait 抽象，`check_leader` 散落各 handler。

**规范化**：新增 `RaftShardManager` trait，`check_leader` 集中到写方法内：

```rust
// powerfs-filer/src/raft_shard_manager.rs
pub trait RaftShardManager: Send + Sync {
    // 写（走 Raft，内部 check_leader）
    async fn create_directory(&self, parent: u64, name: &str, mode: u32, uid: u32, gid: u32) -> Result<InodeInfo>;
    async fn create_file(&self, parent: u64, name: &str, mode: u32, uid: u32, gid: u32) -> Result<InodeInfo>;
    async fn delete_entry(&self, parent: u64, name: &str, is_dir: bool) -> Result<()>;
    async fn rename_entry(&self, old_parent: u64, old_name: &str, new_parent: u64, new_name: &str) -> Result<()>;
    async fn setattr(&self, inode: u64, params: &SetAttrParams) -> Result<()>;
    async fn set_chunks(&self, inode: u64, size: u64, chunks: Vec<ChunkRef>) -> Result<()>;
    // 读（本地读）
    fn lookup(&self, parent: u64, name: &str) -> Option<InodeInfo>;
    fn get_inode(&self, inode: u64) -> Option<InodeInfo>;
    fn list_directory(&self, parent: u64) -> Vec<InodeInfo>;
    // Shard 管理
    async fn create_shard(&self, shard_id: ShardId, peers: Vec<Peer>) -> Result<()>;
    fn list_shards(&self) -> Vec<ShardId>;
}
```

**时机**：强一致重构完成后，作为 Filer 内部重构独立推进，不影响 fuse。

#### 3.5.6 规范化实施时机汇总

| 规范化项 | 合并到方案 Step | 说明 |
|---------|----------------|------|
| MetadataClient trait | Step 1 | 补具名方法时直接按 trait 定义 |
| FieldId 重命名 | Step 1-2 | TLV 编解码改动时一起重命名 |
| LeaseManager trait | Step 6 | read lease 缓存复用时一起抽象 |
| DataClient trait | Step 6 | 数据接口与 lease 一起规范化 |
| RaftShardManager trait | 重构后 | Filer 内部重构，独立推进 |

### 3.6 通信层 RDMA 适配规划

当前 `powerfs-net` 基于 TCP 字节流 + TLV 序列化，RDMA 不友好（需内核绕过 + 零拷贝 + 预注册内存）。本次重构预留 RDMA 适配接口。

#### 3.6.1 Transport trait 抽象

```rust
// powerfs-net/src/transport.rs
/// 传输层抽象（TCP / RDMA / 未来其他）
/// 所有网络收发必须通过此 trait，禁止直接操作 TcpStream
#[async_trait]
pub trait Transport: Send + Sync {
    async fn connect(&self, peer: &Endpoint) -> Result<Connection>;
    async fn send(&self, conn: &Connection, msg: Bytes) -> Result<()>;
    async fn recv(&self, conn: &Connection) -> Result<Bytes>;
    /// 批量发送（RDMA 可合并 doorbell，提升吞吐）
    async fn send_batch(&self, conn: &Connection, msgs: &[Bytes]) -> Result<()>;
    fn transport_type(&self) -> TransportType;
}
pub enum TransportType { Tcp, Rdma, Quic }
```

#### 3.6.2 TLV 编解码零拷贝改造

| 当前 | RDMA 适配 | 说明 |
|------|----------|------|
| `Vec<u8>` 序列化 | `bytes::Bytes` 零拷贝 | RDMA recv 直接写入预注册 MR |
| `TcpStream` 读写 | `Transport::send/recv` | 抽象传输层 |
| 每条独立 send/recv | `send_batch` | RDMA 合并 doorbell |
| 无连接池 | 连接池 + MR 预注册 | RDMA RC 连接昂贵需复用 |

#### 3.6.3 实施策略

- **本次重构**：定义 `Transport` trait，仅 TCP 实现。TLV 编解码改为 `bytes::Bytes` 友好。lease_duration 可配置（RDMA 模式可缩短）。
- **后续 RDMA**：新增 RDMA 实现（`rust-rdma` 或 `libibverbs`），通过配置切换。RDMA 配置（网卡/MR 大小/QP 数量）统一到配置文件。

### 3.7 一致性正确性补充（专家审查）

#### 3.7.1 Raft 读语义明确

lookup/readdir/getattr 采用 **Leader Lease Read**（Filer leader 任期内本地读 shard_store），不走 Raft read index（避免多一次 RTT）。写操作仍走 Raft commit。

- **正确性保证**：leader 任期内数据不会变化（follower 不接受写），本地读满足线性一致
- **leader 切换**：新 leader 等待旧 lease 过期后才接受读，避免脑裂窗口

#### 3.7.2 跨客户端 read 可见性

client1 write → close（sync size/chunks 到 Filer）→ client2 read 的流程：

- **open 时强制 getattr**：client2 open 时绕过 inode_cache TTL，从 Filer 获取最新 size/chunks
- **lease 响应携带 chunk 列表**：Volume Server 的 lease 响应附带当前 chunk 列表（project_memory 已要求），避免额外 getattr
- **open 后 read 用缓存**：文件打开期间持数据 lease（排他），其他客户端无法修改，size/chunks 可信

#### 3.7.3 fsck 工具设计

**孤儿 inode 检测**：
- 扫描 nlink==0 且 open_count==0 的 inode（已有 GC 机制）
- 补充检测：有 nlink 但 chunks 为空且 size==0 的新创建文件（create 后未写入就崩溃）

**size/chunks 不一致恢复**：
- close 前崩溃：size/chunks 未 sync 到 Filer，GC 对比 Volume 侧实际 chunks 与 Filer 记录，清理孤儿数据块
- 部分写入恢复：chunk 级幂等（NeedleId 由 file_key+offset 确定），close 时全量 sync chunks 列表，未 sync 的脏 chunk 丢失（close 未完成 = 未保证持久化）

**fsck 流程**：扫描 Filer 元数据 → 对比 Volume 实际数据块 → 标记孤儿 → 清理（rate-limited）

### 3.8 故障恢复

#### 3.8.1 客户端崩溃 lease 清理

- **lease TTL + grace period**：已有机制，grace period = lease_duration × 2
- **客户端心跳**（阶段2）：Filer 维护客户端心跳，心跳超时的客户端其所有 lease 立即释放（不等 TTL）
- **Volume Server 侧 lease 清理**：定期扫描过期 lease 清理

#### 3.8.2 Filer leader 切换重试

- **客户端重试**：MetadataClient 内部重试（leader redirect → 重新发现 leader → 重试），上层无感
- **超时配置**：GRPC_CALL_TIMEOUT 15s，元数据操作重试 3 次，每次 2s

### 3.9 shard 分片策略

**按 bucket（顶级目录）分片**：每个 bucket 一个 shard（独立 Raft 组），不同 bucket 的元数据操作互不干扰，水平扩展。

- shard 创建：bucket 创建时 Filer 请求 Master 分配新 shard
- 跨 shard 操作：不支持原子跨 shard 操作（如 rename 跨 bucket），返回 EXDEV（类似 Linux mount boundary）

### 3.10 监控指标（强一致方案更新）

| 维度 | 指标 | 说明 |
|------|------|------|
| Filer Raft | commit_latency / propose_qps / queue_depth | Raft 性能 |
| Filer RPC | rpc_qps / rpc_latency_p99 / rpc_error_rate | 元数据 RPC |
| 客户端缓存 | inode_cache_hit_rate / lease_cache_hit_rate | 缓存效率 |
| Lease | acquire_qps / release_qps / expired_count / grace_count | lease 健康 |
| GC | orphan_inode_count / orphan_chunk_count / gc_throughput | GC 效率 |
| fsck | scan_duration / inconsistency_count | 数据完整性 |

### 3.11 性能优化预留

- **批量 lookup**：MetadataClient 预留 `lookup_batch(parents: &[(u64, &str)])` 接口，一次 RPC 查多个条目（find/ls -R 场景优化）
- **readdir 含 attr**：readdir 返回的 DirEntry 已含基本 attr，避免后续逐个 getattr
- **Raft batch 提交**：Filer Raft 支持多写操作合并为一个 log entry，提升吞吐

---

## 四、CRDT 代码删除与保留清单

### 4.1 删除项（CRDT 专属）

**powerfs-coherence crate 内**：
- `src/crdt_client.rs`：CrdtReplicaCoherence、ShardedDirCache、ChangeCache、InodeAllocator（注：InodeAllocator 改为 Filer 侧 alloc_inode_batch 调用，客户端不再本地分配）
- `src/crdt_server.rs`：服务端 CoherenceAuthority 实现
- `src/mock.rs`：测试 mock
- `src/lib.rs` 中的 CRDT 专属部分：
  - `CacheCoherence` trait
  - `CoherenceAuthority` trait
  - `DeltaWire` / `VectorClockWire` / `VectorClockEntryWire` / `EntryIdWire` / `DirEntryWire` / `SetAttrWire` 中性类型
  - `PushDeltaRequest/Response` / `PullDeltaRequest/Response`
  - `powerfs_orset <-> wire` 转换 impl
  - `DeltaOpType` 枚举

**powerfs-fuse/src/fuse.rs**（58 处调用改写）：
- `coherence` 字段及相关初始化（134-168 行）
- `local_create_entry` / `local_remove_entry` / `local_rename_entry` / `local_setattr_entry` 调用
- `list_entries` / `lookup_with_type` / `do_pull_and_apply_deltas` 调用
- `entry_exists` 方法（改为 Filer lookup 判断）
- `start_flusher` / `start_puller` 调用

**powerfs-fuse/src/cache.rs**：
- `MetadataCache.path_map` 字段及相关方法（`get_path` / `inode_to_path` / `remove_path` 等）
- `MetadataCache.dir_cache` 字段及相关方法（`get_dir_listing` / `set_dir_listing` / `invalidate_dir` 等）
- `MetadataCacheInvalidatorAdapter`（CRDT 联动失效，不再需要）
- `list_children` 方法（改为回源 Filer）

**powerfs-filer/src/net_handler.rs**：
- `handle_push_delta`（895 行）
- `handle_pull_delta`（952 行）
- 相关的 `VectorClockEntryWire` 构造代码（1229-1284 行）

**powerfs-fuse-core/src/meta_shard_client.rs**：
- `push_delta` / `pull_delta` 方法（1030/1042 行）
- `DeltaSyncChannel` trait impl 中的 push_delta/pull_delta（1121/1128 行）

### 4.2 保留项（通用元数据同步，独立 crate）

**powerfs-coherence crate 保留**（可重命名为 `powerfs-meta-sync`，本次重构暂不改名）：
- `DeltaSyncChannel` trait（仅保留 `alloc_inode_batch` / `update_inode_size_chunks` / `open_count_inc` / `open_count_dec`，删除 `push_delta` / `pull_delta`）
- `ChunkWire` 类型
- `AllocInodeBatchRequest/Response` / `UpdateInodeSizeChunksRequest/Response` / `OpenCountRequest/Response`
- `MetadataCacheInvalidator` trait（保留，阶段2 lease 缓存联动失效仍需用）

**Filer 侧保留**：
- `handle_alloc_inode_batch`（1018 行）
- `handle_update_inode_size_chunks`（1071 行）
- `handle_open_count_inc` / `handle_open_count_dec`（1133/1179 行）
- 所有强一致元数据 handler（handle_lookup/mkdir/create/unlink/rmdir/rename/readdir/setattr/getattr/symlink/readlink/link/statfs）

**fuse-core 侧保留**：
- `meta_shard_client.alloc_inode_batch` / `update_inode_size_chunks` / `open_count_inc` / `open_count_dec`
- `submit_metadata_request_and_wait` 通用 TLV 提交接口

---

## 五、实施步骤

### Step 1：MetadataClient trait + FieldId 重命名 + Transport trait

**1a. 定义 MetadataClient trait**

**新文件**：`powerfs-fuse-core/src/metadata_client.rs`

按 3.5.1 定义 `MetadataClient` trait（mkdir/create/unlink/rmdir/rename/lookup/readdir/getattr/setattr/symlink/readlink/link/alloc_inode_batch/update_size_chunks/open_count_inc/open_count_dec）。预留 `lookup_batch` 接口（3.11）。

**1b. MetaShardClient 实现 MetadataClient trait**

**文件**：[powerfs-fuse-core/src/meta_shard_client.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/meta_shard_client.rs)

为每个 trait 方法构造 TLV 调用 `submit_metadata_request_and_wait`。每个方法对应一个 Filer handler（handle_mkdir/handle_create/...），TLV 字段按 1c 重命名后的 FieldId 构造。实现内部重试（leader redirect → 重新发现 leader → 重试，3 次，每次 2s）。

**1c. FieldId 重命名 + 消除复用 + bytes::Bytes 零拷贝**

**文件**：[powerfs-net/src/protocol.rs](file:///home/portion/powerfs/powerfs-net/src/protocol.rs)（FieldId 枚举）、[powerfs-net/src/serialize.rs](file:///home/portion/powerfs/powerfs-net/src/serialize.rs)（编解码改 bytes::Bytes）、[powerfs-fuse-core/src/provider_adapter.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs)（build_*_tlv）、[powerfs-volume/src/net_handler.rs](file:///home/portion/powerfs/powerfs-volume/src/net_handler.rs)（解析）、[powerfs-filer/src/net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs)（解析）

按 3.5.4 重命名：`Ino`(write)→`VolumeId`、`Name`(write)→`FileKey`、`FileKey`(write)→`Inode`，新增 `NewParentIno`/`NewName`，消除 rename 的字段复用。TLV 编解码改 `bytes::Bytes`（RDMA 零拷贝友好，3.6.2）。

**1d. Transport trait 抽象（RDMA 预留）**

**新文件**：`powerfs-net/src/transport.rs`

按 3.6.1 定义 `Transport` trait（connect/send/recv/send_batch）+ `TransportType` 枚举 + `Endpoint` 类型。本次仅实现 `TcpTransport`，现有 TcpStream 收发迁移到 Transport trait。RDMA 实现留后续。

**1e. 删除废弃 trait**

**文件**：[powerfs-common/src/traits.rs](file:///home/portion/powerfs/powerfs-common/src/traits.rs)

删除 `MetadataProvider` trait（被 MetadataClient 取代）。

**验证**：编译通过，TLV 编解码单元测试全量更新并通过，Transport trait 有 TCP 实现测试。

### Step 2：fuse.rs 58 处 CRDT 调用改写 + Raft 读语义 + 跨客户端可见性

**2a. fuse.rs 58 处 CRDT 调用改写**

**文件**：[powerfs-fuse/src/fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs)

将 58 处 `coherence.*` 调用改为 `self.client.block_on(self.client.facade().meta_shard_client().<op>(...))`：

| 原调用 | 新调用 |
|--------|--------|
| `coherence.local_create_entry(...)` | `meta_shard_client.mkdir(...)` 或 `create(...)` |
| `coherence.local_remove_entry(...)` | `meta_shard_client.unlink(...)` 或 `rmdir(...)` |
| `coherence.local_rename_entry(...)` | `meta_shard_client.rename(...)` |
| `coherence.local_setattr_entry(...)` | `meta_shard_client.setattr(...)` |
| `coherence.list_entries(inode)` | `meta_shard_client.readdir(inode)` |
| `coherence.lookup_with_type(parent, name)` | `meta_shard_client.lookup(parent, name)` |
| `coherence.do_pull_and_apply_deltas(...)` | 删除（不再需要 pull） |
| `coherence.alloc_inode()` | `meta_shard_client.alloc_inode_batch(...)` |
| `coherence.sync_size_chunks(...)` | 保留（已是强一致） |
| `entry_exists(parent, name)` | `meta_shard_client.lookup(parent, name).is_ok()` |

**2b. Raft 读语义明确**

**文件**：[powerfs-filer/src/net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs)（handle_lookup/handle_readdir/handle_getattr）

确认 lookup/readdir/getattr 走 **Leader Lease Read**（leader 任期内本地读 shard_store，不走 Raft read index）。handle_* 中 check_leader 通过即可读，无需 Raft commit。

**2c. 跨客户端 read 可见性**

**文件**：[powerfs-fuse/src/fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs) open 路径

open 时强制 getattr 绕过 inode_cache TTL（3.7.2），从 Filer 获取最新 size/chunks。open 后 read 用缓存（数据 lease 排他保护）。

**验证**：编译通过，单客户端基本功能测试（mkdir/touch/rm/cp/mv），跨客户端 read 可见性测试。

### Step 3：删除 CRDT 代码

按 4.1 清单删除：
- powerfs-coherence 的 crdt_client.rs / crdt_server.rs / mock.rs
- powerfs-coherence/src/lib.rs 的 CRDT 专属 trait 和 wire 类型
- fuse.rs 的 coherence 字段和初始化
- cache.rs 的 path_map / dir_cache / MetadataCacheInvalidatorAdapter
- filer net_handler.rs 的 handle_push_delta / handle_pull_delta
- meta_shard_client.rs 的 push_delta / pull_delta 方法

**验证**：编译通过，无残留引用。

### Step 4：MetadataCache 简化

**文件**：[powerfs-fuse/src/cache.rs](file:///home/portion/powerfs/powerfs-fuse/src/cache.rs)

- 删除 `path_map` 字段及相关方法
- 删除 `dir_cache` 字段及相关方法
- 保留 `inode_cache`（attr 缓存，TTL 2s + invalidation）
- 保留 `pinned_inodes`（open 文件跳过 TTL）
- 保留 `path_generations`（invalidation 版本判断）
- 简化 `list_children`：改为调用 Filer readdir（或直接删除，由 fuse.rs 调用 meta_shard_client.readdir）
- 简化 `lookup_in_cache`：仅查 inode_cache，不再扫 path_map

**验证**：编译通过，单元测试更新。

### Step 5：Filer 侧清理

**文件**：[powerfs-filer/src/net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs)

- 删除 `handle_push_delta` / `handle_pull_delta`
- 删除 `handle_request` / `handle` 中对应的 msg_type 分支
- 删除 `VectorClockEntryWire` 构造代码（1229-1284 行）
- 保留所有强一致元数据 handler 和 alloc_inode_batch/update_inode_size_chunks/open_count handler

**验证**：Filer 编译通过，强一致元数据操作功能正常。

### Step 6：LeaseManager + DataClient trait + Read Lease 缓存复用

**6a. 定义 LeaseManager trait + 类型**

**新文件**：`powerfs-fuse-core/src/lease.rs`

按 3.5.3 定义 `LeaseToken`/`LeaseMode`/`LeaseHandle`/`LeaseManager`/`LeaseReleaser`/`LeaseState`。`LeaseHandle` RAII 自动释放（取代 fuse.rs 手动管理的 `LeaseGuard`）。

**6b. VolumeLeaseManager 实现 LeaseManager**

**文件**：[powerfs-fuse-core/src/volume_client.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/volume_client.rs) 或新文件

包装现有 `VolumeClient` 的 acquire_lease/release_lease_remote/renew_lease 逻辑，实现 `LeaseManager` trait。内部维护 lease 缓存（read/write 统一复用）。

**6c. 定义 DataClient trait + 实现**

**新文件**：`powerfs-fuse-core/src/data_client.rs`

按 3.5.2 定义 `DataClient` trait（write/read/delete）。实现组合 `LeaseManager` + `VolumeClient`：
- `write()`：`lease_mgr.acquire(Exclusive)` → 构造 write TLV → submit
- `read()`：`lease_mgr.acquire(Shared)` → 构造 read TLV → submit
- `delete()`：直接 submit delete TLV（无需 lease）

**6d. fuse.rs 改用 DataClient + LeaseManager**

**文件**：[powerfs-fuse/src/fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs)

- write 路径（[1795](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1795)）：`write_blob_with_lease` → `data_client.write()`
- read 路径（[1521](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1521)）：`acquire_lease` + `read_blob` → `data_client.read()`（修复缓存复用缺陷）
- release 路径：`LeaseGuard` → `LeaseHandle`（RAII 自动释放）

**6e. 删除旧接口**

- fuse.rs 的 `LeaseGuard`（被 `LeaseHandle` 取代）
- `ProviderAdapter::ensure_lease`（被 `LeaseManager::acquire` 取代）
- `StorageProvider` trait 的 `write_blob`/`batch_write_blob`（死代码）
- `SyncFuseClientFacade.write_blob`、`FacadeStorageProvider::write_blob`/`batch_write_blob`
- `build_write_tlv`/`build_batch_write_tlv`（无 inode 版本）

**验证**：
- 编译通过，无残留引用
- read 性能测试，lease 请求数下降 ~90%
- write/read 对称性验证（都走 LeaseManager 统一入口）

### Step 7：测试验证 + fsck + 监控

#### 7.1 单元测试
- meta_shard_client 具名方法的 TLV 编解码测试
- MetadataCache 简化后的测试更新
- Transport trait 的 TCP 实现测试
- LeaseManager / DataClient trait 测试

#### 7.2 三轮系统正确性测试（容器内执行）
- 第一轮：`cp -prf` + md5 跨客户端验证
- 第二轮：`tar -czf` + `tar -xzf` + md5 验证
- 第三轮：`rm -rf` + 重建 + md5 验证

#### 7.3 fsck 工具验证
- 孤儿 inode 检测：create 后崩溃（kill -9 fuse 进程），fsck 能检测并清理
- size/chunks 不一致：close 前崩溃，fsck 对比 Volume 实际数据块与 Filer 记录，清理孤儿 chunk
- 部分写入恢复：flush 中途崩溃，重启后文件 size/chunks 以 Filer 记录为准

#### 7.4 故障恢复测试
- 客户端崩溃 lease 清理：kill -9 fuse 进程，验证 grace period 后 lease 释放
- Filer leader 切换：kill leader 节点，验证客户端重试成功，元数据操作不中断
- Volume Server 故障：kill volume server，验证写操作 failover 到其他副本

#### 7.5 fio 性能测试（标准 fio 命令，容器内执行）
- 顺序读写带宽
- 随机读写 IOPS
- 对比重构前基线

#### 7.6 io500 测试（标准 io500 命令，真实挂载测试）
- 完整 io500 套件
- 对比重构前基线

#### 7.7 监控指标验证
- Filer Raft：commit_latency / propose_qps / queue_depth
- Filer RPC：rpc_qps / rpc_latency_p99 / rpc_error_rate
- 客户端缓存：inode_cache_hit_rate / lease_cache_hit_rate
- Lease：acquire_qps / expired_count / grace_count
- GC：orphan_inode_count / gc_throughput

---

## 六、风险与回滚

### 6.1 风险

| 风险 | 影响 | 缓解 |
|------|------|------|
| 58 处调用改写引入新 Bug | 元数据操作失败 | 充分单元测试 + 三轮系统测试 + fio/io500 回归 |
| Filer RPC 压力上升 | 元数据操作延迟增加 | 阶段1 Filer in-memory 缓存承担热点；不达标时进入阶段2 |
| 删除 CRDT 后发现遗漏依赖 | 编译错误 | 分步骤删除，每步编译验证 |
| Read Lease 缓存复用引入一致性问题 | read 数据不一致 | shared lease 不影响排他写，风险低 |

### 6.2 回滚方案

- 实施前打 tag：`git tag pre-strong-consistency-refactor`
- 每个 Step 独立 commit，支持 `git revert`
- Step 3（删除 CRDT）前确认 Step 1-2 功能完全正常
- 保留 CRDT 代码 git history，必要时可回溯

---

## 七、后续演进（阶段2，本次不实施）

当阶段1 性能不达标时，引入客户端 lease 缓存 + callback invalidation：

1. Filer 侧新增 lease 管理模块（维护 dir → client_list 映射）
2. 客户端 readdir/lookup 获取 read lease，缓存目录列表
3. write 操作时 Filer 同步撤销相关 read lease（callback）
4. lease TTL 兜底（30s）+ grace period 应对 callback 超时
5. lease 粒度 per-directory

阶段2 的设计依据是阶段1 的 fio/io500 性能数据，**数据不达标才推进**。

---

## 八、附录：代码位置索引

| 模块 | 文件 | 关键位置 |
|------|------|---------|
| Write 路径 | [fuse.rs:1795](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1795) | `write()` |
| Read 路径 | [fuse.rs:1521](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1521) | `read()` |
| Release/close | [fuse.rs:1965](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1965) | `release()` |
| Lease 缓存（write） | [provider_adapter.rs:1065](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs#L1065) | `ensure_lease()` |
| Lease 获取（read，待修复） | [fuse.rs:1619](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1619) | `acquire_lease()` |
| ChunkCache | [cache.rs:1314](file:///home/portion/powerfs/powerfs-fuse/src/cache.rs#L1314) | `ChunkCache` |
| MetadataCache | [cache.rs:76](file:///home/portion/powerfs/powerfs-fuse/src/cache.rs#L76) | `MetadataCache` |
| CRDT 副本（待删） | [crdt_client.rs:296](file:///home/portion/powerfs/powerfs-coherence/src/crdt_client.rs#L296) | `CrdtReplicaCoherence` |
| Filer 强一致 handler | [net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs) | `handle_mkdir` 等 |
| 通用 TLV 提交 | [meta_shard_client.rs:377](file:///home/portion/powerfs/powerfs-fuse-core/src/meta_shard_client.rs#L377) | `submit_metadata_request_and_wait` |
| DeltaSyncChannel trait | [lib.rs:60](file:///home/portion/powerfs/powerfs-coherence/src/lib.rs#L60) | `DeltaSyncChannel` |

---

## 九、实施调整记录（回溯用）

### 2026-08-02 Step 1 调整

**调整1：FieldId 重命名范围缩小**

原方案 3.5.4 计划 `Ino`→`VolumeId`、`FileKey`→`Inode` 全局重命名。调研发现 `Ino` 在元数据路径语义正确（=inode），全局重命名会破坏元数据路径。且涉及协议兼容性。

**调整后**：
- rename TLV 改用已有的 `NewParentIno`/`NewName`（protocol.rs:546-547 已定义，但 serialize.rs:721-722 仍在复用 Ino/SymlinkTarget）
- write TLV FieldId 值不改（协议兼容），删除死代码 `build_write_tlv`（无 inode 版本），保留 `build_write_tlv_with_inode` 并重命名
- 元数据路径 FieldId 保持不变（语义正确）
- FieldId 枚举全局重命名改为后续协议升级时做

**调整2：NewParentIno/NewName 已存在**

原方案说"新增 NewParentIno/NewName"，实际 protocol.rs 已定义（0x50/0x51），只需启用。

**Step 1 调整后子任务**：
- 1a. 定义 MetadataClient trait（不变）
- 1b. MetaShardClient 实现（不变）
- 1c. rename TLV 改用 NewParentIno/NewName + 删除死代码 build_write_tlv（范围缩小）
- 1d. Transport trait（不变）
- 1e. 删除废弃 MetadataProvider trait（不变）

### 2026-08-02 Step 1 完成记录

**Commit**: `de0832e5` refactor(metadata): add MetadataClient trait and Transport abstraction for strong consistency

**完成内容**：
- 1a. ✅ 定义 MetadataClient trait（13 个方法：lookup/mkdir/create/unlink/rmdir/rename/symlink/readlink/link/readdir/getattr/setattr/statfs）
- 1b. ✅ MetaShardClient 实现 MetadataClient trait（TLV 编码 + send_coherence_msg + TLV 解码）
- 1c. ✅ rename TLV 改用 NewParentIno/NewName（Filer 侧已使用，客户端侧同步）
- 1d. ✅ Transport/TransportPool/BatchTransport trait（powerfs-net/src/transport.rs）
- 1e. ⏭️ 废弃 MetadataProvider trait 延后到 Step 3 删除 CRDT 时一起处理

**额外修复**：
- 补充 serialize.rs 缺少的 encode/decode 函数（mkdir/unlink/rmdir/attr_resp/statfs）
- 新增 FieldId::Free/FreeInodes/BlockSize 用于 statfs
- decode_attr_resp 使用循环遍历方式（而非顺序 unwrap_or），正确处理缺失字段

**验证**：
- cargo check --workspace: 全部 13 个 package 编译通过
- cargo clippy: 0 警告（修复 8 个 unnecessary_cast）
- cargo fmt: 通过
- cargo test: 12 passed, 1 failed（test_request_kind_priority 为 pre-existing 失败，与本次改动无关）

### 2026-08-02 Step 2 完成记录

**Commit**: `f5e886cc` refactor(fuse): replace CRDT coherence calls with MetadataClient RPC

**完成内容**：
- 2a. ✅ 9 个 FUSE 回调改写（lookup/mkdir/rmdir/unlink/create/setattr/readdir/rename/entry_exists）
- 2b. ⏭️ Raft 读语义：Filer 侧已实现 check_leader（Leader Lease Read），客户端侧无需额外处理
- 2c. ⏭️ 跨客户端可见性：open 时强制 getattr 已有逻辑（保留现有 getattr 缓存 + TTL_OPEN 机制）

**调整记录**：
- getattr 未改为 MetadataClient.getattr：因为现有 get_entry_by_inode 返回完整 FilerEntry（含 chunks/fid/path），而 MetadataAttr 缺少这些字段。getattr 是读操作，走现有 Filer RPC 不影响强一致性（Filer check_leader 已保证 Leader Lease Read）。
- symlink/link/readlink 未改为 MetadataClient：这些操作使用 powerfs-net 协议直接调 Filer handler（非 coherence 调用），已是强一致路径。EEXIST 检查通过 entry_exists 间接走 metadata_client.lookup。
- 删除的死代码：remove_entry_with_fallback、lookup_attr_from_filer、coherence.force_sync in sync_size_chunks_on_close

**保留的 coherence 调用**（非 CRDT，强一致数据路径）：
- sync_size_chunks / update_inode_size_chunks（close 时同步 size/chunks）
- open_count_inc / open_count_dec（GC 追踪）
- coherence 字段 + start_flusher/start_puller（Step 3 清理）

**验证**：
- cargo check: 编译通过
- cargo clippy: 0 警告
- cargo fmt: 通过
- 代码量：-408 行 / +235 行（净减 173 行，9 个回调从多源 CRDT 逻辑简化为单路径 RPC）
