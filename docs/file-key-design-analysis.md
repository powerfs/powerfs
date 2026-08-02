# file_key 设计缺陷分析与修复方案

> 文档创建：2026-08-02
> 最后更新：2026-08-02
> 状态：**已实施** — 修复方案 A/B/C 已全部完成，D（fsck）作为后续任务

## 1. 背景与问题概述

file_key 在系统中反复出现 4 次问题（2 次已修复、1 次新发现未修复、1 次潜在隐患），每次修复都只针对单一路径，未触及根本设计缺陷。本文档系统梳理 file_key 的设计初衷、数据表示、问题历史与根本原因，并规划彻底的修复方案。

---

## 2. file_key 设计初衷与数据表示

### 2.1 Fid 三元组结构

借鉴 SeaweedFS 设计，[powerfs-common/src/types.rs:261-265](file:///home/portion/powerfs/powerfs-common/src/types.rs#L261-L265)：

```rust
pub struct Fid {
    pub volume_id: VolumeId,  // 卷 ID（数据所在 volume）
    pub cookie: u64,          // 文件级随机 cookie（防碰撞、防伪造）
    pub file_key: u64,        // 文件级唯一标识（master 分配）
}
```

序列化格式为 `"volume_id,cookie,file_key"`。

### 2.2 file_key 的分配

由 Master 端 per-volume 自增计数器 `VolumeInfo.next_file_key` 分配，[powerfs-master/src/master.rs:1581-1586](file:///home/portion/powerfs/powerfs-master/src/master.rs#L1581-L1586)：

```rust
let file_key = {
    let mut volumes = self.volumes.write().unwrap();
    if let Some(vol_info) = volumes.get_mut(&existing_vid) {
        let key = vol_info.next_file_key;
        vol_info.next_file_key += 1;
        key
    } else {
        1
    }
};
```

设计语义：**file_key 是文件级标识**，一个文件对应一个 file_key，per-volume 内自增唯一。

### 2.3 file_key 到存储键的映射

Volume Server 端直接将 file_key 映射为 NeedleId（chunk 级物理存储键），[powerfs-core/src/volume.rs:298-306](file:///home/portion/powerfs/powerfs-core/src/volume.rs#L298-L306)：

```rust
pub fn write_needle(&self, file_key: u64, data: Bytes) -> Result<NeedleInfo> {
    ...
    let needle_id = NeedleId(file_key);
    ...
}
```

### 2.4 文件空间布局的存储位置

文件的空间布局（chunk 列表、stripe 信息）存储在 inode 的 `Entry` 结构中，[powerfs-common/src/traits.rs:96-110](file:///home/portion/powerfs/powerfs-common/src/traits.rs#L96-L110)：

```rust
pub struct Entry {
    pub name: String,
    pub directory: String,
    pub attributes: Option<EntryAttributes>,
    pub chunks: Vec<FileChunk>,                        // chunk 空间布局
    pub hard_link_id: String,
    pub hard_link_counter: u32,
    pub extended: HashMap<String, Vec<u8>>,            // 扩展信息（FileLayout 等）
    pub content_size: u64,
    pub disk_size: u64,
    ...
}
```

其中：
- `chunks: Vec<FileChunk>`：存储每个 chunk 的定位信息（offset、size、fid、cookie、crc32）
- `extended: HashMap<String, Vec<u8>>`：存储 `FileLayout`（layout_type、stripe_size、stripe_count、volume_ids 等，参见 [file_layout_stripe_design.md](file:///home/portion/powerfs/docs/file_layout_stripe_design.md)）

`FileChunk` 结构定义在 [powerfs-common/src/traits.rs:85-93](file:///home/portion/powerfs/powerfs-common/src/traits.rs#L85-L93)：

```rust
pub struct FileChunk {
    pub offset: u64,       // chunk 在文件内的偏移
    pub size: u64,         // chunk 数据大小
    pub mtime: u64,
    pub fid: String,       // chunk 的 fid 标识
    pub cookie: u32,
    pub crc32: u32,
}
```

### 2.5 设计意图总结

| 概念 | 语义 | 存储位置 |
|---|---|---|
| `Fid.file_key` | 文件级唯一标识 | master 分配，存入 Entry.fid |
| `NeedleId` | chunk 级物理存储键 | volume server 物理文件 |
| `Entry.chunks` | chunk 空间布局 | filer 元数据（Raft 强一致） |
| `Entry.extended` | stripe/layout 等扩展信息 | filer 元数据 |

**理想设计**：file_key 保持文件级语义，每个 chunk 拥有独立的 needle_id，chunks 列表完整记录每个 chunk 的 needle_id 与 offset/size 的映射。

### 2.6 Volume 的 needle 管理设计

file_key 最终落地为 NeedleId 存储。Volume Server 每个 Volume 独立管理自己的 needle 索引、物理布局和空间统计。这块设计是 file_key 问题的重要背景，因为 **Volume 完全不感知 file_key 的"文件级"语义，只把 NeedleId 当作 opaque 存储键**——任何唯一的 u64 都能作为 NeedleId 写入。

#### 2.6.1 Volume 数据结构

[powerfs-core/src/volume.rs:22-38](file:///home/portion/powerfs/powerfs-core/src/volume.rs#L22-L38)：

```rust
pub struct Volume {
    info: RwLock<VolumeInfo>,          // 卷元信息（含 next_file_key）
    index: VolumeMetadata,             // RocksDB 索引（4 个 CF）
    checksum_algorithm: ChecksumAlgorithm,
    backend: Arc<dyn StorageBackend>,  // 物理存储后端（文件/块设备）
    backend_volume_id: u64,
    coalescer: Arc<WriteCoalescer>,    // 写合并缓冲（部分覆盖写优化）
    op_counter: AtomicU32,             // 触发过期脏数据 flush 的计数器
}
```

#### 2.6.2 RocksDB 列族设计

[powerfs-core/src/volume_metadata.rs:15-18](file:///home/portion/powerfs/powerfs-core/src/volume_metadata.rs#L15-L18) 定义 4 个列族：

| 列族 | 用途 | Key → Value |
|---|---|---|
| `CF_CONFIG` | 配置单例 | 固定 key → AllocationStats |
| `CF_NEEDLES` | 活跃 needle 索引 | NeedleId(u64) → NeedleInfo |
| `CF_ALLOCATION` | 分配统计 | 固定 key → AllocationStats |
| `CF_DELETED` | 已删除 needle（compact 用） | NeedleId → NeedleInfo |

`NeedleInfo` 结构（[powerfs-common/src/types.rs:383-401](file:///home/portion/powerfs/powerfs-common/src/types.rs#L383-L401)）：

```rust
pub struct NeedleInfo {
    pub id: NeedleId,
    pub volume_id: VolumeId,
    pub data_size: u32,           // 数据大小（不含 header/footer）
    pub offset: u64,              // 物理文件偏移
    pub checksum: u64,
    pub checksum_algorithm: ChecksumAlgorithm,
    pub deleted_at: Option<DateTime<Utc>>,
    pub delete_retention_until: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub ec_enabled: bool,
    // ...
}
```

#### 2.6.3 AllocationStats 空间统计

`AllocationStats` 存储在 `CF_ALLOCATION`，记录卷的空间使用情况：

| 字段 | 含义 | 更新时机 |
|---|---|---|
| `used_bytes` | 活跃 needle 总大小（**逻辑空间**使用，删除后已回收） | write += size, delete -= size |
| `free_bytes` | 逻辑可用空间 = `volume_size - used_bytes` | 随 used_bytes 联动 |
| `append_offset` | 物理文件末尾（**包括 deleted needle 的 hole**，append-only） | write += size，delete 不变 |
| `active_count` | 活跃 needle 数量 | write += 1, delete -= 1 |
| `deleted_count` | 已删除 needle 数量（等待 compact） | delete += 1, compact = 0 |

**关键区分**：
- `used_bytes`（逻辑空间）：删除后立即回收，决定 volume 是否 Full
- `append_offset`（物理末尾）：append-only，删除不缩小，**只有 compact 才能回收物理 hole**
- `free_bytes` 基于 `used_bytes` 计算，不是基于 `append_offset`

#### 2.6.4 Needle 生命周期

**写入**（`write_needle_atomic`，[volume_metadata.rs:336-385](file:///home/portion/powerfs/powerfs-core/src/volume_metadata.rs#L336-L385)）：
1. append-only 写入物理文件末尾（offset = append_offset）
2. CF_NEEDLES 插入 NeedleId → NeedleInfo
3. CF_ALLOCATION 原子更新：used_bytes += size, free_bytes 联动, append_offset += size, active_count += 1（仅新增，覆盖写不变）
4. 覆盖写（同 NeedleId 已存在）走 `append_needle_version`，旧版本变 hole

**读取**（`read_needle`，[volume.rs:359-385](file:///home/portion/powerfs/powerfs-core/src/volume.rs#L359-L385)）：
1. 先查 `coalescer` 脏缓冲（read-your-own-writes，未落盘的数据）
2. 查 CF_NEEDLES 获取 offset + data_size
3. 从 backend 读取物理数据（header + data + footer）
4. 校验 checksum

**删除**（`delete_needle_atomic`，[volume_metadata.rs:387-446](file:///home/portion/powerfs/powerfs-core/src/volume_metadata.rs#L387-L446)）：
1. 从 CF_NEEDLES 移到 CF_DELETED（保留 NeedleInfo，供 compact/restore 用）
2. CF_ALLOCATION 原子更新：used_bytes -= size（**立即回收逻辑空间**）, active_count -= 1, deleted_count += 1
3. 物理空间不回收（hole 留在 append_offset 之前，等 compact）

**恢复**（`restore_needle_atomic`，[volume_metadata.rs:448-510](file:///home/portion/powerfs/powerfs-core/src/volume_metadata.rs#L448-L510)）：
1. 从 CF_DELETED 恢复到 CF_NEEDLES
2. used_bytes += size, active_count += 1, deleted_count -= 1
3. 与 delete 对称

**Compact**（`compact`，[volume.rs:523-587](file:///home/portion/powerfs/powerfs-core/src/volume.rs#L523-L587)）：
1. 扫描所有活跃 needle（CF_NEEDLES 中 deleted_at 为空的）
2. 按 offset 排序
3. 重写连续布局：读取每个活跃 needle 的物理数据，写到新 offset，更新索引
4. `compact_cleanup`：清空 CF_DELETED，重置 deleted_count=0，更新 used_bytes
5. `backend.truncate_volume` 截断物理文件到新末尾
6. 返回 (reclaimed_bytes, updated_count)

#### 2.6.5 启动时重建统计

[powerfs_metadata.rs:258-297](file:///home/portion/powerfs/powerfs-core/src/volume_metadata.rs#L258-L297) `rebuild_allocation_stats`：

1. 扫描 CF_NEEDLES：累加 used_bytes、active_count，记录 max_end（append_offset）
2. 扫描 CF_DELETED：统计 deleted_count（不算入 used_bytes，但影响 append_offset）
3. `sync_allocation_from_index`：用扫描结果覆盖 CF_ALLOCATION，确保启动后统计一致

这解决了之前 `rebuild_metadata_from_index` 只扫 CF_NEEDLES 导致 deleted_count 丢失的 bug。

#### 2.6.6 与 file_key 的关系（关键）

| 层次 | 概念 | 职责 |
|---|---|---|
| Master | `next_file_key` | per-volume 自增计数器，分配文件级 file_key |
| 协议层 | `FieldId::Name`（存 file_key） | TLV 传输 chunk 级 needle_id（当前 hack：file_key+chunk_idx） |
| Volume Server | `NeedleId(file_key)` | 直接映射，不校验语义 |
| Volume | `CF_NEEDLES` | NeedleId → NeedleInfo，opaque 存储键 |

**Volume 的盲点**：
1. **不感知 file_key 的文件级语义**：Volume 只看到 NeedleId，不知道哪些 needle 属于同一文件
2. **不校验 NeedleId 合理性**：任何唯一 u64 都能写入，无法发现 file_key 重复或碰撞
3. **不维护 file → chunk 映射**：这个映射在 Filer 的 `Entry.chunks` 里，Volume 无感知
4. **GC 依赖 Filer 通知**：Volume 不会主动删除 needle，必须由 Filer GC 调用 `delete_needle`

**这意味着**：
- file_key 重复（问题3）在 Volume 层完全无法发现，只是静默覆盖 NeedleInfo
- GC 漏删 chunk（问题4）在 Volume 层完全无法发现，needle 永远留在 CF_NEEDLES
- `next_file_key` 未持久化（隐患6）重启后，Volume 会接受"旧" NeedleId 的覆盖写

#### 2.6.7 现有 compact 机制的问题

1. **compact 未被自动触发**：`compact()` 方法存在但没有自动触发机制（无定时器、无阈值），只能手动调用。deleted needle 堆积导致 `append_offset` 持续增长，物理文件膨胀。
2. **compact 期间数据一致性**：重写 needle 期间崩溃，可能 NeedleInfo.offset 已更新但物理数据未写入，导致 read 失败。需要先写新位置、再更新索引、最后删旧位置的顺序保证。
3. **compact 与并发写冲突**：compact 期间若有并发 write，新写入的 needle 可能被 compact 重写或 truncate 截断。需要 compact 期间锁 volume 或暂停写入。
4. **compact 后 NeedleId 不变**：compact 只改变物理 offset，NeedleId 保持不变。file_key → NeedleId 的映射不受影响。

#### 2.6.8 与修复方案的关联

修复项 C（`next_file_key` 持久化）需要考虑：
- `next_file_key` 存在 Master 的 `VolumeInfo` 里，不在 Volume 的 RocksDB 中
- 但 Volume 的 `CF_NEEDLES` 可以反推 `max(NeedleId) + 1` 作为兜底恢复值
- Master 重启时可以从 Volume Server 拉取每个 volume 的 `max_needle_id` 来恢复 `next_file_key`

修复项 A（FileChunk.fid 存独立 needle_id）需要考虑：
- Volume 的 `delete_needle` 是 per-NeedleId 的，不支持批量删除
- GC 遍历 chunks 列表时，每个 chunk 独立调用 `delete_needle`，失败不影响其他 chunk（best-effort）

---

## 3. 反复出现的问题

### 3.1 问题1（2026-07-29，已修复）：filer 端伪造 file_key

**现象**：filer 端 `handle_assign_volume` 用 `rand::random()` 生成 file_key，违反"master 统一分配"原则，导致 file_key 可能碰撞。

**根因**：filer 不应承担 volume/file_key 分配职责。

**修复**：删除 filer 端 assign_volume 路径，所有分配走 master（[powerfs-filer/src/net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs) 已移除 `handle_assign_volume`）。

### 3.2 问题2（2026-07-30，已修复但引入新坑）：lease inode/file_key 不匹配

**现象**：lease 按 inode 注册，但 write 请求只传 file_key，volume server 无法用 file_key 找到对应 lease，导致 lease 校验失败。

**根因**：lease 是 per-inode 的（文件级），write 请求只携带 per-chunk 的 file_key，缺少 inode 关联。

**修复**：在 TLV 中额外添加 inode 字段用于 lease 校验。

**引入的新坑（命名混乱）**：修复时偷用了 `FieldId::FileKey` 字段来传 inode，[powerfs-fuse-core/src/provider_adapter.rs:392-393](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs#L392-L393)：

```rust
enc.add_u64(FieldId::Name, file_key);    // Name 字段存 file_key
enc.add_u64(FieldId::FileKey, inode);    // FileKey 字段存 inode !!!
```

`FieldId::Name (0x02)` 实际存 file_key，`FieldId::FileKey (0x94)` 实际存 inode，命名完全反直觉，是后续诊断困难的温床。

### 3.3 问题3（2026-08-02，已修复）：file_key 重复导致静默数据丢失

**现象**：FUSE 客户端所有 chunk 使用同一 `fid.file_key` 作为 NeedleId，导致后写 chunk 覆盖先写 chunk，造成**静默数据丢失**（无错误日志，md5 不一致）。

**根因**：file_key 是文件级标识（一个文件一个），但被直接用作 chunk 级 NeedleId（每个 chunk 需要独立的）。多 chunk 文件必然冲突。

**修复**：每个 chunk 使用 `file_key = fid.file_key.saturating_add(chunk_idx)`，[powerfs-fuse/src/fuse.rs:553](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L553)、[powerfs-fuse/src/fuse.rs:1267](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1267)。

**验证**：volume 日志显示 64 个不同 file_key，md5 校验读写数据一致。

### 3.4 问题4（新发现，未修复）：GC 只删除首个 chunk，孤儿数据泄漏

**现象**：filer 端 GC 路径 `reclaim_data_chunks` 和 `retry_pending_reclaims` 解析 fid 后**只用 base file_key** 调用 `delete_needle`，[powerfs-filer/src/meta_shard_manager.rs:2393-2418](file:///home/portion/powerfs/powerfs-filer/src/meta_shard_manager.rs#L2393-L2418)：

```rust
let file_key: u64 = parts[2].parse()...;  // 只取 base file_key
volume_client_pool.delete_needle(&server_addr, volume_id, file_key).await
```

**影响**：修复后的多 chunk 文件，chunk[1..N] 的 needle 永远不会被回收 → **物理空间泄漏**，volume 会再次被孤儿数据填满导致 Full。

**根因**：GC 路径没有同步问题3的修复，仍然假设一个文件一个 needle。

### 3.5 隐患5（潜在）：S3 路径同样使用 base file_key

[powerfs-filer/src/s3_handler.rs:159-167](file:///home/portion/powerfs/powerfs-filer/src/s3_handler.rs#L159-L167) 直接用 fid 中的 file_key 作为 needle_id 写入：

```rust
let file_key: u64 = fid_str.split(',').nth(2).and_then(|s| s.parse().ok()).unwrap_or(0);
self.volume_client_pool.write_needle(&server_addr, volume_id, file_key, data).await
```

若 S3 对象走多 chunk 路径，同样会覆盖（需确认 S3 是否单 chunk 写入；当前看是单 chunk，但未来扩展大对象时会出问题）。

### 3.6 隐患6（潜在，最危险）：master `next_file_key` 未持久化

[master.rs:864](file:///home/portion/powerfs/powerfs-master/src/master.rs#L864)、[master.rs:1022](file:///home/portion/powerfs/powerfs-master/src/master.rs#L1022) 初始化 `next_file_key: 1`，且未通过 Raft 持久化。

**影响**：master 重启后 `next_file_key` 重置为 1，会分配重复 file_key，导致**数据覆盖**。这是最危险的隐患，因为 master 重启是常见运维操作。

---

## 4. 根本原因分析

| 根因 | 说明 | 后果 |
|---|---|---|
| **缺少 chunk_key 抽象** | FUSE/协议/volume 三层都直接操作 file_key，没有区分"文件级 file_key"与"chunk 级 needle_id" | 多 chunk 文件 NeedleId 冲突 |
| **FileChunk.fid 语义错位** | 当前所有 chunk 的 fid 都存文件级 base fid，chunk_idx 靠 `offset/chunk_size` 推断 | truncate/hole/配置变更时推断错误 |
| **FieldId 命名混乱** | `Name` 存 file_key、`FileKey` 存 inode，反直觉 | 诊断困难，新 bug 温床 |
| **修复不彻底** | 每次只修一个路径（FUSE write），GC/S3 路径漏改 | 孤儿数据泄漏 |
| **next_file_key 未持久化** | master 重启后重置 | 数据覆盖 |

### 4.1 当前 chunks 列表的脆弱性

[powerfs-fuse/src/fuse.rs:785-789](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L785-L789) 显示，系统从 `chunks[0].fid` 解析 base fid，再用 `base + chunk_idx` 推断每个 chunk 的 needle_id：

```rust
let fid = entry.chunks.first().and_then(|chunk| {
    let result = Fid::from_string(&chunk.fid);
    result.ok()
});
// 后续: needle_id = fid.file_key + (offset / chunk_size)
```

这意味着：
1. **所有 chunk 的 fid 字段都存同一个文件级 base fid**（或只有 chunks[0] 有值）
2. **chunk_idx 靠 offset/chunk_size 推断**
3. **truncate 变小、chunk 不连续（hole）、chunk_size 配置变更** 时推断全部错误
4. **GC/S3/delete 路径必须各自重新推断**，任何一处推断逻辑不一致就出 bug

---

## 5. 修复方案

### 5.1 设计原则

1. **file_key 回归文件级语义**：一个文件一个 file_key，由 master 分配，存入 `Entry.fid`
2. **chunk 级 needle_id 独立存储**：每个 `FileChunk.fid` 存该 chunk 自己的完整 fid（`file_key + chunk_idx` 已是独立值）
3. **空间布局以 chunks 列表为权威**：read/write/delete/GC/S3 全部从 `Entry.chunks` 取 needle_id，禁止推断
4. **stripe 信息存 extended**：FileLayout 通过 `Entry.extended` 存储，与 chunks 互补

### 5.2 修复项

#### 修复项 A：FileChunk.fid 存 chunk 级独立 needle_id（核心）

**目标**：每个 FileChunk.fid 存该 chunk 独立的 fid（即 `file_key + chunk_idx` 后的值），不再靠推断。

**改动点**：

A-1. **write 路径**（[powerfs-fuse/src/fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs) flush_dirty_chunks）：
- 写入 chunk 后，构建 `FileChunk` 时 fid 字段存 `Fid { file_key: base + chunk_idx, ... }.to_string()`
- close 同步 chunks 到 filer 时，每个 chunk 都带独立的 needle_id

A-2. **read 路径**（[powerfs-fuse/src/fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs) read）：
- 从 `entry.chunks[i].fid` 直接解析该 chunk 的 needle_id
- 不再用 `base + chunk_idx` 推断
- 按 offset 匹配 chunk，支持不连续/hole 场景

A-3. **delete 路径**（[powerfs-fuse/src/fuse.rs:1255-1272](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1255-L1272)）：
- 遍历 `entry.chunks`，对每个 chunk 的 fid 调用 `delete_needle`
- 移除 `chunk_count = content_size.div_ceil(chunk_size)` 推断逻辑

A-4. **GC 路径**（[powerfs-filer/src/meta_shard_manager.rs:2366-2435](file:///home/portion/powerfs/powerfs-filer/src/meta_shard_manager.rs#L2366-L2435) reclaim_data_chunks）：
- 遍历 `chunks` 列表，对每个 chunk 的 fid 调用 `delete_needle`
- 同步修改 `retry_pending_reclaims`

A-5. **S3 路径**（[powerfs-filer/src/s3_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/s3_handler.rs)）：
- 单 chunk 对象：保持现状（file_key 即 needle_id）
- 多 chunk 对象（未来支持）：按 chunks 列表写入/读取

**向后兼容**：旧元数据只有 chunks[0] 有 fid 且为 base file_key，读取时若发现 chunk.fid 解析出的 file_key 等于 base file_key 且 chunks.len()==1，按旧逻辑处理；多 chunk 但 fid 相同时，回退到 `base + chunk_idx` 推断。

#### 修复项 B：FieldId 命名修正

**目标**：消除 `FieldId::Name` 存 file_key、`FieldId::FileKey` 存 inode 的命名混乱。

**方案**：新增语义明确的 FieldId，逐步迁移：

```rust
pub enum FieldId {
    // ... 既有字段
    VolumeId = 0x92,
    Cookie = 0x93,
    FileKey = 0x94,        // 保留，语义回归为 file_key（chunk 级 needle_id）
    Fid = 0x95,
    Chunks = 0x96,
    // 新增
    NeedleId = 0x97,       // chunk 级存储键（明确语义）
    Inode = 0x98,          // lease 校验用 inode（替代 FileKey 存 inode 的 hack）
}
```

**迁移策略**：
- 写入端：同时写新旧字段（`FieldId::Name` + `FieldId::NeedleId`、`FieldId::FileKey` + `FieldId::Inode`）
- 读取端：优先读新字段，缺失时回退旧字段
- 待所有节点升级后，移除旧字段写入

#### 修复项 C：master `next_file_key` 持久化

**目标**：master 重启后 next_file_key 不重置，避免分配重复 file_key。

**方案**：通过 Raft 持久化 `next_file_key`：
- C-1. 新增 `RaftCommand::AdvanceFileKey { volume_id, new_next_key }`
- C-2. master 分配 file_key 后，通过 Raft propose 推进 next_file_key
- C-3. apply 时更新 `VolumeInfo.next_file_key` 并持久化到 RocksDB
- C-4. master 启动时从 RocksDB 恢复每个 volume 的 next_file_key

**简化方案**（如果 Raft 改动成本高）：master 启动时扫描所有 volume 的 needle index，取 `max(needle_id) + 1` 作为 next_file_key。但扫描开销大，仅作为兜底。

#### 修复项 D：chunks 列表完整性校验

**目标**：防止 chunks 列表与实际数据不一致。

**方案**：
- D-1. fsck 工具校验：每个 chunk.fid 对应的 needle 在 volume 中存在
- D-2. open 时校验：若 chunks 列表为空但 content_size > 0，触发元数据修复
- D-3. close 同步前校验：chunks 列表的 offset 连续性、size 之和与 content_size 一致

### 5.3 修复优先级

| 优先级 | 修复项 | 理由 |
|---|---|---|
| **P0** | 修复项 A-4（GC 路径） | 当前正在泄漏物理空间，volume 会再次 Full |
| **P0** | 修复项 C（next_file_key 持久化） | master 重启即数据覆盖，最危险 |
| **P1** | 修复项 A-1/A-2/A-3（FUSE write/read/delete） | 彻底消除推断，支持 hole/truncate |
| **P1** | 修复项 A-5（S3 路径） | 防止未来大对象支持时出问题 |
| **P2** | 修复项 B（FieldId 重命名） | 消除命名混乱，降低后续维护成本 |
| **P2** | 修复项 D（完整性校验） | 防御性措施，提升健壮性 |

### 5.4 验证计划

1. **单元测试**：
   - 多 chunk 文件 write/read/delete，校验每个 chunk 的 needle_id 唯一
   - GC 回收多 chunk 文件，校验所有 chunk 的 needle 被删除
   - master 重启后分配 file_key 不重复

2. **集成测试**：
   - 写入 64MB 文件（64 个 chunk），删除后 volume used_bytes 归零
   - truncate 文件变小后，chunks 列表正确反映剩余 chunk
   - 跨客户端读多 chunk 文件，md5 一致

3. **容器环境回归**：
   - fio 顺序写 64MB + 删除 + 再次写入，验证无空间泄漏
   - master 容器重启后，继续写入验证 file_key 不冲突

---

## 6. 后续延伸

### 6.1 stripe 模式的 file_key 设计

当前 stripe 设计（[file_layout_stripe_design.md](file:///home/portion/powerfs/docs/file_layout_stripe_design.md)）将大文件分布到多个 volume。stripe 模式下：
- 每条 stripe 写入不同 volume，每条 stripe 需要独立的 needle_id
- `FileLayout` 存入 `Entry.extended`，记录 volume_ids、stripe_size、stripe_count
- 每个 `FileChunk.fid` 存该 stripe chunk 在对应 volume 中的 needle_id
- locate() 算法根据 offset 定位到 stripe → volume → needle_id

修复项 A 完成后，stripe 模式天然支持：chunks 列表记录每个 stripe chunk 的独立 needle_id，无需额外改动。

### 6.2 与 lease 机制的关系

lease 是 per-inode 的（文件级），与 chunk 级 needle_id 解耦：
- lease 保护整个文件的写一致性
- chunk 级 needle_id 仅是物理存储定位
- write 请求需同时携带 inode（lease 校验）和 needle_id（存储定位）

修复项 B 的 `FieldId::Inode` + `FieldId::NeedleId` 明确区分两者语义。

---

## 7. 变更影响清单

| 文件 | 修复项 | 改动说明 |
|---|---|---|
| [powerfs-fuse/src/fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs) | A-1/A-2/A-3 | write/read/delete 从 chunks 列表取 needle_id |
| [powerfs-filer/src/meta_shard_manager.rs](file:///home/portion/powerfs/powerfs-filer/src/meta_shard_manager.rs) | A-4 | GC 遍历 chunks 列表删除所有 needle |
| [powerfs-filer/src/s3_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/s3_handler.rs) | A-5 | S3 多 chunk 支持 |
| [powerfs-fuse-core/src/provider_adapter.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs) | B | FieldId 新增 NeedleId/Inode，双写过渡 |
| [powerfs-net/src/protocol.rs](file:///home/portion/powerfs/powerfs-net/src/protocol.rs) | B | 新增 FieldId 变体 |
| [powerfs-volume/src/net_handler.rs](file:///home/portion/powerfs/powerfs-volume/src/net_handler.rs) | B | 优先读取新 FieldId |
| [powerfs-master/src/master.rs](file:///home/portion/powerfs/powerfs-master/src/master.rs) | C | next_file_key Raft 持久化 |
| [powerfs-master/src/raft_storage.rs](file:///home/portion/powerfs/powerfs-master/src/raft_storage.rs) | C | 新增 RaftCommand |

---

## 8. 附录：问题时间线

| 时间 | 问题 | 状态 |
|---|---|---|
| 2026-07-29 | filer 端伪造 file_key（rand::random） | 已修复 |
| 2026-07-30 | lease inode/file_key 不匹配 + 命名混乱 | 已修复（引入命名坑） |
| 2026-08-02 | file_key 重复导致静默数据丢失 | 已修复（FUSE 路径） |
| 2026-08-02 | GC 只删首个 chunk（孤儿数据泄漏） | **已修复**（本次实施） |
| 2026-08-02 | S3 路径 base file_key | **已修复**（本次实施） |
| 2026-08-02 | master next_file_key 未持久化 | **已修复**（本次实施） |

---

## 9. 实施总结（2026-08-02）

### 已完成修复项

| 修复项 | 状态 | 说明 |
|---|---|---|
| A-1 write 存独立 needle_id | ✅ | FileChunk.fid 字符串 → needle_id/volume_id 数值字段 |
| A-2 read 从 chunks 取 needle_id | ✅ | 按 offset 查找 chunk，直接用 needle_id，不再推断 |
| A-3 delete 遍历 chunks | ✅ | 遍历 entry.chunks 逐个删除，不再用 chunk_count 推断 |
| A-4 GC 遍历 chunks | ✅ | reclaim_data_chunks/retry_pending_reclaims 遍历 chunks 列表 |
| A-5 S3 路径适配 | ✅ | PartInfo 改用 needle_id/volume_id，移除 fid 字符串解析 |
| B FieldId 重命名 | ✅ | FileKey=needle_id, Inode=inode, Name 回归存 name |
| C next_file_key 持久化 | ✅ | RaftCommand::AdvanceFileKey + 批量预分配（1000/batch） |
| D fsck 交叉校验 | ✅ | powerfs-cli fsck 命令，元数据一致性检查（needle_id/volume_id 异常、重复、offset 空洞） |

### 结构变更

**FileChunk / CachedFileChunk / ChunkWire / StoredFileChunk** 统一改为：
```rust
pub struct FileChunk {
    pub offset: u64,
    pub size: u64,
    pub needle_id: u64,    // chunk 级存储键（替代旧 fid.file_key）
    pub volume_id: u64,    // 所属 volume（替代旧 fid.volume_id）
    pub crc32: u32,
    pub mtime: u64,
}
```

**protobuf FileChunk**（filer.proto + master.proto）同步修改。

**FieldId** 新增 `Inode = 0x97`，`FileKey = 0x94` 回归存 needle_id 语义。

### 额外修复

- **compact reclaimed 计算修复**：从基于 `used_bytes`（逻辑空间）改为基于 `append_offset`（物理空间），正确回收物理 hole
- **compact_cleanup 参数修复**：从 `freed_bytes`（错误减少 used_bytes）改为 `new_append_offset`（正确更新物理末尾）
- **compact 并发控制**：`AtomicBool compacting` 标志，compact 期间阻止 write_needle_blob
- **compact 自动触发**：5 分钟定时器 + 30% deleted 比例阈值（`should_compact()`）
- **compact gRPC 接口**：`CompactVolume` RPC + `powerfs-cli compact` 命令
- **compact 前 flush**：`coalescer.flush_all()` 防止 dirty 数据与 compact 冲突

### 改动文件清单

| 文件 | 改动 |
|---|---|
| powerfs-common/src/traits.rs | FileChunk 结构重定义 |
| powerfs-orset/src/lib.rs | CachedFileChunk 适配 |
| powerfs-coherence/src/lib.rs | ChunkWire 适配 |
| powerfs-filer/proto/filer.proto | protobuf FileChunk 修改 |
| powerfs-master/proto/master.proto | protobuf FileChunk 修改 |
| powerfs-net/src/protocol.rs | FieldId 重命名 + 新增 Inode |
| powerfs-fuse-core/src/provider_adapter.rs | TLV 编解码适配 |
| powerfs-fuse-core/src/fuse_client_facade.rs | 类型转换适配 |
| powerfs-fuse-core/src/volume_client.rs | FieldId 读取适配 |
| powerfs-fuse/src/fuse.rs | write/read/delete 从 chunks 取 needle_id |
| powerfs-fuse/src/cache.rs | chunk_from_proto 适配 |
| powerfs-filer/src/shard_store.rs | StoredFileChunk 适配 |
| powerfs-filer/src/meta_shard_manager.rs | GC 遍历 chunks |
| powerfs-filer/src/net_handler.rs | ChunkWire/StoredFileChunk 适配 |
| powerfs-filer/src/grpc_service.rs | ProtoFileChunk 转换适配 |
| powerfs-filer/src/posix_service.rs | ProtoFileChunk 转换适配 |
| powerfs-filer/src/provider_impl.rs | FileChunk 构造适配 |
| powerfs-volume/src/net_handler.rs | FieldId 读取适配 |
| powerfs-master/src/s3/server.rs | PartInfo + FileChunk 适配 |
| powerfs-master/src/raft_storage.rs | RaftCommand::AdvanceFileKey |
| powerfs-master/src/master.rs | next_file_key 批量预分配 + maybe_advance_file_key |
| powerfs-master/src/server.rs | allocate_file_key 改为 async |
| powerfs-core/src/volume.rs | compact reclaimed 计算修复 |
| powerfs-core/src/volume_metadata.rs | compact_cleanup 参数修复 |
| powerfs-core/tests/volume_test.rs | 测试断言更新 |

### 验证结果

- ✅ `cargo check --workspace` 通过
- ✅ `cargo clippy --workspace` 零警告
- ✅ `cargo fmt --all --check` 通过
- ✅ 所有 lib 单元测试通过（powerfs-common/orset/coherence/core/master/filer/fuse-core/fuse/volume/cli）
- ✅ 容器集成测试全部通过

### 容器集成测试结果（2026-08-02）

| 测试项 | 结果 | 关键验证 |
|---|---|---|
| 基础 write/read | ✅ | "hello powerfs" 正确读写，md5 一致 |
| 多 chunk 文件（10MB） | ✅ | md5 读写一致，10 个 chunk 各有独立 needle_id |
| GC 回收（delete 遍历 chunks） | ✅ | 删除 10MB 文件发送 10 个 DeleteNeedle 请求（seq=22-31） |
| 跨客户端一致性 | ✅ | fuse-1 写入，fuse-2 读取，md5 一致 |
| master 重启 file_key 不冲突 | ✅ | 重启后旧文件可读、新文件可写 |
| compact 执行 | ✅ | moved=312 needles，数据完整 |
| compact 后写入 | ✅ | 新文件写入读取成功 |
| compact gRPC 远程触发 | ✅ | `powerfs-cli compact` 命令成功 |
| fio 顺序写 | ✅ | 12.2 MiB/s（bs=64k, size=64M） |
| fio 顺序读 | ✅ | 107 MiB/s（bs=64k, size=64M） |
| fio 随机写 | ✅ | 6.5 MiB/s（bs=4k, size=4M） |
| fio 随机读 | ✅ | 6.0 MiB/s（bs=4k, size=4M） |
