# PowerFS 改进方案

> **版本**: v2.0
> **日期**: 2026-07-29
> **状态**: 待评审

---

## 第一部分：FUSE 调用流程问题修复

### 概述

经过对 FUSE 各文件系统调用的全链路分析（fuse.rs → SyncFuseClientFacade → provider_adapter → MetaShardClient/VolumeClient/MasterClient），识别出以下 P0-P3 级别问题。

---

### P0 级问题（阻塞性，必须首先修复）

#### P0-1: `parse_path_to_parent_name()` 对所有路径返回 `parent_ino=1`

| 项目 | 详情 |
|------|------|
| **位置** | `powerfs-fuse-core/src/provider_adapter.rs` |
| **影响** | 所有非 root 下的文件操作（create/lookup/delete）会使用错误的 parent_ino，导致元数据关联错乱 |
| **根因** | 简化实现硬编码返回 `(1, name)`，未正确解析目录层级 |
| **修复方案** | 1. 实现正确的路径 → inode 映射（通过 MetaShardClient 逐级查找）<br>2. 或改为全 inode-based 操作（FUSE 回调已直接提供 parent inode，无需从路径反推） |
| **涉及调用** | lookup, create, mkdir, unlink, rmdir, readdir, rename |

#### P0-2: statfs 返回硬编码假值

| 项目 | 详情 |
|------|------|
| **位置** | `powerfs-fuse/src/fuse.rs:1760-1775` |
| **影响** | 用户看到的文件系统容量/文件数永远是假值（1TB、1000万文件），无法反映真实存储状态 |
| **根因** | 完全没有查询 Volume 服务端，使用硬编码常量 |
| **修复方案** | 1. 通过 VolumeClient 发送 `MsgType::StatFs` 请求获取真实统计<br>2. 聚合多 Volume 信息返回给 FUSE<br>3. 需要在 Volume 服务端实现 StatFs 处理逻辑 |
| **涉及调用** | statfs |

#### P0-3: create_entry 中 parent_ino 硬编码为 1

| 项目 | 详情 |
|------|------|
| **位置** | `powerfs-fuse-core/src/provider_adapter.rs:551` |
| **影响** | 通过 FuseClientFacade 路径创建的文件全部挂载到 root 目录下 |
| **根因** | `FacadeMetadataProvider::create_entry()` 中 `parent_ino` 硬编码为 `1` |
| **修复方案** | 从 `entry.directory` 解析真实 parent_ino，或由上层显式传入 |
| **涉及调用** | create, mkdir |

---

### P1 级问题（功能性缺陷）

#### P1-1: setattr 不持久化到服务端

| 项目 | 详情 |
|------|------|
| **位置** | `powerfs-fuse/src/fuse.rs:603-680` |
| **影响** | chmod/chown/truncate 等操作仅在本地缓存生效，其他客户端不可见，重启后丢失 |
| **根因** | `fuse.rs:setattr()` 仅更新 MetadataCache，未调用 `SyncFuseClientFacade::update_entry()` |
| **修复方案** | 在本地缓存更新后，调用 `update_entry()` 同步到 Filer |
| **涉及调用** | setattr (chmod, chown, truncate, utimens) |

#### P1-2: getattr 缓存 miss 无服务端回退

| 项目 | 详情 |
|------|------|
| **位置** | `powerfs-fuse/src/fuse.rs:588-601` |
| **影响** | 缓存丢失/过期时直接返回 ENOENT，用户需要重新 lookup |
| **根因** | 仅查本地 MetadataCache，miss 时无回退逻辑 |
| **修复方案** | 缓存 miss 时回退调用 `get_entry_by_inode()` 从服务端获取 |
| **涉及调用** | getattr |

#### P1-3: create 操作无原子性保证

| 项目 | 详情 |
|------|------|
| **位置** | `powerfs-fuse/src/fuse.rs:863-991` |
| **影响** | `assign_fid` 成功但 `create_entry` 失败时，FID 已分配但元数据未创建，造成 FID 泄漏 |
| **根因** | 两个独立操作，无事务/回滚机制 |
| **修复方案** | 使用 Master 的 CreateFile 原子接口（分配 FID + 创建元数据一次完成），或实现两阶段提交 |
| **涉及调用** | create, mkdir |

---

### P2 级问题（性能/安全）

#### P2-1: write 中 unsafe 锁生命周期转换

| 项目 | 详情 |
|------|------|
| **位置** | `powerfs-fuse/src/fuse.rs:1280-1284` |
| **影响** | 潜在的内存安全问题，`unsafe` 指针转换获取 `'static` 生命周期锁 |
| **根因** | `get_write_lock()` 返回 `Arc<Mutex<()>>`，但代码将其转为 `&'static Mutex<()>` |
| **修复方案** | 重构锁管理，使用 `Arc<Mutex<()>>` 的正确生命周期管理 |
| **涉及调用** | write |

#### P2-2: 缺少批量 chunk 读写

| 项目 | 详情 |
|------|------|
| **位置** | `powerfs-fuse/src/fuse.rs:read()`, `write()` |
| **影响** | 每个 chunk（1MB）单独网络请求，1GB 文件需要 1024 次网络往返 |
| **根因** | 循环中逐 chunk 调用 `read_blob` / `write_blob` |
| **修复方案** | 1. 实现批量 ReadChunks/WriteChunks 接口<br>2. 减少网络往返，提升吞吐 |
| **涉及调用** | read, write, flush, fsync |

#### P2-3: append 模式下 size 可能过期

| 项目 | 详情 |
|------|------|
| **位置** | `powerfs-fuse/src/fuse.rs:1246-1252` |
| **影响** | 并发 append 时可能覆盖其他客户端已写入的数据 |
| **根因** | 从本地缓存获取 size，未验证是否为最新 |
| **修复方案** | append 前从服务端获取最新 size + flush 本地修改 |
| **涉及调用** | write (append mode) |

---

### P3 级问题（代码质量/清理）

#### P3-1: 大量 eprintln!/println! 调试噪音

| 项目 | 详情 |
|------|------|
| **位置** | `fuse.rs`, `fuse_client_facade.rs`, `provider_adapter.rs` |
| **影响** | 生产环境日志混乱，stderr 输出与日志框架冲突 |
| **修复方案** | 全部替换为 `log::debug!` 或移除 |

#### P3-2: Volume 路由每次读取可能触发 Master 查询

| 项目 | 详情 |
|------|------|
| **位置** | `fuse_client_facade.rs:get_volume_addr()` |
| **影响** | 缓存 miss 时回退查询 Master，增加读写延迟 |
| **修复方案** | 1. Mount 时一次性拉取所有 Volume 路由<br>2. 路由变更时通过 WatchChannel 推送通知<br>3. 减少 Master 查询频率 |

---

### 修复优先级与依赖关系

```
P0-1 (parse_path_to_parent_name)  ←── 基础，阻塞所有路径操作
  │
  ├──→ P0-3 (create_entry parent_ino)
  │
  └──→ P1-3 (create 原子性)

P0-2 (statfs)  ←── 独立，可并行修复

P1-1 (setattr 持久化)  ←── 依赖 P0-1 修复

P1-2 (getattr 回退)  ←── 独立

P2-1 (unsafe 锁)  ←── 可独立修复

P2-2 (批量读写)  ←── 依赖基础流程正确

P2-3 (append size)  ←── 依赖 P2-1

P3-1/2 (清理)  ←── 随时可做
```

---

## 第二部分：Volume 元数据与持久化设计（基于 RocksDB）

### 核心理念

**抛弃自定义 Superblock 二进制格式，将所有 Volume 管理数据存入 RocksDB。**

RocksDB 已内置于代码库（`kv_cache_persist.rs`、`kv_cache.rs`），具备：
- **WAL（Write-Ahead Log）**：崩溃后自动恢复，无需自研原子写协议
- **WriteBatch**：多键原子更新，天然解决一致性问题
- **Column Families**：逻辑隔离不同类型数据
- **LSM-Tree**：高写入吞吐，适合追加写入场景

---

### 1. Volume 目录结构

```
volume_<volume_id>/
  ├── volume.meta              # 极小引导文件 (~256 bytes)
  ├── volume.data              # Needle 数据文件（追加写入，原始二进制）
  └── volume_db/              # RocksDB 实例目录
      ├── ... (RocksDB 内部文件: .sst, .log, CURRENT, MANIFEST)
```

#### volume.meta — 引导文件

**仅包含最小化的引导信息**，用于在 Volume 启动时快速定位和验证 RocksDB：

```rust
pub struct VolumeBootstrap {
    pub magic: [u8; 4],           // b"PVOL" — 快速格式识别
    pub version: u16,             // 引导文件版本
    pub volume_id: u64,           // 用于验证 RocksDB 中的配置一致性
    pub db_path: String,          // RocksDB 目录路径（相对路径）
    pub data_path: String,        // 数据文件路径（相对路径）
    pub created_at: i64,          // 创建时间
    pub checksum: u32,            // CRC32 校验（仅覆盖此引导文件）
}
```

**特点：**
- < 256 bytes，一个扇区足够
- **不存储任何分配状态**（那些全在 RocksDB 里）
- 唯一目的：快速确认"这个目录是一个有效的 Volume"
- 校验失败 → 拒绝挂载或提示数据损坏

---

### 2. 后端存储适配分析

PowerFS 的 Volume 需要支持多种存储后端，核心区别在于**数据访问模型**。当前方案需要分为两种模型：

#### 2.1 两种存储模型

| 特性 | Model A: Append-File | Model B: Object-Per-Needle |
|------|---------------------|--------------------------|
| **数据组织** | 单个 `volume.data` 文件，Needle 追加写入 | 每个 Needle 一个独立对象/文件 |
| **写入方式** | 追加写入（append-only） | 原子创建（写时复制） |
| **读取方式** | 按 offset 随机读取 | 按 key 直接读取 |
| **删除方式** | 标记删除 + Compact 物理清理 | 版本控制 + 延迟删除 |
| **恢复方式** | 扫描文件重建索引 | 列出对象重建索引 |
| **典型后端** | LocalFile, SPDK-NVMe, RBD | S3, MinIO |

#### 2.2 后端适配方案

##### 2.2.1 LocalFile（本地文件） — Model A

| 项目 | 详情 |
|------|------|
| **数据路径** | `volume.data`（本地文件，append 写入） |
| **元数据路径** | `volume_db/`（同文件系统，RocksDB） |
| **Needle 格式** | `[NeedleId(8B)][Size(4B)][Data][Checksum(8B)]` |
| **L4 恢复** | ✅ 扫描 `volume.data` 重建索引 |
| **现有支持** | ✅ 已完整实现（`local_fs.rs`） |

##### 2.2.2 SPDK-NVMe（用户态 NVMe） — Model A

| 项目 | 详情 |
|------|------|
| **数据路径** | SPDK bdev（块设备，通过 RPC 写入） |
| **元数据路径** | 需要独立小分区存放 RocksDB（SPDK 不支持文件系统） |
| **Needle 格式** | 同 LocalFile，但写入通过 SPDK RPC |
| **L4 恢复** | ✅ 扫描 bdev 重建索引（通过 SPDK 读取） |
| **现有支持** | ✅ 已实现框架（`spdk_backend.rs`） |
| **注意** | RocksDB 需要一个小文件系统分区（如 /mnt/spdk-meta/） |

##### 2.2.3 RBD（Ceph RBD） — Model A

| 项目 | 详情 |
|------|------|
| **数据路径** | Ceph RBD 镜像（块设备，通过 librbd 读写） |
| **元数据路径** | 需要独立 RBD 或本地文件系统存放 RocksDB |
| **Needle 格式** | 同 LocalFile，通过 librbd 读写 |
| **L4 恢复** | ✅ 扫描 RBD 重建索引（通过 librbd 读取） |
| **现有支持** | ❌ 未实现，需要新增 `rbd_backend.rs` |
| **注意** | RBD 支持 append（通过 offset 写入），天然兼容 |

##### 2.2.4 S3 / 对象存储 — Model B（关键区别）

| 项目 | 详情 |
|------|------|
| **数据路径** | S3 Bucket，每个 Needle 一个对象 |
| **对象 Key** | `volume_{id}/needle_{needle_id}` |
| **元数据路径** | Volume 节点本地 RocksDB（**不在 S3 中**） |
| **Needle 格式** | 对象体直接存储 Needle 内容（无 Header/Footer 冗余） |
| **L4 恢复** | 列出 S3 objects 重建索引（`aws s3 ls volume_{id}/`） |
| **现有支持** | ❌ 未实现 |
| **核心区别** | S3 不支持 append → 写入即创建新对象，需版本控制 |

**S3 特殊处理：**
- 写入：创建新对象 `volume_{id}/needle_{new_id}` → 更新 RocksDB needles CF
- 删除：标记删除（在 RocksDB 中）+ 延迟删除 S3 对象（TTL 到期后清理）
- 覆盖：创建新版本对象 `volume_{id}/needle_{id}_v2` → 更新索引指向新版本 → 延迟删除旧版本
- 读取：直接 `GetObject(volume_{id}/needle_{id})` 读取，无需 offset 计算
- Compact：扫描 S3 前缀列出所有 Needle → 清理标记删除的对象

#### 2.3 RocksDB 位置说明

| 后端类型 | RocksDB 存储位置 | 说明 |
|----------|-----------------|------|
| LocalFile | 与 `volume.data` 同目录 | 共用文件系统，无需额外配置 |
| SPDK-NVMe | 独立小分区（如 `/mnt/spdk-meta/`） | SPDK 无文件系统支持 |
| RBD | 独立小 RBD + 文件系统 | 小型 ext4/xfs 存放 RocksDB |
| S3 | Volume 节点本地磁盘 | **关键**：元数据存储在本地，不在 S3 |

#### 2.4 后端抽象层设计

为统一两种模型，在 `StorageBackend` trait 上进行扩展：

```rust
pub trait VolumeStorageBackend: Send + Sync {
    // ===== 通用操作（所有模型共享）=====
    
    /// 读取 Needle 数据（通过 NeedleInfo 定位）
    fn read_needle(&self, info: &NeedleInfo) -> Result<Vec<u8>>;
    
    /// 写入 Needle（返回 offset/key 信息用于更新索引）
    fn write_needle(&self, volume_id: u64, needle: &Needle) -> Result<NeedleInfo>;
    
    /// 批量写入（原子性由上层保证）
    fn write_needles(&self, volume_id: u64, needles: &[Needle]) -> Result<Vec<NeedleInfo>>;
    
    /// 删除 Needle（标记删除）
    fn delete_needle(&self, info: &NeedleInfo) -> Result<()>;
    
    /// 获取 Volume 总容量
    fn total_capacity(&self) -> u64;
    
    /// 获取已使用空间
    fn used_space(&self) -> u64;
    
    /// 获取后端类型
    fn backend_type(&self) -> BackendType;
    
    // ===== Model A 专用（Append-File）=====
    
    /// 获取底层文件句柄（用于 L4 扫描重建）
    fn raw_file_handle(&self) -> Option<&dyn RawFileAccess>;
    
    // ===== Model B 专用（Object-Storage）=====
    
    /// 列出所有 Needle 对象（用于 L4 扫描重建 / Compact）
    fn list_objects(&self, volume_id: u64) -> Result<Vec<ObjectInfo>>;
    
    /// 批量删除过期对象
    fn purge_deleted_objects(&self, volume_id: u64, keys: &[String]) -> Result<()>;
}

// Model A: 原始文件访问（用于 L4 扫描）
pub trait RawFileAccess {
    fn read_at(&self, offset: u64, size: u32) -> Result<Vec<u8>>;
    fn file_size(&self) -> u64;
}

// Model B: 对象信息（用于 L4 重建）
pub struct ObjectInfo {
    pub key: String,
    pub needle_id: u64,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
}
```

#### 2.5 后端扩展路线图

| 阶段 | 后端 | 模型 | 状态 | 工作量 |
|------|------|------|------|--------|
| **Phase 3** | LocalFile | Model A | ✅ 现有 + 增强 | 小（扩展现有） |
| **Phase 3** | SPDK-NVMe | Model A | 🔧 框架已有 | 中（完善 RocksDB 位置） |
| **Phase 4** | RBD | Model A | ❌ 未实现 | 大（新增 backend） |
| **Phase 4** | S3 | Model B | ❌ 未实现 | 大（完全不同模型） |

**结论：** 当前 RocksDB 设计**完全支持 Model A 的所有后端**（LocalFile、SPDK-NVMe、RBD），对于 S3（Model B）需要适配层但核心架构不变。

---

### 3. RocksDB Column Families 设计

每个 Volume 一个 RocksDB 实例，包含 4 个 Column Family：

```
┌────────────────────────────────────────────────────────────────────┐
│ RocksDB Instance: volume_db/                                        │
│                                                                    │
│ CF: "config"        ← 不可变配置（写入后不变）                      │
│ ┌────────────────────────────────────────────────────────────────┐ │
│ │ Key: b"volume_config"                                          │ │
│ │ Value: VolumeConfig {                                         │ │
│ │   volume_id, backend_type, disk_uuid, fs_type, file_path,     │ │
│ │   volume_size, needle_hdr_sz, needle_ftr_sz,                  │ │
│ │   collection, replication, node_id, created_at                 │ │
│ │ }                                                             │ │
│ └────────────────────────────────────────────────────────────────┘ │
│                                                                    │
│ CF: "needles"       ← Needle 索引（核心查找路径）                  │
│ ┌────────────────────────────────────────────────────────────────┐ │
│ │ Key: NeedleId (u64 big-endian, 8 bytes)                        │ │
│ │ Value: NeedleInfo {                                           │ │
│ │   needle_id, volume_id, data_size, offset, checksum,          │ │
│ │   checksum_algorithm, created_at, deleted_at (Option)         │ │
│ │ }                                                             │ │
│ └────────────────────────────────────────────────────────────────┘ │
│                                                                    │
│ CF: "allocation"    ← 分配状态（随写操作原子更新）                  │
│ ┌────────────────────────────────────────────────────────────────┐ │
│ │ Key: b"stats"                                                 │ │
│ │ Value: AllocationStats {                                      │ │
│ │   used_bytes, free_bytes, next_needle_id, append_offset,      │ │
│ │   active_count, deleted_count, last_modified_at               │ │
│ │ }                                                             │ │
│ └────────────────────────────────────────────────────────────────┘ │
│                                                                    │
│ CF: "deleted"       ← 已删除 Needle 索引（Compact 触发依据）       │
│ ┌────────────────────────────────────────────────────────────────┐ │
│ │ Key: NeedleId (u64 big-endian)                                │ │
│ │ Value: DeletedInfo { deleted_at, original_size }              │ │
│ └────────────────────────────────────────────────────────────────┘ │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

### 4. 核心数据流

#### 4.1 文件 → 数据的查找路径（读路径）

```
FUSE read(fd, offset, size)
  │
  ├── FilerEntry (元数据服务)
  │   └── volume_id + needle_id (file_key)
  │
  └── VolumeClient → Volume Service
      │
      └── RocksDB "needles" CF
          └── get(needle_id) → NeedleInfo { offset, data_size }
              │
              └── 读取 volume.data 中 [offset, offset+data_size)
                  └── Needle Footer 校验 checksum
```

**关键点：**
- **两跳查找**：FilerEntry → RocksDB → 数据文件
- 所有元数据查询走 RocksDB，O(log n) 查找
- Needle footer 的 checksum 提供数据完整性保障

#### 4.2 写入路径（写路径）

```
FUSE write(fd, offset, data)
  │
  ├── VolumeClient → Volume Service
  │
  ├── 1. 追加写入 volume.data (append)
  │      └── 计算 Needle = [NeedleId | Size | Data | Checksum]
  │      └── 写入位置 = allocation_stats.append_offset
  │
  ├── 2. 原子更新 RocksDB WriteBatch
  │      ├── "needles" CF: insert(needle_id, NeedleInfo)
  │      └── "allocation" CF: update(stats: {append_offset, used_bytes, ...})
  │
  └── 3. 返回写入成功
```

**关键点：**
- **Step 1** 先写数据文件（追加写入，失败则重试或回滚）
- **Step 2** 通过 RocksDB WriteBatch 原子更新索引和分配状态
- 要么全部成功，要么全部失败（RocksDB 保证）
- **无中间状态**，不会出现"数据已写但索引不存在"

#### 4.3 删除路径

```
FUSE unlink / rmdir
  │
  ├── VolumeClient → Volume Service
  │
  ├── 1. RocksDB WriteBatch
  │      ├── "needles" CF: update(needle_id → mark deleted_at = now)
  │      ├── "deleted" CF: insert(needle_id, DeletedInfo)
  │      └── "allocation" CF: update(stats.deleted_count++)
  │
  └── 2. 数据保留在 volume.data 中（标记删除，Compact 时物理清理）
```

#### 4.4 Compact 路径

```
触发条件: stats.deleted_count / stats.active_count > compact_threshold (默认 20%)
  │
  ├── 1. 扫描 "deleted" CF，获取所有被删除的 needle_id
  │
  ├── 2. 扫描 "needles" CF 中活跃的 Needle
  │      └── 读取每个活跃 Needle 的数据
  │      └── 计算新位置（从新 volume.data 起始位置）
  │
  ├── 3. 原子切换
  │      ├── 清空 "deleted" CF
  │      ├── 更新 "needles" CF 中的 offset 为新位置
  │      ├── 重写 volume.data（只包含活跃数据）
  │      └── 更新 "allocation" CF 中的 append_offset
  │
  └── 4. 释放磁盘空间
```

---

### 5. 一致性保证

#### 5.1 写入原子性

```rust
// 伪代码：写 Needle 的原子操作
fn write_needle(volume: &Volume, needle: &Needle) -> Result<()> {
    // Step 1: 写入数据文件（追加）
    let offset = volume.allocation.append_offset;
    volume.data_file.seek_write(offset, &needle.to_bytes())?;
    
    // Step 2: 原子更新 RocksDB（WriteBatch = 原子）
    let mut batch = WriteBatch::default();
    
    let info = NeedleInfo {
        needle_id: needle.id,
        offset,
        data_size: needle.data_size() as u32,
        checksum: needle.checksum,
        created_at: Utc::now(),
        // ...
    };
    batch.put_cf("needles", &needle.id.0.to_be_bytes(), serialize(&info));
    
    let mut stats = volume.allocation_stats();
    stats.append_offset = offset + needle.size() as u64;
    stats.used_bytes += needle.size() as u64;
    stats.active_count += 1;
    batch.put_cf("allocation", b"stats", serialize(&stats));
    
    volume.db.write(batch)?;  // 要么全部成功，要么全部失败
    
    Ok(())
}
```

#### 5.2 崩溃恢复

```
Volume 启动:
  │
  ├── 1. 读取 volume.meta (引导文件)
  │      └── 验证 magic + CRC32
  │
  ├── 2. 打开 RocksDB (volume_db/)
  │      └── RocksDB 自动重放 WAL → 恢复到最后一次 WriteBatch 提交的状态
  │
  ├── 3. 验证一致性
  │      ├── 读取 "config" CF → 验证 volume_id 与 volume.meta 一致
  │      ├── 读取 "allocation" CF → 恢复分配状态
  │      └── 检查 volume.data 末尾是否有未索引的数据
  │           └── 如果有 → 截断到最后一个已知的 append_offset（安全回滚）
  │
  └── 4. 启动完成
```

**关键特性：**
- **不会出现"数据写了但索引没写"**：WriteBatch 保证要么都写要么都不写
- **不会出现"索引写了但数据没写"**：Step 1 先写数据，Step 2 再写索引
- **崩溃后自动恢复**：RocksDB WAL 自动重放，无需手动扫描

#### 5.3 唯一需要处理的边界情况

| 场景 | 处理方式 |
|------|----------|
| 数据文件写入成功，但 WriteBatch 提交前崩溃 | 下次启动时截断数据文件到旧 append_offset |
| WriteBatch 提交成功，但数据文件写入前崩溃 | 不会发生（顺序保证） |
| 两者都成功但 volume.meta 损坏 | volume.meta 不关键，可从 RocksDB 中重建 |

---

### 6. Volume 元数据备份与恢复（四层策略）

**核心理念：** volume.data 中的每个 Needle 是自描述的（Header 含 NeedleId+Size，Footer 含 Checksum），因此 RocksDB 索引本质上是**性能优化**而非正确性要求。即使 RocksDB 完全丢失，也可通过扫描 volume.data 重建索引。

#### 6.1 四层备份恢复架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                        备份恢复分层架构                                │
│                                                                      │
│  L1: RocksDB WAL (已内置)                                             │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ 场景: 进程崩溃 / 机器重启                                        │   │
│  │ 恢复: RocksDB 自动 replay WAL → 恢复到最后一次 WriteBatch       │   │
│  │ RTO: < 1s (启动时自动)                                          │   │
│  │ 丢失窗口: 0 (WAL 保证已提交的 WriteBatch 不丢)                   │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                           ↓ 如果 WAL 也损坏                           │
│  L2: RocksDB Checkpoint (周期性快照)                                  │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ 场景: RocksDB SST 文件损坏 / WAL 丢失                            │   │
│  │ 恢复: 从最近的 Checkpoint 目录复制 → 重启 Volume                 │   │
│  │ RTO: < 30s (复制快照 + 启动)                                    │   │
│  │ 丢失窗口: 最大 5 分钟 (快照间隔)                                  │   │
│  │ 触发: 每 5 分钟 或 每 1000 次写操作                              │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                           ↓ 如果 Checkpoint 也丢失                    │
│  L3: 远程备份 (增量同步到 S3 / 备份节点)                               │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ 场景: 本地磁盘故障 / 整机损坏                                    │   │
│  │ 恢复: 从 S3/备份节点拉取最新快照 → 恢复 RocksDB                  │   │
│  │ RTO: < 5min (网络下载 + 启动)                                   │   │
│  │ 丢失窗口: 最大 15 分钟 (远程同步间隔)                             │   │
│  │ 触发: 每 15 分钟 或 L2 Checkpoint 创建后                         │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                           ↓ 如果远程备份也不可用                       │
│  L4: volume.data 扫描重建 (终极兜底)                                   │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ 场景: RocksDB + Checkpoint + 远程备份全部不可用                   │   │
│  │ 恢复: 扫描 volume.data 中每个 Needle 的 Header → 重建索引        │   │
│  │ RTO: 数分钟~数小时 (取决于 Volume 大小)                          │   │
│  │ 丢失窗口: 0 (Needle 数据本身完好，仅重建索引)                     │   │
│  │ 触发: 检测到 RocksDB 不可恢复 + 管理员确认                      │   │
│  └────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

#### 6.2 L1: RocksDB WAL（已内置）

**无需开发**。RocksDB 自带的 Write-Ahead Log 保证：
- 每次 `WriteBatch` 提交前，先写入 WAL
- 崩溃恢复时自动 replay WAL 到最新一致状态
- 保证已提交的写操作不丢失

#### 6.3 L2: RocksDB Checkpoint（周期性快照）

**实现方式：** 使用 RocksDB 内置的 `Checkpoint` API 创建一致性快照：

```rust
pub fn create_checkpoint(&self, volume_id: u64) -> Result<PathBuf> {
    let checkpoint_dir = self.backup_dir.join(format!(
        "checkpoint_{}_{}",
        volume_id,
        chrono::Utc::now().format("%Y%m%d_%H%M%S")
    ));
    
    let cp = self.db.checkpoint();
    cp.create_checkpoint(&checkpoint_dir)
        .map_err(|e| PowerFsError::Internal(format!("checkpoint failed: {}", e)))?;
    
    // 清理旧检查点（保留最近 3 个）
    self.cleanup_old_checkpoints(volume_id, 3)?;
    
    Ok(checkpoint_dir)
}

pub fn restore_from_checkpoint(&self, checkpoint_dir: &Path) -> Result<()> {
    // 1. 关闭当前 RocksDB
    drop(self.db);
    
    // 2. 清空当前 db 目录
    fs::remove_dir_all(&self.db_path)?;
    
    // 3. 从检查点复制
    fs::copy_dir_recursive(checkpoint_dir, &self.db_path)?;
    
    // 4. 重新打开
    self.db = RocksDB::open(&self.db_options, &self.db_path)?;
    
    Ok(())
}
```

**触发策略：**
```rust
// 每 5 分钟 或 每 1000 次写操作触发
fn should_create_checkpoint(&self) -> bool {
    let now = Utc::now();
    let interval_elapsed = (now - self.last_checkpoint_at).num_seconds() > 300;
    let write_count_exceeded = self.writes_since_last_checkpoint >= 1000;
    interval_elapsed || write_count_exceeded
}
```

#### 6.4 L3: 远程备份（增量同步）

**实现方式：** 将 L2 的 Checkpoint 增量同步到 S3 或备份节点：

```rust
pub fn sync_to_remote(&self, volume_id: u64, checkpoint_dir: &Path) -> Result<()> {
    // 方案 A: S3 上传（复用现有 kv_cache_persist.rs 的 S3 能力）
    let key = format!("volume_backups/{}/checkpoint.tar.zst", volume_id);
    self.s3_client.upload_file(checkpoint_dir, &key)?;
    
    // 方案 B: 备份节点同步
    // self.backup_node.sync_checkpoint(volume_id, checkpoint_dir)?;
    
    Ok(())
}

pub fn restore_from_remote(&self, volume_id: u64) -> Result<PathBuf> {
    let key = format!("volume_backups/{}/checkpoint.tar.zst", volume_id);
    let local_path = self.backup_dir.join(format!("remote_restore_{}", volume_id));
    self.s3_client.download_file(&key, &local_path)?;
    Ok(local_path)
}
```

**触发策略：**
```
L2 Checkpoint 创建完成 → 异步触发 L3 远程同步
  或
每 15 分钟定时检查 → 将最新 Checkpoint 推送到远程
```

#### 6.5 L4: volume.data 扫描重建（终极兜底）

**核心原理：** 每个 Needle 的 Header (12B) 自描述，可逐一解析：

```
volume.data 布局（offset 标注）:
  ┌─────────────────────────────────────────────┐
  │ Offset 0: [NeedleId(8B)|Size(4B)|Data|Checksum(8B)]  ← Needle 0
  │ Offset N: [NeedleId(8B)|Size(4B)|Data|Checksum(8B)]  ← Needle 1
  │ ...                                         │
  └─────────────────────────────────────────────┘

扫描算法:
  从 Offset 0 开始:
    1. 读取 12B Header → 解析 needle_id 和 data_size
    2. 读取完整 Needle (12 + data_size + 8 字节)
    3. 验证 Footer Checksum
       ✓ 有效 → 记录 NeedleInfo { needle_id, offset, data_size, checksum }
       ✗ 无效 → 跳过此 Needle（标记为损坏）
    4. offset += needle_total_size
    5. 重复直到文件结束
```

**伪代码：**

```rust
pub fn rebuild_index_from_data(&self) -> Result<RebuildReport> {
    let mut offset: u64 = 0;
    let mut needles: Vec<NeedleInfo> = Vec::new();
    let mut corrupted: Vec<(u64, String)> = Vec::new(); // (offset, reason)
    let mut total_bytes: u64 = 0;
    
    while offset < self.data_file_size {
        // Step 1: 读取 Needle Header (12 bytes)
        let header = self.read_exact(offset, 12)?;
        if header.len() < 12 { break; } // 文件末尾
        
        let needle_id = u64::from_be_bytes(header[0..8].try_into()?);
        let data_size = u32::from_be_bytes(header[8..12].try_into()?) as usize;
        let needle_total = 12 + data_size + 8; // header + data + footer
        
        // Step 2: 读取完整 Needle
        let full_needle = self.read_exact(offset, needle_total)?;
        
        // Step 3: 验证 Checksum
        let stored_checksum = u64::from_be_bytes(
            full_needle[12 + data_size..20].try_into()?
        );
        let computed_checksum = compute_blake3(&full_needle[12..12+data_size]);
        
        if stored_checksum == computed_checksum {
            // Needle 有效 → 记录索引
            needles.push(NeedleInfo {
                needle_id,
                volume_id: self.config.volume_id,
                data_size: data_size as u32,
                offset,
                checksum: stored_checksum,
                checksum_algorithm: "BLAKE3".to_string(),
                created_at: Utc::now(),
                deleted_at: None,
            });
            total_bytes += needle_total as u64;
        } else {
            // Needle 损坏 → 跳过
            corrupted.push((offset, format!("checksum mismatch for needle {}", needle_id)));
        }
        
        // Step 4: 移动到下一个 Needle
        offset += needle_total as u64;
    }
    
    // Step 5: 批量写入 RocksDB 重建索引
    let mut batch = WriteBatch::default();
    for info in &needles {
        batch.put_cf("needles", &info.needle_id.to_be_bytes(), serialize(info));
    }
    
    let stats = AllocationStats {
        used_bytes: total_bytes,
        free_bytes: self.config.volume_size - total_bytes,
        next_needle_id: needles.iter().map(|n| n.needle_id).max().unwrap_or(0) + 1,
        append_offset: offset,
        active_count: needles.len() as u64,
        deleted_count: 0,
        last_modified_at: Utc::now().timestamp(),
    };
    batch.put_cf("allocation", b"stats", serialize(&stats));
    
    self.db.write(batch)?;
    
    Ok(RebuildReport {
        total_needles: needles.len(),
        corrupted_needles: corrupted.len(),
        total_recovered_bytes: total_bytes,
        last_offset: offset,
    })
}
```

**RebuildReport：**
```rust
pub struct RebuildReport {
    pub total_needles: u64,           // 成功重建的 Needle 数
    pub corrupted_needles: u64,       // 损坏无法恢复的 Needle 数
    pub total_recovered_bytes: u64,   // 恢复的总字节数
    pub last_offset: u64,             // 扫描结束位置
}
```

**L4 恢复流程：**
```
1. 管理员确认执行 volume.data 扫描重建
2. 备份当前损坏的 RocksDB 目录（保留原始证据）
3. 创建新的空 RocksDB 实例
4. 扫描 volume.data，重建 needles CF + allocation CF
5. 验证重建结果（抽查若干 Needle 可正常读取）
6. 标记 Volume 为 Degraded 模式（只读，等待数据校验通过后恢复写入）
7. 生成 RebuildReport 上报给 Master
```

#### 6.6 备份恢复流程总结

| 层级 | 触发场景 | 恢复方式 | RTO | 最大数据丢失 | 代码量 |
|------|----------|----------|-----|-------------|--------|
| **L1** | 进程崩溃/重启 | RocksDB WAL 自动 replay | < 1s | 0 | 0 行（内置） |
| **L2** | RocksDB 损坏/WAL 丢失 | 从 Checkpoint 复制恢复 | < 30s | 5 分钟 | ~100 行 |
| **L3** | 磁盘故障/整机损坏 | 从 S3/备份节点拉取 | < 5min | 15 分钟 | ~80 行 |
| **L4** | 全部备份不可用 | 扫描 volume.data 重建 | 数分钟~小时 | 0（Needle 数据完好） | ~150 行 |

**关键优势：**
- **L4 是零丢失的**：Needle 数据本身完好，仅重建索引，数据不丢
- **L4 可验证**：每个 Needle 都有 Checksum，重建时即可验证完整性
- **RTO 渐进**：从秒级到小时级，覆盖所有故障场景
- **无死角**：即使所有备份机制都失效，数据仍可恢复

---

### 7. Master 端轻量备份

#### 7.1 设计原则

Master **不存储全量 Volume 元数据**，仅保留心跳快照用于跨节点灾难恢复：

```rust
pub struct VolumeHeartbeatInfo {
    pub volume_id: u64,
    pub node_id: String,
    // 不可变属性快照（Volume 创建时上报一次）
    pub backend_type: u8,
    pub disk_uuid: String,
    pub fs_type: String,
    pub file_path: String,
    pub volume_size: u64,
    pub collection_name: String,
    pub replication_config: String,
    // 可变快照（心跳上报，可能过时）
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub active_needle_count: u64,
    pub deleted_needle_count: u64,
    // 一致性标识
    pub heartbeat_at: i64,
}
```

#### 7.2 更新时机

- Volume 注册时 → 上报全量信息
- 心跳时（每 10s）→ 仅上报可变统计
- Volume 注销时 → 从 Master 列表移除

#### 7.3 恢复场景

| 场景 | 恢复方式 |
|------|----------|
| Volume 节点重启 | 本地 RocksDB WAL 自动恢复，无需 Master |
| Volume 数据库损坏但数据文件完好 | 重新注册 Volume，数据由 RocksDB 保证一致 |
| Volume 节点永久故障 | Master 标记 Volume 为 offline，管理员介入 |
| 整个集群灾难恢复 | Master 保留的 Volume 路由表可重建最小化配置 |

#### 7.4 与旧方案的区别

| 对比项 | 旧 Superblock 方案 | 新 RocksDB 方案 |
|--------|-------------------|-----------------|
| Master 存储 | 完整 VolumeMetaBackup（含分配状态） | 轻量心跳快照 |
| 恢复来源 | Master 是唯一可靠恢复源 | 本地 RocksDB 是主恢复源 |
| Master 职责 | 一致性关键路径 | 仅路由 + 心跳监控 |
| 故障影响 | Master 数据丢失 → Volume 不可恢复 | Master 故障不影响 Volume 自身数据 |

---

### 8. 数据结构定义

#### 8.1 VolumeConfig（不可变，写入 "config" CF）

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeConfig {
    pub volume_id: u64,
    pub backend_type: u8,              // 0=LocalFile, 1=SPDK-NVMe, 2=RBD
    pub disk_uuid: String,             // 物理硬盘 UUID
    pub fs_type: String,               // ext4/xfs/btrfs
    pub file_path: String,             // volume.data 的绝对路径
    pub volume_size: u64,              // 逻辑总大小
    pub needle_header_size: u32,        // 默认 12
    pub needle_footer_size: u32,       // 默认 8
    pub collection_name: String,
    pub replication_config: String,
    pub node_id: String,
    pub created_at: i64,
}
```

#### 8.2 NeedleInfo（"needles" CF 的 Value）

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeedleInfo {
    pub needle_id: u64,
    pub volume_id: u64,
    pub data_size: u32,
    pub offset: u64,                    // 在 volume.data 中的偏移
    pub checksum: u64,
    pub checksum_algorithm: String,
    pub created_at: i64,
    pub deleted_at: Option<i64>,        // None=活跃, Some=已删除
}
```

#### 8.3 AllocationStats（"allocation" CF 的 Value）

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationStats {
    pub used_bytes: u64,               // 所有活跃 Needle 实际占用
    pub free_bytes: u64,               // Volume 剩余可用
    pub next_needle_id: u64,           // 下一个可分配的 NeedleId
    pub append_offset: u64,            // 下一个 Needle 的写入偏移
    pub active_count: u64,             // 活跃 Needle 数
    pub deleted_count: u64,            // 已删除未 Compact 数
    pub last_modified_at: i64,
}
```

#### 8.4 DeletedInfo（"deleted" CF 的 Value）

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletedInfo {
    pub deleted_at: i64,
    pub original_size: u64,
}
```

---

### 9. 简化后的收益

| 维度 | 旧 Superblock 方案 | 新 RocksDB 方案 |
|------|-------------------|-----------------|
| **代码复杂度** | 高（~1500 行自研二进制格式） | 低（~300 行配置 + 序列化） |
| **一致性保证** | 需自研原子写协议 + 双重校验 | RocksDB WriteBatch 天然原子 |
| **崩溃恢复** | 自研 WAL + 扫描重建 | RocksDB 自动 WAL replay |
| **校验逻辑** | 需 SHA-256 + CRC32 分层校验 | 可选的 Needle Footer checksum |
| **备份机制** | 尾部副本 + Master 完整备份 | Master 心跳快照（极简） |
| **维护成本** | 高（二进制格式升级需兼容） | 低（JSON 序列化，升级简单） |
| **学习成本** | 高（理解自研格式） | 低（标准 RocksDB 用法） |
| **性能** | 中等（自定义 I/O 路径） | 高（RocksDB LSM-Tree 优化） |

---

## 实施计划

### Phase 1: 修复 FUSE 基础流程（P0 级）

| 任务 | 说明 | 预估时间 |
|------|------|----------|
| 修复 parse_path_to_parent_name | 实现正确的路径解析或改用 inode-based 操作 | 4h |
| 修复 create_entry parent_ino | 从 entry.directory 解析真实 parent_ino | 2h |
| 实现 statfs 真实查询 | Volume 端新增 StatFs 处理，FUSE 端调用 | 6h |
| 编译和单元测试验证 | 确保所有修改编译通过 | 2h |

### Phase 2: 修复功能性缺陷（P1 级）

| 任务 | 说明 | 预估时间 |
|------|------|----------|
| setattr 持久化到 Filer | 本地更新 + 服务端同步 | 4h |
| getattr 缓存回退 | miss 时从服务端获取 | 3h |
| create 原子化 | 实现两阶段提交或 Master 原子接口 | 8h |

### Phase 3: Volume 元数据持久化（RocksDB 方案 + 四层备份恢复）

| 任务 | 说明 | 预估时间 | 状态 |
|------|------|----------|------|
| 定义 VolumeConfig / AllocationStats 结构体 | serde 序列化，放入 powerfs-common | 3h | ✅ 完成 |
| 实现 VolumeBootstrap | 极小引导文件读写 + CRC32 校验 | 2h | ✅ 完成 |
| 实现 VolumeMetadata 模块 | RocksDB 封装（4 个 CF + WriteBatch 原子写） | 8h | ✅ 完成 |
| 迁移 Volume 索引到 RocksDB | 替换现有 sled PersistentIndex | 4h | ✅ 完成 |
| 实现写路径原子更新 | 数据写入 + RocksDB WriteBatch | 4h | ✅ 完成 |
| 消除 Volume 冗余统计字段 | 移除 free_space/next_offset，统一从 RocksDB allocation CF 获取 | 2h | ✅ 完成 |
| 统一删除策略为硬删除 | delete_needle_atomic + restore_needle_atomic + purge_expired_deleted | 3h | ✅ 完成 |
| 实现崩溃恢复（L1） | WAL replay + 数据截断回滚（内置，仅需测试） | 2h | ✅ 完成（3 个测试覆盖） |
| 实现 Checkpoint 快照（L2） | RocksDB Checkpoint API + 触发策略 + 恢复 | 6h | ✅ 已实现（API 层） |
| 实现远程备份（L3） | Checkpoint 异步同步到 S3 + 远程恢复 | 4h | 待开始 |
| 实现 volume.data 扫描重建（L4） | Needle Header 解析 + Checksum 验证 + 索引重建 | 6h | ✅ 已实现（API 层） |
| 实现心跳上报 | Master 端 VolumeHeartbeatInfo | 3h | 待开始 |
| 实现 Compact 逻辑 | 删除扫描 + 数据重写 + 原子切换 | 8h | ✅ 完成（compact_cleanup） |
| 实现磁盘信息采集 | Disk UUID + FS Type + Backend Type | 2h | 待开始 |

### Phase 4: 性能与安全修复（P2 级）

| 任务 | 说明 | 预估时间 |
|------|------|----------|
| 修复 unsafe 锁 | 重构为安全的生命周期管理 | 4h |
| 实现批量 chunk 读写 | 新增批量接口 | 8h |
| 修复 append size 问题 | append 前获取最新 size | 3h |

### Phase 5: 代码清理（P3 级）

| 任务 | 说明 | 预估时间 |
|------|------|----------|
| 清理 eprintln!/println! | 替换为 log::debug! | 2h |
| 优化 Volume 路由缓存 | Mount 时一次性拉取 | 3h |

---

## 附录

### A. 引用的现有类型

| 现有类型 | 位置 | 与新设计的关系 |
|----------|------|---------------|
| `VolumeId` | `powerfs-common/src/types.rs` | 继续使用 u64，由 RocksDB "config" CF 持久化 |
| `VolumeInfo` | `powerfs-common/src/types.rs` | 扩展为 VolumeConfig |
| `VolumeRoute` | `powerfs-common/src/types.rs` | Master 路由表，心跳上报 AllocationStats 摘要 |
| `NeedleInfo` | `powerfs-common/src/types.rs` | 扩展 deleted_at 字段，存入 RocksDB "needles" CF |
| `VolumeMeta` | `powerfs-core/src/storage_backend/local_fs.rs` | 替换为 RocksDB "allocation" CF 中的 AllocationStats |
| `NeedleIndex` trait | `powerfs-core/src/index.rs` | 新增 RocksDBNeedleIndex 实现，替代 sled PersistentIndex |

### B. 需要新增的文件/模块

| 文件 | 说明 |
|------|------|
| `powerfs-common/src/volume_config.rs` | VolumeConfig / AllocationStats / DeletedInfo 结构体 |
| `powerfs-core/src/volume_metadata.rs` | VolumeMetadata 模块（RocksDB 封装） |
| `powerfs-core/src/volume_bootstrap.rs` | VolumeBootstrap 引导文件读写 |

### C. 需要替换的文件

| 文件 | 说明 |
|------|------|
| `powerfs-core/src/index.rs` | PersistentIndex (sled) → RocksDBNeedleIndex |
| `powerfs-core/src/volume.rs` | 替换内存中的 free_space/next_offset 为 RocksDB 查询 |

### D. 配置项新增

```toml
# powerfs.toml 新增
[volume_metadata]
# RocksDB 压缩类型: none / lz4 / zstd / bzip2
compression = "lz4"
# WriteBuffer 大小（MB），影响写入性能
write_buffer_mb = 64
# Compact 触发阈值（删除占比 %）
compact_threshold_percent = 20
# Volume 启动时是否执行数据一致性检查
verify_on_startup = true
```

### E. RocksDB Column Family 完整列表

| CF 名称 | Key 格式 | Value 格式 | 读写频率 | 一致性要求 |
|---------|----------|-----------|----------|-----------|
| `config` | `b"volume_config"` | JSON(VolumeConfig) | 极低（创建时写一次） | 最终一致即可 |
| `needles` | u64 BE | JSON(NeedleInfo) | 高（每次读写） | 与数据文件原子 |
| `allocation` | `b"stats"` | JSON(AllocationStats) | 高（每次写） | 与 needles 原子 |
| `deleted` | u64 BE | JSON(DeletedInfo) | 中（删除时写） | 最终一致即可 |

---

## 第三部分：Filer 元数据备份与恢复

### 核心理念

Filer 元数据（Inode、目录结构、权限等）是文件系统语义的唯一来源，**一旦丢失无法从 Volume 数据反推**。与 Volume 元数据（可通过扫描 `volume.data` 的 Needle Header 重建）不同，Filer 不存在 L4 扫描重建兜底——必须依赖 Raft 副本 + 定期快照 + 远程备份的多层保护。

### 1. Filer 元数据架构

#### 1.1 当前实现

```
┌──────────────────────────────────────────────────────────────────────┐
│ Filer MetaShard 架构                                                  │
│                                                                      │
│ MetaShardManager                                                     │
│  ├── Shard 0: [0, 100000)                                            │
│  │   ├── Raft Group (3 节点副本)                                     │
│  │   └── ShardStore (RocksDB)                                        │
│  │       ├── CF: inodes          → InodeInfo { inode, parent, chunks }│
│  │       ├── CF: dir_entries    → dirname → [child_name, child_inode]│
│  │       ├── CF: stats           → ShardStats                        │
│  │       ├── CF: metadata       → root_inodes (bucket 映射)          │
│  │       ├── CF: orset_state    → CRDT OR-Set 状态                   │
│  │       └── CF: tombstones     → CRDT tombstone 列表                │
│  │                                                                    │
│  ├── Shard 1: [100000, 200000)                                       │
│  │   └── ... (同上)                                                  │
│  │                                                                    │
│  └── Shard N: [(N*100000), ((N+1)*100000))                          │
│      └── ...                                                          │
└──────────────────────────────────────────────────────────────────────┘
```

**关键特性：**
- **Raft 共识**：每个 MetaShard 有 3 个 Raft 副本，已提供高可用
- **RocksDB 持久化**：元数据已持久化到 RocksDB，支持崩溃恢复
- **CRDT OR-Set**：并发目录操作使用无冲突复制数据类型

#### 1.2 与 Volume 元数据的依赖关系

```
文件系统操作依赖链:
  FilerEntry.inode → FilerEntry.chunks[].fid → Volume Index (needle_id)
                                                    ↓
                                              NeedleInfo { offset, size }
                                                    ↓
                                              volume.data [offset, offset+size)

灾难恢复顺序（必须）:
  1. 恢复 Master 路由表 → 知道哪些 Volume 存在
  2. 恢复 Volume 索引 → 数据可定位（通过 Needle Header 扫描重建）
  3. 恢复 Filer 元数据 → 文件系统语义恢复
```

**核心结论：Volume 数据是 Filer 的恢复前提，但 Filer 元数据是最终目标。**

---

### 2. Filer 元数据分层备份策略

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Filer 元数据备份分层                                │
│                                                                      │
│  L1: Raft 副本（已内置）                                              │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ 场景: 单节点故障                                                 │   │
│  │ 恢复: Raft 自动选举新 Leader，Follower 同步日志                  │   │
│  │ RTO: < 5s (Leader 选举 + 日志同步)                              │   │
│  │ 丢失窗口: 0 (Raft 保证已提交日志不丢)                           │   │
│  │ 实现: 已存在（raft_group_manager.rs）                            │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                           ↓ 如果 Raft 日志损坏                         │
│  L2: RocksDB Checkpoint（周期性快照）                                 │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ 场景: Raft 日志丢失/RocksDB 损坏                               │   │
│  │ 恢复: 从 Checkpoint 复制恢复 RocksDB → Raft 从快照重建           │   │
│  │ RTO: < 1min (复制 + Raft 恢复)                                  │   │
│  │ 丢失窗口: 最大 5 分钟 (快照间隔)                                  │   │
│  │ 触发: 每 5 分钟 或 每 5000 次写操作                              │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                           ↓ 如果 Checkpoint 也丢失                    │
│  L3: 远程备份（同步到 S3/备份节点）                                    │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ 场景: 本地磁盘故障/整机损坏                                      │   │
│  │ 恢复: 从 S3 拉取快照 → 恢复 RocksDB → Raft 重建                 │   │
│  │ RTO: < 10min (网络下载 + 恢复)                                   │   │
│  │ 丢失窗口: 最大 15 分钟 (远程同步间隔)                             │   │
│  │ 触发: 每 15 分钟 或 L2 创建后异步同步                             │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ⚠️ 无 L4：Filer 元数据不存在扫描重建兜底！                          │
│     → 必须保证 L3 远程备份的可靠性                                   │
│     → 建议保留至少 3 个历史快照（30 天滚动）                          │
└──────────────────────────────────────────────────────────────────────┘
```

---

### 3. L2: RocksDB Checkpoint 实现

#### 3.1 扩展 ShardStore

```rust
impl ShardStore {
    /// 创建 Checkpoint 快照（不阻塞写入）
    pub fn create_checkpoint(&self) -> Result<PathBuf> {
        let checkpoint_dir = self.backup_dir.join(format!(
            "shard_{}_checkpoint_{}",
            self.shard_id.0,
            chrono::Utc::now().format("%Y%m%d_%H%M%S")
        ));
        
        let cp = self.db.checkpoint();
        cp.create_checkpoint(&checkpoint_dir)
            .map_err(|e| PowerFsError::Internal(format!("checkpoint failed: {}", e)))?;
        
        // 清理旧检查点（保留最近 5 个）
        self.cleanup_old_checkpoints(5)?;
        
        Ok(checkpoint_dir)
    }
    
    /// 从 Checkpoint 恢复
    pub fn restore_from_checkpoint(&self, checkpoint_dir: &Path) -> Result<()> {
        // 1. 停止 Raft 成员
        // 2. 关闭 RocksDB
        // 3. 清空当前 db 目录
        // 4. 从检查点复制
        // 5. 重新打开 RocksDB
        // 6. 重新启动 Raft
        Ok(())
    }
    
    fn should_create_checkpoint(&self) -> bool {
        let now = Utc::now();
        let time_elapsed = (now - self.last_checkpoint_at).num_seconds() > 300;
        let write_count_exceeded = self.writes_since_last_checkpoint >= 5000;
        time_elapsed || write_count_exceeded
    }
}
```

#### 3.2 快照触发策略

| 触发方式 | 间隔 | 说明 |
|----------|------|------|
| 定时触发 | 每 5 分钟 | 保证最大丢失窗口 |
| 写入量触发 | 每 5000 次写操作 | 高负载时更频繁 |
| Raft 触发 | Raft Snapshot Threshold | 与 Raft 快照协同 |

---

### 4. L3: 远程备份实现

#### 4.1 远程备份目标

```
S3 Bucket: powerfs-filer-backups/
  └── shard_<id>/
      ├── checkpoint_20260729_120000.tar.zst   (最新)
      ├── checkpoint_20260729_115500.tar.zst
      ├── ...
      └── checkpoint_20260728_120000.tar.zst   (30 天前)
```

#### 4.2 备份策略

```rust
pub struct FilerBackupConfig {
    pub remote_backup_enabled: bool,
    pub s3_endpoint: String,
    pub s3_bucket: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub sync_interval_seconds: u64,      // 默认 900 (15 分钟)
    pub retention_days: u32,             // 默认 30 天
    pub max_checkpoints_per_shard: u32,  // 默认 5 (本地保留)
}

impl MetaShardManager {
    /// 触发远程备份（L2 完成后异步调用）
    async fn sync_checkpoint_to_remote(
        &self, 
        shard_id: ShardId, 
        checkpoint_dir: &Path
    ) -> Result<()> {
        let key = format!(
            "shard_{}/checkpoint_{}.tar.zst",
            shard_id.0,
            chrono::Utc::now().format("%Y%m%d_%H%M%S")
        );
        
        // 压缩并上传
        let compressed = compress_to_zstd(checkpoint_dir)?;
        self.s3_client.upload(&self.config.s3_bucket, &key, &compressed).await?;
        
        // 清理过期备份（保留 30 天）
        self.cleanup_expired_backups(shard_id, self.config.retention_days).await?;
        
        Ok(())
    }
    
    /// 从远程备份恢复
    async fn restore_from_remote(
        &self, 
        shard_id: ShardId,
        backup_key: &str
    ) -> Result<PathBuf> {
        let local_path = self.config.backup_dir.join(format!(
            "remote_restore_shard_{}_{}",
            shard_id.0,
            chrono::Utc::now().format("%Y%m%d_%H%M%S")
        ));
        
        // 从 S3 下载并解压
        let data = self.s3_client.download(&self.config.s3_bucket, backup_key).await?;
        decompress_from_zstd(&data, &local_path)?;
        
        Ok(local_path)
    }
}
```

#### 4.3 保留策略

| 位置 | 保留数量 | 保留时长 | 说明 |
|------|----------|----------|------|
| 本地 Checkpoint | 5 个 | ~25 分钟 | L2 层快速恢复 |
| S3 远程备份 | 288 个 | 30 天 | L3 层长期保存 |
| Raft 日志 | 自动滚动 | 跟随快照 | Raft 内置管理 |

---

### 5. 全集群灾难恢复流程

#### 5.1 恢复顺序

```
┌─────────────────────────────────────────────────────────────────────┐
│ 全集群灾难恢复（从完全丢失到可用）                                    │
│                                                                     │
│ Step 1: 恢复 Master 路由表                                          │
│ ├── 来源: Master 自身的 RocksDB + Raft 副本                          │
│ ├── 目标: Volume 路由表、Filer Shard 路由表                          │
│ └── 验证: 所有 Volume 和 Filer 节点注册完成                          │
│                                                                     │
│ Step 2: 恢复 Volume 索引（并行，各 Volume 独立）                     │
│ ├── 优先: L1 WAL replay (自动)                                      │
│ ├── 其次: L2 Checkpoint 恢复                                        │
│ ├── 兜底: L4 volume.data 扫描重建                                   │
│ └── 验证: 每个 Volume 的 Needle 可正常读取                          │
│                                                                     │
│ Step 3: 恢复 Filer 元数据（并行，各 Shard 独立）                     │
│ ├── 优先: L1 Raft 副本同步 (如果还有存活节点)                        │
│ ├── 其次: L2 Checkpoint 恢复                                        │
│ ├── 兜底: L3 远程备份恢复                                           │
│ └── 验证: 每个 Inode 可正常 lookup、readdir                         │
│                                                                     │
│ Step 4: 验证端到端功能                                              │
│ ├── 抽查: statfs, lookup, read, write                               │
│ ├── 全量: 运行 e2e 测试套件                                         │
│ └── 完成: 通知管理员，标记集群为 Degraded 模式                       │
└─────────────────────────────────────────────────────────────────────┘
```

#### 5.2 恢复时间估算

| 步骤 | 数据量 | RTO 估算 | 依赖 |
|------|--------|----------|------|
| Master 恢复 | < 1GB | < 1min | 无 |
| Volume 索引恢复 | 100GB~10TB | 10min~2h | Master 可用 |
| Filer 元数据恢复 | 10GB~1TB | 5min~30min | Volume 可用 |
| 端到端验证 | — | 30min~2h | 以上全部 |
| **总计** | — | **~1~4 小时** | — |

#### 5.3 恢复后处理

1. **标记 Degraded 模式**：集群恢复后进入 Degraded 模式，限制高危操作
2. **触发全量校验**：后台校验 Filer 元数据与 Volume 数据的一致性
3. **重建 Raft 副本**：确保每个 Shard 有 3 个副本
4. **记录恢复报告**：生成详细的恢复报告（恢复时间、数据量、校验结果）
5. **人工确认**：等待管理员确认后解除 Degraded 模式

---

### 6. Filer 与 Volume 恢复的协调

#### 6.1 数据一致性校验

恢复完成后，需要校验 Filer 元数据与 Volume 数据的一致性：

```rust
pub struct ConsistencyReport {
    pub total_inodes: u64,
    pub verified_inodes: u64,
    pub missing_volume_data: Vec<(u64, String)>,   // (inode, reason)
    pub orphan_volume_data: Vec<(u64, u64)>,       // (volume_id, needle_id)
    pub size_mismatches: Vec<(u64, u64, u64)>,     // (inode, expected, actual)
    pub checksum_errors: Vec<(u64, u64)>,          // (volume_id, needle_id)
}

impl ConsistencyChecker {
    pub async fn full_check(&self) -> Result<ConsistencyReport> {
        let mut report = ConsistencyReport::default();
        
        // 1. 遍历所有 Filer Inode
        for inode in self.filer.list_all_inodes() {
            report.total_inodes += 1;
            
            // 2. 获取 Inode 的 chunks (volume_id + needle_id)
            let chunks = self.filer.get_chunks(inode)?;
            
            // 3. 验证每个 chunk 在 Volume 中可读取
            for chunk in &chunks {
                match self.volume.read_needle(chunk.volume_id, chunk.needle_id) {
                    Ok(data) => {
                        if data.len() != chunk.expected_size as usize {
                            report.size_mismatches.push((
                                inode, 
                                chunk.expected_size, 
                                data.len() as u64
                            ));
                        }
                        report.verified_inodes += 1;
                    }
                    Err(e) => {
                        report.missing_volume_data.push((inode, e.to_string()));
                    }
                }
            }
        }
        
        // 4. 检查 Volume 中是否有孤立数据（无对应 Filer Inode）
        for volume in self.volumes {
            for needle in volume.list_all_needles() {
                if !self.filer.needle_exists(needle.id) {
                    report.orphan_volume_data.push((volume.id, needle.id));
                }
            }
        }
        
        Ok(report)
    }
}
```

#### 6.2 不一致修复

| 不一致类型 | 修复方式 |
|------------|----------|
| Volume 数据丢失 | 标记 Inode 为只读，等待数据恢复 |
| Filer Inode 丢失 | 从 Volume 数据重建 Inode（手动操作） |
| Size 不匹配 | 以 Volume 为准更新 Filer 元数据 |
| Orphan Volume 数据 | 标记为待清理，后台 Compact 清理 |

---

### 7. 配置项新增

```toml
# powerfs.toml 新增

[filer_backup]
# 远程备份开关
remote_backup_enabled = true
# S3 配置
s3_endpoint = "http://s3-backup:9000"
s3_bucket = "powerfs-filer-backups"
s3_access_key = "minioadmin"
s3_secret_key = "minioadmin"
# 备份策略
checkpoint_interval_seconds = 300    # L2: Checkpoint 间隔
checkpoint_write_threshold = 5000    # L2: 写入量阈值
remote_sync_interval_seconds = 900   # L3: 远程同步间隔
retention_days = 30                  # 保留天数
local_checkpoint_count = 5           # 本地保留数量
# 恢复后自动执行全量校验
auto_consistency_check = true
```

---

### 8. 实施计划更新

| 阶段 | 任务 | 说明 | 预估时间 |
|------|------|------|----------|
| **Phase 3** | Filer L2 Checkpoint | 扩展 ShardStore，添加 Checkpoint API | 6h |
| **Phase 3** | Filer L3 远程备份 | S3 同步 + 恢复实现 | 4h |
| **Phase 3** | 一致性校验工具 | ConsistencyChecker 实现 | 6h |
| **Phase 3** | 恢复流程文档化 | 全集群灾难恢复 Runbook | 2h |
| **Phase 4** | 自动化恢复演练 | 定期模拟故障恢复测试 | 4h |
