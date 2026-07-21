# PowerFS 模块化改造方案

## 一、背景与目标

### 1.1 现状分析

当前PowerFS系统存在以下问题：

| 问题 | 描述 |
|------|------|
| **底座功能混用** | Master同时服务FUSE目录树、KV会话、S3 Entry，职责不清晰 |
| **Redis角色混乱** | 同时用于元数据、事件、限流、指标，单点故障影响范围大 |
| **部署耦合** | 部署FUSE必须启动完整集群，无法按需部署 |
| **接口未抽象** | 各模块直接依赖具体实现，难以替换或扩展 |

### 1.2 改造目标

- **模块化**：将底座功能抽象为Provider接口，实现解耦
- **独立部署**：支持多种部署组合（FUSE-only、FUSE+KV、FUSE+S3、全功能）
- **可替换性**：通过trait接口支持不同实现（如Redis事件 → gRPC streaming）
- **渐进式改造**：保持向后兼容，逐步迁移

## 二、架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              应用模块                                    │
│                                                                         │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐               │
│  │    FUSE     │     │     KV      │     │     S3      │               │
│  │             │     │             │     │             │               │
│  │ VolumeProvider │  │ VolumeProvider │  │ VolumeProvider │            │
│  │ MetadataProvider│ │ KvCacheProvider│ │ MetadataProvider│           │
│  │ StorageProvider │ │ StorageProvider │ │ StorageProvider │           │
│  └─────────────┘     └─────────────┘     └─────────────┘               │
│                                                                         │
│  ┌─────────────┐                                                        │
│  │   Monitor   │                                                        │
│  │             │                                                        │
│  │ EventProvider │                                                       │
│  └─────────────┘                                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           底座层接口                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐  │
│  │VolumeProvider   │  │MetadataProvider │  │EventProvider            │  │
│  │                 │  │                 │  │                         │  │
│  │ - assign_volume │  │ - get_entry     │  │ - publish_event         │  │
│  │ - lookup_volume │  │ - create_entry  │  │ - subscribe_events      │  │
│  │ - heartbeat     │  │ - update_entry  │  │ - read_history          │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────┘  │
│                                                                          │
│  ┌─────────────────┐  ┌─────────────────┐                               │
│  │KvCacheProvider  │  │StorageProvider  │                               │
│  │                 │  │                 │                               │
│  │ - put_block     │  │ - write_blob    │                               │
│  │ - get_block     │  │ - read_blob     │                               │
│  └─────────────────┘  └─────────────────┘                               │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           接口实现                                       │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                     Master Raft Group (3节点)                       ││
│  │  ┌───────────────────────────────────────────────────────────────┐  ││
│  │  │ VolumeProvider 实现 ─→ MasterNode + VolumeClientPool          │  ││
│  │  │ MetadataProvider 实现 ─→ DirectoryTree + MetadataManager      │  ││
│  │  │ KvCacheProvider 实现 ─→ KVCacheEngine                         │  ││
│  │  └───────────────────────────────────────────────────────────────┘  ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                    Volume Server (N节点)                            ││
│  │  ┌───────────────────────────────────────────────────────────────┐  ││
│  │  │ StorageProvider 实现 ─→ StorageManager + Blob存储              │  ││
│  │  └───────────────────────────────────────────────────────────────┘  ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                    Filer Shard Raft Groups (4分片)                   ││
│  │  ┌───────────────────────────────────────────────────────────────┐  ││
│  │  │ MetadataProvider 实现 (S3) ─→ MetaShardManager               │  ││
│  │  └───────────────────────────────────────────────────────────────┘  ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                    EventProvider 实现 (可选)                         ││
│  │  ┌─────────────────┐  ┌─────────────────────────────────────────┐  ││
│  │  │ Redis Streams   │  │ tokio broadcast + gRPC streaming        │  ││
│  │  └─────────────────┘  └─────────────────────────────────────────┘  ││
│  └─────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Provider接口定义

#### VolumeProvider

负责Volume分配、路由和节点管理。

| 方法 | 功能 | 参数 | 返回值 |
|------|------|------|--------|
| `assign_volume` | 分配新Volume | `collection: &str`, `replication: &str` | `Result<(Fid, Vec<Location>)>` |
| `lookup_volume` | 查找Volume位置 | `volume_id: VolumeId` | `Result<Vec<Location>>` |
| `heartbeat` | 节点心跳上报 | `node_id: &NodeId`, `stats: &NodeStats` | `Result<()>` |
| `list_volumes` | 列出所有Volume | `filters: &VolumeFilters` | `Result<Vec<VolumeInfo>>` |

#### MetadataProvider

负责文件/目录元数据的CRUD操作。

| 方法 | 功能 | 参数 | 返回值 |
|------|------|------|--------|
| `get_entry` | 获取Entry | `path: &str` | `Result<Option<Entry>>` |
| `get_entry_by_inode` | 通过inode获取Entry | `inode: u64` | `Result<Option<(Entry, String)>>` |
| `create_entry` | 创建Entry | `entry: &Entry`, `client_id: &str` | `Result<u64>` |
| `update_entry` | 更新Entry | `entry: &Entry`, `client_id: &str`, `old_size: u64`, `is_truncate: bool` | `Result<u64>` |
| `delete_entry` | 删除Entry | `inode: u64`, `is_dir: bool`, `client_id: &str` | `Result<()>` |
| `list_entries` | 列出目录Entry | `inode: u64`, `limit: u32`, `client_id: &str` | `Result<Vec<Entry>>` |

#### KvCacheProvider

负责KV缓存的块管理。

| 方法 | 功能 | 参数 | 返回值 |
|------|------|------|--------|
| `put_block` | 存储Block | `session_id: &str`, `block_id: u64`, `data: &[u8]` | `Result<()>` |
| `get_block` | 获取Block | `session_id: &str`, `block_id: u64` | `Result<Option<Vec<u8>>>` |
| `list_sessions` | 列出会话 | 无 | `Result<Vec<SessionInfo>>` |
| `evict_session` | 驱逐会话 | `session_id: &str` | `Result<()>` |
| `get_session_stats` | 获取会话统计 | `session_id: &str` | `Result<Option<SessionStats>>` |

#### EventProvider

负责系统事件的发布和订阅。

| 方法 | 功能 | 参数 | 返回值 |
|------|------|------|--------|
| `publish` | 发布事件 | `event: Event`, `source_id: &str` | `Result<()>` |
| `subscribe` | 订阅事件流 | `stream_key: &str` | `Result<EventStream>` |
| `read_history` | 读取历史事件 | `stream_key: &str`, `start: &str`, `count: usize` | `Result<Vec<EventEnvelope>>` |

#### StorageProvider

负责底层数据的读写。

| 方法 | 功能 | 参数 | 返回值 |
|------|------|------|--------|
| `write_blob` | 写入Blob | `volume_id: u32`, `file_key: u64`, `offset: i64`, `size: i32`, `data: &[u8]` | `Result<()>` |
| `batch_write_blob` | 批量写入Blob | `volume_id: u32`, `file_key: u64`, `entries: &[(i64, i32, Vec<u8>, u32)]` | `Result<()>` |
| `read_blob` | 读取Blob | `volume_id: u32`, `file_key: u64`, `offset: i64`, `size: i32` | `Result<Vec<u8>>` |
| `delete_blob` | 删除Blob | `volume_id: u32`, `file_key: u64` | `Result<()>` |

### 2.3 模块依赖关系

| 应用模块 | 依赖Provider | 当前实现 |
|---------|-------------|---------|
| FUSE | VolumeProvider + MetadataProvider + StorageProvider | Master Raft + Volume Server |
| KV | VolumeProvider + KvCacheProvider + StorageProvider | Master Raft + Volume Server |
| S3 | VolumeProvider + MetadataProvider + StorageProvider | Master Raft + Filer Raft + Volume Server |
| Monitor | EventProvider | Redis (可选) |

### 2.4 部署组合方案

#### 方案A：仅FUSE（最简）

```bash
# Master
powerfs master --port 9333 --raft-id 1 --peer ...

# Volume
powerfs-volume --grpc-address x.x.x.x:8080 --master-address ...

# FUSE
powerfs-fuse --master x.x.x.x:9333 --mount-point /mnt/fs
```

**组件**：Master Raft + Volume Server

#### 方案B：FUSE + KV

```bash
# Master (含KV服务)
powerfs master --port 9333 --enable-kv

# Volume
powerfs-volume --grpc-address x.x.x.x:8080 --master-address ...

# FUSE + KV客户端
powerfs-fuse --master x.x.x.x:9333 --mount-point /mnt/fs
powerfs-kv-client --master x.x.x.x:9333 --port 6380
```

**组件**：Master Raft (KvCacheProvider) + Volume Server

#### 方案C：FUSE + S3

```bash
# Master
powerfs master --port 9333 --raft-id 1 --peer ...

# Volume
powerfs-volume --grpc-address x.x.x.x:8080 --master-address ...

# Filer (S3元数据)
powerfs filer --port 8888 --grpc-port 8889 --master ... --shard-count 4

# FUSE + S3客户端
powerfs-fuse --master x.x.x.x:9333 --mount-point /mnt/fs
```

**组件**：Master Raft + Volume Server + Filer Shard Raft

#### 方案D：全功能

```bash
# Master (含KV)
powerfs master --port 9333 --raft-id 1 --enable-kv

# Volume
powerfs-volume --grpc-address x.x.x.x:8080 --master-address ...

# Filer
powerfs filer --port 8888 --grpc-port 8889 --master ... --shard-count 4

# Monitor (含事件)
powerfs-monitor --port 8081 --redis-url ...

# FUSE + KV + S3
powerfs-fuse --master x.x.x.x:9333 --mount-point /mnt/fs
powerfs-kv-client --master x.x.x.x:9333 --port 6380
```

**组件**：Master Raft + Volume Server + Filer Raft + Monitor + Redis (可选)

## 三、实施计划

### 3.1 阶段一：接口定义（已完成）

**目标**：定义所有Provider trait接口

| 任务 | 文件 | 状态 |
|------|------|------|
| 创建 `powerfs-common/src/traits.rs` | 定义VolumeProvider | ✅ 已完成 |
| 创建 `powerfs-common/src/traits.rs` | 定义MetadataProvider | ✅ 已完成 |
| 创建 `powerfs-common/src/traits.rs` | 定义KvCacheProvider | ✅ 已完成 |
| 创建 `powerfs-common/src/traits.rs` | 定义EventProvider | ✅ 已完成 |
| 创建 `powerfs-common/src/traits.rs` | 定义StorageProvider | ✅ 已完成 |
| 更新 `powerfs-common/Cargo.toml` | 添加async-trait依赖 | ✅ 已完成 |

### 3.2 阶段二：接口实现（已完成）

**目标**：在现有组件中实现Provider接口

| 任务 | 文件 | 状态 |
|------|------|------|
| 实现MasterNode的VolumeProvider | `powerfs-master/src/provider_impl.rs` | ✅ 已完成 |
| 实现DirectoryTree的MetadataProvider | `powerfs-master/src/provider_impl.rs` | ✅ 已完成 |
| 实现KVCacheEngine的KvCacheProvider | `powerfs-master/src/provider_impl.rs` | ✅ 已完成 |
| 实现MetaShardManager的MetadataProvider | `powerfs-filer/src/provider_impl.rs` | ✅ 已完成 |
| 实现StorageManager的StorageProvider | `powerfs-core/src/provider_impl.rs` | ✅ 已完成 |
| 实现RedisEventProvider和NullEventProvider | `powerfs-common/src/event.rs` | ✅ 已完成 |

### 3.3 阶段三：应用层迁移（已完成）

**目标**：应用层改为通过Provider接口访问底座

| 任务 | 文件 | 状态 |
|------|------|------|
| 创建Provider适配器 | `powerfs-fuse-core/src/provider_adapter.rs` | ✅ 已完成 |
| FUSE迁移到Provider接口 | `powerfs-fuse/src/fuse.rs` | ✅ 已完成 |
| KV迁移到Provider接口 | `powerfs-kv-client/src/client.rs` | ✅ 已完成 |
| S3迁移到Provider接口 | `powerfs-filer/src/s3_handler.rs` | ✅ 已完成 |
| Monitor迁移到EventProvider | `powerfs-monitor/src/event_bus.rs` | ✅ 已完成 |

### 3.4 阶段四：Feature Flags（已完成）

**目标**：添加Cargo feature flags支持按需编译

| 任务 | 文件 | 状态 |
|------|------|------|
| 添加feature flags到powerfs-server | `powerfs-server/Cargo.toml` | ✅ 已完成 |
| 添加feature flags到powerfs-common | `powerfs-common/Cargo.toml` | ✅ 已完成 |
| 添加feature flags到powerfs-master | `powerfs-master/Cargo.toml` | ✅ 已完成 |
| 添加feature flags到powerfs-filer | `powerfs-filer/Cargo.toml` | ✅ 已完成 |
| 添加feature flags到powerfs-monitor | `powerfs-monitor/Cargo.toml` | ✅ 已完成 |
| 修改main.rs支持条件编译 | `powerfs-server/src/main.rs` | ✅ 已完成 |

### 3.5 阶段五：Redis可选化（已完成）

**目标**：将Redis改为可选依赖

| 任务 | 文件 | 状态 |
|------|------|------|
| Redis改为可选feature | `powerfs-common/Cargo.toml` | ✅ 已完成 |
| EventPublisher条件编译 | `powerfs-common/src/event.rs` | ✅ 已完成 |
| RateLimiter改为内存实现 | `powerfs-monitor/src/auth/rate_limiter.rs` | ✅ 已完成 |
| EventBus条件编译 | `powerfs-monitor/src/event_bus.rs` | ✅ 已完成 |
| MetadataStore条件编译 | `powerfs-filer/src/metadata_store.rs` | ✅ 已完成 |
| 移除Filer的Redis依赖 | `powerfs-filer/src/metadata_store.rs` | 待开发 |

## 四、代码规范

### 4.1 Trait命名规范

```rust
/// Provider trait命名：[功能]Provider
pub trait VolumeProvider: Send + Sync {
    // 方法命名：动词开头，小写蛇形
    async fn assign_volume(&self, collection: &str, replication: &str) -> Result<(Fid, Vec<Location>)>;
}
```

### 4.2 错误处理

所有Provider方法返回`Result<T>`，使用`powerfs_common::error::PowerFsError`。

```rust
use powerfs_common::error::{PowerFsError, Result};

async fn get_entry(&self, path: &str) -> Result<Option<Entry>> {
    // ...
    Ok(Some(entry))
}
```

### 4.3 实现类命名

实现类命名为`{组件}Provider`或直接在组件结构体上实现trait。

```rust
// 方式1：在现有结构体上实现
impl VolumeProvider for MasterNode {
    async fn assign_volume(&self, collection: &str, replication: &str) -> Result<(Fid, Vec<Location>)> {
        // 使用现有的assign_fid逻辑
    }
}

// 方式2：创建专门的实现类
pub struct MasterVolumeProvider {
    master_node: Arc<MasterNode>,
}

impl VolumeProvider for MasterVolumeProvider {
    // ...
}
```

## 五、风险评估

| 风险 | 等级 | 应对措施 |
|------|------|---------|
| 接口变更影响现有功能 | 高 | 渐进式迁移，保持向后兼容 |
| 性能影响（trait调度） | 中 | 使用静态分发（泛型参数），避免虚函数开销 |
| Redis移除后监控功能缺失 | 低 | 提供替代实现（tokio broadcast） |
| 部署复杂度增加 | 中 | 提供docker-compose模板覆盖各部署方案 |

## 六、验证标准

### 6.1 编译验证

```bash
# 仅FUSE
cargo build --no-default-features --features fuse

# FUSE + KV
cargo build --no-default-features --features fuse,kv

# 全功能
cargo build --features default

# 全功能 + Redis
cargo build --features default,redis-event
```

### 6.2 测试验证

| 测试场景 | 预期结果 |
|---------|---------|
| FUSE-only部署 | 可正常挂载、读写文件 |
| FUSE+KV部署 | FUSE正常 + KV服务正常 |
| FUSE+S3部署 | FUSE正常 + S3 API正常 |
| 无Redis环境 | 系统正常运行，事件降级为内存实现 |

### 6.3 兼容性验证

- 现有配置文件兼容
- 现有API接口兼容
- 现有部署方式兼容

## 七、附录

### 7.1 Cargo Feature Flags设计

```toml
# powerfs-server/Cargo.toml
[features]
default = ["fuse", "kv", "s3", "monitor"]

# 应用模块
fuse = []
kv = []
s3 = ["powerfs-filer"]
monitor = []

# 可选后端
redis-event = ["powerfs-common/redis-event"]
redis-metrics = ["powerfs-common/redis-metrics"]
```

### 7.2 接口实现优先级

1. **VolumeProvider** - FUSE、KV、S3都依赖
2. **MetadataProvider** - FUSE、S3依赖
3. **StorageProvider** - FUSE、KV、S3都依赖
4. **KvCacheProvider** - KV依赖
5. **EventProvider** - Monitor依赖（可延迟）

### 7.3 参考资料

- [Rust Trait对象指南](https://doc.rust-lang.org/book/ch17-02-trait-objects.html)
- [async-trait crate](https://crates.io/crates/async-trait)
- [Cargo Feature文档](https://doc.rust-lang.org/cargo/reference/features.html)