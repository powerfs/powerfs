# PowerFS 分片优化方案设计

## 1. 设计背景

### 1.1 当前架构问题

| 问题 | 描述 | 影响 |
|------|------|------|
| **Master元数据瓶颈** | 所有inode/dentry存储在单一Master Raft组 | 写入吞吐受限，无法扩展 |
| **Filer无分片能力** | Filer仅做协议转换和缓存，元数据仍依赖Master | 无法发挥水平扩展能力 |
| **跨分片操作复杂** | 纯哈希分片导致大量跨分片操作 | 性能下降，一致性难以保证 |

### 1.2 核心设计目标

1. **高性能**：客户端直连元数据分片，最低延迟
2. **可扩展**：Multi-Raft分片，无限水平扩展
3. **简单可靠**：复用现有Filer基础设施，降低开发复杂度
4. **向后兼容**：平滑迁移，不影响现有接口

---

## 2. 架构设计

### 2.1 整体架构

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           双层Raft + Filer分片架构                           │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                         控制面 (Master)                                │  │
│  │                                                                        │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐                             │  │
│  │  │ Master-1 │  │ Master-2 │  │ Master-3 │                             │  │
│  │  │  (Leader)│  │(Follower)│  │(Follower)│                             │  │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘                             │  │
│  │       │             │             │                                    │  │
│  │       └───────┬─────┴─────┬───────┘                                    │  │
│  │               ▼           ▼                                            │  │
│  │       ┌───────────────┐                                               │  │
│  │       │   Raft Group  │  ◄── 单套全局Raft                             │  │
│  │       │  (控制面数据)  │  ◄── 节点注册/分片路由/配置/调度               │  │
│  │       └───────────────┘                                               │  │
│  │                           │                                            │  │
│  │                           ▼                                            │  │
│  │                 ┌─────────────────┐                                   │  │
│  │                 │   路由推送流     │  gRPC Stream                      │  │
│  │                 └────────┬────────┘                                   │  │
│  └──────────────────────────┼───────────────────────────────────────────┘  │
│                             │                                              │
│      ┌──────────────────────┼──────────────────────┐                       │
│      │                      │                      │                       │
│      ▼                      ▼                      ▼                       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        数据面 (Filer + MetaNode)                      │  │
│  │                                                                      │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │  │
│  │  │                        Filer-1                                 │  │  │
│  │  │  ┌───────────────────────────────────────────────────────────┐  │  │  │
│  │  │  │  S3协议处理层                                             │  │  │  │
│  │  │  │  - PutObject / GetObject / DeleteObject                  │  │  │  │
│  │  │  │  - ListBuckets / ListObjects                             │  │  │  │
│  │  │  └──────────────────┬───────────────────────────────────────┘  │  │  │
│  │  │                     │                                         │  │  │
│  │  │  ┌──────────────────▼───────────────────────────────────────┐  │  │  │
│  │  │  │              分片Raft管理层                              │  │  │  │
│  │  │  │                                                         │  │  │  │
│  │  │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │  │  │  │
│  │  │  │  │ Shard-0     │  │ Shard-2     │  │ Shard-4     │      │  │  │  │
│  │  │  │  │ (Leader)    │  │ (Leader)    │  │ (Follower)  │      │  │  │  │
│  │  │  │  │ inode: 0-1M │  │ inode: 2-3M │  │ inode: 4-5M │      │  │  │  │
│  │  │  │  └─────────────┘  └─────────────┘  └─────────────┘      │  │  │  │
│  │  │  │                                                         │  │  │  │
│  │  │  │  ┌───────────────────────────────────────────────────┐  │  │  │  │
│  │  │  │  │           RaftGroupManager                        │  │  │  │  │
│  │  │  │  │  - 管理多个独立Raft组                             │  │  │  │  │
│  │  │  │  │  - 每个分片一个RawNode                            │  │  │  │  │
│  │  │  │  │  - 共享RocksDB存储引擎                           │  │  │  │  │
│  │  │  │  └───────────────────────────────────────────────────┘  │  │  │  │
│  │  │  └───────────────────────────────────────────────────────────┘  │  │  │
│  │  └───────────────────────────────────────────────────────────────────┘  │  │
│  │                                        │                                │  │
│  │  ┌─────────────────────────────────────┼─────────────────────────────┐  │  │
│  │  │                                     │                             │  │  │
│  │  ▼                                     ▼                             ▼  │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │  │
│  │  │                        Filer-2                                 │  │  │
│  │  │  ┌───────────────────────────────────────────────────────────┐  │  │  │
│  │  │  │  S3协议处理层                                             │  │  │  │
│  │  │  └──────────────────┬───────────────────────────────────────┘  │  │  │
│  │  │                     │                                         │  │  │
│  │  │  ┌──────────────────▼───────────────────────────────────────┐  │  │  │
│  │  │  │              分片Raft管理层                              │  │  │  │
│  │  │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │  │  │  │
│  │  │  │  │ Shard-0     │  │ Shard-1     │  │ Shard-3     │      │  │  │  │
│  │  │  │  │ (Follower)  │  │ (Leader)    │  │ (Leader)    │      │  │  │  │
│  │  │  │  │ inode: 0-1M │  │ inode: 1-2M │  │ inode: 3-4M │      │  │  │  │
│  │  │  │  └─────────────┘  └─────────────┘  └─────────────┘      │  │  │  │
│  │  │  └───────────────────────────────────────────────────────────┘  │  │  │
│  │  └───────────────────────────────────────────────────────────────────┘  │  │
│  │                                        │                                │  │
│  │  ┌─────────────────────────────────────┴─────────────────────────────┐  │  │
│  │  │                                                                 │  │  │
│  │  ▼                                                                 ▼  │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │  │
│  │  │                        Filer-N                                 │  │  │
│  │  │  ┌───────────────────────────────────────────────────────────┐  │  │  │
│  │  │  │  ...                                                      │  │  │  │
│  │  │  └───────────────────────────────────────────────────────────┘  │  │  │
│  │  └───────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                      │  │
│  │  ◄── Filer = S3协议入口 + 元数据分片承载者                            │  │
│  │  ◄── 每个Filer承载多个分片Raft组（Leader或Follower）                   │  │
│  │  ◄── 分片按目录范围切分，父子目录尽量同分片                            │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                             │                                              │
│      ┌──────────────────────┼──────────────────────┐                       │
│      │                      │                      │                       │
│      ▼                      ▼                      ▼                       │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐                │
│  │ FUSE Client │      │  S3 Client  │      │  CLI Client │                │
│  │ (直连模式)   │      │  (直连模式)   │      │  (直连模式)   │                │
│  │ 内嵌路由表   │      │ 内嵌路由表   │      │ 内嵌路由表   │                │
│  └─────────────┘      └─────────────┘      └─────────────┘                │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心设计决策

| 决策 | 方案 | 理由 |
|------|------|------|
| **MetaNode形态** | Filer扩展模块 | 复用Filer已有HTTP/gRPC/RocksDB基础设施 |
| **分片策略** | 目录范围分片 | 99%操作天然单分片，跨分片操作极少 |
| **跨分片事务** | 乐观2PC | 带版本号的两阶段提交，比双写补偿更可靠 |
| **分片数量** | `node_count * 16` | 每节点承载16个分片，后续自动分裂 |
| **路由分发** | Master推送 + 客户端拉取 | 实时更新 + 兜底刷新 |

---

## 3. 分片策略设计

### 3.1 目录范围分片

**核心思想**：按目录树层级切分，父子目录尽量落在同一片

```
目录树结构：
/
├── home/                ──► Shard-0 (inode: 0-1M)
│   ├── user1/           ──► Shard-0
│   │   ├── docs/        ──► Shard-0
│   │   └── photos/      ──► Shard-0
│   └── user2/           ──► Shard-0
│       └── videos/      ──► Shard-0
├── data/                ──► Shard-1 (inode: 1M-2M)
│   ├── logs/            ──► Shard-1
│   └── backup/          ──► Shard-1
└── tmp/                 ──► Shard-2 (inode: 2M-3M)
```

**分片键计算**：

```rust
// 分片键 = 目录inode前缀
// 顶级目录inode直接决定分片
fn calculate_shard(inode: u64) -> ShardId {
    // 获取父目录inode（如果是文件）
    let parent_inode = get_parent_inode(inode);
    
    // 分片键 = 父目录inode的高位部分
    let shard_key = parent_inode >> 24; // 取高8位作为分片键
    
    // 根据分片键映射到分片ID
    ShardId(shard_key % SHARD_COUNT)
}
```

**优势**：

| 场景 | 说明 |
|------|------|
| **创建文件** | 同目录下的文件天然同分片 |
| **删除文件** | 同目录下的删除天然同分片 |
| **列出目录** | 目录下所有文件同分片，一次查询完成 |
| **重命名** | 同目录重命名单分片，跨目录重命名才需要跨分片 |

### 3.2 分片分裂策略

**分裂触发条件**：

| 条件 | 阈值 | 说明 |
|------|------|------|
| inode数量 | 100万 | 分片内inode数量超过阈值 |
| Raft日志大小 | 1GB | Raft日志过大影响快照和恢复 |
| 写入热点 | 单分片QPS > 总QPS/分片数 * 2 | 热点分片自动分裂 |

**分裂算法**：

```rust
pub async fn split_shard(shard_id: ShardId) -> Result<(ShardId, ShardId)> {
    // 1. 获取分片当前信息
    let shard_info = master_client.get_shard_info(shard_id).await?;
    let (start_inode, end_inode) = shard_info.inode_range;
    
    // 2. 计算分裂点：选择最均衡的目录边界
    let split_point = find_best_split_point(shard_id, start_inode, end_inode).await?;
    
    // 3. 创建新分片Raft组
    let new_shard_id = master_client.create_shard(
        split_point, 
        end_inode,
        shard_info.replicas.clone()
    ).await?;
    
    // 4. 复制数据到新分片（增量同步）
    copy_data_incremental(shard_id, new_shard_id, split_point).await?;
    
    // 5. 更新路由表（原子操作）
    let update = RouteTableUpdate {
        updates: vec![
            ShardRouteUpdate {
                shard_id,
                new_range: (start_inode, split_point),
            },
            ShardRouteUpdate {
                shard_id: new_shard_id,
                new_range: (split_point, end_inode),
            },
        ],
        version: shard_info.version + 1,
    };
    master_client.update_route_table(update).await?;
    
    // 6. 推送路由更新
    master_client.broadcast_route_update().await?;
    
    Ok((shard_id, new_shard_id))
}

// 查找最佳分裂点：选择目录边界
async fn find_best_split_point(
    shard_id: ShardId,
    start_inode: u64,
    end_inode: u64,
) -> Result<u64> {
    // 获取分片内所有目录inode
    let directories = get_directories_in_shard(shard_id).await?;
    
    // 选择中间位置的目录作为分裂点
    let mid_index = directories.len() / 2;
    Ok(directories[mid_index])
}
```

### 3.3 分片缺省数量

**计算公式**：

```
缺省分片数 = meta_node_count * 16

说明：
- meta_node_count：Filer节点数量（即元数据节点数量）
- 16：每节点承载的分片数（经验值，可配置）
- 最小分片数：32（避免分片过少导致负载不均）
- 最大分片数：10000（避免分片过多导致管理开销）
```

**示例**：

| Filer节点数 | 缺省分片数 | 每节点分片数 |
|------------|-----------|-------------|
| 3 | 48 | 16 |
| 5 | 80 | 16 |
| 10 | 160 | 16 |
| 50 | 800 | 16 |

**动态调整**：
- 分片数可随Filer节点增减自动调整
- 新节点加入时自动迁移部分分片
- 节点下线时自动将分片迁移到其他节点

---

## 4. 路由表设计

### 4.1 路由表数据结构

```rust
pub struct ShardRouteTable {
    // 分片路由映射：inode范围 → 分片信息
    shards: HashMap<(u64, u64), ShardInfo>,
    
    // 版本号（单调递增）
    version: u64,
    
    // 更新时间
    updated_at: chrono::DateTime<chrono::Utc>,
    
    // 分片总数
    total_shards: u64,
    
    // Filer节点列表
    nodes: Vec<NodeInfo>,
}

pub struct ShardInfo {
    // 分片ID
    shard_id: ShardId,
    
    // inode范围
    inode_range: (u64, u64),
    
    // 副本节点ID
    replicas: Vec<NodeId>,
    
    // 当前Leader节点ID
    leader_id: Option<NodeId>,
    
    // Leader地址
    leader_addr: Option<String>,
    
    // 分片状态
    state: ShardState,
    
    // 统计信息
    stats: ShardStats,
}

pub enum ShardState {
    Normal,     // 正常服务
    Splitting,  // 正在分裂
    Migrating,  // 正在迁移
    Offline,    // 离线
}

pub struct ShardStats {
    inode_count: u64,
    file_count: u64,
    dir_count: u64,
    write_qps: u64,
    read_qps: u64,
}
```

### 4.2 路由分发协议

#### 推送机制（实时更新）

```rust
// Master端：路由更新推送
pub async fn broadcast_route_update(&self, event: RouteUpdateEvent) -> Result<()> {
    self.route_update_tx.send(event)?;
    Ok(())
}

// 路由更新事件
pub struct RouteUpdateEvent {
    // 事件类型
    event_type: RouteEventType,
    
    // 变更的分片信息（增量）
    changed_shards: Vec<ShardInfo>,
    
    // 完整版本号
    version: u64,
}

pub enum RouteEventType {
    AddShard,
    RemoveShard,
    UpdateLeader,
    MigrateShard,
    SplitShard,
}

// 客户端订阅
pub async fn subscribe_route_updates(&self) -> broadcast::Receiver<RouteUpdateEvent> {
    self.master_client.subscribe_route_updates().await
}
```

#### 拉取机制（兜底刷新）

```rust
// 客户端定时刷新
async fn refresh_route_table(&self) {
    loop {
        tokio::time::sleep(self.refresh_interval).await;
        
        let new_table = self.master_client.get_route_table().await?;
        if new_table.version > self.version.load(Ordering::Relaxed) {
            *self.route_table.write().await = new_table;
            self.version.store(new_table.version, Ordering::Relaxed);
        }
    }
}
```

#### 版本控制与增量更新

```rust
// 客户端增量更新
pub async fn apply_route_update(&self, event: RouteUpdateEvent) {
    let mut table = self.route_table.write().await;
    
    // 验证版本连续性
    if event.version != table.version + 1 {
        // 版本不连续，强制全量拉取
        *table = self.master_client.get_route_table().await?;
        return;
    }
    
    // 增量更新
    for shard in event.changed_shards {
        let range = shard.inode_range;
        table.shards.insert(range, shard);
    }
    
    table.version = event.version;
}
```

---

## 5. 跨分片事务设计

### 5.1 乐观两阶段提交（Optimistic 2PC）

**适用场景**：跨目录重命名、跨目录移动文件

**流程图**：

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Client     │     │  Shard-A    │     │  Shard-B    │
│             │     │ (Source)    │     │ (Target)    │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       │  Phase 1: Prepare │                   │
       │──────────────────►│                   │
       │                   │  Lock + Write     │
       │                   │  (带版本号的      │
       │                   │   redirect/tombstone)│
       │                   │                   │
       │                   │     Prepare OK    │
       │◄──────────────────│                   │
       │                   │                   │
       │                   │  Phase 1: Prepare │
       │───────────────────────────────────────►│
       │                   │                   │
       │                   │                   │  Lock + Write
       │                   │                   │  (新条目)
       │                   │                   │
       │                   │                   │  Prepare OK
       │◄───────────────────────────────────────│
       │                   │                   │
       │  Phase 2: Commit  │                   │
       │──────────────────►│                   │
       │                   │  Commit           │
       │                   │  (删除源条目)      │
       │                   │                   │
       │                   │     Commit OK     │
       │◄──────────────────│                   │
       │                   │                   │
       │                   │  Phase 2: Commit  │
       │───────────────────────────────────────►│
       │                   │                   │
       │                   │                   │  Commit
       │                   │                   │  (确认新条目)
       │                   │                   │
       │                   │                   │  Commit OK
       │◄───────────────────────────────────────│
       │                   │                   │
       ▼                   ▼                   ▼
     完成                 完成                 完成
```

**实现细节**：

```rust
// 跨分片重命名
pub async fn rename_cross_shard(
    &self,
    old_parent_inode: u64,
    old_name: &str,
    new_parent_inode: u64,
    new_name: &str,
) -> Result<()> {
    let old_shard = self.calculate_shard(old_parent_inode);
    let new_shard = self.calculate_shard(new_parent_inode);
    
    if old_shard == new_shard {
        // 同分片，直接操作
        return self.meta_client_pool.get_client(&shard_info.leader_addr)?
            .rename(old_parent_inode, old_name, new_parent_inode, new_name)
            .await;
    }
    
    // 乐观2PC
    
    let old_shard_info = self.get_shard_info(old_shard).await?;
    let new_shard_info = self.get_shard_info(new_shard).await?;
    
    let old_client = self.meta_client_pool.get_client(&old_shard_info.leader_addr)?;
    let new_client = self.meta_client_pool.get_client(&new_shard_info.leader_addr)?;
    
    // Phase 1: Prepare
    // 获取源条目
    let entry = old_client.get_entry(old_parent_inode, old_name).await?;
    let source_version = entry.version;
    
    // 在源分片写入redirect（指向新位置）
    old_client.prepare_rename(
        old_parent_inode,
        old_name,
        new_parent_inode,
        new_name,
        source_version,
    ).await?;
    
    // 在目标分片写入新条目（带prepare标记）
    new_client.prepare_create(
        new_parent_inode,
        new_name,
        entry,
    ).await?;
    
    // Phase 2: Commit
    // 在源分片删除源条目（验证版本）
    old_client.commit_delete(
        old_parent_inode,
        old_name,
        source_version,
    ).await?;
    
    // 在目标分片确认新条目（移除prepare标记）
    new_client.commit_create(
        new_parent_inode,
        new_name,
    ).await?;
    
    Ok(())
}
```

**版本冲突处理**：

```rust
// 冲突检测与回滚
pub async fn handle_conflict(
    &self,
    shard_id: ShardId,
    operation: &str,
    version: u64,
) -> Result<()> {
    let shard_info = self.get_shard_info(shard_id).await?;
    let client = self.meta_client_pool.get_client(&shard_info.leader_addr)?;
    
    // 获取当前版本
    let current_version = client.get_version().await?;
    
    if current_version != version {
        // 版本冲突，回滚操作
        client.rollback(operation).await?;
        return Err(PowerFsError::Conflict("Version mismatch"));
    }
    
    Ok(())
}
```

### 5.2 跨分片操作场景分析

| 操作 | 场景 | 是否跨分片 | 处理方式 |
|------|------|-----------|---------|
| **创建文件** | 在目录内创建 | 否（同分片） | 直接写入 |
| **删除文件** | 删除目录内文件 | 否（同分片） | 直接删除 |
| **修改文件** | 修改已有文件 | 否（同分片） | 直接更新 |
| **列出目录** | 列出目录内容 | 否（同分片） | 一次查询 |
| **重命名（同目录）** | 同一目录内重命名 | 否（同分片） | 直接操作 |
| **重命名（跨目录）** | 不同目录间重命名 | 是 | 乐观2PC |
| **移动文件（跨目录）** | 不同目录间移动 | 是 | 乐观2PC |
| **创建硬链接** | 跨目录创建硬链接 | 是 | 乐观2PC |

---

## 6. Filer扩展设计

### 6.1 Filer内部架构改造

```
┌───────────────────────────────────────────────────────────────────────────┐
│                          Filer 扩展架构                                   │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                        HTTP/gRPC 入口层                              │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │  │
│  │  │  S3 Handler  │  │  FUSE Handler│  │  CLI Handler │             │  │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘             │  │
│  │         │                │                │                        │  │
│  └─────────┼────────────────┼────────────────┼───────────────────────┘  │
│            │                │                │                          │
│            ▼                ▼                ▼                          │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                        路由决策层                                    │  │
│  │  ┌───────────────────────────────────────────────────────────────┐  │  │
│  │  │                    MetaRouter                                │  │  │
│  │  │  - 路由表缓存                                                │  │  │
│  │  │  - 分片计算                                                  │  │  │
│  │  │  - 本地分片直接处理                                           │  │  │
│  │  │  - 远程分片路由到对应Filer                                    │  │  │
│  │  └────────────────────────────┬──────────────────────────────────┘  │  │
│  │                               │                                     │  │
│  └───────────────────────────────┼─────────────────────────────────────┘  │
│                                  │                                        │
│            ┌─────────────────────┴─────────────────────┐                 │
│            │                                           │                 │
│            ▼                                           ▼                 │
│  ┌─────────────────────┐                   ┌─────────────────────┐       │
│  │    本地分片处理      │                   │    远程分片路由      │       │
│  │                     │                   │                     │       │
│  │  ┌───────────────┐  │                   │  ┌───────────────┐  │       │
│  │  │ ShardManager  │  │                   │  │ FilerClient   │  │       │
│  │  │  - RaftGroup  │  │                   │  │  - gRPC调用   │  │       │
│  │  │    Manager    │  │                   │  │  - 负载均衡   │  │       │
│  │  │  - Directory  │  │                   │  │               │  │       │
│  │  │    Tree (分片) │  │                   │  └───────────────┘  │       │
│  │  │  - ORSet      │  │                   └─────────────────────┘       │
│  │  └───────────────┘  │                                                 │
│  └─────────────────────┘                                                 │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                        存储层                                       │  │
│  │  ┌───────────────────────────────────────────────────────────────┐  │  │
│  │  │                    RocksDB                                    │  │  │
│  │  │  - 每个分片独立Column Family                                   │  │  │
│  │  │  - Raft日志单独存储                                            │  │  │
│  │  │  - 元数据状态机存储                                             │  │  │
│  │  └───────────────────────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Filer核心组件

```rust
// powerfs-filer/src/meta_shard_manager.rs
pub struct MetaShardManager {
    // Raft组管理器
    raft_group_manager: Arc<RaftGroupManager>,
    
    // 本地分片存储
    shard_stores: HashMap<ShardId, Arc<ShardStore>>,
    
    // 路由表缓存
    route_table_cache: RouteTableCache,
    
    // Filer客户端池（用于路由到其他Filer）
    filer_client_pool: FilerClientPool,
    
    // 分片统计
    shard_stats: ShardStatsManager,
}

impl MetaShardManager {
    // 处理本地分片请求
    pub async fn handle_local_request(
        &self,
        shard_id: ShardId,
        request: MetaRequest,
    ) -> Result<MetaResponse> {
        let shard_store = self.shard_stores.get(&shard_id)
            .ok_or(PowerFsError::NotFound("Shard not found"))?;
        shard_store.handle_request(request).await
    }
    
    // 路由到远程分片
    pub async fn route_to_remote(
        &self,
        shard_id: ShardId,
        request: MetaRequest,
    ) -> Result<MetaResponse> {
        let shard_info = self.route_table_cache.get_shard_info(shard_id).await?;
        let client = self.filer_client_pool.get_client(&shard_info.leader_addr)?;
        client.handle_meta_request(request).await
    }
}
```

### 6.3 Filer请求处理流程

```
请求入口：PUT /bucket/key

1. S3 Handler解析请求
   └─► 获取bucket信息（Redis缓存）
   └─► 获取key对应的inode（路径解析）

2. MetaRouter计算分片
   └─► calculate_shard(inode) → shard_id
   └─► 查询路由表 → ShardInfo

3. 路由决策
   ├─► 本地分片（当前Filer是Leader）
   │   └─► MetaShardManager.handle_local_request()
   │       └─► RaftGroupManager.propose()
   │       └─► DirectoryTree.apply()
   │       └─► 返回响应
   │
   └─► 远程分片（其他Filer是Leader）
       └─► FilerClientPool.route()
           └─► gRPC调用远程Filer
           └─► 返回响应
```

---

## 7. 与现有组件的交互边界

### 7.1 组件职责划分

| 组件 | 职责 | 新增/修改 |
|------|------|-----------|
| **Master** | 集群拓扑、节点注册、分片路由表、分片调度、全局配置 | 修改 |
| **Filer** | S3协议转换、元数据分片承载、客户端路由入口 | 扩展 |
| **Volume** | 数据块存储、副本管理 | 不变 |
| **FUSE Client** | 文件系统挂载、内嵌路由表、直连Filer分片 | 修改 |

### 7.2 Master接口变更

```protobuf
// 新增分片管理接口
service MasterService {
    // 分片路由查询
    rpc GetRouteTable(GetRouteTableRequest) returns (GetRouteTableResponse);
    
    // 路由更新订阅（流式）
    rpc SubscribeRouteUpdates(SubscribeRequest) returns (stream RouteUpdateEvent);
    
    // 创建分片
    rpc CreateShard(CreateShardRequest) returns (CreateShardResponse);
    
    // 分裂分片
    rpc SplitShard(SplitShardRequest) returns (SplitShardResponse);
    
    // 迁移分片
    rpc MigrateShard(MigrateShardRequest) returns (MigrateShardResponse);
    
    // 查询分片信息
    rpc GetShardInfo(GetShardInfoRequest) returns (GetShardInfoResponse);
    
    // 列出所有分片
    rpc ListShards(ListShardsRequest) returns (ListShardsResponse);
}

message GetRouteTableRequest {
    // 可选：版本号，用于增量更新
    uint64 version = 1;
}

message GetRouteTableResponse {
    ShardRouteTable route_table = 1;
}

message SubscribeRequest {}

message RouteUpdateEvent {
    RouteEventType event_type = 1;
    repeated ShardInfo changed_shards = 2;
    uint64 version = 3;
}

enum RouteEventType {
    ADD_SHARD = 0;
    REMOVE_SHARD = 1;
    UPDATE_LEADER = 2;
    MIGRATE_SHARD = 3;
    SPLIT_SHARD = 4;
}
```

### 7.3 Filer接口变更

```protobuf
// 新增元数据操作接口
service FilerMetaService {
    // 元数据查询
    rpc Lookup(LookupRequest) returns (LookupResponse);
    
    // 创建文件
    rpc CreateFile(CreateFileRequest) returns (CreateFileResponse);
    
    // 更新文件
    rpc UpdateFile(UpdateFileRequest) returns (UpdateFileResponse);
    
    // 删除文件
    rpc DeleteFile(DeleteFileRequest) returns (DeleteFileResponse);
    
    // 重命名
    rpc Rename(RenameRequest) returns (RenameResponse);
    
    // 列出目录
    rpc ListDirectory(ListDirectoryRequest) returns (ListDirectoryResponse);
    
    // 跨分片事务准备
    rpc Prepare(PrepareRequest) returns (PrepareResponse);
    
    // 跨分片事务提交
    rpc Commit(CommitRequest) returns (CommitResponse);
    
    // 跨分片事务回滚
    rpc Rollback(RollbackRequest) returns (RollbackResponse);
}
```

---

## 8. 数据迁移路径

### 8.1 迁移策略：双写 + 灰度切换

```
阶段1：双写模式（同步）
┌──────────────┐     ┌──────────────┐
│  旧Master    │◄────│  新Filer分片  │
│  (DirectoryTree)│   │  (新架构)    │
└──────────────┘     └──────────────┘
       │                    │
       └────────┬───────────┘
                ▼
           双写同步

阶段2：只读模式（验证）
┌──────────────┐     ┌──────────────┐
│  旧Master    │     │  新Filer分片  │
│  (只读)      │◄────│  (读写)      │
└──────────────┘     └──────────────┘
       │                    │
       └────────┬───────────┘
                ▼
           数据对比验证

阶段3：切换完成（下线旧Master）
┌──────────────┐     ┌──────────────┐
│  旧Master    │     │  新Filer分片  │
│  (已下线)    │     │  (读写)      │
└──────────────┘     └──────────────┘
```

### 8.2 迁移步骤

| 步骤 | 任务 | 说明 |
|------|------|------|
| 1 | 部署新Filer集群 | 启动多个Filer节点，初始化分片 |
| 2 | 同步路由表 | Master向所有Filer推送初始路由表 |
| 3 | 开启双写 | 所有元数据写入同时写入旧Master和新分片 |
| 4 | 数据同步 | 后台同步旧Master的元数据到新分片 |
| 5 | 验证数据一致性 | 对比新旧数据，修复不一致 |
| 6 | 灰度切换 | 逐步将客户端切换到新架构 |
| 7 | 停止双写 | 关闭旧Master写入 |
| 8 | 下线旧Master | 停止旧Master服务 |

### 8.3 回滚方案

```
如果新架构出现问题，可快速回滚：

1. 停止新分片写入
2. 切换客户端回到旧Master
3. 停止双写同步
4. 恢复旧Master为读写模式
```

---

## 9. 实施路线图

### Phase 1：Filer分片核心框架（3-4周）

| 步骤 | 任务 | 关键文件 |
|------|------|----------|
| 1.1 | 实现 RaftGroupManager | `powerfs-filer/src/raft_group_manager.rs` |
| 1.2 | 实现 ShardStore 和状态机 | `powerfs-filer/src/shard_store.rs` |
| 1.3 | 实现目录范围分片策略 | `powerfs-filer/src/shard_strategy.rs` |
| 1.4 | 实现 MetaShardManager | `powerfs-filer/src/meta_shard_manager.rs` |
| 1.5 | 添加分片gRPC接口 | `powerfs-filer/src/grpc/meta_service.rs` |

### Phase 2：Master控制面改造（2周）

| 步骤 | 任务 | 关键文件 |
|------|------|----------|
| 2.1 | 添加 ShardManager | `powerfs-master/src/shard_manager.rs` |
| 2.2 | 添加路由表协议定义 | `powerfs-master/proto/master.proto` |
| 2.3 | 实现路由推送gRPC stream | `powerfs-master/src/grpc/route_service.rs` |
| 2.4 | 实现分片调度器 | `powerfs-master/src/shard_scheduler.rs` |

### Phase 3：客户端直连改造（2-3周）

| 步骤 | 任务 | 关键文件 |
|------|------|----------|
| 3.1 | 改造 FUSE Client | `powerfs-fuse/src/meta_router.rs` |
| 3.2 | 改造 S3 Handler | `powerfs-filer/src/s3_handler.rs` |
| 3.3 | 添加 Filer 客户端 | `powerfs-common/src/filer_client.rs` |
| 3.4 | 实现路由表缓存和刷新 | `powerfs-common/src/route_cache.rs` |

### Phase 4：跨分片事务（2周）

| 步骤 | 任务 | 关键文件 |
|------|------|----------|
| 4.1 | 实现乐观2PC协议 | `powerfs-filer/src/two_phase_commit.rs` |
| 4.2 | 实现跨分片rename | `powerfs-filer/src/cross_shard_ops.rs` |
| 4.3 | 添加冲突检测和回滚 | `powerfs-filer/src/conflict_resolver.rs` |

### Phase 5：迁移与验证（2周）

| 步骤 | 任务 | 关键文件 |
|------|------|----------|
| 5.1 | 实现数据迁移工具 | `powerfs-filer/src/migration.rs` |
| 5.2 | 集成测试 | `powerfs-filer/tests/integration_test.rs` |
| 5.3 | 性能测试 | `tests/benchmark/meta_benchmark.rs` |
| 5.4 | 灰度发布方案 | docs/shard-migration-guide.md |

---

## 10. 性能预期

| 指标 | 当前架构 | 分片架构 | 提升 |
|------|---------|---------|------|
| **元数据读延迟** | ~50ms | ~5ms | **10x** |
| **元数据写延迟** | ~30ms | ~10ms | **3x** |
| **元数据吞吐** | ~1000 QPS | ~100000 QPS+ | **100x** |
| **支持文件数** | ~1亿 | **无限扩展** | 突破瓶颈 |
| **故障影响范围** | 全集群 | 仅受影响分片 | **隔离** |

---

## 11. 风险与应对

| 风险 | 应对措施 |
|------|---------|
| **分片分裂期间数据不一致** | 分裂前先创建新分片，复制完成后再切换路由 |
| **路由表缓存过期** | 推送+拉取双机制，版本号校验 |
| **跨分片事务失败** | 乐观2PC + 回滚机制 + 后台补偿 |
| **数据迁移复杂** | 双写+灰度切换，支持快速回滚 |
| **开发复杂度高** | 复用现有raft-rs和RocksDB实现 |

---

## 12. 配置参考

```toml
# 分片配置
[shard]
# 缺省分片数（自动计算：node_count * 16）
default_count = 0

# 每节点最大分片数
max_shards_per_node = 100

# 分片inode数量上限（超过自动分裂）
shard_inode_limit = 1000000

# 分片Raft日志大小上限（超过自动分裂）
shard_log_limit_mb = 1024

# 路由表刷新间隔（秒）
route_refresh_interval = 30

# 跨分片事务超时（秒）
cross_shard_timeout = 30
```

---

*文档版本：v1.0*
*创建日期：2026-07-20*
*适用范围：PowerFS 分片优化方案*