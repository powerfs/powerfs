# PowerFS Filer 架构设计

## 1. 设计背景

### 1.1 当前架构问题

| 问题 | 描述 | 影响 |
|------|------|------|
| **Master成为瓶颈** | 每次PUT/GET都要经过Master | 高延迟、低吞吐 |
| **无法发挥大规模Volume能力** | 每次PUT都重新分配Volume | 无法利用已有Volume的空间 |
| **元数据集中存储** | 所有元数据在Master的DirectoryTree | 扩展性受限 |

### 1.2 核心变化

```
当前架构（每次操作都走Master）:
S3 Gateway ──► Master.assign_volume() ──► Volume.write_needle()

Filer架构（Bucket创建时分配Volume）:
Filer ──► Bucket创建时分配Volume ──► 后续操作直接路由到对应Volume
```

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Client Layer                                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  ┌─────────────────────┐  │
│  │ S3 Client│  │ AWS CLI  │  │  AWS SDK/API     │  │   S3 Browser        │  │
│  └────┬─────┘  └────┬─────┘  └───────┬──────────┘  └───────────┬─────────┘  │
└───────┼──────────────┼───────────────┼───────────────────────────┼───────────┘
        │              │               │                           │
        ▼              ▼               ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Filer Layer (S3 Gateway + Filer)                      │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    Filer 元数据管理层                                   │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────────┐  │  │
│  │  │ BucketManager│  │ EntryManager │  │  VolumeRouter              │  │  │
│  │  │ (Bucket→Vol  │  │ (路径→FID)   │  │  (Volume→Server路由)      │  │  │
│  │  │   映射管理)   │  │              │  │                          │  │  │
│  │  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────────┘  │  │
│  │         │                 │                      │                  │  │
│  │         ▼                 ▼                      ▼                  │  │
│  │  ┌──────────────────────────────────────────────────────────────┐   │  │
│  │  │                   元数据存储 (Redis)                          │   │  │
│  │  │  bucket:{name} → {volume_ids[], size_limit, used_size}      │   │  │
│  │  │  entry:{bucket}/{key} → {fid, volume_id, size, mtime}       │   │  │
│  │  │  volume:{id} → {server_addr, size, used}                    │   │  │
│  │  └──────────────────────────────────────────────────────────────┘   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────┬─────────────────────────────────────┘
                                        │
          ┌─────────────────────────────┼─────────────────────────────┐
          │                             │                             │
          ▼                             ▼                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Master Cluster (Raft)                                │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐           │
│  │     Master-1     │  │     Master-2     │  │     Master-3     │           │
│  │    (Leader)      │  │    (Follower)    │  │    (Follower)    │           │
│  │                  │  │                  │  │                  │           │
│  │  职责:           │  │                  │  │                  │           │
│  │  - Volume分配    │  │                  │  │                  │           │
│  │  - 节点心跳      │  │                  │  │                  │           │
│  │  - 集群拓扑      │  │                  │  │                  │           │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘           │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Volume Server Cluster                                │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐               │
│  │  Volume-1  │ │  Volume-2  │ │  Volume-3  │ │   ...     │               │
│  │  (Bucket-A │ │  (Bucket-B │ │  (Bucket-A │ │           │               │
│  │   主副本)   │ │   主副本)   │ │   副本)     │ │           │               │
│  └────────────┘ └────────────┘ └────────────┘ └────────────┘               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 组件职责

| 组件 | 职责 |
|------|------|
| **Filer** | Bucket管理、Entry元数据管理、Volume路由、S3协议处理 |
| **Master** | Volume分配、集群拓扑管理、节点心跳 |
| **Volume Server** | 数据存储（Bucket数据直接写入） |
| **Redis** | 元数据存储（Bucket→Volume映射、Entry→FID映射） |

---

## 3. 核心数据模型

### 3.1 Bucket信息

```rust
pub struct BucketInfo {
    pub name: String,
    pub volume_ids: Vec<VolumeId>,
    pub size_limit: u64,
    pub used_size: u64,
    pub creation_time: chrono::DateTime<chrono::Utc>,
    pub replication: String,
    pub collection: String,
}
```

**Redis存储格式**:
```
Key: bucket:{bucket_name}
Value: JSON serialized BucketInfo
```

### 3.2 Entry信息

```rust
pub struct EntryInfo {
    pub bucket: String,
    pub key: String,
    pub fid: String,
    pub volume_id: u32,
    pub size: u64,
    pub mtime: chrono::DateTime<chrono::Utc>,
    pub etag: String,
    pub chunks: Vec<FileChunk>,
}
```

**Redis存储格式**:
```
Key: entry:{bucket}/{key}
Value: JSON serialized EntryInfo
```

### 3.3 Volume路由信息

```rust
pub struct VolumeRoute {
    pub volume_id: u32,
    pub server_addr: String,
    pub server_id: NodeId,
    pub size: u64,
    pub used: u64,
    pub state: VolumeState,
}
```

**Redis存储格式**:
```
Key: volume:{volume_id}
Value: JSON serialized VolumeRoute
```

---

## 4. Bucket分配策略

### 4.1 创建Bucket流程

```
1. 客户端请求: PUT /{bucket}
2. Filer检查Bucket是否已存在
3. Filer向Master申请Volume分配
   - 根据replication参数决定副本数
   - Master选择合适的Volume Server
4. Filer将Bucket→Volume映射写入Redis
5. 返回成功响应
```

**代码流程**:
```rust
pub async fn create_bucket(&self, bucket: &str, replication: &str) -> Result<BucketInfo> {
    // 检查Bucket是否存在
    if self.bucket_exists(bucket).await {
        return Err(PowerFsError::Conflict("Bucket already exists"));
    }

    // 从Master分配Volume
    let (fid, nodes) = self.master.assign_volume(replication, "default").await?;
    
    // 创建BucketInfo
    let bucket_info = BucketInfo {
        name: bucket.to_string(),
        volume_ids: vec![fid.volume_id],
        size_limit: 0,
        used_size: 0,
        creation_time: chrono::Utc::now(),
        replication: replication.to_string(),
        collection: "default".to_string(),
    };

    // 写入Redis
    self.redis_client.set(
        &format!("bucket:{}", bucket),
        serde_json::to_string(&bucket_info)?
    ).await?;

    // 缓存Volume路由信息
    for node in nodes {
        let route = VolumeRoute {
            volume_id: fid.volume_id.0,
            server_addr: format!("{}:{}", node.address, node.grpc_port),
            server_id: node.id,
            size: 0,
            used: 0,
            state: VolumeState::Available,
        };
        self.redis_client.set(
            &format!("volume:{}", fid.volume_id.0),
            serde_json::to_string(&route)?
        ).await?;
    }

    Ok(bucket_info)
}
```

### 4.2 Volume选择算法

| 策略 | 描述 | 适用场景 |
|------|------|---------|
| **Round-Robin** | 轮询选择Volume Server | 均匀负载 |
| **Least Used** | 选择使用率最低的Volume | 空间优化 |
| **Data Center Aware** | 优先选择同数据中心 | 低延迟 |
| **Collection Based** | 根据Collection选择 | 多租户隔离 |

### 4.3 扩容策略

当Bucket空间不足时，自动扩容：

```
1. 检测Bucket used_size >= size_limit * 0.8
2. Filer向Master申请新Volume
3. 将新Volume添加到Bucket的volume_ids列表
4. 更新Redis中的BucketInfo
```

---

## 5. Filer核心接口

### 5.1 Bucket管理

| 接口 | 方法 | 描述 |
|------|------|------|
| `create_bucket` | PUT /{bucket} | 创建Bucket并分配Volume |
| `delete_bucket` | DELETE /{bucket} | 删除Bucket及关联Volume |
| `get_bucket` | HEAD /{bucket} | 获取Bucket信息 |
| `list_buckets` | GET / | 列出所有Bucket |

### 5.2 Entry管理

| 接口 | 方法 | 描述 |
|------|------|------|
| `put_object` | PUT /{bucket}/{key} | 写入对象（直接路由到Volume） |
| `get_object` | GET /{bucket}/{key} | 读取对象（直接路由到Volume） |
| `delete_object` | DELETE /{bucket}/{key} | 删除对象 |
| `list_objects` | GET /{bucket} | 列出Bucket中的对象 |

### 5.3 Volume路由

| 接口 | 描述 |
|------|------|
| `get_volume_route` | 根据VolumeId获取Server地址 |
| `update_volume_route` | 更新Volume路由信息 |
| `invalidate_volume_cache` | 失效Volume缓存 |

---

## 6. 数据流对比

### 6.1 当前架构

```
PUT Object:
S3 Gateway ──► Master.get_entry(bucket)     ──► Master (Raft读)
            ──► Master.assign_volume()       ──► Master (Raft写)
            ──► Volume.write_needle()        ──► Volume Server

GET Object:
S3 Gateway ──► Master.get_entry(bucket)     ──► Master (Raft读)
            ──► Master.get_entry(object)     ──► Master (Raft读)
            ──► Master.get_volume_info()     ──► Master (Raft读)
            ──► Volume.read_needle()         ──► Volume Server
```

### 6.2 Filer架构

```
PUT Object:
Filer ──► Redis.get(bucket)                 ──► Redis (内存读)
      ──► Volume.write_needle()             ──► Volume Server (直接写入)
      ──► Redis.set(entry)                  ──► Redis (内存写)

GET Object:
Filer ──► Redis.get(entry)                  ──► Redis (内存读)
      ──► Volume.read_needle()              ──► Volume Server (直接读取)

Master参与的操作:
- Bucket创建（仅一次）
- Volume扩容（按需）
- Volume Server发现（定期）
```

---

## 7. 实施步骤

### Phase 1: Filer基础框架

| 步骤 | 任务 | 状态 |
|------|------|------|
| 1.1 | 创建 `powerfs-filer` crate | 待实施 |
| 1.2 | 实现 Redis 元数据存储 | 待实施 |
| 1.3 | 实现 BucketManager | 待实施 |
| 1.4 | 实现 EntryManager | 待实施 |
| 1.5 | 实现 VolumeRouter | 待实施 |

### Phase 2: S3 Gateway集成

| 步骤 | 任务 | 状态 |
|------|------|------|
| 2.1 | 迁移 S3 Handler 到 Filer | 待实施 |
| 2.2 | 修改 create_bucket 分配Volume | 待实施 |
| 2.3 | 修改 put_object 直接路由 | 待实施 |
| 2.4 | 修改 get_object 直接路由 | 待实施 |

### Phase 3: 高级特性

| 步骤 | 任务 | 状态 |
|------|------|------|
| 3.1 | Bucket空间限制 | 待实施 |
| 3.2 | 自动扩容 | 待实施 |
| 3.3 | 跨Volume条带化 | 待实施 |
| 3.4 | 监控与告警 | 待实施 |

---

## 8. 性能预期

| 指标 | 当前架构 | Filer架构 | 提升 |
|------|---------|----------|------|
| GET延迟 | ~50ms | ~5ms | 10x |
| PUT延迟 | ~30ms | ~8ms | 4x |
| Master QPS | ~1000 | ~10000+ | 10x+ |
| 扩展性 | 有限 | 水平扩展 | ✓ |

---

*文档版本：v1.0*
*创建日期：2026-07-20*