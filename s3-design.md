# PowerFS S3 功能设计方案

## 1. 架构设计

### 1.1 整体架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                         S3 API Layer                                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│  │ Bucket   │ │ Object   │ │ Multipart│ │ Policy   │ │  IAM     │   │
│  │ Handler  │ │ Handler  │ │ Handler  │ │ Handler  │ │ Handler  │   │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘   │
└───────┼───────────┼───────────┼───────────┼───────────┼─────────────┘
        │           │           │           │           │
        ▼           ▼           ▼           ▼           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      Protocol & Auth Layer                           │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  - AWS Signature Version 4 (SigV4)                           │   │
│  │  - Presigned URL                                             │   │
│  │  - Token-based Auth                                          │   │
│  └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     PowerFS Master Layer                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐   │
│  │  DirectoryTree   │  │  LockManager     │  │  MetadataSync    │   │
│  │  (元数据管理)     │  │  (分布式锁)       │  │  (多节点同步)     │   │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     Volume Server Layer                              │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐       │
│  │  Volume-1  │ │  Volume-2  │ │  Volume-3  │ │   ...     │       │
│  └────────────┘ └────────────┘ └────────────┘ └────────────┘       │
└──────────────────────────────────────────────────────────────────────┘
```

### 1.2 核心组件关系

| 组件 | 职责 | 状态 |
|------|------|------|
| **S3 API Server** | 处理S3 HTTP请求，路由到对应的Handler | 新增 |
| **DirectoryTree** | 管理文件系统命名空间和元数据 | 已有，需扩展 |
| **LockManager** | 分布式锁服务（三层锁模型） | 新增 |
| **MetadataSync** | 基于Raft的多Master节点元数据同步 | 已有 |
| **VolumeClient** | 与Volume节点通信，执行数据读写 | 已有 |

---

## 2. 高性能分布式锁设计

### 2.1 设计原则

PowerFS利用Raft协议天然的Leader唯一性作为锁的基础，无需额外引入Redis等外部锁服务。

### 2.2 三层锁模型

| 层级 | 锁类型 | 延迟 | 一致性 | 适用场景 |
|------|--------|------|--------|---------|
| 第一层 | Leader本地锁 | <1μs | 强一致 | 普通写操作、短时间任务 |
| 第二层 | Raft Lease锁 | ~10ms | 线性一致 | 长时间操作、跨Leader切换 |
| 第三层 | etcd Lease锁（可选） | ~1ms | 强一致 | 跨集群协调、外部系统集成 |

### 2.3 锁模型架构

```
┌─────────────────────────────────────────────────────────────┐
│                     三层锁模型                               │
├─────────────────────────────────────────────────────────────┤
│  第一层：Leader本地锁（μs级）                                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  适用：普通写操作、短时间任务                          │   │
│  │  延迟：<1μs（纯内存）                                │   │
│  │  一致性：强一致（Leader唯一）                         │   │
│  │  实现：DashMap<Key, Mutex>                          │   │
│  └─────────────────────────────────────────────────────┘   │
│                              │                              │
│                              ▼                              │
│  第二层：Raft Lease锁（ms级）                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  适用：长时间操作、跨Leader切换场景                    │   │
│  │  延迟：~10ms（Raft日志复制）                         │   │
│  │  一致性：线性一致（Raft保证）                         │   │
│  │  实现：Raft日志 + Leader本地锁                       │   │
│  └─────────────────────────────────────────────────────┘   │
│                              │                              │
│                              ▼                              │
│  第三层：全局协调锁（按需）                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  适用：跨集群场景、外部系统协调                        │   │
│  │  延迟：~1ms（etcd）                                  │   │
│  │  一致性：强一致（etcd Raft）                          │   │
│  │  实现：etcd Lease（可选）                            │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 2.4 第一层：Leader本地锁

#### 2.4.1 设计原理

所有写请求由Raft Leader处理，Leader节点上用本地锁保证同一Key的串行化：

```
客户端请求 → Raft Leader（唯一）→ 本地锁（内存操作）→ Raft提交 → 返回结果
          ↘ Follower → 转发到Leader
```

#### 2.4.2 实现结构

```rust
pub struct LeaderLocalLockManager {
    locks: DashMap<String, Arc<Mutex<()>>>,
    lock_cache: DashMap<String, LockGuard>,
}

pub struct LockGuard {
    key: String,
    manager: Arc<LeaderLocalLockManager>,
    released: AtomicBool,
}

impl LeaderLocalLockManager {
    pub async fn acquire(&self, key: &str) -> LockGuard {
        let lock = self.locks.entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        
        let _guard = lock.lock().unwrap();
        
        LockGuard {
            key: key.to_string(),
            manager: self.clone(),
            released: AtomicBool::new(false),
        }
    }
    
    pub async fn try_acquire(&self, key: &str) -> Option<LockGuard> {
        let lock = self.locks.entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        
        if lock.try_lock().is_ok() {
            Some(LockGuard {
                key: key.to_string(),
                manager: self.clone(),
                released: AtomicBool::new(false),
            })
        } else {
            None
        }
    }
}
```

#### 2.4.3 锁粒度设计

| 锁类型 | Key格式 | 用途 |
|--------|---------|------|
| Bucket锁 | `bucket:{name}` | Bucket创建/删除/Policy操作 |
| Object锁 | `object:{bucket}:{key}` | Object写入/删除/复制操作 |
| Volume锁 | `volume:{id}` | Volume分配/删除操作 |
| Directory锁 | `directory:{path}` | 目录重命名/移动操作 |
| Session锁 | `session:{id}` | KV Session创建/删除操作 |

### 2.5 第二层：Raft Lease锁

#### 2.5.1 设计原理

当需要跨Leader切换保持锁时，通过Raft提交Lock/Unlock日志条目：

```
客户端请求 → 获取Leader本地锁 → Raft提交Lock条目 → 执行业务逻辑 → Raft提交Unlock条目 → 释放本地锁
```

#### 2.5.2 实现结构

```rust
pub struct RaftLeaseLockManager {
    raft_node: Arc<RaftNode>,
    local_locks: Arc<LeaderLocalLockManager>,
    active_locks: DashMap<String, LeaseLockState>,
}

pub struct LeaseLockState {
    holder: String,
    acquired_at: Instant,
    ttl: Duration,
    renew_count: u64,
}

pub struct LeaseLockGuard {
    key: String,
    holder: String,
    manager: Arc<RaftLeaseLockManager>,
    local_guard: LockGuard,
}

impl RaftLeaseLockManager {
    pub async fn acquire(&self, key: &str, ttl: Duration) -> Result<LeaseLockGuard> {
        let holder = format!("{}:{}", self.raft_node.id(), uuid::Uuid::new_v4());
        
        // 关键修正：先Raft提交，再获取本地锁，避免失败时持有锁
        let lock_command = RaftCommand::AcquireLock {
            key: key.to_string(),
            holder: holder.clone(),
            ttl: ttl.as_secs() as u64,
        };
        
        self.raft_node.propose(lock_command).await?;
        
        let local_guard = self.local_locks.acquire(key).await;
        
        self.active_locks.insert(key.to_string(), LeaseLockState {
            holder: holder.clone(),
            acquired_at: Instant::now(),
            ttl,
            renew_count: 0,
        });
        
        Ok(LeaseLockGuard {
            key: key.to_string(),
            holder,
            manager: self.clone(),
            local_guard,
        })
    }
    
    pub async fn renew(&self, key: &str, holder: &str) -> Result<()> {
        let renew_command = RaftCommand::RenewLock {
            key: key.to_string(),
            holder: holder.to_string(),
        };
        
        self.raft_node.propose(renew_command).await?;
        
        if let Some(mut state) = self.active_locks.get_mut(key) {
            state.renew_count += 1;
        }
        
        Ok(())
    }
}
```

#### 2.5.3 Raft命令定义

```rust
pub enum RaftCommand {
    AcquireLock {
        key: String,
        holder: String,
        ttl: u64,
    },
    ReleaseLock {
        key: String,
        holder: String,
    },
    RenewLock {
        key: String,
        holder: String,
    },
}
```

### 2.6 第三层：全局协调锁（可选）

基于etcd Lease实现，用于跨集群场景或与外部系统协调：

```rust
pub struct EtcdLeaseLockManager {
    client: Arc<etcd_client::Client>,
    lease_ttl: Duration,
}

impl EtcdLeaseLockManager {
    pub async fn acquire(&self, key: &str) -> Result<EtcdLockGuard> {
        let lease = self.client.lease_grant(self.lease_ttl.as_secs() as i64).await?;
        let result = self.client.put(
            key,
            uuid::Uuid::new_v4().to_string(),
            Some(etcd_client::PutOptions::new().with_lease(lease.id())),
        ).await?;
        
        Ok(EtcdLockGuard {
            key: key.to_string(),
            lease_id: lease.id(),
            manager: self.clone(),
        })
    }
}
```

### 2.7 S3操作锁策略

| S3操作 | 锁类型 | 锁粒度 | 超时时间 |
|--------|--------|--------|---------|
| CreateBucket | Raft Lease锁 | Bucket级 | 30s |
| DeleteBucket | Raft Lease锁 | Bucket级 | 30s |
| PutObject | Leader本地锁 | Object级 | 60s |
| GetObject | 无需锁 | - | - |
| DeleteObject | Leader本地锁 | Object级 | 10s |
| CopyObject | Leader本地锁 | 源+目标Object级 | 120s |
| ListObjects | 无需锁 | - | - |
| CreateMultipartUpload | Raft Lease锁 | Object级 | 86400s（24h） |
| CompleteMultipartUpload | Leader本地锁 | Object级 | 60s |
| AbortMultipartUpload | Leader本地锁 | Object级 | 30s |
| PutBucketPolicy | Raft Lease锁 | Bucket级 | 30s |
| PutBucketTagging | Leader本地锁 | Bucket级 | 10s |
| PutObjectTagging | Leader本地锁 | Object级 | 10s |

### 2.8 与Redis锁方案对比

| 维度 | Leader本地锁 | Raft Lease锁 | Redis锁 |
|------|-------------|-------------|---------|
| **延迟** | <1μs | ~10ms | ~1ms |
| **一致性** | 强一致 | 线性一致 | 最终一致 |
| **网络开销** | 零 | Raft复制（内部） | 每次请求 |
| **额外依赖** | 无 | 无 | Redis |
| **故障恢复** | Leader切换自动释放 | Raft保证 | 需要超时 |
| **实现复杂度** | 低 | 中 | 高（Redlock） |
| **适用场景** | 普通写操作 | 长时间操作 | 跨集群协调 |

---

## 3. Bucket与Object映射设计

### 3.1 Bucket与Directory映射

| S3概念 | PowerFS概念 | 映射关系 |
|--------|------------|---------|
| Bucket | Directory | Bucket名称作为根目录下的一级目录 |
| Object | File | Object Key作为Bucket目录下的文件路径 |

**路径映射规则**：

```
S3: bucket-name/object/path.txt
PowerFS: /bucket-name/object/path.txt
```

### 3.2 Entry扩展字段

在现有的`Entry`结构中添加S3相关字段：

```rust
pub struct Entry {
    // 已有字段...
    s3_metadata: HashMap<String, String>,                    // S3用户元数据
    s3_etag: String,                                        // S3 ETag
    s3_version_id: Option<String>,                          // 版本ID
    s3_storage_class: String,                               // 存储类型
    s3_object_lock_retention: Option<ObjectLockRetention>,  // 对象锁
    s3_owner: Option<ObjectOwner>,                          // 对象所有者
}
```

---

## 4. S3 API设计

### 4.1 API Server结构

```rust
pub struct S3ApiServer {
    master: Arc<MasterNode>,
    address: SocketAddr,
    auth_manager: Arc<AuthManager>,
    lock_manager: Arc<LockManager>,
}
```

### 4.2 支持的S3 API

#### 4.2.1 Bucket操作

| API | HTTP方法 | 路径 | 说明 |
|-----|---------|------|------|
| CreateBucket | PUT | /bucket | 创建Bucket |
| DeleteBucket | DELETE | /bucket | 删除Bucket |
| ListBuckets | GET | / | 列出所有Bucket |
| HeadBucket | HEAD | /bucket | 检查Bucket是否存在 |
| GetBucketPolicy | GET | /bucket?policy | 获取Bucket策略 |
| PutBucketPolicy | PUT | /bucket?policy | 设置Bucket策略 |
| DeleteBucketPolicy | DELETE | /bucket?policy | 删除Bucket策略 |
| GetBucketTagging | GET | /bucket?tagging | 获取Bucket标签 |
| PutBucketTagging | PUT | /bucket?tagging | 设置Bucket标签 |
| DeleteBucketTagging | DELETE | /bucket?tagging | 删除Bucket标签 |

#### 4.2.2 Object操作

| API | HTTP方法 | 路径 | 说明 |
|-----|---------|------|------|
| PutObject | PUT | /bucket/object | 上传Object |
| GetObject | GET | /bucket/object | 下载Object |
| DeleteObject | DELETE | /bucket/object | 删除Object |
| HeadObject | HEAD | /bucket/object | 检查Object是否存在 |
| CopyObject | PUT | /bucket/object?copySource | 复制Object |
| ListObjects | GET | /bucket | 列出Bucket中的Object |
| ListObjectsV2 | GET | /bucket?list-type=2 | 列出Bucket中的Object（V2） |

#### 4.2.3 Multipart操作

| API | HTTP方法 | 路径 | 说明 |
|-----|---------|------|------|
| CreateMultipartUpload | POST | /bucket/object?uploads | 初始化多部分上传 |
| UploadPart | PUT | /bucket/object?uploadId=xxx&partNumber=xxx | 上传分片 |
| CompleteMultipartUpload | POST | /bucket/object?uploadId=xxx | 完成多部分上传 |
| AbortMultipartUpload | DELETE | /bucket/object?uploadId=xxx | 中止多部分上传 |
| ListParts | GET | /bucket/object?uploadId=xxx | 列出已上传分片 |

#### 4.2.4 Object Tagging操作

| API | HTTP方法 | 路径 | 说明 |
|-----|---------|------|------|
| GetObjectTagging | GET | /bucket/object?tagging | 获取Object标签 |
| PutObjectTagging | PUT | /bucket/object?tagging | 设置Object标签 |
| DeleteObjectTagging | DELETE | /bucket/object?tagging | 删除Object标签 |

### 4.3 认证机制

#### 4.3.1 AWS Signature Version 4

```rust
pub struct AuthManager {
    credentials: DashMap<String, Credential>,
}

pub struct Credential {
    access_key: String,
    secret_key: String,
    expire_at: Option<Instant>,
}

impl AuthManager {
    pub async fn verify_signature(&self, req: &http::Request) -> Result<String> {
        // 解析Authorization header
        // 验证SigV4签名
        // 返回用户ID
    }
}
```

#### 4.3.2 Presigned URL

```rust
pub struct PresignedUrlManager {
    auth_manager: Arc<AuthManager>,
}

impl PresignedUrlManager {
    pub fn generate_get_url(&self, bucket: &str, key: &str, expires: Duration) -> String {
        // 生成预签名GET URL
    }
    
    pub fn generate_put_url(&self, bucket: &str, key: &str, expires: Duration) -> String {
        // 生成预签名PUT URL
    }
    
    pub fn verify_presigned_url(&self, req: &http::Request) -> Result<()> {
        // 验证预签名URL
    }
}
```

---

## 5. S3→Volume 数据流程

### 5.1 PutObject 完整流程

```
客户端 PUT /bucket/object
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  S3 API Server                                       │
│  ┌─────────────────────────────────────────────────┐│
│  │  1. 认证校验（SigV4）                           ││
│  │  2. 获取Object锁（Leader本地锁）                 ││
│  │  3. 调用Master.assign_volume()分配可写Volume    ││
│  └─────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  Master.assign_volume()                             │
│  ┌─────────────────────────────────────────────────┐│
│  │  1. 在现有Volume中查找可写Volume                ││
│  │     - 状态为Available                           ││
│  │     - used < size_limit                         ││
│  │  2. 找到 → 返回Volume ID和节点信息               ││
│  │  3. 未找到 → 调用create_new_volume()创建新Volume││
│  │  4. 返回Fid（文件ID）= VolumeId + NeedleId      ││
│  └─────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  Volume Server                                      │
│  ┌─────────────────────────────────────────────────┐│
│  │  1. 接收写入请求                                ││
│  │  2. 创建Needle（文件块）                        ││
│  │  3. 将数据写入Volume文件                        ││
│  │  4. 返回写入确认                               ││
│  └─────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  DirectoryTree                                      │
│  ┌─────────────────────────────────────────────────┐│
│  │  1. 创建Entry记录                               ││
│  │     - path: /bucket/object                      ││
│  │     - chunks: [FileChunk { fid, offset, size }]││
│  │     - s3_etag: 计算MD5                          ││
│  │     - s3_metadata: 用户元数据                    ││
│  │  2. Raft提交Entry                               ││
│  └─────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
        │
        ▼
   返回ETag和Location
```

### 5.2 关键数据结构

#### 5.2.1 FileChunk

```rust
pub struct FileChunk {
    pub fid: String,                    // VolumeId + NeedleId
    pub offset: u64,                    // 在文件中的偏移
    pub size: u64,                      // 块大小
    pub checksum: Option<String>,       // 校验和
}
```

#### 5.2.2 Volume分配逻辑

```rust
impl MasterNode {
    pub async fn assign_volume(&self, collection: &str) -> Result<(Fid, Vec<DataNodeInfo>)> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let volumes = self.volumes.read().unwrap();
        
        // 查找可写Volume
        for (volume_id, volume) in volumes.iter() {
            if volume.state == VolumeState::Available && 
               volume.used < volume.size &&
               volume.collection.0 == collection {
                return Ok((
                    Fid::new(volume_id, volume.next_file_key),
                    vec![self.get_node_info(&volume.node_id)],
                ));
            }
        }

        // 未找到可写Volume，创建新Volume
        self.create_new_volume("3", collection).await
    }
}
```

### 5.3 GetObject 完整流程

```
客户端 GET /bucket/object
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  S3 API Server                                       │
│  ┌─────────────────────────────────────────────────┐│
│  │  1. 认证校验（SigV4）                           ││
│  │  2. 调用DirectoryTree.find_entry()查找Entry    ││
│  └─────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  DirectoryTree.find_entry()                          │
│  ┌─────────────────────────────────────────────────┐│
│  │  1. 查找Entry记录                               ││
│  │  2. 返回chunks列表（包含Fid）                    ││
│  └─────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  Volume Server                                      │
│  ┌─────────────────────────────────────────────────┐│
│  │  1. 根据Fid定位Needle                           ││
│  │  2. 读取Needle数据                              ││
│  │  3. 返回数据                                    ││
│  └─────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
        │
        ▼
   返回Object数据
```

---

## 6. DirectoryTree 分布式策略

### 6.1 当前架构

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Master-1    │    │  Master-2    │    │  Master-3    │
│  (Leader)    │    │  (Follower)  │    │  (Follower)  │
│              │    │              │    │              │
│  RocksDB     │◄───│  RocksDB     │◄───│  RocksDB     │
│  Directory   │    │  Directory   │    │  Directory   │
│  Tree        │    │  Tree        │    │  Tree        │
└──────────────┘    └──────────────┘    └──────────────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            ▼
                      Raft协议同步
```

### 6.2 S3元数据分布策略

| 策略 | 说明 | 优点 | 缺点 |
|------|------|------|------|
| **Raft复制**（推荐） | S3元数据通过Raft同步到所有Master节点 | 强一致性、简单 | 容量受限（所有节点存储全量元数据） |
| **分片存储** | 按Bucket或路径前缀分片到不同Master | 水平扩展 | 复杂、跨分片操作困难 |
| **独立元数据层** | 使用etcd或专用元数据存储 | 高可用、可扩展 | 额外依赖、延迟增加 |

### 6.3 推荐方案：Raft复制

**当前阶段（初期）**：
- S3元数据通过Raft协议复制到所有Master节点
- DirectoryTree的所有变更作为Raft日志提交
- 所有Master节点都有完整的元数据副本

**未来扩展**：
- 当元数据量超过单节点容量时，引入分片机制
- 按Bucket哈希分片到不同的元数据组

---

## 7. 命名空间隔离

### 7.1 S3与FUSE共享同一命名空间

```
根目录 /
├── bucket-name-1/           ← S3 Bucket
│   ├── object1.txt
│   └── subdir/
│       └── object2.txt
├── bucket-name-2/           ← S3 Bucket
│   └── data.json
└── fuse-directory/          ← FUSE目录
    └── local-file.txt
```

### 7.2 隔离机制

| 访问方式 | 根路径 | 可见内容 |
|---------|--------|---------|
| S3 API | `/` | 仅Bucket目录（一级目录） |
| FUSE | `/` | 所有目录（包括Bucket和普通目录） |

**S3 API路径解析规则**：
```
S3请求: GET /mybucket/myobject.txt
解析:    /mybucket → Bucket目录
         /mybucket/myobject.txt → Object文件
```

---

## 8. Proto文件更新

### 8.1 Entry消息扩展

```protobuf
message Entry {
    string name = 1;
    string directory = 2;
    
    // S3相关字段
    map<string, string> s3_metadata = 10;
    string s3_etag = 11;
    optional string s3_version_id = 12;
    string s3_storage_class = 13;
    optional ObjectLockRetention s3_object_lock_retention = 14;
    optional ObjectOwner s3_owner = 15;
    
    // 文件块信息
    repeated FileChunk chunks = 20;
    
    // 其他字段...
}

message ObjectLockRetention {
    string mode = 1;
    int64 retain_until_date = 2;
}

message ObjectOwner {
    string id = 1;
    string display_name = 2;
}

message FileChunk {
    string fid = 1;
    uint64 offset = 2;
    uint64 size = 3;
    optional string checksum = 4;
}
```

### 8.2 向后兼容性

| 变更类型 | 兼容性 | 处理方式 |
|---------|--------|---------|
| 新增字段 | 完全兼容 | 老版本忽略新字段 |
| 修改字段类型 | 不兼容 | 使用新字段编号 |
| 删除字段 | 不兼容 | 保留字段但标记为deprecated |

---

## 9. 详细实施计划

### Phase 1：基础设施准备（1周）

| 任务ID | 任务描述 | 涉及文件 | 依赖 | 时间 | 优先级 |
|--------|---------|---------|------|------|--------|
| P1-01 | 创建LockManager模块 - Leader本地锁实现 | `powerfs-master/src/lock_manager/local_lock.rs` | 无 | 2天 | P0 |
| P1-02 | 创建LockManager模块 - Raft Lease锁实现 | `powerfs-master/src/lock_manager/raft_lease_lock.rs` | P1-01 | 2天 | P0 |
| P1-03 | 创建LockManager模块 - 统一接口封装 | `powerfs-master/src/lock_manager/mod.rs` | P1-01, P1-02 | 1天 | P0 |
| P1-04 | 创建S3 API Server模块 - HTTP服务框架 | `powerfs-master/src/s3_server/mod.rs` | 无 | 2天 | P0 |
| P1-05 | 创建S3 API Server模块 - 请求路由与Handler注册 | `powerfs-master/src/s3_server/router.rs` | P1-04 | 1天 | P0 |
| P1-06 | 创建AuthManager模块 - SigV4签名验证 | `powerfs-master/src/auth_manager/sigv4.rs` | 无 | 2天 | P0 |
| P1-07 | 创建AuthManager模块 - 凭证管理 | `powerfs-master/src/auth_manager/credentials.rs` | P1-06 | 1天 | P1 |

**验证标准**：
- LockManager单元测试通过
- S3 API Server启动成功，响应健康检查
- AuthManager能正确验证SigV4签名

---

### Phase 2：Bucket管理功能（1周）

| 任务ID | 任务描述 | 涉及文件 | 依赖 | 时间 | 优先级 |
|--------|---------|---------|------|------|--------|
| P2-01 | CreateBucket Handler - 创建Bucket目录 | `powerfs-master/src/s3_server/handlers/bucket.rs` | P1-03, P1-04, P1-05 | 2天 | P0 |
| P2-02 | DeleteBucket Handler - 删除Bucket目录 | `powerfs-master/src/s3_server/handlers/bucket.rs` | P2-01 | 1天 | P0 |
| P2-03 | ListBuckets Handler - 列出所有Bucket | `powerfs-master/src/s3_server/handlers/bucket.rs` | P1-04 | 1天 | P0 |
| P2-04 | HeadBucket Handler - 检查Bucket存在 | `powerfs-master/src/s3_server/handlers/bucket.rs` | P2-01 | 0.5天 | P1 |
| P2-05 | Bucket Tagging Handler - 标签管理 | `powerfs-master/src/s3_server/handlers/bucket_tagging.rs` | P2-01 | 1天 | P1 |
| P2-06 | Bucket Policy Handler - 策略管理 | `powerfs-master/src/s3_server/handlers/bucket_policy.rs` | P2-01, P1-07 | 3天 | P1 |

**验证标准**：
- 使用AWS CLI创建/删除/列出Bucket成功
- Bucket标签和策略功能正常

---

### Phase 3：Object管理功能（2周）

| 任务ID | 任务描述 | 涉及文件 | 依赖 | 时间 | 优先级 |
|--------|---------|---------|------|------|--------|
| P3-01 | 扩展Entry结构 - 添加S3相关字段 | `powerfs-common/src/types/entry.rs` | 无 | 1天 | P0 |
| P3-02 | 更新proto文件 - Entry消息扩展 | `powerfs-proto/proto/entry.proto` | P3-01 | 1天 | P0 |
| P3-03 | 更新assign_volume - 优先使用可写Volume | `powerfs-master/src/master.rs` | 无 | 2天 | P0 |
| P3-04 | PutObject Handler - 完整写入流程 | `powerfs-master/src/s3_server/handlers/object.rs` | P3-01, P3-03, P1-03 | 4天 | P0 |
| P3-05 | GetObject Handler - 完整读取流程 | `powerfs-master/src/s3_server/handlers/object.rs` | P3-04 | 2天 | P0 |
| P3-06 | DeleteObject Handler - 删除对象 | `powerfs-master/src/s3_server/handlers/object.rs` | P3-04 | 1天 | P0 |
| P3-07 | HeadObject Handler - 检查对象存在 | `powerfs-master/src/s3_server/handlers/object.rs` | P3-04 | 0.5天 | P1 |
| P3-08 | CopyObject Handler - 对象复制 | `powerfs-master/src/s3_server/handlers/object.rs` | P3-04 | 3天 | P1 |
| P3-09 | ListObjects Handler - V1列表 | `powerfs-master/src/s3_server/handlers/list_objects.rs` | P2-01 | 2天 | P0 |
| P3-10 | ListObjectsV2 Handler - V2列表 | `powerfs-master/src/s3_server/handlers/list_objects.rs` | P3-09 | 2天 | P0 |

**验证标准**：
- 使用AWS CLI上传/下载/删除对象成功
- 大文件（>1GB）上传下载正常
- 列表分页功能正常

---

### Phase 4：高级功能（2周）

| 任务ID | 任务描述 | 涉及文件 | 依赖 | 时间 | 优先级 |
|--------|---------|---------|------|------|--------|
| P4-01 | Multipart Upload - 初始化上传 | `powerfs-master/src/s3_server/handlers/multipart.rs` | P3-01 | 2天 | P0 |
| P4-02 | Multipart Upload - 上传分片 | `powerfs-master/src/s3_server/handlers/multipart.rs` | P4-01 | 2天 | P0 |
| P4-03 | Multipart Upload - 完成/中止上传 | `powerfs-master/src/s3_server/handlers/multipart.rs` | P4-01, P4-02 | 2天 | P0 |
| P4-04 | Object Tagging Handler - 对象标签 | `powerfs-master/src/s3_server/handlers/object_tagging.rs` | P3-04 | 1天 | P1 |
| P4-05 | Object ACL Handler - 对象访问控制 | `powerfs-master/src/s3_server/handlers/object_acl.rs` | P3-04, P1-07 | 2天 | P1 |
| P4-06 | Presigned URL - 生成预签名URL | `powerfs-master/src/s3_server/presigned_url.rs` | P1-06 | 2天 | P0 |
| P4-07 | Presigned URL - 验证预签名请求 | `powerfs-master/src/s3_server/presigned_url.rs` | P4-06 | 1天 | P0 |
| P4-08 | Versioning - 基础版本控制 | `powerfs-master/src/s3_server/handlers/versioning.rs` | P3-01 | 3天 | P1 |

**验证标准**：
- AWS CLI多部分上传成功
- 预签名URL能正确上传/下载对象
- 版本控制功能正常

---

### Phase 5：测试与优化（1周）

| 任务ID | 任务描述 | 涉及文件 | 依赖 | 时间 | 优先级 |
|--------|---------|---------|------|------|--------|
| P5-01 | 运行s3tests兼容性测试 | 外部测试套件 | P3-10, P4-03 | 3天 | P0 |
| P5-02 | 修复兼容性问题 | 相关Handler文件 | P5-01 | 2天 | P0 |
| P5-03 | 性能基准测试 | 测试脚本 | P3-04, P3-05 | 2天 | P0 |
| P5-04 | 性能优化（锁竞争、Raft提交） | `powerfs-master/src/lock_manager/`, `powerfs-master/src/raft_node.rs` | P5-03 | 2天 | P1 |

**验证标准**：
- s3tests通过率>95%
- Object写入延迟<50ms
- S3 API吞吐量>10,000 QPS（单节点）

---

### 整体甘特图

```
时间线 (周)
W1       W2       W3       W4       W5       W6       W7
│        │        │        │        │        │        │
P1───────┘        │        │        │        │        │
         P2───────┘        │        │        │        │
                  P3────────────────────────┘        │
                           P4────────────────────────┘
                                            P5───────┘
```

### 依赖关系图

```
P1-01 ──┬──→ P1-03 ──→ P2-01 ──→ P2-02 ──→ P2-03
        │              │         │
P1-02 ──┘              │         └──→ P2-04
                       │
P1-04 ──→ P1-05 ───────┘              └──→ P2-05
                                 └──→ P2-06

P3-01 ──→ P3-02
P3-03 ──→ P3-04 ──→ P3-05 ──→ P3-06
                    │         └──→ P3-07
                    └──→ P3-08
P2-01 ──→ P3-09 ──→ P3-10

P3-01 ──→ P4-01 ──→ P4-02 ──→ P4-03
P3-04 ──→ P4-04
P3-04 ──→ P4-05
P1-06 ──→ P4-06 ──→ P4-07
P3-01 ──→ P4-08

P3-10 ──┬──→ P5-01 ──→ P5-02
P4-03 ──┘
P3-04 ──┬──→ P5-03 ──→ P5-04
P3-05 ──┘
```

### 关键里程碑

| 里程碑 | 时间 | 完成标准 |
|--------|------|---------|
| M1：基础设施就绪 | W1结束 | LockManager、S3 API Server、AuthManager完成 |
| M2：Bucket功能就绪 | W2结束 | 所有Bucket操作可用 |
| M3：Object功能就绪 | W4结束 | 所有Object操作可用 |
| M4：高级功能就绪 | W6结束 | 多部分上传、预签名URL等可用 |
| M5：S3兼容发布 | W7结束 | s3tests通过率>95% |

### 资源需求

| 角色 | 人数 | 主要职责 |
|------|------|---------|
| Rust后端开发 | 2人 | 核心功能实现 |
| 测试工程师 | 1人 | 兼容性测试和性能测试 |
| DevOps | 1人 | 部署和CI/CD |

### 风险与缓解

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| SigV4认证实现复杂 | 中 | 高 | 参考AWS官方文档，使用成熟的Rust库 |
| s3tests兼容性问题 | 高 | 中 | 提前规划测试时间，参考SeaweedFS实现 |
| Raft性能瓶颈 | 中 | 中 | 优化Raft日志提交，批量写入 |
| Volume分配效率 | 低 | 中 | 优化assign_volume逻辑，缓存可写Volume列表 |

### 成功标准

| 维度 | 标准 |
|------|------|
| **功能完整** | 支持设计文档中列出的所有S3 API |
| **兼容性** | s3tests通过率>95% |
| **性能** | Object写入延迟<50ms，吞吐量>10,000 QPS |
| **可靠性** | Leader切换后服务正常，数据不丢失 |
| **安全性** | SigV4认证正确，无未授权访问 |

---

## 10. 关键设计决策

| 决策 | 说明 |
|------|------|
| **Bucket-Directory映射** | Bucket直接映射为根目录下的一级目录，简化实现 |
| **分布式锁** | 基于Raft的三层锁模型，零外部依赖，高性能 |
| **Entry扩展** | 在现有Entry结构中添加S3相关字段，避免重构 |
| **API兼容** | 完全兼容AWS S3 API，支持标准S3客户端 |
| **认证机制** | 支持SigV4和预签名URL，兼容AWS SDK |
| **数据流程** | PutObject先分配Volume，写入Needle，再记录Entry到DirectoryTree |
| **元数据分布** | S3元数据通过Raft复制到所有Master节点，保证强一致性 |
| **命名空间隔离** | S3与FUSE共享同一命名空间，S3仅可见Bucket目录 |

---

## 11. 性能预期

| 指标 | 预期值 | 说明 |
|------|--------|------|
| Leader本地锁延迟 | <1μs | 纯内存操作 |
| Raft Lease锁延迟 | ~10ms | Raft日志复制 |
| S3 API吞吐量 | 10,000+ QPS | 单节点 |
| Object写入延迟 | <50ms | 包含锁获取、Volume分配、Raft提交 |
| Bucket创建延迟 | <30ms | Raft Lease锁 |

---

## 12. 与SeaweedFS S3功能对比

| 特性 | PowerFS | SeaweedFS |
|------|---------|-----------|
| **架构模式** | 基于Raft的分布式锁 | 独立Filer + 分布式锁 |
| **锁延迟** | <1μs（Leader本地锁） | ~1ms（Redis/etcd） |
| **一致性** | 线性一致（Raft保证） | 最终一致 |
| **额外依赖** | 无 | Redis/etcd |
| **代码语言** | Rust | Go |
| **API兼容** | 完全兼容 | 完全兼容 |
| **多节点同步** | Raft原生支持 | MetaAggregator |
| **元数据存储** | Raft复制到所有Master | 独立Filer存储 |

---

## 13. 风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Leader切换时锁丢失 | 中 | Raft Lease锁保证跨Leader一致性 |
| 锁竞争导致性能下降 | 低 | 细粒度锁设计，避免热点Key |
| S3 API兼容性问题 | 中 | 参考SeaweedFS实现，执行s3tests |
| 认证实现复杂 | 中 | 参考AWS官方文档，使用成熟库 |
| 元数据容量瓶颈 | 低 | 当前阶段使用Raft复制，未来支持分片 |
| Volume分配效率 | 中 | 优化assign_volume逻辑，优先使用可写Volume |

---

## 14. 附录

### 14.1 Raft Lease锁顺序修正说明

**问题**：原设计先获取本地锁再Raft提交，若Raft提交失败，本地锁会被长时间持有。

**修正**：先Raft提交Lock条目，成功后再获取本地锁，失败时不会持有任何锁。

**伪代码**：
```rust
// 修正前（错误）
let local_guard = self.local_locks.acquire(key).await;  // 获取锁
self.raft_node.propose(lock_command).await?;             // 可能失败，锁被持有

// 修正后（正确）
self.raft_node.propose(lock_command).await?;             // 先提交，失败不影响
let local_guard = self.local_locks.acquire(key).await;   // 再获取锁
```

### 14.2 参考资料

- [AWS S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
- [AWS Signature Version 4](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)
- [SeaweedFS S3 Implementation](https://github.com/seaweedfs/seaweedfs/tree/master/weed/s3api)

---

*文档版本：v1.1*  
*创建日期：2026-07-06*  
*最后更新：2026-07-06*