# PowerFS S3 功能设计方案

## 1. 架构设计

### 1.1 整体架构（内置 S3 Gateway 模式）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Client Layer                                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  ┌─────────────────────┐  │
│  │ S3 Client│  │ AWS CLI  │  │  AWS SDK/API     │  │   S3 Browser        │  │
│  └────┬─────┘  └────┬─────┘  └───────┬──────────┘  └───────────┬─────────┘  │
└───────┼──────────────┼───────────────┼───────────────────────────┼───────────┘
        │              │               │                           │
        │              │               │                           │
        ▼              ▼               ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         S3 Gateway Layer (PowerFS S3)                      │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │ Bucket   │ │ Object   │ │ Multipart│ │ Policy   │ │  Auth    │           │
│  │ Handler  │ │ Handler  │ │ Handler  │ │ Handler  │ │ Handler  │           │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘           │
└───────┼───────────┼───────────┼───────────┼───────────┼─────────────────────┘
        │           │           │           │           │
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
| **S3 Gateway** | S3协议处理、统一认证、HTTP API服务 | 新增 |
| **DirectoryTree** | 管理文件系统命名空间和元数据（S3 Bucket/Object） | 已有，需扩展 |
| **LockManager** | 分布式锁服务（三层锁模型） | 新增 |
| **MetadataSync** | 基于Raft的多Master节点元数据同步 | 已有 |
| **VolumeClient** | 与Volume节点通信，执行数据读写 | 已有 |
| **AuthManager** | S3访问密钥管理、SigV4签名验证 | 新增 |

### 1.3 S3 Gateway 集成架构

```
客户端请求 ──→ PowerFS S3 Gateway ──→ Master（元数据）/ Volume（数据）
   │                    │                        │
   │                    ▼                        ▼
   │              协议处理/认证            元数据操作/数据存储
   │                    │                        │
   │                    ▼                        ▼
   │              Raft元数据同步          分布式数据存储
   │
   ▼
用户访问
```

### 1.4 S3 Gateway 部署配置

```bash
# PowerFS S3 Gateway启动命令
powerfs s3 --port 9000 --master localhost:9333 \
  --access-key powerfs --secret-key powerfs123
```

**Docker Compose配置**：
```yaml
services:
  powerfs-s3:
    image: powerfs:latest
    command: s3 --port 9000 --master master:9333
    ports:
      - "9000:9000"
```

**端口分配**：
| 组件 | 端口 | 用途 |
|------|------|------|
| PowerFS S3 Gateway | 9000 | 对外S3 API服务 |

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
}
```

### 2.6 第三层：全局协调锁（可选）

```rust
pub struct EtcdLockManager {
    etcd_client: Arc<EtcdClient>,
    lease_id: Option<LeaseKeepAlive>,
}

impl EtcdLockManager {
    pub async fn acquire(&self, key: &str, ttl: Duration) -> Result<EtcdLockGuard> {
        let lease = self.etcd_client.lease(ttl).await?;
        
        match self.etcd_client.put(key, "locked", Some(lease.id())).await {
            Ok(_) => Ok(EtcdLockGuard {
                key: key.to_string(),
                lease,
                etcd_client: self.clone(),
            }),
            Err(e) => Err(PowerFsError::Internal(format!(
                "Failed to acquire etcd lock: {}",
                e
            ))),
        }
    }
}
```

---

## 3. S3 API 实现

### 3.1 Bucket 操作

#### 3.1.1 CreateBucket

```
请求路径: PUT /{bucket}
认证要求: 签名认证
权限要求: s3:CreateBucket
```

**实现逻辑**：
1. 验证Bucket名称合法性
2. 获取Bucket锁
3. 检查Bucket是否已存在
4. 创建Bucket元数据（Entry）
5. 更新目录树
6. 释放Bucket锁

#### 3.1.2 DeleteBucket

```
请求路径: DELETE /{bucket}
认证要求: 签名认证
权限要求: s3:DeleteBucket
```

**实现逻辑**：
1. 获取Bucket锁
2. 检查Bucket是否存在且为空
3. 删除Bucket元数据
4. 释放Bucket锁

#### 3.1.3 ListBuckets

```
请求路径: GET /
认证要求: 签名认证
权限要求: s3:ListAllMyBuckets
```

**实现逻辑**：
1. 扫描根目录下所有Bucket类型的Entry
2. 返回Bucket列表

### 3.2 Object 操作

#### 3.2.1 PutObject

```
请求路径: PUT /{bucket}/{key}
认证要求: 签名认证
权限要求: s3:PutObject
```

**实现逻辑**：
1. 获取Object锁
2. 验证Bucket存在
3. 通过Master分配Volume（assign_volume）
4. 获取目标Volume Server地址
5. 将数据写入Volume Server（write_needle）
6. 创建/更新Object元数据（路径 → FID映射）
7. 释放Object锁

**数据流向**：
```
S3 Gateway ──→ Master.assign_volume() ──→ Volume.write_needle()
                    │                              │
                    ▼                              ▼
              返回 FID +                    实际数据存储
              Volume节点信息
```

#### 3.2.2 GetObject

```
请求路径: GET /{bucket}/{key}
认证要求: 签名认证
权限要求: s3:GetObject
```

**实现逻辑**：
1. 验证Bucket和Object存在
2. 从DirectoryTree获取Object元数据和FID
3. 通过Master获取Volume Server地址
4. 从Volume Server读取数据（read_needle）
5. 返回数据

**数据流向**：
```
S3 Gateway ──→ Master.get_entry() ──→ Volume.read_needle()
                    │                              │
                    ▼                              ▼
              获取 FID +                    返回实际数据
              Volume节点信息
```

#### 3.2.3 DeleteObject

```
请求路径: DELETE /{bucket}/{key}
认证要求: 签名认证
权限要求: s3:DeleteObject
```

**实现逻辑**：
1. 获取Object锁
2. 验证Bucket和Object存在
3. 获取Object元数据（含FID）
4. 删除DirectoryTree中的元数据
5. 通知Volume Server删除数据（delete_needle）
6. 释放Object锁

### 3.3 Multipart Upload

#### 3.3.1 InitiateMultipartUpload

```
请求路径: POST /{bucket}/{key}?uploads
认证要求: 签名认证
权限要求: s3:InitiateMultipartUpload
```

**实现逻辑**：
1. 创建MultipartUpload会话
2. 生成Upload ID
3. 存储会话状态

#### 3.3.2 UploadPart

```
请求路径: PUT /{bucket}/{key}?uploadId={uploadId}&partNumber={partNumber}
认证要求: 签名认证
权限要求: s3:PutObject
```

**实现逻辑**：
1. 验证Upload ID有效性
2. 分配Volume并写入数据
3. 记录Part信息（ETag、大小）

#### 3.3.3 CompleteMultipartUpload

```
请求路径: POST /{bucket}/{key}?uploadId={uploadId}
认证要求: 签名认证
权限要求: s3:PutObject
```

**实现逻辑**：
1. 验证所有Part存在
2. 合并Part到最终对象
3. 创建最终Object元数据
4. 清理Multipart会话

#### 3.3.4 AbortMultipartUpload

```
请求路径: DELETE /{bucket}/{key}?uploadId={uploadId}
认证要求: 签名认证
权限要求: s3:AbortMultipartUpload
```

**实现逻辑**：
1. 删除所有已上传的Part数据
2. 清理Multipart会话状态

---

## 4. 认证与权限

### 4.1 双认证架构

```
┌──────────────────────────────────────────────────────────────┐
│                    S3 Gateway                                │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              认证入口层                                │   │
│  │  ┌──────────────┐  ┌─────────────────────────────┐   │   │
│  │  │   SigV4      │  │     Token Auth             │   │   │
│  │  │  签名验证    │  │  (内部服务/管理接口)        │   │   │
│  │  └──────┬───────┘  └───────────┬─────────────────┘   │   │
│  │         │                      │                     │   │
│  │         ▼                      ▼                     │   │
│  │  ┌──────────────────────────────────────────────┐   │   │
│  │  │            访问密钥管理                        │   │   │
│  │  │  ┌──────────────┐  ┌─────────────────────┐   │   │   │
│  │  │  │ AccessKey    │  │ SecretKey           │   │   │   │
│  │  │  │  (明文存储)   │  │  (HMAC-SHA256)      │   │   │   │
│  │  │  └──────────────┘  └─────────────────────┘   │   │   │
│  │  └──────────────────────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

### 4.2 SigV4 签名验证

```rust
pub struct SigV4Authenticator {
    auth_manager: Arc<AuthManager>,
}

impl SigV4Authenticator {
    pub async fn authenticate(&self, request: &Request<Body>) -> Result<Credentials> {
        let signature = self.extract_signature(request)?;
        let credentials = self.auth_manager.get_credentials(&signature.access_key)?;
        
        let expected_signature = self.compute_signature(request, &credentials)?;
        
        if signature.signature == expected_signature {
            Ok(credentials)
        } else {
            Err(PowerFsError::Unauthorized("Invalid signature".to_string()))
        }
    }
}
```

### 4.3 访问密钥管理 API

#### 4.3.1 CreateAccessKey

```
请求路径: POST /admin/access-keys
认证要求: 管理员认证
权限要求: s3:CreateAccessKey
```

#### 4.3.2 ListAccessKeys

```
请求路径: GET /admin/access-keys
认证要求: 管理员认证
权限要求: s3:ListAccessKeys
```

#### 4.3.3 DeleteAccessKey

```
请求路径: DELETE /admin/access-keys/{accessKey}
认证要求: 管理员认证
权限要求: s3:DeleteAccessKey
```

---

## 5. 元数据管理

### 5.1 S3 元数据结构

S3 Bucket 和 Object 的元数据存储在 Master 的 DirectoryTree 中：

| 类型 | 路径格式 | 元数据内容 |
|------|----------|-----------|
| Bucket | `/s3/{bucket}` | Bucket属性、创建时间、Policy |
| Object | `/s3/{bucket}/{key}` | FID、大小、修改时间、ETag |

### 5.2 元数据与数据分离

```
┌──────────────────────────────────────────────────────────────┐
│                      Master 节点                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              DirectoryTree                            │   │
│  │  ┌──────────────┐  ┌─────────────────────────────┐   │   │
│  │  │  Bucket元数据 │  │  Object元数据 (路径→FID)    │   │   │
│  │  │  - 名称      │  │  - FID (VolumeId, FileKey) │   │   │
│  │  │  - 创建时间   │  │  - 大小                   │   │   │
│  │  │  - Policy    │  │  - 修改时间               │   │   │
│  │  └──────────────┘  └─────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                    Volume Server 节点                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              StorageManager                           │   │
│  │  ┌──────────────┐  ┌─────────────────────────────┐   │   │
│  │  │  Needle数据  │  │  Needle索引                 │   │   │
│  │  │  - 文件内容  │  │  - FileKey→偏移             │   │   │
│  │  │  - 校验和    │  │  - 大小                    │   │   │
│  │  └──────────────┘  └─────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

### 5.3 目录树扩展

为支持 S3 命名空间，DirectoryTree 需要扩展：

```rust
pub struct DirectoryTree {
    db: sled::Db,
    root: String,
}

impl DirectoryTree {
    pub fn get_s3_bucket(&self, bucket: &str) -> Option<Entry> {
        self.get_entry(&format!("/s3/{}", bucket))
    }
    
    pub fn create_s3_bucket(&self, bucket: &str) -> Result<u64> {
        let entry = Entry {
            name: bucket.to_string(),
            directory: "/s3".to_string(),
            attributes: Some(FuseAttributes {
                mode: 0o40755,
                ..Default::default()
            }),
            ..Default::default()
        };
        self.create_entry(entry)
    }
    
    pub fn get_s3_object(&self, bucket: &str, key: &str) -> Option<Entry> {
        self.get_entry(&format!("/s3/{}/{}", bucket, key))
    }
}
```

---

## 6. 生产部署配置

### 6.1 单节点开发环境

```bash
# Master节点
powerfs master -p 9333 -d /data/master

# Volume节点
powerfs volume -p 8080 -d /data/volume -m localhost:9333

# S3 Gateway
powerfs s3 --port 9000 --master localhost:9333
```

### 6.2 多节点生产环境

```yaml
services:
  master-1:
    image: powerfs:latest
    command: master -p 9333 -d /data/master
    environment:
      - RAFT_PEERS=master-1:9333,master-2:9333,master-3:9333

  master-2:
    image: powerfs:latest
    command: master -p 9333 -d /data/master
    environment:
      - RAFT_PEERS=master-1:9333,master-2:9333,master-3:9333

  master-3:
    image: powerfs:latest
    command: master -p 9333 -d /data/master
    environment:
      - RAFT_PEERS=master-1:9333,master-2:9333,master-3:9333

  volume-1:
    image: powerfs:latest
    command: volume -p 8080 -d /data/volume -m master-1:9333

  volume-2:
    image: powerfs:latest
    command: volume -p 8080 -d /data/volume -m master-1:9333

  volume-3:
    image: powerfs:latest
    command: volume -p 8080 -d /data/volume -m master-1:9333

  s3-gateway:
    image: powerfs:latest
    command: s3 --port 9000 --master master-1:9333
    ports:
      - "9000:9000"
```

### 6.3 部署拓扑

```
┌──────────────────────────────────────────────────────────────────────┐
│                      生产部署拓扑                                     │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                        负载均衡层                               │  │
│  │                     Nginx / LB                                 │  │
│  │   ┌──────────────┐                                             │  │
│  │   │  Port 9000   │                                             │  │
│  │   │  (S3 API)    │                                             │  │
│  │   └──────┬───────┘                                             │  │
│  └──────────┼─────────────────────────────────────────────────────┘  │
│             │                                                        │
│             ▼                                                        │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                    PowerFS S3 Gateway集群                       │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │  │
│  │  │  Gateway-1   │  │  Gateway-2   │  │  Gateway-N   │         │  │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘         │  │
│  └──────────┼─────────────────┼─────────────────┼─────────────────┘  │
│             │                 │                 │                     │
│             └─────────────────┼─────────────────┘                     │
│                               ▼                                       │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                     PowerFS Master集群                         │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐                     │  │
│  │  │ Master-1 │  │ Master-2 │  │ Master-3 │                     │  │
│  │  │ (Leader) │  │(Follower)│  │(Follower)│                     │  │
│  │  └──────────┘  └──────────┘  └──────────┘                     │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                               │
│                               ▼
│  ┌────────────────────────────────────────────────────────────────┐
│  │                     Volume节点集群                             │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │  │Volume-1  │  │Volume-2  │  │Volume-3  │  │Volume-N  │      │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │
│  └────────────────────────────────────────────────────────────────┘
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 7. Filer 独立进程架构规划（未来实施）

### 7.1 设计背景

当前架构问题：
- **Master成为瓶颈**：所有S3请求都需要经过Master处理元数据操作
- **无法水平扩展**：Master节点数量固定（Raft集群），元数据压力集中
- **延迟较高**：每次GET/PUT都需要多次Master RPC调用

### 7.2 目标架构

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
│                         负载均衡层 (Nginx/HAProxy)                           │
│                              Port 9000                                       │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Filer Cluster (可水平扩展)                            │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐           │
│  │    Filer-1       │  │    Filer-2       │  │    Filer-3       │    ...    │
│  │  ┌────────────┐  │  │  ┌────────────┐  │  │  ┌────────────┐  │           │
│  │  │ S3 Handler │  │  │  │ S3 Handler │  │  │  │ S3 Handler │  │           │
│  │  ├────────────┤  │  │  ├────────────┤  │  │  ├────────────┤  │           │
│  │  │ Meta Store │  │  │  │ Meta Store │  │  │  │ Meta Store │  │           │
│  │  └────────────┘  │  │  └────────────┘  │  │  └────────────┘  │           │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘           │
│           │                     │                     │                      │
│           └─────────────────────┼─────────────────────┘                      │
│                                 ▼                                            │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    元数据存储层 (可选后端)                              │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │  │
│  │  │   Redis     │  │   MySQL     │  │  Cassandra  │  │  LevelDB    │  │  │
│  │  │  (推荐)     │  │  (关系型)    │  │  (分布式)    │  │  (嵌入式)    │  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                │
                                │ 仅分配Volume
                                ▼
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
│  └────────────┘ └────────────┘ └────────────┘ └────────────┘               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.3 组件职责分离

| 组件 | 当前职责 | Filer架构后职责 |
|------|---------|----------------|
| **Master** | Volume分配 + 元数据管理 + Raft同步 | 仅Volume分配 + 集群拓扑管理 |
| **Filer** | 不存在 | S3协议处理 + 元数据存储 + 目录树管理 |
| **Volume** | 数据存储 | 数据存储（不变） |

### 7.4 元数据存储选型

| 后端 | 优点 | 缺点 | 适用场景 |
|------|------|------|---------|
| **Redis** | 高性能、支持集群、已有部署 | 内存成本 | 推荐首选 |
| **MySQL** | 事务支持、成熟稳定 | 性能受限 | 中小规模 |
| **Cassandra** | 分布式、高可用 | 运维复杂 | 大规模 |
| **LevelDB** | 嵌入式、无依赖 | 单机、难扩展 | 开发/测试 |

### 7.5 数据流对比

#### 当前架构（每次请求都经过Master）

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

#### Filer架构（Master压力大幅降低）

```
PUT Object:
Filer ──► MetaStore.get(bucket)        ──► Redis (内存读)
       ──► Master.assign_volume()      ──► Master (仅此一次RPC)
       ──► Volume.write_needle()       ──► Volume Server
       ──► MetaStore.put(object)       ──► Redis (内存写)

GET Object:
Filer ──► MetaStore.get(object)        ──► Redis (内存读)
       ──► Volume.read_needle()        ──► Volume Server
       (不经过Master!)
```

### 7.6 实施步骤

#### Phase 1: Filer基础框架
1. 创建 `powerfs-filer` 独立crate
2. 实现元数据存储抽象层 (`MetaStore` trait)
3. 实现 Redis 后端适配器
4. 迁移 S3 Handler 逻辑到 Filer

#### Phase 2: 集成与迁移
1. Filer 支持 gRPC 与 Master 通信
2. 实现多 Filer 负载均衡
3. 添加元数据缓存层
4. 兼容现有 S3 客户端

#### Phase 3: 高级特性
1. 元数据持久化策略
2. 跨 Filer 事务支持
3. 元数据备份与恢复
4. 监控与告警

### 7.7 API 变更

```bash
# 当前启动方式
powerfs s3 --port 9000 --master master:9333

# Filer架构后启动方式
powerfs filer --port 9000 --master master:9333 --meta-store redis://redis:6379

# 或使用内嵌LevelDB（开发模式）
powerfs filer --port 9000 --master master:9333 --meta-store leveldb:/data/meta
```

### 7.8 性能预期

| 指标 | 当前架构 | Filer架构 | 提升 |
|------|---------|----------|------|
| GET延迟 | ~50ms | ~5ms | 10x |
| PUT延迟 | ~30ms | ~10ms | 3x |
| Master QPS瓶颈 | ~1000 | ~10000+ | 10x+ |
| 可扩展性 | 有限 | 水平扩展 | ✓ |

---

## 8. S3 Gateway 本地缓存优化（当前实施）

### 8.1 缓存设计

为缓解 Master 压力，在 S3 Gateway 添加多级缓存：

```
┌─────────────────────────────────────────────────────────────────────┐
│                      S3 Gateway (带缓存)                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │ Bucket Cache │  │ Entry Cache  │  │ Volume Location Cache    │  │
│  │              │  │              │  │                          │  │
│  │ TTL: 60s     │  │ TTL: 30s     │  │ TTL: 300s                │  │
│  │ Key: bucket  │  │ Key: path    │  │ Key: volume_id           │  │
│  │ Value: bool  │  │ Value: Entry │  │ Value: volume_server_addr│  │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.2 缓存命中率预期

| 缓存类型 | 预期命中率 | 减少Master调用 |
|---------|-----------|---------------|
| Bucket Cache | 99% | GET/PUT不再检查bucket |
| Entry Cache | 80% | GET直接命中 |
| Volume Location Cache | 95% | GET不再查询volume位置 |

---

*文档版本：v1.4*
*创建日期：2026-07-06*
*最后更新：2026-07-20*