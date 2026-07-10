# PowerFS FUSE Coherence 方案设计（v2）

## 1. 架构概述

### 1.1 一致性模型

PowerFS 采用分层一致性模型：

| 层级 | 一致性级别 | 适用场景 | 实现机制 |
|------|-----------|---------|---------|
| **元数据操作** | 强一致 | mkdir, create, unlink, rename, setattr | 同步提交 + 错误回滚 |
| **缓存失效** | 最终一致 | 跨客户端缓存更新 | 服务器驱动推送 |
| **文件读写** | 租约保护 | 热点文件并发访问 | 租约机制 |
| **作业级** | 可选强一致 | AI/LLM 训练场景 | 作业域隔离 |

### 1.2 整体架构

```
┌────────────────────────────────────────────────────────────────────────┐
│                          Master Server                                 │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  DirectoryTree (RocksDB)                                         │  │
│  │  ┌──────────┐  ┌────────────────┐  ┌───────────────────────┐    │  │
│  │  │ Entry    │  │ Generation     │  │ MetadataNotification  │    │  │
│  │  │ (with    │  │ Counter        │  │ (push to subscribers) │    │  │
│  │  │ gen num) │  │                │  └───────────────────────┘    │  │
│  │  └──────────┘  └────────────────┘          │                    │  │
│  └─────────────────────────────────────────────┼────────────────────┘  │
│                                                ▼                       │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  Notification Broadcaster (gRPC Streaming)                       │  │
│  │  - subscribe_metadata RPC                                         │  │
│  │  - path prefix filtering                                          │  │
│  │  - backpressure handling                                           │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                │                       │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  Lease Manager (Optional - Phase 2)                               │  │
│  │  - Lease grant/renew/revoke                                       │  │
│  │  - Per-file lease tracking                                        │  │
│  │  - Lease expiration cleanup                                        │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │ FUSE Client 1│  │ FUSE Client 2│  │ FUSE Client 3│
    │              │  │              │  │              │
    │  Metadata    │  │  Metadata    │  │  Metadata    │
    │  Cache       │  │  Cache       │  │  Cache       │
    │  (with       │  │  (with       │  │  (with       │
    │   generation)│  │   generation)│  │   generation)│
    │              │  │              │  │              │
    │  Invalidator │  │  Invalidator │  │  Invalidator │
    │  (gRPC sub)  │  │  (gRPC sub)  │  │  (gRPC sub)  │
    │              │  │              │  │              │
    │  Lease Client│  │  Lease Client│  │  Lease Client│
    └──────────────┘  └──────────────┘  └──────────────┘
```

---

## 2. Phase 0：修复现有一致性缺陷

### 2.1 功能描述

修复 FUSE 客户端中所有 `warn-and-continue` 模式的元数据操作，改为同步提交 + 错误回滚模式。

### 2.2 代码变更

#### 2.2.1 `powerfs-fuse/src/fuser_fs.rs`

**修改操作**：`mkdir`, `create`, `unlink`, `rename`, `setattr`, `rmdir`, `symlink`, `link`

**模式变更**：
```rust
// 修复前：warn-and-continue
if let Err(e) = self.client.create_entry(filer_entry) {
    warn!("Failed to create directory entry on master: {}", e);
    // 继续执行，返回成功
}

// 修复后：同步提交 + 错误回滚
match self.client.create_entry(filer_entry).await {
    Ok(_) => {
        // 正常返回
        let attr = self.create_file_attr(&entry);
        reply.entry(&TTL, &attr, 0);
    }
    Err(e) => {
        // 回滚本地缓存
        self.cache.remove(inode);
        error!("Failed to create directory: {}", e);
        reply.error(libc::EIO);
    }
}
```

**具体修改位置**：

| 函数 | 行号范围 | 修改内容 |
|------|---------|---------|
| `mkdir` | ~600-650 | 同步提交 + 回滚 |
| `create` | ~650-750 | 同步提交 + 回滚 |
| `unlink` | ~800-850 | 同步提交 + 回滚 |
| `rmdir` | ~850-900 | 同步提交 + 回滚 |
| `rename` | ~1400-1500 | 改为原子操作 |
| `setattr` | ~950-1100 | 同步提交 + 回滚 |
| `symlink` | ~750-800 | 同步提交 + 回滚 |
| `link` | ~1100-1150 | 同步提交 + 回滚 |

#### 2.2.2 `powerfs-fuse/src/client.rs`

**修改**：确保所有 gRPC 调用返回 `Result`，去除 `unwrap_or_default()` 等吞掉错误的模式。

### 2.3 测试方案

#### 2.3.1 单元测试

**测试文件**：`powerfs-fuse/tests/coherence_phase0_test.rs`

| 测试用例 | 测试内容 |
|---------|---------|
| `test_mkdir_failure_rollback` | 创建目录时 Master 返回错误，验证本地缓存回滚 |
| `test_create_failure_rollback` | 创建文件时 Master 返回错误，验证本地缓存回滚 |
| `test_unlink_failure_rollback` | 删除文件时 Master 返回错误，验证本地缓存保留 |
| `test_setattr_failure_rollback` | 修改属性时 Master 返回错误，验证本地属性不变 |

#### 2.3.2 集成测试

**测试文件**：`powerfs-fuse/tests/integration/coherence_integration_test.rs`

| 测试场景 | 验证内容 |
|---------|---------|
| `test_metadata_sync_across_clients` | 客户端 A 创建文件，客户端 B 立即 lookup 应能看到 |
| `test_metadata_delete_across_clients` | 客户端 A 删除文件，客户端 B 后续操作应失败 |
| `test_metadata_update_across_clients` | 客户端 A 修改属性，客户端 B 后续 getattr 应返回新值 |

#### 2.3.3 故障测试

| 测试场景 | 验证内容 |
|---------|---------|
| `test_master_unavailable_during_metadata_op` | Master 不可用时，元数据操作应返回错误而非静默失败 |
| `test_network_partition_metadata_op` | 网络分区时，元数据操作应超时返回错误 |

---

## 3. Phase 1：服务器驱动的缓存失效

### 3.1 功能描述

实现服务器驱动的元数据缓存失效机制：
- Master 在元数据变更时主动推送失效通知
- FUSE 客户端订阅失效通知并立即更新本地缓存
- 基于 generation 号验证缓存有效性

### 3.2 数据结构变更

#### 3.2.1 Proto 定义变更

**文件**：`powerfs-proto/powerfs.proto`（需找到实际 proto 文件）

```protobuf
// Entry 消息新增 generation 字段
message Entry {
    string name = 1;
    string directory = 2;
    FuseAttributes attributes = 3;
    repeated Chunk chunks = 4;
    string hard_link_id = 5;
    int32 hard_link_counter = 6;
    map<string, string> extended = 7;
    uint64 content_size = 8;
    uint64 disk_size = 9;
    string ttl = 10;
    string symlink_target = 11;
    string owner = 12;
    uint64 generation = 13;  // 新增：代次号，每次变更递增
}

// MetadataNotification 消息新增 generation 字段
message MetadataNotification {
    enum EventType {
        CREATE = 0;
        UPDATE = 1;
        DELETE = 2;
        RENAME = 3;
    }
    EventType event_type = 1;
    string path = 2;
    Entry entry = 3;
    uint64 timestamp = 4;
    uint64 generation = 5;  // 新增：新的 generation 号
    string old_path = 6;    // 新增：RENAME 事件的旧路径
}
```

#### 3.2.2 DirectoryTree 变更

**文件**：`powerfs-master/src/directory_tree.rs`

```rust
struct DirectoryTree {
    db: DB,
    inode_counter: std::sync::atomic::AtomicU64,
    generation_counter: std::sync::atomic::AtomicU64,  // 新增：全局 generation 计数器
    notifier: Arc<broadcast::Sender<MetadataNotification>>,
    subscribers: std::sync::RwLock<HashSet<String>>,
}

// create_entry 时分配 generation
pub fn create_entry(&self, mut entry: Entry) -> Result<u64, rocksdb::Error> {
    let inode = self.allocate_inode();
    let generation = self.generation_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    
    if let Some(attrs) = &mut entry.attributes {
        attrs.ino = inode;
    }
    entry.generation = generation;  // 新增：设置 generation
    // ... 其余逻辑
}

// update_entry 时递增 generation
pub fn update_entry(&self, mut entry: Entry) -> Result<(), rocksdb::Error> {
    let generation = self.generation_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    entry.generation = generation;  // 新增：更新 generation
    // ... 其余逻辑
}

// publish_notification 时携带 generation
fn publish_notification(&self, event_type: EventType, path: &str, entry: Option<Entry>) {
    let generation = entry.as_ref().map(|e| e.generation).unwrap_or(0);
    let notification = MetadataNotification {
        event_type: event_type as i32,
        path: path.to_string(),
        entry,
        timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
        generation,  // 新增
        old_path: String::new(),
    };
    // 扩大广播通道容量
    let _ = self.notifier.send(notification);
}
```

#### 3.2.3 FUSE 客户端缓存结构变更

**文件**：`powerfs-fuse/src/cache.rs`

```rust
pub struct CachedEntry {
    pub inode: u64,
    pub generation: u64,  // 新增：保存 Master 返回的 generation
    pub name: String,
    pub directory: String,
    pub attrs: FuseAttributes,
    // ... 其余字段
}
```

### 3.3 FUSE 客户端失效处理

#### 3.3.1 订阅元数据变更

**文件**：`powerfs-fuse/src/fuser_fs.rs`

```rust
impl PowerFsFuse {
    pub async fn new(master_addr: &str, mount_point: &Path) -> Self {
        // ... 现有初始化逻辑
        
        // 启动元数据订阅任务
        let client_clone = self.client.clone();
        let cache_clone = self.cache.clone();
        tokio::spawn(async move {
            Self::subscribe_metadata_updates(client_clone, cache_clone).await;
        });
        
        Self { /* ... */ }
    }
    
    async fn subscribe_metadata_updates(client: Arc<MasterClient>, cache: Arc<Cache>) {
        loop {
            match client.subscribe_metadata().await {
                Ok(mut stream) => {
                    while let Some(notification) = stream.message().await.unwrap_or(None) {
                        cache.invalidate(&notification.path, notification.generation);
                        if notification.event_type == EventType::Rename as i32 {
                            if let Some(old_path) = notification.old_path {
                                cache.invalidate(&old_path, 0);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Metadata subscription failed: {}, reconnecting...", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
```

#### 3.3.2 缓存失效方法

**文件**：`powerfs-fuse/src/cache.rs`

```rust
impl Cache {
    pub fn invalidate(&self, path: &str, new_generation: u64) {
        let mut entries = self.entries.write().unwrap();
        entries.retain(|_, entry| {
            if entry.path == path {
                // 如果新的 generation 更大，则失效
                entry.generation < new_generation
            } else {
                true
            }
        });
    }
    
    pub fn invalidate_parent(&self, directory: &str) {
        let mut entries = self.entries.write().unwrap();
        entries.retain(|_, entry| {
            !entry.directory.starts_with(directory)
        });
    }
}
```

#### 3.3.3 Lookup 时验证 Generation

**文件**：`powerfs-fuse/src/fuser_fs.rs`

```rust
fn lookup(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
    let name_str = name.to_str().unwrap_or("");
    
    // 先检查缓存
    if let Some(entry) = self.lookup_in_cache(parent, name_str) {
        // 验证 generation 是否有效（通过 gRPC 验证）
        if self.is_cache_valid(&entry).await {
            let attr = self.create_file_attr(&entry);
            reply.entry(&TTL, &attr, 0);
            return;
        }
        // generation 过期，失效缓存
        self.cache.invalidate(&entry.path, entry.generation + 1);
    }
    
    // 从 Master 获取最新数据
    let result = self.client.lookup_directory_entry(parent_path, name_str).await;
    // ...
}

async fn is_cache_valid(&self, entry: &CachedEntry) -> bool {
    // 通过 gRPC 获取最新 generation 并比较
    match self.client.get_entry(&entry.path).await {
        Ok(Some(master_entry)) => entry.generation == master_entry.generation,
        _ => false,
    }
}
```

### 3.4 测试方案

#### 3.4.1 单元测试

**测试文件**：`powerfs-fuse/tests/coherence_phase1_test.rs`

| 测试用例 | 测试内容 |
|---------|---------|
| `test_cache_generation_validation` | 验证 generation 过期时缓存被正确失效 |
| `test_invalidate_by_path` | 验证按路径失效缓存 |
| `test_invalidate_by_parent` | 验证失效父目录下所有子项 |
| `test_metadata_notification_parsing` | 验证解析 gRPC 失效通知 |
| `test_subscribe_reconnect` | 验证订阅断开后自动重连 |

**测试文件**：`powerfs-master/tests/directory_tree_test.rs`

| 测试用例 | 测试内容 |
|---------|---------|
| `test_generation_counter_increment` | 验证 create/update 时 generation 递增 |
| `test_notification_contains_generation` | 验证通知包含正确的 generation |
| `test_broadcast_channel_capacity` | 验证广播通道不会丢失事件 |

#### 3.4.2 集成测试

**测试文件**：`powerfs-fuse/tests/integration/cross_client_consistency_test.rs`

| 测试场景 | 验证内容 |
|---------|---------|
| `test_cross_client_create_notification` | 客户端 A 创建文件，客户端 B 立即收到失效通知 |
| `test_cross_client_delete_notification` | 客户端 A 删除文件，客户端 B 立即收到失效通知 |
| `test_cross_client_update_notification` | 客户端 A 修改文件，客户端 B 立即收到失效通知 |
| `test_cross_client_rename_notification` | 客户端 A 重命名文件，客户端 B 收到旧路径和新路径失效通知 |
| `test_cache_staleness_detection` | 验证过期缓存被正确检测并刷新 |

#### 3.4.3 故障测试

| 测试场景 | 验证内容 |
|---------|---------|
| `test_notification_channel_overflow` | 高频率元数据变更时验证通知不丢失 |
| `test_subscription_disconnect_reconnect` | 网络断开后验证订阅自动恢复 |
| `test_master_failover_notification` | Master 切换后验证订阅重新建立 |
| `test_partial_notification_delivery` | 部分客户端离线时验证系统稳定性 |

---

## 4. Phase 2：租约机制

### 4.1 功能描述

实现文件级租约机制，为热点文件提供更强的一致性保证：
- FUSE 客户端打开文件时申请租约
- 持有租约期间，Master 不向其他客户端推送该文件的失效通知
- 租约到期或文件关闭时释放租约
- 租约期间的写入同步到 Master

### 4.2 数据结构变更

#### 4.2.1 Proto 定义

```protobuf
// 新增租约相关消息
message LeaseRequest {
    string path = 1;
    uint64 duration_ms = 2;  // 租约时长，默认 30s
}

message LeaseResponse {
    bool granted = 1;
    uint64 lease_id = 2;
    uint64 expires_at = 3;  // 过期时间戳（纳秒）
    uint64 generation = 4;  // 当前文件 generation
}

message LeaseRenewRequest {
    uint64 lease_id = 1;
    uint64 duration_ms = 2;
}

message LeaseRenewResponse {
    bool success = 1;
    uint64 expires_at = 2;
}

message LeaseReleaseRequest {
    uint64 lease_id = 1;
}

message LeaseReleaseResponse {
    bool success = 1;
}
```

#### 4.2.2 Master 端租约管理器

**文件**：`powerfs-master/src/lease_manager.rs`（新建）

```rust
pub struct LeaseManager {
    leases: RwLock<HashMap<u64, Lease>>,  // lease_id -> Lease
    path_to_lease: RwLock<HashMap<String, u64>>,  // path -> lease_id
    lease_id_counter: std::sync::atomic::AtomicU64,
}

pub struct Lease {
    id: u64,
    path: String,
    client_id: String,
    expires_at: u64,  // 纳秒时间戳
    generation: u64,
}

impl LeaseManager {
    pub fn new() -> Self {
        LeaseManager {
            leases: RwLock::new(HashMap::new()),
            path_to_lease: RwLock::new(HashMap::new()),
            lease_id_counter: std::sync::atomic::AtomicU64::new(1),
        }
    }
    
    pub fn grant(&self, path: &str, client_id: &str, duration_ms: u64) -> Option<Lease> {
        let mut path_to_lease = self.path_to_lease.write().unwrap();
        
        // 检查是否已有租约
        if let Some(existing_lease_id) = path_to_lease.get(path) {
            return None;  // 租约已被占用
        }
        
        let lease_id = self.lease_id_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let expires_at = now + duration_ms * 1_000_000;
        
        let lease = Lease {
            id: lease_id,
            path: path.to_string(),
            client_id: client_id.to_string(),
            expires_at,
            generation: 0,  // 需要从 DirectoryTree 获取
        };
        
        let mut leases = self.leases.write().unwrap();
        leases.insert(lease_id, lease.clone());
        path_to_lease.insert(path.to_string(), lease_id);
        
        Some(lease)
    }
    
    pub fn renew(&self, lease_id: u64, duration_ms: u64) -> bool {
        let mut leases = self.leases.write().unwrap();
        if let Some(lease) = leases.get_mut(&lease_id) {
            let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
            lease.expires_at = now + duration_ms * 1_000_000;
            true
        } else {
            false
        }
    }
    
    pub fn release(&self, lease_id: u64) -> bool {
        let mut leases = self.leases.write().unwrap();
        if let Some(lease) = leases.remove(&lease_id) {
            let mut path_to_lease = self.path_to_lease.write().unwrap();
            path_to_lease.remove(&lease.path);
            true
        } else {
            false
        }
    }
    
    pub fn is_leased(&self, path: &str) -> bool {
        let path_to_lease = self.path_to_lease.read().unwrap();
        path_to_lease.contains_key(path)
    }
    
    pub fn get_lease(&self, path: &str) -> Option<Lease> {
        let path_to_lease = self.path_to_lease.read().unwrap();
        if let Some(&lease_id) = path_to_lease.get(path) {
            let leases = self.leases.read().unwrap();
            leases.get(&lease_id).cloned()
        } else {
            None
        }
    }
    
    pub fn cleanup_expired(&self) {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let mut leases = self.leases.write().unwrap();
        let mut path_to_lease = self.path_to_lease.write().unwrap();
        
        let expired: Vec<u64> = leases
            .iter()
            .filter(|(_, lease)| lease.expires_at < now)
            .map(|(id, _)| *id)
            .collect();
        
        for lease_id in expired {
            if let Some(lease) = leases.remove(&lease_id) {
                path_to_lease.remove(&lease.path);
            }
        }
    }
}
```

#### 4.2.3 DirectoryTree 集成租约检查

**文件**：`powerfs-master/src/directory_tree.rs`

```rust
pub struct DirectoryTree {
    db: DB,
    inode_counter: std::sync::atomic::AtomicU64,
    generation_counter: std::sync::atomic::AtomicU64,
    notifier: Arc<broadcast::Sender<MetadataNotification>>,
    subscribers: std::sync::RwLock<HashSet<String>>,
    lease_manager: Arc<LeaseManager>,  // 新增
}

// 在 publish_notification 时检查租约
fn publish_notification(&self, event_type: EventType, path: &str, entry: Option<Entry>) {
    // 如果文件被租约占用，不发送失效通知
    if self.lease_manager.is_leased(path) {
        return;
    }
    
    let generation = entry.as_ref().map(|e| e.generation).unwrap_or(0);
    let notification = MetadataNotification {
        event_type: event_type as i32,
        path: path.to_string(),
        entry,
        timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
        generation,
        old_path: String::new(),
    };
    let _ = self.notifier.send(notification);
}
```

#### 4.2.4 FUSE 客户端租约管理

**文件**：`powerfs-fuse/src/lease_client.rs`（新建）

```rust
pub struct LeaseClient {
    master_client: Arc<MasterClient>,
    active_leases: RwLock<HashMap<u64, LeaseInfo>>,  // inode -> LeaseInfo
}

pub struct LeaseInfo {
    lease_id: u64,
    path: String,
    expires_at: u64,
    generation: u64,
}

impl LeaseClient {
    pub fn new(master_client: Arc<MasterClient>) -> Self {
        LeaseClient {
            master_client,
            active_leases: RwLock::new(HashMap::new()),
        }
    }
    
    pub async fn acquire(&self, inode: u64, path: &str) -> Result<LeaseInfo> {
        let response = self.master_client.lease(path, 30_000).await?;
        
        if !response.granted {
            return Err(PowerFsError::LeaseDenied);
        }
        
        let lease_info = LeaseInfo {
            lease_id: response.lease_id,
            path: path.to_string(),
            expires_at: response.expires_at,
            generation: response.generation,
        };
        
        let mut active_leases = self.active_leases.write().unwrap();
        active_leases.insert(inode, lease_info.clone());
        
        // 启动续租任务
        self.spawn_renewal(inode, lease_info.clone());
        
        Ok(lease_info)
    }
    
    pub async fn release(&self, inode: u64) {
        let mut active_leases = self.active_leases.write().unwrap();
        if let Some(lease_info) = active_leases.remove(&inode) {
            let _ = self.master_client.release_lease(lease_info.lease_id).await;
        }
    }
    
    fn spawn_renewal(&self, inode: u64, lease_info: LeaseInfo) {
        let master_client = self.master_client.clone();
        let active_leases = self.active_leases.clone();
        
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(20)).await;  // 提前 10s 续租
                
                let mut leases = active_leases.write().unwrap();
                if !leases.contains_key(&inode) {
                    break;
                }
                
                match master_client.renew_lease(lease_info.lease_id, 30_000).await {
                    Ok(response) => {
                        if let Some(lease) = leases.get_mut(&inode) {
                            lease.expires_at = response.expires_at;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to renew lease {}: {}", lease_info.lease_id, e);
                        break;
                    }
                }
            }
        });
    }
    
    pub fn get_lease(&self, inode: u64) -> Option<LeaseInfo> {
        let active_leases = self.active_leases.read().unwrap();
        active_leases.get(&inode).cloned()
    }
}
```

#### 4.2.5 FUSE File Operations 集成租约

**文件**：`powerfs-fuse/src/fuser_fs.rs`

```rust
fn open(&mut self, req: &Request<'_>, inode: u64, flags: u32, reply: ReplyOpen) {
    let path = self.get_path(inode);
    if let Some(path_str) = path {
        // 申请租约
        match self.lease_client.acquire(inode, &path_str).await {
            Ok(_) => debug!("Lease acquired for {}", path_str),
            Err(e) => warn!("Failed to acquire lease for {}: {}", path_str, e),
        }
    }
    // ... 其余逻辑
}

fn release(&mut self, _req: &Request<'_>, inode: u64, _fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) {
    // 释放租约
    self.lease_client.release(inode).await;
    // ... 其余逻辑
}
```

### 4.3 测试方案

#### 4.3.1 单元测试

**测试文件**：`powerfs-master/tests/lease_manager_test.rs`

| 测试用例 | 测试内容 |
|---------|---------|
| `test_lease_grant` | 验证租约授予 |
| `test_lease_deny_when_leased` | 验证文件已被租约占用时拒绝新租约 |
| `test_lease_renew` | 验证租约续租 |
| `test_lease_release` | 验证租约释放 |
| `test_lease_expiration` | 验证租约过期自动清理 |
| `test_lease_cleanup` | 验证过期租约清理 |

**测试文件**：`powerfs-fuse/tests/lease_client_test.rs`

| 测试用例 | 测试内容 |
|---------|---------|
| `test_lease_acquire_release` | 验证租约获取和释放流程 |
| `test_lease_renewal` | 验证租约自动续租 |
| `test_lease_expiration_handled` | 验证租约过期后的处理 |

#### 4.3.2 集成测试

**测试文件**：`powerfs-fuse/tests/integration/lease_integration_test.rs`

| 测试场景 | 验证内容 |
|---------|---------|
| `test_single_client_lease` | 单个客户端打开/关闭文件时租约生命周期 |
| `test_multiple_clients_same_file` | 客户端 A 持有租约时，客户端 B 无法获取租约 |
| `test_lease_protected_write` | 客户端 A 持有租约时写入，其他客户端看不到变更 |
| `test_lease_release_notification` | 租约释放后，其他客户端收到失效通知 |
| `test_lease_expiration_notification` | 租约过期后，其他客户端收到失效通知 |
| `test_concurrent_lease_requests` | 多个客户端同时请求同一文件租约 |

#### 4.3.3 故障测试

| 测试场景 | 验证内容 |
|---------|---------|
| `test_lease_client_crash` | 客户端崩溃后租约过期自动释放 |
| `test_lease_master_failover` | Master 切换后租约状态一致性 |
| `test_lease_network_partition` | 网络分区时租约续租失败处理 |
| `test_lease_denial_of_service` | 恶意客户端占用租约的防护 |

---

## 5. Phase 3：作业级强一致性（可选）

### 5.1 功能描述

为 AI/LLM 训练场景提供作业级强一致性：
- 作业内所有客户端共享一个一致性域
- 作业内文件操作保证顺序一致性
- 作业结束时批量失效所有客户端缓存

### 5.2 数据结构变更

```protobuf
message JobContext {
    string job_id = 1;
    string job_name = 2;
    repeated string client_ids = 3;
    uint64 start_time = 4;
    uint64 end_time = 5;
    bool is_active = 6;
}

message JobRegistrationRequest {
    string job_id = 1;
    string job_name = 2;
    string client_id = 3;
}

message JobRegistrationResponse {
    bool success = 1;
}

message JobDeregistrationRequest {
    string job_id = 1;
    string client_id = 3;
}

message JobDeregistrationResponse {
    bool success = 1;
}

message JobCompletionRequest {
    string job_id = 1;
}

message JobCompletionResponse {
    bool success = 1;
}
```

### 5.3 实现要点

1. **作业注册**：客户端启动时注册到作业
2. **作业级租约**：同一作业内的客户端共享租约
3. **作业结束失效**：作业完成时批量失效所有相关缓存

### 5.4 测试方案

#### 5.4.1 集成测试

| 测试场景 | 验证内容 |
|---------|---------|
| `test_job_registration` | 客户端注册到作业 |
| `test_job_shared_lease` | 同一作业内多个客户端共享文件租约 |
| `test_job_cross_client_consistency` | 作业内跨客户端一致性 |
| `test_job_completion_invalidation` | 作业结束时批量失效缓存 |

---

## 6. 代码质量检查

每个阶段完成后必须执行：

```bash
# 代码格式化检查
cargo fmt --check --all

# 代码静态检查
cargo clippy --all -- -D warnings

# 编译检查
cargo build --all

# 单元测试
cargo test --all --tests

# 集成测试
cargo test --all --features integration
```

---

## 7. 依赖关系

```
Phase 0 (Bug Fixes)
    ↓
Phase 1 (Server-Driven Invalidation)
    ├── Proto 定义变更
    ├── DirectoryTree generation 支持
    ├── FUSE 客户端缓存结构变更
    └── gRPC 订阅失效
    ↓
Phase 2 (Lease Mechanism)
    ├── Proto 租约消息定义
    ├── Master 租约管理器
    ├── DirectoryTree 租约检查
    └── FUSE 客户端租约管理
    ↓
Phase 3 (Job-Level Consistency) [可选]
    ├── Proto 作业消息定义
    ├── Master 作业管理器
    └── FUSE 客户端作业上下文
```

---

## 8. 风险评估

| 风险 | 级别 | 缓解措施 |
|------|------|---------|
| 广播通道溢出导致通知丢失 | 高 | 增大通道容量（10000+），添加背压处理 |
| 租约过期导致数据不一致 | 高 | 提前续租（剩余 1/3 时间），超时检测 |
| 订阅重连期间数据陈旧 | 中 | 使用 generation 验证，定期刷新 |
| 租约占用导致饥饿 | 中 | 设置最大租约时长，强制释放机制 |
| 元数据操作同步化影响性能 | 低 | 批量操作优化，异步写入优化 |

---

## 9. 实施进度

### 9.1 已完成阶段

| 阶段 | 功能 | 状态 | 完成时间 |
|------|------|------|----------|
| Phase 0 | 同步提交 + 错误回滚 | ✅ 已完成 | 2026-07-09 |
| Phase 1 | 服务器驱动缓存失效 | ✅ 已完成 | 2026-07-09 |
| Phase 2 | 租约机制 | ✅ 已完成 | 2026-07-10 |
| Phase 3 | 作业级强一致性 | ✅ 已完成 | 2026-07-10 |

### 9.2 测试覆盖

| 测试类型 | 文件 | 测试用例数 | 状态 |
|----------|------|------------|------|
| Phase 0 单元测试 | `powerfs-fuse/tests/coherence_phase0_test.rs` | 10 | ✅ 全部通过 |
| Phase 1 单元测试 | `powerfs-fuse/tests/coherence_phase1_test.rs` | 10 | ✅ 全部通过 |
| Phase 2 单元测试 | `powerfs-master/tests/coherence_phase2_test.rs` | 12 | ✅ 全部通过 |
| Phase 3 单元测试 | `powerfs-master/tests/coherence_phase3_test.rs` | 12 | ✅ 全部通过 |
| Phase 0 E2E测试 | `scripts/test_coherence_phase0.sh` | 12 | ✅ 已就绪 |
| Phase 1 E2E测试 | `scripts/test_coherence_phase1.sh` | 10 | ✅ 已就绪 |
| Phase 2 E2E测试 | `scripts/test_coherence_phase2.sh` | 10 | ✅ 已就绪 |
| Phase 3 E2E测试 | `scripts/test_coherence_phase3.sh` | 10 | ✅ 已就绪 |
| Docker E2E测试 | `scripts/run_coherence_docker.sh` | 15 | ✅ 已就绪 |

### 9.3 Bug修复记录

| Bug描述 | 影响阶段 | 修复方式 |
|---------|----------|----------|
| 租约异步获取导致保护窗口失效 | Phase 2 | 移除`tokio::spawn`，改同步调用 |
| 租约阻止自身元数据通知 | Phase 2 | 移除Master端`has_active_lease`检查 |
| 同inode多次open租约覆盖 | Phase 2 | `HashMap<u64, Vec<String>>` |
| 过期租约清理TOCTOU竞态 | Phase 2 | 原子化收集+删除过期租约 |
| 缓存失效死锁 | Phase 1 | 缩小`path_map`读锁作用域 |

### 9.4 CI/CD集成

- ✅ 单元测试已集成到 `.github/workflows/rust.yml`
- ✅ 代码质量检查（fmt/clippy）已集成
- ✅ Docker环境测试脚本已就绪

### 9.5 后续工作

| 工作项 | 优先级 | 说明 |
|--------|--------|------|
| 性能基准测试 | P1 | 评估coherence机制对性能的影响 |
| 故障恢复测试 | P1 | Master故障切换、网络分区场景 |
| 长时间稳定性测试 | P2 | 内存泄漏、租约累积检测 |
| 压力测试 | P2 | 高并发下的缓存一致性验证 |