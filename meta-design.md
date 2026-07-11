# PowerFS 元数据与目录机制设计文档

## 1. 概述

PowerFS 采用分层元数据管理架构，Master 节点负责全局元数据管理，FUSE 客户端维护本地缓存以降低延迟。文档详细描述目录树结构、元数据存储、缓存机制、租约机制及相关操作流程。

---

## 2. Master 端目录树结构

### 2.1 核心数据结构

```rust
pub struct DirectoryTree {
    db: DB,                              // RocksDB 存储
    inode_counter: AtomicU64,            // inode 分配计数器
    generation_counter: AtomicU64,       // generation 分配计数器
    epoch: AtomicU64,                    // Epoch 号（Master重启时递增）
    notifier: Arc<broadcast::Sender<MetadataNotification>>,  // 通知广播器
    subscribers: RwLock<HashSet<String>>,                    // 订阅者列表
    leases: RwLock<HashMap<String, Lease>>,                 // lease_id -> Lease
    path_lease_map: RwLock<HashMap<String, HashSet<String>>>, // path -> lease_ids
    jobs: RwLock<HashMap<String, JobInfo>>,                 // job_id -> JobInfo
    current_job_id: RwLock<Option<String>>,                 // 当前作业ID
}
```

### 2.2 Lease 结构

```rust
pub struct Lease {
    pub lease_id: String,     // 租约ID（UUID）
    pub path: String,         // 租约对应的路径
    pub client_id: String,    // 持有租约的客户端ID
    pub expires_at: Instant,  // 过期时间
    pub epoch: u64,           // 租约获取时的epoch号
}
```

### 2.3 JobInfo 结构

```rust
pub struct JobInfo {
    pub job_id: String,             // 作业ID
    pub job_name: String,           // 作业名称
    pub client_ids: HashSet<String>,// 作业内客户端列表
    pub start_time: u64,            // 开始时间
    pub end_time: u64,              // 结束时间
    pub is_active: bool,            // 是否活跃
}
```

---

## 3. 持久化存储

### 3.1 RocksDB Key-Value 设计

| Key | Value | 说明 |
|-----|-------|------|
| `/<name>` | Entry (protobuf) | 根目录下的文件/目录 |
| `/<dir>/<name>` | Entry (protobuf) | 子目录下的文件/目录 |
| `inode_counter` | u64 | inode 计数器持久化 |
| `generation_counter` | u64 | generation 计数器持久化 |
| `epoch` | u64 | Epoch 号持久化 |

### 3.2 Entry Protobuf 定义

```protobuf
message Entry {
    string name = 1;              // 名称
    string directory = 2;         // 父目录路径
    FuseAttributes attributes = 3;// 文件属性
    repeated Chunk chunks = 4;    // 数据块信息
    string hard_link_id = 5;      // 硬链接ID
    int32 hard_link_counter = 6;  // 硬链接计数
    map<string, string> extended = 7; // 扩展属性
    uint64 content_size = 8;      // 内容大小
    uint64 disk_size = 9;         // 磁盘大小
    string ttl = 10;              // TTL
    string symlink_target = 11;   // 符号链接目标
    string owner = 12;            // 所有者
    uint64 generation = 13;       // 代次号（每次变更递增）
}
```

### 3.3 FuseAttributes 定义

```protobuf
message FuseAttributes {
    uint64 ino = 1;       // inode号
    uint32 mode = 2;      // 权限模式（含文件类型位）
    uint32 nlink = 3;     // 链接数
    uint32 uid = 4;       // 用户ID
    uint32 gid = 5;       // 组ID
    uint64 rdev = 6;      // 设备号
    uint64 size = 7;      // 文件大小
    uint64 blksize = 8;   // 块大小
    uint64 blocks = 9;    // 块数
    uint64 atime = 10;    // 访问时间
    uint64 mtime = 11;    // 修改时间
    uint64 ctime = 12;    // 变更时间
    uint64 crtime = 13;   // 创建时间
    uint32 perm = 14;     // 权限
}
```

---

## 4. Epoch 机制

### 4.1 设计目的

Epoch 机制用于解决 Master 重启时的**租约幻觉**问题：客户端持有的租约在 Master 重启后失效，但客户端无法感知。通过 Epoch 号递增，客户端可以检测到 Master 重启并主动清理过期租约。

### 4.2 工作流程

```
Master 启动:
  1. 从 RocksDB 读取当前 epoch 值
  2. 递增 epoch（epoch = current + 1）
  3. 将新 epoch 写回 RocksDB
  4. 所有新租约携带当前 epoch

客户端检测:
  1. 获取租约时记录 epoch
  2. 定期检查或收到通知时对比 epoch
  3. 发现 epoch 变化时清理所有本地租约和缓存
```

### 4.3 关键代码

```rust
fn load_and_increment_epoch(db: &DB) -> u64 {
    let current = if let Ok(Some(val)) = db.get(b"epoch") {
        if let Ok(s) = String::from_utf8(val) {
            s.parse::<u64>().unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };
    let new_epoch = current + 1;
    let _ = db.put(b"epoch", new_epoch.to_string().as_bytes());
    new_epoch
}
```

---

## 5. Generation 机制

### 5.1 设计目的

Generation 机制用于缓存失效验证，每次元数据变更时递增，客户端通过对比 generation 判断缓存是否过期。

### 5.2 工作流程

```
元数据变更（create/update）:
  1. 分配新 generation（generation_counter++）
  2. 将 generation 写入 Entry
  3. 持久化 generation_counter
  4. 发送通知携带新 generation

客户端缓存验证:
  1. Lookup 时检查缓存 entry 的 generation
  2. 对比最新 generation（来自通知或主动获取）
  3. 缓存 generation < 最新 generation 时失效缓存
```

### 5.3 关键代码

```rust
fn allocate_generation(&self) -> u64 {
    let generation = self.generation_counter.fetch_add(1, SeqCst);
    let _ = self.db.put(b"generation_counter", generation.to_string().as_bytes());
    generation
}
```

---

## 6. 通知机制

### 6.1 MetadataNotification 定义

```protobuf
message MetadataNotification {
    enum EventType {
        CREATE = 0;
        UPDATE = 1;
        DELETE = 2;
        RENAME = 3;
        JOB_COMPLETE = 4;
    }
    EventType event_type = 1;
    string path = 2;
    Entry entry = 3;
    uint64 timestamp = 4;
    uint64 generation = 5;
    string old_path = 6;
    string source_client_id = 7;  // 自通知抑制
    string job_id = 8;            // 作业级租约共享
    uint64 epoch = 9;             // Epoch 号
}
```

### 6.2 事件类型说明

| 事件类型 | 触发时机 | 作用 |
|---------|---------|------|
| `CREATE` | 创建文件/目录 | 通知其他客户端新条目 |
| `UPDATE` | 更新文件属性/内容 | 通知其他客户端条目变更 |
| `DELETE` | 删除文件/目录 | 通知其他客户端条目删除 |
| `RENAME` | 重命名文件/目录 | 通知其他客户端路径变更 |
| `JOB_COMPLETE` | 作业结束 | 批量失效所有客户端缓存 |

### 6.3 自通知抑制

为避免客户端自身操作触发的通知导致缓存无效化，通知携带 `source_client_id`，客户端收到通知时检查是否为自身发送，若是则跳过处理。

### 6.4 通知广播流程

```rust
fn publish_notification(&self, event_type: EventType, path: &str, entry: Option<Entry>, client_id: &str) {
    let generation = entry.as_ref().map(|e| e.generation).unwrap_or(0);
    let notification = MetadataNotification {
        event_type: event_type as i32,
        path: path.to_string(),
        entry,
        timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
        generation,
        old_path: String::new(),
        source_client_id: client_id.to_string(),
        job_id: String::new(),
        epoch: self.get_epoch(),
    };
    let _ = self.notifier.send(notification);
}
```

---

## 7. 租约机制

### 7.1 设计目的

租约机制为热点文件提供**写保护**，持有租约期间，其他客户端的写入操作会被阻塞，保证数据一致性。

### 7.2 租约生命周期

```
获取租约（open）:
  1. FUSE 客户端调用 acquire_lease(path, client_id, duration_ms)
  2. Master 检查路径是否已有租约
  3. 分配新租约，记录到 leases 和 path_lease_map
  4. 返回 lease_id 和当前 epoch

租约续租（自动）:
  1. 客户端启动续租任务
  2. 每 5 秒检查一次租约状态
  3. 剩余时间 < 1/3 时调用 renew_lease(lease_id)
  4. 更新 expires_at

释放租约（release）:
  1. FUSE 客户端调用 release_lease(lease_id)
  2. Master 从 leases 和 path_lease_map 移除租约
  3. 发送失效通知给其他客户端

租约过期:
  1. Master 定期清理过期租约
  2. 从数据结构中移除过期租约
  3. 发送失效通知
```

### 7.3 父目录租约

目录内容操作（create/mkdir/unlink/rmdir/rename）需要获取**父目录租约**，防止并发修改冲突。

```
操作流程:
  1. 解析目标路径的父目录路径
  2. 获取父目录租约
  3. 执行操作
  4. 操作完成后释放租约（或保留一段时间）
```

### 7.4 作业级租约共享

同一作业内的多个客户端共享租约，通知携带 `job_id`，同 job 的通知不跳过 invalidation。

---

## 8. FUSE 客户端缓存机制

### 8.1 缓存结构

```rust
pub struct MetadataCache {
    inode_cache: RwLock<LruCache<u64, CachedEntry>>, // inode -> 缓存条目
    path_map: RwLock<HashMap<String, u64>>,          // path -> inode
    dir_cache: RwLock<HashMap<u64, DirCacheEntry>>,  // parent inode -> 目录列表
    next_inode: AtomicU64,                           // 下一个可用 inode
    dir_cache_ttl: Duration,                         // 目录缓存 TTL（5秒）
    path_generations: RwLock<HashMap<String, u64>>,  // path -> 最新 generation
}
```

### 8.2 CachedEntry 结构

```rust
pub struct CachedEntry {
    pub inode: u64,
    pub parent: u64,
    pub name: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    pub symlink_target: Option<String>,
    pub nlink: u32,
    pub fid: Option<Fid>,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: i64,
    pub mtime: i64,
    pub ctime: i64,
    pub xattrs: HashMap<String, Vec<u8>>,
    pub chunks: Vec<CachedFileChunk>,
    pub hard_link_id: String,
    pub hard_link_counter: u32,
    pub content_size: u64,
    pub disk_size: u64,
    pub generation: u64,  // 用于缓存失效验证
}
```

### 8.3 路径计算

缓存条目插入时，通过遍历父节点递归构建完整路径：

```rust
pub fn insert(&self, entry: CachedEntry) {
    let inode = entry.inode;
    let path = if inode == 1 {
        String::from("/")
    } else {
        let mut parts = Vec::new();
        parts.push(entry.name.clone());
        let mut current = entry.parent;
        while current != 1 {
            if let Some(e) = self.get_inode(current) {
                parts.push(e.name.clone());
                current = e.parent;
            } else {
                break;
            }
        }
        parts.reverse();
        let mut path = String::from("/");
        for part in parts {
            if path != "/" {
                path.push('/');
            }
            path.push_str(&part);
        }
        path
    };
    // 插入 path_map 和 inode_cache
}
```

### 8.4 缓存失效

#### 8.4.1 通知驱动失效

客户端订阅元数据变更通知，收到通知后失效对应缓存：

```rust
async fn handle_metadata_notification(&self, notification: MetadataNotification) {
    // 自通知抑制
    if notification.source_client_id == self.client_id {
        return;
    }
    
    // 作业级租约共享
    if let Some(job_id) = notification.job_id {
        if self.current_job_id == job_id {
            // 同作业不跳过
        } else {
            return;
        }
    }
    
    // 失效元数据缓存
    self.cache.invalidate_path(&notification.path);
    
    // 同步失效 chunk 缓存
    if let Some(inode) = self.cache.get_path(&notification.path) {
        self.chunk_cache.remove_inode_chunks(inode);
    }
    
    // JOB_COMPLETE 事件：清空所有缓存
    if notification.event_type == EventType::JobComplete {
        self.cache.clear_all();
        self.chunk_cache.clear();
    }
}
```

#### 8.4.2 Generation 验证失效

Lookup 时验证缓存 generation：

```rust
fn lookup(&mut self, parent: u64, name: &OsStr, reply: ReplyEntry) {
    let name_str = name.to_str().unwrap_or("");
    let parent_path = self.cache.inode_to_path(parent).unwrap_or_else(|| "/".to_string());
    let lookup_path = if parent_path == "/" {
        format!("/{}", name_str)
    } else {
        format!("{}/{}", parent_path, name_str)
    };
    
    if let Some(entry) = self.lookup_in_cache(parent, name_str) {
        // 验证 generation
        let is_stale = self.cache.get_path_generation(&lookup_path)
            .is_some_and(|latest_gen| entry.generation < latest_gen);
        if !is_stale {
            let attr = self.create_file_attr(&entry);
            reply.entry(&TTL, &attr, 0);
            return;
        }
        // generation 过期，失效缓存并从 Master 获取
    }
    
    // 从 Master 获取最新数据
    match self.client.get_entry(&lookup_path) {
        Ok(Some(entry)) => {
            let cached = self.entry_to_cached(parent, &entry);
            self.cache.insert(cached.clone());
            let attr = self.create_file_attr(&cached);
            reply.entry(&TTL, &attr, 0);
        }
        Ok(None) => reply.error(libc::ENOENT),
        Err(e) => {
            warn!("lookup entry failed: {}", e);
            reply.error(libc::ENOENT);
        }
    }
}
```

---

## 9. 目录操作

### 9.1 创建目录 (mkdir)

```
流程:
  1. 检查缓存中是否已存在同名条目
  2. 分配本地 inode
  3. 创建 CachedEntry 并插入缓存
  4. 获取父目录租约
  5. 构建 FilerEntry，调用 Master create_entry
  6. 使用 Master 返回的 inode 更新本地缓存
  7. 返回成功
```

### 9.2 删除目录 (rmdir / rm -rf)

#### 9.2.1 Master 端递归删除

```rust
pub fn delete_entry(&self, path: &str, client_id: &str) -> Result<bool, rocksdb::Error> {
    let exists = self.db.get(path.as_bytes())?.is_some();
    if exists {
        let entry_bytes = self.db.get(path.as_bytes())?;
        if let Some(bytes) = entry_bytes {
            let decode_result: Result<Entry, _> = prost::Message::decode(bytes.as_ref());
            if let Ok(entry) = decode_result {
                if let Some(attr) = entry.attributes {
                    if (attr.mode & 0o40000) != 0 {  // 是目录
                        let mut to_delete = Vec::new();
                        let mut stack = vec![path.to_string()];
                        
                        while let Some(dir_path) = stack.pop() {
                            let prefix = Self::path_prefix(&dir_path);
                            let mut iter = self.db.iterator(IteratorMode::From(&prefix, Forward));
                            while let Some(Ok((key, value))) = iter.next() {
                                if !key.starts_with(&prefix) {
                                    break;
                                }
                                let child_path = String::from_utf8_lossy(&key).to_string();
                                if child_path != dir_path {
                                    to_delete.push(child_path.clone());
                                    let child_decode: Result<Entry, _> = prost::Message::decode(value.as_ref());
                                    if let Ok(child_entry) = child_decode {
                                        if let Some(child_attr) = child_entry.attributes {
                                            if (child_attr.mode & 0o40000) != 0 {
                                                stack.push(child_path);  // 递归处理子目录
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        for child_path in to_delete {
                            self.db.delete(child_path.as_bytes())?;
                            self.publish_notification(EventType::Delete, &child_path, None, client_id);
                        }
                    }
                }
            }
        }
        
        self.db.delete(path.as_bytes())?;
        self.publish_notification(EventType::Delete, path, None, client_id);
        Ok(true)
    } else {
        Ok(false)
    }
}
```

#### 9.2.2 客户端递归缓存清理

```rust
fn remove_inode_recursive(&self, inode: u64) {
    let children = self.cache.list_children(inode);
    for (child_inode, _, _) in children {
        self.remove_inode_recursive(child_inode);
    }
    self.cache.remove(inode);
    self.chunk_cache.remove_inode_chunks(inode);
}
```

### 9.3 重命名 (rename)

```
流程:
  1. 获取源路径和目标路径的父目录租约
  2. 检查目标是否存在
  3. 调用 Master rename_entry
  4. 失效源路径和目标路径的缓存
  5. 更新本地缓存中的路径映射
```

### 9.4 创建文件 (create)

```
流程:
  1. 获取父目录租约
  2. 分配本地 inode
  3. 注册租约到 HashMap（防止 CREATE 通知误失效）
  4. 创建 CachedEntry 并插入缓存
  5. 构建 FilerEntry，调用 Master create_entry
  6. 使用 Master 返回的 inode 更新本地缓存
  7. 返回成功
```

---

## 10. Chunk 缓存机制

### 10.1 数据结构

```rust
pub struct ChunkCache {
    cache: RwLock<HashMap<u64, HashMap<u64, CachedFileChunk>>>, // inode -> offset -> chunk
    dirty_chunks: RwLock<HashMap<u64, Vec<CachedFileChunk>>>,   // inode -> 脏 chunks
    chunk_size: u64,                                            // 默认 64MB
}
```

### 10.2 缓存失效

元数据缓存失效时同步清除对应 inode 的 chunk 缓存：

```rust
pub fn remove_inode_chunks(&self, inode: u64) {
    let mut cache = self.cache.write().unwrap();
    cache.remove(&inode);
    let mut dirty = self.dirty_chunks.write().unwrap();
    dirty.remove(&inode);
}
```

---

## 11. 一致性保障

### 11.1 强一致性操作

| 操作 | 一致性级别 | 保障机制 |
|------|-----------|---------|
| mkdir | 强一致 | 同步提交 + 错误回滚 |
| create | 强一致 | 同步提交 + 错误回滚 |
| unlink | 强一致 | 同步提交 + 错误回滚 |
| rename | 强一致 | 同步提交 + 错误回滚 |
| setattr | 强一致 | 同步提交 + 错误回滚 |

### 11.2 最终一致性操作

| 操作 | 一致性级别 | 保障机制 |
|------|-----------|---------|
| 跨客户端缓存更新 | 最终一致 | 服务器驱动推送 |
| 文件读写 | 租约保护 | 租约机制 |

### 11.3 错误回滚

元数据操作失败时回滚本地缓存：

```rust
match self.client.create_entry(filer_entry) {
    Ok(_) => {
        let attr = self.create_file_attr(&entry);
        reply.entry(&TTL, &attr, 0);
    }
    Err(e) => {
        self.cache.remove(inode);
        error!("Failed to create directory: {}", e);
        reply.error(libc::EIO);
    }
}
```

---

## 12. 性能优化

### 12.1 缓存优化

- **LRU 缓存**: inode_cache 使用 LRU 策略，容量 10000
- **目录缓存 TTL**: dir_cache TTL 5秒，减少目录列表请求
- **路径映射**: path_map 提供 O(1) 路径到 inode 查找

### 12.2 并发优化

- **读写锁**: 缓存使用 RwLock，支持并发读
- **原子计数器**: inode/generation/epoch 使用 AtomicU64
- **广播通道**: 通知使用 tokio broadcast channel，容量 10000

### 12.3 租约优化

- **自动续租**: 租约到期前 1/3 时间自动续租
- **批量清理**: 过期租约批量清理，减少锁竞争

---

## 13. 故障恢复

### 13.1 Master 重启

1. Epoch 递增
2. 所有客户端检测到 epoch 变化
3. 客户端清理本地租约和缓存
4. 重新获取租约和元数据

### 13.2 客户端崩溃

1. 租约过期自动释放
2. Master 定期清理过期租约
3. 其他客户端收到失效通知

### 13.3 网络分区

1. 客户端检测到连接失败
2. 自动重连 gRPC 订阅
3. 重连后重新同步元数据

---

## 14. 测试覆盖

### 14.1 单元测试

| 测试文件 | 测试内容 |
|---------|---------|
| `directory_tree_test.rs` | 目录树操作、generation 递增、递归删除 |
| `lease_manager_test.rs` | 租约授予、续租、释放、过期清理 |
| `cache_test.rs` | 缓存插入、查找、失效、路径计算 |

### 14.2 集成测试

| 测试场景 | 验证内容 |
|---------|---------|
| 跨客户端一致性 | 客户端 A 创建文件，客户端 B 能看到 |
| 租约保护 | 客户端 A 持有租约时，其他客户端写入被阻塞 |
| 递归删除 | rm -rf 能正确删除嵌套目录 |
| Epoch 检测 | Master 重启后客户端能检测到 epoch 变化 |

### 14.3 端到端测试

| 测试脚本 | 测试内容 |
|---------|---------|
| `fuse_correctness_test.sh` | FUSE 基础操作、目录拷贝、跨客户端一致性 |
| `run_failover_e2e.sh` | 故障切换测试、租约失效验证 |

---

## 15. 代码位置

| 模块 | 文件路径 | 职责 |
|------|---------|------|
| Master 目录树 | `powerfs-master/src/directory_tree.rs` | 元数据存储、租约管理、通知广播 |
| FUSE 客户端 | `powerfs-fuse/src/fuser_fs.rs` | FUSE 操作实现、缓存管理、租约处理 |
| 元数据缓存 | `powerfs-fuse/src/cache.rs` | 元数据缓存、路径映射、generation 验证 |
| Chunk 缓存 | `powerfs-fuse/src/chunk_cache.rs` | 数据块缓存、脏块管理 |
| gRPC 客户端 | `powerfs-fuse/src/client.rs` | gRPC 调用、通知订阅 |
| Proto 定义 | `powerfs-proto/src/powerfs.proto` | 协议定义、消息结构 |

---

## 16. 依赖关系

```
powerfs-master
├── directory_tree.rs (核心)
│   ├── RocksDB (持久化)
│   ├── tokio broadcast (通知)
│   └── AtomicU64 (计数器)
│
powerfs-fuse
├── fuser_fs.rs (FUSE 实现)
│   ├── cache.rs (元数据缓存)
│   ├── chunk_cache.rs (数据块缓存)
│   ├── client.rs (gRPC 客户端)
│   └── lease_renewal_loop (租约续租)
└── client.rs (gRPC 通信)
    └── tokio (异步运行时)
```

---

## 17. 后续优化方向

| 优化项 | 优先级 | 说明 |
|--------|--------|------|
| 分布式元数据分片 | P1 | 支持大规模并行目录 |
| 大文件条带化 | P1 | 多节点并行读写 |
| SPDK 用户态 IO | P2 | 内核旁路存储访问 |
| RDMA 网络 | P2 | 低延迟数据传输 |
| 冷热分层 | P3 | 智能数据迁移 |