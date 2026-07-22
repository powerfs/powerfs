# 无锁 Flush 架构设计文档

## 1. 设计背景

当前 FlushManager 采用 **per-inode 锁** 机制，通过 `Arc<RwLock<HashMap<u64, Arc<Mutex<()>>>>>` 实现每个 inode 的互斥 flush。这种设计在高并发场景下存在以下问题：

1. **锁竞争严重**：大量 inode 同时需要 flush 时，锁的创建、查找、获取开销显著
2. **gRPC 连接未复用**：每个 flush 操作临时创建 gRPC 连接，连接建立开销大
3. **线程间协调复杂**：多 worker 线程共享全局锁表，读写锁冲突频繁

### 1.1 当前架构问题分析

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FlushManager                                │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  flush_locks: RwLock<HashMap<inode, Arc<Mutex<()>>>>       │   │
│  │     ↓ 每次 flush 都需要：                                    │   │
│  │     1. 获取 RwLock 读锁查找 inode                            │   │
│  │     2. 不存在则获取写锁插入新锁                               │   │
│  │     3. 获取 Mutex 锁执行 flush                              │   │
│  │     4. 释放所有锁                                           │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ↓                                      │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐               │
│  │Worker 0 │  │Worker 1 │  │Worker 2 │  │Worker 3 │               │
│  │         │  │         │  │         │  │         │               │
│  │random   │  │random   │  │random   │  │random   │               │
│  │tasks    │  │tasks    │  │tasks    │  │tasks    │               │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘               │
└─────────────────────────────────────────────────────────────────────┘
```

**瓶颈点**：
- `flush_locks` 的读写锁竞争
- `Arc<Mutex<()>>` 的 per-inode 锁竞争
- gRPC 连接每次重新建立

---

## 2. 设计目标

- **无锁设计**：消除 per-inode 锁，利用线程亲和性实现无锁并发
- **连接复用**：每个 worker 线程维护自己的 gRPC 连接池
- **高吞吐量**：减少锁开销，提升 flush 吞吐量
- **Stripe 兼容**：支持跨 volume 的 Stripe 文件写入

---

## 3. 核心设计

### 3.1 路由策略：按 inode hash 分配

**核心思想**：通过 `inode_id % worker_count` 将每个 inode 路由到固定的 worker 线程。由于同一个 inode 的所有 flush 请求始终由同一个 worker 处理，worker 内部单线程串行执行，无需任何锁。

```rust
fn route_worker(inode: u64, worker_count: usize) -> usize {
    (inode % worker_count as u64) as usize
}
```

### 3.2 Worker 线程结构

每个 worker 线程维护：
- **专属任务队列**：接收分配给该 worker 的 flush 请求
- **专属 gRPC 连接池**：缓存已建立的 volume gRPC channel
- **单线程执行**：无需锁，自然串行化

```rust
pub struct FlushWorker {
    worker_id: usize,
    task_rx: Receiver<FlushTask>,
    grpc_channels: HashMap<String, Channel>,  // 按 volume addr 缓存
    flush_fn: Arc<dyn Fn(u64) -> Result<(), String> + Send + Sync + 'static>,
    running: Arc<AtomicBool>,
}
```

### 3.3 新架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FlushManager (无锁)                                  │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  inode_dirty: RwLock<HashMap<u64, DirtyInodeInfo>> (仅用于扫描)     │   │
│  │  global_dirty_bytes: AtomicUsize                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              ↓                                              │
│          ┌───────────────────┼───────────────────┐                          │
│          ↓                   ↓                   ↓                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │   Worker 0      │  │   Worker 1      │  │   Worker 2      │             │
│  │                 │  │                 │  │                 │             │
│  │  inode % 3 == 0 │  │  inode % 3 == 1 │  │  inode % 3 == 2 │             │
│  │                 │  │                 │  │                 │             │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │             │
│  │  │ TaskQueue │  │  │  │ TaskQueue │  │  │  │ TaskQueue │  │             │
│  │  └─────┬─────┘  │  │  └─────┬─────┘  │  │  └─────┬─────┘  │             │
│  │        ↓        │  │        ↓        │  │        ↓        │             │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │             │
│  │  │gRPC Pool │  │  │  │gRPC Pool │  │  │  │gRPC Pool │  │             │
│  │  │vol0_addr │  │  │  │vol1_addr │  │  │  │vol2_addr │  │             │
│  │  │vol3_addr │  │  │  │vol4_addr │  │  │  │vol5_addr │  │             │
│  │  └─────┬─────┘  │  │  └─────┬─────┘  │  │  └─────┬─────┘  │             │
│  │        ↓        │  │        ↓        │  │        ↓        │             │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │             │
│  │  │ 单线程    │  │  │  │ 单线程    │  │  │  │ 单线程    │  │             │
│  │  │ 串行flush │  │  │  │ 串行flush │  │  │  │ 串行flush │  │             │
│  │  │ 无需锁    │  │  │  │ 无需锁    │  │  │  │ 无需锁    │  │             │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Stripe 文件处理

### 4.1 问题分析

Stripe 文件的数据分布在多个 volume 上，但同一个 inode 的所有 flush 请求路由到**同一个 worker**。这意味着：

- **优点**：同一个文件的多次 flush 天然串行化，无需额外同步
- **挑战**：单个 worker 需要向多个 volume 写入数据

### 4.2 解决方案

**方案 A：单 worker 内串行写入多个 volume（推荐）**

```rust
// Worker 内部处理 Stripe 文件
for (vid, chunks) in grouped_chunks {
    let addr = get_volume_addr(vid);
    let channel = self.get_or_create_channel(addr);
    batch_write_blob(channel, fid, chunks);  // 串行写入
}
```

**优点**：
- 实现简单，无需跨 worker 协调
- 同一个文件的写入顺序保证
- 代码改动最小

**缺点**：
- Stripe 文件的并行写入能力受限（受限于单个 worker）

**方案 B：按 volume hash 路由（跨 worker 并行）**

```rust
fn route_by_volume(vid: u64, worker_count: usize) -> usize {
    (vid % worker_count as u64) as usize
}
```

**优点**：
- Stripe 文件的不同 volume 可以并行写入
- 充分利用多 worker 并行能力

**缺点**：
- 需要跨 worker 协调（barrier 或 callback）
- 同一个文件的多次 flush 可能由不同 worker 处理
- 实现复杂，需要额外的同步机制

### 4.3 推荐选择

**方案 A** 是当前阶段的最佳选择：
- 代码改动小，风险低
- 满足大多数场景的性能需求
- 保留未来升级到方案 B 的可能性

---

## 5. gRPC 连接池设计

### 5.1 连接池结构

每个 worker 维护自己的 gRPC 连接池：

```rust
struct GrpcChannelPool {
    channels: HashMap<String, Channel>,
    max_channels: usize,
}

impl GrpcChannelPool {
    fn get_or_create(&mut self, addr: &str) -> &Channel {
        self.channels.entry(addr.to_string())
            .or_insert_with(|| Self::create_channel(addr))
    }
    
    fn create_channel(addr: &str) -> Channel {
        Channel::from_shared(format!("http://{}", addr))
            .unwrap()
            .connect()
            .await
            .unwrap()
    }
}
```

### 5.2 连接复用流程

```
flush(inode)
    ↓
获取 FileLayout
    ↓
按 volume 分组 dirty chunks
    ↓
for each volume:
    ↓
get_or_create_channel(volume_addr)
    ↓
batch_write_blob(channel, chunks)
    ↓
连接保留在 pool 中，下次复用
```

---

## 6. 任务分发机制

### 6.1 多通道分发

使用多个 crossbeam channel，每个 worker 一个接收端：

```rust
pub struct FlushManager {
    config: FlushConfig,
    inode_dirty: Arc<RwLock<HashMap<u64, DirtyInodeInfo>>>,
    global_dirty_bytes: Arc<AtomicUsize>,
    cache_max_bytes: usize,
    command_txs: Vec<Sender<FlushCommand>>,  // 每个 worker 一个
    worker_handles: Mutex<Vec<thread::JoinHandle<()>>>,
    scan_handle: Mutex<Option<thread::JoinHandle<()>>>,
    running: Arc<AtomicBool>,
    flush_fn: Arc<dyn Fn(u64) -> Result<(), String> + Send + Sync + 'static>,
}
```

### 6.2 路由分发

```rust
pub fn track_dirty(&self, inode: u64, bytes: usize) -> usize {
    let mut dirty_map = self.inode_dirty.write().unwrap();
    let info = dirty_map.entry(inode).or_insert_with(|| DirtyInodeInfo {
        dirty_bytes: 0,
        first_dirty_at: Instant::now(),
    });
    info.dirty_bytes += bytes;
    let current = info.dirty_bytes;
    drop(dirty_map);

    self.global_dirty_bytes.fetch_add(bytes, Ordering::Relaxed);

    if current >= self.config.per_file_threshold {
        let worker_idx = (inode % self.config.worker_count as u64) as usize;
        let _ = self.command_txs[worker_idx].try_send(FlushCommand::FlushInode(inode));
    }

    current
}
```

---

## 7. 扫描与调度

### 7.1 扫描线程

扫描线程定期检查全局 dirty 状态，将需要 flush 的 inode 分发到对应 worker：

```rust
fn scan_and_schedule(&self) {
    let global_dirty = self.global_dirty_bytes.load(Ordering::Relaxed);

    if global_dirty >= self.config.global_threshold {
        let oldest = self.find_oldest_dirty(self.config.worker_count.max(1));
        for inode in oldest {
            let worker_idx = (inode % self.config.worker_count as u64) as usize;
            let _ = self.command_txs[worker_idx].try_send(FlushCommand::FlushInode(inode));
        }
    }

    let expired = self.find_expired_dirty(self.config.max_dirty_age);
    for inode in expired {
        let worker_idx = (inode % self.config.worker_count as u64) as usize;
        let _ = self.command_txs[worker_idx].try_send(FlushCommand::FlushInode(inode));
    }
}
```

### 7.2 释放触发

```rust
pub fn notify_release(&self, inode: u64) {
    let dirty_bytes = self.inode_dirty_bytes(inode);
    if dirty_bytes > 0 {
        let worker_idx = (inode % self.config.worker_count as u64) as usize;
        let _ = self.command_txs[worker_idx].try_send(FlushCommand::FlushInode(inode));
    }
}
```

---

## 8. Worker 执行逻辑

### 8.1 Worker 主循环

```rust
fn run(self) {
    while self.running.load(Ordering::SeqCst) {
        match self.task_rx.recv_timeout(Duration::from_millis(500)) {
            Ok(FlushCommand::FlushInode(inode)) => {
                self.do_flush_inode(inode);
            }
            Ok(FlushCommand::Shutdown) => {
                break;
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}
```

### 8.2 无锁 flush 执行

```rust
fn do_flush_inode(&self, inode: u64) {
    // 无需获取任何锁！
    // 同一 inode 的请求始终由同一个 worker 处理
    // worker 单线程串行执行，天然互斥
    
    let dirty_bytes = self.inode_dirty_bytes(inode);
    if dirty_bytes == 0 {
        return;
    }

    match (self.flush_fn)(inode) {
        Ok(_) => {
            self.clear_dirty(inode);
            debug!("worker {}: flushed inode={}, bytes={}", self.worker_id, inode, dirty_bytes);
        }
        Err(e) => {
            warn!("worker {}: flush inode={} failed: {}", self.worker_id, inode, e);
        }
    }
}
```

---

## 9. 改造影响分析

### 9.1 移除的组件

| 组件 | 原用途 | 移除原因 |
|------|--------|----------|
| `flush_locks` | per-inode 互斥 | 路由到固定 worker，单线程自然互斥 |
| `Arc<Mutex<()>>` | inode flush 锁 | 不再需要 |

### 9.2 新增的组件

| 组件 | 用途 |
|------|------|
| `command_txs` | 每个 worker 独立的发送通道 |
| `GrpcChannelPool` | 每个 worker 的 gRPC 连接池 |

### 9.3 代码改动范围

| 文件 | 改动内容 |
|------|----------|
| `flush_manager.rs` | 重构整体架构，移除锁，新增多通道分发和连接池 |
| `fuser_fs.rs` | 移除对 `flush_locks` 的引用 |

---

## 10. 性能对比

### 10.1 锁开销对比

| 操作 | 当前方案 | 无锁方案 |
|------|----------|----------|
| flush(inode) | RwLock 读 + Mutex 获取 + Mutex 释放 + RwLock 释放 | 无锁 |
| 并发 flush 100 inode | 锁竞争严重 | 完全并行 |
| gRPC 连接 | 每次重新建立 | 连接池复用 |

### 10.2 预期收益

- **锁开销消除**：约 30-50% 的性能提升
- **连接复用**：减少 gRPC 握手开销，提升网络效率
- **可扩展性**：worker 数量可根据 CPU 核数灵活配置

---

## 11. 迁移步骤

### 11.1 Phase 1：重构 FlushManager

1. 移除 `flush_locks` 字段
2. 将单一 channel 改为多通道 `command_txs`
3. 实现按 inode hash 路由
4. Worker 线程维护独立的 gRPC 连接池

### 11.2 Phase 2：更新 fuser_fs.rs

1. 移除 `flush_locks` 参数传递
2. 更新 `flush_dirty_chunks_inner` 签名
3. 验证 Stripe 文件写入正确性

### 11.3 Phase 3：测试验证

1. 单元测试：验证路由正确性
2. 集成测试：验证 flush 完整性
3. 性能测试：验证无锁架构的性能收益

---

## 12. 代码位置

| 组件 | 文件路径 | 核心改动 |
|------|----------|----------|
| FlushManager | `powerfs-fuse-enterprise/src/flush_manager.rs` | 移除锁，新增多通道分发和连接池 |
| FUSE 集成 | `powerfs-fuse-enterprise/src/fuser_fs.rs` | 移除 flush_locks 引用 |

---

## 13. 未来优化方向

1. **方案 B 升级**：当 Stripe 文件写入成为瓶颈时，可升级为按 volume hash 路由
2. **连接池动态扩容**：根据负载自动调整连接池大小
3. **异步 flush**：使用 tokio 异步运行时替代线程池，提升并发能力
4. **backpressure 优化**：根据每个 worker 的队列长度动态调整分发策略