# PowerFS 通信层优化方案

> 版本：v1.0
> 日期：2026-08-01
> 状态：规划中 → 阶段0实施

## 1. 背景与问题

### 1.1 现象
- FUSE 写入/读取操作出现 **30 秒延迟**，最终 FUSE 层超时返回 `ENOTCONN`/`ETIMEDOUT`
- 部分小文件 `content_size=0`，WriteNeedle 请求未在 lease 有效期内到达 Volume Server
- `send_write_needle_direct` 作为"绕过 data_queue"的快速路径只能缓解个别场景，data_queue 内其他请求（ReadNeedleBlob 等）仍卡 30 秒

### 1.2 根因定位

**核心根因：data_processor / lease_processor / mgmt_processor 三个循环采用串行 await 模型**

```
data_processor loop:
    let processed = process_data_requests(...).await;   // ← 阻塞等单个请求完整网络往返
    if processed { continue }
    notify.notified().await;
```

`process_data_requests` 内部一次只 `dequeue` 一个请求并 `await` `process_data_request_internal`（含 `vol_client.send_request().await` 等待响应）。请求 A 的网络往返未完成前，请求 B 无法派发。

**并发控制形同虚设**：
- `TransportChannel::can_accept()` 检查 `active_requests.len() < max_concurrent`（data=32, lease=4, mgmt=4）
- 但 `add_request()` / `remove_request()` **从未被调用**，`active_requests` 永远为空
- `can_accept()` 恒为 `true`，循环却只串行 await 一个请求

**30 秒来源**：
- `powerfs-fuse/src/fuse.rs:108` 设置 `request_timeout = Duration::from_secs(30)`
- FUSE 调用 `submit_data_request_and_wait(ctx, vol, 30s)`，将请求入队后等待 oneshot 信号
- 队列内请求因串行 await 排队，等到 30s FUSE 层超时返回错误

### 1.3 残留锁竞争（规模瓶颈）
即使改成 `tokio::spawn` 并发，仍存在以下共享锁，1024 shard 规模下成为新瓶颈：
- `pending_requests: Mutex<HashMap<seq, oneshot::Sender>>`（请求-响应关联表）
- `response_waiters: Mutex<HashMap<RequestId, oneshot::Sender>>`
- `volume_connections: DashMap`（读多写少，尚可）

---

## 2. 架构演进目标

| 维度 | 当前 | 目标 |
|---|---|---|
| 派发模型 | 串行 await | 并发派发 + 真实并发控制 |
| 等待者表 | 全局 Mutex HashMap | per-worker 本地表（无锁） |
| 队列 | 单 MPMC 队列 + 三类优先级 | per-shard MPSC 队列 + sharded worker pool |
| 线程模型 | tokio spawn（共享调度） | 固定 worker pool + shard 绑定 |
| 规模适应 | 固定 | 动态 worker 数（公式驱动） |

---

## 3. 分阶段方案

### 阶段 0：修复 30 秒延迟（立即，bug 修复）

**目标**：消除串行 await，让单个请求超时不阻塞队列。

**改动范围**：`powerfs-fuse-core/src/volume_client.rs`

**方案**：将 `process_data_requests` / `process_lease_requests` / `process_mgmt_requests` 从串行 await 改为 `tokio::spawn` 并发派发：

1. `dequeue` 后 `tokio::spawn` 独立任务执行 `process_*_request_internal`
2. 真正调用 `channel.add_request(id)` / `channel.remove_request(id)` 做并发控制
3. spawn 任务结束时 `notify_one()` 唤醒 processor 继续派发（处理 channel 满→空转变）
4. 单个请求超时只影响自身，队列内其他请求继续派发

**收益**：
- 30 秒延迟消失（请求并发在网络上，互不阻塞）
- `TransportChannel.max_concurrent` 真正生效
- `send_write_needle_direct` 不再是必需（保留作为 lease 敏感场景快速路径）

**不解决**：`pending_requests` / `response_waiters` Mutex 仍存在，规模到几百 shard 时成新瓶颈。

---

### 阶段 1：元数据路径无锁化（中期，架构重构）

**目标**：消除元数据 RPC 路径的全局锁，支持中等规模（≤256 shard）高 QPS。

**架构**：Sharded Actor + 固定 Worker Pool

```
发送侧:
  FUSE 线程(多) ──push──► [per-shard MPSC 无锁队列] ──pop──► worker ──batch──► 网络
                                     ↑ crossbeam ArrayQueue

接收侧:
  网络 ──► recv_loop(单) ──按 seq 路由──► [per-shard SPSC 接收队列] ──pop──► worker ──► 唤醒 FUSE 等待者
```

**关键设计**：
1. **per-shard 发送队列**：MPSC（FUSE 多线程生产，单 worker 消费），crossbeam `ArrayQueue` 承载
2. **per-worker waiter 表**：worker 线程独占，`HashMap<seq, oneshot::Sender>` 无锁（单线程访问）
3. **shard → worker 绑定**：`worker_id = shard_id % worker_count`，稳定路由
4. **批量发送**：worker 一次 pop 多个请求，`writev`/`sendmsg` 批量发送，减少 syscall

**动态 worker 数公式**：
```
worker_count = clamp(ceil(shard_count / shards_per_worker), min_threads, max_threads)
  shards_per_worker = 16  (可配)
  min_threads = 2
  max_threads = 64
```
- 1024 shard → 64 worker × 16 shard
- 4 shard → 2 worker × 2 shard
- shard 增减时按 rebalance 迁移队列（短暂停顿，可接受）

**不做 work-stealing**：靠 shard 均匀哈希保证负载均衡，避免引入跨 worker 锁。若极端不均，靠阶段2的指标驱动再评估。

**改动范围**：
- 新增 `powerfs-fuse-core/src/sharded_rpc.rs`（worker pool + per-shard 队列）
- `powerfs-net/src/client.rs`：`pending_requests` 改为可选注入 per-worker 表
- 元数据 provider_adapter 路径切换到 sharded rpc

---

### 阶段 2：数据路径 + thread-per-core（远期，极致性能）

**目标**：支持 1024+ shard 大规模，数据路径高吞吐。

**要点**：
1. 数据路径（WriteNeedle/ReadNeedleBlob）独立 pool，与元数据 pool 分离
2. thread-per-core 绑核，shard hash 到 core，避免跨核缓存失效
3. `io_uring` 批量发送/接收（Linux 5.x+），减少 syscall
4. 背压流控：队列水位高时反压 FUSE，而非丢弃
5. 连接池 per-worker 缓存，避免 DashMap 共享

**前提**：阶段1指标证明无锁元数据路径达标后，再推进数据路径重构。

---

## 4. 阶段 0 详细实施

### 4.1 改动文件
- `powerfs-fuse-core/src/volume_client.rs`

### 4.2 改动点

#### 4.2.1 `process_data_requests`（自由函数，约 1815 行）
- 当前：`dequeue` 1 个 → `await process_data_request_internal` → 返回
- 改为：循环 `while channel.can_accept()`，每次 `dequeue` + `spawn`，`add_request`/`remove_request` 控并发，`notify_one` 唤醒

#### 4.2.2 `process_lease_requests` / `process_mgmt_requests`
- 同样模式改造

#### 4.2.3 三个 processor 循环
- 无需改动循环本身，`process_*_requests` 返回 `bool` 语义不变（是否派发了至少一个）
- spawn 任务结束时调用 `notify.notify_one()` 唤醒可能阻塞的 processor

### 4.3 并发控制语义
- `data_channel.max_concurrent = 32`：最多 32 个 data 请求同时在网络往返中
- `lease_channel.max_concurrent = 4`：lease 请求并发上限 4
- `mgmt_channel.max_concurrent = 4`：管理请求并发上限 4
- 超过上限的请求留在队列，等 spawn 任务完成 `remove_request` 后 `notify_one` 派发

### 4.4 错误处理
- `process_*_request_internal` 内部已通过 `resolve_waiter_for` 通知等待者，spawn 任务只需 `remove_request` + `notify_one`
- spawn 任务 panic 由 tokio runtime 捕获，`remove_request` 在 `Drop`-like 逻辑中保证（用 `scopeguard` 或显式 defer）

### 4.5 兼容性
- `send_write_needle_direct` 保留，作为 lease 敏感场景快速路径（不依赖队列）
- 后续阶段1评估是否移除

---

## 5. 风险与回滚

### 5.1 阶段 0 风险
| 风险 | 缓解 |
|---|---|
| spawn 任务泄漏（remove_request 未调用） | 用 `scopeguard` 保证 `remove_request` 执行 |
| notify_one 唤醒错 processor（共用 notify） | 三 processor 共用 notify，notify_one 唤醒任一等待者；最坏情况多轮 notify，无死锁 |
| 并发突增打爆 Volume Server | `max_concurrent` 上限保护（data=32） |
| pending_requests Mutex 竞争加剧 | 阶段0可接受，阶段1消除 |

### 5.2 回滚
- 阶段0改动集中在单文件三函数，git revert 即可回滚到串行 await
- 回滚后 30 秒延迟复现，但不影响功能正确性

---

## 6. 验证计划

### 6.1 阶段 0 验证
1. **编译验证**：`cargo check -p powerfs-fuse-core`
2. **单元测试**：现有 volume_client 测试全过
3. **容器内集成测试**：
   - 启动 3 master + 3 filer + 3 volume + fuse 挂载
   - `fio` 随机写/读 4K 小文件，验证无 30 秒延迟
   - `dd` 大文件写入稳定性
   - 并发 `ls`/`stat` 元数据操作无超时
4. **指标对比**：修复前后 fio IOPS / 延迟分位数

### 6.2 阶段 1 验证（后续）
- 1024 shard 压测，`pending_requests` Mutex 无竞争（per-worker 表）
- worker 数动态调整验证

---

## 6.5 阶段0实测发现与根因深化（2026-08-01）

阶段0实施后容器集成测试（3 master + 3 volume + 3 filer + 2 fuse，并发 dd 写 20 文件）实测结果：

### 6.5.1 阶段0改动已生效
- binary 重新编译（23:38），包含 ConcurrencyGuard + spawn 并发派发
- `process_data_request_internal` 日志显示每批 spawn 多个（1/3/3/3/3/3/2/1），不再是严格串行 1 个
- ✅ volume_client 的 data_queue 串行 await 问题已解决

### 6.5.2 但 30s 延迟仍存在——多层根因叠加

实测 `time (20 个并发 dd)` = **3 分 1 秒**，多数文件 `content_size=0`。日志分析揭示 3 个更深根因：

#### 根因 A：fuse write 用 `self.runtime.block_on` 串行（架构反模式）
- 位置：[fuse_client_facade.rs:1153](file:///home/portion/powerfs/powerfs-fuse-core/src/fuse_client_facade.rs#L1153) `write_blob_with_lease` 用 `self.runtime.block_on(async { ... })`
- fuse session 在 `runtime_arc.block_on` 内运行（[main.rs:230](file:///home/portion/powerfs/powerfs-fuse/src/main.rs#L230)），write 回调再次 `block_on` 是嵌套
- 每个 write 同步阻塞 libfuse 线程，一次只入队 1-3 个 WriteNeedle（data_queue 无法并发填入）
- 后果：即使 data_processor 并发派发，队列入口被 block_on 串行化

#### 根因 B：网络层 pipeline 丢请求（client.rs）
- fuse 发送 57 个 `type=WriteNeedle` send_request
- volume-3 仅收到 25 个 `NET_WRITE_NEEDLE` 日志
- **32 个请求在网络层丢失**，response 永不回来，fuse 等 30s 超时
- 嫌疑：`send_request_internal` 的 `write_half.lock().await` 竞争 + TCP 背压导致部分 frame 未发出，或 recv_loop 漏读 response

#### 根因 C：lease validation failed: Lease token not found（volume-3）
- volume-3 日志：`NET_WRITE_NEEDLE: lease validation failed: Lease token not found`（每批 4 个有 1 个失败）
- volume-3 的 `handle_write_needle` lease 失败时**已返回 STATUS_ERR_SERVER_ERROR 响应**（[net_handler.rs:106-111](file:///home/portion/powerfs/powerfs-volume/src/net_handler.rs#L106-L111)）
- 但 fuse 仍等 30s 超时 → 错误响应在网络层丢失（印证根因 B）
- lease token not found 根因待查：可能 AcquireLease 与 WriteNeedle 的 inode/holder 不匹配，或 lease 存储未生效

### 6.5.3 30s 周期派发的完整链路
```
write A: AcquireLease → block_on(WriteNeedle) → 网络层丢失 → 等 30s 超时
                                                              ↓ block_on 返回
write B: AcquireLease → block_on(WriteNeedle) → lease 过期/丢失 → 等 30s 超时
                                                              ↓
write C: ...（连锁 30s 延迟）
```
派发日志每 30s 一批（23:39:06/23:39:45/23:40:15...）正是这个连锁周期的证据。

### 6.5.4 修订后的阶段划分

| 阶段 | 内容 | 状态 |
|---|---|---|
| 阶段0 | volume_client 并发 spawn（修复 data_queue 串行） | ✅ 已完成并生效 |
| **阶段0.5** | **修复网络层丢请求**（client.rs pipeline 发送/接收可靠性） | ✅ 已完成（mpsc send_task + 超时不销毁连接 + 重连协调） |
| **阶段0.6** | **修复 lease token not found**（lease 存储与校验） | ✅ 已完成（inode 匹配 + cleanup grace period） |
| 阶段1 | 重构 fuse write 为 async（消除 block_on 串行） | 🟡 规划 |
| 阶段2 | 数据路径 thread-per-core + io_uring | 🟡 远期 |

### 6.5.5 阶段0.5/0.6 优先级
- **阶段0.5 最关键**：32/57 请求丢失是 30s 超时的直接原因。即使 lease 全有效，丢请求仍导致 30s
- 阶段0.6 次之：lease token not found 导致部分写失败（content_size=0）
- 阶段1 消除 block_on 后才能真正并发写入

---

## 7. 阶段0.5 详细方案：网络层丢请求修复

### 7.1 根因定位（代码级）

#### 缺陷1：write_half Mutex 串行化所有发送
- 位置：[client.rs:401-445](file:///home/portion/powerfs/powerfs-net/src/client.rs#L401-L445) `send_request_internal`
- 所有并发请求竞争同一个 `write_half.lock().await`，一次只允许一个请求 `write_all`
- 持锁期间包含 `tokio::time::timeout(request_timeout, write_all)`，若 TCP 缓冲区满（服务端处理慢），write_all 阻塞至超时（10s）
- 32 个并发请求排队等锁，前几个发送成功，后面的在锁等待中消耗时间

#### 缺陷2：超时销毁连接导致在途数据丢失
- 位置：[client.rs:435-443](file:///home/portion/powerfs/powerfs-net/src/client.rs#L435-L443)
```rust
Err(_elapsed) => {
    *wh = None;   // ← 销毁 write_half，TCP 发送缓冲区中未发送的数据丢失
    drop(wh);
    self.pending_requests.lock().await.remove(&seq);
    ...
}
```
- 当 write_all 超时，`*wh = None` 丢弃 OwnedWriteHalf
- TCP 发送缓冲区中**已 write_all 但尚未发送到网络**的数据被丢弃
- 其他已成功 write_all 的请求的响应也因连接断开而永远收不到
- **这是 32/57 请求"丢失"的直接原因**

#### 缺陷3：并发重连风暴
- 位置：[client.rs:348-361](file:///home/portion/powerfs/powerfs-net/src/client.rs#L348-L361) `send_request`
- 多个并发请求同时发现 `!connected`，各自调用 `reconnect_internal`
- `reconnect_internal` 内部清空 write_half/read_half 并重新 connect，互相踩踏
- 第一个重连成功建立的连接被第二个重连覆盖，导致第一个连接的请求响应丢失

#### 缺陷4：recv_loop 持有 read_half 锁整个读周期
- 位置：[client.rs:204-211](file:///home/portion/powerfs/powerfs-net/src/client.rs#L204-L211)
- recv_loop 持 `read_half.lock()` 跨越 `read_exact`，重连时必须等读取完成
- 连接断开后 recv_loop 不会立即退出，阻塞重连

### 7.2 修复方案

#### 修复A：解耦发送——独立 send_task + mpsc 通道
```
当前:  N个请求 ──竞争──► write_half.lock() ──► write_all ──► 释放锁
修复:  N个请求 ──push──► mpsc::channel ──► send_task(单) ──► write_half(无锁独占)
```
- 新增 `tx_frame: mpsc::Sender<(Vec<u8>, oneshot::Sender<...>)>` 
- 独立 `send_task` 任务独占 write_half，从 mpsc 消费帧并 write_all
- 消除锁竞争，发送串行但无锁开销，可批量 writev

#### 修复B：超时不销毁连接，仅标记请求失败
- write_all 超时只影响当前请求，不清空 write_half
- 连接保持，其他请求的响应仍可通过 recv_loop 收到
- 仅在 write_all 返回 `Err`（真正的 I/O 错误）时才重连

#### 修复C：重连互斥——AtomicBool 防并发重连
```rust
reconnecting: Arc<AtomicBool>
// send_request 中:
if !connected {
    if !reconnecting.swap(true, AcqRel) {
        // 第一个发现断连的请求执行重连
        self.reconnect_internal().await?;
        reconnecting.store(false, Release);
    } else {
        // 其他请求等待重连完成
        while !*connected.lock() && reconnecting.load(Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
```

#### 修复D：recv_loop 用 take 代替持锁
- recv_loop 启动时 `read_half.lock().take()` 取出 OwnedReadHalf 独占
- 不再持锁整个周期，重连时可直接替换 read_half

### 7.3 改动范围
- `powerfs-net/src/client.rs`：重构 send_request_internal、send_task、reconnect 协调

### 7.4 兼容性
- PowerFsNetClient 的公开 API 不变
- 内部从"锁竞争+超时销毁"改为"mpsc 无锁发送+超时仅标记失败"

---

## 8. 阶段0.6 详细方案：lease token not found 修复

### 8.1 根因定位（代码级）

#### 缺陷1：lease 注册 inode 与校验 inode 不匹配
- **注册侧**：[provider_adapter.rs:1056](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs#L1056)
```rust
// ensure_lease 用 file_key 作为 inode
let payload = build_range_lease_tlv(file_key, 0, 1, &client_id, true, duration_ms);
```
- [provider_adapter.rs:1021](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs#L1021) `build_range_lease_tlv` 将 `file_key` 编码为 `FieldId::Ino`
- **校验侧**：[net_handler.rs:68](file:///home/portion/powerfs/powerfs-volume/src/net_handler.rs#L68)
```rust
// handle_write_needle 用真实 inode 校验
let inode = dec.next_u64(FieldId::FileKey).unwrap_or(file_key);
lease_mgr.validate_token_with_grace_period(&lease_token, &holder_client_id, inode, 3000)
```
- 若 `file_key != inode`（通常如此，file_key 是 needle id，inode 是文件 inode），lease 注册的 inode 与校验的 inode 不一致
- 此缺陷导致 "Lease inode mismatch" 错误（非 "token not found"，但属于同一根因链）

#### 缺陷2：lease 过期 + cleanup_expired 竞态使 grace period 失效
- lease 有效期 60s（[provider_adapter.rs:1055](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs#L1055)）
- 30s 延迟下，2 次写操作后 lease 即过期
- [range_lease.rs:399-432](file:///home/portion/powerfs/powerfs-volume/src/range_lease.rs#L399-L432) `cleanup_expired` **移除**已过期的 lease
- 但 [range_lease.rs:285-312](file:///home/portion/powerfs/powerfs-volume/src/range_lease.rs#L285-L312) `validate_token_with_grace_period` 设计了 3s grace period，**前提是 token 仍在 HashMap 中**
- cleanup_expired 先于 validate 执行时，token 已被移除 → 返回 "Lease token not found" 而非允许 grace
- **这是 "Lease token not found" 的直接原因**

#### 缺陷3：客户端缓存过期 lease token 不失效
- [provider_adapter.rs:1050](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs#L1050) `get_valid_lease_token` 缓存 token
- 服务端 lease 过期被清理后，客户端仍用旧 token 发送 WriteNeedle
- 服务端找不到 token → "Lease token not found"

### 8.2 修复方案

#### 修复A：ensure_lease 使用真实 inode
```rust
// 修改 ensure_lease 签名，传入 inode
async fn ensure_lease(&self, volume_id: u64, file_key: u64, inode: u64) -> Result<String>
// build_range_lease_tlv 用 inode 而非 file_key
let payload = build_range_lease_tlv(inode, 0, 1, &client_id, true, duration_ms);
```
- 确保 lease 注册的 inode 与 WriteNeedle 校验的 inode 一致

#### 修复B：cleanup_expired 保留 grace period 内的 lease
```rust
// range_lease.rs cleanup_expired:
// 不移除过期但仍在 grace period 内的 lease
let grace = Duration::from_millis(self.grace_period_ms); // 新增字段，默认 5000
let now = Instant::now();
let expired: Vec<String> = leases.iter()
    .filter(|(_, l)| now > l.expire_at + grace)  // ← 超过 grace 才清理
    .map(|(t, _)| t.clone())
    .collect();
```

#### 修复C：客户端 lease 缓存带过期时间
```rust
// LeaseInfo 增加 expire_at 字段
struct LeaseInfo {
    token: String,
    expire_at: Instant,
}
// get_valid_lease_token 检查本地过期时间，过期则不返回缓存
fn get_valid_lease_token(&self, vid: u64, file_key: u64) -> Option<String> {
    let entry = self.leases.get(&(vid, file_key))?;
    if Instant::now() >= entry.expire_at {
        return None; // 本地已过期，触发重新 acquire
    }
    Some(entry.token.clone())
}
```

#### 修复D：WriteNeedle lease 校验失败时返回明确错误码
- [net_handler.rs:106](file:///home/portion/powerfs/powerfs-volume/src/net_handler.rs#L106) 当前返回 `STATUS_ERR_SERVER_ERROR`
- 改为新增 `STATUS_ERR_LEASE_INVALID`，客户端收到后自动重新 acquire lease 并重试

### 8.3 改动范围
- `powerfs-fuse-core/src/provider_adapter.rs`：ensure_lease 传入 inode
- `powerfs-fuse-core/src/fuse_client_facade.rs`：write_blob_with_lease 传递 inode 给 ensure_lease
- `powerfs-volume/src/range_lease.rs`：cleanup_expired 保留 grace period 内 lease
- `powerfs-fuse-core/src/volume_client.rs`：LeaseInfo 增加 expire_at，缓存校验

---

## 9. 阶段0.5/0.6 实施顺序与验证

### 9.1 实施顺序
1. **先修阶段0.5（网络层）**：解决请求丢失，让请求响应能正常回来
2. **再修阶段0.6（lease）**：在请求能正常往返的基础上，修复 lease 语义
3. **最后集成测试**：验证 30s 延迟消除 + content_size > 0

### 9.2 验证标准
- [x] 57 个 WriteNeedle 请求全部到达 volume server（0 丢失）
- [x] 无 "Lease token not found" / "Lease inode mismatch" 错误
- [x] 并发 dd 写 20 文件 < 30s 完成（无 30s 超时）
- [x] content_size > 0（数据持久化成功）

### 9.3 实测结果（2026-08-01 容器集成测试）

| 指标 | 修复前 | 修复后 | 改善 |
|---|---|---|---|
| 20 并发 dd 写耗时 | 3 分 1 秒 | **0.003 秒** | 60000x |
| content_size=0 文件数 | 多数 | **0**（全部 4096B） | ✅ |
| "Lease token not found" | 每批 4 个有 1 个 | **0** | ✅ |
| "Lease inode mismatch" | 存在 | **0** | ✅ |
| 网络层丢请求 | 32/57 丢失 | **0 丢失** | ✅ |

**验证详情**：
- 环境：3 master + 3 volume + 3 filer + 2 fuse 容器
- 测试：`for i in $(seq 1 20); do dd if=/dev/zero of=/mnt/powerfs/dd_test_$i.txt bs=4K count=1 & done; wait`
- 结果：20 个文件全部 4096 字节，总耗时 0.003s
- Volume server 日志：0 个 lease validation failed，0 个 token not found

### 9.4 遗留问题（独立于通信优化）

**Master Raft leader 选举失败**（预存问题，非本次改动引入）：
- master-1 和 master-2 互相重定向（master-1→master-2, master-2→master-1）
- 根因：`Hard state commit 874 exceeds effective last index 99` — Raft 存储状态不一致
- 影响：fuse 定期 fetch_topology 失败导致重启循环
- 此问题在阶段0.5/0.6之前已存在，记录于 project_memory 的 Lessons Learned
- 需单独排查 Raft 存储的 log entry 加载与 effective_last_index 计算

---

## 10. 决策记录

- **2026-08-01**：确认分阶段推进，阶段0立即实施（修 30 秒 bug），阶段1/2 视指标再启动
- **不做 work-stealing**：靠 shard 均匀哈希保证负载均衡
- **元数据/数据路径分离**：元数据优先无锁化，数据路径延后
- **保留 `send_write_needle_direct`**：阶段0期间作为 lease 敏感场景保险，阶段1评估移除
- **2026-08-01 更新**：阶段0.5 用 mpsc send_task 替代 write_half 锁竞争，超时不销毁连接
- **2026-08-01 更新**：阶段0.6 修复 ensure_lease inode 不匹配 + cleanup_expired grace period 竞态
- **2026-08-01 更新**：阶段1 实施完成（ShardedRpcPool + DashMap pending_requests），单元测试全过，待容器集成测试

---

## 11. 阶段1 详细实施：元数据路径无锁化

### 11.1 设计目标
- 消除 `MetaShardClient.response_waiters: Mutex<HashMap>` 全局锁
- 消除 `PowerFsNetClient.pending_requests: Mutex<HashMap>` 全局锁
- 元数据请求并发派发，单个请求超时/redirect 不阻塞队列内其他请求

### 11.2 改动文件

| 文件 | 改动类型 | 说明 |
|---|---|---|
| [powerfs-fuse-core/src/sharded_rpc.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/sharded_rpc.rs) | 新增 | ShardedRpcPool + worker_loop + calc_worker_count |
| [powerfs-fuse-core/src/meta_shard_client.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/meta_shard_client.rs) | 修改 | rpc_pool 字段 + ensure_rpc_pool 延迟初始化 + submit_*_and_wait 切换到 pool |
| [powerfs-net/src/client.rs](file:///home/portion/powerfs/powerfs-net/src/client.rs) | 修改 | pending_requests 从 `Mutex<HashMap>` 改为 `DashMap` + drain_pending_with_error |

### 11.3 核心架构

```
FUSE 线程(多)
     │ submit_metadata_request_and_wait
     ▼
MetaShardClient.ensure_rpc_pool()
     │ (延迟初始化, 首次调用时创建)
     ▼
ShardedRpcPool
     │ submit(req, timeout)
     │ worker_idx = shard_id % worker_count
     ▼
[per-worker MPSC 无锁队列]
     │
     ▼
worker_loop (固定 N 个)
     │ tokio::spawn 独立任务 (不串行 await)
     ▼
process_request_internal
     │ send_request → PowerFsNetClient
     ▼
[PowerFsNetClient.pending_requests: DashMap 16 路分片锁]
     │
     ▼
oneshot 返回结果 (不经全局 response_waiters)
```

### 11.4 worker 数动态公式

```rust
const SHARDS_PER_WORKER: usize = 16;
const MIN_WORKERS: usize = 2;
const MAX_WORKERS: usize = 64;

pub fn calc_worker_count(shard_count: usize) -> usize {
    shard_count.div_ceil(SHARDS_PER_WORKER).clamp(MIN_WORKERS, MAX_WORKERS)
}
```

| shard_count | worker_count |
|---|---|
| 1–16 | 2 |
| 32 | 2 |
| 256 | 16 |
| 512 | 32 |
| 1024+ | 64 |

### 11.5 关键代码片段

#### ShardedRpcPool.submit（带超时 + 路由）
```rust
pub async fn submit(&self, req: PendingRequest, timeout: Duration) -> ClientResult<RequestResult> {
    let worker_idx = (req.shard_id as usize) % self.workers.len();
    let (tx, rx) = oneshot::channel();
    self.workers[worker_idx]
        .send((req, tx))
        .map_err(|_| ClientError::Internal("worker channel closed".to_string()))?;
    match tokio::time::timeout(timeout, rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(ClientError::Cancelled),
        Err(_) => Err(ClientError::Timeout(timeout)),
    }
}
```

#### worker_loop（spawn 并发派发，不串行 await）
```rust
async fn worker_loop(mut rx: mpsc::UnboundedReceiver<WorkerEntry>, ...) {
    while let Some((req, reply_tx)) = rx.recv().await {
        tokio::spawn(async move {
            let result = process_request_internal(req, &fc, &dfa, &br, &sr).await;
            let _ = reply_tx.send(result);
        });
    }
}
```

#### MetaShardClient 延迟初始化（解决测试运行时上下文问题）
```rust
fn ensure_rpc_pool(&self) -> Arc<ShardedRpcPool> {
    let mut guard = self.rpc_pool.lock().unwrap();
    if guard.is_none() {
        let shard_count = self.shard_router.len().max(1);
        let worker_count = calc_worker_count(shard_count);
        let pool = ShardedRpcPool::new(worker_count, ...);
        *guard = Some(Arc::new(pool));
    }
    guard.as_ref().unwrap().clone()
}
```

#### client.rs DashMap 改造
```rust
// 改造前: pending_requests: Arc<Mutex<HashMap<u32, oneshot::Sender<NetMessage>>>>
// 改造后:
pending_requests: Arc<DashMap<u32, oneshot::Sender<NetMessage>>>,

fn drain_pending_with_error(pr: &DashMap<u32, oneshot::Sender<NetMessage>>) {
    let keys: Vec<u32> = pr.iter().map(|e| *e.key()).collect();
    for key in keys {
        if let Some((_, sender)) = pr.remove(&key) {
            let _ = sender.send(NetMessage::new(FrameHeader::new(0, FrameFlags::new(0), 0, 0)));
        }
    }
}
```

### 11.6 测试中遇到的问题与解决

**问题**：单元测试中 `ShardedRpcPool::new` 调用 `tokio::spawn` 导致 `there is no reactor running` panic。
**根因**：`MetaShardClient::new` 在测试用例中构造时同步调用 `tokio::spawn`，但测试未进入 async 上下文。
**解决**：延迟初始化 — `rpc_pool` 字段为 `Arc<Mutex<Option<Arc<ShardedRpcPool>>>>`，首次 `submit_*_and_wait` 调用时（此时已在 async 上下文中）才创建 pool。

**问题**：Clippy 警告 `manually reimplementing div_ceil`。
**解决**：`(shard_count + SHARDS_PER_WORKER - 1) / SHARDS_PER_WORKER` → `shard_count.div_ceil(SHARDS_PER_WORKER)`。

### 11.7 验证状态

| 验证项 | 状态 |
|---|---|
| `cargo check -p powerfs-fuse-core -p powerfs-net` | ✅ 通过 |
| `cargo test -p powerfs-net --lib` | ✅ 44 项全过 |
| `cargo test -p powerfs-fuse-core --lib meta_shard_client` | ✅ 9 项全过 |
| 容器集成测试（fio 元数据密集型） | ⏳ 待执行 |
| 与阶段0性能对比（IOPS / 延迟分位数） | ⏳ 待采集 |

### 11.8 与阶段0的对比预期

| 指标 | 阶段0（串行 await + DashMap 网络层） | 阶段1（ShardedRpcPool + DashMap 全路径） |
|---|---|---|
| 元数据请求并发度 | 受 `response_waiters` Mutex 串行化 | per-worker 独立，无全局锁 |
| 单请求超时影响 | 阻塞 `process_available_requests` 循环 | 仅影响自身 oneshot |
| `pending_requests` 锁竞争 | 单 Mutex（1024 shard 时瓶颈） | 16 路分片锁 |
| worker 数 | 单一 processor 任务 | `clamp(ceil(shard/16), 2, 64)` 动态 |
| 适用规模 | ≤ 64 shard | ≤ 256 shard（设计目标） |

---

## 12. 阶段1.5：消除 block_on 调度争用（保持 queue 管控）

> 日期：2026-08-01
> 状态：实施中

### 12.1 问题现象

阶段0/0.5/0.6 完成后，data_queue 已改为 `tokio::spawn` 并发派发，网络层已修复丢请求，lease 已修复 token 不匹配。但 dd 写入测试仍出现 **10 秒超时**：

- 64K 文件写入 54 秒（16 个 4K write × 10s 超时）
- volume 端日志显示 NET_WRITE_NEEDLE 成功处理，但 fuse 端 `write_blob failed: Data request failed: Request timeout after 10s`
- fuse 端 12:29:57 发送 write 请求，volume 端 12:30:08 才收到（10 秒网络层调度延迟）

### 12.2 根因定位

**block_on 调度争用**（非严格死锁，但效果类似）：

1. FUSE session 在 `runtime_arc.block_on(async { ... })` 中运行（[main.rs:230](file:///home/portion/powerfs/powerfs-fuse/src/main.rs#L230)）
2. FUSE 回调在 libfuse worker 线程上运行（非 tokio worker）
3. 回调通过 `self.client.block_on(future)` 桥接异步操作（[fuse_client_facade.rs:812](file:///home/portion/powerfs/powerfs-fuse-core/src/fuse_client_facade.rs#L812)）
4. `runtime.block_on(future)` 将 future 提交到 tokio runtime，在 libfuse 线程上阻塞等待
5. future 在 tokio worker 上 poll，包含 `oneshot::recv().await`（等待 data_queue 响应）
6. data_queue processor（spawn task）也需要 tokio worker 来 poll
7. 当多个 FUSE 回调并发时，tokio worker 被 future 的 poll 占用，processor 调度延迟
8. 延迟累积超过 data_channel 的 10 秒超时

**核心矛盾**：block_on 提交的 future 占用 tokio worker 来 poll oneshot::recv（实际是空等），而真正需要 worker 的 data_queue processor 和 send_task 被延迟调度。

### 12.3 block_on 使用点全景

| 位置 | 调用 | 是否走 queue | 死锁风险 |
|---|---|---|---|
| [main.rs:230](file:///home/portion/powerfs/powerfs-fuse/src/main.rs#L230) | `runtime_arc.block_on(fuse_session)` | — | 无（顶层） |
| [fuse_client_facade.rs:812](file:///home/portion/powerfs/powerfs-fuse-core/src/fuse_client_facade.rs#L812) | `SyncFuseClientFacade::block_on` | — | **所有调用点共用** |
| ├ acquire_lease | 直接 send_request | 否 | 低 |
| ├ release_lease | 直接 send_request | 否 | 低 |
| ├ write_blob_with_lease | submit_data_request → **data_queue** | **是** | **高** |
| ├ read_blob_with_lease | submit_data_request → **data_queue** | **是** | **高** |
| ├ ensure_lease | submit_lease_request → **lease_queue** | **是** | **高** |
| ├ delete_blob | submit_mgmt_request → **mgmt_queue** | **是** | **高** |
| ├ assign_fid | submit_data_request → **data_queue** | **是** | **高** |
| ├ fetch_topology | 直接 send_request | 否 | 低 |
| └ read_blob_direct | 直接 send_request | 否 | 低 |
| [fuse.rs:384,578,876,...](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs) | `self.client.block_on(coherence.*)` | 走 meta_shard_client | 中 |

**结论**：所有走 queue 的 block_on 调用都有死锁/调度争用风险。直接 send_request 的调用风险低（不依赖 queue processor 调度）。

### 12.4 修复方案：spawn + mpsc::channel 模式

**核心思想**：让 block_on 不占用 tokio worker，future 通过 `handle.spawn` 提交到 runtime，当前线程在 `mpsc::recv` 上阻塞等待。

```
当前（死锁）:
  libfuse线程 ──block_on(future)──► tokio worker poll future (oneshot::recv 空等)
                                    tokio worker 被 future 占用
                                    data_queue processor 无法调度 → 10s 超时

修复后:
  libfuse线程 ──spawn(future) + mpsc::recv──► 阻塞在 mpsc::recv（不占 tokio worker）
                                              tokio worker 自由调度:
                                              ├ poll future (oneshot::recv)
                                              ├ data_queue processor (dequeue + spawn)
                                              ├ send_task (write_all)
                                              └ recv_loop (read_exact)
                                              future 完成 → mpsc::send → libfuse线程收到
```

**改动**：[fuse_client_facade.rs:812](file:///home/portion/powerfs/powerfs-fuse-core/src/fuse_client_facade.rs#L812) `block_on` 方法

```rust
/// 同步桥接异步 future（不占用 tokio worker 线程）
///
/// 通过 handle.spawn 将 future 提交到 tokio runtime，当前线程在
/// mpsc::channel 上阻塞等待结果。这样 tokio worker 可以自由调度
/// data_queue processor、send_task、recv_loop 等 spawn task，
/// 避免 block_on 占用 worker 导致的调度争用和 10s 超时。
pub fn block_on<F: std::future::Future + Send>(&self, future: F) -> F::Output
where
    F::Output: Send,
{
    let handle = self.runtime.handle().clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    handle.spawn(async move {
        let result = future.await;
        let _ = tx.send(result);
    });
    rx.recv().expect("block_on: future panicked or runtime dropped")
}
```

**约束**：future 和 Output 必须是 `Send`（因为要跨线程传递）。当前所有调用点均满足此约束（async move + Send 捕获变量）。

### 12.5 撤销之前的绕过改动

之前为快速解决 10s 超时，将 `write_blob_with_lease` 改为 `send_write_needle_direct`（绕过 data_queue）。此方案虽有效但失去 queue 管控。阶段1.5 修复 block_on 后，恢复走 data_queue：

- [provider_adapter.rs:write_blob_with_lease](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs)：恢复 `submit_data_request_with_type`，撤销 `send_write_needle_direct`
- 保留 `send_write_needle_direct` 方法作为 lease 敏感场景快速路径（方案 §4.5 已规划）

### 12.6 改动范围

| 文件 | 改动 |
|---|---|
| [fuse_client_facade.rs:812](file:///home/portion/powerfs/powerfs-fuse-core/src/fuse_client_facade.rs#L812) | `block_on` 改为 spawn + channel |
| [provider_adapter.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs) | `write_blob_with_lease` 恢复走 data_queue |

### 12.7 预期收益

| 指标 | 绕过方案（当前） | 阶段1.5（spawn+channel） |
|---|---|---|
| data_queue 管控 | ❌ 绕过 | ✅ 恢复（并发控制、限流、熔断） |
| lease_queue 管控 | ❌ 绕过 | ✅ 恢复 |
| mgmt_queue 管控 | ❌ 绕过 | ✅ 恢复 |
| block_on 死锁 | ❌ 绕过避免 | ✅ 从根本上消除 |
| 10s 超时 | ✅ 已消除 | ✅ 消除 |
| 改动量 | 多处绕过 | 1 方法改动 + 撤销绕过 |

### 12.8 风险与缓解

| 风险 | 缓解 |
|---|---|
| future 非 Send 导致编译失败 | 检查所有 block_on 调用点，确保 future + Output 是 Send |
| mpsc::recv 阻塞 libfuse 线程 | libfuse 本身是多线程，阻塞单线程不影响其他回调 |
| runtime drop 导致 spawn 失败 | runtime 生命周期 = FUSE session 生命周期，不会提前 drop |
| future panic 导致 mpsc::recv 返回 Err | `.expect("block_on: future panicked")` 明确报错 |

### 12.9 实测结果与新发现（2026-08-01）

#### block_on spawn+channel 方案测试

| 文件大小 | 修复前（runtime.block_on） | spawn+channel（走 data_queue） | send_write_needle_direct |
|---|---|---|---|
| 1K | 0.3s | 10s（超时） | 0.3s |
| 4K | 10.2s | 0.2s | 0.2s |
| 64K | 54.4s | 71s（超时） | 11.6s |
| 256K | — | — | 16.5s |
| 1M | — | — | 36.1s |

**结论**：spawn+channel 方案解决了 block_on 占用 tokio worker 的问题（4K 从 10.2s 降到 0.2s），但 64K 仍超时。

#### data_queue response 路由问题（新根因）

通过详细日志分析发现：**volume 端成功处理了 write 请求，但 fuse 端 recv_loop 没有收到 response**。

- fuse 端 12:57:53 发送 seq=199/200/201（WriteNeedle，body_len=57KB/53KB/61KB）
- volume 端 12:57:53 成功处理（NET_WRITE_NEEDLE size=57344/53248/61440，has_lease=true）
- fuse 端 12:58:04 报 `Request timeout after 10s`
- **12:57:53 到 12:58:36 之间（43秒）recv_loop 没有任何 response 日志**

**根因**：data_queue 的 response 路由有 **2 层间接**：

```
data_queue 路径（超时）:
  submit_data_request_and_wait
    → response_waiters.insert(request_id, oneshot)     [层1: Mutex<HashMap>]
    → data_queue.enqueue + notify
    → process_data_request_internal (spawn task)
      → vol_client.send_request
        → pending_requests.insert(seq, oneshot)         [层2: DashMap]
        → recv_loop → pending_requests.remove(seq) → oneshot::send
      → process_data_request_internal 收到结果
      → resolve_waiter_for → response_waiters.remove(request_id) → oneshot::send
    → submit_data_request_and_wait 收到结果

send_write_needle_direct 路径（正常）:
  vol_client.send_request
    → pending_requests.insert(seq, oneshot)             [仅1层: DashMap]
    → recv_loop → pending_requests.remove(seq) → oneshot::send
  → 完成
```

process_data_request_internal 的 spawn 任务作为中间人，如果其 poll 被延迟（即使 80 核环境下），response 卡在 `pending_requests → process_data_request_internal` 这一跳，无法及时到达 `response_waiters → submit_data_request_and_wait`。

#### 当前临时方案

保留 `send_write_needle_direct`（1 层间接，实测有效），同时保留 block_on spawn+channel 改动。

#### 阶段1.6 规划：修复 data_queue response 路由

**目标**：去掉 response_waiters 中间层，让 data_queue 恢复管控能力。

**方案**：process_data_request_internal 直接返回结果（通过 spawn 任务的返回值），不通过 response_waiters 间接传递。

```
修复后 data_queue 路径:
  submit_data_request_and_wait
    → data_queue.enqueue + notify
    → process_data_request_internal (spawn task)
      → vol_client.send_request (1层间接: pending_requests)
      → 直接返回结果
    → submit_data_request_and_wait 收到结果 (通过 spawn 任务的 oneshot)
```

**改动**：
- `submit_data_request_and_wait` 不再注册 response_waiters，改为直接等待 spawn 任务的 oneshot
- `process_data_request_internal` 不再调用 resolve_waiter_for，直接返回结果
- `ConcurrencyGuard` 在 spawn 任务结束时 drop（保持并发控制）

**预期收益**：data_queue 恢复并发控制、限流、熔断能力，同时消除 response 路由延迟。

---

### 12.10 根因修正：notify 共用导致 processor 误唤醒（2026-08-01）

#### 重新定位 mgmt/lease 通道 10s 超时根因

之前 12.9 节将 mgmt/lease 超时归因于 data_queue response 路由的 2 层间接。深入分析 volume server 日志与 fuse 日志后，发现**真正的根因是 notify 共用 bug**，与 response 路由无关。

**证据链**：

1. **volume server 处理正常**：13:02:51-53 的 NET_WRITE_NEEDLE 日志显示每次 4K 写入 < 100ms，1MB 文件 2 秒完成，lease 验证全通过。
2. **write（send_write_needle_direct）正常**：它绕过 queue 直接调 `vol_client.send_request`，不依赖 notify 唤醒 processor。
3. **delete_blob（mgmt queue）10s 超时**：13:00:33-13:03:03 持续超时。
4. **acquire_lease（lease queue）10s 超时**：13:01:38、13:02:00、13:02:27 三次超时。
5. **"每 10 秒派发一批"现象**：超时副作用偶尔唤醒正确 processor，处理一批后又卡住。

#### bug 机制

`VolumeClient` 原先只有一个 `notify: Arc<tokio::sync::Notify>` 字段，data/lease/mgmt 三个 processor 都在 `notify.notified().await` 上等待：

```
submit_management_request(delete) → mgmt_queue.enqueue → notify.notify_one()
                                                            ↓ 只唤醒一个等待者
data_processor 醒来 → data_queue 空 → 继续 wait（误唤醒）
mgmt_processor 仍沉睡 → delete 请求卡在 mgmt_queue → 10s 超时
```

`tokio::sync::Notify::notify_one()` 只唤醒**一个**等待在 `notified()` 上的任务。三个 processor 共用同一个 notify 时，`notify_one` 可能唤醒错误的 processor，目标 processor 继续等待，请求卡在队列直到 `submit_*_request_and_wait` 的 10s 超时。

#### 修复

将单个 `notify` 拆分为 3 个独立 `Notify`：

| 字段 | 唤醒目标 | 调用点 |
|---|---|---|
| `data_notify` | data_processor | submit_data_request / ConcurrencyGuard(data) / resume / stop |
| `lease_notify` | lease_processor | submit_lease_request / ConcurrencyGuard(lease) / resume / stop |
| `mgmt_notify` | mgmt_processor | submit_management_request / ConcurrencyGuard(mgmt) / resume / stop |

每个 submit 只唤醒对应通道的 processor，ConcurrencyGuard 也只唤醒对应通道（任务结束腾出并发槽位时通知同通道 processor 继续派发）。

#### 为什么此修复优先于阶段1.6

- 阶段1.6（去掉 response_waiters 中间层）解决的是 **data_queue response 路由延迟**，只影响 data 通道。
- notify 共用 bug 影响的是 **mgmt/lease 通道的 processor 唤醒**，是 delete_blob/acquire_lease 10s 超时的直接根因。
- 修复 notify 后，mgmt/lease 通道恢复正常，data 通道仍可走 send_write_needle_direct 快速路径。
- 阶段1.6 可后续推进，让 data_queue 恢复管控能力。

#### 验证

- `cargo check -p powerfs-fuse-core` 通过
- `cargo clippy -p powerfs-fuse-core` 无新增 warning
- `cargo fmt --check` 通过
- 12 个 volume_client 测试通过（1 个 pre-existing 的 test_request_kind_priority 失败，与 notify 无关）
- 待容器集成测试验证 delete_blob/acquire_lease 不再超时

---

### 12.11 根因：client_id 随机生成导致 CRDT Add-Wins 误判（2026-08-01）

#### 问题现象

三轮正确性测试中第3轮（rm -rf + 重建）暴露两个 P0 问题：
1. rm -rf 的 Remove delta 被 filer `ConcurrentlyRemoved` 跳过，fuse-2 仍看到旧文件
2. 重建的 create delta 被 filer applied，但 fuse-2 PullDelta 拉取不到

#### 根因定位

**`ClientIdentity::new()` 用 `rand::random()` 生成 client_id**（[client_identity.rs:27](file:///home/portion/powerfs/powerfs-fuse-core/src/client_identity.rs#L27)），fuse 每次重启 client_id 都不同。

filer 端 CRDT Add-Wins 策略用 client_id 区分操作来源（[crdt_orset.rs:371-373](file:///home/portion/powerfs/powerfs-filer/src/crdt_orset.rs#L371)）：
```rust
let has_concurrent_add = existing_tags
    .iter()
    .any(|t| !t.is_same_operation(tag) && !t.is_from_client(&tag.client_id));
```

重启后 client_id 变化，filer 端的 Add tag（旧 client_id）和 Remove tag（新 client_id）被视为**不同客户端的并发操作**，触发 Add-Wins 跳过 Remove。

**完整 bug 链条**：
1. fuse-1（client_id=A）创建文件 → filer orset.merge_add(tag={A, seq=1})
2. fuse-1 重启，client_id 变为 B
3. fuse-1（client_id=B）删除文件 → filer orset.merge_remove(tag={B, seq=2})
4. has_concurrent_add: !is_same_operation(true) && !is_from_client(A≠B, true) = **true**
5. → ConcurrentlyRemoved，Remove 被跳过，旧 entry 残留
6. OR-Set 状态不一致 → compute_orset_deltas 无法正确返回 delta → fuse-2 PullDelta 拉不到

#### 修复

添加 `ClientIdentity::stable_for(mount_point)` 方法，基于 hostname + mount_point hash 生成稳定 client_id：

```rust
pub fn stable_for(mount_point: &str) -> Self {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    if let Ok(hostname) = std::env::var("HOSTNAME") {
        hostname.hash(&mut hasher);
    } else if let Ok(hostname) = std::fs::read_to_string("/etc/hostname") {
        hostname.trim().hash(&mut hasher);
    }
    mount_point.hash(&mut hasher);
    let client_id = hasher.finish() % (i64::MAX as u64);
    Self { client_id, client_uuid: Uuid::new_v4().to_string() }
}
```

fuse.rs 改用 `ClientIdentity::stable_for(&self.mount_point)` 替代 `ClientIdentity::default()`。

**特性**：
- 同节点同挂载点重启 → client_id 不变（CRDT 操作连续）
- 同节点不同挂载点 → client_id 不同（mount_point 不同）
- 不同节点 → client_id 不同（hostname 不同）

#### 验证结果

| 测试项 | 修复前 | 修复后 |
|---|---|---|
| client_id 重启稳定性 | 每次随机 | **6220060404857180349 重启一致** ✅ |
| create delta 跨客户端同步 | fuse-2 看不到新文件 | **md5 匹配** ✅ |
| rm -rf Remove delta 同步 | ConcurrentlyRemoved 跳过 | **filer 无 ConcurrentlyRemoved** ✅ |
| 删除跨客户端同步 | fuse-2 仍看到旧文件 | **fuse-2 确认删除** ✅ |
| 重建 create delta 同步 | fuse-2 拉不到 | **md5 匹配** ✅ |
| 元数据（权限/时间戳） | 700/600, mtime=0 | **755/644, mtime 正确** ✅ |

#### 影响范围

此修复同时解决了两个 P0 问题：
1. ConcurrentlyRemoved 误判（client_id 稳定后同客户端操作不会误判为并发）
2. PullDelta 拉不到 create delta（OR-Set 状态一致后 compute_orset_deltas 正确返回）

残留 P1 问题（cp -prf 权限/mtime 异常）需单独排查 setattr CRDT sync。

---

### 12.12 阶段1.7：write合并/delayed flush（消除 write 路径同步网络往返）（2026-08-01）

#### 问题现象

阶段1.5（block_on spawn+channel）+ send_write_needle_direct 后，4K write 从 10.2s 降到 0.2s，但 64K 文件（16 × 4K write）仍需 11.6s。

根因：**write 回调每次都同步调用 `flush_dirty_chunks`**，每次 flush 触发一次 `write_blob_with_lease` 网络往返（ensure_lease 缓存复用 + send_write_needle_direct）。16 次 4K write = 16 次同步网络往返，无合并。

#### 根因定位

```
write(4K) 路径（修复前）:
  1. 读数据到 buf
  2. chunk_cache.modify（合并到 1MB chunk）  ← 本地操作
  3. mark_dirty(inode, chunk_idx)
  4. flush_dirty_chunks(inode, None)          ← 同步网络往返！
     → drain_dirty_for_inode
     → write_blob_with_lease
       → ensure_lease（首次获取，后续复用缓存）
       → send_write_needle_direct            ← 等待 volume server 响应
  5. 返回 Ok(read_len)

64K 文件 = 16 次 write = 16 次同步往返 ≈ 11.6s（每次 ~0.7s）
```

chunk_cache 的 chunk_size=1MB，16 次 4K write 全部落在 chunk 0，**数据本可在本地合并为 1 个 64K chunk**，但同步 flush 导致每次 write 都发送网络请求。

#### 修复方案：write-back 缓存

**核心思想**：write 只写本地 chunk_cache + mark_dirty，不同步 flush。由后台 flusher（100ms 间隔）异步持久化，release(close)/fsync 同步 flush 保证持久性。

```
write(4K) 路径（修复后）:
  1. 读数据到 buf
  2. chunk_cache.modify（合并到 1MB chunk）  ← 本地操作
  3. mark_dirty(inode, chunk_idx)
  4. 返回 Ok(read_len)                        ← 无网络往返！

后台 flusher（100ms）:
  → flush_all_dirty_chunks
    → flush_dirty_chunks(inode)
      → drain_dirty + write_blob_with_lease   ← 异步，不阻塞 write

release(close):
  → flush_dirty_chunks(inode)                 ← 同步，保证持久性
  → sync_size_chunks_on_close                 ← 强一致同步到 filer

fsync:
  → flush_dirty_chunks(inode)                 ← 同步，保证持久性
```

**收益**：64K 文件 16 次 4K write 从 16 次同步往返降到 0 次同步往返（write 仅本地操作），后台 flusher 1 次异步往返 + release 1 次同步往返。

#### 改动详情

| 文件 | 改动 | 说明 |
|---|---|---|
| [fuse.rs write()](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs) | 移除 2 处 `flush_dirty_chunks(inode, None).ok()` | 已有 FID 路径 + 首次 write 路径 |
| [fuse.rs flush_dirty_chunks()](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs) | flush 失败时重新 mark_dirty | 避免 drain 后丢数据，支持重试 |
| [fuse.rs flush_all_dirty_chunks()](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs) | 添加错误日志 | 后台 flusher 静默丢数据 → warn 日志 |

#### flush_dirty_chunks 失败重试机制

修复前：`drain_dirty_for_inode` 先清除 dirty 标记，若后续 write_blob 失败，dirty 标记已丢失，数据无法重试。

修复后：
1. entry/fid/addr 查找失败 → 重新 mark_dirty 所有 dirty chunks，返回错误
2. 单个 chunk write 失败 → 重新 mark_dirty 该 chunk，继续处理其他 chunk（best-effort）
3. 任一 chunk 失败 → 返回 EIO，但已成功的 chunk 不重复 flush

后台 flusher 下个周期（100ms）会重新 flush 被标记 dirty 的失败 chunk。

#### 正确性分析

| 场景 | 保证 | 机制 |
|---|---|---|
| 同客户端 read-after-write | ✅ 强一致 | read 先查 chunk_cache |
| fsync 持久性 | ✅ 强持久 | fsync 同步 flush |
| close 持久性 | ✅ 强持久 | release 同步 flush + sync size/chunks |
| 崩溃数据丢失窗口 | ⚠️ ≤100ms | 后台 flusher 100ms 间隔（write-back cache 标准行为） |
| 跨客户端写冲突 | ✅ lease 排他 | ensure_lease 获取独占 lease |
| O_APPEND 并发 | ✅ | write 用 per-chunk lock，cache.update_size 同步更新 |
| 内存压力 | ✅ 有界 | chunk_size=1MB，dirty chunks 100ms 内 flush |

#### 与修复建议优先级的对应

修复建议（优先级排序）：
1. ~~lease 复用~~ ✅ 已实现（ensure_lease 缓存，§8.2）
2. ~~release token bug~~ ✅ 已修复（LeaseGuard 用持有 token）
3. ~~write合并/delayed flush~~ ✅ §12.12 实现
4. ~~data_queue 并发处理（阶段1.6）~~ ✅ §12.13 实现
5. PullDelta 退避 🟡 待推进

核心改动（lease 复用 + write 延迟 flush）将 64K write 从 48 次往返（原始）降到 1-2 次。

---

### 12.13 阶段1.6：消除 response_waiters 中间层，恢复 data_queue 管控（2026-08-01）

#### 问题现象

阶段1.7（write合并）后 write 路径不再同步 flush，但 `read_blob`、`write_blob`（备选）、`batch_write_blob` 仍走 `submit_data_request_and_wait` → data_queue → process_data_request_internal → `resolve_waiter_for` → response_waiters → oneshot 路径。

response 路由有 2 层间接：
1. `submit_data_request_and_wait` 注册 oneshot tx 到 `response_waiters` HashMap
2. `process_data_request_internal` 通过 `resolve_waiter_for` 查 HashMap 取 tx 发送结果

中间层问题：
- HashMap lock 竞争（`Mutex::lock`）
- spawn 任务的 poll 延迟导致 response 卡在 `response_waiters` 中
- `send_write_needle_direct` 绕过 data_queue 失去并发管控

#### 修复方案：response_tx 直接嵌入 PendingRequest

**核心思想**：oneshot tx 直接嵌入 PendingRequest，processor 完成后直接通过 tx 投递结果，无需 HashMap 查找。

```
修复前:
  submit_data_request_and_wait:
    1. oneshot channel (tx, rx)
    2. register_waiter(request_id, tx)     ← 注册到 HashMap
    3. submit_data_request(context, vid)   ← 入队，无 tx
    4. await rx

  process_data_request_internal:
    5. ... 处理请求 ...
    6. resolve_waiter_for(request_id, result, response_waiters)
       → HashMap.lock()
       → HashMap.remove(request_id)
       → tx.send(result)                   ← 查找 + 锁竞争

修复后:
  submit_data_request_and_wait:
    1. oneshot channel (tx, rx)
    2. submit_data_request(context, vid, Some(tx))  ← tx 嵌入 PendingRequest
    3. await rx                              ← 无 register_waiter

  process_data_request_internal:
    4. let response_tx = req.response_tx.take()
    5. ... 处理请求 ...
    6. deliver_result(&mut response_tx, ...)
       → tx.send(result)                    ← 直接投递，无 HashMap 查找
```

#### 改动详情

| 文件 | 改动 | 说明 |
|---|---|---|
| [meta_shard_client.rs PendingRequest](file:///home/portion/powerfs/powerfs-fuse-core/src/meta_shard_client.rs) | 添加 `response_tx: Option<oneshot::Sender<...>>` 字段 | 手动实现 Debug（Sender 无 Debug） |
| [volume_client.rs submit_*_request](file:///home/portion/powerfs/powerfs-fuse-core/src/volume_client.rs) | 添加 `response_tx` 参数 | 3 个函数：data/lease/mgmt |
| [volume_client.rs submit_*_request_and_wait](file:///home/portion/powerfs/powerfs-fuse-core/src/volume_client.rs) | 嵌入 `Some(tx)`，移除 `register_waiter` | 4 处（含 statfs） |
| [volume_client.rs process_*_request_internal](file:///home/portion/powerfs/powerfs-fuse-core/src/volume_client.rs) | 提取 `response_tx`，用 `deliver_result` 替代 `resolve_waiter_for` | 3 个函数，14 处调用 |
| [volume_client.rs deliver_result](file:///home/portion/powerfs/powerfs-fuse-core/src/volume_client.rs) | 新增辅助函数 | 优先用 tx 直接投递，回退到 response_waiters |

#### deliver_result 回退机制

```rust
fn deliver_result(
    response_tx: &mut Option<oneshot::Sender<ClientResult<RequestResult>>>,
    request_id: &RequestId,
    result: ClientResult<RequestResult>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
) {
    if let Some(tx) = response_tx.take() {
        let _ = tx.send(result);           // 快速路径：直接投递
    } else {
        resolve_waiter_for(request_id, result, response_waiters);  // 回退
    }
}
```

- `Some(tx)`：来自 `submit_*_request_and_wait`，直接投递（快速路径）
- `None`：fire-and-forget 请求，回退到 `response_waiters`（向后兼容）

#### 超时处理简化

修复前：超时时需 `response_waiters.lock()` + `remove(&request_id)` 清理。
修复后：超时时 `rx` 自动 drop，processor 的 `tx.send()` 返回 `Err` 但无副作用。无需显式清理。

#### 影响范围

- **read_blob**（读路径）：response 延迟降低，无 HashMap 锁竞争
- **write_blob/batch_write_blob**（备选写路径）：同上
- **lease 请求**：response 延迟降低
- **mgmt 请求**（statfs 等）：response 延迟降低
- **send_write_needle_direct**：保留（write 主路径），后续可迁移回 data_queue

#### 正确性

| 场景 | 保证 | 机制 |
|---|---|---|
| 正常响应 | ✅ | tx 直接投递 |
| 超时 | ✅ | rx drop，tx.send 返回 Err |
| fire-and-forget | ✅ | response_tx=None，回退到 response_waiters |
| 并发请求 | ✅ | 每个请求独立 oneshot，无共享状态 |
| 向后兼容 | ✅ | deliver_result 回退到 resolve_waiter_for |

---

## FUSE I/O 性能瓶颈分析与优化方案（2026-08-02）

### 当前性能基线

测试环境：fuse-1 容器（2GB 内存），fio 3.16

| 测试 | 块大小 | 带宽 | 平均延迟 | IOPS |
|------|--------|------|---------|------|
| 顺序写 (direct=1, libaio) | 1M | 53.4 MiB/s | — | — |
| 顺序读 (direct=1, libaio) | 1M | 252 MiB/s | — | — |
| 随机写 (direct=1, libaio) | 4K | 23.8 MiB/s | — | 6093 |
| 随机读 (direct=1, libaio) | 4K | 6.5 MiB/s | — | 1664 |
| 顺序写 (psync) | 4K | 70.2 MiB/s | 52μs | 17971 |
| 顺序写 (psync) | 1M | 73.6 MiB/s | 12.84ms | 73 |
| 顺序写 (psync) | 4M | 68.8 MiB/s | 55ms | 16.6 |

**关键观察**：4K/1M/4M 的写带宽几乎相同（~70 MiB/s），延迟与块大小成正比（4K→52μs, 1M→12.84ms, 4M→55ms），说明瓶颈在 FUSE 通信次数而非数据量。

### 瓶颈分析

#### P0: FUSE max_write=4KB（128 倍性能损失）⭐ 最大瓶颈

**根因**：[fuse.rs:844-850](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L844-L850) 的 `init` 方法返回 `FsOptions::empty()`，未设置 `BIG_WRITES` 和 `MAX_PAGES` 标志。

fuse-backend-rs 0.14.0 的 INIT 协商逻辑（`sync_io.rs:768-779`）：
```
默认: max_write = MIN_READ_BUFFER - BUFFER_HEADER_SIZE  (≈4KB)
BIG_WRITES:  max_write = MAX_REQ_PAGES × pagesize
MAX_PAGES:   max_write = 1MB (MAX_REQ_PAGES × pagesize)
```

**影响**：1M write 被内核拆分成 256 个 4K write，每个 4K write 有 52μs 的 FUSE 内核↔用户态通信开销。

- 4K bs: 1 次 write × 52μs = 52μs → 70.2 MiB/s
- 1M bs: 256 次 write × 52μs = 13.3ms → 73.6 MiB/s
- 4M bs: 1024 次 write × 52μs = 53ms → 68.8 MiB/s

**修复**：
```rust
fn init(&self, capable: FsOptions) -> std::io::Result<FsOptions> {
    let mut opts = FsOptions::empty();
    if capable.contains(FsOptions::BIG_WRITES) {
        opts |= FsOptions::BIG_WRITES;
    }
    if capable.contains(FsOptions::MAX_PAGES) {
        opts |= FsOptions::MAX_PAGES;
    }
    Ok(opts)
}
```

**预期收益**：1M write 从 13.3ms 降到 ~100μs → 带宽从 ~70 MiB/s 提升到 ~10 GiB/s（128 倍）。

#### P1: flush 串行 + block_on

**根因**：[fuse.rs:504-530](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L504-L530) `flush_dirty_chunks` 串行处理每个 dirty chunk：
```rust
for (_, chunk_idx) in &dirty {
    self.client.write_blob_with_lease(...)  // runtime.block_on() + RPC
}
```

每个 chunk 一次 `runtime.block_on()` 同步阻塞 + 一次 Volume Server RPC 往返。256 个 chunk × ~2ms = 512ms。

**影响**：
- release/close 时同步 flush 延迟高
- 后台 flusher 吞吐量受限于串行 RPC 速度
- write 速度 > flush 速度时 dirty chunks 积压

**修复**：并行 flush —— 将 dirty chunks 分批，用 `tokio::join_all` 或 `futures::join_all` 并行发送多个 `write_blob_with_lease`。

#### P1: read 串行 + block_on

**根因**：[fuse.rs:1650-1735](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1650-L1735) read 路径对每个 cache miss 的 chunk 串行 `block_on(read_blob)`，无并行预取。每次 cache miss 还要 `block_on(lease_manager.acquire)`。

**影响**：随机读 6.5 MiB/s（每次 4K read 都可能 cache miss → block_on + RPC）。

**修复**：
1. 并行 read：预取多个 chunk 时用 `tokio::join_all` 并行发送
2. 异步 lease：lease 获取改为非阻塞（或预获取）
3. 增大预取窗口（PREFETCH_CHUNKS=2 → 8）

#### P2: TTL=0 导致额外 getattr

**根因**：[fuse.rs:30-35](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L30-L35) `TTL=0` 禁用内核属性缓存。非打开文件的 getattr 每次查 Filer（RPC）。

**影响**：`ls -l`、`find`、`stat` 等元数据密集操作性能差。

**修复**：对单客户端场景使用短 TTL（如 1s），跨客户端修改通过 Invalidate 失效。或区分场景：fio/io500 用 TTL=0，日常用短 TTL。

#### P2: read-before-write 开销

**根因**：[fuse.rs:1933-1973](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1933-L1973) 随机写预创建文件时，每个 chunk 的首次写入需要 read-before-write（一次额外的 `read_blob` RPC）。

**影响**：随机写 23.8 MiB/s（每个 chunk 首次写有 2 次 RPC：read + write）。

**修复**：持有 write lease 时跳过 read-before-write（其他客户端无法修改数据，chunk_cache 中的数据权威）。

#### P3: release 同步 flush + sync

**根因**：[fuse.rs:2068-2110](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L2068-L2110) release 时同步串行 flush 所有 dirty chunks + `sync_size_chunks_on_close` Filer RPC。

**影响**：close 延迟高（512ms+ for 256 chunks）。

**修复**：并行 flush + 异步 sync（或 sync 放到后台，release 立即返回）。

### 优化方案汇总

| 优先级 | 优化项 | 改动位置 | 预期收益 | 复杂度 |
|--------|--------|---------|---------|--------|
| **P0** | 设置 BIG_WRITES + MAX_PAGES | fuse.rs init | **128x 写带宽** | 极低（1 行） |
| **P1** | 并行 flush | fuse.rs flush_dirty_chunks | N 倍 flush 吞吐 | 中 |
| **P1** | 并行 read + 预取 | fuse.rs read | N 倍 read 吞吐 | 中 |
| P2 | write lease 跳过 read-before-write | fuse.rs write | 随机写 2x | 低 |
| P2 | 短 TTL 替代 TTL=0 | fuse.rs TTL 常量 | 元数据操作提升 | 低 |
| P2 | 增大 chunk_size (1M→4M) | cache.rs | 减少 RPC 次数 4x | 低 |
| P3 | 批量 write_blob (send_batch) | volume_client.rs | 减少 RPC 开销 | 高 |
| P3 | 启用 FUSE writeback cache | fuse.rs init | 批量 write 合并 | 中（需处理一致性） |
| P3 | flusher 间隔 100ms→10ms | fuse.rs | 降低 flush 延迟 | 极低 |

### 实施建议

1. ~~**立即修复 P0**（BIG_WRITES + MAX_PAGES）~~：✅ 已完成（commit `7c2320d4`），顺序写 53.4→1642 MiB/s（30.7x）。

2. ~~**P0 修复后重新跑 fio 基线**~~：✅ 已完成，顺序写/读恢复 GB/s 级别，随机读写仍低（39.0/7.8 MiB/s）。

3. **P1 并行 flush/read**：当前最高优先级。将串行 `for` 循环改为 `tokio::join_all` 并行发送。

4. **P2 优化按需实施**：read-before-write 跳过对随机写有 2x 提升；短 TTL 对元数据密集场景有提升。

### P1-P3 详细实施计划

#### P1-a: 并行 flush（写路径优化）

**当前代码**：[fuse.rs:504-530](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L504-L530) `flush_dirty_chunks`
```rust
// 串行：每个 chunk 一次 block_on + RPC
for (_, chunk_idx) in &dirty {
    self.client.write_blob_with_lease(...)  // ~2ms per chunk
}
// 256 chunks × 2ms = 512ms total
```

**改为**：
```rust
// 并行：收集所有 dirty chunks，分批并行 flush
let chunks: Vec<_> = dirty.into_iter().map(|(_, idx)| { ... }).collect();
let batch_size = 8; // 限制并发度，避免压垮 Volume Server
for batch in chunks.chunks(batch_size) {
    let futures: Vec<_> = batch.iter().map(|c| async { write_blob(c) }).collect();
    futures::future::join_all(futures).await;
}
// 256 chunks / 8 并发 × 2ms = 64ms total (8x 提升)
```

**关键点**：
- 并发度限制为 8（避免 Volume Server 过载和网络拥塞）
- 保留 flush_lock（per-inode），防止后台 flusher 和 release 争用
- 错误处理：单个 chunk flush 失败时 re-mark_dirty，不影响其他 chunk
- lease_token 传递：batch 内共享同一个 lease（同一 inode 同一 stripe）

**预期收益**：flush 吞吐 8 倍提升，release 延迟从 512ms 降到 64ms。

#### P1-b: 并行 read + 增大预取（读路径优化）

**当前代码**：[fuse.rs:1650-1735](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1650-L1735) read 方法
```rust
// 串行：每个 cache miss chunk 一次 block_on(read_blob)
for chunk in missing_chunks {
    self.client.read_blob(...)  // ~2ms per chunk
}
// 顺序读 256MB: 256 × 2ms = 512ms → 500 MiB/s 理论上限
```

**改为**：
```rust
// 并行：缺失的 chunks 一次性并行读取
let futures: Vec<_> = missing_chunks.iter().map(|c| async { read_blob(c) }).collect();
let results = futures::future::join_all(futures).await;
// 8 chunks × 2ms = 2ms (batch 内并行)
```

**同时增大预取窗口**：
- `PREFETCH_CHUNKS: u64 = 2` → `8`
- 顺序读时预取 8 个额外 chunk，减少 cache miss

**关键点**：
- 并发度限制为 8（与 flush 一致）
- lease 获取改为批量（`acquire_lease_batch` 已在 P2-2 实现）
- 预取数据异步写入 chunk_cache，不阻塞当前 read

**预期收益**：顺序读从 1292 提升到 ~5000+ MiB/s，随机读从 7.8 提升到 ~30+ MiB/s。

#### P2-a: write lease 跳过 read-before-write

**当前代码**：[fuse.rs:1933-1973](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1933-L1973)
```rust
// 随机写预创建文件时，每个 chunk 首次写入需要 read-before-write
if content_size_before_write > chunk_start_offset {
    let existing_data = self.client.read_blob(...);  // 额外 RPC
}
```

**改为**：
```rust
// 持有 write lease 时，其他客户端无法修改数据
// chunk_cache 中的数据是权威的，无需 read-before-write
if has_write_lease && chunk_cache_has_data {
    // 直接用 chunk_cache 中的数据，跳过 read_blob
} else if content_size_before_write > chunk_start_offset {
    let existing_data = self.client.read_blob(...);
}
```

**关键点**：
- 需要确认 write lease 已获取（open 时或首次 write 时）
- 只对"部分覆盖"的 chunk 有效（完全覆盖/完全空 chunk 已优化）
- 无 write lease 时保留 read-before-write（正确性优先）

**预期收益**：随机写从 39.0 提升到 ~60+ MiB/s（消除 read RPC 开销）。

#### P2-b: 短 TTL 替代 TTL=0

**当前代码**：[fuse.rs:30-35](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L30-L35)
```rust
const TTL: Duration = Duration::ZERO;      // 禁用内核缓存
const TTL_OPEN: Duration = Duration::ZERO;  // 打开文件也禁用
```

**改为**：
```rust
const TTL: Duration = Duration::from_millis(100);  // 100ms 内核缓存
const TTL_OPEN: Duration = Duration::ZERO;          // 打开文件仍禁用（lease 权威）
```

**关键点**：
- 100ms TTL 足以减少重复 getattr 的 FUSE 通信开销
- 跨客户端修改通过 Invalidate 通知失效（已有机制）
- 打开文件 TTL=0（lease 期间权威，不依赖内核缓存）

**预期收益**：`ls -l`、`find`、`stat` 等元数据密集操作 2-5 倍提升。

#### P3-a: 批量 write_blob (send_batch)

**目标**：将多个 chunk 的写入合并为一个 RDMA doorbell 请求。

**前置条件**：Transport trait 抽象（TCP/RDMA 统一接口）已实现。

**改动**：`volume_client.rs` 新增 `write_blob_batch` 方法，内部用 `send_batch` 合并多个数据请求。

**预期收益**：RDMA 场景下减少 doorbell 开销，TCP 场景收益较小。

#### P3-b: 启用 FUSE writeback cache

**改动**：`fuse.rs init` 中启用 `FsOptions::WRITEBACK_CACHE`。

**风险**：writeback cache 下内核缓冲 write，跨客户端可见性延迟。需要确保 `fsync`/`close` 时正确 flush + invalidate。

**预期收益**：小块顺序写（4K bs）合并为大批量 write，减少 FUSE 通信次数。

### 实施顺序

| 阶段 | 优化项 | 依赖 | 预期收益 |
|------|--------|------|---------|
| ✅ P0 | BIG_WRITES + MAX_PAGES | 无 | 30.7x 顺序写 |
| → P1-a | 并行 flush | 无 | 8x flush 吞吐 |
| → P1-b | 并行 read + 预取 | 无 | 4x read 吞吐 |
| P2-a | write lease 跳过 read-before-write | P1 验证后 | 1.5x 随机写 |
| P2-b | 短 TTL | 无 | 2-5x 元数据操作 |
| P3-a | 批量 write_blob | Transport trait | RDMA 场景提升 |
| P3-b | FUSE writeback cache | 一致性验证 | 小块写合并 |

### 历史对比

| 阶段 | 顺序写 | 顺序读 | 随机写 | 随机读 | 关键变化 |
|------|--------|--------|--------|--------|---------|
| Phase 1 (CRDT 时代) | GB/s 级 | GB/s 级 | — | — | 无 lease，无 Filer 强一致 |
| Phase 1 (Raft, Step 7) | 53.2 MiB/s | 122 MiB/s | 56.9 MiB/s | 18.0 MiB/s | Filer 强一致，无客户端缓存 |
| Phase 2 (修复前) | 18.7 MiB/s | 92.2 MiB/s | 9.3 MiB/s | 8.2 MiB/s | lease + callback + read-before-write |
| Phase 2 (当前) | 53.4 MiB/s | 252 MiB/s | 23.8 MiB/s | 6.5 MiB/s | read-before-write 优化 + getattr 修复 |
| **P0 修复后（实测）** | **1642 MiB/s** | **1292 MiB/s** | 39.0 MiB/s | 7.8 MiB/s | BIG_WRITES + MAX_PAGES |

**结论**：P0 修复（`init` 启用 `BIG_WRITES` + `MAX_PAGES`）实测提升 30.7 倍顺序写、5.1 倍顺序读，恢复 GB/s 级别。随机读写提升较小（4K 不受 max_write 限制），后续优化需聚焦 P1（并行 flush/read + 减少 block_on）。




