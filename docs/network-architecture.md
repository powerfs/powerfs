# PowerFS 网络架构 (powerfs-net)

> 统一网络通信层，为 FUSE (Rust) 和内核 (C) 客户端提供与 PowerFS 服务端
> (Master / Filer / Volume) 的高性能二进制通信。

## 1. 总体架构

```text
FUSE Client (Rust)          Kernel Client (C)
       │                           │
       ▼                           ▼
  PowerFsNetClient          powerfs-net (C impl)
  (Rust impl)               (same wire protocol)
       │                           │
       ▼                           ▼
  TCP Socket  ───────────────►  Master / Filer / Volume Server
                                    │
                                    ▼
                            PowerFsNetServer
                            ├── Acceptor (1 task)
                            ├── IoLoop × N (tokio tasks)
                            ├── Worker pool (Semaphore 限并发)
                            └── ConnRegistry + ServerConnectionManager
```

### 设计原则

- **固定线程模型**: N 个 IoLoop + M 个 Worker，线程数不随客户端数增长
- **IO 与业务分离**: IoLoop 只负责帧收发，Worker 只负责业务处理
- **单一数据源**: 每条连接的所有状态 (连接状态/lease/统计/通知) 统一在 `ClientConn` 中管理
- **网络层无业务语义**: 网络层不包含文件系统操作 (lookup/create/delete 等)，仅提供 `send_request` / `send_notify` 原语

## 2. 服务端架构

### 2.1 PowerFsNetServer

服务端主入口，负责初始化 Acceptor、IoLoop、Worker 等组件。

| 绑定方式 | 说明 |
|---------|------|
| `bind(addr, port, handler)` | 最简模式，无 session 管理 |
| `bind_with_manager(addr, port, handler)` | 自动创建 ConnRegistry + ServerConnectionManager |
| `bind_with_pipeline(addr, port, handler, pipeline, config)` | 自定义中间件管道 |
| `bind_with_registry(addr, port, handler, registry)` | 共享外部 ConnRegistry (用于 InodeNotifier 等需要共享注册表的场景) |

### 2.2 Acceptor + IoLoop + Worker

```text
Acceptor (1 task)
  │ accept TCP → handshake → create ClientConn → register to ConnRegistry
  │ assign to IoLoop (round-robin)
  ▼
IoLoop × N (tokio tasks)
  │ read frames → push Work to WorkQueue
  │ write responses/notifications from outbound_tx
  ▼
Worker pool (Semaphore 限并发)
  │ dequeue Work → process via NetHandler → send response via ClientConn
```

- **IoLoop**: 每个管理多个连接的读循环和写循环。读取帧后封装为 `Work` 推入有界 `WorkQueue`
- **Worker**: 从 `WorkQueue` 取出 `Work`，通过 `NetHandler` trait 分发给业务处理器
- **WorkQueue**: 有界 channel (默认 4096)，防止积压

### 2.3 ClientConn — 服务端连接抽象

每条客户端连接对应一个 `ClientConn`，统一管理:

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | `u64` | 握手时分配的客户端 ID |
| `holder_uuid` | `RwLock<Option<String>>` | lease 持有者标识 |
| `addr` | `SocketAddr` | 客户端地址 |
| `client_type` | `ClientType` | Fuse / Kernel / Admin |
| `state` | `RwLock<ConnState>` | 连接状态机 |
| `policy` | `RwLock<ClientPolicy>` | QoS 策略 (优先级/限速/并发) |
| `stats` | `RwLock<ClientStats>` | 请求数/错误数/字节数 |
| `held_leases` | `RwLock<HashSet<u64>>` | 持有的 inode lease (断连清理) |
| `rate_limiter` | `RwLock<RateLimiter>` | Token Bucket 限流器 |
| `outbound_tx` | `UnboundedSender<Vec<u8>>` | 出站帧通道 (响应 + 通知) |
| `close_handle` | `RwLock<Option<CloseHandle>>` | 主动断开句柄 |

#### 连接状态机

```text
Active ──disconnect()──► Closing ──► Closed
  │                        ▲
  └──suspend()/限流──► Suspended
```

### 2.4 ConnRegistry — 全局连接注册表

线程安全 (DashMap) 的连接注册表，替代分散的 `client_id_map`。

| 方法 | 说明 |
|------|------|
| `register(conn)` | 注册新连接 |
| `unregister(id)` | 注销连接 (返回 conn 供 on_disconnect 清理) |
| `get(id)` / `get_by_holder(holder)` | 查询连接 |
| `disconnect(id)` | 主动断开 |
| `set_policy(id, policy)` | 动态设置 QoS |
| `notify(id, &msg)` | 向指定客户端推送通知 |
| `broadcast(&msg)` | 广播通知 |
| `metrics_snapshot()` | 聚合指标快照 |
| `health_check()` | 健康检查 |
| `list()` | 列出所有连接信息 |

### 2.5 ServerConnectionManager — 服务端基础设施门面

薄封装层，委托 `ConnRegistry` 管理连接状态，保留:

- **中间件管道** (`RequestPipeline`): 日志 + 指标 + 追踪
- **指标中间件** (`MetricsMiddleware`): 请求级聚合指标
- **通知构建辅助**: 构建 TLV 消息并通过 `ConnRegistry` 推送

> 历史说明: 旧版 `ServerConnectionManager` 维护了独立的 `HashMap<client_id, ClientSession>`，
> 与 `ConnRegistry` 的 `ClientConn` 数据重复，导致状态不一致。`ClientSession` 已移除，
> 所有连接状态统一在 `ClientConn` 中。

## 3. 客户端架构

### 3.1 PowerFsNetClient

客户端主结构，提供 `connect` / `disconnect` / `send_request` / `send_notify` / `ping` 原语。

#### 连接状态机

```text
  Disconnected ──connect()──► Connecting ──handshake_ok──► Connected
       ▲                          │                           │
       │                     handshake_fail               error/disconnect
       │                          ▼                           │
       └──────────────────── Reconnecting ◄──────────────────┘
                                 │
                             3× fail
                                 ▼
                           Disconnected
```

| 状态 | 说明 |
|------|------|
| `Disconnected` | 初始状态或显式断开后 |
| `Connecting` | TCP 连接 + 握手中 |
| `Connected` | 连接建立，send_task/recv_loop 运行中 |
| `Reconnecting` | `reconnect_internal()` 重连中 |

#### 连接级指标 (ClientMetrics)

无锁原子计数器，热路径零开销:

| 指标 | 说明 |
|------|------|
| `requests_sent` | 已发送请求总数 |
| `responses_received` | 已接收响应总数 |
| `request_errors` | 请求错误数 (超时/网络/服务端错误) |
| `reconnect_attempts` | 重连尝试次数 |
| `reconnect_successes` | 重连成功次数 |
| `reconnect_failures` | 重连失败次数 (3 次耗尽) |

#### 事件监听 (ClientEventListener)

```rust
pub trait ClientEventListener: Send + Sync {
    fn on_connected(&self, addr: &str, port: u16) {}
    fn on_disconnected(&self, addr: &str, port: u16) {}
    fn on_reconnect_attempt(&self, addr: &str, port: u16, attempt: u32) {}
    fn on_reconnect_failed(&self, addr: &str, port: u16, attempts: u32) {}
}
```

通过 `client.set_event_listener(Box::new(listener))` 安装，无需轮询 `is_connected()`。

#### 内部架构

```text
send_request() ──► frame_tx (mpsc) ──► send_task (owns write_half)
                                         │ write_all with timeout
                                         ▼
                                      TCP Socket
                                         │
                                         ▼
recv_loop (owns read_half) ──► pending_requests (DashMap<seq, oneshot::Sender>)
                              ├── response → dispatch by seq
                              └── NOTIFY → NotificationHandler
```

- **send_task**: 独立 task 拥有 `write_half`，通过 mpsc channel 接收帧，避免写锁竞争
- **recv_loop**: 独立 task 拥有 `read_half`，读取响应帧并按 seq 分发到 pending requests
- **pending_requests**: DashMap (16 路分片锁)，高并发下无单锁瓶颈
- **自动重连**: `send_request` 检测断连后自动触发 `reconnect_internal` (最多 3 次)

### 3.2 ClientConnPool — 客户端连接池

管理 `PowerFsNetClient` 实例，按 `"addr:port"` 复用连接。

| 特性 | 说明 |
|------|------|
| 连接复用 | 相同 `addr:port` → 相同 `Arc<PowerFsNetClient>` |
| 延迟连接 | 首次 `get_or_connect` 时才建立 TCP |
| 自动重连 | 连接断开后由 `PowerFsNetClient` 内部自动重连 |
| 通知处理 | 通知 handler 在重连后自动重新安装 |

### 3.3 NetRpcClient — 服务间 RPC 客户端

用于服务间 TLV 通信 (Raft 消息、RegisterFiler、心跳等)，替代各服务重复的连接逻辑。

| 模式 | 说明 |
|------|------|
| `call_once(addr, port, msg_type, body)` | 一次性 RPC，每次新建短连接 |
| `NetRpcClient::call(msg_type, body)` | 持久连接，多次调用复用 |

> 注意: `NetRpcClient` 不处理 `STATUS_ERR_REDIRECT`，重定向是路由层关注点，
> 由调用方 (如 `TlvMasterClient`) 处理。

## 4. TLV 协议

### 4.1 帧格式

```text
┌─────────────── FrameHeader (16 bytes) ───────────────┐
│ msg_type (2) │ flags (2) │ seq (4) │ body_len (4) │ data_len (4) │
└──────────────────────────────────────────────────────┘
┌───────────── Body (TLV-encoded, body_len bytes) ─────┐
│ FieldId(2) + Type(1) + Len(4) + Value(N) │ ...      │
└──────────────────────────────────────────────────────┘
┌───────────── Data (raw bytes, data_len bytes) ───────┐
│ (binary payload, e.g. file data)                     │
└──────────────────────────────────────────────────────┘
```

### 4.2 帧类型

| flags | 说明 |
|-------|------|
| `REQUEST` | 请求帧 (Client → Server) |
| `RESPONSE` | 响应帧 (Server → Client) |
| `NOTIFY` | 通知帧 (Server → Client，无需响应) |

### 4.3 握手

连接建立后，客户端发送 `HandshakeRequest` (client_type + client_id)，
服务端返回 `HandshakeResponse` (server_id + status)。

## 5. 通知机制 (Server → Client)

```text
Filer (InodeNotifier)                    FUSE Client
       │                                      │
       ├── broadcast_invalidate_notification  │
       │   → ConnRegistry.broadcast(&msg)     │
       │   → ClientConn.notify(&msg)          │
       │   → outbound_tx.send(msg.to_frame()) │
       │                                      │
       │                              recv_loop receives NOTIFY frame
       │                              → NotificationHandler.handle_notification()
       │                              → InvalidateHandler invalidates cache
```

- `InodeNotifier` 管理 inode → client_id 订阅关系
- 通知通过 `ClientConn.outbound_tx` 直接推送，无中间 channel 转发
- 客户端 `recv_loop` 识别 `NOTIFY` 帧并分发到 `NotificationHandler`

## 6. 中间件管道

```text
Request → LoggingMiddleware → MetricsMiddleware → TracingMiddleware → Handler
                                                                    │
Response ◄─────────────────────────────────────────────────────────┘
```

| 中间件 | 说明 |
|--------|------|
| `LoggingMiddleware` | 请求日志 |
| `MetricsMiddleware` | 请求计数 + 延迟统计 |
| `TracingMiddleware` | 分布式追踪 |
| `RateLimitMiddleware` | 请求级限流 |

通过 `PipelineBuilder` 或 `RequestPipeline::new().add_middleware(...)` 构建。

## 7. 管理接口 (AdminServer)

提供 HTTP REST API 用于监控和管理:

| 端点 | 说明 |
|------|------|
| `/sessions` | 列出所有连接 |
| `/sessions/{id}` | 连接详情 (状态/统计/策略) |
| `/metrics` | 聚合指标 |
| `/health` | 健康检查 |
| `/sessions/{id}/disconnect` | 强制断开连接 |

## 8. 模块结构

```text
powerfs-net/src/
├── lib.rs                  # 公共 API 导出
├── client.rs               # 客户端 (PowerFsNetClient + ClientState + ClientMetrics)
├── client_conn.rs          # 服务端连接抽象 (ClientConn + ConnRegistry + RateLimiter)
├── client_pool.rs          # 客户端连接池 (ClientConnPool)
├── server.rs               # 服务端 (PowerFsNetServer: Acceptor + IoLoop + Worker)
├── server_connection.rs    # 服务端管理门面 (ServerConnectionManager + NetHandler)
├── io_loop.rs              # IO 循环 (帧收发)
├── worker.rs               # Worker (业务处理)
├── work.rs                 # Work 封装 (请求 + 连接引用)
├── protocol.rs             # TLV 协议 (帧格式 + 消息类型)
├── serialize.rs            # TLV 编解码 (TlvEncoder + TlvDecoder)
├── middleware.rs           # 中间件管道 (Logging + Metrics + Tracing)
├── request_context.rs      # 请求上下文 (ClientInfo + RequestContext)
├── rpc_client.rs           # 服务间 RPC 客户端 (call_once + NetRpcClient)
└── admin_server.rs         # 管理 API 服务器
```

## 9. 关键设计决策

| 决策 | 理由 |
|------|------|
| 删除 `ConnectionManager` 和 `Transport` trait | 死代码，未实现或已被替代 |
| `ClientConfig` → `ClientPolicy` 重命名 | 消除与 `client::ClientConfig` 的命名冲突 |
| `ClientSession` 合并到 `ClientConn` | 消除服务端双重连接跟踪，状态一致性 |
| 移除 `PowerFsNetClient` 业务方法 | 网络层不应包含文件系统语义 (lookup/create/delete) |
| 通知通过 `outbound_tx` 直接推送 | 减少中间 channel 转发，降低资源消耗 |
| `bind_with_registry` 支持共享 ConnRegistry | Filer 的 InodeNotifier 需要在 bind 前访问注册表 |
| 客户端状态机替代 bool 标志 | 区分 Connecting/Reconnecting/Connected/Disconnected |
| 无锁原子指标 | 热路径零开销，避免锁竞争 |
