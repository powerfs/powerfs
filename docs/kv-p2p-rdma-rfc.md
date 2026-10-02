# RFC：P2P RDMA 直传 KV Cache 读路径（方案五）

> **Status**: Final (v3 + 文字一致性修正，评审 APPROVED)
> **Phase**: D（路线图第四阶段，本期仅产出 RFC，不写实现）
> **Author**: AI-assisted
> **Upstream**: [kv-mooncake-borrow-plan.md](file:///home/portion/powerfs/docs/kv-mooncake-borrow-plan.md) 方案五
> **Depends on**: Phase A（孤儿 needle GC）、Phase B（成组驱逐）、Phase C（读热度）已完成
> **Review**: [kv-p2p-rdma-rfc-review.md](file:///home/portion/powerfs/docs/kv-p2p-rdma-rfc-review.md)

## 1. 动机与问题

当前 PowerFS KV Cache 的读路径：

```
Client → Master gRPC GetBlock → Master fetch_block → Volume net (TCP 890x)
        ← Master gRPC 返回 ← fetch_block 返回
```

- **路径长**：每次读都走 Master 中转，Master 成为热点。
- **带宽浪费**：Master 到 Volume 是 TCP 数据面，无 RDMA 加速；block 大（默认 2MB）时 CPU 开销显著。
- **Mooncake 做法**：LLM 推理集群中，Prefill 节点把 KV Cache 直传给 Decode 节点（P2P RDMA），不经中心节点。

## 2. 目标

- **减少 Master 读路径中转**：Client 能直接从 Volume 读取 block，Master 只负责路由（返回 fid 和 volume 位置）。
- **利用 RDMA**：数据面从 TCP 升级到 RDMA（当硬件可用时），保持 TCP fallback。
- **保持正确性**：不改变 Phase A/B/C 的语义（GC、pin、成组驱逐、读热度统计）。

## 3. 非目标（本期不做）

- 不实现 Client 直接访问 Volume 的**写路径**（写仍走 Master 中转，保证 raft 一致性）。
- 不引入 GPUDirect / NVLink 等 GPU 内存直接传输。
- 不改 Mooncake 的异步 oplog / 弱仲裁等设计。

## 4. 方案概览

### 4.1 架构变化

**现状（Phase A-C 后）**：

```
┌────────┐  GetBlock   ┌────────┐  fetch_block   ┌────────┐
│ Client │────────────→│ Master │────────────────→│ Volume │
│        │←────────────│        │←────────────────│ (TCP)  │
└────────┘  block data └────────┘   needle data   └────────┘
```

**目标（P2P 直传）**：

```
┌────────┐  GetBlockMeta   ┌────────┐
│ Client │────────────────→│ Master │  ← 只返回 fid + volume 位置（无数据）
│        │←────────────────│        │
│   ↓    │   fid + addr    └────────┘
│   │    │
│   ↓    │  read_needle    ┌────────┐
│        │────────────────→│ Volume │  ← RDMA（若 Volume 配置为 RDMA）
│        │←────────────────│ (RDMA) │     失败时回退 Master GetBlock
└────────┘   needle data   └────────┘
```

### 4.2 关键设计

1. **Master 返回 GetBlockMeta**：新增/扩展 gRPC 方法，只返回 block 的 fid、volume 位置（IP + 端口 + transport 类型）、大小，**不返回数据**。
2. **Client 直连 Volume**：Client 用返回的位置直连 Volume 的 RDMA/TCP 端口读取 needle 数据。
3. **RDMA 传输**：Volume 数据面已有 RDMA 支持（`rdma_device`、`require_rdma` 配置）；Client 侧新增 RDMA client transport（依赖 powerfs-net）。
4. **回源兼容**：非驻留 block 的 Master 回源逻辑保留，作为 fallback（当 Client 不支持直连或 RDMA 不可用时）。
5. **GC 兼容**：依赖 Phase A GC 宽限期（600s）天然覆盖单次读全过程，不引入额外登记。
6. **安全认证**（新增）：Client 直连 Volume 需通过认证（复用现有 ClientCert TLV + 注册 token 机制）。

## 5. 详细设计

### 5.1 协议变更

新增 gRPC 方法：

```protobuf
rpc GetBlockMeta (GetBlockMetaRequest) returns (GetBlockMetaResponse);

message GetBlockMetaRequest {
    uint64 block_id = 1;
    // Client 能力声明（用于 Master 返回兼容的位置列表）
    bool supports_rdma = 2;
    bool supports_tcp = 3;
}

enum TransportType {
    TCP = 0;
    RDMA = 1;
}

message BlockLocation {
    uint64 volume_id = 1;
    string address = 2;           // IP:port
    TransportType transport = 3;  // TCP 或 RDMA
    // rdma_device 是服务端本地配置，不下发给 Client
}

message GetBlockMetaResponse {
    bool found = 1;
    uint64 block_id = 2;
    string fid = 3;          // volume_id,cookie,file_key
    uint64 size_bytes = 4;
    // checksum 暂无来源（KVBlockMeta 不存），暂不返回
    repeated BlockLocation locations = 5;  // 多副本位置（如 EC）
}
```

**保留现有 GetBlock** 作为兼容路径（返回数据），新增 GetBlockMeta 只返回元数据。

### 5.2 Client 读路径

```rust
// Client 侧逻辑（伪代码）
async fn read_block(block_id: u64) -> Result<Vec<u8>> {
    // 1. 向 Master 查询位置（声明自身能力）
    let meta = master_client.get_block_meta(GetBlockMetaRequest {
        block_id,
        supports_rdma: has_rdma_device(),
        supports_tcp: true,
    }).await?;
    if !meta.found {
        return Err(NotFound);
    }

    // 2. 选择最优位置（优先 RDMA，Client 不支持则 fallback TCP）
    let client_supports_rdma = has_rdma_device();
    let loc = meta.locations.iter()
        .find(|l| l.transport == TransportType::Rdma && client_supports_rdma)
        .or_else(|| meta.locations.iter().find(|l| l.transport == TransportType::Tcp));

    let data = match loc {
        Some(l) => {
            // 直连 Volume 读取（带认证）
            let mut client = VolumeClient::connect_with_auth(&l.address, l.transport, &client_cert).await?;
            client.read_needle(&meta.fid, meta.size_bytes).await?
        }
        None => {
            // fallback：回退到 Master GetBlock 中转
            return master_client.get_block(block_id).await?;
        }
    };

    // 3. 通知 Master 登记 read_count（Phase C 读热度统计）
    master_client.record_block_access(block_id, true).await?;

    Ok(data)
}
```

### 5.3 Master 实现

Master 的 `GetBlockMeta` handler：

1. 查 engine 的 block_id → fid + 元数据；
2. 查 topology 的 volume_id → 节点地址；
3. **从心跳 TLV 解析 transport 能力**（需扩展心跳协议，见 §5.6）；
4. 根据 Client 能力（`supports_rdma` / `supports_tcp`）过滤位置列表；
5. 返回位置列表（不含数据）。

**计数兼容**：Master 在返回 GetBlockMeta 时**不**更新 read_count（Phase C 的读热度）；Client 直连成功后需**回执**（见 §5.5）。

### 5.4 Volume 端

Volume 已有 net server（TCP 890x），需：

1. **RDMA 端口**：确认 RDMA 配置的端口（当前 `rdma_device` 配置在 `powerfs-net/src/transport.rs`）。
2. **read_needle 接口**：已存在（TCP），需确认 RDMA transport 是否暴露同接口。
3. **认证检查**（新增）：接受 Client 连接时验证 ClientCert TLV + 注册 token。

### 5.5 读热度回执

Phase C 的 read_count 在 Master 的 `fetch_block` 中更新。Client 直连后 Master 不知道读成功。

**新增 gRPC 方法**：

```protobuf
rpc RecordBlockAccess (RecordBlockAccessRequest) returns (RecordBlockAccessResponse);

message RecordBlockAccessRequest {
    uint64 block_id = 1;
    bool success = 2;  // 读取是否成功（失败也上报，用于统计）
}

message RecordBlockAccessResponse {
    bool success = 1;
}
```

**Master 处理逻辑**：回执是观测登记，不用于 GC 保护；客户端必须发往 leader（follower 的本地更新不会同步）。

- `success=true`：复用 read_count 计数函数（与回源成功同一路径），block 已删除则 no-op；
- `success=false`：**不**增 read_count（失败未产生有效读），计入 `KVCacheStats.failed_direct_reads` 并经 `GetStatsResponse.failed_direct_reads` 导出——否则"失败上报"无消费方；
- 恒返回 `success=true`（未知块也接受；存在性不做侧信道）。

> `failed_direct_reads` 是 leader 进程内计数器，failover/重启归零，监控消费方需处理重置点（与 hits/evictions 同性质，未接入 Prometheus）。

### 5.6 GC 交互与时序分析（重点）

**关键问题**：Client 直读期间，Phase A 的 GC 宽限期到后可能删除 needle。

**分析**：
- Phase A GC 的宽限期默认 600s；
- Client 从 Master 获取位置（GetBlockMeta）到读完数据，耗时远小于 600s（网络 RTT 级）；
- 即使 GetBlockMeta 与开始读之间有延迟，Client 应在合理时间内完成读取（如 30s 超时）。

**结论**：**不引入额外的 referenced 登记**。GC 宽限期天然覆盖单次读的全过程。Client 崩溃或长挂读的风险由应用层超时控制。

**约束**：Client 获取 GetBlockMeta 后，应**尽快**发起直连读，不应缓存位置超过 GC 宽限期。

### 5.7 心跳扩展（transport 能力上报）

当前心跳 TLV 只上报 `grpc_port`（数据面地址），不上报 transport 类型与 RDMA 能力。需扩展：

```protobuf
// 心跳消息新增字段（实际实现用 TLV，此处用 protobuf 示意）
message HeartbeatRequest {
    // ... 现有字段 ...
    // 节点级 capability（非 per-volume）
    repeated VolumeCapability capabilities = 10;
}

message VolumeCapability {
    uint64 volume_id = 1;
    TransportType transport = 2;  // TCP / RDMA
    uint32 port = 3;              // 该 transport 的监听端口
    string rdma_device = 4;       // RDMA 设备名（仅 transport=RDMA 时）
}
```

Master 存储这些能力到 topology，`GetBlockMeta` 据此填充 `BlockLocation.transport`。

### 5.8 安全认证设计

Client 直连 Volume 绕过 Master，需独立认证：

1. **ClientCert TLV**：Client 连接时发送证书（现有机制）；
2. **注册 token**：Volume 验证 token 有效性（新机制，Volume 侧新增缓存/验证）；
3. **fid 校验**：Volume 验证 fid 格式与 volume_id 匹配，cookie 校验（防止跨 Volume 访问）。

**认证流程**：
```
Client ──connect──→ Volume
Client ──ClientCert TLV + token──→ Volume
Volume ──本地验证 token（缓存）──→ accept/reject
```

**信任模型**：集群级 token，无 block 级授权。读取权限由 Master 的 GetBlockMeta 控制（返回位置即授权）。

### 5.9 单 listener 与 fallback 策略

**现状**：Volume 的 `AutoTransport::bind` 只绑定**一种** transport（优先 RDMA，否则 TCP）。同 Volume 不会同时暴露 RDMA+TCP 两条 listener。

**影响**：
- 若 Volume 配置为 RDMA（`rdma_device` 非空），Client 直连失败时**无法回退到同 Volume 的 TCP**（因为没绑 TCP）。
- 此时 fallback 只能是 **Master GetBlock 中转**（走现有 TCP 路径）。

**策略**：
1. GetBlockMeta 返回的 `BlockLocation` 列表中，每条 location 的 transport 与 Volume 实际绑定的 transport 一致；
2. Client 尝试 RDMA 直连失败后，**回退到 Master GetBlock**（而非尝试同 Volume TCP）；
3. 未来如需双 listener（RDMA+TCP 同时存在），需改造 `AutoTransport`，不在本期范围。

**配置说明**：`rdma_port` 暂不需要（单 listener 时端口就是现有数据端口，RDMA 复用或独立由 transport 层决定）。

### 5.10 与 Mooncake 的差异说明

**本方案是 Client-direct，不是 Mooncake 的同构 P2P**：
- Mooncake：Prefill 节点（也是 Volume）直传 Decode 节点（也是 Volume），同构；
- PowerFS：Client（推理进程）直连 Volume（存储节点），异构。

**为何不直接复用 Mooncake Transfer Engine**：见附录 A。

**借鉴点**：元数据与数据面分离（Master 只管理元数据，数据直传）；RDMA 编程模型；TCP fallback。

## 6. 部署与配置

### 6.1 配置项

```toml
[volume]
# 已有配置
rdma_device = "mlx5_0"
require_rdma = false

# 本期无需新增配置（RDMA 端口由 transport 层决定）
```

### 6.2 兼容性

- **Client 侧**：检测环境是否有 RDMA 设备（`ibv_devices`）；无则 Master 返回 TCP location。
- **混合集群**：部分 Volume 支持 RDMA、部分不支持时，Master 按 Volume 实际配置返回对应 location。
- **心跳兼容**：旧版 Volume 不上报 transport 能力，Master 默认填充 `transport=TCP`。

## 7. 风险与缓解

| 风险 | 影响 | 缓解 |
|------|------|------|
| Client 直连 Volume 失败 | 读失败 | fallback 到 Master GetBlock 中转 |
| RDMA 设备不可用 | 无法加速 | 自动降级到 TCP |
| 读热度统计不准（方案 B） | 热点判断偏差 | 用 RecordBlockAccess 回执 |
| Master 返回的位置过期 | Client 连错 Volume | Client 校验 fid 存在性，失败回退 |
| 多副本（EC）位置选择 | 读到旧数据 | 位置列表按优先级排序，Client 优先选 primary |
| Client 直读 needle 被 GC 误删 | 数据丢失 | 依赖 GC 宽限期（600s）覆盖单次读全过程；Client 应在获取位置后尽快读取 |
| Client 认证失败 | 无法直连 | fallback 到 Master GetBlock 中转 |
| 心跳扩展不兼容旧版 Volume | Master 无法识别 transport | 默认 TCP，不影响功能 |

## 8. 验收标准（AC）

- **AC-1**：Client 能通过 GetBlockMeta 获取 block 位置并直连 Volume 读取数据，数据字节正确。
- **AC-2**：RDMA 可用时优先使用 RDMA；RDMA 不可用时回退到 Master GetBlock 中转（非同 Volume TCP fallback）。
- **AC-3**：Master 的 GetBlockMeta 不返回数据，只返回 fid + 位置。
- **AC-4**：读热度统计在 Client 直连场景下仍准确（RecordBlockAccess 回执）。
- **AC-5**：不影响 Phase A/B/C 的正确性（GC、pin、成组驱逐、E2E 测试通过）。
- **AC-6**：Client 直读的 needle 在 GC 宽限期内不被误删（依赖 GC 宽限期，非额外登记）。
- **AC-7**：Client 直连需通过认证（ClientCert TLV + 注册 token）。

## 9. 实施阶段

- **Phase D.1（本期）**：产出本 RFC，评审通过。
- **Phase D.2**：心跳扩展（transport 能力上报，节点级）+ Master topology 存储。
- **Phase D.3**：实现 GetBlockMeta gRPC + Master 位置查询。
- **Phase D.4**：实现 RecordBlockAccess gRPC（仅读热度统计）。
- **Phase D.5**：Volume 认证检查（ClientCert TLV + token 本地缓存验证）。
- **Phase D.6**：Client RDMA transport + 直连读路径（依赖 powerfs-net）。
- **Phase D.7**：E2E 验证（RDMA 硬件环境 + Master GetBlock fallback + GC 交互）。

## 10. 参考资料

- Mooncake Transfer Engine（C++/Rust 绑定）：`/home/portion/powerfs/Mooncake/mooncake-transfer-engine/`
- Mooncake EP（Elastic Parallelism）：`/home/portion/powerfs/Mooncake/mooncake-ep/`
- PowerFS RDMA transport：`/home/portion/powerfs/powerfs-net/src/transport_rdma.rs`
- PowerFS net client：`/home/portion/powerfs/powerfs-net/src/client.rs`

## 附录 A：与 Mooncake 的对比

| 维度 | Mooncake | PowerFS（本方案） |
|------|----------|-------------------|
| 场景 | LLM 推理（Prefill→Decode KV Cache 传输） | 通用分布式存储（KV Cache 读路径优化） |
| 架构 | 同构 P2P（Prefill 节点直传 Decode 节点） | Client-direct（Client 直连 Volume） |
| 一致性 | 弱一致（异步 oplog，允许短暂不一致） | 强一致（raft 复制，GC 有 referenced 检查） |
| 传输 | RDMA（Transfer Engine，支持 TCP fallback） | RDMA（powerfs-net transport_rdma，TCP fallback） |
| 元数据 | 中心节点（Conductor） | Master（raft 强一致） |
| 数据校验 | 应用层（LLM 框架） | 暂无（可后续加 CRC32） |

**为何不直接复用 Mooncake Transfer Engine**：
- Mooncake 是 C++ 库，PowerFS 是 Rust 生态；虽有 Rust 绑定，但引入外部依赖增加维护成本。
- Mooncake 的 P2P 是同构节点间传输（Prefill→Decode），PowerFS 是 Client→Volume 的异构传输，场景不同。
- Mooncake 的弱一致设计不适合 PowerFS 的强一致存储语义。

**借鉴点**：
- Transfer Engine 的 RDMA 编程模型（QP、MR、CQ）。
- 元数据与数据面分离（Conductor 只管理元数据，数据直传）。
- TCP fallback 机制（RDMA 不可用时自动降级）。
