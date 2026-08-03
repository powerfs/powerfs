# PowerFS 文章重新规划方案

## 一、背景：架构演进与文章脱节

PowerFS 经历了一次重大架构演进：**从 OR-Set CRDT 弱一致元数据 → Filer Raft 强一致元数据**。现有 articles 目录下 15 篇文章几乎全部基于已废弃的 OR-Set CRDT 架构编写，与当前代码实现严重脱节。

### 架构变更总结

| 维度 | 旧架构（文章描述） | 新架构（当前代码） |
|------|-------------------|-------------------|
| **元数据一致性** | OR-Set CRDT 弱一致 + Delta 同步 | Filer Raft 强一致（propose/apply） |
| **元数据分片** | 客户端本地 OR-Set 缓存 | Filer 按 inode 分片，独立 Raft groups |
| **写路径** | 本地 OR-Set 写即返回（写零阻塞） | Raft propose → apply → poll 确认 |
| **冲突处理** | CRDT 自动收敛 + .conflicts/ 目录 | Raft 强一致，无冲突 |
| **Delta 同步** | 2s 增量 + 30s 全量对齐 | 已废弃，Invalidate 通知机制替代 |
| **S3 Gateway 位置** | Master 内置（端口 9000） | Filer 内置（S3Handler） |
| **通信协议** | gRPC | TLV（Type-Length-Value）+ Transport trait |
| **数据一致性** | CRDT + 客户端缓存 | Lease 锁（per-stripe 64MB）+ Invalidate 通知 |
| **file_key 设计** | file_key = NeedleId（语义重载） | file_key 块分配（FILE_KEY_BLOCK_SIZE=1M） |

### 新架构核心组件

```
┌──────────────────────────────────────────────────────────────────┐
│                        客户端层                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │  FUSE 客户端  │  │  S3 Gateway   │  │  KV Client   │           │
│  │ (TLV 协议)   │  │ (在 Filer 内)  │  │              │           │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘           │
└─────────┼──────────────────┼──────────────────┼──────────────────┘
          │                  │                  │
          │ TLV              │                  │
┌─────────▼──────────────────▼──────────────────▼──────────────────┐
│                    Filer 层（Raft 强一致）                        │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  MetaShardManager（按 inode 分片，独立 Raft groups）        │  │
│  │  - ShardCommand: Create/Delete/Rename/SetAttr/...          │  │
│  │  - Leader Lease Read（避免额外 RTT）                       │  │
│  │  - UpdateInodeSizeChunks（close 时强一致同步 size+chunks）  │  │
│  │  - Invalidate 通知（推送给订阅客户端）                      │  │
│  │  - S3Handler（S3 Gateway）                                  │  │
│  └────────────────────────┬───────────────────────────────────┘  │
└───────────────────────────┼───────────────────────────────────────┘
                            │
┌───────────────────────────▼───────────────────────────────────────┐
│                   Master 层（Raft 调度）                           │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  VolumeAssigner（卷分配）                                    │  │
│  │  ClusterScheduler（集群拓扑）                                │  │
│  │  ResilientMasterClient（领导者发现 + 故障转移）              │  │
│  │  next_file_key 块预分配（Raft batch, 1000/batch）           │  │
│  └────────────────────────┬───────────────────────────────────┘  │
└───────────────────────────┼───────────────────────────────────────┘
                            │
┌───────────────────────────▼───────────────────────────────────────┐
│                  Volume 层（Needle 存储）                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐            │
│  │ Volume Server │  │ Volume Server │  │ Volume Server │            │
│  │ - Needle O(1)│  │ - Lease 管理  │  │ - Compact     │            │
│  │ - per-stripe │  │   (64MB)      │  │   机制         │            │
│  │   Lease 锁   │  │ - Circuit     │  │ - used_bytes  │            │
│  │              │  │   Breaker     │  │   跟踪         │            │
│  └──────────────┘  └──────────────┘  └──────────────┘            │
└───────────────────────────────────────────────────────────────────┘
```

## 二、现有文章问题诊断

### 严重脱节（需完全重写）

| 文章 | 核心问题 |
|------|---------|
| **04-metadata-bottleneck.md** | 全文基于 OR-Set CRDT，需改为 Filer Raft 强一致 |
| **06-protocol-consistency.md** | 全文基于 OR-Set CRDT 冲突处理，需改为 Raft 强一致 |
| **09-s3-gateway.md** | S3 Gateway 位置错误（Master→Filer），OR-Set 描述错误 |
| **14-distributed-lock.md** | 全文基于 OR-Set CRDT 无锁设计，需改为 Lease 锁机制 |

### 部分脱节（需大幅修订）

| 文章 | 核心问题 |
|------|---------|
| **05-needle-o1-io.md** | 元数据层 O(1) 描述基于 OR-Set，需改为 Filer Raft + LRU 缓存 |
| **07-zero-jitter.md** | "写零阻塞"基于 OR-Set，需改为 Lease + 后台 flusher 机制 |
| **10-powerfs-vs-ceph-vs-lustre.md** | 对比维度基于 OR-Set CRDT，需更新为 Raft 强一致 |
| **11-enterprise-reliability.md** | 元数据可靠性基于 OR-Set，需改为 Raft + RocksDB |
| **13-rust-storage-kernel.md** | 代码示例基于 OR-Set，需更新为 Raft + TLV 实现 |
| **15-roadmap.md** | Phase 1 基于 OR-Set CRDT，需更新已完成阶段 |
| **16-enterprise-edition-benefits.md** | 分层一致性描述需更新 |

### 轻微脱节（需小幅修订）

| 文章 | 核心问题 |
|------|---------|
| **08-kv-cache-llm.md** | KV Cache 基础架构可保留，OR-Set 引用需移除 |
| **12-production-deployment.md** | 部署命令需更新（CLI→配置文件） |

### 基本可用

| 文章 | 状态 |
|------|------|
| **FUSE coherence.md** | 分层一致性概念仍有效，需小幅更新 |
| **作业级分层一致性FUSE文件系统 全套测试验证方案.md** | 测试方案框架可用，需补充新架构测试项 |

## 三、新文章规划

### 规划原则

1. **基于当前代码实现**：所有架构描述、代码示例、性能数据均来自实际代码
2. **技术深度与可读性并重**：既要有代码级深度，也要有架构级可读性
3. **覆盖已验证的性能数据**：使用 fio/IO500 实测数据
4. **删除虚构内容**：移除未实现的功能描述（SPDK、RDMA、GPU Direct 等标注为"规划中"）

### 新文章目录（16 篇）

#### 第一部分：架构设计（4 篇）

| 编号 | 新标题 | 核心内容 | 对应旧文 |
|------|--------|---------|---------|
| 01 | PowerFS 整体架构设计 | 四层架构（Master/Filer/Volume/FUSE）、组件职责、通信协议（TLV）、数据流 | README-origin |
| 02 | Filer Raft 强一致元数据管理 | ShardCommand/Raft propose-apply、分片策略、Leader Lease Read、UpdateInodeSizeChunks、Invalidate 通知 | 04 重写 |
| 03 | Needle O(1) 数据存储引擎 | Needle 格式、file_key 块分配、Volume 容量管理、Compact 机制、used_bytes 跟踪 | 05 修订 |
| 04 | TLV 通信协议与 Transport 抽象 | TLV 编解码、bytes::Bytes 零拷贝、TCP 流拆分、Transport trait（TCP/RDMA/QUIC） | 新增 |

#### 第二部分：数据一致性与并发（4 篇）

| 编号 | 新标题 | 核心内容 | 对应旧文 |
|------|--------|---------|---------|
| 05 | Lease 锁机制：per-stripe 强一致写入 | Lease 申请/续约/释放、Follower→Leader 模式、64MB stripe、LeaseGuard、客户端崩溃清理 | 14 重写 |
| 06 | FUSE 客户端缓存架构 | MetadataCache（LRU+pinned）、ChunkCache（512MB+backpressure）、dirty flag 管理、flusher | 新增 |
| 07 | 跨客户端可见性与 Invalidate 通知 | Filer 推送机制、InvalidateHandler、pinned/dirty 跳过策略、open-time getattr | 06 重写 |
| 08 | 作业级分层一致性模型 | 作业内强一致（Lease+Raft）、作业外最终一致（Invalidate+TTL）、Coherent PRELOAD | FUSE coherence 修订 |

#### 第三部分：性能优化（3 篇）

| 编号 | 新标题 | 核心内容 | 对应旧文 |
|------|--------|---------|---------|
| 09 | FUSE 写路径性能优化 | Vec→Bytes 零拷贝、batch_size=32、flusher 20ms、backpressure lock、read-before-write | 07 修订 |
| 10 | FUSE 读路径性能优化 | PREFETCH_CHUNKS=4、HashMap O(1) 索引、offset 语义、ChunkCache 命中率 | 新增 |
| 11 | IO500 测试实践与性能数据 | 测试环境、配置、实测结果（ior/mdtest）、性能瓶颈分析与优化历程 | 新增 |

#### 第四部分：功能与生态（3 篇）

| 编号 | 新标题 | 核心内容 | 对应旧文 |
|------|--------|---------|---------|
| 12 | S3 Gateway 设计与实现 | Filer 内置 S3Handler、Bucket/Object 映射、S3 API 兼容性 | 09 重写 |
| 13 | KV Cache 引擎设计 | Session 隔离、LRU+TTL 淘汰、GPU Direct 规划、当前实现状态 | 08 修订 |
| 14 | PowerFS vs Ceph vs Lustre | 基于实测数据的对比（架构、一致性、性能、运维） | 10 修订 |

#### 第五部分：工程实践（2 篇）

| 编号 | 新标题 | 核心内容 | 对应旧文 |
|------|--------|---------|---------|
| 15 | 生产部署与运维 | 统一配置文件、Docker Compose 部署、init 工具、监控指标、故障排查 | 12 修订 |
| 16 | Rust 存储内核：安全与性能 | 所有权系统、async/await、bytes::Bytes、RocksDB 集成、无 GC | 13 修订 |

### 删除/合并的旧文

| 旧文 | 处理方式 | 原因 |
|------|---------|------|
| 11-enterprise-reliability.md | 合并入 02/05 | 可靠性内容分散到 Raft 和 Lease 文章 |
| 15-roadmap.md | 删除 | 路线图已过时，Phase 1-2 已完成 |
| 16-enterprise-edition-benefits.md | 合并入 08 | 分层一致性内容合并 |
| 作业级分层一致性FUSE文件系统 全套测试验证方案.md | 保留为附录 | 测试方案框架仍有参考价值 |

## 四、实施计划

### 阶段一：架构设计文章（01-04）
优先重写核心架构文章，建立正确的架构认知基础。

### 阶段二：一致性与并发文章（05-08）
重写一致性机制文章，这是架构变更最大的部分。

### 阶段三：性能优化文章（09-11）
基于实测数据编写，包含 fio 和 IO500 测试结果。

### 阶段四：功能与生态文章（12-14）
更新 S3 Gateway、KV Cache 和对比文章。

### 阶段五：工程实践文章（15-16）
更新部署和 Rust 实现文章。

## 五、文章编写规范

1. **代码引用**：所有代码示例必须来自当前代码库，标注文件路径和行号
2. **性能数据**：使用 fio/IO500 实测数据，标注测试环境和参数
3. **架构图**：使用 ASCII 图或 Mermaid，与当前代码一致
4. **未实现功能**：明确标注"规划中"或"未实现"，不与已实现功能混淆
5. **语言**：中文为主，技术术语保留英文
