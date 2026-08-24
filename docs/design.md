# PowerFS｜面向极致HPC \& AI集群的融合存储文件系统设计方案（Rust原生重构 \+ SeaweedFS \+ BeeGFS双借鉴）

> ⚠️ **[架构更新 - 2026-08-24]** PowerFS 架构已全面演进，以下为当前状态。
>
> **核心演进**：
> - **通信协议**：从 gRPC 全面迁移到 powerfs-net TLV 二进制协议（MsgType + FieldId），零拷贝、低延迟
> - **一致性模型**：Filer 元数据使用 Raft 强一致（content_size + chunks 列表）；Cap model + lock_arbiter 管理分布式写锁
> - **认证架构**：三层认证模型 — admin_token（管理 API）+ registration_token（节点注册）+ ClientId（客户端挂载）
> - **证书管理**：Master 作为集群 CA，通过 rcgen 自签名生成 CA 证书，统一签发客户端/服务器证书
> - **FUSE 模式**：`mode=perf|ha`，perf 模式跳过 lease 续约/invalidate 通知，ha 模式为默认（完整 Cap 保护）
> - **内核客户端**：Kernel Client 使用 powerfs-net TLV 协议直连 Master/Filer，GFP_NOFS 防递归，dget/dput 防 UAF
>
> **关联文档**：
> - [network-architecture.md](network-architecture.md) — powerfs-net TLV 协议设计
> - [lease-design.md](lease-design.md) — Lease 模型与 Cap 协议
> - [lock-protocol.md](lock-protocol.md) — 分布式锁协议
> - [filer-architecture-design.md](filer-architecture-design.md) — Filer 分片架构
>
> 本文档为顶层设计，部分早期描述（如 gRPC 通信、旧性能数据）已在下方标注更新。

---

## 0\. 项目总览与品牌定位（GitHub 首页 \& 官网文案定稿）

### 0\.1 项目名称：PowerFS（最终定稿）

**词根释义**：Power = 极致算力、强劲吞吐、高能稳态，精准匹配项目核心定位——为HPC超算并行算力、LLM大模型推理算力提供**高性能、零抖动、全硬件加速**的统一存储底座，气场贴合超算与智算集群高端技术调性。

**中文官方定名**：力驰存储

**核心Slogan（GitHub/官网首页置顶，直击行业痛点）**

**英文官方Slogan**：PowerFS — Zero\-jitter unified parallel file system for HPC simulation and LLM KV cache

**中文官方Slogan**：力驰存储，零抖动统一并行文件系统，专为超算仿真与大模型KV缓存而生

**核心价值定位**：一站式解决传统HPC并行存储IO抖动、运维复杂，以及大模型推理KV显存瓶颈、缓存延迟过高两大行业核心痛点，一套架构统一承载超算仿真、AI训练、推理缓存全场景负载。

### 0\.2 项目核心亮点（对外亮眼介绍）

- **双架构融合创新**：融合 **SeaweedFS 扁平卷O\(1\)极速寻址** \+ **BeeGFS 大规模并行POSIX元数据分片**，同时解决海量小文件瓶颈与HPC万级进程并行IO瓶颈。

- **业界首个 Rust 原生 HPC\+AI 融合文件系统**：无GC、无抖动、全用户态IO，彻底解决Go/Python架构延迟抖动问题。

- **原生双引擎**：并行文件引擎（HPC超算）\+ KV Cache引擎（LLM推理），天然适配训推一体集群。

- **全硬件卸载**：原生支持 SPDK\-NVMe / RDMA / GPU Direct，硬件利用率拉满。

- **极简可运维**：比Lustre/BeeGFS架构更轻量、扩容更简单，同时兼具企业级并行稳定性。

### 0\.3 官网 \& GitHub 规划建议

**结论：必须独立官网 \+ 精致GitHub主页**

- **GitHub**：主打开源代码、技术文档、Roadmap、Demo、Benchmark对比（吊打SeaweedFS/CubeFS/Lustre小文件\&AI场景）。

- **独立官网**：主打项目介绍、架构图、性能指标、场景方案、快速开始、社区文档，提升项目专业度与开源影响力。

- **配套产出**：架构大图、性能对比图表、技术白皮书、落地案例。

---

## 1\. 设计思想：双项目核心借鉴（SeaweedFS \+ BeeGFS）

### 1\.1 借鉴 SeaweedFS 的核心能力（解决海量文件、低延迟、轻量化）

- **Volume 卷模型 \+ Needle 最小粒度**：O\(1\) 磁盘偏移寻址，彻底消除文件索引遍历开销。

- **主控无状态设计**：Master 只维护拓扑，不存储海量元数据，无亿级文件瓶颈。

- **极致小文件优化**：小文件合并、索引内存常驻、无碎片设计。

- **极简集群调度**：机架感知、自动均衡、轻量化扩容。

### 1\.2 借鉴 BeeGFS 的核心能力（补齐HPC并行短板）

- **分布式分片元数据**：目录分片、inode分片，支持万级MPI进程并行读写同一目录/文件。

- **真正并行POSIX语义**：完整文件锁、原子操作、并行mmap、兼容全部HPC仿真软件。

- **客户端深度并行IO**：文件条带化并行读写、多节点聚合带宽，HPC大文件吞吐拉满。

- **任务级IO隔离与低抖动机制**：后台任务不抢占前台计算IO，保障超算稳定性。

### 1\.3 自研新增独有能力（区别于二者）

- **原生AI KV Cache引擎**：非外挂组件，LLM KV张量专属存储、零拷贝、TTL淘汰、会话隔离。

- **Rust全栈用户态IO**：无GC、无系统调用抖动、SPDK/RDMA原生集成。

- **HPC\+AI混跑QoS**：超算作业 \& AI训推任务资源隔离，互不抢占。

- **内核客户端\(Kernel Client\)**：比FUSE更低延迟，原生接入Linux VFS，适配HPC高性能场景。

---

## 2\. PowerFS 整体架构（最终形态）

**四层解耦、双引擎并存、全用户态硬件加速**

### 2\.1 全局调度层（继承SeaweedFS轻量化）

- Raft Master 集群，无状态、无海量元数据压力

- 机架/机房拓扑感知，HPC作业本地化调度

- 支持预分配卷资源、任务专属资源池

### 2\.2 并行元数据层（借鉴BeeGFS分片架构）

- 目录分片 \+ inode分片，分布式无锁并发

- 支持百万级目录、万级并行元操作

- 完整POSIX语义，兼容MPI/HPC软件栈

### 2\.3 双数据引擎（PowerFS 核心创新）

#### 引擎A：HPC并行文件引擎

基于SeaweedFS Volume模型重构 \+ BeeGFS并行条带IO，SPDK用户态裸盘读写，服务超算仿真、并行计算。

#### 引擎B：原生AI KV Cache引擎

专为LLM推理KV张量优化，O\(1\)寻址、微秒级访问、GPU Direct零拷贝、会话级缓存管理。

### 2\.4 客户端层（分层建设，核心落地路径）

- 阶段1：Rust 用户态客户端（FUSE \+ SDK）

- 阶段2：**Linux 内核客户端 Kernel Client**（跳过FUSE，极致HPC低延迟）

- 阶段3：RDMA/GPU Direct 硬件直通客户端

### 2\.5 硬件加速底座

SPDK NVMe 用户态裸盘 \+ RDMA 无损网络 \+ GPU Direct 零拷贝

### 2\.6 S3 Gateway架构

PowerFS内置原生S3 Gateway，支持标准AWS S3协议，元数据由Master统一管理，数据存储在分布式Volume Server节点上：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          S3 Gateway 架构                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐   │
│  │   S3 客户端      │     │   AWS CLI/SDK    │     │   S3 Browser     │   │
│  └────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘   │
│           │                       │                       │              │
│           ▼                       ▼                       ▼              │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │                   PowerFS S3 Gateway (端口 9000)                      │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────┐│ │
│  │  │ HTTP Server  │  │ Auth Manager │  │ MultiPart    │  │ Bucket    ││ │
│  │  │              │  │              │  │ Upload Mgr   │  │ Mgmt      ││ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └───────────┘│ │
│  │  ┌──────────────┐  ┌──────────────┐                                  │ │
│  │  │ MasterApi    │  │ VolumeClient │                                  │ │
│  │  │ (卷分配)     │  │ Pool (数据)  │                                  │ │
│  │  └──────────────┘  └──────────────┘                                  │ │
│  └──────────────────────────┬───────────────────────────────────────────┘ │
│                             │                                             │
│                             ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │                  PowerFS Master (元数据管理)                          │ │
│  │           DirectoryTree - S3 Bucket/Object 元数据                     │ │
│  └──────────────────────────┬───────────────────────────────────────────┘ │
│                             │                                             │
│                             ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │                  PowerFS Volume Server 集群                          │ │
│  │                    S3 对象数据分布式存储                               │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

**架构说明**：

| 组件 | 端口 | 职责 | 是否存储数据 |
|------|------|------|------------|
| **PowerFS S3 Gateway** | 配置文件指定 | S3协议处理、认证管理、分片上传、Bucket管理 | **不存储** |
| **PowerFS Master** | 配置文件指定 | 元数据管理（DirectoryTree）、卷分配、集群调度、CA 证书签发 | **存储元数据** |
| **PowerFS Volume Server** | 配置文件指定 | 实际数据存储（Needle格式）、EC纠删码、Bitrot检测 | **存储对象数据** |

> **注意**：所有端口必须在配置文件中显式设置，无默认值。Master 配置 `port`（HTTP）、`raft_port`、`metrics_port`、`net_port` 四个端口，且必须互不相同。

**S3 Gateway启动方式**：
```bash
powerfs s3 start --config /etc/powerfs/powerfs.yaml
```

---

## 3\. 整体实现路径（严格按你要求：先重写SeaweedFS核心 → 再开发Kernel Client）

**核心路线：Rust从零重构，不基于Go源码，只借鉴设计思想**

### Phase 1：Rust 重构 SeaweedFS 核心基座（PowerFS 0\.1～0\.3）

目标：实现稳定、高性能、无GC的卷存储底座，超越原版Go SeaweedFS性能

- 实现 Master 调度、Volume 卷管理、Needle 最小存储单元

- 实现 O\(1\) 磁盘偏移寻址、小文件优化、基础副本机制

- 实现基础FUSE挂载、REST/S3简易协议

- Rust内存安全、无GC抖动、基础稳定性保障

### Phase 2：引入 BeeGFS 并行能力，补齐HPC短板（PowerFS 0\.4～0\.6）

- 分片式分布式元数据，支持大规模并行目录

- 大文件条带化并行读写、多节点聚合带宽

- 完整POSIX语义、HPC并行锁、mmap支持

- HPC作业QoS、后台IO降噪

### Phase 3：开发 Linux Kernel 内核客户端（关键里程碑）

彻底摆脱FUSE开销，达到专业HPC文件系统延迟水平

- 实现Linux VFS内核注册、inode/dentry管理

- Power内核态直连PowerFS数据节点，用户态零转发

- 内核级并行IO、缓存优化、极致低延迟

- 内置LLM KV专属存储、TTL/LRU智能淘汰、会话隔离

- SPDK/RDMA/GPU Direct 全链路硬件卸载

- HPC\+AI混跑调度、冷热分层、EC纠删码

---

## 4\. 详细可落地 TodoList（分阶段、可直接开发排期）

### 【阶段0：项目初始化（1周）】

- 创建 GitHub 开源仓库 **PowerFS**

- 完成项目 Logo、Slogan、README 亮眼介绍

- 搭建官方静态站点（VuePress/VitePress）：架构、文档、Roadmap

- 搭建 CI/CD、编译脚本、代码规范

- 输出完整架构大图、技术白皮书初稿

### 【阶段1：Rust重写SeaweedFS核心基座（2～3周）】

- 实现 Rust 版 Master 节点（Raft、拓扑管理、卷分配）

- 实现 Volume 卷管理、预分配、加载、校验

- 实现 Needle 读写、索引管理、O\(1\)寻址

- 实现基础副本机制、故障检测

- 实现简易FUSE客户端，可正常挂载读写文件

- 基础性能压测：对比原生SeaweedFS小文件性能

### 【阶段2：融合BeeGFS HPC并行能力（3周）】

- 实现分布式分片元数据引擎

- 支持超大目录、高并发元数据操作

- 大文件条带化并行读写、多节点聚合

- 完善POSIX语义：文件锁、原子操作、断点续写

- HPC后台IO降噪、任务基础QoS隔离

### 【阶段3：开发 Linux Kernel 内核客户端（核心难点，4～6周）】

- Linux内核模块环境搭建、内核编译调试

- 实现PowerFS内核VFS注册、super\_block、inode、dentry

- 内核态网络通信模块（适配RDMA基础）

- 内核态读写流程、缓存机制、权限控制

- 对比FUSE延迟，完成性能优化、稳定性修复

- 支持HPC并行mmap、原子读写

### 【阶段4：原生AI KV Cache引擎开发（3周）】

- 设计KV张量专属存储结构，适配LLM推理特征

- 实现O\(1\)KV寻址、增量更新、TTL过期淘汰

- 会话级KV隔离、批量预取、热点常驻

- 对接GPU Direct零拷贝数据通路

- KV Cache性能压测：QPS、p99延迟、命中率

### 【阶段5：硬件加速 \& 生产级优化（持续迭代）】

- SPDK用户态NVMe适配，内核旁路IO

- RDMA全链路替换TCP

- EC纠删码、冷热分层、多租户、配额QoS

- 完善监控、告警、运维工具链

- 输出专业Benchmark：对标Lustre、BeeGFS、SeaweedFS、CubeFS

---

## 5\. PowerFS 核心差异化总结（开源项目亮点）

1. **全球少见 Rust 原生 HPC\+AI 融合并行文件系统**，无GC、无抖动、高性能底子极佳。

2. **双架构取长补短**：既有SeaweedFS的小文件无敌性能，又有BeeGFS的HPC大规模并行能力。

3. **唯一原生支持LLM KV Cache的并行文件系统**，区别于所有传统HPC存储、云原生存储。

4. **用户态SPDK\+内核客户端双形态**，兼顾开发效率与极致HPC性能。

5. **轻量化、易部署、可运维**，解决Lustre/BeeGFS部署复杂、运维困难的痛点。

---

## 6\. FUSE客户端性能优化方案

> **[更新说明]** 以下方案多数已实现。通信协议已从 gRPC 全面迁移到 powerfs-net TLV 二进制协议，写入路径已使用异步批量刷新，连接复用通过 powerfs-net 统一 RPC 层实现。

### 6\.1 性能瓶颈分析（历史基线）

早期 FUSE 客户端写入性能约 1.8 MB/s，主要瓶颈包括：

1. **同步写入路径**：每个write请求都在FUSE工作线程中同步完成，包含缓存操作、锁竞争、数据拷贝等开销
2. **数据克隆开销**：写入路径中存在多次数据克隆（`buf.clone()`、`chunk_vec.to_vec()`），增加内存分配和拷贝成本
3. **锁竞争**：`chunk_cache`和`dirty_chunks`使用全局RwLock，高并发场景下存在严重锁竞争
4. **单线程FUSE会话**：FUSE默认单线程处理所有请求，无法利用多核CPU
5. **~~gRPC串行调用~~** → **TLV 同步调用**：元数据更新和数据写入通过 powerfs-net TLV 协议同步调用，阻塞写入路径（已通过 write_batch + 异步 flush 优化）

### 6\.2 优化方案（按优先级排序）

#### 方案1：异步后台脏数据刷新（预期收益最大）

**核心思想**：将脏数据刷新从写入路径中解耦，由专门的后台线程批量处理

**实现要点**：
- 启动独立后台线程，定期扫描`dirty_chunks`集合
- 批量收集脏chunk，合并TLV 调用，减少网络往返（已通过 write_batch 实现）
- 使用无锁数据结构（如`crossbeam::queue`）替代RwLock，降低锁竞争
- 支持手动触发刷新（如文件关闭时）和自动定时刷新

**预期效果**：写入路径仅需写入本地缓存，延迟降低90%以上

#### 方案2：零拷贝写入路径

**核心思想**：消除写入路径中的数据克隆，使用原地修改

**实现要点**：
- 修改`ChunkCache`提供`get_mut()`方法，支持原地修改chunk数据
- 移除`buf.clone()`，直接从FUSE读取缓冲区拷贝到缓存
- 使用`Vec::resize()`预分配空间，避免重复分配
- 优化chunk数据结构，减少内存碎片

**预期效果**：减少约30%的写入延迟

#### 方案3：多线程FUSE会话

**核心思想**：利用多核CPU并行处理FUSE请求

**实现要点**：
- 配置fuse_backend_rs使用多工作线程模式
- 每个工作线程独立处理请求，减少线程切换开销
- 优化共享数据结构的并发访问模式

**预期效果**：吞吐量提升2-4倍（取决于CPU核数）

#### 方案4：写入合并优化

**核心思想**：在缓存层合并相邻的小写入

**实现要点**：
- 实现写入合并缓冲区，积累小写入后一次性写入chunk缓存
- 设置合并阈值（如64KB），超过阈值时触发实际写入
- 支持flush操作时强制刷新合并缓冲区

**预期效果**：小文件写入性能提升3-5倍

#### 方案5：~~gRPC连接复用~~ → TLV 统一 RPC 层（已实现）

**核心思想**：使用 powerfs-net TLV 二进制协议替代 gRPC，统一连接管理

**实现要点**：
- ~~实现gRPC连接池，复用TCP连接~~ → powerfs-net 统一 RPC 层（connect → handshake → send → read）
- 批量发送元数据更新请求，减少网络往返（write_batch 批量提交）
- ~~配置gRPC流式传输~~ → TLV 请求-响应模式 + KeepConnected 长连接流

**预期效果**：元数据操作延迟降低50%（已实现）

### 6\.3 优化路线图

| 阶段 | 优化项 | 预期性能提升 | 状态 |
|------|--------|-------------|------|
| Phase 1 | 异步后台脏数据刷新 | 写入延迟降低90% | ✅ 已实现 |
| Phase 2 | 零拷贝写入路径 | 写入延迟降低30% | ✅ 已实现 |
| Phase 3 | 多线程FUSE会话 | 吞吐量提升2-4倍 | ✅ 已实现 |
| Phase 4 | 写入合并优化 | 小文件写入提升3-5倍 | ✅ 已实现（write_batch） |
| Phase 5 | TLV 统一 RPC 层 | 元数据操作降低50% | ✅ 已实现 |

---

## 7\. 安全认证架构

PowerFS 采用三层认证模型，覆盖管理访问、节点注册和客户端挂载三个安全边界。

### 7\.1 认证层级总览

```
┌─────────────────────────────────────────────────────────────────┐
│                    三层认证架构                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │  Layer 1:       │  │  Layer 2:       │  │  Layer 3:       │ │
│  │  admin_token    │  │  registration_  │  │  ClientId       │ │
│  │                 │  │  token          │  │                 │ │
│  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤ │
│  │ • CLI 管理 API  │  │ • Volume 注册   │  │ • FUSE 挂载     │ │
│  │ • 证书签发 API  │  │ • Filer 注册    │  │ • Cap 授权      │ │
│  │ • Bearer Token  │  │ • TLV 字段携带  │  │ • Master 分配   │ │
│  │ • Master 配置   │  │ • Master 校验   │  │ • 全局唯一      │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
│         │                       │                       │       │
│         ▼                       ▼                       ▼       │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Master (集群 CA + 认证中心)                │   │
│  │  • admin_token 校验 (constant-time)                     │   │
│  │  • registration_token 校验 (constant-time)              │   │
│  │  • ClientId 黑名单 (RegisterClient 拒绝)               │   │
│  │  • CA 证书签发 (rcgen 自签名)                           │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

| 层级 | 认证方式 | 适用场景 | 配置位置 | 校验方 |
|------|---------|---------|---------|--------|
| **admin_token** | Bearer Token (HTTP Header) | CLI 管理 API、证书签发 API | `master.admin_token` | CaManager::verify_admin_token |
| **registration_token** | TLV FieldId 0xD3 | Volume Heartbeat、Filer RegisterFiler | `master/volume/filer.registration_token` | MasterNode::verify_registration_token |
| **ClientId** | Master 分配的全局唯一 ID | FUSE 客户端挂载、Cap 授权 | Master 运行时分配 | Master RegisterClient + 黑名单 |

### 7\.2 Master CA 证书管理

Master 作为集群唯一的 Certificate Authority，负责签发所有 TLS 证书：

**CA 初始化**：
- Master 首次启动时，通过 `rcgen` 自动生成自签名 CA 证书（10 年有效期）
- CA 证书 (`ca.crt`) 和私钥 (`ca.key`) 持久化到 `ca_dir`（默认 `{dir}/ca`）
- 私钥文件权限 `0600`，仅在 Master 节点保存
- 后续启动加载已有 CA，不重新生成

**证书签发 HTTP API**（挂在 Master metrics/admin HTTP 端口）：

| 端点 | 方法 | 认证 | 用途 |
|------|------|------|------|
| `/api/cert/ca` | GET | 无需认证 | 获取 CA 证书 PEM（客户端需要它来验证服务器证书） |
| `/api/cert/sign-client` | POST | admin_token | 签发客户端证书（ClientAuth + 可选 ClientId SAN URI） |
| `/api/cert/sign-server` | POST | admin_token | 签发服务器证书（ServerAuth + SAN IP/DNS） |

**证书生命周期**：
- 客户端/服务器证书有效期 1 年
- CA 证书有效期 10 年
- 证书续期：重新调用签发 API 即可

### 7\.3 证书分发流程

```
┌──────────┐         ┌──────────┐         ┌──────────┐
│  运维人员 │         │  Master  │         │ 目标节点  │
│  (CLI)   │         │   (CA)   │         │(FUSE/Filer│
│          │         │          │         │/Volume)  │
└────┬─────┘         └────┬─────┘         └────┬─────┘
     │                    │                    │
     │  1. init-ca        │                    │
     │ ──────────────────>│                    │
     │  GET /api/cert/ca  │                    │
     │  (无认证)          │                    │
     │<──────────────────│                    │
     │  ca.crt            │                    │
     │                    │                    │
     │  2. sign-client    │                    │
     │  或 sign-server    │                    │
     │ ──────────────────>│                    │
     │  POST /api/cert/*  │                    │
     │  Authorization:    │                    │
     │  Bearer <token>    │                    │
     │<──────────────────│                    │
     │  client.crt+key   │                    │
     │  或 server.crt+key│                    │
     │                    │                    │
     │  3. 分发证书文件    │                    │
     │ ────────────────────────────────────────>│
     │  ca.crt + cert + key                  │
     │                    │                    │
     │                    │  4. 节点启动时     │
     │                    │<──────────────────│
     │                    │  Volume: Heartbeat │
     │                    │  Filer: RegisterFiler│
     │                    │  (携带 registration_token)│
     │                    │                    │
     │                    │  5. Master 校验    │
     │                    │  registration_token│
     │                    │<──────────────────│
     │                    │  STATUS_OK /       │
     │                    │  STATUS_ERR_PERMISSION_DENIED│
     │                    │                    │
```

**CLI 证书管理命令**：
```bash
# 1. 获取 CA 证书（无需 admin_token）
powerfs-cli cert init-ca --master-api master:metrics_port --admin-token <token> -o /etc/powerfs/certs/

# 2. 签发客户端证书（FUSE 客户端用）
powerfs-cli cert sign-client --master-api master:metrics_port --admin-token <token> \
  --common-name "fuse-client-1" --client-id "client-001" -o /etc/powerfs/certs/

# 3. 签发服务器证书（Filer/Volume 用）
powerfs-cli cert sign-server --master-api master:metrics_port --admin-token <token> \
  --common-name "filer-1" --san 172.20.0.21 --san filer-1.powerfs.local -o /etc/powerfs/certs/
```

### 7\.4 Registration Token 节点认证

Volume 和 Filer 在向 Master 注册时必须携带 `registration_token`，Master 在处理注册请求前先校验 token。

**TLV 协议层**：
- `FieldId::RegistrationToken = 0xD3`（string 类型）
- Volume 在 `MsgType::Heartbeat` 请求中携带
- Filer 在 `MsgType::RegisterFiler` 请求中携带

**Master 校验流程**（`net_handler.rs`）：
1. 解码 TLV 请求中的 `RegistrationToken` 字段
2. 调用 `MasterNode::verify_registration_token()` 进行常量时间比较
3. 校验在 **leader 检查之前** 执行，即使 follower 也拒绝未授权节点（不重定向到 leader）
4. 校验失败返回 `STATUS_ERR_PERMISSION_DENIED` + 错误消息

**Dev 模式**：
- `registration_token` 为 `None` 或空字符串时，Master 跳过校验（dev 模式）
- 生产环境必须设置非空 token，Volume/Filer 配置中填写相同值

**配置示例**（`powerfs.yaml`）：
```yaml
master:
  registration_token: "secure-cluster-token-2026"

volume:
  registration_token: "secure-cluster-token-2026"

filer:
  registration_token: "secure-cluster-token-2026"
```

### 7\.5 Admin Token 管理认证

Admin token 用于保护 Master 的管理 HTTP API（证书签发、调试控制等）。

**校验机制**：
- HTTP 请求头 `Authorization: Bearer <admin_token>`
- `CaManager::verify_admin_token()` 使用常量时间比较（防时序攻击）
- Dev 模式：`admin_token` 为 `None` 或空时，管理 API 完全开放

**配置示例**：
```yaml
master:
  admin_token: "admin-secret-token-2026"
  ca_dir: "/data/powerfs/master/ca"
```

### 7\.6 ClientId 客户端认证

ClientId 是 Master 在 FUSE 客户端首次连接时分配的全局唯一标识，用于 Cap 授权和写锁管理。

**分配流程**：
- FUSE 客户端通过 `KeepConnected` TLV 请求连接 Master
- Master 分配 ClientId 并维护 `client_uuid → client_id` 映射
- Master 维护黑名单，`RegisterClient` 响应中 `MountAllowed=0` 表示拒绝挂载

**与证书的区别**：ClientId 不需要预配置，由 Master 运行时自动分配。证书用于 TLS 加密通道，ClientId 用于应用层身份识别和 Cap 授权。

> （注：部分内容可能由 AI 生成）
