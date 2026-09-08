# PowerFS 写预测与内容指纹去重方案设计

> 状态: **草案讨论中**
> 创建: 2026-09-08
> 作者: PowerFS Team
> 关联文档: [`ml-prefetch-kernel-rdma-plan.md`](./ml-prefetch-kernel-rdma-plan.md)（读侧 ML 预取）、[`file-layout-prediction-design.md`](./file-layout-prediction-design.md)（布局预测）
> 分支: 待定

---

## 1. 问题背景

### 1.1 场景动机

在 HPC/AI/数据分析等应用中，同一组数据文件经常被多次重复写入：

| 场景 | 重复模式 | 重复率 |
|------|----------|--------|
| ML 训练 | checkpoint 周期性覆盖写（内容 90%+ 相同） | 极高 |
| 日志轮转 | 模板化日志，结构高度重复 | 高 |
| 容器镜像 | 多版本共享 base layer，差异层极小 | 极高 |
| CI/CD | 构建产物增量编译，大部分 object 不变 | 中-高 |
| 数据分析 | 同源数据集多次处理，中间结果重复 | 中 |

传统文件系统对每次写都做完整 I/O，即使数据内容完全相同也要重新写入存储。现有 dedup 技术（如 ZFS dedup、LessFS）对**所有写**都计算 hash，CPU 开销大且无选择性。

### 1.2 核心创新点

**ML 预测 + 内容指纹去重**：用 ML 模型预判写数据的"重复概率"，只对高概率写计算指纹并发服务端匹配，而非对所有数据盲目 hash。

与传统 dedup 的关键区别：

| 维度 | 传统 dedup | 本方案 |
|------|-----------|--------|
| hash 计算范围 | 所有写入数据 | 仅 ML 判定为高重复概率的写 |
| CPU 开销 | O(N) 全量 hash | O(k) 选择性 hash，k << N |
| 判断依据 | 无（全量算） | 写模式特征（路径模式、写大小序列、时间周期性等） |
| 适用场景 | 通用存储 | 重复写入密集的 HPC/AI 工作负载 |
| 误判代价 | N/A | 低概率漏判 → 正常写（无损失）；高概率误判 → 多算一次 hash（可接受） |

### 1.3 PowerFS 架构优势

PowerFS 的现有架构天然支持此方案：

| 现有机制 | 文件 | 对本方案的支持 |
|----------|------|----------------|
| Needle 已是 content-addressed | [needle.rs:12-19](file:///home/portion/powerfs/powerfs-core/src/needle.rs#L12-L19) `struct Needle { id, volume_id, data, checksum, checksum_algorithm }` | needle 的 checksum 本身就是内容指纹的基础 |
| StoredFileChunk 映射 logical→physical | [shard_store.rs:164-173](file:///home/portion/powerfs/powerfs-filer/src/shard_store.rs#L164-L173) `{ offset, size, needle_id, volume_id, crc32 }` | 引用机制只需在 chunk 层增加 ref 指向已有 needle |
| WriteCoalescer 已批量合并写 | [write_coalescer.rs](file:///home/portion/powerfs/powerfs-core/src/write_coalescer.rs) | hash 计算自然嵌入 coalescer flush 路径 |
| Scrubber 已有 GC 机制 | [scrubber.rs](file:///home/portion/powerfs/powerfs-filer/src/scrubber.rs) | refcount 回收扩展 scrubber 即可 |
| ChecksumAlgorithm 已支持 Blake3 | [utils.rs:102-107](file:///home/portion/powerfs/powerfs-common/src/utils.rs#L102-L107) `CRC32C / CRC64 / Blake3` | Blake3 抗碰撞，可直接做指纹 |
| ML trace 基础设施（读侧） | [readahead_trace.rs](file:///home/portion/powerfs/powerfs-filer/src/readahead_trace.rs)、[readahead_policy.rs](file:///home/portion/powerfs/powerfs-filer/src/readahead_policy.rs) | trace 采集→特征提取→NN 训练→xattr 下发链路可复用 |
| xattr 策略下发通道 | `user.powerfs.readahead_policy` | 可扩展 `user.powerfs.write_predict_policy` |
| Raft 元数据强一致 | meta_shard_manager → `InodeInfo.chunks` | 指纹索引和 refcount 可走 Raft 保证一致 |

---

## 2. 整体架构

### 2.1 三层处理流水线

```
┌─────────────────────────────────────────────────────────────────┐
│ 客户端 (kernel / FUSE)                                            │
│                                                                   │
│  写请求 → WriteCoalescer 批量合并                                   │
│      ↓                                                            │
│  ① 第一层: ML 概率预判 (轻量, 基于写模式特征)                        │
│      ↓ 概率 > 阈值              ↓ 概率 ≤ 阈值                      │
│  ② 第二层: 算指纹(Blake3)       正常写 (不上指纹, 不增加 CPU 开销)    │
│      ↓                         ↓                                  │
│  ③ 指纹 + 写数据 → Filer     写数据 → Filer (原路径)               │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│ Filer (Raft 强一致)                                              │
│                                                                   │
│  ④ 指纹索引查找 (Bloom filter 一级过滤 + HashMap 精确匹配)        │
│      ↓ 命中                       ↓ 未命中                       │
│  ⑤ 引用已有 needle              存新 needle + 记录指纹           │
│  (refcount++, StoredFileChunk   (正常写路径 + 索引插入)            │
│   指向已有 needle_id)                                          │
│      ↓                            ↓                              │
│  ⑥ 返回写完成 (跳过数据传输)     返回写完成 (数据已存储)           │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 数据流路径

```
正常写 (低重复概率):
  client → WriteCoalescer → filer::write_needle → volume::append → RocksDB

预测写 (高重复概率):
  client → WriteCoalescer → ML预判 → Blake3(client端)
         → filer::fingerprint_lookup (Bloom+HashMap)
            → 命中: refcount++ + chunk 指向已有 needle (零数据传输)
            → 未命中: fallback 到正常写 + 记录指纹
```

### 2.3 关键设计决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 指纹计算位置 | 客户端 | 避免传输重复数据到服务端才算 hash；coalescer flush 后已有完整 merged data |
| 指纹算法 | Blake3 | 抗碰撞（256-bit），比 CRC32 安全，比 SHA-256 快 5-10× |
| 索引存储 | Filer Raft（一致性）+ Volume 本地缓存（查询加速） | 指针索引需要强一致，不能用 CRDT |
| 引用机制 | needle refcount + StoredFileChunk 指向已有 needle_id | 复用现有 chunk 映射，无需硬链接语义 |
| Bloom filter | Filer 内存，按 shard 分区 | O(1) 过滤，减少精确查找的 HashMap 压力 |
| ML 模型位置 | Filer 端训练，xattr 下发到客户端 | 与读侧 ML 预取架构一致 |

---

## 3. 详细设计

### 3.1 第一层：ML 概率预判

#### 3.1.1 写模式特征

| 特征 | 来源 | 说明 |
|------|------|------|
| `write_path_pattern` | 写路径哈希模式 | 路径前缀/后缀模式（如 `/checkpoint/epoch_N` → 周期性） |
| `write_size_seq` | 连续写大小序列 | 固定大小写 → 高重复概率（模板化数据） |
| `write_offset_pattern` | 偏移序列模式 | 整文件覆盖写 vs 增量追加 |
| `file_size` | inode size | 大文件重复率通常低于小文件 |
| `write_interval` | 两次写间隔 | 周期性写（checkpoint）→ 高重复 |
| `overwrite_ratio` | 历史覆盖写比例 | 同一 inode 的写是否多为覆盖写 |
| `path_similarity` | 路径与历史路径的相似度 | 相似路径 → 相似内容 |

#### 3.1.2 模型结构

复用读侧的轻量 MLP 架构，调整输入维度：

```
WritePredictNN:
  输入层: 7 特征 (write_path_pattern, write_size_seq, write_offset_pattern,
                  file_size, write_interval, overwrite_ratio, path_similarity)
  隐藏层: 16 维 (tanh 激活)
  输出层: 1 维 (sigmoid, 概率 P(重复))
  训练: SGD + 梯度裁剪, 1000 epochs
  标签: 指纹命中=1.0, 未命中=0.0 (来自历史去重结果)
```

#### 3.1.3 预判策略

```python
def should_compute_fingerprint(features, model):
    p = model.predict(features)  # 0.0 ~ 1.0
    if p > THRESHOLD:  # 默认 0.3, 可通过 xattr 调整
        return True   # 算指纹
    else:
        return False  # 直接写
```

- 阈值低（0.3）→ 多算指纹，多发现重复 → 节省网络/存储，但 CPU 开销增加
- 阈值高（0.7）→ 少算指纹，CPU 开销低，但漏掉部分重复
- xattr 下发：`user.powerfs.write_predict_policy = "NN:<threshold>"` 或 `"RULE:<threshold>"`

#### 3.1.4 冷启动（无训练数据）

规则引擎兜底：

| 写模式 | 规则 | 阈值 |
|--------|------|------|
| 同一 inode 第 2+ 次覆盖写 | 高重复概率 | 0.5 |
| 路径匹配 checkpoint/snapshot/save 模式 | 高重复概率 | 0.5 |
| 文件 < 4KB（小文件） | 中重复概率 | 0.3 |
| 首次写（新 inode） | 低重复概率 | 0.0（不算指纹） |
| 追加写（offset == file_size） | 低重复概率 | 0.1 |

### 3.2 第二层：内容指纹计算

#### 3.2.1 指纹粒度

| 粒度 | 大小 | 适用场景 | 优势 | 劣势 |
|------|------|----------|------|------|
| **chunk 级** | 4KB-1MB | 通用，通用文件系统 | 精细去重，部分重复可发现 | hash 计算多，索引大 |
| **needle 级** | 1MB（默认） | PowerFS 原生粒度 | 与现有存储对齐，索引小 | 小于 needle 的重复无法发现 |
| **变长块** | Rabin 窗口 | 深度去重 | 边界对齐，发现插入/删除后的重复 | 实现复杂，CPU 开销大 |

**推荐**：needle 级指纹为主（与 WriteCoalescer flush 粒度对齐），大文件可叠加 chunk 级。

#### 3.2.2 指纹计算

```rust
// 在 WriteCoalescer flush 后计算 (已有完整 merged data)
fn compute_fingerprint(data: &[u8]) -> Fingerprint {
    // Blake3: 256-bit, 抗碰撞, ~6 GB/s on modern CPU
    let hash = blake3::hash(data);
    Fingerprint(hash.as_bytes().try_into().unwrap())  // [u8; 32]
}
```

#### 3.2.3 指针缓存

客户端缓存最近 N 个指纹结果（LRU），避免同一数据反复计算：

```rust
struct FingerprintCache {
    cache: LruCache<ContentHash, bool>,  // hash → "服务端是否有匹配"
    max_entries: 256,
}
```

### 3.3 第三层：服务端指纹匹配

#### 3.3.1 指纹索引结构

```
Filer 端 (per-shard, Raft 一致):
  ┌──────────────────────────────────────────────┐
  │ FingerprintIndex                             │
  │                                              │
  │  BloomFilter (内存, 1% FPR, ~10 bits/entry)  │
  │  → O(1) 初筛, 过滤掉绝大部分未重复指纹        │
  │                                              │
  │  HashMap<Fingerprint, NeedleRef> (精确匹配)  │
  │  → NeedleRef { needle_id, volume_id,        │
  │                refcount, data_size,          │
  │                status: Active|Tombstoned,    │
  │                tombstoned_at,                │
  │                retention_until }             │
  └──────────────────────────────────────────────┘
```

**NeedleRef.status 三态**：

| 状态 | 含义 | 指纹索引行为 |
|------|------|-------------|
| `Active` | needle 活跃，有 chunk 引用 | 命中 → refcount++，正常引用 |
| `Tombstoned` | needle 逻辑删除（`deleted_at` 已设），retention 未过期 | 命中 → **恢复**：清除删除标记 + refcount=1 |
| `Expired` | retention 已过期，needle 已物理回收 | 指纹索引清除，走 NoMatch |

#### 3.3.2 匹配流程

```rust
async fn fingerprint_lookup(
    fp: &Fingerprint,
    data: &[u8],  // 数据前 64 字节（二次验证）
) -> LookupResult {
    // ① Bloom filter 初筛 (本地内存, O(1))
    if !bloom.may_contain(fp) {
        return LookupResult::NoMatch;  // 绝大部分走到这里
    }

    // ② 精确匹配 (HashMap, 本地内存)
    if let Some(existing) = hashmap.get(fp) {
        // ③ 二次验证: 防止 Bloom 误报 / hash 碰撞
        if !verify_content_match(data, &existing.prefix_64bytes) {
            return LookupResult::NoMatch;
        }

        // ④ 根据 needle 状态决定操作
        return match existing.status {
            NeedleStatus::Active => {
                // 活跃 needle: 正常引用
                LookupResult::Match(existing.clone())
            }
            NeedleStatus::Tombstoned => {
                // 逻辑删除但 retention 未过期: 恢复
                // (数据物理上仍在 volume server 上)
                LookupResult::Recoverable(existing.clone())
            }
            NeedleStatus::Expired => {
                // retention 已过期, needle 已物理回收
                // 清除过期指纹索引, 走正常写
                hashmap.remove(fp);
                bloom_remove(fp);
                LookupResult::NoMatch
            }
        };
    }

    LookupResult::NoMatch
}
```

#### 3.3.3 引用机制（Active needle）

```rust
// 命中 Active needle: refcount++ + chunk 指向 (无需传输数据)
async fn reference_existing_needle(
    inode: u64,
    offset: u64,
    chunk_size: u64,
    needle_ref: &NeedleRef,
) -> Result<()> {
    // ① refcount++ (Raft propose, 原子操作)
    shard_manager.increment_needle_refcount(
        needle_ref.needle_id,
        needle_ref.volume_id,
    ).await?;

    // ② StoredFileChunk 指向已有 needle
    let chunk = StoredFileChunk {
        offset,
        size: chunk_size,
        needle_id: needle_ref.needle_id,  // 复用已有 needle
        volume_id: needle_ref.volume_id,
        crc32: needle_ref.crc32,
        mtime: now(),
    };
    shard_manager.update_chunks(inode, vec![chunk]).await?;

    // ③ 不传输数据, 直接返回写完成
    Ok(())
}
```

#### 3.3.4 删除恢复机制（Tombstoned needle）

当文件被删除后，needle 进入逻辑删除状态（`deleted_at` 已设，retention 期内数据物理仍在 volume server）。此时相同内容的写请求可以通过指纹匹配命中 tombstoned needle，**恢复**而非重新写入：

```rust
// 命中 Tombstoned needle: 恢复 needle + 建立引用
async fn recover_tombstoned_needle(
    inode: u64,
    offset: u64,
    chunk_size: u64,
    needle_ref: &NeedleRef,
) -> Result<()> {
    // ① 恢复 needle: 清除 deleted_at, refcount 重置为 1 (Raft propose)
    shard_manager.recover_needle(
        needle_ref.needle_id,
        needle_ref.volume_id,
    ).await?;
    // 恢复操作:
    //   - NeedleInfo.deleted_at = None
    //   - NeedleInfo.delete_retention_until = None
    //   - refcount = 1 (新引用)
    //   - FingerprintIndex.status = Active

    // ② StoredFileChunk 指向恢复的 needle
    let chunk = StoredFileChunk {
        offset,
        size: chunk_size,
        needle_id: needle_ref.needle_id,
        volume_id: needle_ref.volume_id,
        crc32: needle_ref.crc32,
        mtime: now(),
    };
    shard_manager.update_chunks(inode, vec![chunk]).await?;

    // ③ 不传输数据, 直接返回写完成
    // (数据物理上一直在 volume server, 只是逻辑标记被清除)
    Ok(())
}
```

**恢复 vs 重新写入的收益**：

| 操作 | 网络 I/O | 磁盘 I/O | RocksDB 写 | 总延迟 |
|------|---------|---------|-----------|--------|
| 正常写 (无去重) | 传输 1MB 数据 | 写 1MB needle | 1 次 put | ~网络 RTT + 磁盘写 |
| 引用 Active needle | 0 (仅指纹 RPC) | 0 | 1 次 refcount 更新 | ~RPC RTT |
| **恢复 Tombstoned needle** | 0 (仅指纹 RPC) | 0 | 1 次 recover + 1 次 refcount | ~RPC RTT |
| 未命中 (新写) | 传输 1MB 数据 | 写 1MB needle | 1 次 put + 1 次 fingerprint 插入 | ~网络 RTT + 磁盘写 |

恢复机制的核心价值：**删了又写的数据（如 checkpoint 删除后重新训练、容器镜像层删除后重建）不需要重新传输和存储，直接从 tombstone 池恢复**。

#### 3.3.5 并发控制

两个客户端同时写相同内容时的竞争处理：

```
Client A: hash(data) → fp_A → Bloom hit → lookup → refcount++
Client B: hash(data) → fp_B → Bloom hit → lookup → refcount++

fp_A == fp_B (相同内容 → 相同 hash)

解决方案: NeedleRef 的 refcount 操作通过 Raft propose 序列化
  - Raft log 天然全序, 两个 propose 会被排序执行
  - 第一个 propose: refcount 1 → 2
  - 第二个 propose: refcount 2 → 3
  - 无需额外锁, Raft 的一致性保证足够
```

**Tombstone 恢复的并发**：

```
Client A 写文件 → 删除文件 → needle 进入 Tombstoned 状态
Client B 同时写相同内容 → 指纹命中 Tombstoned needle → 恢复

Client A 删除: Raft propose → status=Active → Tombstoned, refcount=0
Client B 恢复: Raft propose → status=Tombstoned → Active, refcount=1

两个 propose 通过 Raft 全序序列化:
  情况1 (A先): Active→Tombstoned→Active(refcount=1), B 正常恢复
  情况2 (B先): Active→恢复为Active(refcount=1)→Tombstoned(refcount=0), B的写完成但随后被A删除
  → 情况2 是正确语义: B 写完后 A 才删除, 最终文件确实被删
```

### 3.4 读取路径

读取引用的 needle 时，客户端透明处理：

```
read(inode, offset, size)
  → 查 InodeInfo.chunks (已有逻辑)
    → StoredFileChunk { needle_id, volume_id }
      → volume_server.read_needle(needle_id)  (已有逻辑)
```

**无需修改读取路径**。引用的 needle_id 与正常写入的 needle_id 在 chunk 层无差异，读取完全透明。

### 3.5 垃圾回收与 Tombstone 生命周期

needle 删除后的三阶段生命周期：

```
阶段1: Active (refcount=0, 有指纹索引)
  │  ← inode 删除触发 refcount--, 归零后不立即物理删除
  ↓
阶段2: Tombstoned (refcount=0, deleted_at 已设, retention 期内)
  │  ← 指纹索引保留, 可被恢复
  │  ← retention 未过期: 等待恢复或过期
  │  ← 若有相同内容写请求: 恢复 → 回到 Active (refcount=1)
  ↓
阶段3: Expired (retention 过期, 物理回收)
  │  ← Scrubber 扫描, 物理删除 needle
  │  ← 清除指纹索引 (Bloom + HashMap)
  ↓
  完成
```

扩展现有 Scrubber：

| GC 阶段 | 触发条件 | 操作 |
|---------|---------|------|
| 删除标记 | inode 删除 → refcount 归零 | `deleted_at = now()`, `retention_until = now() + TTL`, 指纹索引 status → Tombstoned |
| Tombstone 扫描 | Scrubber 定期扫描 | retention 未过期 → 跳过；retention 已过期 → 物理删除 needle |
| 指纹清理 | 物理删除后 | 清除 Bloom + HashMap 中的指纹条目 |
| **恢复** | 指纹匹配命中 Tombstoned needle | `deleted_at = None`, `retention_until = None`, refcount=1, status → Active |

```rust
// scrubber.rs 扩展
async fn gc_needle_with_fingerprint(&self, needle_id: NeedleId, volume_id: VolumeId) {
    // ① 检查 needle 状态
    let info = self.get_needle_info(needle_id, volume_id).await?;
    let refcount = self.get_needle_refcount(needle_id, volume_id).await?;

    if refcount > 0 {
        return;  // 仍有引用, 不回收
    }

    // ② refcount=0: 检查是否已标记删除
    if info.deleted_at.is_none() {
        // 首次发现 refcount=0: 标记 Tombstoned (不立即物理删除)
        self.mark_tombstoned(needle_id, volume_id,
            retention = now() + TOMBSTONE_RETENTION
        ).await?;
        // 指纹索引 status → Tombstoned (不清除!)
        self.fingerprint_index.update_status(needle_id, NeedleStatus::Tombstoned).await?;
        return;
    }

    // ③ 已 Tombstoned: 检查 retention 是否过期
    if let Some(retention_until) = info.delete_retention_until {
        if retention_until > now() {
            return;  // retention 未过期, 仍可恢复
        }
    }

    // ④ retention 已过期: 物理删除 needle
    self.delete_needle_physical(needle_id, volume_id).await?;

    // ⑤ 清除指纹索引 (Bloom + HashMap)
    if let Some(fp) = self.fingerprint_index.remove_by_needle(needle_id, volume_id).await? {
        self.bloom_remove(&fp);
        self.hashmap_remove(&fp);
        info!("P5_DEDUP_GC: needle {} (volume {}) physically deleted, fingerprint cleared", needle_id, volume_id);
    }
}
```

**Tombstone retention 配置**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `tombstone_retention` | 1h | 逻辑删除后保留可恢复状态的时间 |
| `tombstone_max_entries` | 100k | Tombstone 池最大条目数，超限时 LRU 淘汰最早 tombstone |
| `tombstone_scan_interval` | 5min | Scrubber 扫描 tombstone 池的间隔 |

### 3.6 xattr 策略下发

复用读侧 ML 预取的 xattr 通道：

| xattr | 值格式 | 说明 |
|-------|--------|------|
| `user.powerfs.write_predict_policy` | `NN:0.3` | ML 预测阈值 0.3 |
| `user.powerfs.write_predict_policy` | `RULE:0.5` | 规则引擎阈值 0.5 |
| `user.powerfs.write_predict_policy` | `off` | 关闭写预测 |

客户端内核侧（kernel module）读取 xattr，决定是否对当前 write 计算 fingerprint。

---

## 4. 模块划分

### 4.1 新增文件

| 模块 | 文件 | 职责 |
|------|------|------|
| **客户端指纹计算** | `powerfs-core/src/fingerprint.rs` | Blake3 hash + LRU 缓存 + 指纹 RPC 构建 |
| **客户端写预判** | `kernel/powerfs_mod/powerfs_write_predict.c` / `.h` | 内核侧写 trace 采集 + xattr 策略读取 + 预判触发 |
| **Filer 指纹索引** | `powerfs-filer/src/fingerprint_index.rs` | BloomFilter + HashMap + NeedleRef 管理 |
| **Filer 写预测 NN** | `powerfs-filer/src/write_predict_policy.rs` | 写模式特征提取 + NN 训练 + xattr 下发 |
| **Filer 写 trace 聚合** | 扩展 `readahead_trace.rs` → `io_trace.rs` | 合并读/写 trace 聚合器 |

### 4.2 修改文件

| 文件 | 改动 |
|------|------|
| [write_coalescer.rs](file:///home/portion/powerfs/powerfs-core/src/write_coalescer.rs) | flush 路径增加 fingerprint hook：flush 后计算 hash，通过 RPC 发 Filer 匹配 |
| [shard_store.rs](file:///home/portion/powerfs/powerfs-filer/src/shard_store.rs) | `StoredFileChunk` 增加 `is_reference: bool` 标记引用来源 |
| [needle.rs](file:///home/portion/powerfs/powerfs-core/src/needle.rs) | `Needle` 增加 `refcount: AtomicU32` 字段 |
| [scrubber.rs](file:///home/portion/powerfs/powerfs-filer/src/scrubber.rs) | GC 扩展：refcount 归零时清除指纹索引 |
| [net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs) | 新增 `MsgType::FingerprintLookup` (0x0042) + `MsgType::PushWriteTrace` (0x0043) |
| [powerfs_readahead.h](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_readahead.h) | 扩展为 `powerfs_predict.h`，增加写预测字段和接口 |

---

## 5. 协议设计

### 5.1 新增 RPC 消息

| MsgType | 值 | 方向 | 用途 |
|---------|----|------|------|
| `FingerprintLookup` | 0x0042 | Client → Filer | 发送指纹 + 数据摘要，请求匹配 |
| `FingerprintLookupResp` | 0x0043 | Filer → Client | 返回匹配结果（命中含 NeedleRef，未命中含 false） |
| `PushWriteTrace` | 0x0044 | Client → Filer | 批量上报写 trace（路径模式、大小序列、偏移模式） |
| `WritePolicyUpdate` | 0x0045 | Filer → Client | 下发写预测 xattr 策略（异步） |

### 5.2 FingerprintLookup TLV 格式

```
FingerprintLookup Body:
  ┌─────────────────────────────────────────────────┐
  │ FieldId::Fingerprint (0xD8)  [32 bytes]          │ Blake3 hash
  │ FieldId::DataSize (0xD9)      [u64]               │ 数据大小
  │ FieldId::DataPrefix (0xDA)    [64 bytes]          │ 数据前 64 字节（二次验证）
  │ FieldId::Inode (0x01)         [u64]               │ 请求来源 inode
  │ FieldId::Offset (0x02)        [u64]               │ 写偏移
  └─────────────────────────────────────────────────┘
  帧的 DATA 段: 仅在 Bloom hit 时携带完整数据（按需传输）

FingerprintLookupResp Body:
  ┌─────────────────────────────────────────────────┐
  │ FieldId::Match (0xDB)         [bool]              │ 是否命中
  │ FieldId::NeedleId (0x03)      [u64]               │ 命中时: needle_id
  │ FieldId::VolumeId (0x04)      [u64]               │ 命中时: volume_id
  │ FieldId::Refcount (0xDC)      [u32]               │ 命中时: 当前 refcount
  └─────────────────────────────────────────────────┘
```

### 5.3 PushWriteTrace TLV 格式

```
PushWriteTrace Body:
  ┌─────────────────────────────────────────────────┐
  │ FieldId::ShardId (0x05)       [u64]               │ shard 路由
  │ FieldId::Count (0x06)         [u32]               │ trace 条数
  │ FieldId::WriteTraceEntries    [variable]          │ WriteTraceEntry[]
  └─────────────────────────────────────────────────┘

WriteTraceEntry (packed, 64 bytes):
  ino:         u64     // inode
  path_hash:   u64     // 路径哈希（隐私保护）
  write_size:  u32     // 写大小
  write_offset: u64    // 写偏移
  file_size:   u64     // 写后文件大小
  overwrite:   bool    // 是否覆盖写
  interval_ms: u32     // 距上次写间隔
  fp_hit:      bool    // 本次指纹是否命中（训练标签来源）
  reserved:    [u8; 7] // 对齐
```

---

## 6. 实施计划

### 6.1 Phase C-0：骨架验证（最小可行）

目标：验证指纹匹配 + 引用机制基本工作，不含 ML。

- [x] C-0.1 `fingerprint.rs`：Blake3 计算 + LRU 缓存 — 5 tests pass
- [x] C-0.2 `fingerprint_index.rs`：Filer 端 Bloom filter + HashMap + NeedleRef 三态 — 9 tests pass
- [x] C-0.3 `FingerprintLookup` RPC (0x0042) + handler — compiles
- [x] C-0.4 `StoredFileChunk.is_reference` 字段（`Needle.refcount` 用 NeedleRef 管理） — compiles
- [x] C-0.5 `FingerprintRecord` RPC (0x0043) + handler — 客户端写完 needle 后注册指纹
- [x] C-0.6 引用建立路径（lookup handler 中 refcount++ + recover） — compiles
- [x] C-0.7 Scrubber GC 扩展（tombstone 扫描 + 过期标记 + Bloom rebuild） — compiles
- [x] C-0.8 Tombstone 恢复路径（lookup handler 中 Recoverable → recover()） — 1 test pass
- [ ] C-0.9 集成测试：相同文件写 2 次，第 2 次命中引用；不同文件不命中；删除后重写命中恢复

### 6.2 Phase C-1：ML 写预测

- [x] C-1.1 kernel 写 trace 采集 — 复用 `powerfs_io_trace_record(kind=1)`, 已在 `powerfs_file.c:799` 调用
- [x] C-1.2 Filer 端写特征聚合 — `IoTraceAggregator::extract_write_features()` 7 特征, `traces_snapshot()`
- [x] C-1.3 `write_predict_policy.rs`: WritePredictNN (7→16→1 MLP) — 4 tests pass
- [x] C-1.4 xattr 下发 `user.powerfs.write_predict_policy` — `apply_ml_write_policy()` 在 trace handler 中触发
- [ ] C-1.5 客户端预判逻辑 + 指纹计算触发
- [ ] C-1.6 安全回退：ML 置信度低 → 不算指纹（正常写）
- [ ] C-1.7 测试：checkpoint 工作负载（周期性覆盖写），验证 ML 正确预判

### 6.3 Phase C-2：性能评估

- [ ] C-2.1 fio 基线对比：写预测开启 vs 关闭，重复写工作负载
- [ ] C-2.2 IO500 写子项对比
- [ ] C-2.3 内存/索引开销评估（Bloom filter FPR、HashMap 大小）
- [ ] C-2.4 多客户端并发写相同内容（Raft 序列化验证）

---

## 7. 风险与缓解

| 风险 | 缓解 |
|------|------|
| **Blake3 碰撞** | 256-bit hash，碰撞概率 2^-128，配合数据前 64 字节二次验证 |
| **指纹索引内存膨胀** | Bloom filter 1% FPR 仅需 ~10 bits/entry；HashMap 按 needle 数估算（1M needle ≈ 48MB） |
| **Tombstone 池膨胀** | `tombstone_max_entries` 上限 + LRU 淘汰；retention 默认 1h 过期后自动物理回收 |
| **Tombstone 恢复后数据损坏** | 恢复操作只清除逻辑标记（`deleted_at`），不触碰物理数据；恢复前二次验证数据前缀 |
| **ML 误判（漏判重复）** | 漏判 → 正常写（无损失，只是少了去重收益）；误判 → 多算一次 hash（可接受 CPU 开销） |
| **并发竞争** | Raft propose 序列化 refcount/recover 操作，天然全序 |
| **引用链断裂** | refcount 归零前不物理删 needle；scrubber 双重检查；tombstone retention 内不回收 |
| **客户端算 hash 增加 latency** | Blake3 ~6 GB/s，1MB needle < 0.2ms；WriteCoalescer 已批量合并减少次数 |
| **冷启动无训练数据** | 规则引擎兜底（覆盖写、checkpoint 路径模式等规则） |
| **恶意碰撞（adversarial）** | Blake3 抗碰撞 + 数据前缀验证；生产环境可加 HMAC |
| **指纹索引与 Raft 一致性延迟** | 指纹索引走 Raft propose，与 chunks 更新同一 log entry，原子一致 |
| **Tombstone 恢复的并发语义** | Raft 全序保证：恢复+删除的先后顺序由 log 决定，最终状态一致 |

---

## 8. 与现有系统的关系

### 8.1 与读侧 ML 预取的关系

| 维度 | 读侧 ML 预取 | 写侧 ML 预测（本方案） |
|------|-------------|----------------------|
| 目标 | 预测读模式 → 调整 readahead | 预测写重复 → 指纹去重 |
| trace 来源 | 内核 read 路径 | 内核 write 路径 |
| 特征 | IOPS, offset CMA, delta_mean, file_size, readahead_mb | write_path_pattern, size_seq, offset_pattern, interval, overwrite_ratio |
| 模型 | ReadaheadNN (5→16→1) | WritePredictNN (7→16→1) |
| xattr | `user.powerfs.readahead_policy` | `user.powerfs.write_predict_policy` |
| 下发策略 | NN:16 / NN:0 / RULE:N | NN:0.3 / RULE:0.5 / off |
| 基础设施复用 | trace 聚合器、NN 训练、xattr 通道、PushTrace RPC | 同 |

**可合并为统一 `io_trace` 模块**：读/写 trace 共享聚合器和传输通道，分别训练独立模型。

### 8.2 与布局预测的关系

[布局预测](file:///home/portion/powerfs/docs/file-layout-prediction-design.md)决定文件数据放在哪里（Inline/Flat/Stripe），本方案决定写数据是否需要去重。两者正交：

```
写请求 → 布局预测 (放哪) → 写预测 (是否去重) → 存储
```

### 8.3 与 WriteCoalescer 的关系

WriteCoalescer 已在 flush 时合并多次写。指纹计算嵌入 flush 后：

```
WriteCoalescer::flush(entry)
  → merged_data = entry.merged  // 已合并的完整数据
  → if write_predict_enabled:
      fp = compute_fingerprint(merged_data)  // Blake3
      match = filer.fingerprint_lookup(fp, merged_data.prefix)
      → hit: reference_existing_needle()
      → miss: normal_write(merged_data) + record_fingerprint(fp, needle_id)
  → else:
      normal_write(merged_data)
```

**无额外延迟**：hash 计算在 flush 时进行（不是每次 write），且 flush 本身已是异步批量操作。

---

## 9. 顶刊可能性评估

### 9.1 Novelty

| 维度 | 传统 dedup | 本方案 | KML |
|------|-----------|--------|-----|
| 预测对象 | 无 | 写重复概率 | 读模式 |
| ML 应用 | 无 | 选择性指纹 | readahead 调优 |
| 分布式 | 部分 | filer 聚合多客户端 trace | N/A（单机） |
| RDMA 感知 | 无 | 指纹匹配可跳过数据传输 | MR 池感知 |

### 9.2 论文故事线

1. **问题**：HPC/AI 工作负载重复写入大量相同数据，传统 dedup 全量 hash 开销大
2. **创新**：ML 预测写重复概率，选择性指纹去重
3. **系统**：PowerFS 分布式文件系统，filer 端协同训练 + RDMA 传输优化
4. **评估**：重复写工作负载下，CPU 开销降低 X%，存储节省 Y%，网络传输减少 Z%

### 9.3 现实路径

- Phase C-0/C-1 工程实现 + fio/IO500 评估 → CLUSTER/SC 工业轨
- 加上读侧 ML 预取（Phase A/B）→ 统一 "ML-driven storage optimization" 故事 → FAST/SC 研究
