# Raft RocksDbStorage Bug 修复记录

> **日期**: 2026-07-31
> **组件**: `powerfs-master/src/raft_storage.rs`
> **参考实现**: `raft-rs/src/storage.rs` (MemStorage)
> **影响范围**: 3 节点 Raft 集群无法选举 Leader，所有 Master 节点持续报 "not leader" 错误

---

## 概述

PowerFS 的 `RocksDbStorage` 实现了 `raft::Storage` trait，但在 6 个关键方法上与 raft-rs 的 `MemStorage` 语义不一致。这些偏差导致 Raft 协议无法正确工作，集群始终无法选举出稳定的 Leader。

**症状**:
- master-1 和 master-2 互相报告 `"not leader"` 错误
- 两个节点同时认为对方是 Leader
- 持续 2+ 小时无 Leader 选举完成
- Filer / Volume 心跳被拒绝处理

**根因**: `RocksDbStorage` 的 Storage trait 实现存在系统性偏差。

---

## Bug 1: `term()` 返回错误的 term 值

### 错误代码

```rust
fn term(&self, idx: u64) -> Result<u64, raft::Error> {
    let hs = self.hard_state.read().unwrap();

    // ❌ BUG: hs.term 是当前 Raft 的 term，不是该 entry 的 term
    if idx == hs.commit {
        return Ok(hs.term);
    }
    // ...
}
```

### raft-rs 正确实现

```rust
// raft-rs/src/storage.rs:478-493
fn term(&self, idx: u64) -> Result<u64> {
    let core = self.rl();
    if idx == core.snapshot_metadata.index {
        return Ok(core.snapshot_metadata.term);  // 返回 snapshot 的 term
    }
    let offset = core.first_index();
    if idx < offset {
        return Err(Error::Store(StorageError::Compacted));
    }
    if idx > core.last_index() {
        return Err(Error::Store(StorageError::Unavailable));
    }
    Ok(core.entries[(idx - offset) as usize].term)  // 返回 entry 实际的 term
}
```

### 根因分析

- `hs.commit` 不等于 entry 的 index。Raft 提交到 index N 时，entry 的 term 可能不等于 `hs.term`。
- `hs.term` 是节点的当前任期号，而不是某个具体日志条目的任期。
- 当 raft-rs 调用 `term(commit_index)` 时，期望获取该 entry 的实际 term（用于匹配和冲突检测）。

### 修复方案

```rust
fn term(&self, idx: u64) -> Result<u64, raft::Error> {
    // 先检查 snapshot 边界
    {
        let sm = self.snapshot_meta.read().unwrap();
        if idx == sm.index {
            return Ok(sm.term);
        }
        if idx < sm.index {
            return Err(RaftError::Store(StorageError::Compacted));
        }
    }

    // 查找 entry 实际的 term
    let entries = self.entries.read().unwrap();
    for entry in entries.iter() {
        if entry.index == idx {
            return Ok(entry.term);
        }
    }
    Err(RaftError::Store(StorageError::Unavailable))
}
```

---

## Bug 2: `first_index()` 未考虑 Snapshot

### 错误代码

```rust
fn first_index(&self) -> Result<u64, raft::Error> {
    let entries = self.entries.read().unwrap();
    Ok(entries.front().map_or(1, |e| e.index))  // ❌ 空 entries 返回硬编码 1
}
```

### raft-rs 正确实现

```rust
// raft-rs/src/storage.rs:223-228
fn first_index(&self) -> u64 {
    match self.entries.first() {
        Some(e) => e.index,
        None => self.snapshot_metadata.index + 1,  // ← snapshot 后 first index
    }
}
```

### 根因分析

- Raft 应用 snapshot 后日志被清空，此时 `first_index` 必须返回 `snapshot.index + 1`。
- 硬编码返回 `1` 导致 raft-rs 认为有索引为 1 的 entry，与实际 snapshot 状态不符。
- 当 `term()` 检查 `idx < first_index` 时，错误地将有效的 snapshot 条目判断为 "已压缩"。

### 修复方案

```rust
fn first_index(&self) -> Result<u64, raft::Error> {
    let entries = self.entries.read().unwrap();
    if let Some(e) = entries.front() {
        Ok(e.index)
    } else {
        // 日志为空 — 第一个索引在 snapshot 之后
        let sm = self.snapshot_meta.read().unwrap();
        Ok(sm.index + 1)
    }
}
```

---

## Bug 3: `last_index()` 未考虑 Snapshot

### 错误代码

```rust
fn last_index(&self) -> Result<u64, raft::Error> {
    let entries = self.entries.read().unwrap();
    let hs = self.hard_state.read().unwrap();
    Ok(entries.back().map_or(hs.commit, |e| e.index))  // ❌ 空 entries 返回 hs.commit
}
```

### raft-rs 正确实现

```rust
// raft-rs/src/storage.rs:230-235
fn last_index(&self) -> u64 {
    match self.entries.last() {
        Some(e) => e.index,
        None => self.snapshot_metadata.index,  // ← snapshot index
    }
}
```

### 根因分析

- `hs.commit` 可能小于 `snapshot.index`（如果 snapshot 后还没有新的提交）。
- 返回错误的 `last_index` 导致 raft-rs 的日志匹配（log matching）算法失败。
- 选举和复制过程中，follower 返回错误的 last_index 被 leader 拒绝。

### 修复方案

```rust
fn last_index(&self) -> Result<u64, raft::Error> {
    let entries = self.entries.read().unwrap();
    if let Some(e) = entries.back() {
        Ok(e.index)
    } else {
        // 日志为空 — 最后一个索引就是 snapshot index
        let sm = self.snapshot_meta.read().unwrap();
        Ok(sm.index)
    }
}
```

---

## Bug 4: 完全缺失 `SnapshotMetadata` 跟踪

### 问题描述

`MemStorageCore` 持有 `snapshot_metadata: SnapshotMetadata` 字段，跟踪：
- `index`: snapshot 覆盖的最高索引
- `term`: snapshot 的任期
- `conf_state`: snapshot 时的配置状态

`RocksDbStorage` 完全没有这个概念，导致 `term()`、`first_index()`、`last_index()` 无法正确处理 snapshot 后的空日志状态。

### 修复方案

1. 在 `RocksDbStorage` 结构体中添加 `snapshot_meta: RwLock<SnapshotMetadata>` 字段
2. 在 `save_state()` 中持久化 snapshot_metadata 到 RocksDB
3. 在 `load_state()` 中加载 snapshot_metadata
4. 在 `apply_snapshot()` 中更新 snapshot_metadata
5. 在 `snapshot()` 返回时同步 snapshot_metadata

```rust
pub struct RocksDbStorage {
    // ...
    /// Snapshot metadata (index + term + conf_state), mirrors raft-rs MemStorage
    snapshot_meta: RwLock<SnapshotMetadata>,
}
```

---

## Bug 5: `new_with_peers()` 未覆盖旧持久化状态

### 错误代码

```rust
pub fn new_with_peers(path: &str, peers: &[u64]) -> Result<Self, String> {
    // ... 设置默认 conf_state (peers) 和 hard_state (term=1)
    
    storage.load_state()?;  // ❌ 加载了旧的持久化状态覆盖了新设置
    
    // ❌ 缺少 save_state() 调用来覆盖
    Ok(storage)
}
```

### raft-rs 正确实现

```rust
// raft-rs/src/storage.rs:408-421
fn initialize_with_conf_state<T>(&self, conf_state: T) {
    assert!(!self.initial_state().unwrap().initialized());  // 必须未初始化
    core.raft_state.conf_state = ConfState::from(conf_state);
}
```

### 根因分析

- `new_with_peers()` 先创建默认的 `conf_state`（包含正确的 peers 列表），然后 `load_state()` 从磁盘加载旧的持久化状态**覆盖**了它。
- 如果之前的运行留下了不同的 voters 列表（如旧节点 ID），新启动的节点会使用错误的配置。
- 这是最可能导致 Leader 选举失败的原因之一：节点认为集群有 3 个成员，但配置与其他节点不匹配。

### 修复方案

```rust
pub fn new_with_peers(path: &str, peers: &[u64]) -> Result<Self, String> {
    // ... 创建 storage，设置默认 hard_state/conf_state/snapshot_meta
    
    storage.load_state()?;  // 加载持久化状态
    
    // 关键：用当前 peers 配置覆盖加载的 conf_state，并持久化
    {
        let mut cs = storage.conf_state.write().unwrap();
        cs.voters.clear();
        cs.voters.extend_from_slice(peers);
    }
    storage.save_state()?;  // 确保新配置被持久化
    
    Ok(storage)
}
```

---

## Bug 6: `entries()` 缺少边界检查

### 错误代码

```rust
fn entries(&self, low: u64, high: u64, ...) -> Result<Vec<Entry>, raft::Error> {
    let max_size = max_size.into().unwrap_or(u64::MAX);
    let entries = self.entries.read().unwrap();
    let mut result = Vec::new();
    // ❌ 没有检查 low < first_index（应返回 Compacted）
    // ❌ 没有检查 high > last_index + 1（应 panic）
    // ...
}
```

### raft-rs 正确实现

```rust
// raft-rs/src/storage.rs:443-475
fn entries(&self, low: u64, high: u64, ...) -> Result<Vec<Entry>> {
    let max_size = max_size.into();
    let mut core = self.wl();
    if low < core.first_index() {
        return Err(Error::Store(StorageError::Compacted));  // ← Compacted 检查
    }
    if high > core.last_index() + 1 {
        panic!(  // ← 越界 panic
            "index out of bound (last: {}, high: {})",
            core.last_index() + 1, high
        );
    }
    // ...
}
```

### 根因分析

- raft-rs 在请求已压缩的日志范围时返回 `Compacted` 错误，让上层知道需要发送 snapshot。
- 缺少这个检查可能导致 raft-rs 在某些路径上 panic 或行为异常。

### 修复方案

```rust
fn entries(&self, low: u64, high: u64, max_size: ..., _context: ...) -> Result<Vec<Entry>, raft::Error> {
    // 检查 Compacted
    let first_idx = self.first_index()?;
    if low < first_idx {
        return Err(RaftError::Store(StorageError::Compacted));
    }
    
    // 检查越界
    let last_idx = self.last_index()?;
    if high > last_idx + 1 {
        panic!("entries range [{}, {}) out of bounds (last_index={})", low, high, last_idx);
    }
    // ...
}
```

---

## 修复验证

### 代码质量

```bash
# 格式化检查
cargo fmt --all -- --check  # ✅ Passed

# Clippy 零警告
cargo clippy --package powerfs-master -- -D warnings  # ✅ Passed

# 编译
cargo build --release --package powerfs-master  # ✅ Passed

# 单元测试
cargo test --package powerfs-master  # ✅ 5 passed, 0 failed
```

### 集群验证

清理旧数据卷后重建集群：

```bash
docker volume rm docker_master-1-data docker_master-2-data docker_master-3-data
# ... 重建镜像并启动 ...
docker compose up -d redis master-1 master-2 master-3 volume-1 volume-2 volume-3 ...
```

**选举结果**:
- master-2 (172.20.0.12) → **Leader**, Term=1
- master-1 (172.20.0.11) → **Follower**, Term=1
- master-3 (172.20.0.13) → **Follower**, Term=1

**关键指标**:
- ✅ 零 "not leader" 错误
- ✅ 3 节点稳定心跳交换
- ✅ Volume 心跳正常处理（Leader 接收并响应）
- ✅ 拓扑 API 正确显示 3 Masters + 3 Filers + 3 Volume Servers

---

## 经验总结

### 实现 raft::Storage trait 的关键原则

1. **term(idx)** 必须返回 **entry 实际的 term**，而不是节点的当前 term。这是 Raft 日志匹配的基础。

2. **first_index()** 和 **last_index()** 必须正确处理**空日志 + snapshot** 的边界情况。空日志时分别返回 `snapshot.index + 1` 和 `snapshot.index`。

3. **SnapshotMetadata** 是 Storage 的必要组成部分。它跟踪 snapshot 的 index、term 和 conf_state，是 term/first_index/last_index 正确工作的前提。

4. **初始化必须覆盖旧状态**。`new_with_peers()` 加载旧状态后必须用当前配置覆盖 conf_state，否则旧的 voters 列表会导致集群配置不一致。

5. **边界检查与 raft-rs 对齐**。`entries()` 必须对 `low < first_index` 返回 `Compacted`，对 `high > last_index + 1` 触发 panic。

6. **始终用 MemStorage 做参考**。raft-rs 的 MemStorage 是 Storage trait 的参考实现，任何自定义实现都应严格对齐其语义。

### 调试技巧

- **没有 Raft Debug 日志**: 检查 `load_state()` 是否正确加载了 hard_state（特别是 term 和 commit 值）
- **持续 "not leader"**: 检查 conf_state 的 voters 列表是否在所有节点上一致
- **选举超时**: 检查 `first_index()` / `last_index()` 在空日志时的返回值，它们决定了日志匹配是否成功
- **快速验证**: 使用 `docker logs master-X | grep "not leader"` 确认是否还有错误

### 相关文件

| 文件 | 说明 |
|------|------|
| `powerfs-master/src/raft_storage.rs` | RocksDbStorage 实现（已修复） |
| `powerfs-master/src/raft_node.rs` | Raft 节点封装 |
| `raft-rs/src/storage.rs` | raft-rs MemStorage 参考实现 |
| `raft-rs/src/errors.rs` | StorageError 定义 |
