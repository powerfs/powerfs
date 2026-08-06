# PowerFS Lease 双模式实现变更说明

**日期**: 2026-08-06
**提交**: `51a72138` feat(lease): implement dual-mode lease with range and inode support
         `5d2a76ab` test(lease): add concurrent inode lease verification test suite
**影响范围**: powerfs-net, powerfs-common, powerfs-filer, powerfs-volume, powerfs-fuse-core, powerfs-fuse, powerfs-lease, powerfs-cli

---

## 概述

本次变更实现了 PowerFS 的双模式 Lease 机制，修复了 Range Lease (方案 D) 的客户端缓存粒度问题，并新增了 Inode Metadata Lease (方案 A) 以支持 NVMe-oF target 等不支持 lease 的后端存储。

### 两种 Lease 模式

| 模式 | 配置值 | 管理方 | 粒度 | 适用场景 |
|------|--------|--------|------|----------|
| Range Lease (方案 D) | `mode = "range"` (默认) | Volume Server | per-stripe (64MB) | 标准 Volume Server 后端，支持高并发写不同 stripe |
| Inode Metadata Lease (方案 A) | `mode = "inode"` | Filer | per-inode | NVMe-oF target 等不支持 lease 的后端 |

---

## Phase 1: Range Lease 修复 (方案 D)

### 问题

1. **缓存粒度不匹配**: 客户端 lease 缓存 key 为 `(volume_id, inode)`，服务端按 `(inode, stripe_start)` 校验。写 stripe 1+ 时服务端校验失败。
2. **ensure_lease 固定 stripe 0**: `build_range_lease_tlv(inode, 0, 1, ...)` 硬编码获取 stripe 0，写其他 stripe 时获取了错误的 lease。
3. **release 只释放单 stripe**: `get_valid_lease_token` 只取一个 token，多 stripe 文件的 lease 释放不完整。

### 修复

| 文件 | 修改内容 |
|------|----------|
| `powerfs-fuse-core/src/volume_client.rs` | 缓存 key 改为 `(volume_id, inode, stripe_start)` 三元组，所有 lease 相关方法添加 `stripe_start` 参数 |
| `powerfs-fuse-core/src/provider_adapter.rs` | `ensure_lease` 添加 `offset` 参数，按 `stripe_start = offset / 64MB * 64MB` 动态计算 |
| `powerfs-fuse-core/src/lease.rs` | `release_all_for_inode` 返回 `(stripe_start, token, client_id)` 三元组，调用方使用实际 stripe_start 释放 |
| `powerfs-fuse-core/src/fuse_client_facade.rs` | 适配 volume_client 接口变更，所有 lease 方法传递 stripe_start |
| `powerfs-fuse/src/fuse.rs` | release 回调遍历释放所有 stripe lease（新增 `get_all_valid_lease_tokens_for_inode`） |
| `powerfs-lease/src/manager.rs` | `LeaseManager::release_all_for_inode` trait 签名返回类型改为 `Vec<(u64, String, String)>` |
| `powerfs-lease/src/guard.rs` | Mock 实现适配新返回类型 |

---

## Phase 2: Inode Metadata Lease 实现 (方案 A)

### 设计

Inode Metadata Lease 由 Filer 管理，per-inode 粒度的排他锁。当后端不支持 range lease 时（如 NVMe-oF target），通过 Filer 的 inode lease 保证写入互斥。数据一致性仍由 Raft (`UpdateInodeSizeChunks`) 保证，lease 仅作为准入控制。

**生命周期**:
```
1. acquire_inode_lease(inode) → token
2. write data to Volume Server (lease_enabled=false, 跳过校验)
3. update_inode_size_chunks (Raft 原子提交)
4. release_inode_lease(inode, token)
```

### 协议层

| 文件 | 修改内容 |
|------|----------|
| `powerfs-net/src/protocol.rs` | 新增 `AcquireInodeLease (0x0085)`, `ReleaseInodeLease (0x0086)`, `RenewInodeLease (0x0087)` 消息类型 |

**TLV 字段定义**:

| 消息 | 请求字段 | 响应字段 |
|------|----------|----------|
| AcquireInodeLease | Ino, ClientId, LeaseDuration | LeaseId (token), LeaseDuration (expire_at_ms) |
| ReleaseInodeLease | Ino, ClientId, LeaseToken | (空) |
| RenewInodeLease | Ino, ClientId, LeaseToken, LeaseDuration | (空) |

### 配置层

| 文件 | 修改内容 |
|------|----------|
| `powerfs-common/src/config.rs` | 新增 `LeaseConfig` 结构体 (`mode`, `lease_duration_ms`, `renew_interval_ms`)；`FuseConfig` 添加 `lease: LeaseConfig` 字段；`VolumeConfig` 添加 `lease_enabled: bool` 字段 |
| `powerfs-cli/src/commands/config_gen.rs` | 配置模板添加 `[fuse.lease]` 段和 `volume.lease_enabled` 字段 |

**配置示例**:
```toml
[volume]
lease_enabled = true   # false = 方案 A (NVMe-oF), true = 方案 D (默认)

[fuse.lease]
mode = "range"                # "range" (方案D) 或 "inode" (方案A)
lease_duration_ms = 30000     # lease 有效期 30s
renew_interval_ms = 10000     # 续租间隔 10s
```

### Filer 端

| 文件 | 修改内容 |
|------|----------|
| `powerfs-filer/src/inode_lease_manager.rs` | **新文件**。`InodeLeaseManager` 基于 `RwLock<HashMap<u64, InodeLeaseEntry>>`，支持 acquire/release/renew/validate，grace period 保护，disconnect_holder 清理 |
| `powerfs-filer/src/lib.rs` | 注册 `pub mod inode_lease_manager` |
| `powerfs-filer/src/net_handler.rs` | `FilerNetHandler` 注入 `InodeLeaseManager`，新增 `handle_acquire_inode_lease`/`handle_release_inode_lease`/`handle_renew_inode_lease`，支持 shard leader 路由和 REDIRECT |

### Volume Server 端

| 文件 | 修改内容 |
|------|----------|
| `powerfs-volume/src/server.rs` | `VolumeServer` 新增 `lease_enabled: bool` 字段 + `with_lease_enabled()` builder |
| `powerfs-volume/src/net_handler.rs` | `handle_write_needle` 和 `handle_batch_write_needle` 在 `lease_enabled=false` 时跳过 range lease 校验 |
| `powerfs-volume/src/main.rs` | 从 `cfg.volume.lease_enabled` 读取配置并传入 VolumeServer |

### FUSE 客户端

| 文件 | 修改内容 |
|------|----------|
| `powerfs-fuse-core/src/meta_shard_client.rs` | 新增 `acquire_inode_lease`/`release_inode_lease`/`renew_inode_lease` 异步方法，通过 `send_coherence_msg` 与 Filer 通信，自动处理 REDIRECT + 重试 |
| `powerfs-fuse-core/src/fuse_client_facade.rs` | 新增 `lease_mode`/`lease_duration_ms`/`lease_renew_interval_ms` 配置字段；inode lease 缓存 (`Arc<Mutex<HashMap<u64, InodeLeaseCacheEntry>>>`)；acquire/release/renew 自动管理缓存；`is_inode_lease_mode()` 判断当前模式 |
| `powerfs-fuse-core/src/provider_adapter.rs` | `ensure_lease` 按模式分流：`range` → Volume Server range lease，`inode` → `ensure_inode_lease` (Filer inode lease)；`ensure_inode_lease` 实现快速路径 (缓存命中 + 主动续租) 和慢速路径 (从 Filer 获取) |
| `powerfs-fuse/src/fuse.rs` | release 回调按 lease 模式分流：inode 模式释放单个 inode lease，range 模式保持 per-stripe 释放 |
| `powerfs-fuse/src/main.rs` | 从 `fuse_cfg.lease` 读取 mode/duration/renew_interval，传入 FuseApp |

### 自动缓存管理

`FuseClientFacade` 的 inode lease 方法自动管理缓存，调用方无需手动缓存：

| 方法 | 缓存行为 |
|------|----------|
| `acquire_inode_lease` | 成功后自动缓存 token + 过期时间 |
| `release_inode_lease` | 成功后自动清除缓存 |
| `renew_inode_lease` | 成功后自动更新缓存过期时间 |
| `get_valid_inode_lease_token` | 从缓存读取有效 token + 剩余时间 (无网络) |
| `invalidate_inode_lease` | 手动清除缓存 (用于异常路径) |

### ensure_lease 分流逻辑

```
ensure_lease(volume_id, file_key, inode, offset)
  │
  ├─ is_inode_lease_mode() = true → ensure_inode_lease(inode)
  │   ├─ Fast path: get_valid_inode_lease_token(inode)
  │   │   ├─ remaining < RENEW_THRESHOLD (10s) → renew_inode_lease (best-effort)
  │   │   └─ return cached token
  │   └─ Slow path: acquire_inode_lease(inode) → cache → return token
  │
  └─ is_inode_lease_mode() = false → range lease (existing logic)
      └─ stripe_start = offset / 64MB * 64MB
          └─ acquire/renew range lease from Volume Server
```

---

## 测试验证

### Layer 1: InodeLeaseManager 并发单元测试 (6 个)

| 测试 | 验证内容 |
|------|----------|
| `test_concurrent_acquire_same_inode_mutual_exclusion` | 16 线程竞争同一 inode，只有 1 个成功 |
| `test_concurrent_acquire_different_inodes_no_contention` | 16 线程获取不同 inode，全部成功 |
| `test_concurrent_release_then_acquire` | 释放后等待者通过轮询获取 |
| `test_concurrent_renew_blocks_other_clients` | 持续续租阻止其他客户端获取 |
| `test_concurrent_same_client_idempotent` | 同一 client 并发获取返回相同 token |
| `test_concurrent_disconnect_and_acquire` | disconnect 释放后其他线程立即获取 |

### Layer 2: 客户端集成测试 (9 个)

Mock Filer 实现完整 powerfs-net 二进制协议（握手 + TLV 帧），验证 `FuseClientFacade` 全链路：

| 测试 | 验证内容 |
|------|----------|
| `test_inode_lease_basic_acquire_and_release` | acquire → 缓存命中 → release → 缓存清除 |
| `test_inode_lease_cache_hit_no_network` | 第二次查询走缓存，无网络请求 |
| `test_inode_lease_renew` | 续租请求到达 Filer |
| `test_inode_lease_concurrent_different_inodes` | 8 task 并发不同 inode，全部成功 |
| `test_inode_lease_concurrent_same_inode_mutual_exclusion` | 8 task 并发同一 inode，只有 1 个成功 |
| `test_inode_lease_release_then_reacquire` | 释放后不同 client 重新获取 |
| `test_inode_lease_cache_expiry` | 缓存 100ms 后自动过期 |
| `test_inode_lease_proactive_renew_near_expiry` | 临近过期触发续租，缓存刷新 |
| `test_inode_lease_idempotent_same_client` | 同一 client 二次获取返回相同 token |

### 测试结果

```
powerfs-filer:       68 tests passed (含 11 个 inode_lease_manager 测试)
powerfs-fuse-core:   73 lib + 13 integration + 3 mock_server + 9 inode_lease = 98 tests passed
powerfs-volume:      18 lib + 8 grpc tests passed
cargo clippy:        0 errors (1 pre-existing warning in powerfs-cli)
cargo fmt:           all clean
```

---

## 文件变更汇总

| 分类 | 新增文件 | 修改文件 |
|------|----------|----------|
| 协议 | — | powerfs-net/src/protocol.rs |
| 配置 | — | powerfs-common/src/config.rs, powerfs-cli/src/commands/config_gen.rs |
| Filer | powerfs-filer/src/inode_lease_manager.rs | powerfs-filer/src/lib.rs, net_handler.rs |
| Volume | — | powerfs-volume/src/server.rs, net_handler.rs, main.rs |
| FUSE 核心 | — | powerfs-fuse-core/src/meta_shard_client.rs, fuse_client_facade.rs, provider_adapter.rs, volume_client.rs, lease.rs |
| FUSE | — | powerfs-fuse/src/fuse.rs, main.rs |
| Lease 库 | — | powerfs-lease/src/manager.rs, guard.rs |
| 测试 | powerfs-fuse-core/tests/inode_lease_test.rs | integration_test.rs, mock_server_test.rs |
| 格式化 | — | powerfs-net/*, powerfs-master/*, powerfs-filer/* (cargo fmt) |

**总计**: 2 个新文件, 36 个源码文件修改, 4 个测试文件修改, +2878/-479 行

---

## 迁移指南

### 从纯 Range Lease 迁移到双模式

1. **无需修改**: 默认配置 `mode = "range"`, `lease_enabled = true`，行为与之前一致
2. **启用 Inode Lease (方案 A)**: 修改配置文件
   ```toml
   [volume]
   lease_enabled = false    # Volume Server 跳过 lease 校验

   [fuse.lease]
   mode = "inode"           # FUSE 客户端使用 Filer inode lease
   ```
3. **重启服务**: 依次重启 Volume Server → Filer → FUSE 客户端
4. **验证**: 检查 FUSE 日志中出现 `Lease mode: inode` 和 `ensure_inode_lease: acquiring for inode=...`

### 注意事项

- Inode Lease 模式下，同一 inode 的并发写会被串行化（排他锁），写入吞吐量可能低于 Range Lease 模式
- Filer leader 切换时，inode lease 状态丢失（内存态），客户端通过 `send_coherence_msg` 的 REDIRECT + 重试机制自动恢复
- `lease_duration_ms` 建议 ≥ 30s，`renew_interval_ms` 建议 ≤ `lease_duration_ms / 3`
- Volume Server 的 `lease_enabled` 必须与 FUSE 客户端的 `lease.mode` 一致，否则可能导致一致性校验异常
