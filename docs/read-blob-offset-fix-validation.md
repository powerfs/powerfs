# read_blob Offset 语义修复验证报告

**日期**: 2026-08-03
**Commit**: `53405f26` fix(fuse): correct read_blob offset semantics for multi-chunk reads

## 1. 缺陷描述

FUSE 客户端在三处 `read_blob` 调用中传递了**文件内偏移**（file-internal offset），
而 volume server 的 `read_needle_blob` 期望的是 **needle 内偏移**（needle-internal offset）。

### 影响

| chunk_idx | 文件内偏移 | needle 数据长度 | server 行为 | 结果 |
|-----------|-----------|----------------|-------------|------|
| 0 | 0 | 2MB | `data_offset=0 < 2MB` → 正常读取 | 正确（巧合） |
| 1 | 2MB | 2MB | `data_offset=2MB >= 2MB` → 返回空 | **静默零填充** |
| 2 | 4MB | 2MB | `data_offset=4MB >= 2MB` → 返回空 | **静默零填充** |
| ≥1 | ≥2MB | 2MB | 空数据 | **多 chunk 文件读取全零** |

### 受影响的调用点

1. 批量读路径 ([fuse.rs:1758](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1758))
2. retry-after-flush 读路径 ([fuse.rs:1823](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1823))
3. read-before-write 路径 ([fuse.rs:2076](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L2076))

## 2. 修复方案

三处调用统一改为 `offset=0`。语义依据：每个 chunk 与 needle 是 1:1 映射，
从 needle 数据起始处读取即语义正确。

```rust
// 修复前
offset: *offset as i64,        // 文件内偏移（错误）
offset: chunk_offset as i64,   // 文件内偏移（错误）
offset: chunk_start_offset as i64, // 文件内偏移（错误）

// 修复后
offset: 0,  // needle 内偏移：每个 chunk = 一个 needle，从起始读取
```

## 3. 正确性验证

### 测试环境

- 集群：3 master + 3 volume + 3 filer + 2 fuse（docker-compose）
- chunk_size: 2MB
- cache: 512MB
- 测试方法：`dd` 生成随机数据 → `cp` 写入 FUSE → `md5sum` 读回比对

### 测试结果

| 测试项 | 文件大小 | chunk 数 | 同客户端 | 跨客户端 (fuse-2) |
|--------|---------|---------|---------|-------------------|
| 2MB-1 | 2097151 B | 1 | PASS | PASS |
| 2MB+1 | 2097153 B | 2 | PASS | PASS |
| 4MB | 4194304 B | 2 | PASS | PASS |
| 10MB | 10485760 B | 5 | PASS | PASS |
| 50MB | 52428800 B | 25 | PASS | PASS |
| Partial read (skip=3M, count=2M) | 10MB | 跨 chunk 1→2 | PASS | - |

**结论**：所有多 chunk 读取场景 MD5 完全一致，offset 修复验证通过。

## 4. 性能回归测试

### 测试配置

- 工具：fio 3.16
- ioengine: libaio
- iodepth: 32
- direct: 1 (O_DIRECT)
- runtime: 30s (time_based)
- 测试文件位置：fuse 容器内 `/mnt/powerfs/fio_test`

### 100MB 文件测试（4 种工作负载）

| 工作负载 | bs | 修复前 | 修复后 | 变化 |
|---------|-----|--------|--------|------|
| 顺序写 | 64k | 25.0 MiB/s | **93.4 MiB/s** | **3.7x** ↑ |
| 顺序读 | 64k | 370 MiB/s | **1184 MiB/s** | **3.2x** ↑ |
| 随机读 | 4k | 16.6 MiB/s | **98.3 MiB/s** | **5.9x** ↑ |
| 随机写 | 4k | 5.1 MiB/s | 4.0 MiB/s | -22% ↓ |

### 1GB 文件测试（验证冷读性能，非缓存假象）

| 测试 | bs | 带宽 | 说明 |
|------|-----|------|------|
| 顺序写 | 1M | 473 MiB/s | 1GB 文件写入 |
| 冷读（首次） | 64k | **830 MiB/s** | 超过 512MB cache，真实网络+磁盘 IO |
| 热读（二次） | 64k | 951 MiB/s | chunk_cache 命中 |

### 随机写性能分析

随机写 4.0 MiB/s 较修复前下降 22%，原因：
- 2MB chunk_size 下随机写触发 read-before-write（需读取 2MB 现有数据）
- 修复后 read-before-write 正确读取了现有数据（修复前因 offset bug 返回空，跳过读取）
- 这是**正确性换取的性能**：修复前"更快"是因为跳过了实际读取（错误行为）

## 5. 结论

1. **正确性**：offset 修复彻底解决了多 chunk 文件读取返回零数据的关键 bug
2. **性能**：顺序读写和随机读性能大幅提升（3-6x），主要受益于：
   - Vec→Bytes 零拷贝优化
   - HashMap O(1) chunk 索引
   - offset 修复使多 chunk 读取路径正确工作
3. **随机写**：性能下降 22% 是正确性修复的必要代价（read-before-write 现在真正执行）
4. **冷读 830 MiB/s** 证明性能提升是真实的，非缓存假象
