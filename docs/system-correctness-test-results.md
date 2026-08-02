# PowerFS 系统正确性测试结果

## 概述

在多客户端（fuse-1 / fuse-2）跨节点挂载环境下，针对 PowerFS FUSE 文件系统的目录条目一致性（CRDT DirORSet）和基础文件操作正确性进行三轮回归测试。测试覆盖大目录递归复制、批量打包/解包、删除重建等典型工作负载，并通过 md5 校验验证数据完整性。

测试期间共发现 5 个一致性问题，已全部修复并通过回归验证。相关修复提交：

| Commit | 修复内容 |
|--------|---------|
| `0a0744f6` | fix(fuse): preserve directory type in lookup and list_children |
| `cffbcf37` | fix(fuse): use DirORSet as authoritative source in readdir/rmdir/rename |
| `23871fcc` | fix(coherence): deduplicate DirORSet entries by name in list_entries |
| `69dca05c` | fix(coherence): remove all same-name EntryIds on delete |
| `d8312d62` | fix(fuse): use DirORSet for EEXIST checks in create/mkdir/symlink/link/rename |

## 测试环境

- 部署方式：Docker Compose 多容器
- 集群规模：
  - Master × 3（master-1/2/3，Raft 3 节点）
  - Filer × 3（filer-1/2/3，Raft 3 节点）
  - Volume × 3（volume-1/2/3）
  - FUSE 客户端 × 2（fuse-1 / fuse-2），均挂载到 `/mnt/powerfs`
- 一致性模型：
  - 目录条目（name→inode）：CRDT DirORSet + 异步 delta sync（弱一致，最终一致）
  - 文件数据（size/chunks）：Filer Raft 强一致
  - 文件数据 I/O：Volume Lease 排他锁（强一致，线性化）

---

## 第一轮：cp -prf + md5 跨客户端验证

### 测试目标

验证大目录递归复制的正确性，以及跨客户端（在 fuse-1 写入，在 fuse-2 读取校验）的目录条目同步一致性。

### 测试方法

1. 在 fuse-1 准备源目录 `cp_src/`，包含多层子目录和不同大小文件。
2. 使用 `cp -prf cp_src cp_dst` 递归复制（保留权限/时间）。
3. 在 fuse-2 上 `find` 列出复制的目录，验证跨客户端可见性。
4. 对源目录和目标目录分别执行 `find ... -exec md5sum`，diff 比对 md5 列表。

### 发现的问题

#### 问题 1.1：cp -prf 复制子目录失败

- **现象**：`cp -prf` 复制到子目录层级时失败，子目录被当作普通文件处理。
- **根因**：`lookup` 路径调用 `lookup_attr_from_filer` 时硬编码 `is_dir = false`，导致子目录类型信息丢失。
- **修复**（commit `0a0744f6`）：
  - 在 [crdt_client.rs](file:///home/portion/powerfs/powerfs-coherence/src/crdt_client.rs) 新增 `lookup_with_type(dir_ino, name) -> Option<(u64, bool)>`，返回 inode 和 `is_dir`。
  - 在 [fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs) lookup 流程中优先调用 `lookup_with_type`，将 `is_dir` 透传到 `lookup_attr_from_filer`。
  - 修复 [cache.rs](file:///home/portion/powerfs/powerfs-fuse/src/cache.rs) `list_children` 直接从 `inode_cache.peek()` 读取 `is_dir`，绕过 TTL 检查避免目录被误判为文件。

#### 问题 1.2：readdir 幽灵条目（No such file or directory）

- **现象**：readdir 返回的某些条目在后续 lookup 时报 `No such file or directory`。
- **根因**：`MetadataCache.path_map` 残留已删除文件的条目，readdir 优先读取 `path_map` 导致返回幽灵文件。
- **修复**（commit `cffbcf37`）：readdir 改为以 **DirORSet 为权威源**，优先读取 `coherence.list_entries(inode)`，仅在 DirORSet 为空时才 fallback 到 `MetadataCache.list_children`。rmdir 和 rename 的目录空性检查同样改用 DirORSet。

#### 问题 1.3：find 阶段未找到文件（io500/pfind 复现）

- **现象**：`find` 命令（及 io500 pfind 阶段）遗漏部分文件。
- **根因**：`MetadataCache::list_children` 因 TTL 过期将目录误判为文件，readdir 返回的 d_type 错误，find 不递归进入子目录。
- **修复**（commit `0a0744f6`）：`list_children` 改为直接读取 `inode_cache`（无 TTL），使用 `peek` 方法避免 LRU 顺序更新，确保 `is_dir` 字段正确。

### 修复后验证结果

- `cp -prf` 递归复制完整成功，所有子目录层级正确。
- fuse-2 跨客户端 `find` 列出条目数与 fuse-1 一致。
- md5 列表 diff 无差异，数据完整性通过。

---

## 第二轮：tar -czf + tar -xzf + md5 验证

### 测试目标

验证大批量文件打包/解包的正确性，以及打包过程中元数据同步延迟对操作的影响。

### 测试方法

1. 在 fuse-1 准备大目录 `tar_src/`（大量小文件 + 部分大文件混合）。
2. `tar -czf /tmp/archive.tar.gz -C tar_src .` 打包。
3. 在 fuse-2 上 `tar -xzf /tmp/archive.tar.gz -C tar_dst` 解包。
4. 对源目录和解包目录分别执行 md5 比对。

### 发现的问题

#### 问题 2.1：tar 打包警告 "file changed as we read it"

- **现象**：`tar -czf` 偶发输出警告 `tar: file changed as we read it`。
- **根因**：FUSE 元数据（mtime）异步 delta sync 延迟，导致 tar 读取过程中文件的 mtime 发生变化。
- **处理**：该警告不影响数据完整性，md5 校验通过。属于 CRDT 弱一致性模型的预期行为，不作为缺陷处理。后续可考虑在 close 时强制同步 mtime 以减少该警告。

### 修复后验证结果

- tar 打包/解包完成，文件数量一致。
- md5 比对全部通过，数据完整。
- "file changed as we read it" 警告偶发但不影响正确性。

---

## 第三轮：rm -rf + 重建 + md5 验证

### 测试目标

验证大规模删除后目录条目彻底清除，以及删除后重建同名文件不出现冲突（CRDT OR-Set 并发语义）。

### 测试方法

1. 在 fuse-1 创建大目录 `rebuild_src/`，包含多层结构和文件。
2. `rm -rf rebuild_src` 删除整个目录树。
3. 在 fuse-2 上验证目录已删除（跨客户端同步）。
4. 在 fuse-2 重建同名目录 `rebuild_src/` 并写入新文件。
5. 在 fuse-1 上 md5 校验重建后的文件。

### 发现的问题

#### 问题 3.1：rm -rf 无法删除非空目录（ENOTEMPTY）

- **现象**：`rm -rf` 删除目录时报 `ENOTEMPTY`，部分子目录无法删除。
- **根因**：DirORSet 是 OR-Set，同一文件名可能存在多个 EntryId（不同 client_id/seq，例如跨客户端并发创建或删除后重建）。`local_remove_entry` 只删除一个 EntryId，`list_entries` 仍返回该名称，导致目录非空判断失败。
- **修复**（commit `23871fcc` + `69dca05c`）：
  - `list_entries` 按名称去重（HashSet），每个文件名只返回一个条目，符合文件系统语义。
  - `local_remove_entry` 遍历删除所有同名 EntryId，为每个删除生成独立的 Remove delta 推送到 ChangeCache。
  - `apply_remote_delta`（Remove 操作）精确匹配指定 EntryId 后，再按名称删除所有剩余同名条目，并记录 tombstone，确保彻底清除。

#### 问题 3.2：cp -prf 偶发 EEXIST 错误

- **现象**：重建目录时 `cp -prf` 偶发报 `EEXIST`，但目标路径实际不存在。
- **根因**：`create`/`mkdir`/`symlink`/`link`/`rename` 的文件存在性检查使用 `MetadataCache`，缓存中残留已删除文件的条目导致误判。
- **修复**（commit `d8312d62`）：新增 `entry_exists(parent, name)` 辅助方法，以 **DirORSet 为权威源** 检查文件存在性：
  - 优先调用 `coherence.lookup_with_type(parent, name)` 判断。
  - DirORSet 无本地副本时（冷启动场景）回退到 `lookup_in_cache`。
  - 所有 EEXIST 检查点（create/mkdir/symlink/link/rename）统一改用 `entry_exists`。

### 修复后验证结果

- `rm -rf` 完整删除目录树，跨客户端同步后 fuse-2 确认目录已不存在。
- 重建同名目录成功，无 EEXIST 误判。
- 跨客户端 md5 校验通过，删除-重建操作的一致性正确。

---

## 修复总结

### 核心设计原则确立

通过本轮测试确立了 PowerFS FUSE 端的一致性权威源分层原则：

| 操作类型 | 权威源 | Fallback |
|---------|--------|----------|
| readdir 目录条目列表 | DirORSet（CRDT 本地副本） | MetadataCache（仅 DirORSet 为空时） |
| rmdir/rename 目录空性检查 | DirORSet | MetadataCache（DirORSet 为空时） |
| EEXIST 文件存在性检查 | DirORSet | MetadataCache（DirORSet 无本地副本时，冷启动） |
| lookup 文件类型 | DirORSet `lookup_with_type` | Filer 查询 |
| 文件数据 size/chunks | Filer Raft（强一致） | - |

### 关键修复点

1. **目录类型透传**：`lookup_with_type` 方法保留 `is_dir` 信息，避免硬编码 `false` 导致子目录被误判为文件。
2. **DirORSet 条目去重**：OR-Set 语义允许同名多 EntryId，但文件系统语义要求每个名称唯一，`list_entries` 按名称去重保证语义正确。
3. **删除彻底性**：`local_remove_entry` 和 `apply_remote_delta` 删除所有同名 EntryId，避免残留导致 ENOTEMPTY。
4. **缓存绕过 TTL**：`list_children` 直接读取 `inode_cache.peek()`，避免 TTL 过期导致的目录类型误判。
5. **EEXIST 权威判断**：以 DirORSet 为权威源，避免 MetadataCache 残留条目导致的误判。

---

## 待办事项

- [ ] fio 性能测试（标准 fio 命令，容器内执行，记录带宽/IOPS/延迟）
- [ ] io500 测试（标准 io500 命令，真实挂载测试）
- [ ] tar "file changed as we read it" 警告的根因优化（可选，不影响正确性）
- [ ] 长时间运行下的 CRDT delta sync 稳定性观察

## 结论

三轮系统正确性测试共发现 5 个一致性问题，全部修复并通过回归验证。PowerFS FUSE 文件系统在多客户端跨节点环境下，目录条目 CRDT 一致性、文件数据强一致性、删除-重建冲突处理均表现正确，md5 数据完整性校验全部通过，具备进入 fio/io500 性能测试阶段的基础。
