# PowerFS 目录级 Lease 租约设计方案

> 状态：**待讨论确认**
> 编写日期：2026-08-07
> 适用范围：**内核态客户端（powerfs_mod）+ 服务器（Filer/Master/Volume）**
> 不涉及：FUSE 客户端（FUSE 客户端的 lease 机制独立，见 `lease-design.md`）
> 相关文件：`kernel/powerfs_mod/powerfs_fs.c`、`kernel/powerfs_mod/powerfs.h`
> 替代：per-dentry lease 方案（`powerfs_dentry_info.lease_expire`）

---

## 0. 适用范围

本方案仅针对 **内核态客户端**（`powerfs_mod`，即 `powerfs.ko` 内核模块）与 **服务器端**（Filer/Master/Volume）之间的目录 lease 机制。

- **内核客户端**：通过 `powerfs_net` TCP 协议直连 Filer，无用户态代理
- **服务器端**：Filer 管理 inode/dentry 元数据，Volume 管理数据
- **FUSE 客户端**：不在本方案范围内，FUSE 客户端的 data lease 机制见 `lease-design.md`

后续文中"客户端"均指内核态客户端（`powerfs_mod`），"服务器"均指 Filer。

## 1. 背景与问题

### 1.1 当前架构（per-dentry lease）

PowerFS 内核态文件系统当前对每个 dentry 维护独立 lease：

```c
struct powerfs_dentry_info {
    unsigned long lease_expire;   /* per-dentry 租约过期时间 */
    struct list_head lease_list;  /* 全局 lease 链表节点 */
    ...
};
```

`d_revalidate` 检查 per-dentry lease：
```c
if (time_after(jiffies, di->lease_expire)) {
    return 0;  /* 触发 d_invalidate + d_drop + re-lookup */
}
```

### 1.2 发现的致命问题

**RCU stall 根因：负 dentry 的 `d_revalidate` 返回 0**

测试确认：
| d_revalidate 行为 | 测试结果 |
|---|---|
| `NULL`（始终返回 1） | 4 分钟无 stall |
| 负 dentry 返回 1，正 dentry 返回 0 | 4 分钟无 stall |
| 负 dentry 返回 0（lease 过期），正 dentry 返回 0 | **75s 后 stall** |

返回 0 触发的 VFS 路径：
```
d_revalidate return 0
  → d_invalidate(dentry)
    → d_drop(dentry)          [从 hash 链移除]
  → dput(dentry)              [refcount--, 负 dentry 无 inode 走快速释放]
  → d_alloc_parallel()
    → __d_lookup_rcu()        [RCU 无锁遍历 hash 链]
      → 无限循环 → RCU stall
```

负 dentry 释放路径比正 dentry 更短（无 `iput`/`dentry_unlist`），dentry 更快被回收，与 `__d_lookup_rcu` 的 RCU 遍历产生竞态。

### 1.3 per-dentry lease 的其他缺陷

- **逻辑分散**：每个 dentry 独立 TTL，目录下 N 个文件需要 N 次校验
- **查询雪崩**：lease 过期后，目录下所有子文件同时触发网络查询
- **复杂度高**：维护 lease_list 全局链表 + 定时扫描续约
- **内存开销**：每个 dentry 多 16 字节（lease_expire + lease_list）

---

## 2. 核心设计思路（目录级 Lease）

### 2.1 核心规则

**租约主体绑定在父目录 inode（`powerfs_inode_info`）**，子文件/子目录（正 dentry + 负 dentry）校验逻辑统一简化：

```
d_revalidate(dentry, flags):
    if (RCU 模式):
        if (父目录 lease 有效):
            return 1                        // 正/负 dentry 全部放行
        else:
            return -ECHILD                  // 降级 REF 路径
    
    // REF 路径: 检查父目录 lease
    if (父目录 lease 有效):
        return 1
    
    // lease 过期: 仍返回 1, 不触发 d_drop
    // 由 readdir / lookup 统一刷新父目录 lease
    return 1
```

**硬性约束**：`d_revalidate` 全程 **永不返回 0**。RCU 路径只返回 `1` 或 `-ECHILD`，REF 路径统一返回 `1`。

### 2.2 优势对比

| 维度 | per-dentry lease | 目录级 lease |
|---|---|---|
| RCU 安全性 | 负 dentry return 0 → stall | **永不 return 0，无 stall** |
| 校验开销 | 每个子文件独立校验 | **一次父目录 lease 校验** |
| 网络交互 | lease 过期后逐个 RPC | **readdir 聚合刷新** |
| 热目录性能 | 每文件独立 TTL 命中 | **全程 RCU 无锁命中** |
| 冷目录回收 | 逐个 dentry 过期 | **dcache shrinker 自然回收** |
| 集群适配 | 需逐文件推送变更 | **撤销目录 lease 即可** |

### 2.3 短板与规避

| 短板 | 规避策略 |
|---|---|
| 父目录内任一文件修改 → 整个目录 lease 过期 | 高频写入目录：短 lease（2s）；静态只读：长 lease（15s） |
| 跨客户端新创建文件可见性延迟 = lease 时长 | TTL 可配，默认 5s；getattr 绕过 lease 直查 Filer |
| 超大目录 lease 粒度过粗 | 后续可扩展分片 lease 或子目录独立 lease |

---

## 3. 结构体改造

### 3.1 `powerfs_inode_info`（已有字段复用）

现有字段已满足需求，无需新增：

```c
struct powerfs_inode_info {
    ...
    /* 目录缓存 */
    bool dir_complete;              /* 目录内容是否完整缓存 */
    struct list_head dir_entries;   /* 目录项链表 (readdir 缓存) */
    struct mutex dir_mutex;         /* 保护 dir_entries */
    
    /* === 目录 Lease (本方案核心) ===
     * dir_lease_expire: 目录 lease 过期时间 (jiffies)
     *   - readdir 成功后设为 now + POWERFS_DIR_LEASE_TTL
     *   - lookup 成功后续约
     *   - 本地 mutation (mkdir/rmdir/create/unlink/rename) 时清零
     *   - d_revalidate 检查此字段决定是否放行缓存
     * dir_lease_epoch: 单调递增, 本地 mutation 时自增, 留给 Phase 3
     *   callback 比对 */
    unsigned long dir_lease_expire;
    u64 dir_lease_epoch;
    ...
};
```

### 3.2 `powerfs_dentry_info`（简化）

移除 per-dentry lease 字段，仅保留必要的 RCU 释放和 readdir 偏移：

```c
struct powerfs_dentry_info {
    struct dentry *dentry;          /* 所属 dentry */
    /* 移除: unsigned long lease_expire; */
    /* 移除: struct list_head lease_list; */
    unsigned long time;             /* 创建时间 (调试用) */
    u64 offset;                     /* readdir 偏移 */
    struct rcu_head rcu;            /* RCU 延迟释放 */
};
```

### 3.3 TTL 配置调整

```c
/* 目录 lease 超时 (jiffies, 默认 5 秒)
 * - readdir/lookup 成功后续约
 * - 本地 mutation 时清零
 * 平衡: 太短 → 频繁网络查询; 太长 → 跨客户端可见性延迟 */
#define POWERFS_DIR_LEASE_TTL       (5 * HZ)

/* 移除: per-dentry lease TTL */
/* #define POWERFS_DENTRY_LEASE_TTL  (5 * HZ) */  /* 已废弃 */
```

---

## 4. 核心代码实现

### 4.1 `d_revalidate` 改造（核心）

```c
/*
 * d_revalidate - 基于父目录 Lease 校验 dentry 有效性
 *
 * 核心原则:
 *   1. RCU 路径: 只读检查父目录 lease, 有效返回 1, 过期返回 -ECHILD
 *   2. REF 路径: 统一返回 1 (永不返回 0)
 *   3. 永不触发 d_invalidate + d_drop + re-lookup 循环
 *
 * 返回值:
 *   1: dentry 有效, 使用缓存
 *   -ECHILD: 退出 RCU, 切换 REF 路径 (仅 RCU 模式)
 *
 * 参考: ceph_d_revalidate (fs/ceph/dir.c)
 */
int powerfs_d_revalidate(struct inode *dir, const struct qstr *name,
                         struct dentry *dentry, unsigned int flags)
{
    struct powerfs_inode_info *parent_pi;
    unsigned long lease_expire;

    /* === RCU 路径: 无锁检查父目录 lease ===
     * 
     * 用 READ_ONCE 读取 dir_lease_expire, 不持任何 spinlock.
     * 值可能略旧 (并发 mutation 刚清零), 但最差情况是放行一个
     * stale dentry, 下次访问会纠正, 不会导致 stall. */
    if (flags & LOOKUP_RCU) {
        parent_pi = POWERFS_I(dir);
        lease_expire = READ_ONCE(parent_pi->dir_lease_expire);

        if (time_before(jiffies, lease_expire))
            return 1;       /* 父目录 lease 有效: 正/负 dentry 全部放行 */
        else
            return -ECHILD; /* lease 过期: 降级 REF 路径 */
    }

    /* === REF 路径: 统一返回 1 ===
     *
     * 不在此处做 lease 续约 RPC (避免 d_revalidate 阻塞路径遍历).
     * lease 续约由 readdir / lookup 统一处理.
     * 
     * 为什么不返回 0:
     *   return 0 触发 d_invalidate + d_drop + d_alloc_parallel,
     *   负 dentry 释放与 __d_lookup_rcu 竞态 → RCU stall.
     *   返回 1 让 VFS 使用缓存, stale dentry 由 readdir/shrinker 清理. */
    return 1;
}
```

### 4.2 `powerfs_lookup` 改造

lookup 成功后续约父目录 lease：

```c
static struct dentry *powerfs_lookup(struct inode *dir,
                                      struct dentry *dentry,
                                      unsigned int flags)
{
    struct powerfs_inode_info *dir_pi = POWERFS_I(dir);
    /* ... 网络查询逻辑不变 ... */

    if (inode_found) {
        d_add(dentry, inode);
    } else {
        d_add(dentry, NULL);  /* 负 dentry, 依赖父目录 lease */
    }

    /* lookup 成功: 续约父目录 lease (一次 RPC 同时完成查询+续约) */
    WRITE_ONCE(dir_pi->dir_lease_expire,
               jiffies + POWERFS_DIR_LEASE_TTL);

    return NULL;
}
```

### 4.3 `powerfs_readdir` 改造（已有，微调）

readdir 成功后刷新 lease（现有逻辑已满足，确认无冲突）：

```c
int powerfs_readdir(struct file *file, struct dir_context *ctx)
{
    /* ... */
    
    /* fast-path: lease 未过期, 直接用缓存 */
    if (READ_ONCE(dpi->dir_complete) &&
        time_before(jiffies, READ_ONCE(dpi->dir_lease_expire))) {
        goto emit_cached;
    }
    
    /* lease 过期: 从 Filer 拉取目录列表 */
    ret = powerfs_fill_readdir_cache(dfi, dir);
    if (ret == 0) {
        /* 拉取成功: 续约 */
        WRITE_ONCE(dpi->dir_lease_expire,
                   jiffies + POWERFS_DIR_LEASE_TTL);
    }
    
    /* ... emit ... */
}
```

### 4.4 `powerfs_invalidate_dir_lease`（已有，保留）

本地 mutation 时失效父目录 lease：

```c
static void powerfs_invalidate_dir_lease(struct inode *dir)
{
    struct powerfs_inode_info *dpi = POWERFS_I(dir);
    
    if (!dir || !S_ISDIR(dir->i_mode))
        return;
    
    mutex_lock(&dpi->dir_mutex);
    WRITE_ONCE(dpi->dir_lease_expire, 0);   /* lease 失效 */
    dpi->dir_lease_epoch++;                 /* 版本号自增 */
    WRITE_ONCE(dpi->dir_complete, false);   /* 缓存不完整 */
    mutex_unlock(&dpi->dir_mutex);
}
```

调用点（已有，确认全部覆盖）：
- `powerfs_mkdir` / `powerfs_rmdir`
- `powerfs_create` / `powerfs_unlink`
- `powerfs_symlink` / `powerfs_link`
- `powerfs_rename`（old_dir + new_dir 都失效）

### 4.5 `d_init` / `d_release` 改造

`d_init` 简化（移除 lease 初始化）：
```c
int powerfs_d_init(struct dentry *dentry)
{
    struct powerfs_dentry_info *di;
    di = kmem_cache_zalloc(powerfs_dentry_cachep, GFP_KERNEL);
    if (!di)
        return -ENOMEM;
    di->dentry = dentry;
    di->time = jiffies;
    /* 移除: di->lease_expire / INIT_LIST_HEAD(&di->lease_list) */
    dentry->d_fsdata = di;
    return 0;
}
```

`d_release` 恢复（释放 dentry_info，RCU 延迟释放防止 UAF）：
```c
void powerfs_d_release(struct dentry *dentry)
{
    struct powerfs_dentry_info *di = dentry->d_fsdata;
    if (!di)
        return;
    /* 移除: lease_list 摘除逻辑 */
    dentry->d_fsdata = NULL;
    call_rcu(&di->rcu, powerfs_di_free_rcu);
}
```

### 4.6 dentry_operations 注册

```c
static const struct dentry_operations powerfs_dentry_operations = {
    .d_revalidate   = powerfs_d_revalidate,
    .d_init         = powerfs_d_init,
    .d_release      = powerfs_d_release,    /* 恢复: 防止 di 内存泄漏 */
    .d_prune        = powerfs_d_prune,      /* 恢复: 清父目录 dir_complete */
};
```

---

## 5. dentry 生命周期与 RCU 安全性分析

### 5.1 d_revalidate 不返回 0 的安全性

| 场景 | d_revalidate 返回 | VFS 行为 | 安全性 |
|---|---|---|---|
| 父目录 lease 有效 (RCU) | 1 | 使用缓存 dentry | 无 d_drop, 无 stall |
| 父目录 lease 过期 (RCU) | -ECHILD | 退出 RCU, 切 REF | 无 d_drop, 无 stall |
| REF 路径 | 1 | 使用缓存 dentry | 无 d_drop, 无 stall |

**对比旧方案**：旧方案 lease 过期返回 0 → `d_invalidate` → `d_drop` → `d_alloc_parallel` → `__d_lookup_rcu` → stall。

### 5.2 stale dentry 的清理路径

由于 `d_revalidate` 不再返回 0，stale dentry 通过以下路径清理：

1. **readdir 刷新**：lease 过期后，下次 `readdir` 重新拉取目录列表，`dir_complete = false` 触发清空旧 `dir_entries`
2. **lookup 覆盖**：新 lookup 调用 `powerfs_lookup`，`d_add` 覆盖旧 dentry 的 inode
3. **shrinker 回收**：内存紧张时，`prune_dcache` 回收冷 dentry
4. **本地 mutation**：`mkdir/unlink` 等主动失效 lease + 清 dir_complete

### 5.3 负 dentry 的生命周期

```
创建: lookup 返回 -ENOENT → d_add(dentry, NULL)
命中: d_revalidate 检查父目录 lease → 有效返回 1 → VFS 返回 ENOENT
清理: 
  - readdir 刷新 → 父目录 dir_complete=false → shrink_dcache_parent 回收
  - 文件被创建 → 本地 mutation 失效 lease → 下次 lookup 覆盖负 dentry
  - 内存紧张 → prune_dcache 回收
```

---

## 6. 并发与一致性分析

### 6.1 RCU 路径无锁读取

```c
lease_expire = READ_ONCE(parent_pi->dir_lease_expire);
```

- **并发写**：`WRITE_ONCE(dir_lease_expire, ...)` 由 readdir/lookup/mutation 调用
- **原子性**：`unsigned long` 在 64 位平台读写原子
- **竞态容忍**：最差情况放行一个 stale dentry（lease 刚被清零但读取到旧值），下次访问纠正

### 6.2 跨客户端一致性

| 事件 | 本地 lease 状态 | 行为 |
|---|---|---|
| 客户端 A 创建文件 X | A 的目录 lease 不变 | A 下次 readdir 刷新看到 X |
| 客户端 B 持有目录 lease | B 的 lease 仍有效 | **B 在 lease 过期前看不到 X** |
| B 的 lease 过期 | `d_revalidate` 返回 1 (REF) | B 下次 readdir/lookup 刷新 lease, 看到 X |

最大延迟 = `POWERFS_DIR_LEASE_TTL`（默认 5s）。对于强一致性要求场景，可通过 `getattr` 绕过 lease 直查 Filer。

### 6.3 本地 mutation 顺序

```
mkdir /mnt/pfs/dir/newfile
  → powerfs_mkdir
    → 网络请求 Filer 创建
    → 成功后:
      1. powerfs_invalidate_dir_lease(dir)  [lease_expire=0, dir_complete=false]
      2. 本地 add_dir_entry
  → 返回用户

下次 ls /mnt/pfs/dir:
  → d_revalidate 检查 lease → 过期 (返回 1)
  → readdir 检查 dir_complete=false → 重新拉取 → 看到 newfile
```

---

## 7. 实施计划

### Phase 1：核心改造（本次实施）

1. **`powerfs_d_revalidate`**：改为父目录 lease 校验，永不返回 0
2. **`powerfs_d_init`**：移除 per-dentry lease 初始化
3. **`powerfs_d_release`**：恢复（移除 lease_list 逻辑，保留 RCU 释放）
4. **`powerfs_d_prune`**：恢复（清父目录 dir_complete）
5. **`powerfs_lookup`**：成功后续约父目录 lease
6. **`powerfs_dentry_operations`**：注册 d_release + d_prune

### Phase 2：测试验证

1. **基础测试**：create + ls 简单操作，QEMU 运行 ≥1 分钟无 stall
2. **dmesg 检查**：确认无 RCU stall、无 SLUB 报错
3. **功能验证**：负 dentry 正确返回 ENOENT、正 dentry 属性正确
4. **并发测试**：多进程同时 create + ls

### Phase 3：性能验证

1. **fio 测试**：对比 per-dentry lease 的性能差异
2. **热目录场景**：频繁 ls 同一目录，验证 RCU 命中率
3. **冷目录场景**：大量目录，验证 shrinker 回收效率

### Phase 4：集群主动失效（后续）

1. Filer 端目录 lease 管理（创建/撤销）
2. 客户端 callback 处理（收到撤销通知 → `lease_valid = false`）
3. `dir_lease_epoch` 比对逻辑

---

## 8. 改动文件清单

| 文件 | 改动 |
|---|---|
| `powerfs.h` | 移除 `POWERFS_DENTRY_LEASE_TTL`；`powerfs_dentry_info` 移除 `lease_expire` + `lease_list` |
| `powerfs_fs.c` | `d_revalidate` 改为父目录 lease 校验；`d_init`/`d_release`/`d_prune` 恢复并简化；`lookup` 续约父目录 lease |
| `powerfs_net.c` | 无改动（lease renew 网络逻辑后续 Phase 4） |

---

## 9. 风险评估

| 风项 | 影响 | 缓解 |
|---|---|---|
| stale dentry 导致错误 ENOENT | 跨客户端创建文件后，本地负 dentry 仍命中 | TTL=5s 可接受；getattr 绕过 lease |
| stale 正 dentry 属性过期 | 文件 size/mtime 可能过时 | getattr 路径直查 Filer；open 时刷新 |
| d_release 恢复引入新问题 | RCU 释放逻辑可能有边界 case | 已有 call_rcu 延迟释放，UAF 已修复 |
| 内存泄漏 | d_release 不恢复会导致 di 泄漏 | 必须恢复 d_release |
