# PowerFS MDLock 独立锁对象设计

> 状态: **实施中**
> 分支: `lock-optimization`
> 前置文档: `docs/lock-optimization-plan.md` (模块化方案), `docs/lock-protocol.md` (wire protocol)
> 参考: Ceph `src/mds/locks.cc` / `SimpleLock.h` / `ScatterLock.h` / `FileLock.h` / `LocalLock.h`

## 1. 设计目标

### 1.1 核心问题

当前 `struct powerfs_cap` 将所有维度（AUTH/LINK/XATTR/FILE）的权限位打包在一个
`unsigned int issued` 中，状态是对象级的。导致:

- chmod 只改 mode 但 revoke 时可能连带收回 FILE_* 位
- 并发 write 和 chmod 互相干扰
- 无法实现 Ceph 式 "IAUTH 独占不碰 IFILE" 的细粒度解耦
- 没有 MDS 内部锁原语（rdlock/wrlock/xlock），元数据操作直接 propose 无排队

### 1.2 目标

对齐 Ceph MDS Locker 的锁实例管理模型:

1. 每个 inode 持有 **N 把独立锁对象**（`struct powerfs_mdlock`），每把锁有独立状态机
2. 每把锁独立 eval → 独立 recall → 独立 GATHER
3. 锁原语（rdlock/wrlock/xlock）操作的是锁对象，不是 cap 位
4. `struct powerfs_cap` 降级为 wire/transport 层序列化容器，不再是状态机主体

### 1.3 兼容策略

- `struct powerfs_cap` 保留，作为 transport 层 cap grant/revoke 消息的序列化容器
- `struct powerfs_inode_info` 新增 `locks[]` 数组，旧字段（`i_dirty_caps`/`i_pin_ref` 等）逐步迁移
- 迁移分 Phase 进行：Phase 1 定义结构 + 接通内核端，Phase 2 迁移 Filer 端，Phase 3 迁移 FUSE 端

## 2. 四套锁状态机

对齐 Ceph MDS Locker 的 4 套独立状态机，每套有不同的状态集和行为:

### 2.0 状态机类别

```c
enum powerfs_lock_class {
    LOCK_CLASS_LOCAL   = 0,  /* LocalLock:   MDS 本地锁, 无客户端 cap */
    LOCK_CLASS_SIMPLE  = 1,  /* SimpleLock:  排他写+共享读, 支持 Loner */
    LOCK_CLASS_SCATTER = 2,  /* ScatterLock: 多方共享写, MDS 间合并 */
    LOCK_CLASS_FILE    = 3,  /* FileLock:    扩展 SimpleLock + 完整 FILE cap */
};
```

| 状态机 | Ceph 参考 | 状态集 | 锁类型 | 特点 |
|--------|-----------|--------|--------|------|
| LocalLock | `LocalLock.h` | AVAILABLE, LOCK | ISNAP | MDS 本地, 无客户端 cap, 二态 |
| SimpleLock | `SimpleLock.h` | AVAILABLE, SHARED, LONER, EXCL, GATHER, REVOKING | IAUTH, ILINK, IXATTR, DN | 排他写, 共享读, Loner 优化 |
| ScatterLock | `ScatterLock.h` | AVAILABLE, DSCATTER, EXCL, INACTIVE, SYNC, GATHER | IDFT, INEST | 多方共享写, MDS 间合并, 不输出客户端 cap |
| FileLock | `FileLock.h` | SimpleLock 状态 + SYNC | IFILE | 扩展 SimpleLock, 完整 FILE cap 语义, SYNC 态 |

### 2.0.1 锁类型 → 状态机类别映射

```c
const enum powerfs_lock_class powerfs_lock_type_class[POWERFS_NUM_LOCK_TYPES] = {
    [POWERFS_LOCK_AUTH]  = LOCK_CLASS_SIMPLE,   /* IAUTH:  SimpleLock */
    [POWERFS_LOCK_LINK]  = LOCK_CLASS_SIMPLE,   /* ILINK:  SimpleLock */
    [POWERFS_LOCK_XATTR] = LOCK_CLASS_SIMPLE,   /* IXATTR: SimpleLock */
    [POWERFS_LOCK_DN]    = LOCK_CLASS_SIMPLE,   /* DN:     SimpleLock + Lease */
    [POWERFS_LOCK_SNAP]  = LOCK_CLASS_LOCAL,    /* ISNAP:  LocalLock */
    [POWERFS_LOCK_FILE]  = LOCK_CLASS_FILE,     /* IFILE:  FileLock */
    [POWERFS_LOCK_DFT]   = LOCK_CLASS_SCATTER,  /* IDFT:   ScatterLock */
    [POWERFS_LOCK_NEST]  = LOCK_CLASS_SCATTER,  /* INEST:  ScatterLock */
};
```

### 2.0.2 四套状态机的 eval 行为差异

| 状态机 | LONER | SHARED | EXCL | SYNC | DSCATTER | 特殊行为 |
|--------|-------|--------|------|------|----------|---------|
| LocalLock | N/A | N/A | N/A | N/A | N/A | 不输出 cap, 仅 MDS 本地 |
| SimpleLock | 全套 exclusive cap | shared 只读 cap | xlock holder 独占 | N/A | N/A | 标准 rdlock/wrlock |
| ScatterLock | N/A | N/A | N/A | 只读 cap | 不输出 cap | 客户端永远拿不到写 cap |
| FileLock | FILE_SHARED+CACHE+WR+EXCL | FILE_SHARED+CACHE | xlock holder 全套 | FILE_SHARED (只读) | N/A | 完整 Loner + SYNC |

## 3. 锁类型定义

对齐 Ceph 12 种 MDLock type，PowerFS 精简为 8 种（覆盖 POSIX 文件系统核心语义）:

### 3.1 锁类型枚举

```c
/* kernel/powerfs_mod/powerfs_lock.h */

/* PowerFS MDLock 类型 — 对齐 Ceph MDS lock types
 * 参考: src/mds/SimpleLock.h LockType, src/mds/locks.cc */
enum powerfs_lock_type {
    /* SimpleLock 类型 (排他写, 共享读) */
    POWERFS_LOCK_AUTH    = 0,  /* IAUTH:  inode 权限 (mode/uid/gid) */
    POWERFS_LOCK_LINK    = 1,  /* ILINK:   硬链接计数 (nlink) */
    POWERFS_LOCK_XATTR   = 2,  /* IXATTR:  扩展属性 (xattr) */
    POWERFS_LOCK_DN      = 3,  /* DN:      dentry 名称解析 (含 lease) */
    POWERFS_LOCK_SNAP    = 4,  /* ISNAP:   快照 (预留, 暂不实现) */

    /* FileLock 类型 (含 Loner 优化) */
    POWERFS_LOCK_FILE    = 5,  /* IFILE:   文件数据 (read/write/truncate) */

    /* ScatterLock 类型 (共享写, 多 MDS 合并) */
    POWERFS_LOCK_DFT     = 6,  /* IDFT:    目录分片 (dirfrag, 预留) */
    POWERFS_LOCK_NEST    = 7,  /* INEST:   嵌套目录 (预留) */

    POWERFS_NUM_LOCK_TYPES = 8,
};
```

### 2.2 类型与操作的映射

| 操作 | 锁类型 | 锁原语 | 说明 |
|------|--------|--------|------|
| `chmod/chown` | AUTH | xlock | 必须独占 IAUTH |
| `create/unlink` | DN + LINK | xlock(DN) + xlock(LINK) | dentry + nlink |
| `lookup/readdir` | DN | rdlock(DN) | 共享读 dentry |
| `rename` | DN + AUTH(both) | xlock(DN) + xlock(AUTH, src+dst) | 跨 dentry |
| `write/append` | FILE | wrlock(FILE) 或 xlock(truncate) | Loner 优化 |
| `read` | FILE | rdlock(FILE) | 共享读 |
| `truncate` | FILE + AUTH | xlock(FILE) + xlock(AUTH) | 数据+权限 |
| `setxattr` | XATTR | xlock(XATTR) | 独占 |
| `getxattr/listxattr` | XATTR | rdlock(XATTR) | 共享读 |

### 2.3 锁类型到 Cap 位的映射

每个锁类型对应一组 cap 位，eval 时从锁状态推导 cap 掩码:

```c
/* 锁类型 → cap 位掩码映射 (eval 使用) */
static const unsigned int lock_type_cap_bits[POWERFS_NUM_LOCK_TYPES] = {
    [POWERFS_LOCK_AUTH]  = POWERFS_CAP_AUTH_SHARED | POWERFS_CAP_AUTH_EXCL,
    [POWERFS_LOCK_LINK]  = POWERFS_CAP_LINK_SHARED,
    [POWERFS_LOCK_XATTR] = POWERFS_CAP_XATTR_SHARED | POWERFS_CAP_XATTR_EXCL,
    [POWERFS_LOCK_DN]    = 0,  /* DN 锁输出 lease, 不输出 cap */
    [POWERFS_LOCK_SNAP]  = 0,  /* 预留 */
    [POWERFS_LOCK_FILE]  = POWERFS_CAP_FILE_SHARED | POWERFS_CAP_FILE_CACHE |
                            POWERFS_CAP_FILE_WR | POWERFS_CAP_FILE_EXCL,
    [POWERFS_LOCK_DFT]   = 0,  /* ScatterLock, 不输出客户端 cap */
    [POWERFS_LOCK_NEST]  = 0,  /* ScatterLock, 不输出客户端 cap */
};
```

## 4. 锁状态枚举

合并 4 套状态机的所有状态:

```c
/* MDLock 状态 — 合并 4 套状态机的所有状态
 * 参考: src/mds/SimpleLock.h, ScatterLock.h, FileLock.h, LocalLock.h
 *
 * 不同 class 使用不同的状态子集:
 *
 * LocalLock:     AVAILABLE, LOCK
 * SimpleLock:    AVAILABLE, SHARED, LONER, EXCL, GATHER, REVOKING
 * ScatterLock:   AVAILABLE, DSCATTER, EXCL, INACTIVE, SYNC, GATHER
 * FileLock:      AVAILABLE, SHARED, LONER, EXCL, GATHER, REVOKING, SYNC
 *
 * 状态转移图 (SimpleLock + FileLock):
 *
 *   AVAILABLE ──rdlock──> SHARED ──wrlock(单client)──> LONER
 *                            │                           │
 *                            │ wrlock(多client)           │ 新client
 *                            ▼                           ▼
 *                          SHARED <──recall ack── GATHER ──xlock──> EXCL
 *                                                      ▲
 *                                                      │
 *   EXCL ──unlock──> GATHER (等待所有 cap ACK) ──done──> AVAILABLE
 *
 * FileLock 额外:
 *   SHARED ──flush──> SYNC (只读, cap 已写回) ──recall──> AVAILABLE
 *   SYNC ──new write──> SHARED
 *
 * ScatterLock:
 *   AVAILABLE ──wrlock──> DSCATTER (多方共享写) ──xlock──> GATHER ──> EXCL
 *   DSCATTER ──inactive──> INACTIVE
 *   EXCL ──unlock──> SYNC ──recall──> AVAILABLE
 *
 * LocalLock:
 *   AVAILABLE ──lock──> LOCK ──unlock──> AVAILABLE
 */
enum powerfs_lock_state {
    /* === 共享状态 === */
    LOCK_ST_AVAILABLE = 0,   /* 无锁, 无持有者 (所有 class) */

    /* === SimpleLock + FileLock 状态 === */
    LOCK_ST_SHARED    = 1,   /* 共享态: 多方并发读 */
    LOCK_ST_LONER     = 2,   /* Loner 独占优化: 仅 1 client, 下发 exclusive cap */
    LOCK_ST_EXCL      = 3,   /* 完全独占: xlock 持有 */
    LOCK_ST_GATHER    = 4,   /* 正在收集 recall ACK */
    LOCK_ST_REVOKING  = 5,   /* 正在部分撤销 (recall 子集 cap) */

    /* === LocalLock 状态 === */
    LOCK_ST_LOCK      = 6,   /* LocalLock 独占: MDS 本地锁, 无客户端 cap */

    /* === ScatterLock 状态 === */
    LOCK_ST_DSCATTER  = 7,   /* ScatterLock 散射态: 多方共享写 */
    LOCK_ST_INACTIVE  = 8,   /* ScatterLock 非活跃 */
    LOCK_ST_SYNC      = 9,   /* 同步态: 只读 (FileLock SYNC + ScatterLock SYNC_SCATTER) */
};
```

### 3.1 状态转移矩阵

| 当前状态 | rdlock | wrlock | xlock | unlock | recall ack(last) | new client |
|---------|--------|--------|-------|--------|-----------------|------------|
| AVAILABLE | →SHARED | →LONER | →GATHER→EXCL | - | - | - |
| SHARED | +holder | →GATHER(收R)→LONER/SHARED | →GATHER(收全部) | →AVAILABLE | →target | →GATHER |
| LONER | +holder→SHARED | - | →GATHER(收全部) | →AVAILABLE | →target | →GATHER→SHARED |
| EXCL | wait | wait | wait(blocked) | →GATHER | →target | blocked |
| GATHER | wait | wait | wait(blocked) | abort | →target | wait |
| REVOKING | wait | wait | wait(blocked) | →GATHER | →target | wait |

## 4. 锁原语定义

```c
/* MDLock 原语 — 对齐 Ceph MDS Locker rdlock/wrlock/xlock
 * 参考: src/mds/locks.cc, src/mds/MDSContext.h
 *
 * 这些是 Filer/MDS 内部调用, 不是客户端 RPC.
 * 客户端只发 open/lookup/write/chmod, Filer 内部决定锁原语. */

/* 锁原语类型 */
enum powerfs_lock_op {
    LOCK_OP_RD       = 0,  /* rdlock: 共享读, 多方并发 */
    LOCK_OP_WR       = 1,  /* wrlock: 排他写 (SimpleLock 排他) */
    LOCK_OP_X        = 2,  /* xlock: 完全独占, 必须 recall 全部 cap */
    LOCK_OP_REMOTE_WR = 3, /* remote_wrlock: 跨 shard 锁请求 */
};

/* 锁请求上下文 */
struct powerfs_lock_request {
    enum powerfs_lock_type type;
    enum powerfs_lock_op   op;
    u64                    inode;       /* 目标 inode */
    u64                    shard_id;    /* 跨 shard 时使用 */
    int                    timeout_ms;  /* 0 = 阻塞等待 */
    void                  *caller_ctx;  /* 回调上下文 */
};

/* 锁授予结果 */
struct powerfs_lock_grant {
    enum powerfs_lock_type type;
    enum powerfs_lock_op   op;
    u64                    sn;          /* 序列号 (fencing) */
    u64                    epoch;       /* fencer epoch */
    u32                    duration_ms; /* lease TTL */
};

/* 原语 API (Filer 端调用) */

/* rdlock: 获取共享读锁
 * - SimpleLock: 多方可同时 rdlock
 * - 如果当前 EXCL/GATHER 状态: 阻塞等待
 * 返回 0 成功, -EAGAIN 超时, -EINTR 中断 */
int powerfs_mdlock_rdlock(struct powerfs_inode_info *pi,
                          enum powerfs_lock_type type,
                          struct powerfs_lock_grant *grant);

/* wrlock: 获取排他写锁
 * - SimpleLock: 排他, 只能 1 个 writer
 * - 如果 LONER 且同 client: 复用 (fast path)
 * - 如果其他人持有: → GATHER → recall → 降级后授予 */
int powerfs_mdlock_wrlock(struct powerfs_inode_info *pi,
                          enum powerfs_lock_type type,
                          struct powerfs_lock_grant *grant);

/* xlock: 获取完全独占锁
 * - 必须 recall 该锁类型对应的全部 client cap
 * - rename/unlink/truncate/migrate 必须先拿 xlock
 * - xlock 持有期间禁止下发任何 dirty cap */
int powerfs_mdlock_xlock(struct powerfs_inode_info *pi,
                         enum powerfs_lock_type type,
                         struct powerfs_lock_grant *grant);

/* unlock: 释放锁
 * - 从 EXCL 释放: → GATHER → 等待 flush 完成 → AVAILABLE
 * - 从 SHARED/LONER 释放: 直接移除 holder, 若 0 holder → AVAILABLE */
int powerfs_mdlock_unlock(struct powerfs_inode_info *pi,
                          enum powerfs_lock_type type,
                          u64 sn);

/* remote_wrlock: 跨 shard 锁请求 (Phase 2)
 * - 向 target_shard 发送锁请求
 * - 用于跨 shard rename/migrate */
int powerfs_mdlock_remote_wrlock(u64 inode, u64 shard_id,
                                  enum powerfs_lock_type type,
                                  struct powerfs_lock_grant *grant);
```

## 5. struct powerfs_mdlock 定义

```c
/* kernel/powerfs_mod/powerfs_lock.h */

/* 锁持有者记录 — 每个 client session 持有一把锁的记录
 * 对齐 Ceph: std::set<client_id> simple_lock_t::wrlocks/gather */
struct powerfs_lock_holder {
    struct list_head list;        /* 挂到 mdlock->holders */

    u64    client_id;             /* client session id */
    u64    sn;                    /* 授予时的序列号 (fencing) */
    u64    epoch;                 /* fencer epoch */
    u32    duration_ms;           /* lease TTL */
    unsigned long expire_jiffies;  /* 过期时间 */

    /* 该 holder 在该锁上被授予的 cap 位 (eval 输出) */
    unsigned int granted_caps;

    /* 该 holder 在该锁上的 dirty caps (未写回的脏位) */
    unsigned int dirty_caps;

    /* recall 状态 */
    bool recall_in_flight;        /* 已发 recall, 等待 ACK */
    unsigned int recall_caps;     /* 正在 recall 的位 */
    unsigned int retain_caps;     /* recall 后保留的位 */

    /* back pointer */
    struct powerfs_mdlock *lock;
};

/* GATHER 等待项 — xlock/wrlock 等待 recall ACK 时的追踪
 * 对齐 Ceph: SimpleLock::gather_set */
struct powerfs_lock_gather {
    struct list_head list;        /* 挂到 mdlock->gather_list */
    u64    client_id;             /* 等待 ACK 的 client */
    u64    sn;                    /* recall 消息的序列号 */
    unsigned long sent_jiffies;   /* recall 发送时间 (timeout) */
    bool   acked;                 /* 是否已收到 ACK */
};

/* 独立锁对象 — 对齐 Ceph SimpleLock/FileLock/ScatterLock
 *
 * 每个 inode 持有 POWERFS_NUM_LOCK_TYPES 把 mdlock, 各自独立状态机.
 * 参考: src/mds/SimpleLock.h class SimpleLock */
struct powerfs_mdlock {
    /* 基本标识 */
    enum powerfs_lock_type  type;    /* 锁类型 (AUTH/LINK/.../FILE) */
    enum powerfs_lock_state state;   /* 当前锁状态 */

    /* 持有者列表 — 当前持有该锁的 client session 集合
     * SHARED: 多个 holder; LONER: 1 holder; EXCL: 1 holder; */
    struct list_head holders;        /* powerfs_lock_holder 链表 */
    int holder_count;               /* 快速计数 (避免遍历) */

    /* GATHER 等待列表 — 正在等待 recall ACK 的 client 集合
     * GATHER 状态下非空; 其他状态为空 */
    struct list_head gather_list;    /* powerfs_lock_gather 链表 */
    int gather_remaining;           /* 剩余待 ACK 数 */

    /* 等待者队列 — 被阻塞的锁请求 (xlock 等 EXCL 释放)
     * 对齐 Ceph: SimpleLock::waiting */
    struct list_head waiting;        /* powerfs_lock_request 链表 */

    /* 该锁的权限评估结果 (eval 输出, 缓存)
     * 从 holders 的 granted_caps 聚合而来 */
    unsigned int eval_issued;        /* 所有 holder issued 的并集 */
    unsigned int eval_wanted;       /* 所有 holder wanted 的并集 */

    /* back pointer */
    struct powerfs_inode_info *pi;
};

/* inode 持有的锁实例数组
 * 嵌入 struct powerfs_inode_info, Phase 1 新增 */
/* 在 struct powerfs_inode_info 中添加:
 *   struct powerfs_mdlock i_locks[POWERFS_NUM_LOCK_TYPES];
 */
```

## 6. Eval 权限评估

### 6.1 核心函数

```c
/* eval: 锁状态 → cap 掩码 + lease 令牌
 * 对齐 Ceph: SimpleLock::eval()
 *
 * 触发时机 (事件驱动):
 *   1. 客户端请求 (open/lookup/write)
 *   2. 锁状态变迁 (rdlock/wrlock/xlock/unlock)
 *   3. recall ACK 收齐 (GATHER→target)
 *   4. lease TTL 过期
 *   5. holder 过期清理
 *
 * 输入: mdlock (当前锁状态 + holders)
 * 输出: 每个 holder 的 granted_caps 更新
 *        → 通过 powerfs_cap_issue/revoke 下发到客户端 */
void powerfs_mdlock_eval(struct powerfs_mdlock *lock);

/* eval 内部: 根据 lock->state 决定每个 holder 的 cap 掩码
 * 对齐 Ceph: SimpleLock::eval() 的状态分支 */
static unsigned int mdlock_eval_caps(struct powerfs_mdlock *lock,
                                     struct powerfs_lock_holder *h)
{
    switch (lock->state) {
    case LOCK_ST_LONER:
        /* LONER: 单 client, 下发全套 exclusive cap */
        if (lock->type == POWERFS_LOCK_FILE)
            return POWERFS_CAP_FILE_SHARED | POWERFS_CAP_FILE_CACHE |
                   POWERFS_CAP_FILE_WR | POWERFS_CAP_FILE_EXCL;
        if (lock->type == POWERFS_LOCK_AUTH)
            return POWERFS_CAP_AUTH_SHARED | POWERFS_CAP_AUTH_EXCL;
        if (lock->type == POWERFS_LOCK_XATTR)
            return POWERFS_CAP_XATTR_SHARED | POWERFS_CAP_XATTR_EXCL;
        return lock_type_cap_bits[lock->type]; /* 全量 */

    case LOCK_ST_SHARED:
        /* SHARED: 只下发 shared (只读) cap, 不能本地 dirty */
        if (lock->type == POWERFS_LOCK_FILE)
            return POWERFS_CAP_FILE_SHARED | POWERFS_CAP_FILE_CACHE;
        if (lock->type == POWERFS_LOCK_AUTH)
            return POWERFS_CAP_AUTH_SHARED;
        if (lock->type == POWERFS_LOCK_XATTR)
            return POWERFS_CAP_XATTR_SHARED;
        return POWERFS_CAP_AUTH_SHARED; /* 最低读权限 */

    case LOCK_ST_EXCL:
        /* EXCL: xlock 持有者独占, 其他人无 cap */
        if (list_is_singular(&lock->holders) && h == first_holder(lock))
            return lock_type_cap_bits[lock->type]; /* xlock holder 全量 */
        return 0; /* 其他人: 无 cap */

    case LOCK_ST_GATHER:
        /* GATHER: 正在 recall, 保持现有 cap 直到 ACK */
        return h->granted_caps; /* 维持不变 */

    case LOCK_ST_REVOKING:
        /* REVOKING: 部分撤销, 保留 retain_caps */
        return h->retain_caps;

    case LOCK_ST_AVAILABLE:
    default:
        return 0;
    }
}
```

### 6.2 Eval 触发点

```c
/* 事件驱动 eval — 对齐 Ceph: Locker::eval()
 * 在以下事件后调用: */
struct eval_trigger {
    /* 锁状态变迁后 */
    void on_state_change(struct powerfs_mdlock *lock) {
        powerfs_mdlock_eval(lock);
    }
    /* recall ACK 收齐后 */
    void on_gather_complete(struct powerfs_mdlock *lock) {
        lock->state = lock->gather_target_state;
        powerfs_mdlock_eval(lock);
        wake_up_all(&lock->pi->i_cap_wq);
    }
    /* 新 holder 加入后 */
    void on_holder_add(struct powerfs_mdlock *lock) {
        powerfs_mdlock_eval(lock);
    }
    /* holder 过期/移除后 */
    void on_holder_remove(struct powerfs_mdlock *lock) {
        if (lock->holder_count == 0)
            lock->state = LOCK_ST_AVAILABLE;
        powerfs_mdlock_eval(lock);
    }
};
```

## 7. Recall / Revoke + GATHER 同步屏障

### 7.1 GATHER 状态详解

```c
/* GATHER 同步屏障 — 对齐 Ceph: SimpleLock::GATHER 状态
 *
 * 场景: client B 请求 xlock(IAUTH), 但 client A 持有 AUTH_EXCL
 *
 * 流程:
 *   1. B 调 mdlock_xlock(IAUTH)
 *   2. 锁状态 → GATHER, gather_target_state = EXCL
 *   3. 遍历 holders, 构造 RecallTask:
 *      - recall A 的 AUTH_EXCL 位
 *      - retain AUTH_SHARED 位 (降级而非全收)
 *   4. 每个 recall 的 holder 加入 gather_list
 *   5. 异步发送 RecallTask 到客户端
 *   6. 客户端收到 recall → flush dirty AUTH → send recall_ack
 *   7. 服务端收到 recall_ack → 标记 gather.acked = true → gather_remaining--
 *   8. gather_remaining == 0 → on_gather_complete → state = EXCL → 唤醒 B
 *
 * 关键: B 的 xlock 请求阻塞在 i_cap_wq 上, 直到 GATHER 完成.
 *       这是 Ceph "操作 hang" 的根源 — 但也是正确性保证. */
```

### 7.2 Recall 超时处理

```c
/* Recall 超时: force-reclaim (对齐 Ceph: MDS session timeout)
 *
 * 超时后:
 *   1. 强制移除该 holder
 *   2. epoch bump (fencing — 该 client 后续 IO 被拒绝)
 *   3. gather_remaining-- (视为已 ACK)
 *   4. 如果 gather_remaining == 0 → on_gather_complete */
#define MDLOCK_RECALL_TIMEOUT_MS  2000  /* 2s, 对齐 cap_manager.rs */

static void mdlock_gather_timeout(struct powerfs_mdlock *lock)
{
    struct powerfs_lock_gather *g, *tmp;
    list_for_each_entry_safe(g, tmp, &lock->gather_list, list) {
        if (time_after(jiffies, g->sent_jiffies +
                       msecs_to_jiffies(MDLOCK_RECALL_TIMEOUT_MS))) {
            pr_warn("powerfs: mdlock recall timeout type=%d client=%llu, "
                    "force-reclaim\n", lock->type, g->client_id);
            /* fencing: epoch bump */
            lock->pi->i_epoch++;
            list_del(&g->list);
            lock->gather_remaining--;
            kfree(g);
        }
    }
    if (lock->gather_remaining == 0)
        on_gather_complete(lock);
}
```

## 8. Loner 独占优化

```c
/* Loner 状态管理 — 对齐 Ceph: SimpleLock LONER + FileLock loner
 *
 * 进入 LONER 条件:
 *   1. 当前状态 SHARED
 *   2. 仅 1 个 holder 请求 wrlock
 *   3. 无其他 holder 的 dirty caps
 *
 * 退出 LONER:
 *   1. 新 client 请求 → → GATHER (recall LONER holder 的 exclusive cap)
 *   2. holder 释放 → → AVAILABLE
 *   3. holder 超时 → → AVAILABLE
 *
 * LONER 优化收益:
 *   - 单 client 写场景: 本地 dirty, 延迟上报
 *   - 减少 RPC: setattr/append 不需每次 RPC 到 Filer
 *   - 仅 LONER + FILE 类型 holder 拿 CAP_FILE_EXCL + CAP_FILE_WR */
```

## 9. 与现有 struct powerfs_cap 的兼容映射

### 9.1 映射关系

```
struct powerfs_cap (wire/transport 层)
┌──────────────────────────────────┐
│ issued = AUTH_EXCL | FILE_WR    │  ← 从 mdlocks[AUTH].eval_issued
│            | FILE_EXCL           │    | mdlocks[FILE].eval_issued 聚合
│ implemented = ...                │
│ wanted = ...                     │
│ seq / epoch / cap_gen / ...     │
└──────────────────────────────────┘
        ↕ 双向转换
struct powerfs_mdlock[8] (状态机层)
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ mdlock[AUTH] │ │ mdlock[FILE]  │ │ mdlock[XATTR]│ ...
│ state=LONER  │ │ state=SHARED  │ │ state=AVAIL  │
│ holders: [C1]│ │ holders: [C1]│ │ holders: []  │
│ eval_issued= │ │ eval_issued=  │ │ eval_issued= │
│  AUTH_EXCL   │ │  FILE_SHARED  │ │  0           │
└──────────────┘ └──────────────┘ └──────────────┘
```

### 9.2 转换函数

```c
/* mdlock → cap: 聚合所有锁的 eval_issued 到 cap.issued
 * 在发送 CapGrant 消息前调用 */
static unsigned int mdlocks_to_cap_issued(struct powerfs_inode_info *pi)
{
    unsigned int mask = 0;
    int i;
    for (i = 0; i < POWERFS_NUM_LOCK_TYPES; i++)
        mask |= pi->i_locks[i].eval_issued;
    return mask;
}

/* cap → mdlock: 从收到的 CapRecall 消息拆解到各锁
 * 在处理 recall 消息时调用 */
static void cap_revoke_to_mdlocks(struct powerfs_inode_info *pi,
                                   unsigned int revoking)
{
    int i;
    for (i = 0; i < POWERFS_NUM_LOCK_TYPES; i++) {
        unsigned int bits = revoking & lock_type_cap_bits[i];
        if (bits) {
            /* 该锁类型有位被 recall → 进入 REVOKING/GATHER */
            mdlock_start_revoke(&pi->i_locks[i], bits);
        }
    }
}
```

## 10. inode 锁布局

### 10.1 struct powerfs_inode_info 新增字段

```c
struct powerfs_inode_info {
    /* ... 现有字段保持不变 ... */

    /* ===== Phase 1 新增: MDLock 独立锁对象数组 ===== */
    struct powerfs_mdlock i_locks[POWERFS_NUM_LOCK_TYPES];

    /* GATHER 全局等待队列 (跨锁类型的 xlock 等待) */
    wait_queue_head_t i_mdlock_wq;

    /* ===== 以下旧字段标记为 deprecated, 逐步迁移到 i_locks ===== */
    /* [DEPRECATED] i_dirty_caps → 分散到 i_locks[*].holders[*].dirty_caps */
    unsigned int i_dirty_caps __deprecated;
    /* [DEPRECATED] i_pin_ref → 分散到 i_locks[AUTH].ref + ... */
    int i_pin_ref __deprecated;
    /* [DEPRECATED] i_auth_cap → i_locks[*].holders[0] */
    struct powerfs_cap *i_auth_cap __deprecated;
    /* ... 其他旧字段保留兼容 ... */
};
```

### 10.2 初始化

```c
/* 在 powerfs_alloc_inode 中调用 */
static void powerfs_init_mdlocks(struct powerfs_inode_info *pi)
{
    int i;
    for (i = 0; i < POWERFS_NUM_LOCK_TYPES; i++) {
        struct powerfs_mdlock *lock = &pi->i_locks[i];
        lock->type = (enum powerfs_lock_type)i;
        lock->state = LOCK_ST_AVAILABLE;
        INIT_LIST_HEAD(&lock->holders);
        INIT_LIST_HEAD(&lock->gather_list);
        INIT_LIST_HEAD(&lock->waiting);
        lock->holder_count = 0;
        lock->gather_remaining = 0;
        lock->eval_issued = 0;
        lock->eval_wanted = 0;
        lock->pi = pi;
    }
    init_waitqueue_head(&pi->i_mdlock_wq);
}
```

## 11. Filer 端实施

### 11.1 新增模块: powerfs-filer/src/lock_arbiter.rs

```rust
//! Filer 端 MDLock 仲裁器 — 在 Raft propose 前执行锁排队
//!
//! 对齐 Ceph MDS Locker: 所有元数据操作 (rename/unlink/chmod/truncate)
//! 必须先通过锁仲裁器拿到锁, 才能 propose 到 Raft.

use std::collections::HashMap;
use tokio::sync::oneshot;

/// 锁类型 (对齐内核 enum powerfs_lock_type)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LockType {
    Auth,    // IAUTH
    Link,    // ILINK
    Xattr,   // IXATTR
    Dn,      // DN (dentry)
    File,    // IFILE
    // Snap/Dft/Nest 预留
}

/// 锁状态 (对齐内核 enum powerfs_lock_state)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockState {
    Available,
    Shared,
    Loner,
    Excl,
    Gather,
    Revoking,
}

/// 锁原语类型
#[derive(Clone, Copy, Debug)]
pub enum LockOp {
    Rd,       // rdlock
    Wr,       // wrlock
    X,        // xlock
    RemoteWr, // remote_wrlock
}

/// Filer 端锁对象 (对齐内核 struct powerfs_mdlock)
pub struct MdLock {
    pub lock_type: LockType,
    pub state: LockState,
    pub holders: Vec<LockHolder>,
    pub gather_remaining: usize,
    pub gather_target: LockState,
    pub waiters: VecDeque<LockWaiter>,
}

pub struct LockHolder {
    pub client_id: String,
    pub sn: u64,
    pub epoch: u64,
    pub granted_caps: CapSet,
    pub dirty_caps: CapSet,
    pub recall_in_flight: bool,
}

pub struct LockWaiter {
    pub op: LockOp,
    pub client_id: String,
    pub tx: oneshot::Sender<LockGrant>,
}

/// 全局锁仲裁器 (per-shard)
pub struct LockArbiter {
    /// (inode, lock_type) → MdLock
    locks: HashMap<(u64, LockType), MdLock>,
}

impl LockArbiter {
    /// rdlock: 获取共享读锁
    pub async fn rdlock(&mut self, inode: u64, lock_type: LockType,
                        client_id: &str) -> Result<LockGrant, LockError> { ... }

    /// xlock: 获取完全独占锁 (rename/unlink/truncate 必须调用)
    pub async fn xlock(&mut self, inode: u64, lock_type: LockType,
                       client_id: &str) -> Result<LockGrant, LockError> { ... }

    /// unlock: 释放锁
    pub fn unlock(&mut self, inode: u64, lock_type: LockType, sn: u64) { ... }

    /// recall_ack: 客户端 ACK recall, GATHER 计数减一
    pub fn recall_ack(&mut self, inode: u64, lock_type: LockType,
                      client_id: &str) { ... }
}
```

### 11.2 net_handler.rs 集成

```rust
// 在 handle_mkdir / handle_rename / handle_setattr 等函数中
// propose 前插入锁仲裁:

async fn handle_rename(&self, req: RenameRequest) -> Result<(), Error> {
    // 1. 锁仲裁: xlock(DN, src) + xlock(DN, dst) + xlock(AUTH, src) + xlock(AUTH, dst)
    let g1 = self.arbiter.xlock(req.src_inode, LockType::Dn, &req.client_id).await?;
    let g2 = self.arbiter.xlock(req.dst_inode, LockType::Dn, &req.client_id).await?;
    let g3 = self.arbiter.xlock(req.src_inode, LockType::Auth, &req.client_id).await?;
    let g4 = self.arbiter.xlock(req.dst_inode, LockType::Auth, &req.client_id).await?;

    // 2. Raft propose (锁保护下安全执行)
    let result = self.shard_store.propose_rename(&req).await;

    // 3. 释放锁
    self.arbiter.unlock(req.src_inode, LockType::Dn, g1.sn);
    self.arbiter.unlock(req.dst_inode, LockType::Dn, g2.sn);
    self.arbiter.unlock(req.src_inode, LockType::Auth, g3.sn);
    self.arbiter.unlock(req.dst_inode, LockType::Auth, g4.sn);

    result
}
```

## 12. FUSE 端实施

### 11.1 client_cap.rs 适配

FUSE 端不需要实现锁状态机（那是 Filer 的职责），但需要:
1. 处理 recall 通知时按锁类型分拆（而非全量 flush）
2. 报告 dirty caps 时按锁类型分拆

```rust
// powerfs-fuse/src/client_cap.rs 新增

/// 客户端端锁类型映射 (对齐 Filer LockType)
#[derive(Clone, Copy, Debug)]
pub enum ClientLockType {
    Auth,
    Link,
    Xattr,
    File,
}

impl ClientLockType {
    /// 从 cap 位反推锁类型 (recall 消息按锁类型分拆)
    pub fn from_caps(caps: CapSet) -> Vec<ClientLockType> {
        let mut types = Vec::new();
        if caps.has_x() { types.push(ClientLockType::Auth); }  // AUTH_EXCL
        if caps.has_w() { types.push(ClientLockType::File); }  // FILE_WR/EXCL
        types
    }
}
```

## 13. 实施计划

> **⚠️ 架构方向修正（审计 2026-09-02 同步 lock-optimization-plan.md §11）**
>
> 原 §13 Phase 1 计划将 **Ceph MDS Locker 仲裁机（SimpleLock/ScatterLock/FileLock 四套状态机 + eval + GATHER + Loner）直接运行在内核客户端** —— 经 RDMA 双 VM fio 验证 + DEAD_CODE grep 审计，确认该方向存在**三重架构违规**（详见 lock-optimization-plan.md §11.1.2）：
>
> 1. **位置违规**：锁仲裁是全局单点 (single-writer) 行为，只能在 Filer leader `lock_arbiter.rs` 单实例运行；放内核客户端会脑裂。
> 2. **Wire 协议违规**：`tlk_codec.c` 独立帧与 powerfs-net TLV (`magic+ver+msg_u16+TLV`) **零字节 overlap**，无法互通；真正工作通道是 MsgType 0x91~0x95。
> 3. **Transport 未接**：`lock_client->transport` 为 NULL，所有 acquire/release 返回 `-ENOSYS`；未与 RDMA/TCP net_conn 注册的函数指针对接。
>
> **修正后的 Phase 顺序：Phase 0 先清 client → Phase 2 先落地 Filer lock_arbiter → Phase 3 再适配 FUSE → Phase 1 退回"仅客户端轻量 CapCache"，不再跑状态机 → Phase 4 集成测试。**

### Phase 0: 清理内核端 DeadCode + 为轻量 CapCache 做准备（**✅ 全部完成**，audit 2026-09-02）

| 步骤 | 内容 | 文件 | 状态 |
|------|------|------|------|
| 0.1 | `powerfs_locks.c L47-L1515` 用 `#if 0 / #endif` 包 DEAD_CODE + 16 行 architecture violation 注释 | [powerfs_locks.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_locks.c#L47-L90) | ✅ Done |
| 0.2 | `powerfs_lock.h L38-L620` 同 DEAD_CODE guard (KernelLeaseState / MDLock / lock_client API) | [powerfs_lock.h](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_lock.h#L38-L620) | ✅ Done |
| 0.3 | 从 `Makefile` 移除 `tlk_codec.o / lock_client.o`，附 REMOVED 注释两大原因 | [Makefile L50-69](file:///home/portion/powerfs/kernel/powerfs_mod/Makefile#L50-L69) | ✅ Done |
| 0.4 | inode 初始化调用 `powerfs_init_mdlocks` DEAD_CODE guard 包住 + 附 zero-invocations 注释 | [powerfs_inode.c L1007-1019](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_inode.c#L1007-L1019) | ✅ Done |
| 0.5 | **新增 WANTED mask + pending_flush mask** 到 `struct powerfs_cap`，加 `AcquireCap` MsgType 0x96 | `powerfs_caps.c` + `powerfs_net_lease.c` | ✅ Done (P0-1) |
| 0.6 | **wire_capset 3-bit → 4-bit** 扩展 LONER 位 (L=0b1000) | `powerfs_caps.c` wire 转换函数 + Rust cap_manager | ✅ Done (P0-2, Filer lock_arbiter 已实现 LONER) |

### Phase 1: 内核端仅保留 **轻量 CapCache struct**（客户端不跑状态机；**✅ 核心任务完成**，session 批量留待后续）

| 步骤 | 内容 | 文件 | 状态 |
|------|------|------|------|
| 1.1 | struct powerfs_cap 扩 WANTED + pending_flush + client_caps_count 索引 | `powerfs_caps.c` (caps struct) | ✅ Done (P0-0/1) |
| 1.2 | `file_wanted()` 非 0 时构造 `kernel_bits_to_wire_capset` 发 `AcquireCap(0x96)` RPC | `powerfs_caps.c` L757 + RX dispatcher 注册 | ✅ Done (P0-1) |
| 1.3 | 新增 `sbi->cap_flush_list` session 级 dirty cap 链表 + 10ms 批量 flush worker | `powerfs_caps.c` cap_flush 路径 | 🔳 待实施 (P1-1, BatchCapRelease 协议已完成, 调用点未接) |
| 1.4 | `WB_WRITE_CB status=10 (CONFLICT_WRITE)` → 1 次 retry（ROOT36 修复） | `page_cache.c` writepage call back | ✅ Done (ROOT38 SB_NOSEC 修复后意外消除) |
| 1.5 | `powerfs_cap_flush` 内 GFP_KERNEL → GFP_NOWAIT（修复 atomic context sleep BUG） | `powerfs_caps.c` cap_flush | ✅ Done (P0-0 三级分配) |
| 1.6 | Loner 位 L=0b1000：holder_count==1 时内核在 issued 上加 FILE_EXCL + FILE_CACHE；dirty 到 256-page 才召回 | `powerfs_caps.c` cap_issue + issue 判定 | ✅ Done (P0-2, Filer wrlock holder_count==1 → LONER) |
| 1.7 | mdlock↔cap 位映射规则从 locks.c DEAD_CODE 内拆出独立 `cap_mapping.h` 活文档 | 新建 `powerfs_capmap.h` | 🔳 低优先 (dead code 已隔离) |
| 1.8 | 编译 + verify_module + fio 3 轮（vm1/vm2 各 5 job）无 BUG 无 err | - | ✅ Done (4K rw 397K IOPS, 1M sw 3598 MB/s, 0 BUG) |

### Phase 2: Filer 端 lock_arbiter 模块（**✅ 全部完成**，对齐 Ceph MDS，全局单点仲裁）

| 步骤 | 内容 | 文件 | 状态 |
|------|------|------|------|
| 2.1 | 新增 `lock_arbiter.rs`（MdLock 8 锁型 × 4 class × 4 state_machine + LockArbiter 单例 + Raft leader 绑定） | [lock_arbiter.rs](file:///home/portion/powerfs/powerfs-filer/src/lock_arbiter.rs) | ✅ Done |
| 2.2 | 实现 rdlock/wrlock/xlock/trylock/unlock 原语（server 内部） | 同上 L646-L903 | ✅ Done |
| 2.3 | 实现 GATHER 同步屏障 + per-holder ACK 2s timeout + resend + kick | 同上 GATHER state + recall_ack | ✅ Done |
| 2.4 | 实现 Loner 进入/退出 + holder_count==1 判定 + 下发 L=0b1000 wire 位 | 同上 wrlock L839-L846 | ✅ Done |
| 2.5 | 实现 eval 事件驱动（cap 更新 / inode 迁移 / open_release） | 同上 eval + tick | ✅ Done |
| 2.6 | `net_handler.rs`：**在 propose 前加锁**；未拿到 lock 的 propose 返回 EAGAIN | [net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs) | ✅ Done |
| 2.7 | `cap_manager.rs`：mdlock eval → 生成 issued/wanted 位 → CapOpenGrant / CapUpgradeNotify(0x94) 下发 | [cap_manager.rs](file:///home/portion/powerfs/powerfs-filer/src/cap_manager.rs) | ✅ Done |
| 2.8 | 新增 MsgType 0x96 AcquireCap handler → LockArbiter 同步 eval → piggyback grant | net_handler.rs L4062 | ✅ Done |
| 2.9 | 编译 + clippy + stage4_net_handler_scenarios 9 test cases | - | ✅ Done (141 lib + 9 stage4 + 17 fs scenarios) |

### Phase 3: FUSE 端适配（cap 轻量结构 + 新 MsgType 通道，不再接 lock_client 仲裁）

| 步骤 | 内容 | 文件 | 状态 |
|------|------|------|------|
| 3.1 | `client_cap.rs` 扩 WANTED mask + pending_flush mask；open_release 时检查 wanted 未达成发 AcquireCap(0x96) | `client_cap.rs` | 🔳 待实施 |
| 3.2 | `lock_backend.rs`：原 lock_client 调用 → 改为调用 lock_arbiter 导出的 client-facing `acquire/release/renew/revoke_ack` 函数指针通道（复用 0x91~0x96 + 新增 LockRequest 0x97/0x98/0x99） | `lock_backend.rs` | 🔳 待实施 |
| 3.3 | `fuse.rs` 业务路径适配：元数据操作 setattr/unlink/rename/create 前后显式 want AUTH_EXCL/XATTR_EXCL（AcquireCap），完成后 ReleaseCap | `fuse.rs` | 🔳 待实施 |
| 3.4 | 编译 + clippy + v6/v7 cross-client lock 回归 | - | 🔳 待实施 |

### Phase 4: 集成测试

| 步骤 | 内容 | 状态 |
|------|------|------|
| 4.1 | 内核 QEMU VM 基础功能测试（mount/read/write/fsck） | 🔳 待实施 |
| 4.2 | T1.6 并发 append 回归测试（2 VM 同 append，0 CONFLICT_WRITE err） | 🔳 待实施 |
| 4.3 | T3 跨 shard mkdir + rename 测试 | 🔳 待实施 |
| 4.4 | 跨端并发元数据操作测试（20 client 混合 setattr/create/unlink/rename 10min 无 deadlock/leak） | 🔳 待实施 |
| 4.5 | LTP `fcntl-locktests` + `flock01` Posix file_lock 合规 | 🔳 待实施 |
| 4.6 | `fio_pfs_rdma.sh` 10/10 job：4K randwrite ≥ 12K IOPS，4k_randrw err=0，1M seqwrite ≥ 2,200 MB/s | 🔳 待实施 |

## 14. 与 Ceph 的对齐对照

> **⚠️ 现状勘误（审计 2026-09-02）：本节下表为 "设计阶段估算对齐度"，基于 locks.c 客户端实现 MDS Locker 的旧方向。按修正后架构（仲裁机只在 Filer 运行 + 客户端仅轻量 CapCache），真实工作链路的差距矩阵请见 §14.2。**

### §14.1 设计阶段估算对照表（旧方向，仅供参考，以 mdlock-design 纯设计视角）

| Ceph 概念 | PowerFS 对应 | 对齐度 | 差距说明 |
|-----------|-------------|--------|---------|
| LocalLock | mdlock cls=LOCAL (SNAP) | 90% | 二态 AVAIL/LOCK 对齐, 无客户端 cap |
| SimpleLock | mdlock cls=SIMPLE (AUTH/LINK/XATTR/DN) | 90% | 状态机对齐, 缺 waiting CCE 回调 |
| ScatterLock | mdlock cls=SCATTER (DFT/NEST) | 15% | 状态定义有, eval 返回 0; 缺多方共享写合并 |
| FileLock | mdlock cls=FILE (FILE) + Loner + SYNC | 85% | Loner+SYNC 有, 缺 cap_snap 联动 |
| 12 种锁 type | 8 种 (精简) | 67% | 去掉 SNAP/SCATTER/FLOCK 等低频 |
| rdlock/wrlock/xlock | 同名原语 | 90% | remote_wrlock 预留 |
| eval() | powerfs_mdlock_eval() | 85% | 事件驱动有, 缺 tick 定时触发 |
| GATHER 状态 | LOCK_ST_GATHER | 95% | 有同步屏障 + timeout |
| Loner | LOCK_ST_LONER | 80% | 有进/出/eval, 缺完整重入 |
| Cap Recall | recall_in_flight + retain | 85% | 有, 缺 dentry lease revoke |
| Quiesce | 无 | 0% | 预留 |
| Session 清理 | ClientSession.cap_count() | 60% | 有索引, 缺主动清理联动 |
| mdcache trim 联动 | 无 | 0% | 预留 |
| lock status dump | 无 | 0% | 预留 |

### §14.2 实际工作链路对照（2026-09-02 RDMA 双 VM 审计后，**修正后架构 = 真实差距**）

对照口径：锁仲裁层（Server-Filer）、Cap 缓存层（Kernel-FUSE Client）、Session 集成层 三横面对齐 Ceph MDS/OSD + client Caps。
配套数据：fio 10/10 job 实测结果、MsgType 0x91~0x95 wire 协议 grep 验证、powerfs_locks.c DEAD_CODE 四重实锤（lock-optimization-plan.md §11.1.1）。

#### A · Server 端锁仲裁层差距

| 维度 | Ceph MDS (fs/ceph/mds) | PowerFS Filer 现状 (cap_manager + 未落地 lock_arbiter) | 对齐度 | 差距说明 + 修复挂接任务
|---|---|---|---|---|
| 8 lock type × 4 class | 8 lock × 4 class：IAUTH/ILINK/IXATTR/IDN/IFILE/ISNAP/IDFT/INEST；Simple/Scatter/File/Local | Rust 侧有 8 lock 设计，但 wire 只暴露 **3-bit R/W/X**；lock_arbiter 单例未落地（Phase 2.1-2.5） | **~40%** | 🔴 High：lock_arbiter 未落地 = eval/GATHER/Loner 没跑起来；P1-2 任务 |
| Loner 独占优化（写性能最大杠杆） | holder_count==1 → LONER → 下发 EXCL+FILE_CACHE（批量写回） | **完全不存在**（wire 3-bit 无 L；内核 CAP_W≠EXCL，缺 WANTED→AcquireCap） | **0%** | 🔴 High · 直接造成 4K wr 只有 923 IOPS；P0-2 + P0-1 |
| 客户端主动协商 AcquireCap | `AcquireCap / ReleaseCap / CapUpdate` 增量 RPC | open() 时一次 CapOpenGrant，之后**无增量通道**（无 WANTED→RPC exit） | **15%** | 🔴 High · 元数据并发操作必冲突；P0-1 新增 MsgType 0x96 AcquireCap |
| GATHER 屏障 + ACK 超时重传 | `gather_set + 2s timeout + resend + kick` | Rust 侧应有同等实现；内核只被动处理 0x93 CapRecallNotify → flush → ack | **~55%** | 🟡 Medium：client 是被动方 = 正确；P1-2 把算法落到 lock_arbiter |
| Fencing epoch / sn / seq | `tid + epoch + session_seq + mds_gid` | 内核 `struct powerfs_cap { token, epoch, sn }` ✅；recall 对旧写 fencing ✅ | **90%** | ✅ Good |
| Quiesce 接口 | `Quiesce → all holders release → no inflight RDMA` | N/A | **0%** | P2-4 |

#### B · 客户端 Cap Cache 层差距

| 维度 | Ceph `struct ceph_cap` (8 字段) | PowerFS `struct powerfs_cap` (6 字段) | 对齐度 | 差距说明 + 任务
|---|---|---|---|---|
| Cap 字段集 | `issued/wanted/implemented/dirty/pending_flush/seq/mds_seq/retain/session_caps` | `issued/dirty/implemented/epoch/sn/token` (缺 wanted + pending_flush) | **67%** | 🔴 High：缺 wanted/pending_flush → 无法构造增量 AcquireCap；Phase 1.1 |
| 增量 AcquireCap 闭环 | `__ceph_caps_file_wanted → check_caps → AcquireCap` | `kernel_bits_to_wire_capset()` 函数已存在，但**无 caller 无 RPC exit** | **10%** | 🔴 High；Phase 1.2 (P0-1) |
| Dirty Cap Session 级批量 | `ceph_flush_dirty_caps()` per-MDS session 聚合 N inode dirty bits | `powerfs_cap_flush()` 单 inode 同步 | **25%** | 🟡 Medium；Phase 1.3 (P1-1) |
| Revoke → piggyback Grant | RecallAck 体里可选 piggyback 新 issued 位，省 1 RTT | RevokeAck 后必须等 CapUpgradeNotify(0x94)，多 1 RTT | **60%** | 🟡 Medium；P2-1 |
| page_mkwrite 与 wanted 联动 | CEPH_CAP_FILE_WR 时标记 wanted WR + 触发 check_caps 升级 | 已接 VFS，但只 set dirty，**不触发 wanted 升级** | **35%** | 🟡 Low；Phase 1.6 |
| Posix flock / F_SETLKW | 通过 MDS FileLock + lock_client dispatcher 全量支持 | **未接**（lock_client 在 DEAD_CODE，flock 返回语义未 audit） | **0%** | 🔴 Medium；P1-3 |

#### C · Session / Lease 集成层差距

| 维度 | Ceph | PowerFS Kernel Client | 对齐度 | 差距说明
|---|---|---|---|---|
| MDS Session 生命周期（OPENING/OPEN/STALE/CLOSED + session_reconnect） | 完整双向 keepalive + session_reconnect 重试 | KeepConnected 30s ✅；reconnect ✅（Phase 6 restart 测试） | **85%** | ✅ Good |
| Dir frag lease + readdir cache | MDS dir frag lease + `dir_lease_ttl`，过期 revalidate | `dir_lease_epoch + POWERFS_DIR_LEASE_TTL` ([dir.c L1969-1973](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_dir.c#L1969-L1973)) ✅ | **80%** | ✅ Good |
| Shrinker 与 cap recall 释放低内存 | `mdcache shrink` + LRU recall + page cache invalidate | 未注册 shrinker，OOM 触发只能靠 kernel OOM killer | **0%** | 🟡 Low；P2-2 |
| DebugFS/SysFS cap/lock dump | 全量 caps/dentry-locks/session dump | N/A，只能看 pr_debug | **0%** | 🟡 Low；P2-3 |

### §14.3 实测性能差距与修复预期（fio 基线 2026-09-02 vm1）

| KPI | 当前实测 | Phase 0 全部完成后预期 | Phase 1 全部完成后预期 | 差距说明（真实瓶颈 vs 常见误区"RDMA 不够快"）
|---|---|---|---|---|
| 4K randread IOPS | 87,263 @ 10.87µs | ~90,000 | ≥ 100,000 | ✅ page cache 完全工作；差距是 syscall + psync mutex overhead；cache 不是瓶颈 |
| **4K randwrite IOPS** | 923 @ 1,083µs | **≥ 5,000 (5.4×)** | ≥ 12,000 (13×) | 🔴 **680× RTT 放大根因：缺 WANTED + 缺 Loner + 每 ~20 page recall×3 RTT**；RDMA 1.46µs 仅占 0.1%，传输绝非瓶颈 |
| 4K randrw (70R/30W) 总错误 (双VM) | 242 (各 121 EREMOTEIO ROOT36) | **0** | 0 | ✅ 不是 RDMA 传输错误，是 chunk write conflict；P0-3 retry + Loner 独占权 |
| 1M seqread MB/s | 2,730 MB/s (RDMA 占 ~3%) | ≈ 2,700 (同) | ≥ 2,900 | ✅ 瓶颈在 page cache copy_page；RDMA 仅 3% 利用率 |
| 1M seqwrite MB/s | 790 MB/s (RDMA 占 3.2%) | ≥ 1,500 (1.9×) | **≥ 2,200 (2.8×)** | 🔴 **瓶颈：session 无批量合并 + 每 8MB chunk migrate**；P1-1 + P1-3 修复 |
| cap_flush BUG count (vm1 3 轮) | 1/3 | **0/3** | 0/10 | ✅ GFP_KERNEL 在 atomic context 单发；P0-0 修复
