/*
 * PowerFS 内核文件系统 - VFS 文件系统操作实现
 *
 * 参考:
 *   - cephfs (fs/ceph/dir.c, fs/ceph/inode.c) - 网络文件系统架构
 *   - ramfs (fs/ramfs/inode.c) - 纯内存 VFS 缓存使用范例
 *
 * 设计原则:
 *   - 完全依赖 VFS dcache 和 page cache，不重复造轮子
 *   - inode 生命周期由 VFS 管理（引用计数 + LRU + shrinker）
 *   - dentry 通过 d_revalidate 做缓存有效性验证
 *   - 数据存储在 page cache 中，使用 ram_aops
 */

#include <linux/module.h>
#include <linux/fs.h>
#include <linux/pagemap.h>
#include <linux/dcache.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/uaccess.h>
#include <linux/time.h>
#include <linux/atomic.h>
#include <linux/fs_context.h>
#include <linux/fs_parser.h>
#include <linux/seq_file.h>
#include <linux/sched.h>
#include <linux/namei.h>
#include <linux/statfs.h>
#include <linux/list_lru.h>
#include <linux/backing-dev.h>
#include <linux/ramfs.h>
#include <linux/writeback.h>     /* folio_mark_dirty, writeback_control */
#include <linux/mnt_idmapping.h> /* mnt_idmap, nop_mnt_idmap (6.5+) */
#include <linux/pagevec.h>       /* pagevec_lookup_range_tag (writepages 批量遍历) */
#include <linux/delay.h>        /* msleep (release 重试退避) */
#include <linux/xattr.h>        /* simple_xattr API for xattr support */
#include <linux/falloc.h>       /* FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE */

#include "powerfs.h"
#include "powerfs_comm.h"
#include "powerfs_net.h"
#include "powerfs_flow.h"

/* ========== netfs 请求操作 (Stage C: 对接 netfs 子系统) ==========
 *
 * 参照 fs/ceph/addr.c 的 ceph_netfs_issue_read 实现.
 *
 * read 路径改造:
 *   - powerfs_aops.read_folio = netfs_read_folio (netfs 提供)
 *   - powerfs_aops.readahead  = netfs_readahead  (netfs 提供)
 *   - powerfs_netfs_ops.issue_read = powerfs_netfs_issue_read (实际网络读取)
 *
 * netfs 子系统管理 folio 生命周期 (锁定/解锁/uptodate), filesystem 只需
 * 在 issue_read 中填充数据并调用 netfs_subreq_terminated 完成.
 *
 * 基本功能阶段: issue_read 中同步调用 powerfs_net_read, 简单但会阻塞
 * 当前进程. 后续可改为异步 (提交网络请求 -> 回调 netfs_subreq_terminated).
 */

static void powerfs_netfs_issue_read(struct netfs_io_subrequest *subreq)
{
    struct netfs_io_request *rreq = subreq->rreq;
    struct inode *inode = rreq->inode;
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    size_t len = subreq->len;
    loff_t start = subreq->start;
    struct iov_iter iter;
    void *buf;
    __u32 read_len = 0;
    int err;

    pr_debug("powerfs: netfs_issue_read ino=%lu start=%llu len=%zu i_size=%llu\n",
            inode->i_ino, (unsigned long long)start, len,
            (unsigned long long)rreq->i_size);

    /* 超出文件大小的部分: 由 netfs 处理 (设置 NETFS_SREQ_CLEAR_TAIL),
     * issue_read 只读取有效数据部分 */
    if (start >= rreq->i_size) {
        pr_debug("powerfs: issue_read start >= i_size, skip (start=%llu i_size=%llu)\n",
                (unsigned long long)start, (unsigned long long)rreq->i_size);
        netfs_read_subreq_terminated(subreq);
        return;
    }
    if (start + len > rreq->i_size)
        len = rreq->i_size - start;

    /* K2: Inline 模式 — 直接从 inline_data 读取, 不走 Volume Server RPC.
     * inline_data 由 i_lock 保护, 持锁期间直接通过 iov_iter 拷贝到 page cache.
     *
     * 注意: netfs_issue_read 在 readahead 路径中调用, preempt 可能被禁用
     * (read_pages → page_cache_ra_unbounded 持 xa_lock). 不能使用 GFP_KERNEL
     * 分配临时缓冲. 直接用 copy_to_iter 从 inline_data 拷贝到 xarray folio.
     * x86-64 上 kmap_local_folio 是 page_address (不睡眠), 持 spinlock 安全. */
    if (pi->placement == POWERFS_PLACEMENT_INLINE) {
        u8 *src;
        u32 src_len;
        size_t copy_len;

        spin_lock(&pi->i_lock);
        src = pi->inline_data;
        src_len = pi->inline_len;
        if (!src || src_len == 0) {
            /* Inline 文件无 inline_data: 新建文件尚未写入, 或 GETATTR 未携带.
             * 不回退到 Volume 路径 (Inline 文件无 needle, locate_chunk 返回 -EINVAL).
             * 返回 0 字节 + HIT_EOF: netfs 将 folio 填零并标记 uptodate,
             * write_begin 正常进行. 对齐 FUSE: read-before-write 对空文件返回 0.
             * HIT_EOF 必须设置, 否则 netfs_read_collect 将 short read 转为 -ENODATA. */
            spin_unlock(&pi->i_lock);
            pr_debug("powerfs: issue_read INLINE ino=%lu no inline_data, return 0 (HIT_EOF)\n",
                    inode->i_ino);
            subreq->transferred = 0;
            __set_bit(NETFS_SREQ_HIT_EOF, &subreq->flags);
            netfs_read_subreq_terminated(subreq);
            return;
        }

        /* 计算可拷贝长度: 从 start 开始, 不超过 inline_len, 不超过请求 len */
        if (start >= src_len) {
            /* 请求超出 inline 数据范围, 返回 0 字节 (EOF) */
            spin_unlock(&pi->i_lock);
            pr_debug("powerfs: issue_read INLINE ino=%lu start=%llu >= inline_len=%u, EOF\n",
                    inode->i_ino, (unsigned long long)start, src_len);
            subreq->transferred = 0;
            __set_bit(NETFS_SREQ_HIT_EOF, &subreq->flags);
            netfs_read_subreq_terminated(subreq);
            return;
        }
        copy_len = min_t(size_t, len, src_len - start);

        /* 直接从 inline_data 拷贝到 page cache (xarray folio).
         * 持 i_lock 防止 inline_data 被并发释放/修改.
         * copy_to_iter 内部 kmap_local_folio 在 x86-64 上不睡眠. */
        if (copy_len > 0) {
            iov_iter_xarray(&iter, ITER_DEST, &rreq->mapping->i_pages,
                            start, copy_len);
            copy_to_iter(src + start, copy_len, &iter);
        }

        /* K2: 调试 — 在锁内计算 checksum 和前 8 字节, 避免锁外
         * src 指针被 GETATTR 并发释放导致 use-after-free. */
        {
            __u32 i, csum = 0;
            __u8 b[8] = {0};
            __u8 *p = src + start;
            for (i = 0; i < copy_len && i < src_len - start; i++) {
                csum += p[i];
                if (i < 8)
                    b[i] = p[i];
            }
            spin_unlock(&pi->i_lock);

            subreq->transferred = copy_len;
            /* 部分读取 (copy_len < len): inline 数据不足请求长度, 即 EOF.
             * 必须设置 HIT_EOF, 否则 netfs_read_collect 将 short read 转为 -ENODATA. */
            if (copy_len < len)
                __set_bit(NETFS_SREQ_HIT_EOF, &subreq->flags);
            pr_info("powerfs: issue_read INLINE ino=%lu start=%llu copy=%zu len=%u csum=%u first8=%02x%02x%02x%02x%02x%02x%02x%02x\n",
                    inode->i_ino, (unsigned long long)start, copy_len, src_len, csum,
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]);
        }
        netfs_read_subreq_terminated(subreq);
        return;
    }

volume_read:
    /* K3: powerfs_net_read 内部按 offset 调用 powerfs_locate_chunk 定位
     * (volume_id, needle_id), 统一支持 Flat/Stripe/WideStripe. */

    /* 新建 Flat 文件尚无 chunks (无 volume_id, file_key, chunks 数组):
     * GETATTR 可能在 create 后返回 placement=Flat 但尚未分配 chunks.
     * 此时不走 powerfs_net_read (locate_chunk 会返回 -EINVAL),
     * 返回 0 字节 + HIT_EOF, 让 write_begin 正常进行 (read-before-write 无数据可读). */
    if (pi->placement != POWERFS_PLACEMENT_INLINE &&
        !pi->volume_id && !pi->file_key &&
        !pi->chunks && !pi->volume_ids) {
        pr_debug("powerfs: issue_read FLAT ino=%lu no chunks yet, return 0 (HIT_EOF)\n",
                inode->i_ino);
        subreq->transferred = 0;
        __set_bit(NETFS_SREQ_HIT_EOF, &subreq->flags);
        netfs_read_subreq_terminated(subreq);
        return;
    }

    /* 基本功能阶段: 同步读取到临时 buffer, 再拷贝到 xarray 中的 folio.
     * 后续优化: 直接从 xarray 映射 folio, 避免额外拷贝 (参照 ceph).
     * GFP_NOFS: 避免 FS 回调递归 (netfs readahead 上下文). */
    buf = kvmalloc(len, GFP_NOFS);
    if (!buf) {
        subreq->error = -ENOMEM;
        netfs_read_subreq_terminated(subreq);
        return;
    }

    err = powerfs_net_read(pi, inode->i_ino, start, len, buf, len, &read_len);
    {
        __u8 *b = (__u8 *)buf;
        pr_debug("powerfs: issue_read powerfs_net_read ret=%d read_len=%u buf[0..7]=%02x %02x %02x %02x %02x %02x %02x %02x\n",
                err, read_len,
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]);
    }
    if (err) {
        pr_warn("powerfs: netfs_issue_read ino=%lu start=%llu len=%zu failed: %d\n",
                inode->i_ino, (unsigned long long)start, len, err);
        kvfree(buf);
        subreq->error = err;
        netfs_read_subreq_terminated(subreq);
        return;
    }

    /* 拷贝到 xarray 中的 folio (netfs 已预先分配并锁定 folio) */
    if (read_len > 0) {
        iov_iter_xarray(&iter, ITER_DEST, &rreq->mapping->i_pages,
                        start, read_len);
        copy_to_iter(buf, read_len, &iter);
        pr_debug("powerfs: issue_read copied %u bytes to folio\n", read_len);
    }

    kvfree(buf);
    subreq->transferred = read_len;
    /* 部分读取 (read_len < len): volume 数据不足请求长度, 即 EOF.
     * 必须设置 HIT_EOF, 否则 netfs_read_collect 将 short read 转为 -ENODATA. */
    if (read_len < len)
        __set_bit(NETFS_SREQ_HIT_EOF, &subreq->flags);
    netfs_read_subreq_terminated(subreq);
}

static const struct netfs_request_ops powerfs_netfs_ops = {
    .issue_read = powerfs_netfs_issue_read,
};

/* ========== 全局 slab 缓存 (参考 ceph 全局 cache) ========== */

static struct kmem_cache *powerfs_inode_cachep;
static struct kmem_cache *powerfs_dentry_cachep;

/* 全局超级块指针 (用于跨模块访问) */
static struct super_block *g_powerfs_sb;

/* ========== 异步 inode 刷新工作队列 ==========
 * 用于 powerfs_invalidate_one: 从 NOTIFY 回调中异步刷新 inode 元数据.
 * 独立于通信层调度器线程, 避免 self-deadlock (调度器等待自己处理的响应). */
struct powerfs_refresh_work {
    struct work_struct work;
    u64 ino;
    struct inode *inode;  /* igrab 引用, work 完成后 iput */
    struct rcu_head rcu;  /* RCU 延迟释放 (workqueue 在 work_fn 返回后仍引用 work_struct) */
};
static struct workqueue_struct *powerfs_refresh_wq;

/*
 * powerfs_get_sb - 获取全局超级块指针
 *
 * 用于通信层等模块访问文件系统的 inode 哈希表
 * 返回的引用需要在使用时持有 (不会增加引用计数)
 */
struct super_block *powerfs_get_sb(void)
{
    return g_powerfs_sb;
}

/* ========== 前向声明 ========== */

static const struct super_operations powerfs_super_ops;
static const struct inode_operations powerfs_dir_inode_operations;
static const struct inode_operations powerfs_file_inode_operations;
static const struct file_operations powerfs_file_operations;
static const struct file_operations powerfs_dir_operations;
static const struct dentry_operations powerfs_dentry_operations;
static const struct address_space_operations powerfs_aops;

/* 目录项管理函数 (前向声明) */
static int powerfs_add_dir_entry(struct inode *dir, u64 ino,
                                  unsigned int type, const char *name);
static int powerfs_remove_dir_entry(struct inode *dir, const char *name);
static void powerfs_clear_dir_entries(struct inode *dir);

/* lease 续约 work 函数 (前向声明，Step 2 实现真实续约) */
static void powerfs_lease_renew_work_func(struct work_struct *work);
/* 异步 setattr work 函数 (writeback offload, 定义在 write_inode 之前) */
static void powerfs_setattr_work_fn(struct work_struct *work);
/* lease 释放 (evict_inode 调用, 定义在 lease 管理段) */
static void release_all_leases(struct inode *inode);
/* lease 获取 (写路径调用, 定义在 lease 管理段) */
static int ensure_lease(struct inode *inode, loff_t offset);

/* ========== Dentry operations 实现 ========== */

/*
 * d_init - 新 dentry 创建时分配私有数据
 *
 * 目录级 lease 方案: dentry_info 不再维护独立 lease, 仅保留 RCU 释放和
 * readdir 偏移. dentry 有效性完全依赖父目录 inode 的 dir_lease_expire.
 *
 * 参考 ceph_d_init (fs/ceph/dir.c)
 *
 * 返回 0 表示成功，负值表示失败 (d_fsdata 将为 NULL)
 */
int powerfs_d_init(struct dentry *dentry)
{
    struct powerfs_dentry_info *di;

    di = kmem_cache_zalloc(powerfs_dentry_cachep, GFP_KERNEL);
    if (!di)
        return -ENOMEM;

    di->dentry = dentry;
    di->time = jiffies;

    dentry->d_fsdata = di;

    pr_debug("powerfs: d_init '%pd' (di=%p)\n", dentry, di);
    return 0;
}

/*
 * powerfs_di_free_rcu - RCU 回调, 延迟释放 dentry 私有数据
 *
 * 为什么需要 RCU 延迟释放:
 *   __d_lookup_rcu 在 RCU 临界区中返回 dentry, 调用者 lookup_fast 随后调用
 *   d_revalidate, 后者读 dentry->d_fsdata. 如果 d_release 直接 kmem_cache_free
 *   释放 di, RCU reader 仍可能解引用已释放的指针 -> UAF in __d_lookup_rcu.
 *
 *   d_release 只是设置 d_fsdata = NULL 并 call_rcu 排队, 真正的 kmem_cache_free
 *   在 RCU grace period 之后执行, 此时所有 RCU reader 已退出临界区.
 */
static void powerfs_di_free_rcu(struct rcu_head *head)
{
    struct powerfs_dentry_info *di =
        container_of(head, struct powerfs_dentry_info, rcu);

    kmem_cache_free(powerfs_dentry_cachep, di);
}

/*
 * d_release - dentry 销毁前释放私有数据
 *
 * 目录级 lease 方案: 不再有 lease_list 需要摘除, 仅做 RCU 延迟释放.
 *
 * 重要: d_fsdata 必须用 call_rcu 延迟释放, 不能裸 kmem_cache_free.
 * 原因: RCU path walk (__d_lookup_rcu + d_revalidate) 可能并发读 d_fsdata.
 *
 * 参考 ceph_d_release (fs/ceph/dir.c)
 */
void powerfs_d_release(struct dentry *dentry)
{
    struct powerfs_dentry_info *di = dentry->d_fsdata;

    if (!di)
        return;

    pr_debug("powerfs: d_release '%pd' (di=%p)\n", dentry, di);

    /* 设置 d_fsdata = NULL 并通过 RCU 延迟释放 di.
     * RCU reader 在 d_revalidate 中读 d_fsdata 可看到 NULL (安全:
     * d_revalidate 不再解引用 di) 或旧指针 (仍有效, 因为 di 还没被 free).
     * grace period 后才真正 free, 保证无 UAF. */
    dentry->d_fsdata = NULL;
    call_rcu(&di->rcu, powerfs_di_free_rcu);
}

/* Phase 1 前置声明: 目录 lease 失效 (定义在 readdir 区段, 但 mknod 等更早使用). */
static void powerfs_invalidate_dir_lease(struct inode *dir);

/*
 * d_revalidate - 基于父目录 Lease 校验 dentry 有效性
 *
 * 核心原则 (目录级 Lease 方案):
 *   1. RCU 路径: 无锁读取父目录 dir_lease_expire, 有效返回 1, 过期返回 -ECHILD
 *   2. REF 路径: 统一返回 1 (永不返回 0)
 *   3. 永不触发 d_invalidate + d_drop + re-lookup 循环
 *
 * 为什么永不返回 0:
 *   return 0 触发 VFS 的 d_invalidate → d_drop → dput → d_alloc_parallel →
 *   __d_lookup_rcu. 负 dentry 释放路径短 (无 iput), 与 __d_lookup_rcu 的
 *   RCU 遍历竞态 → dentry 哈希链环 → RCU stall.
 *   返回 1 让 VFS 使用缓存, stale dentry 由 readdir/shrinker 清理.
 *
 * 返回值:
 *   1: dentry 有效, 使用缓存 (正/负 dentry 无差别)
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
     * 用 READ_ONCE 读取 dir_lease_expire, 不持任何 spinlock (RCU 临界区禁止
     * 取 spinlock, 否则会导致 RCU stall).
     * 值可能略旧 (并发 mutation 刚清零), 但最差情况是放行一个 stale dentry,
     * 下次访问会纠正, 不会导致 stall. */
    if (flags & LOOKUP_RCU) {
        if (!dir)
            return -ECHILD;
        parent_pi = POWERFS_I(dir);
        lease_expire = READ_ONCE(parent_pi->dir_lease_expire);

        if (time_before(jiffies, lease_expire))
            return 1;       /* 父目录 lease 有效: 正/负 dentry 全部放行 */
        else
            return -ECHILD; /* lease 过期: 降级 REF 路径 */
    }

    /* === REF 路径: 统一返回 1 ===
     *
     * 不在此处做 lease 续约 RPC (d_revalidate 在路径遍历中频繁调用, 内嵌
     * RPC 会阻塞). lease 续约由 readdir / lookup 统一处理.
     * stale dentry 通过 readdir 刷新 / shrinker 回收 / 本地 mutation 失效. */
    return 1;
}

/*
 * d_prune - dentry 被 shrinker 回收前的通知
 *
 * 参考 ceph_d_prune (fs/ceph/dir.c)
 *
 * 用于清除父目录的 complete 标志，因为目录内容缓存不再完整
 */
void powerfs_d_prune(struct dentry *dentry)
{
    struct dentry *parent = dentry->d_parent;
    struct powerfs_inode_info *ppi;
    struct inode *dir;

    /* 根 dentry 不 prune (参考 ceph_d_prune) */
    if (IS_ROOT(dentry))
        return;

    /* d_prune 在 dentry->d_lock 被持有时调用 (见 __dentry_kill).
     * 只能用 d_parent (d_lock 保护) 和无锁操作, 不能获取任何 spinlock
     * (如 i_lock), 否则会与持 i_lock 后尝试 d_lock 的路径死锁. */
    dir = d_inode(parent);
    if (!dir || !S_ISDIR(dir->i_mode))
        return;

    ppi = POWERFS_I(dir);

    pr_debug("powerfs: d_prune '%pd' (parent=%pd, clearing dir_complete)\n",
             dentry, parent);

    /* 清除父目录的 complete 标志.
     * 参照 ceph __ceph_dir_clear_complete (atomic64_inc, 无锁).
     * dir_complete 是 bool, WRITE_ONCE 保证原子写入, 读取侧用 READ_ONCE. */
    WRITE_ONCE(ppi->dir_complete, false);
}

/* Dentry operations 表
 *
 * 目录级 lease 方案:
 *   - d_revalidate: 检查父目录 lease, 永不返回 0 (避免 RCU stall)
 *   - d_init: 分配 dentry_info (RCU 释放用)
 *   - d_release: RCU 延迟释放 dentry_info (防止内存泄漏 + UAF)
 *   - d_prune: 清父目录 dir_complete (shrinker 回收时目录缓存不再完整)
 *
 * 参考 ceph dentry_operations
 */
static const struct dentry_operations powerfs_dentry_operations = {
    .d_revalidate   = powerfs_d_revalidate,
    .d_init         = powerfs_d_init,
    .d_release      = powerfs_d_release,
    .d_prune        = powerfs_d_prune,
};

/* ========== 辅助函数 ========== */

/*
 * powerfs_ino_compare - inode 比较函数 (用于 iget5_locked/ilookup5)
 *
 * 参考 ceph_ino_compare (fs/ceph/super.h)
 *
 * 比较 inode 的 ino 是否匹配。
 * 目前只比较 ino，后续如果支持快照等可以扩展。
 */
static int powerfs_ino_compare(struct inode *inode, void *data)
{
    u64 *pino = (u64 *)data;
    return inode->i_ino == *pino;
}

/*
 * powerfs_set_ino_cb - 设置新 inode 的 ino (用于 iget5_locked)
 *
 * 参考 ceph_set_ino_cb (fs/ceph/inode.c)
 */
static int powerfs_set_ino_cb(struct inode *inode, void *data)
{
    u64 *pino = (u64 *)data;
    inode->i_ino = *pino;
    return 0;
}

/*
 * powerfs_iget - 获取或创建 inode (参考 ceph_get_inode)
 *
 * 使用 iget5_locked 在内核 inode 哈希表中查找：
 *   - 如果找到：增加引用计数并返回
 *   - 如果没找到：分配新 inode，设置 I_NEW 状态并返回
 *
 * 调用者需要检查 inode->i_state & I_NEW 来判断是否是新创建的，
 * 如果是新创建的，需要调用 powerfs_init_inode 初始化，
 * 然后调用 unlock_new_inode 解锁。
 */
struct inode *powerfs_iget(struct super_block *sb, u64 ino)
{
    struct inode *inode;
    u64 ino_val = ino;

    inode = iget5_locked(sb, (unsigned long)ino,
                         powerfs_ino_compare,
                         powerfs_set_ino_cb,
                         &ino_val);
    if (!inode)
        return ERR_PTR(-ENOMEM);

    pr_debug("powerfs: iget ino=%llu inode=%p new=%d\n",
             ino, inode, !!(inode->i_state & I_NEW));

    return inode;
}

/*
 * powerfs_find_inode - 查找已存在的 inode (参考 ceph_find_inode)
 *
 * 使用 ilookup5 在内核 inode 哈希表中查找：
 *   - 如果找到：增加引用计数并返回
 *   - 如果没找到：返回 NULL
 *
 * 注意: 不会创建新 inode，只查找已存在的。
 */
struct inode *powerfs_find_inode(struct super_block *sb, u64 ino)
{
    u64 ino_val = ino;
    return ilookup5(sb, (unsigned long)ino,
                    powerfs_ino_compare, &ino_val);
}

/*
 * powerfs_locate_chunk - K1-5 统一 chunk 定位 (Flat/Stripe 多卷入口)
 *
 * 对齐 FUSE powerfs-fuse/src/fuse.rs resolve_stripe_chunk() (L462) 逻辑:
 *   - Flat 模型: needle_id = file_key + offset / chunk_size,
 *     volume_id = inode->volume_id (单卷)
 *   - Stripe 模型 (K3):
 *       stripe_unit_idx = offset / stripe_size
 *       chunk_idx_in_unit = (offset % stripe_size) / chunk_size
 *       volume_id = volume_ids[stripe_unit_idx]
 *       needle_id = file_key + chunk_idx_in_unit
 *   - Inline 模型 (K2): 返回 -EINVAL, inline 不走 volume 路径
 *
 * 注意: 调用方应持 pi->i_lock 或确保 volume_ids/chunks 不被并发释放.
 *       当前 read/write 路径在持锁快照后调用, 满足约束.
 */
int powerfs_locate_chunk(struct powerfs_inode_info *pi, loff_t offset,
                         u64 *volume_id_out, u64 *needle_id_out)
{
    u32 chunk_size;
    u64 chunk_idx;

    if (!pi || !volume_id_out || !needle_id_out)
        return -EINVAL;

    /* Inline 文件不走 volume 路径 (K2) */
    if (pi->placement == POWERFS_PLACEMENT_INLINE)
        return -EINVAL;

    /* chunk_size: 优先用 layout 解析值, 兜底 POWERFS_CHUNK_SIZE */
    chunk_size = pi->layout_chunk_size ? pi->layout_chunk_size : POWERFS_CHUNK_SIZE;
    if (chunk_size == 0)
        return -EINVAL;

    chunk_idx = (u64)(offset / chunk_size);

    /* K3 多卷路径: chunks 数组存在且 chunk_idx 命中.
     * 用于 Flat 模式下 GETATTR 返回的显式 chunks 列表. */
    if (pi->chunks && chunk_idx < pi->chunk_count) {
        struct powerfs_chunk_map *cm = &pi->chunks[chunk_idx];
        if (cm->volume_id != 0 && cm->needle_id != 0) {
            *volume_id_out = cm->volume_id;
            *needle_id_out = cm->needle_id;
            return 0;
        }
    }

    /* K3 Stripe 多卷路径: volume_ids 数组 + file_key base needle.
     * 对齐 FUSE resolve_stripe_chunk (fuse.rs L462).
     * stripe_unit_idx 索引 volume_ids[], chunk_idx_in_unit 偏移 needle_id. */
    if ((pi->placement == POWERFS_PLACEMENT_STRIPE ||
         pi->placement == POWERFS_PLACEMENT_WIDESTRIPE) &&
        pi->volume_ids && pi->volume_ids_count > 0) {
        u64 stripe_size = pi->stripe_size ? pi->stripe_size : chunk_size;
        u64 stripe_unit_idx = (u64)(offset / stripe_size);
        u64 chunk_idx_in_unit;

        if (stripe_unit_idx >= pi->volume_ids_count) {
            pr_debug("powerfs: locate stripe_unit_idx=%llu >= count=%u (offset=%lld)\n",
                     stripe_unit_idx, pi->volume_ids_count, offset);
            return -EINVAL;
        }

        chunk_idx_in_unit = (u64)((offset % stripe_size) / chunk_size);
        *volume_id_out = pi->volume_ids[stripe_unit_idx];
        *needle_id_out = pi->file_key + chunk_idx_in_unit;
        return 0;
    }

    /* Flat 模型: file_key + chunk_idx, 单卷 */
    if (!pi->volume_id || !pi->file_key)
        return -EINVAL;

    *volume_id_out = pi->volume_id;
    *needle_id_out = pi->file_key + chunk_idx;
    return 0;
}

/*
 * powerfs_apply_layout_to_inode - K3-1 将 FileLayout 解析结果应用到 inode
 *
 * 在持 pi->i_lock 的情况下调用. volume_ids/inline_data 所有权从 layout 转移到 inode.
 * 若 inode 已有 volume_ids/inline_data, 先 kfree 旧的再替换 (避免泄漏).
 */
void powerfs_apply_layout_to_inode(struct powerfs_inode_info *pi,
                                   struct powerfs_file_layout *layout)
{
    u64 *old_vids;
    u8 *old_inline;

    if (!pi || !layout)
        return;

    /* K2: 保护有未提交 inline_data 的 inode — refresh_work 的 getattr 可能在
     * 文件写入过程中触发, 此时 Filer 还没收到 inline_data (close 时才提交),
     * getattr 响应中 placement=Flat (info.inline_data=None).
     * 若直接覆盖 placement=Flat, 后续 write_end 不再走 Inline 分支,
     * 导致 inline_data 不一致 + 数据丢失.
     * 修复: inode 有 inline_dirty 时, 不从 getattr 覆盖 placement. */
    if (layout->has_placement) {
        if (pi->placement == POWERFS_PLACEMENT_INLINE && pi->inline_dirty &&
            layout->placement != POWERFS_PLACEMENT_INLINE) {
            pr_info("powerfs: apply_layout skip placement=%u→%u, inline_dirty ino=%lu\n",
                    pi->placement, layout->placement, pi->netfs.inode.i_ino);
        } else {
            pi->placement = layout->placement;
        }
    }
    if (layout->has_reliability)
        pi->reliability = layout->reliability;
    pi->reliability_state = layout->reliability_state;
    if (layout->chunk_size > 0)
        pi->layout_chunk_size = layout->chunk_size;

    /* K2: InlineMaxSize — 从 layout 同步到 inode */
    if (layout->inline_max_size > 0)
        pi->inline_max_size = layout->inline_max_size;
    else if (pi->inline_max_size == 0)
        pi->inline_max_size = POWERFS_INLINE_MAX_SIZE;

    /* K2: InlineData — 仅在 placement==INLINE 时应用.
     * 其他模式不应携带 inline_data, 但若误传则释放避免泄漏. */
    if (pi->placement == POWERFS_PLACEMENT_INLINE && layout->has_inline_data) {
        /* inline_data 所有权转移: 先释放旧 buffer, 再挂载新 buffer */
        old_inline = pi->inline_data;
        pi->inline_data = layout->inline_data;
        pi->inline_len = layout->inline_len;
        layout->inline_data = NULL;       /* 所有权转移, 防止 double-free */
        layout->inline_len = 0;
        kfree(old_inline);
    } else {
        /* Flat/Stripe: 不应持有 inline_data, 释放误传的 buffer */
        kfree(layout->inline_data);
        layout->inline_data = NULL;
        layout->inline_len = 0;
        /* 若 placement 从 Inline 切换到 Flat (迁移后), 清除 inode 的 inline_data */
        if (pi->placement != POWERFS_PLACEMENT_INLINE && pi->inline_data) {
            kfree(pi->inline_data);
            pi->inline_data = NULL;
            pi->inline_len = 0;
            pi->inline_dirty = false;
        }
    }

    /* K3: Stripe 元数据. 仅在 placement 为 Stripe/WideStripe 时应用.
     * Flat/Inline 模式不应携带 volume_ids, 但若误传则释放避免泄漏. */
    if (pi->placement == POWERFS_PLACEMENT_STRIPE ||
        pi->placement == POWERFS_PLACEMENT_WIDESTRIPE) {
        pi->stripe_size = layout->stripe_size;
        pi->stripe_count = layout->stripe_count;
        pi->start_volume_idx = layout->start_volume_idx;

        /* volume_ids 所有权转移: 先释放旧数组, 再挂载新数组 */
        old_vids = pi->volume_ids;
        pi->volume_ids = layout->volume_ids;
        pi->volume_ids_count = layout->volume_ids_count;
        layout->volume_ids = NULL;       /* 所有权转移, 防止 double-free */
        layout->volume_ids_count = 0;
        kfree(old_vids);
    } else {
        /* Flat/Inline: 不应持有 volume_ids, 释放误传的数组 */
        kfree(layout->volume_ids);
        layout->volume_ids = NULL;
        layout->volume_ids_count = 0;
        /* 清零 inode 上可能残留的 Stripe 字段 (placement 切换场景) */
        if (pi->volume_ids) {
            kfree(pi->volume_ids);
            pi->volume_ids = NULL;
            pi->volume_ids_count = 0;
        }
        pi->stripe_size = 0;
        pi->stripe_count = 0;
    }

    /* K4-8: EC 元数据 — 从 Reliability EC 分支解析.
     * 仅在 reliability==EC 时应用, 非 EC 清零. */
    if (pi->reliability == POWERFS_RELIABILITY_EC) {
        pi->ec_data_shards = layout->ec_data_shards;
        pi->ec_parity_shards = layout->ec_parity_shards;
    } else {
        pi->ec_data_shards = 0;
        pi->ec_parity_shards = 0;
    }

    /* K4-2: ReplicaChunks — 读 failover 使用.
     * 所有权转移: 先释放旧数组, 再挂载新数组.
     * 仅在 reliability==REPLICATED 时有意义, 但解析不区分 (apply 时决定). */
    if (layout->has_replica_chunks) {
        struct powerfs_chunk_map *old_rep = pi->replica_chunks;
        pi->replica_chunks = layout->replica_chunks;
        pi->replica_count = layout->replica_count;
        layout->replica_chunks = NULL;     /* 所有权转移, 防止 double-free */
        layout->replica_count = 0;
        kfree(old_rep);
    }

    /* K4-5: EC shards — EC 读取路径使用.
     * 从 ChunkLayout PER_CHUNK 解析, 所有权转移到 inode. */
    if (layout->has_ec_chunks) {
        struct powerfs_chunk_map *old_ec = pi->ec_chunks;
        pi->ec_chunks = layout->ec_chunks;
        pi->ec_chunk_count = layout->ec_chunk_count;
        layout->ec_chunks = NULL;
        layout->ec_chunk_count = 0;
        kfree(old_ec);
    }
}

/*
 * powerfs_refresh_inode_work - 异步刷新 inode 元数据 (workqueue 回调)
 *
 * 在独立工作队列中执行, 避免 self-deadlock:
 *   - 调度器线程收到 NOTIFY → powerfs_invalidate_one → queue_work
 *   - 本函数在独立线程中执行 powerfs_net_getattr
 *   - getattr 响应由调度器线程处理 (不阻塞本线程)
 *
 * 步骤:
 *   1. 发 getattr 获取最新 size/volume_id/file_key
 *   2. 更新 inode 属性 (i_size, volume_id, file_key)
 *   3. 失效 page cache (clean pages)
 *   4. 清 need_refresh
 */
/* RCU 回调: 延迟释放 refresh_work (workqueue 在 work_fn 返回后仍引用 work_struct) */
static void powerfs_refresh_work_free_rcu(struct rcu_head *head)
{
    struct powerfs_refresh_work *rw =
        container_of(head, struct powerfs_refresh_work, rcu);
    kfree(rw);
}

static void powerfs_refresh_inode_work(struct work_struct *work)
{
    struct powerfs_refresh_work *rw =
        container_of(work, struct powerfs_refresh_work, work);
    struct inode *inode = rw->inode;
    struct powerfs_inode_info *pi;
    __u32 mode = 0, uid = 0, gid = 0, nlink = 0;
    __u64 size = 0, mtime = 0, atime = 0, ctime = 0;
    __u64 volume_id = 0, file_key = 0;
    int ret;

    /* If inode is NULL, do the lookup here (in workqueue context, not RX thread).
     * This avoids blocking the RX dispatcher in ilookup5 → __wait_on_freeing_inode
     * when the inode is being freed. The lookup may block, but only this workqueue
     * thread is affected, not the RX thread that handles all network responses. */
    if (!inode) {
        struct super_block *sb = powerfs_get_sb();
        if (!sb)
            goto out_free;
        inode = powerfs_find_inode(sb, rw->ino);
        if (!inode)
            goto out_free;
        /* ilookup5 returned a referenced inode; iput at out_iput. */
    }

    /* 防御性检查: inode 是否已被 evict (I_FREEING/I_CLEAR).
     * 虽然修复了 igrab 检查后不应出现此情况, 但 getattr 是长时间
     * 阻塞网络调用, 返回后 inode 状态可能已变. 持有 igrab 引用
     * 保证 inode 不会被释放, 但 evict_inode 可能已标记 shutdown.
     * 此时不应再修改 inode 字段 (可能与 evict 并发). */
    if (inode->i_state & (I_FREEING | I_CLEAR | I_WILL_FREE)) {
        pr_debug("powerfs: refresh_work ino=%llu inode being evicted, skip\n",
                 rw->ino);
        goto out_iput;
    }

    pi = POWERFS_I(inode);

    /* evict_inode 已设置 shutdown: inode 私有数据正在被释放,
     * 不应再修改 pi 字段. */
    if (pi->shutdown) {
        pr_debug("powerfs: refresh_work ino=%llu inode shutdown, skip\n",
                 rw->ino);
        goto out_iput;
    }

    /* Mark need_refresh: signals concurrent readers that a refresh is in flight.
     * (Previously set in powerfs_invalidate_one before scheduling, but now
     * the inode lookup is deferred to this work function.) */
    spin_lock(&pi->i_lock);
    pi->need_refresh = true;
    spin_unlock(&pi->i_lock);

    /* 1. 发 getattr 获取最新元数据 */
    {
        struct powerfs_file_layout layout = {0};
        ret = powerfs_net_getattr(inode->i_ino, &mode, &uid, &gid,
                                  &size, &nlink,
                                  &mtime, &atime, &ctime,
                                  &volume_id, &file_key, &layout);
        if (ret == 0) {
            spin_lock(&pi->i_lock);
            powerfs_apply_layout_to_inode(pi, &layout);
            spin_unlock(&pi->i_lock);
            /* apply 后 layout.volume_ids/inline_data/replica_chunks 已转移到 inode (或已释放).
             * 防御性: 若 apply 异常未消费, 这里释放. */
            kfree(layout.volume_ids);
            kfree(layout.inline_data);
            kfree(layout.replica_chunks);
            kfree(layout.ec_chunks);
        } else {
            /* getattr 失败: 释放 parse 可能分配的 volume_ids/inline_data/replica_chunks/ec_chunks */
            kfree(layout.volume_ids);
            kfree(layout.inline_data);
            kfree(layout.replica_chunks);
            kfree(layout.ec_chunks);
        }
    }
    if (ret) {
        pr_warn("powerfs: refresh_work ino=%llu getattr failed: %d\n",
                rw->ino, ret);
        /* getattr 失败: 清 cache_valid, 让下次访问触发 re-lookup.
         * 但若 inode 已 shutdown, 不修改 pi 字段 (evict 进行中). */
        if (!pi->shutdown) {
            spin_lock(&pi->i_lock);
            pi->cache_valid = false;
            pi->need_refresh = false;
            spin_unlock(&pi->i_lock);
        }
        goto out_iput;
    }

    /* getattr 返回后再次检查: 长时间网络阻塞期间 inode 可能被 evict. */
    if (pi->shutdown || (inode->i_state & (I_FREEING | I_CLEAR))) {
        pr_debug("powerfs: refresh_work ino=%llu inode evicted during getattr, skip update\n",
                 rw->ino);
        goto out_iput;
    }

    /* 2. 更新 inode 属性 */
    spin_lock(&inode->i_lock);
    /* 如果本地有脏页 (未刷盘的写入), 不用 filer 端的 size 覆盖本地 i_size.
     * 因为 write_end 已设置 i_size, 但 writeback 尚未通过 setattr 同步到 filer,
     * filer 端的 size 可能是旧值 (0).
     * 参考 ceph: 有 i_dirty_caps 时不覆盖 i_size. */
    if (mapping_tagged(inode->i_mapping, PAGECACHE_TAG_DIRTY) ||
        mapping_tagged(inode->i_mapping, PAGECACHE_TAG_WRITEBACK)) {
        pr_debug("powerfs: refresh_work ino=%llu has dirty/writeback pages, skip size update (local=%lld filer=%llu)\n",
                rw->ino, i_size_read(inode), (unsigned long long)size);
    } else {
        if (i_size_read(inode) != size) {
            i_size_write(inode, size);
        }
    }
    set_nlink(inode, nlink);
    inode_set_mtime(inode, mtime, 0);
    inode_set_atime(inode, atime, 0);
    inode_set_ctime(inode, ctime, 0);
    spin_unlock(&inode->i_lock);

    spin_lock(&pi->i_lock);
    /* 仅在 Filer 返回非零值时更新 volume_id/file_key.
     * close 前的 getattr 可能返回 0 (Filer 端 chunks 在 close 时才同步),
     * 若用 0 覆盖迁移后已设置的值, 会导致 writeback locate 失败 (-EINVAL). */
    if (volume_id != 0)
        pi->volume_id = volume_id;
    if (file_key != 0)
        pi->file_key = file_key;
    pi->cache_valid = true;
    pi->cache_expire = jiffies + POWERFS_INODE_CACHE_TTL;
    pi->need_refresh = false;
    spin_unlock(&pi->i_lock);

    pr_debug("powerfs: refresh_work ino=%llu size=%llu vid=%llu fkey=%llu\n",
            rw->ino, (unsigned long long)size,
            (unsigned long long)volume_id,
            (unsigned long long)file_key);

    /* 3. 失效 page cache (clean unlocked pages), 使下次读从 volume 重新拉取.
     * 使用非阻塞版本 invalidate_mapping_pages: 跳过已锁定/脏页, 不阻塞等待.
     *
     * 之前用 invalidate_inode_pages2 会在 __folio_lock 阻塞:
     *   - 写路径持页锁期间 refresh worker 进入 D 状态
     *   - 读路径持页锁期间同样阻塞 (页虽干净但被锁)
     * 导致 hung_task 检测 (60s) 或测试脚本 D-state 检查失败.
     *
     * invalidate_mapping_pages 使用 trylock, 遇到锁定的页直接跳过:
     *   - 被跳过的页会在下次 lookup 时自然失效 (cache_valid=false)
     *   - 无 mmap 支持, 不需要 unmap 处理 */
    invalidate_mapping_pages(inode->i_mapping, 0, (pgoff_t)-1);

    /* 4. For directories, expire the readdir lease so next readdir
     * re-fetches entries from the Filer. (Moved from powerfs_invalidate_one
     * since the inode lookup is now deferred to this work function.) */
    if (S_ISDIR(inode->i_mode))
        powerfs_invalidate_dir_lease(inode);

out_iput:
    iput(inode);
out_free:
    /* 使用 call_rcu 延迟释放: workqueue 的 worker_thread 在 work_fn 返回后
     * 仍需引用 work_struct (assign_work 等), 直接 kfree 会导致 use-after-free.
     * 与 writeback 路径 (powerfs_wb_final_cleanup) 使用相同的 call_rcu 模式. */
    call_rcu(&rw->rcu, powerfs_refresh_work_free_rcu);
}

/*
 * powerfs_invalidate_one - Invalidate one inode's caches
 *
 * Called from the powerfs-net RX path when a NOTIFY frame arrives
 * from the Filer (triggered by another client's metadata mutation).
 *
 * This function is NON-BLOCKING and safe to call from the RX dispatcher.
 * It defers ALL work (inode lookup, getattr, page cache invalidation,
 * dir lease expiry) to powerfs_refresh_wq via powerfs_refresh_inode_work.
 *
 * Why deferral is required:
 *   ilookup5 → find_inode → __wait_on_freeing_inode blocks if the inode
 *   is being freed. In the RX thread, this blocks ALL response processing
 *   (writeback completions, read responses, etc.), causing hung_task panic
 *   after 60s. By deferring to a workqueue, only the workqueue thread
 *   blocks, not the RX thread.
 *
 * We intentionally do NOT d_drop() here: the Fuser-side Invalidate
 * only carries (inode, version), so we don't know if the inode was
 * deleted or merely modified.  The next lookup/getattr will fetch
 * fresh metadata; if the inode no longer exists on the Filer, the
 * lookup returns negative and VFS evicts the dentry naturally.
 */
int powerfs_invalidate_one(u64 ino)
{
    struct powerfs_refresh_work *rw;

    /* Schedule async refresh work with inode=NULL: the work function will
     * do the ilookup5 in workqueue context, NOT in the RX dispatcher thread.
     *
     * Why: ilookup5 → find_inode → __wait_on_freeing_inode blocks if the
     * inode is being freed. In the RX thread, this blocks ALL response
     * processing (including writeback completions), causing hung_task panic
     * after 60s. Moving the lookup to a workqueue isolates the blocking. */
    rw = kmalloc(sizeof(*rw), GFP_ATOMIC);
    if (!rw) {
        pr_warn("powerfs: invalidate_one ino=%llu kmalloc failed, skipped\n", ino);
        return -ENOMEM;
    }

    INIT_WORK(&rw->work, powerfs_refresh_inode_work);
    rw->ino = ino;
    rw->inode = NULL;  /* NULL → work function will do ilookup5 */
    queue_work(powerfs_refresh_wq, &rw->work);

    pr_debug("powerfs: invalidate_one ino=%llu queued (lookup deferred to workqueue)\n", ino);
    return 0;
}
EXPORT_SYMBOL_GPL(powerfs_invalidate_one);

/*
 * powerfs_init_inode - 初始化新 inode 的字段
 *
 * 当 powerfs_iget 返回 I_NEW 状态的 inode 时，
 * 调用此函数初始化 inode 的各个字段。
 *
 * 参考 ramfs_get_inode + powerfs_new_inode 的逻辑。
 */
int powerfs_init_inode(struct inode *inode, umode_t mode,
                       u64 parent_ino, const char *name)
{
    struct powerfs_inode_info *pi = POWERFS_I(inode);

    pr_debug("powerfs: init_inode ino=%lu mode=%o, S_IFDIR=%d\n",
            inode->i_ino, mode, S_ISDIR(mode));

    /* 初始化所有者和权限 */
    inode_init_owner(&nop_mnt_idmap, inode, NULL, mode);

    /* 设置 page cache 操作 */
    inode->i_mapping->a_ops = &powerfs_aops;
    mapping_set_gfp_mask(inode->i_mapping, GFP_HIGHUSER);
    mapping_set_unevictable(inode->i_mapping);

    /* 设置时间戳 */
    {
        struct timespec64 now = current_time(inode);
        inode_set_atime(inode, now.tv_sec, now.tv_nsec);
        inode_set_mtime(inode, now.tv_sec, now.tv_nsec);
        inode_set_ctime(inode, now.tv_sec, now.tv_nsec);
    }

    /* 初始化私有字段 */
    pi->parent_ino = parent_ino;
    strncpy(pi->name, name ? name : "", POWERFS_MAX_NAME_LEN - 1);
    pi->cache_valid = true;
    pi->cache_expire = jiffies + POWERFS_INODE_CACHE_TTL;
    /* dir_complete = false: looked-up directories must fetch entries from
     * Filer on first readdir. Only mkdir (new empty directory) sets this
     * to true after calling powerfs_init_inode. Setting true here causes
     * readdir to take the fast-path with an empty dir_entries list,
     * making directories appear empty after remount. */
    pi->dir_complete = false;
    pi->dir_lease_expire = 0;
    pi->dir_lease_epoch = 0;

    /* 初始化目录项链表 */
    INIT_LIST_HEAD(&pi->dir_entries);
    mutex_init(&pi->dir_mutex);

    /* 根据文件类型设置操作表 */
    switch (mode & S_IFMT) {
    case S_IFREG:
        inode->i_op = &powerfs_file_inode_operations;
        inode->i_fop = &powerfs_file_operations;
        set_nlink(inode, 1);
        pr_debug("powerfs: init_inode REG, i_fop=%p\n", inode->i_fop);
        break;

    case S_IFDIR:
        inode->i_op = &powerfs_dir_inode_operations;
        inode->i_fop = &powerfs_dir_operations;
        set_nlink(inode, 2);  /* "." + ".." */
        /* dir_complete stays false (set above). mkdir path sets it to
         * true after powerfs_init_inode returns (new empty directory). */
        pr_debug("powerfs: init_inode DIR, i_fop=%p\n", inode->i_fop);
        break;

    case S_IFLNK:
        inode->i_op = &page_symlink_inode_operations;
        inode_nohighmem(inode);
        set_nlink(inode, 1);
        break;

    default:
        init_special_inode(inode, mode, 0);
        set_nlink(inode, 1);
        break;
    }

    return 0;
}

/* ========== Inode 生命周期管理 ========== */

/*
 * inode 初始化 (slab 构造函数)
 *
 * 只初始化 powerfs 私有字段，不触碰 inode 结构本身
 * VFS 的 inode_init_once 会处理 inode 核心字段
 */
static void powerfs_inode_init_once(void *foo)
{
    struct powerfs_inode_info *pi = foo;

    inode_init_once(&pi->netfs.inode);

    /* 初始化私有字段 */
    pi->parent_ino = 0;
    pi->name[0] = '\0';
    spin_lock_init(&pi->i_lock);
    pi->cache_valid = false;
    pi->cache_expire = 0;
    pi->dir_complete = false;
}

/*
 * powerfs_alloc_inode - 分配 inode (super_operations)
 *
 * 参考 ceph_alloc_inode (fs/ceph/inode.c)
 * 使用 alloc_inode_sb 辅助函数
 */
struct inode *powerfs_alloc_inode(struct super_block *sb)
{
    struct powerfs_inode_info *pi;

    pi = alloc_inode_sb(sb, powerfs_inode_cachep, GFP_NOFS);
    if (!pi)
        return NULL;

    /* netfs 初始化 (参考 ceph_alloc_inode) */
    netfs_inode_init(&pi->netfs, &powerfs_netfs_ops, false);

    /* 初始化 Lease 相关字段 */
    pi->lease_tree = RB_ROOT;
    spin_lock_init(&pi->lease_lock);
    INIT_DELAYED_WORK(&pi->lease_renew_work, powerfs_lease_renew_work_func);
    pi->chunks = NULL;
    pi->chunk_count = 0;
    pi->content_size = 0;
    pi->volume_id = 0;
    pi->file_key = 0;
    pi->shutdown = false;

    /* K3: FileLayout 默认值 (placement 枚举 0=INLINE, 必须显式设 FLAT).
     * layout_chunk_size 默认 POWERFS_CHUNK_SIZE, 后续 GETATTR 可覆盖. */
    pi->placement = POWERFS_PLACEMENT_FLAT;
    pi->reliability = POWERFS_RELIABILITY_SINGLE;
    pi->reliability_state = POWERFS_RSTATE_PENDING;
    pi->layout_chunk_size = POWERFS_CHUNK_SIZE;
    pi->stripe_size = 0;
    pi->stripe_count = 0;
    pi->start_volume_idx = 0;
    pi->volume_ids = NULL;
    pi->volume_ids_count = 0;
    pi->replica_chunks = NULL;
    pi->replica_count = 0;

    /* K4-5: EC shards 列表 — 必须初始化为 NULL!
     * 修复 crash: slab 重用旧 inode 内存时, ec_chunks 残留悬空指针,
     * apply_layout_to_inode 的 kfree(old_ec) 对垃圾指针 kfree → GPF.
     * 同理 ec_data_shards/ec_parity_shards 也需初始化. */
    pi->ec_chunks = NULL;
    pi->ec_chunk_count = 0;
    pi->ec_data_shards = 0;
    pi->ec_parity_shards = 0;

    /* K2: Inline 数据缓冲初始化 (默认无 inline_data, GETATTR 时按需分配) */
    pi->inline_data = NULL;
    pi->inline_len = 0;
    pi->inline_max_size = 0;  /* GETATTR 响应覆盖, 或 apply_layout 时设默认值 */
    pi->inline_dirty = false;

    /* 异步 setattr work (writeback offload) */
    INIT_WORK(&pi->setattr_work, powerfs_setattr_work_fn);
    pi->setattr_pending = false;

    /* writeback 互斥: 防止并发 RMW 数据覆盖 */
    mutex_init(&pi->wb_mutex);
    atomic_set(&pi->wb_batch_count, 0);

    /* 初始化目录缓存字段.
     * MUST reset dir_complete/dir_lease_expire/dir_lease_epoch on every
     * alloc_inode: the slab constructor (powerfs_inode_init_once) runs only
     * on first slab page creation, NOT on object reuse. Without this reset,
     * a reused inode inherits stale dir_complete=true + future
     * dir_lease_expire from the previous inode, causing readdir to skip the
     * Filer fetch and emit an empty dir_entries list (T3b/T3c/T9a/T9b
     * "Directory not empty" failures after remount). */
    INIT_LIST_HEAD(&pi->dir_entries);
    mutex_init(&pi->dir_mutex);
    WRITE_ONCE(pi->dir_complete, false);
    WRITE_ONCE(pi->dir_lease_expire, 0);
    pi->dir_lease_epoch = 0;

    /* xattr 存储 (simple_xattr in-memory) — stubbed: running kernel
     * doesn't EXPORT_SYMBOL simple_xattr* functions. xattrs are
     * in-memory only (not persisted to Filer), so stubbing is safe. */
    memset(&pi->xattrs, 0, sizeof(pi->xattrs));

    pr_debug("powerfs: alloc_inode (pi=%p, inode=%p)\n", pi, &pi->netfs.inode);

    return &pi->netfs.inode;
}

/*
 * powerfs_free_inode - 释放 inode (super_operations)
 *
 * 参考 ceph_free_inode (fs/ceph/inode.c)
 * 只释放私有数据，inode 核心由 VFS 管理
 */
void powerfs_free_inode(struct inode *inode)
{
    struct powerfs_inode_info *pi = POWERFS_I(inode);

    pr_debug("powerfs: free_inode ino=%lu (pi=%p)\n", inode->i_ino, pi);

    /* 释放 chunk 映射 */
    kfree(pi->chunks);
    pi->chunks = NULL;

    /* K2: 释放 Inline 数据缓冲 */
    kfree(pi->inline_data);
    pi->inline_data = NULL;
    pi->inline_len = 0;
    pi->inline_dirty = false;

    /* K3-1: 释放 Stripe volume_ids 数组 */
    kfree(pi->volume_ids);
    pi->volume_ids = NULL;
    pi->volume_ids_count = 0;

    /* K4: 释放副本 chunk 列表 */
    kfree(pi->replica_chunks);
    pi->replica_chunks = NULL;
    pi->replica_count = 0;

    /* K4-5: 释放 EC shards 列表 */
    kfree(pi->ec_chunks);
    pi->ec_chunks = NULL;
    pi->ec_chunk_count = 0;

    /* xattr 存储: 释放所有 xattr 条目 — stubbed (see alloc_inode) */
    /* simple_xattrs_free(&pi->xattrs, NULL); — no-op */

    kmem_cache_free(powerfs_inode_cachep, pi);
}

/*
 * powerfs_evict_inode - 驱逐 inode (super_operations)
 *
 * 参考 ceph_evict (fs/ceph/inode.c)
 *
 * 标准流程:
 *   1. truncate_inode_pages_final - 清理页面缓存
 *   2. clear_inode - 清除 inode 核心
 *   3. 清理文件系统私有资源
 */
void powerfs_evict_inode(struct inode *inode)
{
    struct powerfs_inode_info *pi = POWERFS_I(inode);

    pr_debug("powerfs: evict_inode ino=%lu\n", inode->i_ino);

    /* 1. 先截断 page cache (参考 ceph_evict_inode) */
    truncate_inode_pages_final(&inode->i_data);

    /* 2. 取消后台 lease 续约 work 和异步 setattr work
     *    （必须在 clear_inode 之前，因为 work 可能引用 inode）
     *    参考 ceph_evict_inode: cancel_writeback 是在 clear_inode 之前 */
    cancel_delayed_work_sync(&pi->lease_renew_work);
    cancel_work_sync(&pi->setattr_work);

    /* 3. 释放所有 lease (通知 volume server, 必须在 clear_inode 之前) */
    release_all_leases(inode);

    /* 4. VFS 清理 */
    clear_inode(inode);

    /* 5. 清理残留 lease (release_all_leases 批量限制, 可能有剩余) */
    while (!RB_EMPTY_ROOT(&pi->lease_tree)) {
        struct rb_node *n = rb_first(&pi->lease_tree);
        rb_erase(n, &pi->lease_tree);
        kfree(rb_entry(n, struct powerfs_lease, node));
    }

    /* 释放动态分配的布局数据. 持锁防止与 refresh_work/apply_layout 竞争.
     * 修复 crash: 连接断开时 refresh_work getattr 失败 → iput → evict_inode,
     * 若不持锁, apply_layout 可能并发释放/替换指针, 导致 kfree 无效地址. */
    spin_lock(&pi->i_lock);

    /* 释放 chunk 映射 */
    if (pi->chunks && virt_addr_valid(pi->chunks))
        kfree(pi->chunks);
    pi->chunks = NULL;
    pi->chunk_count = 0;

    /* K3-1: 释放 Stripe volume_ids 数组 (evict 时释放, 避免 slab 重分配后悬挂) */
    if (pi->volume_ids && virt_addr_valid(pi->volume_ids))
        kfree(pi->volume_ids);
    pi->volume_ids = NULL;
    pi->volume_ids_count = 0;
    pi->stripe_size = 0;
    pi->stripe_count = 0;

    /* K4: 释放副本 chunk 列表 */
    if (pi->replica_chunks && virt_addr_valid(pi->replica_chunks))
        kfree(pi->replica_chunks);
    pi->replica_chunks = NULL;
    pi->replica_count = 0;

    /* K4-5: 释放 EC shards 列表 */
    if (pi->ec_chunks && virt_addr_valid(pi->ec_chunks))
        kfree(pi->ec_chunks);
    pi->ec_chunks = NULL;
    pi->ec_chunk_count = 0;

    /* K2: 释放 inline_data (之前遗漏, 导致内存泄漏) */
    if (pi->inline_data && virt_addr_valid(pi->inline_data))
        kfree(pi->inline_data);
    pi->inline_data = NULL;
    pi->inline_len = 0;
    pi->inline_dirty = false;

    /* 7. 清理状态 (仍在 i_lock 下) */
    pi->cache_valid = false;
    pi->dir_complete = false;
    /* Phase 1: 清目录 lease, 防止 inode 复用 (slab 重分配) 后误命中旧 lease. */
    pi->dir_lease_expire = 0;
    pi->dir_lease_epoch = 0;
    pi->shutdown = true;
    spin_unlock(&pi->i_lock);

    /* 6. 清理目录缓存链表 (使用 dir_mutex, 不在 i_lock 下) */
    powerfs_clear_dir_entries(inode);
}

/*
 * powerfs_setattr_work_fn - 异步 setattr work 函数 (WQ_UNBOUND 上下文)
 *
 * 在 writeback_wq (WQ_UNBOUND) 中执行, 不阻塞 per-CPU writeback workqueue.
 * 读取最新 i_size (可能已有多次 writeback 累积), 调用 powerfs_net_setattr
 * 同步到 Filer, 成功后更新 pi->content_size.
 */
static void powerfs_setattr_work_fn(struct work_struct *work)
{
    struct powerfs_inode_info *pi = container_of(work,
                                                   struct powerfs_inode_info,
                                                   setattr_work);
    struct inode *inode = &pi->netfs.inode;
    u64 i_size;
    int ret;

    if (powerfs_net_is_stopping())
        goto out;

    i_size = i_size_read(inode);
    if (i_size == 0)
        goto out;

    ret = powerfs_net_setattr(inode->i_ino, POWERFS_ATTR_SIZE,
                               0, 0, 0, i_size, 0, 0);
    if (ret < 0) {
        pr_warn("powerfs: async setattr ino=%lu size=%llu failed: %d\n",
                inode->i_ino, i_size, ret);
        goto out;  /* content_size 不更新, 下次 writeback 重试 */
    }

    spin_lock(&pi->i_lock);
    pi->content_size = i_size;
    spin_unlock(&pi->i_lock);

out:
    spin_lock(&pi->i_lock);
    pi->setattr_pending = false;
    spin_unlock(&pi->i_lock);
}

/*
 * powerfs_write_inode - VFS writeback 时同步 inode 元数据到 Filer
 *
 * 作用:
 *   write_end 更新内存 inode 的 i_size, writepage 只刷数据页, 二者都
 *   不会把 i_size 元数据同步到 Filer. 若不同步, sync/writeback 后 Filer
 *   端 i_size 仍为 0, remount 后 lookup 返回 size=0, 文件内容丢失.
 *
 * 触发时机:
 *   - sync(2) / sync 命令 (sync_filesystem -> writeback_inodes_sb)
 *   - fsync(2) (file_write_and_wait_range -> __writeback_single_inode)
 *   - 内存压力下的 writeback
 *
 * 优化:
 *   用 pi->content_size 跟踪上次成功同步的 size, 仅在 i_size 变化时
 *   发送 setattr, 避免每次 writeback 都网络往返.
 *
 * WB_SYNC_ALL (fsync/sync): 调用方进程上下文, 可安全同步等待网络.
 * WB_SYNC_NONE (背景 writeback): 内核 per-CPU writeback workqueue,
 *   必须 offload 到 WQ_UNBOUND, 否则同步网络调用阻塞 per-CPU worker
 *   导致 workqueue lockup (实测 598s).
 *
 * 参考: ceph_write_inode (fs/ceph/inode.c) 同步 caps 模式.
 */
static int powerfs_write_inode(struct inode *inode, struct writeback_control *wbc)
{
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    struct powerfs_sb_info *sbi = POWERFS_SB_INFO(inode->i_sb);
    loff_t i_size;
    u64 last_synced;

    if (!S_ISREG(inode->i_mode))
        return 0;

    /* umount 期间 (stopping=1): 跳过网络同步, 返回 0 允许 inode 驱逐.
     * 否则 writeback 会 redirty, evict_inodes 无法驱逐, umount 挂起. */
    if (powerfs_net_is_stopping())
        return 0;

    i_size = i_size_read(inode);
    if (i_size == 0)
        return 0;

    /* 仅在 i_size 变化时同步 */
    spin_lock(&pi->i_lock);
    last_synced = pi->content_size;
    spin_unlock(&pi->i_lock);

    if ((u64)i_size == last_synced)
        return 0;

    if (wbc->sync_mode == WB_SYNC_ALL && !(current->flags & PF_WQ_WORKER)) {
        /* fsync: 调用方进程上下文 (非 writeback workqueue), 可安全同步等待网络.
         * 注意: sync(2) 会在 per-CPU writeback 线程中触发 WB_SYNC_ALL,
         * 此时不能阻塞 (PF_WQ_WORKER), 走 offload 路径. */
        int ret = powerfs_net_setattr(inode->i_ino, POWERFS_ATTR_SIZE,
                                       0, 0, 0, (__u64)i_size, 0, 0);
        if (ret < 0) {
            pr_warn("powerfs: write_inode sync setattr ino=%lu size=%llu failed: %d\n",
                    inode->i_ino, (u64)i_size, ret);
            return ret;
        }
        spin_lock(&pi->i_lock);
        pi->content_size = (u64)i_size;
        spin_unlock(&pi->i_lock);
    } else {
        /* WB_SYNC_NONE (背景 writeback): 内核 per-CPU writeback workqueue,
         * 必须不能阻塞 — offload 到 WQ_UNBOUND (sbi->writeback_wq).
         * 若已有 pending work, 跳过 (work 函数会读取最新 i_size).
         * 返回 0 (不 redirty), setattr 失败时 content_size 不更新, 下次重试. */
        bool already_pending;

        spin_lock(&pi->i_lock);
        already_pending = pi->setattr_pending;
        if (!already_pending)
            pi->setattr_pending = true;
        spin_unlock(&pi->i_lock);

        if (!already_pending)
            queue_work(sbi->writeback_wq, &pi->setattr_work);
    }

    return 0;
}

/* Phase 3: lease 续约 work 函数.
 *
 * 每次触发时扫描 inode 的 lease_tree, 对即将过期的 lease (剩余有效期 <
 * POWERFS_LEASE_RENEW_THRESHOLD) 发送续约请求.
 *
 * 设计要点 (参考 Ceph cap renew + GFS2 glock queue_delayed_work):
 *   - delayed_work 按 inode 独立调度, 不同 inode 的续约互不阻塞
 *   - 两阶段: 锁内收集待续约 lease → 解锁逐个续约 → 重新加锁更新
 *   - 续约失败不删除 lease: lease 自然过期后由 acquire 路径重新获取
 *   - shutting_down 时停止重调度, 避免 destroy_workqueue flush 循环
 *
 * 调度策略:
 *   - 下次触发 = earliest_expiry - RENEW_THRESHOLD
 *   - 最小间隔 1s, 避免过于频繁的重调度
 *
 * 批量大小: POWERFS_LEASE_RENEW_BATCH 限制单次收集的 lease 数.
 *   一个 inode 的 lease 数 = file_size / STRIPE_SIZE, 多数文件 1 个,
 *   1GB 文件 16 个. 批量不够时剩余 lease 下次 work 触发时续约. */
#define POWERFS_LEASE_RENEW_BATCH  16

/*
 * Lease 管理函数 (Phase 3: 强一致性写)
 *
 * ensure_lease: 写路径中获取 per-stripe lease (Follower→Leader)
 * release_lease: close 时释放单个 stripe lease
 * release_all_leases: inode 销毁时释放所有 lease
 */

/* 在 lease_tree 中查找 stripe_start 对应的 lease (调用方持有 lease_lock) */
static struct powerfs_lease *powerfs_lease_find(struct powerfs_inode_info *pi,
                                                 u64 stripe_start)
{
    struct rb_node *n = pi->lease_tree.rb_node;
    while (n) {
        struct powerfs_lease *l = rb_entry(n, struct powerfs_lease, node);
        if (stripe_start < l->stripe_start)
            n = n->rb_left;
        else if (stripe_start > l->stripe_start)
            n = n->rb_right;
        else
            return l;
    }
    return NULL;
}

/* 将 lease 插入 lease_tree (调用方持有 lease_lock) */
static void powerfs_lease_insert(struct powerfs_inode_info *pi,
                                  struct powerfs_lease *lease)
{
    struct rb_node **p = &pi->lease_tree.rb_node;
    struct rb_node *parent = NULL;
    while (*p) {
        struct powerfs_lease *l = rb_entry(*p, struct powerfs_lease, node);
        parent = *p;
        if (lease->stripe_start < l->stripe_start)
            p = &(*p)->rb_left;
        else if (lease->stripe_start > l->stripe_start)
            p = &(*p)->rb_right;
        else
            return; /* 已存在, 不插入 */
    }
    rb_link_node(&lease->node, parent, p);
    rb_insert_color(&lease->node, &pi->lease_tree);
}

/*
 * ensure_lease - 确保持有指定 offset 所在 stripe 的 lease
 *
 * 写路径调用: 在写入数据前获取 lease, 保证强一致性.
 * stripe_start = (offset / STRIPE_SIZE) * STRIPE_SIZE
 * 如果 lease 已存在且未过期, 直接返回 (快速路径).
 * 如果 lease 已过期或不存在, 向 volume server 请求获取.
 *
 * 返回 0 成功, 负值错误.
 */
static int ensure_lease(struct inode *inode, loff_t offset)
{
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    struct super_block *sb = inode->i_sb;
    struct powerfs_sb_info *sbi = sb ? POWERFS_SB_INFO(sb) : NULL;
    u64 stripe_start = (offset / POWERFS_STRIPE_SIZE) * POWERFS_STRIPE_SIZE;
    struct powerfs_lease *lease, *old = NULL;
    unsigned long now = jiffies;
    size_t token_len;
    int ret;

    /* 快速路径: 持有有效 lease */
    spin_lock(&pi->lease_lock);
    lease = powerfs_lease_find(pi, stripe_start);
    if (lease && time_after(lease->expire_jiffies, now)) {
        spin_unlock(&pi->lease_lock);
        return 0;
    }
    /* 过期 lease: 移除并稍后释放 */
    if (lease) {
        rb_erase(&lease->node, &pi->lease_tree);
        old = lease;
    }
    spin_unlock(&pi->lease_lock);

    /* 锁外释放过期 lease (网络操作) */
    if (old) {
        powerfs_net_release_lease(pi->volume_id, inode->i_ino,
                                   old->token, strlen(old->token), NULL);
        kfree(old);
    }

    /* 分配新 lease */
    lease = kzalloc(sizeof(*lease), GFP_NOFS);
    if (!lease)
        return -ENOMEM;

    lease->stripe_start = stripe_start;
    lease->stripe_count = 1;
    lease->exclusive = true;
    token_len = sizeof(lease->token);

    /* 向 volume server 请求 lease (网络操作, 锁外) */
    ret = powerfs_net_acquire_lease(pi->volume_id, inode->i_ino,
                                     stripe_start, 1, NULL,
                                     lease->token, &token_len,
                                     &lease->epoch, &lease->content_size,
                                     &lease->expire_jiffies);
    if (ret) {
        pr_warn("powerfs: ensure_lease failed ino=%lu stripe=%llu: %d\n",
                inode->i_ino, (unsigned long long)stripe_start, ret);
        kfree(lease);
        return ret;
    }

    /* 插入 lease_tree */
    spin_lock(&pi->lease_lock);
    /* 如果在此期间有人插入了相同 stripe 的 lease, 保留旧的 */
    if (!powerfs_lease_find(pi, stripe_start)) {
        powerfs_lease_insert(pi, lease);
        lease = NULL; /* 被树接管 */
    }
    spin_unlock(&pi->lease_lock);

    if (lease) {
        /* 竞争失败, 释放多余的 lease */
        powerfs_net_release_lease(pi->volume_id, inode->i_ino,
                                   lease->token, strlen(lease->token), NULL);
        kfree(lease);
        lease = NULL;  /* 置 NULL, 避免悬空指针检查 */
    }

    /* 调度续约工作 (仅在新 lease 成功插入时启动).
     * delay = LEASE_DURATION - RENEW_THRESHOLD (20s), 在过期前 10s 续约.
     * queue_delayed_work 不会重复入队: 若 work 已 pending, 返回 false. */
    if (sbi && sbi->lease_wq && !sbi->shutting_down) {
        unsigned long delay = POWERFS_LEASE_DURATION - POWERFS_LEASE_RENEW_THRESHOLD;
        queue_delayed_work(sbi->lease_wq, &pi->lease_renew_work, delay);
    }

    return 0;
}

/*
 * powerfs_get_lease_token - 获取指定 offset 所在 stripe 的 lease token
 *
 * writeback 路径辅助函数: 从 lease_lock 下读取已持有的 token.
 *
 * 重要修复: 不再调用 ensure_lease (同步网络调用 powerfs_net_acquire_lease).
 * writeback 工作队列 (powerfs_wb) 禁止阻塞在网络 I/O 上, 否则会导致:
 *   - workqueue lockup (实测 679s+)
 *   - wb_workfn 在 powerfs_write_inode spin_lock 上死等
 *   - sync(2) 进入 D 状态永久卡死
 *   - RCU stall
 *
 * lease 获取由 open/write 路径预先完成, writeback 时 lease 应已持有.
 * 若 lease 未持有或已过期, 返回 -ENOENT, writeback 继续无 lease 写入
 * (Volume Server 容许无 lease 写入, lease_token 为可选字段).
 *
 * 返回 0 成功 (token/token_len 已填充), 负值错误 (-ENOENT=无 lease).
 */
static int powerfs_get_lease_token(struct inode *inode, loff_t offset,
                                    char *token, size_t *token_len)
{
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    u64 stripe_start = (offset / POWERFS_STRIPE_SIZE) * POWERFS_STRIPE_SIZE;
    struct powerfs_lease *l;

    spin_lock(&pi->lease_lock);
    l = powerfs_lease_find(pi, stripe_start);
    if (l && time_after(l->expire_jiffies, jiffies)) {
        strncpy(token, l->token, 63);
        token[63] = '\0';
        *token_len = strlen(token);
        spin_unlock(&pi->lease_lock);
        return 0;
    }
    spin_unlock(&pi->lease_lock);
    *token_len = 0;
    return -ENOENT;
}

/*
 * release_all_leases - 释放 inode 的所有 lease (close/evict 时调用)
 *
 * 锁内收集 lease 列表, 锁外逐个释放 (网络操作).
 */
static void release_all_leases(struct inode *inode)
{
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    struct powerfs_lease *leases[POWERFS_LEASE_RENEW_BATCH];
    int count = 0, i;
    struct rb_node *n;

    /* 收集所有 lease (锁内) */
    spin_lock(&pi->lease_lock);
    while (!RB_EMPTY_ROOT(&pi->lease_tree) && count < POWERFS_LEASE_RENEW_BATCH) {
        n = rb_first(&pi->lease_tree);
        rb_erase(n, &pi->lease_tree);
        leases[count++] = rb_entry(n, struct powerfs_lease, node);
    }
    spin_unlock(&pi->lease_lock);

    /* 释放 lease (锁外, 网络操作) */
    for (i = 0; i < count; i++) {
        powerfs_net_release_lease(pi->volume_id, inode->i_ino,
                                   leases[i]->token,
                                   strlen(leases[i]->token), NULL);
        kfree(leases[i]);
    }
}

static void powerfs_lease_renew_work_func(struct work_struct *work)
{
    struct delayed_work *dw = to_delayed_work(work);
    struct powerfs_inode_info *pi =
        container_of(dw, struct powerfs_inode_info, lease_renew_work);
    struct inode *inode = &pi->netfs.inode;
    struct super_block *sb = inode->i_sb;
    struct powerfs_sb_info *sbi = sb ? POWERFS_SB_INFO(sb) : NULL;
    struct rb_node *n;
    unsigned long now = jiffies;
    unsigned long earliest_expiry = 0;
    bool has_lease = false;

    /* 待续约 lease 信息 (锁内收集, 锁外续约) */
    struct {
        u64 stripe_start;
        char token[64];
        size_t token_len;
    } renew_list[POWERFS_LEASE_RENEW_BATCH];
    int renew_count = 0;
    u64 volume_id;
    int i;

    /* 卸载中或 workqueue 已销毁: 停止续约, 不重调度 */
    if (!sbi || !sbi->lease_wq || sbi->shutting_down || pi->shutdown)
        return;

    /* Phase 1: 锁内扫描, 收集需要续约的 lease 信息 + 跟踪最早过期时间.
     * 不在锁内执行网络 I/O: spinlock 不可睡眠, 网络 RPC 可阻塞 5s. */
    spin_lock(&pi->lease_lock);
    volume_id = pi->volume_id;
    for (n = rb_first(&pi->lease_tree); n; n = rb_next(n)) {
        struct powerfs_lease *lease =
            rb_entry(n, struct powerfs_lease, node);

        has_lease = true;

        /* 跟踪最早过期时间 (用于重调度) */
        if (earliest_expiry == 0 ||
            time_before(lease->expire_jiffies, earliest_expiry))
            earliest_expiry = lease->expire_jiffies;

        /* 检查是否需要续约: 剩余有效期 < RENEW_THRESHOLD */
        if (time_after(now + POWERFS_LEASE_RENEW_THRESHOLD,
                       lease->expire_jiffies)) {
            if (renew_count < POWERFS_LEASE_RENEW_BATCH) {
                renew_list[renew_count].stripe_start = lease->stripe_start;
                memcpy(renew_list[renew_count].token, lease->token, 64);
                renew_list[renew_count].token_len =
                    strnlen(renew_list[renew_count].token, 64);
                renew_count++;
            }
            /* 批量满时剩余 lease 下次 work 处理 */
        }
    }
    spin_unlock(&pi->lease_lock);

    /* Phase 2: 锁外逐个发送续约请求 (网络 I/O 可睡眠) */
    for (i = 0; i < renew_count; i++) {
        unsigned long new_expire = 0;
        int ret;

        ret = powerfs_net_renew_lease(volume_id, inode->i_ino,
                                      renew_list[i].token,
                                      renew_list[i].token_len,
                                      &new_expire);
        if (ret == 0) {
            /* Phase 3: 重新加锁, 按 stripe_start 查找并更新 */
            spin_lock(&pi->lease_lock);
            for (n = rb_first(&pi->lease_tree); n; n = rb_next(n)) {
                struct powerfs_lease *l =
                    rb_entry(n, struct powerfs_lease, node);
                if (l->stripe_start == renew_list[i].stripe_start) {
                    l->expire_jiffies = new_expire;
                    pr_debug("powerfs: lease renewed ino=%lu stripe=%llu\n",
                             inode->i_ino,
                             (unsigned long long)l->stripe_start);
                    break;
                }
            }
            spin_unlock(&pi->lease_lock);
        } else {
            /* 续约失败: 可能是连接断开导致 volume server 释放了 lease
             * (on_disconnect → disconnect_holder), 或者 lease 已过期被
             * cleanup_expired 清理. 重新获取 lease (acquire_lease). */
            char new_token[64];
            size_t new_token_len = sizeof(new_token);
            u64 new_epoch = 0;
            u64 new_content_size = 0;
            unsigned long new_expire = 0;
            int acq_ret;

            pr_warn("powerfs: lease renew failed ino=%lu stripe=%llu "
                    "err=%d, re-acquiring...\n",
                    inode->i_ino,
                    (unsigned long long)renew_list[i].stripe_start,
                    ret);

            acq_ret = powerfs_net_acquire_lease(volume_id, inode->i_ino,
                                                renew_list[i].stripe_start,
                                                1, NULL,
                                                new_token, &new_token_len,
                                                &new_epoch, &new_content_size,
                                                &new_expire);
            if (acq_ret == 0) {
                /* 重新获取成功: 更新 lease token 和 expire */
                spin_lock(&pi->lease_lock);
                for (n = rb_first(&pi->lease_tree); n; n = rb_next(n)) {
                    struct powerfs_lease *l =
                        rb_entry(n, struct powerfs_lease, node);
                    if (l->stripe_start == renew_list[i].stripe_start) {
                        size_t copy_len = min(new_token_len,
                                              sizeof(l->token) - 1);
                        memcpy(l->token, new_token, copy_len);
                        l->token[copy_len] = '\0';
                        l->epoch = new_epoch;
                        l->content_size = new_content_size;
                        l->expire_jiffies = new_expire;
                        pr_info("powerfs: lease re-acquired ino=%lu "
                                "stripe=%llu\n",
                                inode->i_ino,
                                (unsigned long long)l->stripe_start);
                        break;
                    }
                }
                spin_unlock(&pi->lease_lock);
            } else {
                pr_warn("powerfs: lease re-acquire failed ino=%lu "
                        "stripe=%llu err=%d\n",
                        inode->i_ino,
                        (unsigned long long)renew_list[i].stripe_start,
                        acq_ret);
            }
        }
    }

    /* 重调度: 在最早过期时间前 RENEW_THRESHOLD 触发下次续约.
     * 若没有 lease, 不重调度 (由 acquire 路径重新启动). */
    if (has_lease && !sbi->shutting_down && sbi->lease_wq) {
        unsigned long delay;

        if (time_before(now + POWERFS_LEASE_RENEW_THRESHOLD,
                        earliest_expiry)) {
            /* lease 还远未到续约时间 */
            delay = earliest_expiry - POWERFS_LEASE_RENEW_THRESHOLD - now;
        } else {
            /* lease 已在续约窗口内或已过期, 短延迟后重试 */
            delay = HZ;  /* 1s */
        }

        queue_delayed_work(sbi->lease_wq, &pi->lease_renew_work, delay);
    }
}

/* ========== Inode 创建辅助函数 ========== */

/*
 * powerfs_new_inode - 创建新的 inode
 *
 * 内部改用 powerfs_iget (基于 iget5_locked)
 * 这样 inode 会被加入内核 inode 哈希表，可以通过 ino 查找
 *
 * 注意: 调用者必须确保 ino 未被使用
 */
struct inode *powerfs_new_inode(struct super_block *sb, umode_t mode,
                                 u64 ino, u64 parent_ino, const char *name)
{
    struct inode *inode;
    int err;

    inode = powerfs_iget(sb, ino);
    if (IS_ERR(inode))
        return NULL;

    /* 如果是新创建的 inode，初始化它 */
    if (inode->i_state & I_NEW) {
        err = powerfs_init_inode(inode, mode, parent_ino, name);
        if (err) {
            iput(inode);
            return NULL;
        }
        unlock_new_inode(inode);
    } else {
        /* inode 已存在 - 这不应该发生在创建场景 */
        pr_warn("powerfs: new_inode called for existing inode %llu\n", ino);
        iput(inode);
        return NULL;
    }

    pr_debug("powerfs: new_inode ino=%llu mode=%o (%s)\n",
             ino, mode, name ? name : "");

    return inode;
}

/* ========== 目录操作: lookup ========== */

/*
 * powerfs_lookup - 查找文件/目录
 *
 * 策略:
 *   1. 快速路径: dentry 已有 inode (在 dcache 中)，直接返回
 *   2. 通信层可用: 通过代理查询后端 Filer
 *      - 找到: 创建 inode 并实例化 dentry
 *      - 未找到: 添加负 dentry
 *   3. 通信层不可用: 纯本地模式，添加负 dentry
 *
 * 注意: 不在 RCU read-side critical section 中做任何阻塞操作
 */
/*
 * powerfs_lookup - 查找文件/目录
 *
 * 策略 (更新版 - Delta Sync + powerfs_net):
 *   1. 快速路径: dentry 已有 inode (在 dcache 中)
 *   2. powerfs_net 连接可用: 直接通过 powerfs_net_lookup 查询
 *      - 找到: 创建 inode，设置 generation，记录路径
 *      - 未找到: 添加负 dentry
 *   3. powerfs_comm 连接可用 (兼容旧接口): 使用旧代理查询
 *   4. 纯本地模式: 添加负 dentry
 */
struct dentry *powerfs_lookup(struct inode *dir, struct dentry *dentry,
                               unsigned int flags)
{
    struct powerfs_sb_info *sbi = POWERFS_SB_INFO(dir->i_sb);
    struct inode *inode;
    int err;
    u64 ts_entry, ts_net, net_dur_us, total_us;

    (void)flags;

    ts_entry = ktime_get_ns();

    pr_debug("powerfs: lookup '%pd' in dir=%lu\n", dentry, dir->i_ino);

    /*
     * VFS 契约: ->lookup 仅在 dentry 处于 DCACHE_PAR_LOOKUP (新鲜、
     * d_inode==NULL、d_alias/d_in_lookup_hash 共享 union 且当前由
     * in_lookup_hash 占用) 时调用 (见 namei.c __lookup_slow/__lookup_hash)。
     * 因此这里不需要、也不应该检查 d_inode(dentry) —— 如果它非空，
     * 说明 VFS 已被破坏，提前返回 NULL 只会把 dentry 留在 PAR_LOOKUP
     * 悬挂状态，进一步污染 dcache。交由 d_add 走正常实例化路径。
     */

    /* === powerfs_net 直接通信模式 === */
    if (powerfs_net_is_connected()) {
        __u64 ino = 0;
        __u32 mode = 0;
        __u32 uid = 0, gid = 0;
        __u64 size = 0;
        __u32 nlink = 0;
        __u64 mtime = 0, atime = 0, ctime = 0;
        __u64 volume_id = 0, file_key = 0;
        struct powerfs_file_layout lookup_layout;
        int timeout_ms;

        /* K3: 零初始化, 确保 err/ENOENT 路径 kfree(volume_ids=NULL) 安全 */
        memset(&lookup_layout, 0, sizeof(lookup_layout));

        pr_debug("powerfs: lookup '%pd' via powerfs_net\n", dentry);

        /* Phase 1: 超时策略.
         *
         * lookup 在 VFS 持有 dir->i_rwsem 读锁时调用, 长超时会阻塞
         * 整个目录的所有操作. 使用固定 2s 短超时:
         *   - 正常 RPC 在 LAN 内 <10ms, 2s 足够
         *   - 断连/filer 重启时快速返回 -EAGAIN, VFS 上层自动重试
         *   - 不使用 powerfs_net_pick_timeout (自适应 10s), 因为
         *     lookup 在 i_rwsem 内, 必须快速释放锁
         *
         * 重试由 VFS 上层处理 (namei.c __lookup_slow 循环),
         * 每次重试间隔由 VFS 调度, 提供自然退避. */
        timeout_ms = POWERFS_LOOKUP_TIMEOUT_MS;  /* 2s: short timeout under i_rwsem */

        /* 通过 powerfs_net 直接查询 (含时间戳 + volume_id/file_key + layout) */
        ts_net = ktime_get_ns();
        err = powerfs_net_lookup_timeout(dir->i_ino, dentry->d_name.name,
                                          strlen(dentry->d_name.name),
                                          &ino, &mode, &uid, &gid,
                                          &size, &nlink,
                                          &mtime, &atime, &ctime,
                                          &volume_id, &file_key,
                                          &lookup_layout,
                                          timeout_ms);
        net_dur_us = div_u64(ktime_get_ns() - ts_net, 1000);

        /* 断连/重连期间超时或网络不可达, 返回 -EAGAIN 让 VFS/应用层重试.
         * - ETIMEDOUT: 2s 内未收到响应 (filer 不可达或繁忙)
         * - ENOTCONN: disconnect_one 已 complete(-ENOTCONN) (在途请求被取消)
         * - ESHUTDOWN: pool 正在 stopping
         *
         * 2s 短超时策略: 不等待 filer 重启完成, 快速返回 -EAGAIN 让 VFS 重试.
         * VFS 的 __lookup_slow 循环会自动重试, 每次重试间隔由调度器决定,
         * filer 恢复后下一次 lookup 即可成功. 避免在 i_rwsem 读锁内长时间阻塞. */
        if (err == -ETIMEDOUT || err == -ENOTCONN || err == -ESHUTDOWN) {
            total_us = div_u64(ktime_get_ns() - ts_entry, 1000);
            pr_warn("powerfs: LOOKUP '%pd' EAGAIN err=%d net=%lluus total=%lluus\n",
                    dentry, err, net_dur_us, total_us);
            return ERR_PTR(-EAGAIN);
        }

        if (err == 0 && ino != 0) {
            /* 找到文件: 创建 inode */
            pr_debug("powerfs: lookup '%pd' found ino=%llu mode=%o\n",
                     dentry, (unsigned long long)ino, mode);

            inode = powerfs_iget(dir->i_sb, ino);
            if (IS_ERR(inode)) {
                pr_warn("powerfs: lookup '%pd' iget failed: %ld\n",
                        dentry, PTR_ERR(inode));
                /* K3: iget 失败, 释放 parse 分配的 volume_ids/inline_data/replica_chunks */
                kfree(lookup_layout.volume_ids);
                kfree(lookup_layout.inline_data);
                kfree(lookup_layout.replica_chunks);
                kfree(lookup_layout.ec_chunks);
                d_add(dentry, NULL);
                return NULL;
            }

            if (inode->i_state & I_NEW) {
                /*
                 * 新 inode: 先用 powerfs_init_inode 设置 i_op/i_fop/a_ops
                 * 和默认属性。缺少这一步会导致 looked-up inode 的操作表为
                 * NULL，后续 open/read 等触发空指针解引用。
                 */
                powerfs_init_inode(inode, mode, dir->i_ino,
                                   dentry->d_name.name);
                /* 用 Filer 返回的权威属性覆盖默认值 */
                inode->i_mode = mode;
                inode->i_uid = make_kuid(&init_user_ns, uid);
                inode->i_gid = make_kgid(&init_user_ns, gid);
                inode->i_size = size;
                set_nlink(inode, nlink);
                inode_set_mtime(inode, mtime, 0);
                inode_set_atime(inode, atime, 0);
                inode_set_ctime(inode, ctime, 0);

                {
                    struct powerfs_inode_info *pi = POWERFS_I(inode);
                    spin_lock(&pi->i_lock);
                    pi->cache_valid = true;
                    /* 数据直连: 存储 volume_id/file_key 用于 ReadNeedle/WriteNeedle.
                     * 目录的 volume_id/file_key 为 0 (无数据). */
                    pi->volume_id = volume_id;
                    pi->file_key = file_key;
                    /* K3: 应用 FileLayout (placement/volume_ids 等).
                     * Stripe 文件在 lookup 时即获取 volume_ids, 无需等待 getattr. */
                    powerfs_apply_layout_to_inode(pi, &lookup_layout);
                    spin_unlock(&pi->i_lock);
                }

                unlock_new_inode(inode);
            } else {
                /* 已有 inode (非 I_NEW): d_revalidate 返回 0 触发 re-lookup.
                 * 用 Filer 返回的权威属性更新现有 inode, 否则跨客户端
                 * 修改后内核仍用旧的 i_size/volume_id/file_key, 导致
                 * 读取空内容 (size=0 跳过 read) 或 needle not found. */
                spin_lock(&inode->i_lock);
                inode->i_mode = mode;
                inode->i_uid = make_kuid(&init_user_ns, uid);
                inode->i_gid = make_kgid(&init_user_ns, gid);
                if (i_size_read(inode) != size) {
                    i_size_write(inode, size);
                }
                set_nlink(inode, nlink);
                inode_set_mtime(inode, mtime, 0);
                inode_set_atime(inode, atime, 0);
                inode_set_ctime(inode, ctime, 0);
                spin_unlock(&inode->i_lock);

                {
                    struct powerfs_inode_info *pi = POWERFS_I(inode);
                    spin_lock(&pi->i_lock);
                    pi->cache_valid = true;
                    pi->cache_expire = jiffies + POWERFS_INODE_CACHE_TTL;
                    /* 仅在 Filer 返回非零值时更新 volume_id/file_key.
                     * close 前的 lookup 可能返回 0 (Filer 端 chunks 在 close 时才同步),
                     * 若用 0 覆盖已有值, 会导致 writeback/read locate 失败. */
                    if (volume_id != 0)
                        pi->volume_id = volume_id;
                    if (file_key != 0)
                        pi->file_key = file_key;
                    /* K3: 更新 FileLayout (placement 可能从 Flat 切换到 Stripe) */
                    powerfs_apply_layout_to_inode(pi, &lookup_layout);
                    spin_unlock(&pi->i_lock);
                }
                pr_debug("powerfs: lookup '%pd' updated existing inode ino=%llu size=%llu vid=%llu fkey=%llu\n",
                        dentry, (unsigned long long)ino,
                        (unsigned long long)size,
                        (unsigned long long)volume_id,
                        (unsigned long long)file_key);
            }

            /*
             * 实例化 dentry —— 必须使用 d_add，不能用 d_instantiate。
             *
             * 根因: ->lookup 期间 dentry 仍处于 DCACHE_PAR_LOOKUP，
             * d_alias 与 d_in_lookup_hash 共享 union (dcache.h:109-112)，
             * 此时 in_lookup_hash 已链入 in_lookup 哈希表，故 d_alias 也
             * 表现为 "hashed"。d_instantiate 直接 BUG_ON(!hlist_unhashed
             * (&d_alias)) 即触发 kernel BUG (dcache.c:2032)。
             *
             * d_add -> __d_add (dcache.c:2775) 先 d_in_lookup 检测并
             * __d_lookup_unhash 清理 in_lookup_hash，再 hlist_add_head
             * (&d_alias)，最后 __d_rehash 加入主 dcache 哈希。这是 ->lookup
             * 中实例化 dentry 的正确接口 (参考 ceph_lookup -> d_splice_alias
             * -> __d_add)。
             */
            d_add(dentry, inode);

            /* 目录级 lease: lookup 成功后续约父目录 lease.
             * 一次 RPC 同时完成查询+续约, 后续同目录的 d_revalidate
             * 全部 RCU 命中, 无网络交互. */
            WRITE_ONCE(POWERFS_I(dir)->dir_lease_expire,
                       jiffies + POWERFS_DIR_LEASE_TTL);

            pr_debug("powerfs: lookup '%pd' completed\n", dentry);
            total_us = div_u64(ktime_get_ns() - ts_entry, 1000);
            pr_info_ratelimited("powerfs: LOOKUP '%pd' found ino=%llu net=%lluus total=%lluus\n",
                    dentry, (unsigned long long)ino, net_dur_us, total_us);
            return NULL;
        }

        if (err == -ENOENT) {
            /* 文件不存在: 添加负 dentry.
             * 目录级 lease: 负 dentry 不再维护独立 TTL, 有效性依赖父目录 lease.
             * lookup 成功 (即使 ENOENT) 也续约父目录 lease. */
            pr_debug("powerfs: lookup '%pd' not found (powerfs_net)\n", dentry);
            /* K3: ENOENT 时 lookup_layout 已被 parse_file_layout 零初始化,
             * volume_ids/inline_data/replica_chunks 为 NULL (Filer 不对不存在的文件编码 layout), 安全. */
            kfree(lookup_layout.volume_ids);
            kfree(lookup_layout.inline_data);
            kfree(lookup_layout.replica_chunks);
            kfree(lookup_layout.ec_chunks);
            d_add(dentry, NULL);
            WRITE_ONCE(POWERFS_I(dir)->dir_lease_expire,
                       jiffies + POWERFS_DIR_LEASE_TTL);
            total_us = div_u64(ktime_get_ns() - ts_entry, 1000);
            pr_info_ratelimited("powerfs: LOOKUP '%pd' enoent net=%lluus total=%lluus\n",
                    dentry, net_dur_us, total_us);
            return NULL;
        }

        /* 其他错误: 返回错误给 VFS, 不缓存负 dentry (避免误缓存 "不存在").
         * Phase 1: 之前是 "记录 + 添加负 dentry", 这会让网络瞬态错误被
         * 当成 "文件不存在" 缓存, 后续访问该文件继续返回 ENOENT.
         * 现在返回错误, 让 VFS/userspace 看到真实错误码并重试. */
        total_us = div_u64(ktime_get_ns() - ts_entry, 1000);
        pr_warn("powerfs: LOOKUP '%pd' error=%d net=%lluus total=%lluus\n",
                dentry, err, net_dur_us, total_us);
        /* K3: err 路径 lookup_layout 已零初始化, kfree(NULL) 安全 */
        kfree(lookup_layout.volume_ids);
        kfree(lookup_layout.inline_data);
        kfree(lookup_layout.replica_chunks);
        kfree(lookup_layout.ec_chunks);
        return ERR_PTR(err);
    }

    /* === 纯本地模式: 添加负 dentry === */
    pr_debug("powerfs: lookup '%pd' not found (local mode)\n", dentry);
    d_add(dentry, NULL);
    return NULL;
}

/* ========== 目录操作: mknod (通用创建函数) ========== */

/*
 * powerfs_mknod - 创建文件/目录/设备节点 (内部通用函数)
 *
 * 参考 ramfs_mknod (fs/ramfs/inode.c)
 */
static int powerfs_mknod(struct mnt_idmap *idmap, struct inode *dir,
                          struct dentry *dentry, umode_t mode, dev_t dev)
{
    struct powerfs_sb_info *sbi = POWERFS_SB_INFO(dir->i_sb);
    struct powerfs_inode_info *dpi = POWERFS_I(dir);
    struct inode *inode;
    u64 new_ino;
    u64 mknod_volume_id = 0, mknod_file_key = 0;  /* P3.4: Filer 自分配, 存入 inode */
    /* K1-4: CREATE 响应携带的 FileLayout (Inline/Stripe 模式有值, Flat 无).
     * mknod_has_layout=false 时按默认 Flat 处理. */
    struct powerfs_file_layout mknod_layout;
    bool mknod_has_layout = false;
    int type;

    (void)idmap;

    pr_debug("powerfs: mknod '%pd' mode=%o in dir=%lu\n",
             dentry, mode, dir->i_ino);

    /*
     * 向 filer 发 CREATE/MKDIR 请求获取权威 ino, 使文件元数据持久化到
     * filer (Raft 强一致); remount 后 lookup 能找回.
     * 断连时 powerfs_net_create 返回 -ENOTCONN, 操作失败 (不再本地分配).
     * Symlinks 有独立 powerfs_net_symlink 接口, 不走此路径.
     * 特殊文件 (fifo/block/char/socket) 也需持久化到 Filer, 否则 remount
     * 后丢失. Filer handle_create 对特殊文件跳过 volume/needle 分配.
     */
    if (S_ISREG(mode) || S_ISDIR(mode) || S_ISFIFO(mode) ||
        S_ISBLK(mode) || S_ISCHR(mode) || S_ISSOCK(mode)) {
        u64 remote_ino = 0;
        u64 volume_id = 0, file_key = 0;
        struct powerfs_file_layout layout = {0};
        int rerr = powerfs_net_create(dir->i_ino, dentry->d_name.name,
                                       dentry->d_name.len, mode,
                                       from_kuid(&init_user_ns, current_fsuid()),
                                       from_kgid(&init_user_ns, current_fsgid()),
                                       S_ISDIR(mode), &remote_ino,
                                       &volume_id, &file_key, &layout);
        if (rerr) {
            pr_warn("powerfs: net_create '%pd' failed: %d\n", dentry, rerr);
            return rerr;
        }
        new_ino = remote_ino ? remote_ino
                             : (u64)atomic_inc_return(&sbi->next_ino) + POWERFS_INO_START;
        /*
         * P3.4: 保存 Filer 自分配的 volume_id + needle_id 到 mknod 局部变量,
         * inode 创建后写入 powerfs_inode_info, 供后续直连 volume 读写.
         * 目录无数据, volume_id/file_key 为 0 (Filer handle_mkdir 不分配).
         *
         * K1-4: 保存 layout 到 mknod 局部变量, inode 创建后应用.
         *       Inline/Stripe 模式 Filer encode layout; Flat 模式 has_placement=false,
         *       应用时按默认 Flat 处理 (不覆盖 inode 默认值).
         */
        mknod_volume_id = volume_id;
        mknod_file_key = file_key;
        mknod_layout = layout;
        mknod_has_layout = true;
    } else {
        new_ino = atomic_inc_return(&sbi->next_ino) + POWERFS_INO_START;
    }

    /* 创建 inode */
    inode = powerfs_new_inode(dir->i_sb, mode, new_ino,
                               dir->i_ino, dentry->d_name.name);
    if (!inode)
        return -ENOSPC;

    /* For newly created directories, mark dir_complete=true (empty directory,
     * no need to fetch from Filer). powerfs_init_inode sets it to false for
     * the general case (LOOKUP path), so we override it here for mkdir. */
    if (S_ISDIR(mode)) {
        struct powerfs_inode_info *pi = POWERFS_I(inode);
        WRITE_ONCE(pi->dir_complete, true);
        WRITE_ONCE(pi->dir_lease_expire, jiffies + POWERFS_DIR_LEASE_TTL);
    }

    /* P3.4: 将 Filer 分配的 volume_id + needle_id 存入 inode 私有数据 */
    if (S_ISREG(mode) && mknod_volume_id != 0) {
        struct powerfs_inode_info *pi = POWERFS_I(inode);
        spin_lock(&pi->i_lock);
        pi->volume_id = mknod_volume_id;
        pi->file_key = mknod_file_key;
        spin_unlock(&pi->i_lock);
        pr_debug("powerfs: create '%pd' ino=%lu volume_id=%llu file_key=%llu\n",
                 dentry, inode->i_ino, mknod_volume_id, mknod_file_key);
    }

    /* K1-4 / K3-1: 应用 CREATE 响应中的 FileLayout 到 inode.
     * Inline 模式: placement=Inline, 后续 write/read 走 inline_data 路径 (K2).
     * Stripe 模式: placement=Stripe, volume_ids 已由 parse_file_layout 分配,
     *             powerfs_apply_layout_to_inode 转移所有权到 inode.
     * Flat 模式: has_placement=false, 保持 inode 默认值 (FLAT/SINGLE). */
    if (S_ISREG(mode) && mknod_has_layout) {
        struct powerfs_inode_info *pi = POWERFS_I(inode);
        spin_lock(&pi->i_lock);
        powerfs_apply_layout_to_inode(pi, &mknod_layout);
        spin_unlock(&pi->i_lock);
        pr_info("powerfs: create '%pd' ino=%lu placement=%u reliability=%u chunk_size=%u stripe_cnt=%u vids=%u has_inline=%d\n",
                 dentry, inode->i_ino, pi->placement, pi->reliability,
                 pi->layout_chunk_size, pi->stripe_count, pi->volume_ids_count,
                 pi->inline_data ? 1 : 0);
    } else if (mknod_has_layout && (mknod_layout.volume_ids || mknod_layout.inline_data || mknod_layout.replica_chunks)) {
        /* 未应用到 inode (非 regular 文件), 释放 parse 分配的 volume_ids/inline_data/replica_chunks */
        kfree(mknod_layout.volume_ids);
        mknod_layout.volume_ids = NULL;
        kfree(mknod_layout.inline_data);
        mknod_layout.inline_data = NULL;
        kfree(mknod_layout.replica_chunks);
        mknod_layout.replica_chunks = NULL;
        kfree(mknod_layout.ec_chunks);
        mknod_layout.ec_chunks = NULL;
    }

    /* 关联 dentry 和 inode.
     *
     * 必须用 d_instantiate, 不能用 d_add! 原因:
     * powerfs_mknod 被 .create/.mkdir 调用时, dentry 已经被 powerfs_lookup
     * 通过 d_add 加入 hash 链. 如果再次调用 d_add → __d_rehash →
     * hlist_bl_add_head_rcu, 会将同一个 dentry 重复插入 hash 链 → 形成环 →
     * __d_lookup_rcu 无限循环 → RCU stall.
     *
     * d_instantiate 只附加 inode (hlist_add_head 到 d_alias), 不操作 d_hash.
     * 参考: ramfs_mknod (fs/ramfs/inode.c) 也用 d_instantiate. */
    d_instantiate(dentry, inode);

    /* 更新父目录时间戳 */
    {
        struct timespec64 now = current_time(dir);
        inode_set_mtime(dir, now.tv_sec, now.tv_nsec);
        inode_set_ctime(dir, now.tv_sec, now.tv_nsec);
    }

    /* 添加目录项到本地链表 */
    if (S_ISREG(mode))
        type = S_IFREG;
    else if (S_ISDIR(mode))
        type = S_IFDIR;
    else if (S_ISLNK(mode))
        type = S_IFLNK;
    else
        type = mode & S_IFMT;

    powerfs_add_dir_entry(dir, new_ino, type, dentry->d_name.name);

    /* Phase 1: 本地 mutation 清父目录 lease, 下次 readdir 重新拉取. */
    powerfs_invalidate_dir_lease(dir);

    pr_debug("powerfs: mknod '%pd' success, ino=%llu\n",
             dentry, new_ino);

    return 0;
}

/* ========== 目录操作: mkdir ========== */

static struct dentry *powerfs_mkdir(struct mnt_idmap *idmap, struct inode *dir,
                          struct dentry *dentry, umode_t mode)
{
    struct inode *inode;
    int err;

    pr_debug("powerfs: mkdir '%pd' in dir=%lu mode=%o\n",
            dentry, dir->i_ino, mode);

    /*
     * 本地操作 + 远程同步通知:
     *   1. 在本地创建 inode 和 dentry
     *   2. 通过 powerfs_net 同步到远端（Delta Sync）
     *   3. 通过 powerfs_comm 异步通知代理（兼容旧接口）
     */
    err = powerfs_mknod(idmap, dir, dentry, mode | S_IFDIR, 0);
    if (err) {
        pr_warn("powerfs: mkdir '%pd' mknod failed: %d\n", dentry, err);
        return ERR_PTR(err);
    }

    /* 增加父目录 nlink (".." 指向父目录) */
    inc_nlink(dir);

    /* 获取新创建的 inode 用于远程同步 */
    inode = d_inode(dentry);

    pr_debug("powerfs: mkdir '%pd' success, ino=%lu\n",
            dentry, inode ? inode->i_ino : 0);

    return NULL;
}

/* ========== 目录操作: rmdir ========== */

static int powerfs_rmdir(struct inode *dir, struct dentry *dentry)
{
    struct inode *inode = d_inode(dentry);
    int rerr;

    pr_debug("powerfs: rmdir '%pd' in dir=%lu\n", dentry, dir->i_ino);

    /* 简单检查 (VFS 也会检查，这里双重保险) */
    if (!d_is_dir(dentry))
        return -ENOTDIR;

    if (!inode) {
        pr_warn("powerfs: rmdir '%pd' no inode\n", dentry);
        return -ENOENT;
    }

    /*
     * 先向 filer 发 RMDIR 持久化删除, 成功后才改本地数据结构.
     * 断连时 powerfs_net_unlink 返回 -ENOTCONN, 操作失败, 本地不变.
     * -ENOENT 视为幂等成功 (目录已在 filer 端删除).
     */
    rerr = powerfs_net_unlink(dir->i_ino, dentry->d_name.name,
                               dentry->d_name.len, true);
    if (rerr && rerr != -ENOENT) {
        pr_warn("powerfs: net_rmdir '%pd' failed: %d\n", dentry, rerr);
        return rerr;
    }

    /* === 以下操作仅在 filer 删除成功后执行 === */

    /* 更新父目录时间戳 */
    {
        struct timespec64 now = current_time(dir);
        inode_set_mtime(dir, now.tv_sec, now.tv_nsec);
        inode_set_ctime(dir, now.tv_sec, now.tv_nsec);
    }

    /* 减少被删除目录的链接数 (参考 simple_rmdir, 不 ihold/iput) */
    drop_nlink(inode);

    /* 清空子目录的目录项链表 */
    powerfs_clear_dir_entries(inode);

    /* Invalidate the removed directory's own lease. Without this, the inode
     * may stay in the inode cache (dentry references) with stale
     * dir_complete=true + future dir_lease_expire. When the Filer reuses
     * this inode number for a new directory, iget5_locked returns the cached
     * inode, and readdir takes the fast-path with an empty dir_entries list,
     * skipping the Filer fetch (T3b/T3c/T9a/T9b "Directory not empty"). */
    powerfs_invalidate_dir_lease(inode);

    /* 从父目录的本地目录项链表中移除 */
    powerfs_remove_dir_entry(dir, dentry->d_name.name);

    /* 减少父目录的链接数 (因删除了一个子目录) */
    drop_nlink(dir);

    /* Phase 1: 本地 mutation 清父目录 lease, 下次 readdir 重新拉取. */
    powerfs_invalidate_dir_lease(dir);

    pr_debug("powerfs: rmdir '%pd' success\n", dentry);

    /*
     * 注意: 不要在这里 iput(inode)
     *       让 do_rmdir 的 iput 统一处理
     */
    return 0;
}

/* ========== 目录操作: create ========== */

static int powerfs_create(struct mnt_idmap *idmap, struct inode *dir,
                           struct dentry *dentry, umode_t mode, bool excl)
{
    struct inode *inode;
    int err;

    (void)excl;

    pr_debug("powerfs: create '%pd' in dir=%lu mode=%o\n",
             dentry, dir->i_ino, mode);

    /*
     * 本地操作 + 远程同步通知:
     *   1. 在本地创建 inode 和 dentry
     *   2. 通过 powerfs_net 触发 Delta Sync 失效
     *   3. 通过 powerfs_comm 异步通知代理（兼容旧接口）
     *
     * dget 锁定 dentry 后，stat() 直接从 dcache 查找，不触发 lookup，
     * 避免 lookup 时代理还没处理完 CREATE 导致 -ENOENT 的问题
     */
    err = powerfs_mknod(idmap, dir, dentry, mode | S_IFREG, 0);
    if (err) {
        pr_warn("powerfs: create '%pd' mknod failed: %d\n", dentry, err);
        return err;
    }

    /* 获取新创建的 inode 用于通知 */
    inode = d_inode(dentry);

    pr_debug("powerfs: create '%pd' success, ino=%lu\n",
             dentry, inode ? inode->i_ino : 0);

    return 0;
}

/* ========== 目录操作: unlink ========== */

static int powerfs_unlink(struct inode *dir, struct dentry *dentry)
{
    struct inode *inode = d_inode(dentry);
    int rerr;

    pr_debug("powerfs: unlink '%pd' in dir=%lu\n", dentry, dir->i_ino);

    if (!inode) {
        pr_warn("powerfs: unlink '%pd' no inode\n", dentry);
        return -ENOENT;
    }

    /*
     * 先向 filer 发 UNLINK 持久化删除, 成功后才改本地数据结构.
     * 断连时 powerfs_net_unlink 返回 -ENOTCONN, 操作失败, 本地不变.
     * -ENOENT 视为幂等成功 (文件已在 filer 端删除).
     * 之前是"先改本地再发网络", 断连时本地已改但 filer 未删 → 不一致.
     */
    rerr = powerfs_net_unlink(dir->i_ino, dentry->d_name.name,
                               dentry->d_name.len, false);
    if (rerr && rerr != -ENOENT) {
        pr_warn("powerfs: net_unlink '%pd' failed: %d\n", dentry, rerr);
        return rerr;
    }

    /* === 以下操作仅在 filer 删除成功后执行 === */

    /* 更新父目录时间戳 */
    {
        struct timespec64 now = current_time(dir);
        inode_set_mtime(dir, now.tv_sec, now.tv_nsec);
        inode_set_ctime(dir, now.tv_sec, now.tv_nsec);
    }

    /* 减少 inode 链接数 (参考 simple_unlink, 不 ihold/iput, 由 VFS dentry 管理) */
    drop_nlink(inode);

    /* 从本地目录项链表中移除 */
    powerfs_remove_dir_entry(dir, dentry->d_name.name);

    /* Phase 1: 本地 mutation 清父目录 lease, 下次 readdir 重新拉取. */
    powerfs_invalidate_dir_lease(dir);

    pr_debug("powerfs: unlink '%pd' success\n", dentry);

    return 0;
}

/* ========== 目录操作: symlink ========== */

static int powerfs_symlink(struct mnt_idmap *idmap, struct inode *dir,
                            struct dentry *dentry, const char *symname)
{
    struct powerfs_sb_info *sbi = POWERFS_SB_INFO(dir->i_sb);
    struct inode *inode;
    struct powerfs_inode_info *pi;
    u64 new_ino;
    int err;

    (void)idmap;

    pr_debug("powerfs: symlink '%pd' -> '%s'\n", dentry, symname);

    /*
     * 先向 filer 发 SYMLINK 持久化, 成功后才创建本地 inode.
     * 断连时 powerfs_net_symlink 返回 -ENOTCONN, 操作失败.
     */
    {
        __u64 sym_ino = 0;
        int nret = powerfs_net_symlink(dir->i_ino,
                                        dentry->d_name.name,
                                        dentry->d_name.len,
                                        symname, strlen(symname),
                                        &sym_ino);
        if (nret < 0) {
            pr_warn("powerfs: symlink net_sync name=%s failed: %d\n",
                    dentry->d_name.name, nret);
            return nret;
        }
        new_ino = sym_ino ? sym_ino
                          : (u64)atomic_inc_return(&sbi->next_ino) + POWERFS_INO_START;
    }

    /* === 以下操作仅在 filer 创建成功后执行 === */

    /* 创建符号链接 inode */
    inode = powerfs_new_inode(dir->i_sb, S_IFLNK | 0777, new_ino,
                               dir->i_ino, dentry->d_name.name);
    if (!inode)
        return -ENOSPC;

    pi = POWERFS_I(inode);

    /* 使用 page cache 存储符号链接目标 */
    err = page_symlink(inode, symname, strlen(symname) + 1);
    if (err) {
        iput(inode);
        return err;
    }

    /* Filer 会将 symlink target 存储为 InlineData (placement=Inline).
     * 本地也存储一份到 inline_data, 以便 page cache 被 evict 后
     * netfs_issue_read 能从 inline_data 恢复 (而非回退到 volume_read
     * 导致 -EINVAL, 因为 Inline 文件无 needle). */
    {
        size_t target_len = strlen(symname) + 1;
        u8 *buf = kvmalloc(target_len, GFP_KERNEL);
        if (buf) {
            memcpy(buf, symname, target_len);
            spin_lock(&pi->i_lock);
            kfree(pi->inline_data);
            pi->inline_data = buf;
            pi->inline_len = target_len;
            pi->placement = POWERFS_PLACEMENT_INLINE;
            pi->inline_dirty = false;  /* Filer 已知 target */
            spin_unlock(&pi->i_lock);
        }
    }

    /* 关联 dentry 和 inode (用 d_instantiate, 不能用 d_add — 见 powerfs_mknod 注释) */
    d_instantiate(dentry, inode);

    /* 更新父目录时间戳 */
    {
        struct timespec64 now = current_time(dir);
        inode_set_mtime(dir, now.tv_sec, now.tv_nsec);
        inode_set_ctime(dir, now.tv_sec, now.tv_nsec);
    }

    /* 添加目录项到本地链表 */
    powerfs_add_dir_entry(dir, new_ino, S_IFLNK, dentry->d_name.name);

    /* Phase 1: 本地 mutation 清父目录 lease, 下次 readdir 重新拉取. */
    powerfs_invalidate_dir_lease(dir);

    pr_debug("powerfs: symlink '%pd' success, ino=%llu\n",
             dentry, (unsigned long long)new_ino);

    return 0;
}

/* ========== 目录操作: readlink ========== */

/*
 * powerfs_readlink - 读取符号链接目标
 *
 * 参考 ceph_readlink (fs/ceph/symlink.c)
 *
 * 策略:
 *   - 通信层可用时: 优先从服务端获取最新目标路径
 *   - 纯内存模式: 直接从 page cache 读取
 */
int powerfs_readlink(struct dentry *dentry, char *buffer, int buflen)
{
    struct inode *inode = d_inode(dentry);
    struct powerfs_inode_info *pi;
    struct page *page;
    void *page_addr;
    u32 len;

    pr_debug("powerfs: readlink '%pd'\n", dentry);

    if (!inode)
        return -ENOENT;

    if (!S_ISLNK(inode->i_mode))
        return -EINVAL;

    if (inode->i_size == 0) {
        /* Symlink size may be 0 if created by an older Filer that didn't
         * set size=target.len(). Fall through to inline_data / GETATTR
         * instead of returning empty string. */
        pr_debug("powerfs: readlink '%pd' size=0, trying inline/getattr\n", dentry);
    }

    len = min_t(u32, (u64)inode->i_size, buflen - 1);
    if (len == 0)
        len = buflen - 1;  /* fallback: allow GETATTR to fill the target */

    /* 1. Try page cache first (fast path, works before remount). */
    page = find_get_page(inode->i_mapping, 0);
    if (!page) {
        pi = POWERFS_I(inode);

        /* 2. Check inline_data (set by powerfs_symlink or GETATTR response). */
        spin_lock(&pi->i_lock);
        if (pi->inline_data && pi->inline_len > 0) {
            u32 copy_len = min_t(u32, pi->inline_len, len);
            memcpy(buffer, pi->inline_data, copy_len);
            buffer[copy_len] = '\0';
            spin_unlock(&pi->i_lock);
            pr_debug("powerfs: readlink '%pd' from inline_data: '%s'\n",
                     dentry, buffer);
            return 0;
        }
        spin_unlock(&pi->i_lock);

        /* 3. No inline_data: fetch via GETATTR (symlink target is stored as
         * inline_data on the Filer, but LOOKUP response may not include it).
         * After GETATTR, inline_data is populated and we can read from it. */
        {
            struct powerfs_file_layout layout = {0};
            __u32 mode = 0, uid = 0, gid = 0, nlink = 0;
            __u64 size = 0, mtime = 0, atime = 0, ctime = 0;
            __u64 volume_id = 0, file_key = 0;
            int ret;

            ret = powerfs_net_getattr(inode->i_ino, &mode, &uid, &gid,
                                      &size, &nlink, &mtime, &atime, &ctime,
                                      &volume_id, &file_key, &layout);
            if (ret == 0) {
                spin_lock(&pi->i_lock);
                powerfs_apply_layout_to_inode(pi, &layout);
                if (pi->inline_data && pi->inline_len > 0) {
                    u32 copy_len = min_t(u32, pi->inline_len, len);
                    memcpy(buffer, pi->inline_data, copy_len);
                    buffer[copy_len] = '\0';
                    spin_unlock(&pi->i_lock);
                    pr_debug("powerfs: readlink '%pd' from GETATTR inline: '%s'\n",
                             dentry, buffer);
                    kfree(layout.volume_ids);
                    kfree(layout.replica_chunks);
                    kfree(layout.ec_chunks);
                    return 0;
                }
                spin_unlock(&pi->i_lock);
            }
            kfree(layout.volume_ids);
            kfree(layout.inline_data);
            kfree(layout.replica_chunks);
            kfree(layout.ec_chunks);
        }

        /* 4. GETATTR didn't return inline_data: try read_cache_page as
         * last resort (triggers netfs_read_folio for Flat files). */
        page = read_cache_page(inode->i_mapping, 0, NULL, NULL);
        if (IS_ERR(page)) {
            pr_warn("powerfs: readlink '%pd' read_cache_page failed: %ld\n",
                    dentry, PTR_ERR(page));
            buffer[0] = '\0';
            return 0;
        }
    }

    page_addr = kmap(page);
    memcpy(buffer, page_addr, len);
    buffer[len] = '\0';
    kunmap(page);
    put_page(page);

    pr_debug("powerfs: readlink '%pd' from cache: '%s'\n", dentry, buffer);
    return 0;
}

/* ========== 目录操作: link (硬链接) ========== */

/*
 * powerfs_link - 创建硬链接
 *
 * 参考 ceph_link (fs/ceph/dir.c)
 *
 * 策略 (本地操作 + 异步通知，与mkdir/create/unlink保持一致):
 *   1. 本地增加 nlink 和 dentry
 *   2. 异步通知代理增加后端硬链接计数
 */
static int powerfs_link(struct dentry *old_dentry, struct inode *dir,
                        struct dentry *new_dentry)
{
    struct inode *inode = d_inode(old_dentry);
    int nret;

    pr_debug("powerfs: link '%pd' -> '%pd' (ino=%lu)\n",
             old_dentry, new_dentry, inode->i_ino);

    /* 不允许对目录创建硬链接 */
    if (S_ISDIR(inode->i_mode))
        return -EPERM;

    /*
     * 先向 filer 发 LINK 持久化, 成功后才改本地数据结构.
     * 断连时 powerfs_net_link 返回 -ENOTCONN, 操作失败, 本地不变.
     */
    nret = powerfs_net_link(inode->i_ino, dir->i_ino,
                             new_dentry->d_name.name,
                             new_dentry->d_name.len);
    if (nret < 0) {
        pr_warn("powerfs: link net_sync name=%s failed: %d\n",
                new_dentry->d_name.name, nret);
        return nret;
    }

    /* === 以下操作仅在 filer link 成功后执行 === */

    /*
     * VFS (vfs_link) 不会自动 inc_nlink, 需要文件系统自己调用.
     * d_instantiate 不递增 i_count! 每个 dentry 必须持有独立的 i_count,
     * 参考 simple_link / ramfs_link 均在 d_instantiate 前调用 ihold.
     * 用 d_instantiate (不能用 d_add): new_dentry 已被 lookup 路径加入 hash 链.
     */
    inc_nlink(inode);
    ihold(inode);
    d_instantiate(new_dentry, inode);

    {
        struct timespec64 now = current_time(dir);
        inode_set_mtime(dir, now.tv_sec, now.tv_nsec);
        inode_set_ctime(dir, now.tv_sec, now.tv_nsec);
    }

    /* 添加目录项到本地链表 */
    powerfs_add_dir_entry(dir, inode->i_ino,
                          inode->i_mode & S_IFMT,
                          new_dentry->d_name.name);

    /* Phase 1: 本地 mutation 清父目录 lease, 下次 readdir 重新拉取. */
    powerfs_invalidate_dir_lease(dir);

    return 0;
}

/* ========== getattr ========== */

/*
 * powerfs_getattr - 获取文件属性
 *
 * 参考 ceph_getattr (fs/ceph/inode.c)
 *
 * 策略:
 *   - 通信层可用时: 优先从服务端获取最新属性
 *   - 纯内存模式: 直接使用本地 inode 缓存
 */
static int powerfs_getattr(struct mnt_idmap *idmap, const struct path *path,
                            struct kstat *stat, u32 request_mask,
                            unsigned int query_flags)
{
    struct inode *inode = d_inode(path->dentry);
    struct powerfs_inode_info *pi = POWERFS_I(inode);

    (void)idmap;
    (void)query_flags;

    pr_debug("powerfs: getattr '%pd'\n", path->dentry);

    /*
     * 本地缓存模式: 直接使用本地 inode 属性
     *
     * 不再从代理获取属性 (避免同步通信导致的 RCU stall)
     * 如果需要从服务端获取属性，可在后续阶段添加异步机制
     */

    /* 使用 VFS 通用属性获取 (6.17: 增加 request_mask 参数) */
    generic_fillattr(idmap, request_mask, inode, stat);

    /* block 数 (512 字节为单位) */
    stat->blocks = (i_size_read(inode) + 511) / 512;
    stat->blksize = 4096;

    return 0;
}

/* ========== setattr ========== */

/*
 * powerfs_setattr - 设置文件属性
 *
 * 参考 ceph_setattr (__ceph_setattr) (fs/ceph/inode.c)
 *
 * 策略:
 *   - 通信层可用时: 先向服务端发送 SETATTR 请求，成功后更新本地缓存
 *   - 纯内存模式: 直接修改本地 inode 属性
 *
 * 支持的属性:
 *   - ATTR_MODE: 文件权限
 *   - ATTR_UID/ATTR_GID: 所有者
 *   - ATTR_SIZE: 文件大小 (truncate)
 *   - ATTR_ATIME/ATTR_MTIME: 时间戳
 */
int powerfs_setattr(struct mnt_idmap *idmap, struct dentry *dentry,
                    struct iattr *attr)
{
    struct inode *inode = d_inode(dentry);
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    int err;
    unsigned int ia_valid;

    (void)idmap;

    pr_debug("powerfs: setattr '%pd' ia_valid=0x%x\n", dentry, attr->ia_valid);
    pr_debug("powerfs: SETATTR ino=%lu ia_valid=0x%x ia_size=%lld cur_size=%lld\n",
            inode->i_ino, attr->ia_valid,
            (attr->ia_valid & ATTR_SIZE) ? attr->ia_size : -1,
            i_size_read(inode));

    /*
     * 纯本地操作 + 异步通知:
     *   1. 先在本地修改 inode 属性
     *   2. 异步通知代理更新后端记录
     *
     * 同步通信会导致高并发下超时和卡顿，改为异步通知模式
     */
    ia_valid = attr->ia_valid;

    /* 第一步: 执行 VFS 通用 setattr 处理 (权限检查等) */
    err = setattr_prepare(idmap, dentry, attr);
    if (err)
        return err;

    /* 处理文件大小变更
     *
     * 注意: setattr_copy 不会设置 i_size (它只处理 uid/gid/mode/time).
     * 必须调用 truncate_setsize 来同时截断 page cache 并更新 i_size.
     * 之前只调 truncate_pagecache 导致 i_size 未更新, O_TRUNC 后
     * i_size 仍为旧值, O_APPEND 写到了错误的 offset.
     */
    if (ia_valid & ATTR_SIZE) {
        truncate_setsize(inode, attr->ia_size);
        /* 同步 pi->content_size, 否则 write_end 的
         * (new_size != pi->content_size) 检查会误判为 "未变化"
         * 而跳过 net_setattr, 导致 O_TRUNC 后的写入不持久化 size.
         * 例: 文件原 size=4, O_TRUNC 设 size=0, 再写 4 字节:
         *   new_size=4 == pi->content_size=4 → 跳过 setattr
         *   Filer 端 size 仍为 0 → remount 后文件为空 */
        pi->content_size = attr->ia_size;

        /* K2: Inline 文件 truncate 时必须同步 inline_data.
         * O_TRUNC (size=0): 释放 inline_data, 清除 dirty 标记.
         *   否则旧 inline_data 残留, dd '>' 覆盖旧文件时:
         *   - inline_len 仍为旧值 (如 4096)
         *   - write_end 中 inline_len >= need_len, 不重新分配
         *   - 旧 inline_data buffer 被部分覆盖, 数据不一致
         * truncate 到非 0: 截断 inline_data 到新大小 */
        if (pi->placement == POWERFS_PLACEMENT_INLINE) {
            spin_lock(&pi->i_lock);
            if (attr->ia_size == 0) {
                kfree(pi->inline_data);
                pi->inline_data = NULL;
                pi->inline_len = 0;
                pi->inline_dirty = false;
                pr_info("powerfs: SETATTR truncate INLINE ino=%lu size=0, cleared inline_data\n",
                        inode->i_ino);
            } else if (pi->inline_data && attr->ia_size < pi->inline_len) {
                pi->inline_len = attr->ia_size;
                pi->inline_dirty = true;
                pr_info("powerfs: SETATTR truncate INLINE ino=%lu size=%llu, inline_len=%u\n",
                        inode->i_ino, attr->ia_size, pi->inline_len);
            }
            spin_unlock(&pi->i_lock);
        }
    }

    /* 第二步: 在本地修改 inode 属性 */
    setattr_copy(idmap, inode, attr);
    mark_inode_dirty(inode);

    /* 更新缓存标志 */
    spin_lock(&pi->i_lock);
    pi->cache_valid = true;
    pi->cache_expire = jiffies + POWERFS_INODE_CACHE_TTL;
    spin_unlock(&pi->i_lock);

    /*
     * powerfs_net 同步: truncate/fchmod/chown 等必须持久化到 Filer,
     * 否则 remount 后属性丢失. fsync 路径已单独同步 size, 这里覆盖
     * setattr 回调路径 (truncate/ftruncate/chmod/chown/utimes).
     */
    if (powerfs_net_is_connected()) {
        __u32 valid = 0;
        __u32 m = 0, u = 0, g = 0;
        __u64 sz = 0, mt = 0, at = 0;

        if (ia_valid & ATTR_MODE) {
            valid |= POWERFS_ATTR_MODE;
            m = inode->i_mode;
        }
        if (ia_valid & ATTR_UID) {
            valid |= POWERFS_ATTR_UID;
            u = from_kuid(&init_user_ns, inode->i_uid);
        }
        if (ia_valid & ATTR_GID) {
            valid |= POWERFS_ATTR_GID;
            g = from_kgid(&init_user_ns, inode->i_gid);
        }
        if (ia_valid & ATTR_SIZE) {
            valid |= POWERFS_ATTR_SIZE;
            sz = i_size_read(inode);
        }
        /* ATTR_MTIME/ATTR_ATIME: persist timestamps so utimes(2)/touch
         * survive remount. Read inode mtime/atime (already updated by
         * setattr_copy above) via 6.17 accessors, convert to unix seconds.
         * ATTR_CTIME is not forwarded (Filer sets ctime implicitly on
         * size change). */
        if (ia_valid & ATTR_MTIME) {
            valid |= POWERFS_ATTR_MTIME;
            mt = inode_get_mtime(inode).tv_sec;
        }
        if (ia_valid & ATTR_ATIME) {
            valid |= POWERFS_ATTR_ATIME;
            at = inode_get_atime(inode).tv_sec;
        }

        if (valid) {
            int sret = powerfs_net_setattr(inode->i_ino, valid,
                                            m, u, g, sz, mt, at);
            pr_debug("powerfs: SETATTR net ino=%lu valid=0x%x sz=%llu mt=%llu at=%llu sret=%d\n",
                    inode->i_ino, valid, sz, mt, at, sret);
            if (sret < 0)
                pr_warn("powerfs: setattr net sync ino=%lu failed: %d\n",
                        inode->i_ino, sret);
        }
    }

    pr_debug("powerfs: setattr '%pd' success (async)\n", dentry);
    return 0;
}

/* ========== rename ========== */

/*
 * powerfs_rename - 重命名/移动文件
 *
 * 参考 ceph_rename (fs/ceph/dir.c)
 *
 * 策略:
 *   - 纯本地操作: 在 VFS dcache 中直接进行重命名
 *   - 异步通知: 操作完成后异步通知代理更新后端
 *
 * 处理情况:
 *   - 同一目录内重命名
 *   - 跨目录移动
 *   - 目标已存在 (覆盖)
 */
int powerfs_rename(struct mnt_idmap *idmap,
                   struct inode *old_dir, struct dentry *old_dentry,
                   struct inode *new_dir, struct dentry *new_dentry,
                   unsigned int flags)
{
    struct powerfs_inode_info *old_dpi, *new_dpi;
    struct inode *inode = d_inode(old_dentry);

    (void)idmap;

    pr_debug("powerfs: rename '%pd' -> '%pd' (flags=%u)\n",
             old_dentry, new_dentry, flags);

    /* 不支持的标志 */
    if (flags & ~(RENAME_NOREPLACE | RENAME_EXCHANGE | RENAME_WHITEOUT))
        return -EINVAL;

    if (!inode) {
        pr_warn("powerfs: rename '%pd' no inode\n", old_dentry);
        return -ENOENT;
    }

    old_dpi = POWERFS_I(old_dir);
    new_dpi = POWERFS_I(new_dir);

    /*
     * 保存旧/新名称 (VFS 会在 rename 返回后调用 d_move 改变 dentry 名称)
     */
    {
        char old_name_buf[POWERFS_MAX_NAME_LEN + 1];
        char new_name_buf[POWERFS_MAX_NAME_LEN + 1];
        int nret;

        strncpy(old_name_buf, old_dentry->d_name.name, POWERFS_MAX_NAME_LEN);
        old_name_buf[POWERFS_MAX_NAME_LEN] = '\0';
        strncpy(new_name_buf, new_dentry->d_name.name, POWERFS_MAX_NAME_LEN);
        new_name_buf[POWERFS_MAX_NAME_LEN] = '\0';

        /* 检查目标已存在的情况 (但不修改 nlink, 等 net_rename 成功后再改) */
        if (d_really_is_positive(new_dentry)) {
            struct inode *target = d_inode(new_dentry);

            /* 不支持 RENAME_EXCHANGE */
            if (flags & RENAME_EXCHANGE)
                return -EINVAL;

            if (!target)
                return -ENOENT;

            /* 检查是否可以删除目标 */
            if (S_ISDIR(target->i_mode)) {
                /* 目录必须为空 */
                if (!simple_empty(new_dentry))
                    return -ENOTEMPTY;
            }
        } else if (flags & RENAME_NOREPLACE) {
            return -EEXIST;
        }

        /*
         * 先向 filer 发 RENAME 持久化, 成功后才改本地数据结构.
         * 断连时 powerfs_net_rename 返回 -ENOTCONN, 操作失败, 本地不变.
         */
        nret = powerfs_net_rename(old_dir->i_ino,
                                   old_name_buf, strlen(old_name_buf),
                                   new_dir->i_ino,
                                   new_name_buf, strlen(new_name_buf));
        if (nret < 0) {
            pr_warn("powerfs: rename net_sync old=%s new=%s failed: %d\n",
                    old_name_buf, new_name_buf, nret);
            return nret;
        }

        /* === 以下操作仅在 filer rename 成功后执行 === */

        /*
         * 目标已存在: 减少目标 inode 的 nlink
         * 参考 ramfs_rename (fs/ramfs/inode.c)
         */
        if (d_really_is_positive(new_dentry)) {
            struct inode *target = d_inode(new_dentry);
            if (S_ISDIR(target->i_mode))
                drop_nlink(new_dir);
            drop_nlink(target);
        }

        /*
         * 目录跨移动: drop_nlink(old_dir) + inc_nlink(new_dir)
         * 参考 simple_rename_exchange
         */
        if (S_ISDIR(inode->i_mode) && old_dir != new_dir) {
            drop_nlink(old_dir);
            inc_nlink(new_dir);
        }

        /* 更新时间戳 (6.17: 使用 inode_set_mtime_to_ts/inode_set_ctime_to_ts) */
        {
            struct timespec64 now = current_time(old_dir);
            inode_set_mtime_to_ts(old_dir, now);
            inode_set_ctime_to_ts(old_dir, now);
            if (old_dir != new_dir) {
                now = current_time(new_dir);
                inode_set_mtime_to_ts(new_dir, now);
                inode_set_ctime_to_ts(new_dir, now);
            }
        }

        mark_inode_dirty(old_dir);
        if (old_dir != new_dir)
            mark_inode_dirty(new_dir);

        /* 更新本地目录项链表 */
        /* 从旧目录移除旧名称 */
        powerfs_remove_dir_entry(old_dir, old_name_buf);

        /* 在新目录添加新名称 (或在同一目录更新) */
        {
            unsigned int entry_type = inode->i_mode & S_IFMT;
            powerfs_add_dir_entry(new_dir, inode->i_ino,
                                  entry_type, new_name_buf);
        }

        /* 如果目标已存在，移除目标目录项 */
        if (d_really_is_positive(new_dentry)) {
            struct inode *target = d_inode(new_dentry);
            if (target) {
                powerfs_remove_dir_entry(new_dir, new_name_buf);
            }
        }

        /* Phase 1: 本地 mutation 清两个父目录的 lease + epoch++.
         * rename 涉及 old_dir 和 new_dir (可能相同), 都要清,
         * 下次 readdir 重新拉取, 看到重命名后的结果. */
        powerfs_invalidate_dir_lease(old_dir);
        if (old_dir != new_dir)
            powerfs_invalidate_dir_lease(new_dir);
    }

    pr_debug("powerfs: rename '%pd' -> '%pd' success\n",
             old_dentry, new_dentry);

    return 0;
}

/* ========== readdir: 目录读取操作 ========== */

/*
 * powerfs_dir_open - 打开目录文件
 *
 * 参考 ceph_dir_open (fs/ceph/dir.c)
 * 分配目录文件私有数据，初始化readdir状态
 */
int powerfs_dir_open(struct inode *inode, struct file *file)
{
    struct powerfs_dir_file_info *dfi;

    pr_debug("powerfs: dir_open ino=%lu\n", inode->i_ino);

    dfi = kzalloc(sizeof(*dfi), GFP_KERNEL);
    if (!dfi)
        return -ENOMEM;

    dfi->file = file;
    dfi->dir = POWERFS_I(inode);
    dfi->last_ino = 0;
    dfi->last_name = NULL;
    dfi->next_offset = 0;
    dfi->cached_entries = NULL;
    dfi->cached_count = 0;
    dfi->cached_index = 0;
    mutex_init(&dfi->lock);

    file->private_data = dfi;

    pr_debug("powerfs: dir_open success, fop=%p\n", file->f_op);

    return 0;
}

/*
 * powerfs_dir_release - 关闭目录文件
 *
 * 释放目录文件私有数据和缓存
 */
int powerfs_dir_release(struct inode *inode, struct file *file)
{
    struct powerfs_dir_file_info *dfi = file->private_data;

    pr_debug("powerfs: dir_release ino=%lu\n", inode->i_ino);

    if (!dfi)
        return 0;

    if (dfi->last_name)
        kfree(dfi->last_name);
    if (dfi->cached_entries)
        kfree(dfi->cached_entries);
    kfree(dfi);

    file->private_data = NULL;
    return 0;
}

/*
 * powerfs_fill_readdir_cache - 填充目录项缓存
 *
 * 从通信层批量获取目录项，缓存到内存中供readdir使用
 * 返回 0 表示成功，负值表示错误
 *
 * 获取成功后设置 dir_complete = true，表示目录内容已完整缓存
 */
static int powerfs_fill_readdir_cache(struct powerfs_dir_file_info *dfi,
                                       struct inode *dir)
{
    struct powerfs_msg_header req_hdr, resp_hdr;
    struct powerfs_readdir_req req_data;
    struct powerfs_inode_info *dpi = POWERFS_I(dir);
    void *resp_data = NULL;
    int ret;
    u32 max_entries = 64;
    u32 resp_size;

    pr_debug("powerfs: fill_readdir_cache dir_ino=%lu offset=%u\n",
             dir->i_ino, dfi->next_offset);

    /* 如果通信层不可用，返回 ENOTCONN，调用方会用内存模式 */
    if (!powerfs_net_is_connected())
        return -ENOTCONN;

    resp_size = max_entries * sizeof(struct powerfs_dirent);
    resp_data = kmalloc(resp_size, GFP_KERNEL);
    if (!resp_data)
        return -ENOMEM;

    /* 构建请求 */
    memset(&req_hdr, 0, sizeof(req_hdr));
    req_hdr.type = POWERFS_MSG_READDIR;
    req_hdr.ino = dir->i_ino;
    req_hdr.data_len = sizeof(req_data);

    memset(&req_data, 0, sizeof(req_data));
    req_data.dir_ino = dir->i_ino;
    req_data.offset = dfi->next_offset;
    req_data.max_entries = max_entries;

    memset(&resp_hdr, 0, sizeof(resp_hdr));

    /* 发送请求 */
    ret = powerfs_comm_send_request(&req_hdr, &req_data,
                                     &resp_hdr, resp_data, 500);
    if (ret < 0) {
        kfree(resp_data);
        return ret;
    }

    if (resp_hdr.status != 0) {
        kfree(resp_data);
        return resp_hdr.status;
    }

    /* 解析响应 */
    if (dfi->cached_entries)
        kfree(dfi->cached_entries);
    dfi->cached_entries = NULL;
    dfi->cached_count = 0;
    dfi->cached_index = 0;

    if (resp_hdr.data_len == 0) {
        kfree(resp_data);
        /* 没有更多条目，标记目录完整 */
        spin_lock(&dpi->i_lock);
        dpi->dir_complete = true;
        spin_unlock(&dpi->i_lock);
        return 0;
    }

    /* 计算有多少个目录项 */
    dfi->cached_count = resp_hdr.data_len / sizeof(struct powerfs_dirent);
    if (dfi->cached_count == 0) {
        kfree(resp_data);
        return 0;
    }

    /* 分配并拷贝目录项 */
    dfi->cached_entries = kmemdup(resp_data,
                                   dfi->cached_count * sizeof(struct powerfs_dirent),
                                   GFP_KERNEL);
    if (!dfi->cached_entries) {
        dfi->cached_count = 0;
        kfree(resp_data);
        return -ENOMEM;
    }

    kfree(resp_data);

    /* 获取成功，标记目录内容已完整缓存 */
    spin_lock(&dpi->i_lock);
    dpi->dir_complete = true;
    spin_unlock(&dpi->i_lock);

    pr_debug("powerfs: fill_readdir_cache got %u entries (dir_complete=true)\n",
             dfi->cached_count);
    return 0;
}

/* ========== 目录项管理 (本地 readdir) ========== */

/**
 * powerfs_add_dir_entry - 添加目录项到链表
 *
 * 使用 dir_mutex 保护
 */
static int powerfs_add_dir_entry(struct inode *dir, u64 ino,
                                  unsigned int type, const char *name)
{
    struct powerfs_inode_info *dpi = POWERFS_I(dir);
    struct powerfs_dir_entry *entry;

    if (!S_ISDIR(dir->i_mode))
        return 0;

    entry = kmalloc(sizeof(*entry), GFP_KERNEL);
    if (!entry)
        return -ENOMEM;

    entry->ino = ino;
    entry->type = type;
    strncpy(entry->name, name, POWERFS_MAX_NAME_LEN - 1);
    entry->name[POWERFS_MAX_NAME_LEN - 1] = '\0';

    mutex_lock(&dpi->dir_mutex);
    list_add_tail(&entry->list, &dpi->dir_entries);
    mutex_unlock(&dpi->dir_mutex);

    return 0;
}

/**
 * powerfs_remove_dir_entry - 从链表中移除目录项
 *
 * 使用 dir_mutex 保护
 */
static int powerfs_remove_dir_entry(struct inode *dir, const char *name)
{
    struct powerfs_inode_info *dpi = POWERFS_I(dir);
    struct powerfs_dir_entry *entry, *tmp;

    if (!S_ISDIR(dir->i_mode))
        return 0;

    mutex_lock(&dpi->dir_mutex);
    list_for_each_entry_safe(entry, tmp, &dpi->dir_entries, list) {
        if (strcmp(entry->name, name) == 0) {
            list_del_init(&entry->list);
            mutex_unlock(&dpi->dir_mutex);
            kfree(entry);
            return 0;
        }
    }
    mutex_unlock(&dpi->dir_mutex);

    return -ENOENT;
}

/**
 * powerfs_clear_dir_entries - 清空目录项链表
 *
 * 使用 dir_mutex 保护
 */
static void powerfs_clear_dir_entries(struct inode *dir)
{
    struct powerfs_inode_info *dpi = POWERFS_I(dir);
    struct powerfs_dir_entry *entry, *tmp;

    if (!S_ISDIR(dir->i_mode))
        return;

    mutex_lock(&dpi->dir_mutex);
    list_for_each_entry_safe(entry, tmp, &dpi->dir_entries, list) {
        list_del_init(&entry->list);
        kfree(entry);
    }
    mutex_unlock(&dpi->dir_mutex);
}

/* Phase 1: 本地 mutation 后清父目录 lease + bump epoch.
 * 调用时机: mkdir/rmdir/create/unlink/symlink/link/rename 网络请求成功后,
 *           在修改本地数据结构的同时清目录 lease.
 * 效果: 下次 readdir 看到 lease 过期会重新拉取, 看到自己刚加/删的项.
 *       epoch++ 留给 Phase 3 callback 比对 (callback 携带 epoch,
 *       不匹配说明有过本地修改, 需重新拉取).
 * 注意: 调用方已持 dir->i_rwsem 写锁 (VFS 保证), 此处无需额外锁. */
static void powerfs_invalidate_dir_lease(struct inode *dir)
{
    struct powerfs_inode_info *dpi;

    if (!dir || !S_ISDIR(dir->i_mode))
        return;
    dpi = POWERFS_I(dir);
    /* dir_mutex 保护 dir_entries 链表; dir_lease_expire/epoch 是 atomic-like
     * 字段, 用 WRITE_ONCE 配合 readdir 的 READ_ONCE. */
    mutex_lock(&dpi->dir_mutex);
    WRITE_ONCE(dpi->dir_lease_expire, 0);
    dpi->dir_lease_epoch++;
    mutex_unlock(&dpi->dir_mutex);
}

/*
 * powerfs_readdir - 读取目录内容 (使用本地链表)
 *
 * 关键设计:
 *   1. 在 dir_mutex 保护下复制目录项到临时数组
 *   2. 释放 dir_mutex 后再调用 dir_emit
 *
 * 为什么不能在 dir_mutex 内调用 dir_emit:
 *   - dir_emit 可能触发 d_revalidate (路径遍历)
 *   - create/unlink 也需要 dir_mutex
 *   - 如果在 dir_mutex 内调用 dir_emit，而 dir_emit 内部
 *     触发 d_revalidate 需要 d_lock，同时 create 持有
 *     dir_mutex 尝试获取 d_lock → ABBA 死锁
 *
 * 解决方案:
 *   - 在锁内快速复制所有条目 (O(n) 一次性)
 *   - 释放锁后逐条 emit，允许并发修改
 */
int powerfs_readdir(struct file *file, struct dir_context *ctx)
{
    struct inode *dir = file_inode(file);
    struct powerfs_inode_info *dpi = POWERFS_I(dir);
    struct powerfs_dir_entry *entry, *tmp;
    loff_t pos = 0;

    /* 处理 "." 和 ".." */
    if (ctx->pos == 0) {
        if (!dir_emit_dots(file, ctx))
            return 0;
    }

    pos = ctx->pos - 2;

    /* === Phase 1: 目录 lease fast-path ===
     * dir_complete && dir_lease_expire 未过期: 直接用本地缓存, 不发网络.
     * 30s 内重复 readdir 走纯内存 (POWERFS_DIR_LEASE_TTL).
     * lease 过期或 dir_complete=false: 清缓存重新拉取 (本地 mutation 后). */
    if (READ_ONCE(dpi->dir_complete) &&
        time_before(jiffies, READ_ONCE(dpi->dir_lease_expire))) {
        pr_debug("powerfs: readdir ino=%lu lease fast-path (cached)\n",
                 dir->i_ino);
        goto emit_cached;
    }

    /* 如果目录 lease 过期或缓存为空, 且网络可用, 从 Filer 获取目录列表.
     * lease 过期但缓存非空: 先清缓存 (避免新旧条目混合). */
    if (powerfs_net_is_connected()) {
        struct powerfs_net_dir_entry *net_entries;
        __u32 net_count = 0;
        bool has_more = false;
        char last_name[256] = "";
        int ret;
        int timeout_ms;
        bool pulled_any = false;  /* 是否已拉取到至少一页 (超时容错) */

        /* lease 过期: 标记需要重新拉取, 但不清空 dir_entries.
         * 原因: 本地 mutation (create/mkdir) 通过 powerfs_add_dir_entry 添加的条目
         * 可能尚未出现在 Filer 的 readdir 响应中, 清空会导致这些条目丢失.
         * 网络拉取时通过 ino 去重, 旧条目保留, 新条目追加. */
        if (READ_ONCE(dpi->dir_complete) &&
            time_after(jiffies, READ_ONCE(dpi->dir_lease_expire))) {
            WRITE_ONCE(dpi->dir_complete, false);
            WRITE_ONCE(dpi->dir_lease_expire, 0);
        }

        net_entries = kmalloc_array(256, sizeof(*net_entries), GFP_KERNEL);
        if (!net_entries)
            return -ENOMEM;

        /* Phase 1: 短超时策略 (同 lookup). */
        timeout_ms = powerfs_net_pick_timeout(POWERFS_READDIR_TIMEOUT_MS);

        /* 从 Filer 获取目录条目 (分页循环直到获取全部) */
        do {
            ret = powerfs_net_readdir_timeout(dir->i_ino, last_name, 256,
                                              net_entries, 256,
                                              &net_count, &has_more,
                                              timeout_ms);
            if (ret < 0) {
                /* Phase 1: 瞬态错误处理.
                 * - 已拉取部分页: 用已拉取的, 不返回错误 (best-effort).
                 * - 一页都没拉到:
                 *   * 缓存有内容 (lease 过期但未清): 用旧缓存
                 *   * 缓存为空: 返回 -EAGAIN 让 VFS/应用层重试 */
                if (pulled_any) {
                    pr_warn("powerfs: readdir ino=%lu partial fetch err=%d, "
                            "using entries pulled so far\n",
                            dir->i_ino, ret);
                    break;
                }
                if (!list_empty(&dpi->dir_entries)) {
                    pr_warn("powerfs: readdir ino=%lu net err=%d, "
                            "using stale cache\n", dir->i_ino, ret);
                    break;
                }
                if (ret == -ETIMEDOUT || ret == -ENOTCONN ||
                    ret == -ESHUTDOWN) {
                    pr_warn("powerfs: readdir ino=%lu transient err=%d "
                            "(timeout_ms=%d), return -EAGAIN\n",
                            dir->i_ino, ret, timeout_ms);
                    kfree(net_entries);
                    return -EAGAIN;
                }
                pr_warn("powerfs: readdir ino=%lu net error: %d\n",
                        dir->i_ino, ret);
                kfree(net_entries);
                return ret;
            }
            pulled_any = true;

            /* 将网络条目添加到本地缓存 */
            mutex_lock(&dpi->dir_mutex);
            for (__u32 i = 0; i < net_count; i++) {
                struct powerfs_dir_entry *de;
                struct powerfs_net_dir_entry *ne = &net_entries[i];

                /* 检查是否已存在 (按名称去重, 不能用 ino — hardlink
                 * 的多个目录项共享同一 inode 但名称不同) */
                bool found = false;
                list_for_each_entry(de, &dpi->dir_entries, list) {
                    if (strcmp(de->name, ne->name) == 0) {
                        found = true;
                        break;
                    }
                }
                if (found)
                    continue;

                de = kmalloc(sizeof(*de), GFP_KERNEL);
                if (!de) {
                    mutex_unlock(&dpi->dir_mutex);
                    kfree(net_entries);
                    return -ENOMEM;
                }

                de->ino = ne->ino;
                de->type = ne->mode & S_IFMT;
                strncpy(de->name, ne->name, POWERFS_MAX_NAME_LEN - 1);
                de->name[POWERFS_MAX_NAME_LEN - 1] = '\0';
                list_add_tail(&de->list, &dpi->dir_entries);

                /* 更新 last_name 用于分页 */
                strncpy(last_name, ne->name, sizeof(last_name) - 1);
                last_name[sizeof(last_name) - 1] = '\0';
            }
            mutex_unlock(&dpi->dir_mutex);
        } while (has_more && net_count > 0);

        kfree(net_entries);

        /* 拉取成功 (或部分成功): 设置 dir_complete + dir_lease_expire.
         * 部分成功时也设 dir_complete (避免反复部分拉取), 下次 lease 过期再补. */
        WRITE_ONCE(dpi->dir_complete, true);
        WRITE_ONCE(dpi->dir_lease_expire, jiffies + POWERFS_DIR_LEASE_TTL);
    }

emit_cached:

    /*
     * 第一阶段: 在锁内复制目录项到临时缓冲区
     */
    {
        struct dentry_emit_entry {
            u64 ino;
            unsigned int type;
            unsigned short namelen;
            char name[POWERFS_MAX_NAME_LEN];
        } *buf;
        int count = 0;
        int max = 256;
        int i;

        buf = kmalloc_array(max, sizeof(*buf), GFP_KERNEL);
        if (!buf) {
            /* 分配失败，直接在锁内 emit (回退方案) */
            mutex_lock(&dpi->dir_mutex);
            list_for_each_entry_safe(entry, tmp, &dpi->dir_entries, list) {
                unsigned char d_type;
                if (pos > 0) { pos--; continue; }
                switch (entry->type) {
                case S_IFREG:  d_type = DT_REG; break;
                case S_IFDIR:  d_type = DT_DIR; break;
                case S_IFLNK:  d_type = DT_LNK; break;
                default:       d_type = DT_UNKNOWN; break;
                }
                if (!dir_emit(ctx, entry->name, strlen(entry->name),
                              entry->ino, d_type)) {
                    mutex_unlock(&dpi->dir_mutex);
                    return 0;
                }
                ctx->pos++;
            }
            mutex_unlock(&dpi->dir_mutex);
            return 0;
        }

        mutex_lock(&dpi->dir_mutex);

        list_for_each_entry_safe(entry, tmp, &dpi->dir_entries, list) {
            if (count >= max)
                break;
            if (pos > 0) {
                pos--;
                continue;
            }
            buf[count].ino = entry->ino;
            buf[count].type = entry->type;
            buf[count].namelen = strlen(entry->name);
            memcpy(buf[count].name, entry->name, buf[count].namelen + 1);
            count++;
        }

        mutex_unlock(&dpi->dir_mutex);

        /* 第二阶段: 释放锁后逐条 emit */
        for (i = 0; i < count; i++) {
            unsigned char d_type;

            switch (buf[i].type) {
            case S_IFREG:  d_type = DT_REG; break;
            case S_IFDIR:  d_type = DT_DIR; break;
            case S_IFLNK:  d_type = DT_LNK; break;
            default:       d_type = DT_UNKNOWN; break;
            }

            if (!dir_emit(ctx, buf[i].name, buf[i].namelen,
                          buf[i].ino, d_type)) {
                kfree(buf);
                return 0;
            }

            ctx->pos++;
        }

        kfree(buf);
    }

    return 0;
}

/* 目录文件操作表 - 使用本地链表实现 readdir */
static const struct file_operations powerfs_dir_operations = {
    .open           = powerfs_dir_open,
    .release        = powerfs_dir_release,
    .iterate_shared = powerfs_readdir,
    .llseek         = generic_file_llseek,
};

/* ========== 地址空间操作 (page cache) ========== */

/* powerfs_read_folio 已移除 (Stage C): read 路径由 netfs_read_folio +
 * powerfs_netfs_issue_read 接管, 参照 ceph_aops.read_folio = netfs_read_folio. */

/* ========== Stage C: writepages 批量异步写入 ==========
 *
 * 参照 ceph_writepages_start 模式, 批量收集脏页减少 work item 数量:
 *   1. writepages 遍历脏页, 收集到 powerfs_writepage_work 批量结构
 *   2. batch 满 (sbi->write_batch_pages 页) 或遍历完成时提交 work item
 *   3. workqueue 线程串行发送 batch 内所有页面
 *   4. max_active=4 限制全局并发 worker 数
 *
 * 批量大小可配 (mount option write_batch_kb):
 *   - 默认 64KB (16 pages): TCP 网络
 *   - ROCE 推荐 1MB (256 pages): 高吞吐
 *   - 最大 stripe size 64MB (16384 pages): 单 stripe 一次提交
 *
 * 性能: 1MB 文件, batch=1MB → 1 个 work item (vs 旧方案 256 个)
 *
 * work 结构体使用 kvmalloc 单块分配 (header + 3 个动态数组), 因为
 * max 可达 16384 页 (数组 ~384KB, 超过 kmalloc 上限). */

struct powerfs_writepage_work {
    struct work_struct work;
    struct inode *inode;
    int num_pages;
    int max_pages;
    struct page **pages;     /* max_pages 项, 紧随结构体后 */
    loff_t *offsets;         /* max_pages 项, 紧随 pages 后 */
    size_t *counts;          /* max_pages 项, 紧随 offsets 后 */
    /* 异步提交计数: 跟踪尚未完成的 needle 写入. work_fn 持有 +1 ref,
     * 每个 needle ctx 完成时 dec. 归零时执行 final cleanup (iput/dec/kvfree).
     * 防止提前归零: work_fn 在所有 ctx 提交后才 dec 自己的 ref. */
    atomic_t pending_needles;
    /* RCU 延迟释放头: work_struct 在 wpw 中, work_fn 返回后 workqueue 仍
     * 访问 work->data. 用 call_rcu 延迟 kvfree 到 RCU 宽限期后, 确保
     * workqueue 对 work_struct 的访问已完成. */
    struct rcu_head rcu;
};

/*
 * powerfs_alloc_write_batch - 分配批量写 work 结构 (含动态数组)
 *
 * 单块 kvmalloc: [header][pages[]][offsets[]][counts[]]
 * 大批量 (如 64MB=16384 页) 时数组 ~384KB, 需 kvmalloc 自动回退 vmalloc.
 */
static struct powerfs_writepage_work *
powerfs_alloc_write_batch(int max_pages, gfp_t gfp)
{
    struct powerfs_writepage_work *batch;
    size_t arr_sz = (size_t)max_pages *
                    (sizeof(struct page *) + sizeof(loff_t) + sizeof(size_t));
    size_t sz = sizeof(*batch) + arr_sz;

    batch = kvmalloc(sz, gfp | __GFP_ZERO);
    if (!batch)
        return NULL;

    batch->max_pages = max_pages;
    batch->num_pages = 0;
    batch->pages = (struct page **)(batch + 1);
    batch->offsets = (loff_t *)(batch->pages + max_pages);
    batch->counts = (size_t *)(batch->offsets + max_pages);
    return batch;
}

/*
 * ========== Stage D: page writeback 异步提交模式 ==========
 *
 * 旧同步实现 (powerfs_net_write_needle/read_needle) 在 workqueue 线程内
 * wait_for_completion 等待响应 (最长 30s/请求), 多个 needle 串行 →
 * workqueue lockup 598s. 新实现将 read_needle + write_needle 改为异步提交:
 *
 *   writepage_work_fn (workqueue 线程, 快速返回):
 *     1. 按 needle_id 分组 batch 内页面
 *     2. 每个 needle 组: alloc ctx (含 2MB needle_buf), 获取 lease (同步, 快速),
 *        submit read_needle_async (非阻塞入队)
 *     3. work_fn 返回, workqueue 线程立即释放
 *
 *   read_cb (调度器 RX 上下文, 响应到达时):
 *     1. 读取结果填入 ctx->needle_buf (scheduler 已完成 memcpy)
 *     2. 合并脏页数据到 needle_buf (read-modify-write 的 modify)
 *     3. submit write_needle_async (非阻塞入队)
 *
 *   write_cb (调度器 RX 上下文, 写响应到达时):
 *     1. end_page_writeback + put_page (清除该 needle 的所有页)
 *     2. free ctx (needle_buf + ctx)
 *     3. atomic_dec pending_needles; 归零则 final_cleanup (iput/dec/kvfree wpw)
 *
 * 引用计数 (pending_needles):
 *   - 初始 = num_needle_groups + 1 (work_fn 持有 +1 ref)
 *   - 每个 ctx 完成 (成功或失败) dec 1
 *   - work_fn 在所有 ctx 提交后 dec 自己的 +1
 *   - 归零时 final_cleanup, 防止提前归零 (work_fn 仍在访问 wpw)
 *
 * 超时兜底: 异步请求用 delayed_work (system_wq) 在 deadline 到期时以
 *   -ETIMEDOUT 完成, 防止 page 永久滞留 PageWriteback → hung task.
 */

/* Per-needle 异步上下文 (一个 needle 组对应一个 ctx) */
struct powerfs_wb_ctx {
    struct powerfs_writepage_work *wpw;   /* 所属 batch */
    int needle_start_idx;                  /* wpw->pages 中的起始索引 */
    int needle_end_idx;                    /* 结束索引 (exclusive) */
    __u64 volume_id;
    __u64 needle_id;
    __u8 *needle_buf;                      /* 2MB, per-ctx (read 写入, write 读出) */
    __u32 needle_len;                      /* needle 有效数据长度 */
    char lease_token[64];
    size_t lease_token_len;
    /* 持久缓冲区: 异步请求的 req_body / resp_body, 存活到 callback 触发 */
    __u8 req_body[256];
    __u8 resp_body[64];
};

/* 前向声明 */
static void powerfs_wb_final_cleanup(struct powerfs_writepage_work *wpw);
static int powerfs_wb_read_cb(struct powerfs_request *req);
static int powerfs_wb_write_cb(struct powerfs_request *req);

/* powerfs_wb_fail_pages - 失败一个 ctx 范围内的所有页面
 *
 * 用于 ctx 分配失败或异步提交失败时, 同步清理页面 (在 work_fn 上下文).
 * 调用方负责 dec pending_needles + free ctx. */
static void powerfs_wb_fail_pages(struct powerfs_wb_ctx *ctx, int err)
{
    struct powerfs_writepage_work *wpw = ctx->wpw;
    int j;

    for (j = ctx->needle_start_idx; j < ctx->needle_end_idx; j++) {
        struct page *p = wpw->pages[j];
        if (wpw->counts[j] == 0)
            continue;
        if (err < 0)
            mapping_set_error(p->mapping, err);
        end_page_writeback(p);
        put_page(p);
    }
}

/* powerfs_wb_free_rcu - RCU 回调: 延迟释放 wpw 内存
 *
 * work_struct 在 wpw 中, work_fn 返回后 workqueue 子系统仍访问 work->data
 * (清除 WORK_BUSY_PENDING 等标志). 直接 kvfree 会导致 use-after-free
 * (assign_work NULL deref oops). 用 call_rcu 延迟到 RCU 宽限期后释放,
 * 确保 workqueue 对 work_struct 的访问已完成. */
static void powerfs_wb_free_rcu(struct rcu_head *rcu)
{
    struct powerfs_writepage_work *wpw =
        container_of(rcu, struct powerfs_writepage_work, rcu);
    kvfree(wpw);
}

/* powerfs_wb_final_cleanup - 所有 needle 完成后的最终清理
 *
 * 释放 batch 级资源: inode 引用, wb_in_flight 计数.
 * wpw 内存通过 call_rcu 延迟释放 (work_struct 生命周期约束).
 * 由最后一个完成的 ctx (或 work_fn 的自身 ref) 触发. */
static void powerfs_wb_final_cleanup(struct powerfs_writepage_work *wpw)
{
    struct inode *inode = wpw->inode;
    struct super_block *sb = inode->i_sb;
    struct powerfs_sb_info *sbi = POWERFS_SB_INFO(sb);
    struct powerfs_inode_info *pi = POWERFS_I(inode);

    pr_debug("powerfs: WB_FINAL_CLEANUP ino=%lu wb_in_flight=%d\n",
            inode->i_ino, atomic_read(&sbi->wb_in_flight));
    atomic_dec(&sbi->wb_in_flight);

    /* writeback 互斥: 最后一个 batch 完成时释放 wb_mutex,
     * 允许下一次 powerfs_writepages 执行. */
    if (atomic_dec_and_test(&pi->wb_batch_count))
        mutex_unlock(&pi->wb_mutex);

    iput(inode);
    /* 延迟释放 wpw: work_struct 在 wpw 中, workqueue 在 work_fn 返回后
     * 仍访问 work->data. call_rcu 等待 RCU 宽限期后释放. */
    call_rcu(&wpw->rcu, powerfs_wb_free_rcu);
}

/* powerfs_wb_read_cb - read_needle 异步完成回调
 *
 * 由 powerfs_req_complete 在调度器 RX 上下文 (process context) 调用.
 * 1. 处理读取结果 (NOT_FOUND=新 needle, 其他错误=失败页面)
 * 2. 合并脏页数据到 needle_buf (read-modify-write 的 modify)
 * 3. 提交 write_needle_async 进入第二阶段
 *
 * 注意: 此函数在调度器线程上下文执行, 应避免长时间阻塞. 合并操作是
 * N×4KB memcpy (16页=64KB ≈ 32µs), 可接受. write_needle_async 仅入队, 不阻塞. */
static int powerfs_wb_read_cb(struct powerfs_request *req)
{
    struct powerfs_wb_ctx *ctx = req->priv;
    struct powerfs_writepage_work *wpw = ctx->wpw;
    struct inode *inode = wpw->inode;
    int ret;

    pr_debug("powerfs: WB_READ_CB ino=%lu nid=%llu err=%d status=%u pages=[%d,%d)\n",
            inode->i_ino, (unsigned long long)ctx->needle_id,
            req->error, req->resp_status,
            ctx->needle_start_idx, ctx->needle_end_idx);

    /* 解析读取结果 */
    if (req->error < 0) {
        /* 网络错误 (断连/超时/截断): 失败页面 */
        pr_warn("powerfs: wb read_cb ino=%lu nid=%llu net_err=%d\n",
                inode->i_ino, (unsigned long long)ctx->needle_id,
                req->error);
        powerfs_wb_fail_pages(ctx, req->error);
        powerfs_request_free(req);
        kvfree(ctx->needle_buf);
        kfree(ctx);
        if (atomic_dec_and_test(&wpw->pending_needles))
            powerfs_wb_final_cleanup(wpw);
        return 0;
    }

    if (req->resp_status == POWERFS_NET_STATUS_ERR_NOT_FOUND) {
        /* needle 不存在 (新文件首次写): 全零 needle, 正常继续 */
        ctx->needle_len = 0;
    } else if (req->resp_status != POWERFS_NET_STATUS_OK) {
        /* 其他服务端错误: 失败页面 */
        int err = net_status_to_errno(req->resp_status);
        pr_warn("powerfs: wb read_cb ino=%lu nid=%llu status=%u err=%d\n",
                inode->i_ino, (unsigned long long)ctx->needle_id,
                req->resp_status, err);
        powerfs_wb_fail_pages(ctx, err);
        powerfs_request_free(req);
        kvfree(ctx->needle_buf);
        kfree(ctx);
        if (atomic_dec_and_test(&wpw->pending_needles))
            powerfs_wb_final_cleanup(wpw);
        return 0;
    } else {
        /* 读取成功: scheduler 已将 needle 内容写入 ctx->needle_buf */
        ctx->needle_len = (__u32)req->resp_data_len;
    }

    powerfs_request_free(req);  /* 释放 read 请求 */

    /* 合并脏页数据到 needle_buf (read-modify-write 的 modify) */
    {
        int j;
        for (j = ctx->needle_start_idx; j < ctx->needle_end_idx; j++) {
            struct page *page = wpw->pages[j];
            loff_t offset = wpw->offsets[j];
            size_t count = wpw->counts[j];
            size_t offset_in_needle;

            if (count == 0)
                continue;

            offset_in_needle = offset % POWERFS_CHUNK_SIZE;
            {
                char *kaddr = kmap_local_page(page);
                memcpy(ctx->needle_buf + offset_in_needle, kaddr, count);
                kunmap_local(kaddr);
            }
            if (offset_in_needle + count > ctx->needle_len)
                ctx->needle_len = offset_in_needle + count;
        }
    }

    /* 提交 write_needle_async (第二阶段) */
    pr_debug("powerfs: WB_WRITE_SUBMIT ino=%lu nid=%llu len=%u\n",
            inode->i_ino, (unsigned long long)ctx->needle_id,
            ctx->needle_len);
    ret = powerfs_net_write_needle_async(
        ctx->volume_id, ctx->needle_id, inode->i_ino,
        ctx->needle_buf, ctx->needle_len,
        ctx->lease_token_len > 0 ? ctx->lease_token : NULL,
        ctx->lease_token_len,
        ctx->req_body, sizeof(ctx->req_body),
        ctx->resp_body, sizeof(ctx->resp_body),
        30000, powerfs_wb_write_cb, ctx);

    if (ret) {
        /* 提交失败: callback 不会触发, 手动清理 */
        pr_warn("powerfs: wb read_cb ino=%lu nid=%llu write submit failed: %d\n",
                inode->i_ino, (unsigned long long)ctx->needle_id, ret);
        powerfs_wb_fail_pages(ctx, ret);
        kvfree(ctx->needle_buf);
        kfree(ctx);
        if (atomic_dec_and_test(&wpw->pending_needles))
            powerfs_wb_final_cleanup(wpw);
    }

    return 0;
}

/* powerfs_wb_write_cb - write_needle 异步完成回调
 *
 * 写响应到达时调用, 清除该 needle 所有页的 PageWriteback, 释放 ctx.
 * 最后一个 needle 完成时触发 final_cleanup. */
static int powerfs_wb_write_cb(struct powerfs_request *req)
{
    struct powerfs_wb_ctx *ctx = req->priv;
    struct powerfs_writepage_work *wpw = ctx->wpw;
    struct inode *inode = wpw->inode;
    int err = 0;

    pr_debug("powerfs: WB_WRITE_CB ino=%lu nid=%llu err=%d status=%u pages=[%d,%d)\n",
            inode->i_ino, (unsigned long long)ctx->needle_id,
            req->error, req->resp_status,
            ctx->needle_start_idx, ctx->needle_end_idx);

    if (req->error < 0) {
        err = req->error;
    } else if (req->resp_status != POWERFS_NET_STATUS_OK) {
        err = net_status_to_errno(req->resp_status);
    }

    if (err)
        pr_warn("powerfs: wb write_cb ino=%lu nid=%llu err=%d status=%u\n",
                inode->i_ino, (unsigned long long)ctx->needle_id,
                err, req->resp_status);

    /* 完成该 needle 的所有页面 */
    {
        int j;
        for (j = ctx->needle_start_idx; j < ctx->needle_end_idx; j++) {
            struct page *p = wpw->pages[j];
            if (wpw->counts[j] == 0)
                continue;
            if (err)
                mapping_set_error(p->mapping, err);
            end_page_writeback(p);
            put_page(p);
        }
    }

    powerfs_request_free(req);
    kvfree(ctx->needle_buf);
    kfree(ctx);

    if (atomic_dec_and_test(&wpw->pending_needles))
        powerfs_wb_final_cleanup(wpw);

    return 0;
}

/*
 * powerfs_writepage_work_fn - 批量异步写 workqueue 函数 (异步提交模式)
 *
 * 按 needle_id 分组, 每组创建 ctx 并异步提交 read_needle. work_fn 在所有
 * ctx 提交后立即返回 (不阻塞等待网络响应), 由 read_cb → write_cb 两阶段
 * 回调完成实际写入并清除 PageWriteback.
 *
 * needle 模型: write_needle 整体替换 needle 内容, 不支持 partial write.
 * 需 read-modify-write: 读现有 needle (异步) → 合并脏页 (read_cb) → 写回 (异步).
 */
static void powerfs_writepage_work_fn(struct work_struct *work)
{
    struct powerfs_writepage_work *wpw =
        container_of(work, struct powerfs_writepage_work, work);
    struct inode *inode = wpw->inode;
    struct super_block *sb = inode->i_sb;
    struct powerfs_sb_info *sbi = POWERFS_SB_INFO(sb);
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    int i;
    int num_groups = 0;
    int group_start = -1;
    __u64 group_needle_id = 0;
    __u64 group_volume_id = 0;

    pr_debug("powerfs: WP_START ino=%lu npages=%d\n",
            inode->i_ino, wpw->num_pages);

    if (powerfs_net_is_stopping())
        goto fail_all;

    /* 第一遍: 完成 count==0 的空页, 统计 needle 组数.
     * K3: 按 offset 调用 powerfs_locate_chunk 获取 (volume_id, needle_id),
     *     分组判断改为 (needle_id, volume_id) 元组, 确保 Stripe 模式下
     *     不同 volume 的 chunk 分到不同组. */
    for (i = 0; i < wpw->num_pages; i++) {
        struct page *page = wpw->pages[i];
        loff_t offset = wpw->offsets[i];
        size_t count = wpw->counts[i];
        __u64 needle_id, volume_id;
        int loc_ret;

        cond_resched();

        if (count == 0) {
            /* 空页: 直接完成, 不参与 needle 分组 */
            end_page_writeback(page);
            put_page(page);
            continue;
        }

        spin_lock(&pi->i_lock);
        loc_ret = powerfs_locate_chunk(pi, offset, &volume_id, &needle_id);
        spin_unlock(&pi->i_lock);
        if (loc_ret) {
            pr_warn("powerfs: writepage locate ino=%lu offset=%lld err=%d\n",
                    inode->i_ino, offset, loc_ret);
            mapping_set_error(page->mapping, loc_ret);
            end_page_writeback(page);
            put_page(page);
            continue;
        }

        if (group_start < 0) {
            /* 开始新组 */
            group_start = i;
            group_needle_id = needle_id;
            group_volume_id = volume_id;
            num_groups++;
        } else if (needle_id != group_needle_id ||
                   volume_id != group_volume_id) {
            /* needle/volume 切换: 当前组结束, 开始新组 */
            group_start = i;
            group_needle_id = needle_id;
            group_volume_id = volume_id;
            num_groups++;
        }
    }

    /* 若无有效页面 (全为 count==0 或 locate 失败), 直接清理 */
    if (num_groups == 0) {
        atomic_dec(&sbi->wb_in_flight);
        /* writeback 互斥: 最后一个 batch 完成时释放 wb_mutex */
        if (atomic_dec_and_test(&pi->wb_batch_count))
            mutex_unlock(&pi->wb_mutex);
        iput(inode);
        /* 延迟释放: 同 final_cleanup, work_fn 上下文不能直接 kvfree wpw. */
        call_rcu(&wpw->rcu, powerfs_wb_free_rcu);
        return;
    }

    /* 设置计数: num_groups + 1 (work_fn 持有 +1 ref, 防止提前归零) */
    atomic_set(&wpw->pending_needles, num_groups + 1);

    /* 第二遍: 为每个 needle 组创建 ctx 并异步提交 read_needle.
     * K3: 每个页面持锁 locate 获取 (volume_id, needle_id), 组切换时用
     *     cur_volume_id/cur_needle_id 设置 ctx (Stripe 模式下每组 volume 不同). */
    {
        int cur_start = -1;
        __u64 cur_needle_id = 0;
        __u64 cur_volume_id = 0;

        for (i = 0; i < wpw->num_pages; i++) {
            loff_t offset = wpw->offsets[i];
            size_t count = wpw->counts[i];
            __u64 needle_id, volume_id;
            int loc_ret;

            if (count == 0)
                continue;

            spin_lock(&pi->i_lock);
            loc_ret = powerfs_locate_chunk(pi, offset, &volume_id, &needle_id);
            spin_unlock(&pi->i_lock);
            if (loc_ret) {
                /* locate 失败: 跳过该页 (第一遍已 end_page_writeback) */
                continue;
            }

            if (cur_start < 0) {
                cur_start = i;
                cur_needle_id = needle_id;
                cur_volume_id = volume_id;
            } else if (needle_id != cur_needle_id ||
                       volume_id != cur_volume_id) {
                /* 提交 [cur_start, i) 这一组 */
                struct powerfs_wb_ctx *ctx;
                loff_t group_offset;
                size_t token_len = 0;
                int ret;

                ctx = kzalloc(sizeof(*ctx), GFP_NOFS);
                if (!ctx) {
                    /* 分配失败: 失败该组页面, dec 计数 (此 ctx 不会回调) */
                    struct powerfs_wb_ctx fail_ctx = {
                        .wpw = wpw,
                        .needle_start_idx = cur_start,
                        .needle_end_idx = i,
                    };
                    powerfs_wb_fail_pages(&fail_ctx, -ENOMEM);
                    atomic_dec(&wpw->pending_needles);
                    goto next_group;
                }

                ctx->needle_buf = kvmalloc(POWERFS_CHUNK_SIZE, GFP_NOFS);
                if (!ctx->needle_buf) {
                    struct powerfs_wb_ctx fail_ctx = {
                        .wpw = wpw,
                        .needle_start_idx = cur_start,
                        .needle_end_idx = i,
                    };
                    kfree(ctx);
                    powerfs_wb_fail_pages(&fail_ctx, -ENOMEM);
                    atomic_dec(&wpw->pending_needles);
                    goto next_group;
                }
                memset(ctx->needle_buf, 0, POWERFS_CHUNK_SIZE);

                ctx->wpw = wpw;
                ctx->needle_start_idx = cur_start;
                ctx->needle_end_idx = i;
                ctx->volume_id = cur_volume_id;
                ctx->needle_id = cur_needle_id;
                ctx->needle_len = 0;

                /* 获取 lease (同步, 快速: ensure_lease 有快速路径) */
                group_offset = wpw->offsets[cur_start];
                if (powerfs_get_lease_token(inode, group_offset,
                                            ctx->lease_token,
                                            &token_len)) {
                    pr_warn("powerfs: writepage lease ino=%lu stripe=%llu, continuing without lease\n",
                            inode->i_ino,
                            (unsigned long long)(group_offset / POWERFS_STRIPE_SIZE
                                                 * POWERFS_STRIPE_SIZE));
                }
                ctx->lease_token_len = token_len;

                /* 异步提交 read_needle (非阻塞) */
                pr_debug("powerfs: WB_READ_SUBMIT ino=%lu vid=%llu nid=%llu pages=[%d,%d)\n",
                        inode->i_ino, (unsigned long long)cur_volume_id,
                        (unsigned long long)cur_needle_id, cur_start, i);
                ret = powerfs_net_read_needle_async(
                    cur_volume_id, cur_needle_id,
                    ctx->needle_buf, POWERFS_CHUNK_SIZE,
                    ctx->req_body, sizeof(ctx->req_body),
                    30000, powerfs_wb_read_cb, ctx);

                if (ret) {
                    /* 提交失败: callback 不会触发, 手动清理 */
                    pr_warn("powerfs: writepage read submit ino=%lu nid=%llu err=%d\n",
                            inode->i_ino, (unsigned long long)cur_needle_id,
                            ret);
                    powerfs_wb_fail_pages(ctx, ret);
                    kvfree(ctx->needle_buf);
                    kfree(ctx);
                    /* dec 计数: 此 ctx 不会回调 */
                    if (atomic_dec_and_test(&wpw->pending_needles))
                        powerfs_wb_final_cleanup(wpw);
                }

next_group:
                cur_start = i;
                cur_needle_id = needle_id;
                cur_volume_id = volume_id;
            }
        }

        /* 提交最后一组 [cur_start, num_pages) */
        if (cur_start >= 0) {
            struct powerfs_wb_ctx *ctx;
            loff_t group_offset;
            size_t token_len = 0;
            int ret;

            ctx = kzalloc(sizeof(*ctx), GFP_NOFS);
            if (!ctx) {
                struct powerfs_wb_ctx fail_ctx = {
                    .wpw = wpw,
                    .needle_start_idx = cur_start,
                    .needle_end_idx = wpw->num_pages,
                };
                powerfs_wb_fail_pages(&fail_ctx, -ENOMEM);
                atomic_dec(&wpw->pending_needles);
                goto last_group_done;
            }

            ctx->needle_buf = kvmalloc(POWERFS_CHUNK_SIZE, GFP_NOFS);
            if (!ctx->needle_buf) {
                struct powerfs_wb_ctx fail_ctx = {
                    .wpw = wpw,
                    .needle_start_idx = cur_start,
                    .needle_end_idx = wpw->num_pages,
                };
                kfree(ctx);
                powerfs_wb_fail_pages(&fail_ctx, -ENOMEM);
                atomic_dec(&wpw->pending_needles);
                goto last_group_done;
            }
            memset(ctx->needle_buf, 0, POWERFS_CHUNK_SIZE);

            ctx->wpw = wpw;
            ctx->needle_start_idx = cur_start;
            ctx->needle_end_idx = wpw->num_pages;
            ctx->volume_id = cur_volume_id;
            ctx->needle_id = cur_needle_id;
            ctx->needle_len = 0;

            group_offset = wpw->offsets[cur_start];
            if (powerfs_get_lease_token(inode, group_offset,
                                        ctx->lease_token,
                                        &token_len)) {
                pr_warn("powerfs: writepage final lease ino=%lu stripe=%llu, continuing without lease\n",
                        inode->i_ino,
                        (unsigned long long)(group_offset / POWERFS_STRIPE_SIZE
                                             * POWERFS_STRIPE_SIZE));
            }
            ctx->lease_token_len = token_len;

            ret = powerfs_net_read_needle_async(
                cur_volume_id, cur_needle_id,
                ctx->needle_buf, POWERFS_CHUNK_SIZE,
                ctx->req_body, sizeof(ctx->req_body),
                30000, powerfs_wb_read_cb, ctx);

            if (ret) {
                pr_warn("powerfs: writepage read submit ino=%lu nid=%llu err=%d\n",
                        inode->i_ino, (unsigned long long)cur_needle_id,
                        ret);
                powerfs_wb_fail_pages(ctx, ret);
                kvfree(ctx->needle_buf);
                kfree(ctx);
                if (atomic_dec_and_test(&wpw->pending_needles))
                    powerfs_wb_final_cleanup(wpw);
            }
        }
    }

last_group_done:
    /* work_fn 自身 ref: 所有 ctx 已提交 (或失败已处理), dec 自身引用.
     * 若所有 ctx 已完成 (极端竞态: 提交后立即回调), 此处归零触发 final_cleanup. */
    if (atomic_dec_and_test(&wpw->pending_needles))
        powerfs_wb_final_cleanup(wpw);

    pr_debug("powerfs: WP_SUBMIT_DONE ino=%lu groups=%d\n",
            inode->i_ino, num_groups);
    return;  /* workqueue 线程立即释放 */

fail_all:
    for (i = 0; i < wpw->num_pages; i++) {
        mapping_set_error(wpw->pages[i]->mapping, -EIO);
        end_page_writeback(wpw->pages[i]);
        put_page(wpw->pages[i]);
    }
    atomic_dec(&sbi->wb_in_flight);
    /* writeback 互斥: 最后一个 batch 完成时释放 wb_mutex */
    if (atomic_dec_and_test(&pi->wb_batch_count))
        mutex_unlock(&pi->wb_mutex);
    iput(inode);
    /* 延迟释放: 同 final_cleanup, work_fn 上下文不能直接 kvfree wpw
     * (work_struct 生命周期约束). */
    call_rcu(&wpw->rcu, powerfs_wb_free_rcu);
}

/*
 * powerfs_writepages - 批量 writeback 入口
 *
 * 自己遍历脏页 (pagevec_lookup_range_tag), 批量收集到 work item.
 * 替代 VFS 默认的 write_cache_pages + writepage 逐页模式.
 *
 * 并发控制:
 *   - max_active=4 限制全局并发 worker (workqueue 级)
 *   - batch 内串行发送 (单 work item 内页面顺序写)
 *   - writepages 由 writeback 子系统串行调用 (per-inode 不会并发)
 */
int powerfs_writepages(struct address_space *mapping,
                               struct writeback_control *wbc)
{
    struct inode *inode = mapping->host;
    struct super_block *sb = inode->i_sb;
    struct powerfs_sb_info *sbi = POWERFS_SB_INFO(sb);
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    struct folio_batch fbatch;
    pgoff_t index = wbc->range_start >> PAGE_SHIFT;
    pgoff_t end = wbc->range_end >> PAGE_SHIFT;
    struct powerfs_writepage_work *batch = NULL;
    int batch_pages = sbi->write_batch_pages;
    int ret = 0;
    bool wb_submitted = false;

    pr_debug("powerfs: WPAGES ino=%lu range=%llu-%llu nr_to_write=%ld sync_mode=%d\n",
            inode->i_ino, wbc->range_start, wbc->range_end, wbc->nr_to_write,
            wbc->sync_mode);

    /* 目录无数据页: 跳过 writeback 避免无意义的网络 I/O.
     * 根目录 (ino=1) 的元数据修改 (nlink/mtime) 由 write_inode 同步,
     * 不走 page cache writeback 路径. */
    if (S_ISDIR(inode->i_mode)) {
        pr_debug("powerfs: WPAGES skip directory ino=%lu\n", inode->i_ino);
        return 0;
    }

    /* K2: Inline 文件不走 Volume Server writeback.
     * 数据已在 write_end 中同步到 inline_data 缓冲,
     * close 时通过 UPDATE_INODE 提交到 Filer.
     * 这里清理脏页标记, 让 writeback 认为已完成. */
    if (POWERFS_I(inode)->placement == POWERFS_PLACEMENT_INLINE) {
        struct folio_batch fbatch2;
        pgoff_t idx2 = wbc->range_start >> PAGE_SHIFT;
        pgoff_t end2 = wbc->range_end >> PAGE_SHIFT;

        pr_debug("powerfs: WPAGES INLINE ino=%lu, clean dirty pages\n", inode->i_ino);
        folio_batch_init(&fbatch2);
        while (filemap_get_folios_tag(mapping, &idx2, end2,
                                       PAGECACHE_TAG_DIRTY, &fbatch2) > 0) {
            int i;
            for (i = 0; i < folio_batch_count(&fbatch2); i++) {
                struct folio *f = fbatch2.folios[i];
                folio_lock(f);
                folio_clear_dirty(f);
                folio_unlock(f);
                folio_put(f);
            }
            folio_batch_init(&fbatch2);
            if (idx2 > end2)
                break;
        }
        return 0;
    }

    if (powerfs_net_is_stopping()) {
        pr_debug("powerfs: WPAGES net stopping, skip ino=%lu\n", inode->i_ino);
        return 0;
    }

    /* 并发限制: 防止过多 work item 同时阻塞在网络 I/O 导致 workqueue lockup.
     * 如果已有太多 in-flight work item, 让 VFS 稍后重试. */
    if (atomic_read(&sbi->wb_in_flight) >= POWERFS_WB_MAX_IN_FLIGHT) {
        pr_debug("powerfs: WPAGES throttled ino=%lu in_flight=%d\n",
                inode->i_ino, atomic_read(&sbi->wb_in_flight));
        wbc->pages_skipped += wbc->nr_to_write;
        return 0;
    }

    /* writeback 互斥: 确保同一 inode 的 writeback 串行执行.
     * powerfs_writepages 是异步的 (queue_work), 返回后异步 work 可能仍在执行.
     * 若 writeback 线程再次调用 powerfs_writepages 处理同一 needle 的不同
     * 页面, 两个 RMW 并发会导致后写入的覆盖先写入的数据 (data corruption).
     * mutex_lock 等待上一次 writeback 的所有 batch 完成. */
    mutex_lock(&pi->wb_mutex);

    folio_batch_init(&fbatch);

    while (index <= end) {
        int nr_pages, i;

        nr_pages = filemap_get_folios_tag(mapping, &index, end,
                                           PAGECACHE_TAG_DIRTY, &fbatch);
        if (!nr_pages) {
            pr_debug("powerfs: WPAGES no dirty pages, index=%lu end=%lu\n",
                    index, end);
            break;
        }
        pr_debug("powerfs: WPAGES found %d dirty pages, index=%lu\n",
                nr_pages, index);

        for (i = 0; i < nr_pages; i++) {
            struct page *page = folio_page(fbatch.folios[i], 0);
            loff_t offset;
            size_t count = PAGE_SIZE;
            u64 cur_needle_idx;

            lock_page(page);
            if (!PageDirty(page)) {
                pr_debug("powerfs: WPAGES page not dirty, skip\n");
                unlock_page(page);
                continue;
            }

            clear_page_dirty_for_io(page);

            offset = page_offset(page);
            /* 按 needle (chunk) 边界分组: 当页面的 chunk_idx 变化时,
             * 提交当前 batch, 确保同一 needle 的所有页面在同一个 batch
             * 中完成 read-modify-write. 避免多个 batch 并发对同一 needle
             * 做 RMW 导致数据覆盖 (1MB 文件 16 个 batch 并发覆盖问题). */
            cur_needle_idx = offset / POWERFS_CHUNK_SIZE;
            if (batch && batch->num_pages > 0) {
                u64 prev_needle_idx = batch->offsets[batch->num_pages - 1]
                                      / POWERFS_CHUNK_SIZE;
                if (cur_needle_idx != prev_needle_idx) {
                    /* needle 边界变化: 提交当前 batch */
                    atomic_inc(&pi->wb_batch_count);
                    wb_submitted = true;
                    atomic_inc(&sbi->wb_in_flight);
                    queue_work(sbi->writeback_wq, &batch->work);
                    batch = NULL;
                }
            }

            pr_debug("powerfs: WPAGES page ino=%lu offset=%lld i_size=%lld\n",
                    inode->i_ino, offset, i_size_read(inode));
            if (offset >= i_size_read(inode)) {
                pr_debug("powerfs: WPAGES offset >= i_size, SKIP page\n");
                unlock_page(page);
                continue;
            }
            if (offset + count > i_size_read(inode))
                count = i_size_read(inode) - offset;

            /* 分配 batch (如果当前没有 pending) */
            if (!batch) {
                struct inode *grabbed;
                batch = powerfs_alloc_write_batch(batch_pages, GFP_NOFS);
                if (!batch) {
                    redirty_page_for_writepage(wbc, page);
                    unlock_page(page);
                    continue;
                }
                grabbed = igrab(inode);
                if (!grabbed) {
                    /* inode 正在被 evict (I_FREEING), 无法提交 writeback.
                     * 释放 batch, redirty 页面. */
                    kvfree(batch);
                    batch = NULL;
                    redirty_page_for_writepage(wbc, page);
                    unlock_page(page);
                    continue;
                }
                INIT_WORK(&batch->work, powerfs_writepage_work_fn);
                batch->inode = grabbed;
            }

            /* 添加页面到 batch */
            get_page(page);
            batch->pages[batch->num_pages] = page;
            batch->offsets[batch->num_pages] = offset;
            batch->counts[batch->num_pages] = count;
            batch->num_pages++;

            set_page_writeback(page);
            unlock_page(page);

            /* batch 满了，提交到 workqueue */
            if (batch->num_pages >= batch_pages) {
                atomic_inc(&pi->wb_batch_count);
                wb_submitted = true;
                atomic_inc(&sbi->wb_in_flight);
                queue_work(sbi->writeback_wq, &batch->work);
                batch = NULL;
            }

            wbc->nr_to_write--;
            if (wbc->nr_to_write <= 0)
                goto done;
        }
        folio_batch_release(&fbatch);
        cond_resched();
    }

done:
    folio_batch_release(&fbatch);

    /* 提交剩余的 batch */
    if (batch) {
        if (batch->num_pages > 0) {
            atomic_inc(&pi->wb_batch_count);
            wb_submitted = true;
            atomic_inc(&sbi->wb_in_flight);
            queue_work(sbi->writeback_wq, &batch->work);
        } else {
            iput(batch->inode);
            kvfree(batch);
        }
    }

    /* writeback 互斥: 如果没有提交任何 batch, 直接释放 wb_mutex */
    if (!wb_submitted)
        mutex_unlock(&pi->wb_mutex);

    return ret;
}

/*
 * powerfs_writepage - 单页 writeback (fallback, VFS 内部路径使用)
 *
 * writepages 注册后, VFS 优先调用 writepages. writepage 仅作为 fallback:
 *   - migrate_pages 等内核内部路径可能直接调用 writepage
 *   - 保持与旧接口兼容
 */
int powerfs_writepage(struct page *page, struct writeback_control *wbc)
{
    struct inode *inode = page->mapping->host;
    struct super_block *sb = inode->i_sb;
    struct powerfs_sb_info *sbi = POWERFS_SB_INFO(sb);
    struct powerfs_writepage_work *wpw;
    loff_t offset = page_offset(page);
    size_t count = PAGE_SIZE;

    if (powerfs_net_is_stopping()) {
        redirty_page_for_writepage(wbc, page);
        unlock_page(page);
        return 0;
    }

    if (offset >= i_size_read(inode)) {
        unlock_page(page);
        return 0;
    }
    if (offset + count > i_size_read(inode))
        count = i_size_read(inode) - offset;

    wpw = powerfs_alloc_write_batch(1, GFP_NOFS);
    if (!wpw) {
        redirty_page_for_writepage(wbc, page);
        unlock_page(page);
        return 0;
    }

    INIT_WORK(&wpw->work, powerfs_writepage_work_fn);
    wpw->inode = igrab(inode);
    if (!wpw->inode) {
        /* inode 正在被 evict (I_FREEING), 无法提交 writeback.
         * 释放 wpw, redirty 页面. */
        kvfree(wpw);
        redirty_page_for_writepage(wbc, page);
        unlock_page(page);
        return 0;
    }
    get_page(page);
    wpw->pages[0] = page;
    wpw->offsets[0] = offset;
    wpw->counts[0] = count;
    wpw->num_pages = 1;

    set_page_writeback(page);
    unlock_page(page);
    atomic_inc(&sbi->wb_in_flight);
    queue_work(sbi->writeback_wq, &wpw->work);

    return 0;
}

/*
 * powerfs_write_begin - 写开始 (Stage C: 对接 netfs 子系统)
 *
 * 参照 ceph_write_begin (fs/ceph/addr.c):
 *   调用 netfs_write_begin 让 netfs 管理页面准备, 包括:
 *   - 分配并锁定 folio
 *   - 若 folio 不在 page cache 或非 uptodate, 通过 issue_read
 *     从 Filer 拉取现有数据 (处理 partial write 的 read-modify-write)
 *   - folio 返回时已锁定, 调用者写入后通过 write_end 解锁
 *
 * 替代旧的 grab_cache_page_write_begin + zero_user 方案:
 *   旧方案对 partial write (offset/len 未覆盖整页) 会丢失未写入部分
 *   的现有数据, 因为 zero_user 把整页清零. netfs_write_begin 会先读取
 *   现有数据, write_end 只更新写入部分, 保证 read-modify-write 正确性.
 */
int powerfs_write_begin(const struct kiocb *iocb, struct address_space *mapping,
                         loff_t pos, unsigned int len, struct folio **foliop,
                         void **fsdata)
{
    struct inode *inode = mapping->host;
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    struct folio *folio = NULL;
    int ret;

    pr_debug("powerfs: write_begin ino=%lu pos=%lld len=%u i_size=%lld\n",
            inode->i_ino, pos, len, i_size_read(inode));

    /* page_symlink() 调用时 iocb==NULL (无 file 描述符),
     * 不能走 netfs_write_begin (需要 file 参数).
     * 改用 __filemap_get_folio 直接获取 page cache folio. */
    if (!iocb) {
        folio = __filemap_get_folio(mapping, pos / PAGE_SIZE,
                                    FGP_WRITEBEGIN | FGP_LOCK | FGP_CREAT,
                                    mapping_gfp_mask(mapping));
        if (!folio)
            return -ENOMEM;
        *foliop = folio;
        *fsdata = NULL;
        return 0;
    }

    ret = netfs_write_begin(&pi->netfs, iocb->ki_filp, inode->i_mapping,
                            pos, len, &folio, NULL);
    if (ret < 0) {
        pr_warn("powerfs: write_begin netfs_write_begin failed: %d\n", ret);
        return ret;
    }

    pr_debug("powerfs: WB_BEGIN ok ino=%lu folio=%px order=%d locked=%d uptodate=%d index=%lu\n",
            inode->i_ino, folio, folio_order(folio),
            folio_test_locked(folio), folio_test_uptodate(folio),
            folio->index);

    WARN_ON_ONCE(!folio_test_locked(folio));
    *foliop = folio;
    *fsdata = NULL;
    return 0;
}

/*
 * powerfs_migrate_inline_to_flat - K2-7 Inline → Flat 自动迁移
 *
 * 对齐 FUSE write inline migrate (powerfs-fuse/src/fuse.rs L3446):
 *   1. 快照 inline_data (持 i_lock)
 *   2. 调 Filer MIGRATE_INLINE_ALLOC 分配 (volume_id, needle_id)
 *   3. 同步写 merged_data 到 Volume Server (WriteNeedle, lease_token=NULL)
 *   4. 持锁切换 inode: placement=Flat + volume_id + file_key + 清 inline_data
 *
 * crash safety (对齐 FUSE):
 *   - Filer 分配后不修改 inode, 保留 inline_data
 *   - 客户端崩溃后 Filer 仍有 inline_data, 文件仍可作 Inline 读
 *   - 分配的 needle_id 泄漏 (可接受, 同 CREATE 失败)
 *   - 客户端写 Volume Server 成功后, close 时 UPDATE_INODE_SIZE_CHUNKS
 *     原子完成切换 (清除 inline_data + 设 Flat chunks)
 *
 * 注意: 本函数在 write_end 中调用, 持有 folio lock. 网络 I/O 期间
 *       folio lock 被持有, 但 inline 路径已在 write_end 中做 kvmalloc
 *       (可睡眠), 故阻塞操作可接受. 迁移数据最大 8KB, 网络往返 <100ms.
 *
 * 返回 0 成功, 负数错误码 (网络错误透传, inline_data 保持原状).
 */
static int powerfs_migrate_inline_to_flat(struct inode *inode,
                                          struct powerfs_inode_info *pi)
{
    u8 *snap_data = NULL;
    u32 snap_len = 0;
    u64 shard_id, ino = inode->i_ino;
    u64 volume_id = 0, file_key = 0;
    int ret;

    /* 1. 持锁快照 inline_data (网络 I/O 不能持 spinlock) */
    spin_lock(&pi->i_lock);
    if (!pi->inline_data || pi->inline_len == 0) {
        spin_unlock(&pi->i_lock);
        pr_warn("powerfs: MIGRATE ino=%lu no inline_data to migrate\n", ino);
        return -EINVAL;
    }
    snap_len = pi->inline_len;
    spin_unlock(&pi->i_lock);

    snap_data = kmalloc(snap_len, GFP_KERNEL);
    if (!snap_data) {
        pr_warn("powerfs: MIGRATE ino=%lu kmalloc %u failed\n", ino, snap_len);
        return -ENOMEM;
    }
    spin_lock(&pi->i_lock);
    if (pi->inline_data && pi->inline_len == snap_len) {
        memcpy(snap_data, pi->inline_data, snap_len);
    } else {
        spin_unlock(&pi->i_lock);
        pr_warn("powerfs: MIGRATE ino=%lu inline_data changed during snapshot\n", ino);
        kfree(snap_data);
        return -EAGAIN;
    }
    spin_unlock(&pi->i_lock);

    pr_info("powerfs: MIGRATE ino=%lu inline_len=%u → triggering Flat migration\n",
            ino, snap_len);

    /* 2. 调 Filer MIGRATE_INLINE_ALLOC 分配 (volume_id, needle_id).
     * shard_id = shard_map_route(parent_ino) — 区间路由, 对齐 FUSE ShardMap. */
    shard_id = shard_map_route(pi->parent_ino ? pi->parent_ino : ino);
    ret = powerfs_net_migrate_inline_alloc(shard_id, ino, &volume_id, &file_key);
    if (ret < 0) {
        pr_warn("powerfs: MIGRATE ino=%lu alloc failed: %d, inline buffer unmodified\n",
                ino, ret);
        kfree(snap_data);
        return ret;  /* 透传网络错误 (-ENOTCONN/-ETIMEDOUT 等), 非 EFBIG */
    }

    /* 3. 同步写 snap_data 到 Volume Server (WriteNeedle).
     * lease_token=NULL: Volume Server 不校验 lease (net_handler.rs L92).
     * ClientId="kernel-client" 必须发送 (write_needle L5934 注释). */
    ret = powerfs_net_write_needle(volume_id, file_key, ino,
                                    snap_data, snap_len,
                                    NULL, 0);
    if (ret < 0) {
        pr_warn("powerfs: MIGRATE ino=%lu write_needle failed: %d, needle_id=%#llx leaked\n",
                ino, ret, (unsigned long long)file_key);
        kfree(snap_data);
        return ret;  /* 透传网络错误, 非 EFBIG */
    }

    pr_info("powerfs: MIGRATE ino=%lu write_needle OK volume_id=%llu needle_id=%#llx size=%u\n",
            ino, (unsigned long long)volume_id,
            (unsigned long long)file_key, snap_len);

    /* 4. 持锁切换 inode 到 Flat: 释放 inline_data, 更新布局元数据.
     * 后续 write 走 Flat writeback 路径, close 时 UPDATE_INODE_SIZE_CHUNKS
     * 同步 size+chunks 到 Filer (原子清除 inline_data + 设 Flat chunks). */
    spin_lock(&pi->i_lock);
    pi->placement = POWERFS_PLACEMENT_FLAT;
    pi->volume_id = volume_id;
    pi->file_key = file_key;
    pi->layout_chunk_size = POWERFS_CHUNK_SIZE;
    kfree(pi->inline_data);
    pi->inline_data = NULL;
    pi->inline_len = 0;
    pi->inline_dirty = false;
    spin_unlock(&pi->i_lock);

    kfree(snap_data);
    pr_info("powerfs: MIGRATE ino=%lu → Flat done, subsequent writes → Volume Server\n", ino);
    return 0;
}

/*
 * powerfs_write_end - 写结束 (Stage C: 纯 page cache 更新, 无网络 IO)
 *
 * 参照 ceph_write_end (fs/ceph/addr.c):
 *   - 标记 folio uptodate
 *   - 更新本地 i_size (i_size_write + mark_inode_dirty)
 *   - folio_mark_dirty 让 writeback 子系统负责持久化
 *   - 不做任何网络 IO, 数据持久化由 writepage 异步完成,
 *     i_size 同步由 write_inode (writeback 时) 完成
 *
 * 替代旧的同步写方案:
 *   旧方案在 write_end 中同步调用 powerfs_net_write + powerfs_net_setattr,
 *   导致 write() 系统调用阻塞在网络往返上, 高并发下性能差.
 *   Stage C 改为纯 page cache 更新, write() 立即返回,
 *   持久化推迟到 writeback (writepage + write_inode).
 */
int powerfs_write_end(const struct kiocb *iocb, struct address_space *mapping,
                       loff_t pos, unsigned int len, unsigned int copied,
                       struct folio *folio, void *fsdata)
{
    struct inode *inode = mapping->host;
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    loff_t end_pos = pos + copied;

    pr_debug("powerfs: WB_END ino=%lu pos=%lld copied=%u len=%u i_size=%lld uptodate=%d\n",
            inode->i_ino, pos, copied, len, i_size_read(inode),
            folio_test_uptodate(folio));

    if (copied > 0) {
        if (!folio_test_uptodate(folio)) {
            if (copied < len) {
                pr_debug("powerfs: WB_END partial write, return 0 for retry\n");
                copied = 0;
                goto out;
            }
            folio_mark_uptodate(folio);
        }
        if (end_pos > i_size_read(inode)) {
            i_size_write(inode, end_pos);
            mark_inode_dirty(inode);
        }
        folio_mark_dirty(folio);

        /* K2: Inline 模式 — 同步写入数据到 inline_data 缓冲.
         * writeback 不会将 Inline 文件的数据发送到 Volume Server,
         * 而是在 close 时通过 UPDATE_INODE 提交 inline_data 到 Filer.
         *
         * 这里在 write_end 中直接拷贝 folio 数据到 inline_data,
         * 确保 inline_data 始终与 page cache 同步.
         *
         * 并发保护: 持 i_lock 修改 inline_data.
         * 注意: kvmalloc 在持 spinlock 时可能睡眠 (GFP_KERNEL),
         * 因此先在锁外分配, 再持锁替换. */
        if (pi->placement == POWERFS_PLACEMENT_INLINE) {
            size_t folio_off = offset_in_folio(folio, pos);
            size_t need_len = end_pos;
            u8 *new_buf = NULL;

            /* K2: 仅首次写入(pos==0)输出日志, 避免 bs=1 时 8192 条日志淹没 serial */
            if (pos == 0)
                pr_info("powerfs: WB_END INLINE first write ino=%lu copied=%u end=%zu\n",
                        inode->i_ino, copied, end_pos);

            /* K2-7: 检查是否超出 inline 硬上限 (8KB).
             * 超出时截断到 INLINE_MAX_SIZE, 写入后触发迁移到 Flat.
             * 迁移阈值 = min(inline_max_size × 1.5, INLINE_MAX_SIZE),
             * 对齐 FUSE fuse.rs L3444. */
            if (need_len > POWERFS_INLINE_MAX_SIZE) {
                pr_warn("powerfs: WB_END INLINE ino=%lu write end=%zu > INLINE_MAX=%d, "
                        "will migrate after write\n",
                        inode->i_ino, need_len, POWERFS_INLINE_MAX_SIZE);
                need_len = POWERFS_INLINE_MAX_SIZE;
                copied = min_t(unsigned int, copied,
                               POWERFS_INLINE_MAX_SIZE - pos);
                if (copied == 0) {
                    /* pos 已超 8KB, 无法写入 inline_data.
                     * 返回 -EFBIG 触发上层重试 (此时若已迁移则走 Flat). */
                    folio_unlock(folio);
                    folio_put(folio);
                    return -EFBIG;
                }
                end_pos = pos + copied;
            }

            /* 若 inline_data 未分配或不够大, 在锁外分配新 buffer */
            spin_lock(&pi->i_lock);
            if (!pi->inline_data || pi->inline_len < need_len) {
                u8 *old = pi->inline_data;
                u32 old_len = pi->inline_len;
                spin_unlock(&pi->i_lock);

                /* 锁外分配 (GFP_KERNEL 可睡眠) */
                new_buf = kvmalloc(need_len, GFP_KERNEL);
                if (!new_buf) {
                    pr_warn("powerfs: WB_END INLINE ino=%lu kvmalloc %zu failed\n",
                            inode->i_ino, need_len);
                    /* 分配失败不影响 page cache, writeback 仍会标脏页.
                     * inline_data 不更新, close 时可能丢失数据.
                     * 后续可回退到 Flat 模式. */
                    goto inline_done;
                }
                /* 拷贝旧数据到新 buffer */
                if (old && old_len > 0)
                    memcpy(new_buf, old, old_len);
                /* 新区域清零 */
                if (need_len > old_len)
                    memset(new_buf + old_len, 0, need_len - old_len);

                /* 持锁替换 */
                spin_lock(&pi->i_lock);
                kfree(pi->inline_data);
                pi->inline_data = new_buf;
                pi->inline_len = need_len;
                /* new_buf 所有权已转移, 防止下方 kfree */
                new_buf = NULL;
            }

            /* 从 folio 拷贝写入的数据到 inline_data */
            if (pi->inline_data && pos + copied <= pi->inline_len) {
                void *kaddr = kmap_local_folio(folio, folio_off);
                memcpy(pi->inline_data + pos, kaddr, copied);
                kunmap_local(kaddr);
                pi->inline_dirty = true;
                pr_debug("powerfs: WB_END INLINE ino=%lu pos=%lld copied=%u inline_len=%u\n",
                        inode->i_ino, pos, copied, pi->inline_len);
            }
            spin_unlock(&pi->i_lock);

            /* 分配了但未使用 (被并发覆盖) 的 buffer 释放 */
            kfree(new_buf);

            /* K2-7: 检查是否需要迁移到 Flat.
             * 迁移阈值 = min(inline_max_size × 1.5, POWERFS_INLINE_MAX_SIZE).
             * 对齐 FUSE fuse.rs L3444: 滞后窗口避免边界抖动.
             * 迁移成功: inode 切换到 Flat, 后续 write 走 Flat writeback.
             * 迁移失败: 透传网络错误码, inline_data 保持原状 (close 时同步到 Filer). */
            {
                u32 migrate_threshold = min(pi->inline_max_size * 3 / 2,
                                            (u32)POWERFS_INLINE_MAX_SIZE);
                /* 注意: 用 >= 而非 >, 否则当 inline_max_size == POWERFS_INLINE_MAX_SIZE
                 * (如 8192) 时, migrate_threshold=8192, inline_len 最大也是 8192,
                 * 条件 inline_len > 8192 永远为 false, 迁移不会触发. */
                if (pi->inline_len >= migrate_threshold) {
                    int mig_ret = powerfs_migrate_inline_to_flat(inode, pi);
                    if (mig_ret < 0) {
                        pr_warn("powerfs: WB_END INLINE ino=%lu migrate failed: %d\n",
                                inode->i_ino, mig_ret);
                        folio_unlock(folio);
                        folio_put(folio);
                        return mig_ret;
                    }
                    /* 迁移成功: inode 已切换到 Flat.
                     * folio 仍标记 dirty, writeback 会走 Flat 路径写 Volume Server.
                     * close 时 UPDATE_INODE_SIZE_CHUNKS 同步 size+chunks 到 Filer. */
                }
            }
        }
    }

inline_done:
out:
    folio_unlock(folio);
    folio_put(folio);

    return copied;
}

/*
 * powerfs_bmap - 逻辑块到物理块映射
 *
 * 对于网络文件系统，通常不需要实现或返回 0
 */
sector_t powerfs_bmap(struct address_space *mapping, sector_t block)
{
    return 0;
}

/* 地址空间操作表 - 内核 6.2 netfs 风格
 *
 * .dirty_folio 必须设置: folio_mark_dirty() 内部通过
 * mapping->a_ops->dirty_folio() 间接调用 (mm/page-writeback.c),
 * 若未设置则为 NULL, write_end 标脏页时触发 NULL instruction fetch oops.
 * 参考 fs/nfs/file.c, fs/btrfs/inode.c, fs/ceph/addr.c (均设置 dirty_folio).
 * 我们不使用 buffer_heads, 故用 filemap_dirty_folio (与 nfs/btrfs/zonefs 一致).
 */
static const struct address_space_operations powerfs_aops = {
    .read_folio    = netfs_read_folio,   /* Stage C: netfs 子系统管理 folio 生命周期 */
    .readahead     = netfs_readahead,    /* Stage C: 批量预读 */
    .writepages    = powerfs_writepages,  /* 批量 writeback (6.17: 无 writepage 回调) */
    .write_begin   = powerfs_write_begin,
    .write_end     = powerfs_write_end,
    .dirty_folio   = filemap_dirty_folio,
    .bmap          = powerfs_bmap,
};

/* ========== statfs ========== */

int powerfs_statfs(struct dentry *dentry, struct kstatfs *buf)
{
    pr_debug("powerfs: statfs\n");
    return powerfs_net_statfs(buf);
}

/* ========== 文件操作 ========== */

/*
 * powerfs_fsync - 数据同步操作
 *
 * Flat/Stripe: file_write_and_wait_range 触发 writepage→powerfs_net_write 将脏页刷到
 *   volume server, 然后调用 powerfs_net_setattr 同步 i_size.
 * Inline (K2-5): 脏页已在 write_end 同步到 inline_data, fsync 时若 inline_dirty
 *   则通过 UPDATE_INODE 将 inline_data 提交到 Filer (数据+元数据 Raft 强一致).
 *
 * 断连时 writepage 和 setattr/update 返回 -ENOTCONN, fsync 传播错误.
 */
static int powerfs_fsync(struct file *file, loff_t start, loff_t end, int datasync)
{
    struct inode *inode = file->f_mapping->host;
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    int ret;
    loff_t i_size;

    pr_debug("powerfs: fsync ino=%lu start=%llu end=%llu datasync=%d i_size=%lld\n",
            inode->i_ino, start, end, datasync, i_size_read(inode));

    /* 触发脏页写回 (Flat: writepage→powerfs_net_write; Inline: 仅清脏标) */
    ret = file_write_and_wait_range(file, start, end);
    if (ret < 0) {
        pr_warn("powerfs: fsync write_and_wait error: %d\n", ret);
        return ret;
    }

    /* K2-5: Inline 模式 — 通过 UPDATE_INODE 同步 inline_data 到 Filer.
     * 复用 release 路径逻辑: 快照 inline_data → 锁外网络 I/O → 清 dirty.
     * fsync 不做重试 (由调用方决定是否重试), 单次同步失败返回错误. */
    if (pi->placement == POWERFS_PLACEMENT_INLINE && pi->inline_dirty) {
        u8 *snap_data;
        u32 snap_len;
        u64 shard_id;

        spin_lock(&pi->i_lock);
        if (!pi->inline_data || pi->inline_len == 0) {
            pi->inline_dirty = false;
            spin_unlock(&pi->i_lock);
            return 0;
        }
        snap_len = pi->inline_len;
        spin_unlock(&pi->i_lock);

        snap_data = kmalloc(snap_len, GFP_KERNEL);
        if (!snap_data)
            return -ENOMEM;

        spin_lock(&pi->i_lock);
        if (pi->inline_data && pi->inline_len == snap_len) {
            memcpy(snap_data, pi->inline_data, snap_len);
        } else {
            spin_unlock(&pi->i_lock);
            kfree(snap_data);
            return 0;
        }
        spin_unlock(&pi->i_lock);

        shard_id = shard_map_route(pi->parent_ino ? pi->parent_ino : inode->i_ino);
        ret = powerfs_net_update_inode_size_chunks(shard_id, inode->i_ino,
                                                    (__u64)snap_len,
                                                    "kernel",
                                                    NULL, 0,
                                                    snap_data, snap_len);
        kfree(snap_data);
        if (ret < 0) {
            pr_warn("powerfs: fsync INLINE ino=%lu update failed: %d\n",
                    inode->i_ino, ret);
            return ret;
        }
        /* 成功则清 dirty */
        spin_lock(&pi->i_lock);
        pi->inline_dirty = false;
        spin_unlock(&pi->i_lock);
        return 0;
    }

    /* Flat/Stripe: 同步 i_size 到 Filer */
    i_size = i_size_read(inode);
    pr_debug("powerfs: fsync after writeback i_size=%lld\n", i_size);
    if (i_size > 0) {
        int sret = powerfs_net_setattr(inode->i_ino, POWERFS_ATTR_SIZE,
                                        0, 0, 0, (__u64)i_size, 0, 0);
        if (sret < 0) {
            pr_warn("powerfs: fsync setattr size=%llu ino=%lu failed: %d\n",
                    (u64)i_size, inode->i_ino, sret);
            return sret;
        }
    }

    return 0;
}

/*
 * Phase 2: 流控准入 wrapper — 在 file_operations 层排队等待.
 *
 * 不能在 address_space_operations (write_begin/read_folio/writepages) 里 wait:
 *   - write_begin 持有 inode->i_rwsem 写锁
 *   - read_folio 持有 folio lock
 *   - writepages 持有 folio lock (writeback 线程内)
 *
 * file_operations 层 (read_iter/write_iter) 不持有 page lock,
 * generic_file_write_iter 内部才获取 i_rwsem, 此时 admit_wait 已返回.
 */
static ssize_t powerfs_file_read_iter(struct kiocb *iocb, struct iov_iter *to)
{
    powerfs_flow_admit_wait(POWERFS_FLOW_OP_READ, 2000);
    return generic_file_read_iter(iocb, to);
}

static ssize_t powerfs_file_write_iter(struct kiocb *iocb, struct iov_iter *from)
{
    struct inode *inode = iocb->ki_filp->f_inode;
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    loff_t offset = iocb->ki_pos;
    size_t count = iov_iter_count(from);

    powerfs_flow_admit_wait(POWERFS_FLOW_OP_WRITE, 2000);

    /* 预先获取 lease (同步网络调用, 用户进程上下文可阻塞).
     * writeback 工作队列不允许阻塞在网络 I/O 上, 故 lease 必须在
     * write 路径预先获取, writeback 时只检查已持有的 lease (非阻塞).
     *
     * 跳过条件:
     *   - Inline 模式 (数据不走 Volume Server)
     *   - count == 0 (无数据写入)
     *   - 目录 (不应走到这里, 防御性检查) */
    if (pi->placement != POWERFS_PLACEMENT_INLINE &&
        S_ISREG(inode->i_mode) && count > 0) {
        int lease_ret = ensure_lease(inode, offset);
        if (lease_ret && lease_ret != -ENOMEM)
            pr_debug("powerfs: write_iter ensure_lease ino=%lu off=%lld ret=%d, continuing without lease\n",
                     inode->i_ino, offset, lease_ret);
        /* lease 获取失败不阻止写入, Volume Server 容许无 lease 写入 */
    }

    return generic_file_write_iter(iocb, from);
}

/*
 * powerfs_file_release - 文件关闭 (最后一个 fd 释放时调用)
 *
 * K2-5: Inline 模式下, 若 inline_dirty 为 true, 通过 UPDATE_INODE 将
 * inline_data 提交到 Filer (单次 Raft 提交 = 数据 + 元数据).
 *
 * 对齐 FUSE release inline 路径 (fuse.rs L3988):
 *   - shard_id = shard_map_route(parent_ino) (Filer 路由, 对齐 FUSE ShardMap)
 *   - chunks 为空 (Inline 不走 Volume Server)
 *   - size = inline_len
 *   - 5 次重试, 500ms×attempt 退避 (覆盖 Raft 选举窗口)
 *
 * 并发保护: 持 i_lock 快照 inline_data 指针/长度, 锁外做网络 I/O.
 * 网络成功后持锁清 inline_dirty.
 *
 * 注意: VFS 忽略 release 返回值, 故即使同步失败也返回 0 (仅告警).
 *       数据丢失风险由 fsck 兜底 (与 FUSE 一致).
 *
 * 局限: mmap 写入的脏页未同步到 inline_data (writepages 对 Inline 仅清脏标),
 *       当前仅支持 write_iter 路径的 Inline 持久化.
 */
static int powerfs_file_release(struct inode *inode, struct file *file)
{
    struct powerfs_inode_info *pi = POWERFS_I(inode);
    u8 *snap_data = NULL;
    u32 snap_len = 0;
    u64 shard_id;
    u64 ino = inode->i_ino;
    int attempt;
    int ret = 0;
    bool synced = false;

    /* Inline 模式 + dirty: 同步 inline_data 到 Filer (K2-5).
     * Flat 模式: 同步 size+chunks 到 Filer (对齐 FUSE sync_size_chunks_on_close).
     *   不同步的话 Filer 端 chunks 为空, remount/lookup 后 read locate 失败 (-EINVAL). */
    if (pi->placement == POWERFS_PLACEMENT_INLINE && pi->inline_dirty) {
    pr_info("powerfs: RELEASE INLINE ino=%lu dirty=%d inline_len=%u\n",
            ino, pi->inline_dirty ? 1 : 0, pi->inline_len);

    /* 持锁快照 inline_data (网络 I/O 不能持 spinlock) */
    spin_lock(&pi->i_lock);
    if (!pi->inline_data || pi->inline_len == 0) {
        /* dirty 标记但无数据 — 异常状态, 清标志避免反复重试 */
        pi->inline_dirty = false;
        spin_unlock(&pi->i_lock);
        pr_warn("powerfs: RELEASE INLINE ino=%lu dirty but no inline_data\n", ino);
        return 0;
    }
    snap_len = pi->inline_len;
    spin_unlock(&pi->i_lock);

    /* 锁外分配快照缓冲并拷贝 */
    snap_data = kmalloc(snap_len, GFP_KERNEL);
    if (!snap_data) {
        pr_warn("powerfs: RELEASE INLINE ino=%lu kmalloc %u failed, data may be lost\n",
                ino, snap_len);
        return 0;
    }
    spin_lock(&pi->i_lock);
    if (pi->inline_data && pi->inline_len == snap_len) {
        memcpy(snap_data, pi->inline_data, snap_len);
    } else {
        /* 并发修改了 inline_data, 放弃本次同步 */
        spin_unlock(&pi->i_lock);
        pr_warn("powerfs: RELEASE INLINE ino=%lu inline_data changed during snapshot\n", ino);
        kfree(snap_data);
        return 0;
    }
    spin_unlock(&pi->i_lock);

    /* K2: 调试 — 输出 snap_data 的 checksum, 与 issue_read 的 csum 比较 */
    {
        __u32 i, csum = 0;
        for (i = 0; i < snap_len; i++)
            csum += snap_data[i];
        pr_info("powerfs: RELEASE INLINE ino=%lu snap_len=%u csum=%u first8=%02x%02x%02x%02x%02x%02x%02x%02x\n",
                ino, snap_len, csum,
                snap_len > 0 ? snap_data[0] : 0, snap_len > 1 ? snap_data[1] : 0,
                snap_len > 2 ? snap_data[2] : 0, snap_len > 3 ? snap_data[3] : 0,
                snap_len > 4 ? snap_data[4] : 0, snap_len > 5 ? snap_data[5] : 0,
                snap_len > 6 ? snap_data[6] : 0, snap_len > 7 ? snap_data[7] : 0);
    }

    shard_id = shard_map_route(pi->parent_ino ? pi->parent_ino : ino);

    /* 5 次重试, 500ms×attempt 退避 (对齐 FUSE, 覆盖 Raft 选举窗口 ~3s) */
    for (attempt = 1; attempt <= 5; attempt++) {
        ret = powerfs_net_update_inode_size_chunks(shard_id, ino,
                                                    (__u64)snap_len,
                                                    "kernel",
                                                    NULL, 0,
                                                    snap_data, snap_len);
        if (ret == 0) {
            synced = true;
            pr_info("powerfs: RELEASE INLINE ino=%lu synced size=%u (attempt %d)\n",
                     ino, snap_len, attempt);
            break;
        }
        pr_warn("powerfs: RELEASE INLINE ino=%lu attempt %d failed: %d\n",
                ino, attempt, ret);
        if (attempt < 5)
            msleep(500 * attempt);
    }

    /* 成功则清 dirty; 失败保留 dirty (evict_inode 时 kfree, 数据丢失) */
    if (synced) {
        spin_lock(&pi->i_lock);
        pi->inline_dirty = false;
        spin_unlock(&pi->i_lock);
    } else {
        pr_err("powerfs: RELEASE INLINE ino=%lu FAILED after 5 attempts: %d — data may be lost\n",
               ino, ret);
    }

    kfree(snap_data);
    return 0;
    } /* end Inline release */

    /* Flat 模式: close 时同步 size+chunks 到 Filer.
     * 对齐 FUSE sync_size_chunks_on_close (fuse.rs L990).
     * writeback 只刷数据到 Volume Server, 不同步 chunks 到 Filer.
     * 若不同步, Filer 端 chunks 为空, remount/lookup 后 read locate 失败.
     *
     * 性能修复: close 时先 flush 脏页到 Volume Server (filemap_write_and_wait_range),
     * 确保数据在 lease 有效期内写入. 否则 lease 过期后 writeback 无 lease 写入,
     * 每个 writepage 需 5s (实测 780MB → 65min), 严重影响 sync 性能.
     * file_release 在用户进程上下文调用, 可安全阻塞. */
    if (pi->placement == POWERFS_PLACEMENT_FLAT && pi->volume_id && pi->file_key) {
        loff_t i_size = i_size_read(inode);
        u64 shard_id;
        u32 chunk_size = pi->layout_chunk_size ? pi->layout_chunk_size : POWERFS_CHUNK_SIZE;
        u32 chunk_count;
        struct powerfs_chunk_map *chunks = NULL;
        u32 i;
        int flush_ret;

        if (i_size == 0) {
            pr_debug("powerfs: RELEASE FLAT ino=%lu size=0, skip\n", ino);
            return 0;
        }

        /* Flush 脏页到 Volume Server (在 lease 过期前完成数据写入).
         * ensure_lease 刷新 stripe 0 的 lease (快速路径: 已持有则立即返回).
         * filemap_write_and_wait_range 触发 writeback, 此时 lease 仍有效,
         * powerfs_get_lease_token 能找到 lease token, 写入快速完成. */
        ensure_lease(inode, 0);
        flush_ret = filemap_write_and_wait_range(inode->i_mapping, 0, LLONG_MAX);
        if (flush_ret)
            pr_warn("powerfs: RELEASE FLAT ino=%lu filemap_write_and_wait_range: %d\n",
                    ino, flush_ret);

        /* 仅在 i_size 变化时同步 (用 content_size 跟踪上次同步值) */
        spin_lock(&pi->i_lock);
        if ((u64)i_size == pi->content_size) {
            spin_unlock(&pi->i_lock);
            pr_debug("powerfs: RELEASE FLAT ino=%lu size=%llu unchanged, skip\n",
                     ino, (u64)i_size);
            return 0;
        }
        spin_unlock(&pi->i_lock);

        chunk_count = (u32)div_u64(i_size + chunk_size - 1, chunk_size);
        /* 限制最大 chunk_count 避免过大分配 (4096 chunks = 4GB @ 1MB chunk) */
        if (chunk_count > 4096) {
            pr_warn("powerfs: RELEASE FLAT ino=%lu chunk_count=%u > 4096, truncating\n",
                    ino, chunk_count);
            chunk_count = 4096;
        }

        chunks = kmalloc_array(chunk_count, sizeof(*chunks), GFP_KERNEL);
        if (!chunks) {
            pr_warn("powerfs: RELEASE FLAT ino=%lu kmalloc %u chunks failed, metadata not synced\n",
                    ino, chunk_count);
            return 0;
        }

        for (i = 0; i < chunk_count; i++) {
            chunks[i].chunk_idx = i;
            chunks[i].needle_id = pi->file_key + i;
            chunks[i].volume_id = pi->volume_id;
            chunks[i].crc32 = 0;
        }

        shard_id = shard_map_route(pi->parent_ino ? pi->parent_ino : ino);

        pr_info("powerfs: RELEASE FLAT ino=%lu size=%llu chunks=%u vid=%llu fkey=%llu\n",
                ino, (u64)i_size, chunk_count,
                (unsigned long long)pi->volume_id,
                (unsigned long long)pi->file_key);

        for (attempt = 1; attempt <= 5; attempt++) {
            ret = powerfs_net_update_inode_size_chunks(shard_id, ino,
                                                        (__u64)i_size,
                                                        "kernel",
                                                        chunks, chunk_count,
                                                        NULL, 0);
            if (ret == 0) {
                spin_lock(&pi->i_lock);
                pi->content_size = (u64)i_size;
                spin_unlock(&pi->i_lock);
                pr_info("powerfs: RELEASE FLAT ino=%lu synced size=%llu chunks=%u (attempt %d)\n",
                        ino, (u64)i_size, chunk_count, attempt);
                break;
            }
            pr_warn("powerfs: RELEASE FLAT ino=%lu attempt %d failed: %d\n",
                    ino, attempt, ret);
            if (attempt < 5)
                msleep(500 * attempt);
        }

        kfree(chunks);
        return 0;
    }

    pr_debug("powerfs: RELEASE skip ino=%lu placement=%u dirty=%d\n",
             ino, pi->placement, pi->inline_dirty ? 1 : 0);
    return 0;
}

/* Forward declaration — defined below */
static long powerfs_fallocate(struct file *file, int mode,
                              loff_t offset, loff_t len);

/*
 * 文件操作表 - 尽可能复用 VFS 通用实现
 *
 * 参考 ramfs_file_operations (fs/ramfs/file-mmu.c)
 */
static const struct file_operations powerfs_file_operations = {
    .read_iter    = powerfs_file_read_iter,
    .write_iter   = powerfs_file_write_iter,
    .mmap         = generic_file_mmap,
    .release      = powerfs_file_release,
    .fsync        = powerfs_fsync,
    .fallocate    = powerfs_fallocate,
    .splice_read  = filemap_splice_read,
    .splice_write = iter_file_splice_write,
    .llseek       = generic_file_llseek,
};

/* ========== fallocate 回调 (空间预分配) ==========
 *
 * PowerFS 不预分配物理块 (空间按需分配), fallocate 仅更新 i_size:
 * - mode 0 (默认): 扩展 i_size 到 offset+len, 页面全零
 * - FALLOC_FL_KEEP_SIZE: 不改变 i_size (no-op)
 * - FALLOC_FL_PUNCH_HOLE: 释放页面缓存 (truncate_pagecache_range)
 *
 * 参考: brd_fallocate (ramfs), shmem_fallocate (tmpfs)
 */
static long powerfs_fallocate(struct file *file, int mode,
                              loff_t offset, loff_t len)
{
    struct inode *inode = file_inode(file);
    loff_t new_size = offset + len;
    int ret;

    /* Only support default, KEEP_SIZE, and PUNCH_HOLE modes */
    if (mode & ~(FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE))
        return -EOPNOTSUPP;

    /* PUNCH_HOLE requires KEEP_SIZE */
    if ((mode & FALLOC_FL_PUNCH_HOLE) && !(mode & FALLOC_FL_KEEP_SIZE))
        return -EOPNOTSUPP;

    if (offset < 0 || len <= 0)
        return -EINVAL;

    inode_lock(inode);

    if (mode & FALLOC_FL_PUNCH_HOLE) {
        /* Punch hole: release page cache in the given range */
        truncate_pagecache_range(inode, offset, new_size - 1);
        ret = 0;
    } else if (!(mode & FALLOC_FL_KEEP_SIZE)) {
        /* Default mode: extend file size */
        if (new_size > i_size_read(inode)) {
            i_size_write(inode, new_size);
            mark_inode_dirty(inode);
        }
        ret = 0;
    } else {
        /* KEEP_SIZE: no-op (no physical pre-allocation) */
        ret = 0;
    }

    inode_unlock(inode);
    return ret;
}

/* ========== xattr 回调 (simple_xattr in-memory) ==========
 *
 * xattr 存储在 inode 内存中 (powerfs_inode_info.xattrs), 不持久化到 Filer.
 * 使用内核 simple_xattr API + xattr_handler 机制 (kernel 6.17 移除了
 * inode_operations 中的 setxattr/getxattr/removexattr 回调, 改用
 * xattr_handler 注册到 super_block.s_xattr).
 * 支持 user.* / trusted.* / security.* 前缀.
 */

static int powerfs_xattr_handler_get(const struct xattr_handler *handler,
                                     struct dentry *unused, struct inode *inode,
                                     const char *name, void *buffer, size_t size)
{
    /* Stubbed: running kernel doesn't EXPORT_SYMBOL simple_xattr_get */
    return -ENODATA;
}

static int powerfs_xattr_handler_set(const struct xattr_handler *handler,
                                     struct mnt_idmap *idmap,
                                     struct dentry *unused, struct inode *inode,
                                     const char *name, const void *value,
                                     size_t size, int flags)
{
    /* Stubbed: running kernel doesn't EXPORT_SYMBOL simple_xattr_set */
    return -EOPNOTSUPP;
}

static ssize_t powerfs_listxattr(struct dentry *dentry, char *buffer,
                                 size_t size)
{
    /* Stubbed: running kernel doesn't EXPORT_SYMBOL simple_xattr_list */
    return 0;
}

static const struct xattr_handler powerfs_security_xattr_handler = {
    .prefix = XATTR_SECURITY_PREFIX,
    .get = powerfs_xattr_handler_get,
    .set = powerfs_xattr_handler_set,
};

static const struct xattr_handler powerfs_trusted_xattr_handler = {
    .prefix = XATTR_TRUSTED_PREFIX,
    .get = powerfs_xattr_handler_get,
    .set = powerfs_xattr_handler_set,
};

static const struct xattr_handler powerfs_user_xattr_handler = {
    .prefix = XATTR_USER_PREFIX,
    .get = powerfs_xattr_handler_get,
    .set = powerfs_xattr_handler_set,
};

static const struct xattr_handler * const powerfs_xattr_handlers[] = {
    &powerfs_security_xattr_handler,
    &powerfs_trusted_xattr_handler,
    &powerfs_user_xattr_handler,
    NULL
};

/* ========== Inode operations 表 ========== */

/* 目录 inode 操作 */
static const struct inode_operations powerfs_dir_inode_operations = {
    .create     = powerfs_create,
    .lookup     = powerfs_lookup,
    .link       = powerfs_link,
    .unlink     = powerfs_unlink,
    .symlink    = powerfs_symlink,
    .readlink   = powerfs_readlink,
    .mkdir      = powerfs_mkdir,
    .rmdir      = powerfs_rmdir,
    .mknod      = powerfs_mknod,
    .rename     = powerfs_rename,
    .getattr    = powerfs_getattr,
    .setattr    = powerfs_setattr,
    .listxattr  = powerfs_listxattr,
};

/* 普通文件 inode 操作 */
static const struct inode_operations powerfs_file_inode_operations = {
    .getattr    = powerfs_getattr,
    .setattr    = powerfs_setattr,
    .listxattr  = powerfs_listxattr,
};

/*
 * powerfs_show_options - 显示挂载选项
 */
static int powerfs_show_options(struct seq_file *m, struct dentry *root)
{
    struct powerfs_sb_info *sbi = POWERFS_SB_INFO(root->d_sb);

    seq_printf(m, ",master_addr=%s", sbi->master_addr);
    seq_printf(m, ",master_port=%u", sbi->master_port);

    return 0;
}

/* ========== Super operations 表 ========== */

static const struct super_operations powerfs_super_ops = {
    .alloc_inode   = powerfs_alloc_inode,
    .free_inode    = powerfs_free_inode,
    .evict_inode   = powerfs_evict_inode,
    .write_inode   = powerfs_write_inode,  /* Stage C: writeback 时同步 i_size 到 Filer */
    .statfs        = powerfs_statfs,
    .drop_inode    = generic_delete_inode,
    .show_options  = powerfs_show_options,
};

/* ========== 全局 slab 缓存初始化/销毁 ========== */

int powerfs_init_inode_cache(void)
{
    /* inode slab 缓存 */
    powerfs_inode_cachep = kmem_cache_create(
        "powerfs_inode_cache",
        sizeof(struct powerfs_inode_info),
        __alignof__(struct powerfs_inode_info),
        SLAB_RECLAIM_ACCOUNT | SLAB_ACCOUNT,
        powerfs_inode_init_once);
    if (!powerfs_inode_cachep)
        return -ENOMEM;

    /* dentry_info slab 缓存 */
    powerfs_dentry_cachep = kmem_cache_create(
        "powerfs_dentry_cache",
        sizeof(struct powerfs_dentry_info),
        __alignof__(struct powerfs_dentry_info),
        SLAB_RECLAIM_ACCOUNT,
        NULL);
    if (!powerfs_dentry_cachep) {
        kmem_cache_destroy(powerfs_inode_cachep);
        powerfs_inode_cachep = NULL;
        return -ENOMEM;
    }

    pr_debug("powerfs: slab caches created\n");
    return 0;
}

void powerfs_destroy_inode_cache(void)
{
    if (powerfs_dentry_cachep) {
        kmem_cache_destroy(powerfs_dentry_cachep);
        powerfs_dentry_cachep = NULL;
    }
    if (powerfs_inode_cachep) {
        kmem_cache_destroy(powerfs_inode_cachep);
        powerfs_inode_cachep = NULL;
    }
    pr_debug("powerfs: slab caches destroyed\n");
}

/*
 * powerfs_set_sb_dentry_ops - 设置超级块的默认 dentry 操作
 *
 * 6.17 内核使用 set_default_d_op() 替代直接赋值 sb->s_d_op
 */
void powerfs_set_sb_dentry_ops(struct super_block *sb)
{
    set_default_d_op(sb, &powerfs_dentry_operations);
}

/* ========== fill_super: 填充超级块 (fs_context 风格) ========== */

int powerfs_fill_super(struct super_block *sb, struct fs_context *fc)
{
    struct powerfs_ctx_simple {
        char master_addr[64];
        u16  master_port;
        u32  write_batch_kb;
    };
    /* 注意: sget_fc() 会将 fc->s_fs_info 转移到 sb->s_fs_info, 然后将
     * fc->s_fs_info 置 NULL. 因此必须从 sb->s_fs_info 获取 ctx, 而不是
     * 从 fc->s_fs_info 获取 (后者已经是 NULL).
     * 参考: fs/super.c sget_fc() 第 566-574 行 */
    struct powerfs_ctx_simple *ctx = sb->s_fs_info;
    struct powerfs_sb_info *sbi;
    struct inode *root;
    u32 batch_kb = POWERFS_WRITE_BATCH_DEFAULT_KB;
    int ret;

    pr_debug("powerfs: fill_super (sb->s_fs_info=%px, master_addr='%s')\n",
            ctx, ctx ? ctx->master_addr : "(null)");

    /* 创建超级块私有信息 */
    sbi = kzalloc(sizeof(*sbi), GFP_KERNEL);
    if (!sbi)
        return -ENOMEM;

    sbi->sb = sb;

    /* 保存挂载参数 (ctx 来自 init_fs_context 通过 sget_fc 转移到 sb->s_fs_info) */
    if (ctx) {
        strncpy(sbi->master_addr, ctx->master_addr, sizeof(sbi->master_addr) - 1);
        sbi->master_port = ctx->master_port;
        batch_kb = ctx->write_batch_kb;
        /* 释放 init_fs_context 分配的 ctx, 释放后 sb->s_fs_info 仍指向已释放内存,
         * 必须在下方设置 sb->s_fs_info = sbi 之前完成 */
        kfree(ctx);
        ctx = NULL;
    }

    /* 验证并转换 write_batch_kb → write_batch_pages.
     * 范围: 4KB (1 page) ~ 64MB (stripe size, 16384 pages).
     * 越界值 clamp 到合法范围并告警, 不拒绝挂载 (避免配置笔误导致不可用). */
    if (batch_kb < POWERFS_WRITE_BATCH_MIN_KB) {
        pr_warn("powerfs: write_batch_kb=%u too small, clamped to %d\n",
                batch_kb, POWERFS_WRITE_BATCH_MIN_KB);
        batch_kb = POWERFS_WRITE_BATCH_MIN_KB;
    } else if (batch_kb > POWERFS_WRITE_BATCH_MAX_KB) {
        pr_warn("powerfs: write_batch_kb=%u too large, clamped to %d\n",
                batch_kb, POWERFS_WRITE_BATCH_MAX_KB);
        batch_kb = POWERFS_WRITE_BATCH_MAX_KB;
    }
    /* 转换为页数: kb * 1024 / PAGE_SIZE = kb / (PAGE_SIZE/1024) = kb / 4 */
    sbi->write_batch_pages = (int)(batch_kb * 1024 / PAGE_SIZE);
    /* K1-5 fix: 确保 batch 至少能容纳一个 chunk 的所有页面,
     * 避免 same-needle 的 RMW 并发覆盖. chunk_size=1MB → 256 页. */
    {
        int min_pages = (int)(POWERFS_CHUNK_SIZE / PAGE_SIZE);
        if (sbi->write_batch_pages < min_pages)
            sbi->write_batch_pages = min_pages;
    }
    pr_debug("powerfs: write_batch_kb=%u → write_batch_pages=%d\n",
            batch_kb, sbi->write_batch_pages);

    /* 初始化 inode 号分配器 (从 100 开始，1 是 root) */
    atomic_set(&sbi->next_ino, 100);

    /* 设置超级块 (覆盖 sb->s_fs_info, 之前指向已释放的 ctx) */
    sb->s_fs_info = sbi;
    sb->s_op = &powerfs_super_ops;
    sb->s_xattr = powerfs_xattr_handlers;
    sb->s_magic = POWERFS_SUPER_MAGIC;
    sb->s_blocksize = 4096;
    sb->s_blocksize_bits = 12;
    sb->s_maxbytes = MAX_LFS_FILESIZE;
    sb->s_time_gran = 1;

    /* Stage C: 设置 BDI 支持 writeback.
     *
     * super_setup_bdi 创建独立的 backing_dev_info 并设置 BDI_CAP_WRITEBACK.
     * 没有 BDI_CAP_WRITEBACK, mapping_can_writeback() 返回 false,
     * folio_account_dirtied 不增加 dirty 统计, writeback 子系统不扫描
     * dirty folio, 导致 sync/fsync 不触发 writepage, 数据无法持久化.
     *
     * 参考: ceph_fill_super (fs/ceph/super.c) 调用 super_setup_bdi.
     * ramfs 不需要 (纯内存, 无 writeback). */
    ret = super_setup_bdi(sb);
    if (ret) {
        pr_err("powerfs: super_setup_bdi failed: %d\n", ret);
        kfree(sbi);
        return ret;
    }

    /* 设置默认 dentry operations (所有 dentry 共享) */
    powerfs_set_sb_dentry_ops(sb);

    /* 创建根目录 inode */
    root = powerfs_create_root(sb);
    if (!root) {
        kfree(sbi);
        sb->s_fs_info = NULL;
        return -ENOMEM;
    }

    /* 创建根 dentry */
    sb->s_root = d_make_root(root);
    if (!sb->s_root) {
        iput(root);
        kfree(sbi);
        sb->s_fs_info = NULL;
        return -ENOMEM;
    }

    sbi->initialized = true;
    sbi->shutting_down = false;

    /* 设置全局超级块指针 (供通信层使用) */
    g_powerfs_sb = sb;

    /* Stage C: 创建 writeback 异步 workqueue.
     * 使用 WQ_UNBOUND 提高扩展性 (work 不绑定到特定 CPU).
     * max_active=4 限制并发: powerfs_net_write 是同步网络调用, 过多并发
     * worker 会压垮单连接 (256 页 1MB 文件曾导致 132 个 worker 线程锁死). */
    sbi->writeback_wq = alloc_workqueue("powerfs_wb",
                                        WQ_UNBOUND | WQ_MEM_RECLAIM, 4);
    if (!sbi->writeback_wq) {
        pr_err("powerfs: failed to create writeback workqueue\n");
        /* 注意: 此处不清理, kill_sb 会处理. */
        return -ENOMEM;
    }
    atomic_set(&sbi->wb_in_flight, 0);

    /* 创建异步 inode 刷新工作队列 (NOTIFY → getattr 刷新元数据).
     * max_active=4: 允许多个 refresh work 并发执行, 避免单个 ilookup5 阻塞
     * (等待 I_FREEING inode) 导致所有 NOTIFY 处理积压.
     * WQ_MEM_RECLAIM: 确保 memory reclaim 路径可以提交 work. */
    powerfs_refresh_wq = alloc_workqueue("powerfs_refresh",
                                          WQ_UNBOUND | WQ_MEM_RECLAIM, 4);
    if (!powerfs_refresh_wq) {
        pr_err("powerfs: failed to create refresh workqueue\n");
        return -ENOMEM;
    }

    /* Phase 3: lease 续约 workqueue.
     * WQ_UNBOUND: 不绑定 CPU, 允许调度器自由调度.
     * WQ_MEM_RECLAIM: 参考 nfsiod/GFS2, 内存回收路径可用.
     * 不使用 WQ_HIGHPRI: 大量 lease 续约 (50+ 文件) 会饥饿其他工作队列
     *   (writeback/refresh), 导致 workqueue lockup + RCU stall.
     *   lease 续约不是延迟敏感的 — 过期前 10s 续约即可, 延迟 1-2s 可接受.
     * max_active=4: 限制并发续约数, 防止网络 I/O 阻塞淹没 workqueue. */
    sbi->lease_wq = alloc_workqueue("powerfs_lease",
                                     WQ_UNBOUND | WQ_MEM_RECLAIM, 4);
    if (!sbi->lease_wq) {
        pr_err("powerfs: failed to create lease workqueue\n");
        return -ENOMEM;
    }

    /* === 初始化 powerfs_net 连接池 (多节点 Delta Sync) === */
    powerfs_net_pool_init();

    /* 配置 Filer 节点 — 全部通过 Master 动态发现.
     *
     * 架构: 只需配置 master_addr (3 个 Raft 节点), Filer 和 Volume 地址
     * 全部通过 Master 的 GetTopology / ListFilers 动态发现.
     * 不再支持手动配置 filer_addr / volume_addr.
     *
     * 流程:
     *   1. 添加 Master 到连接池 (discover_filers/volumes 需要)
     *   2. powerfs_net_discover_filers 从 Master 获取 filer 列表
     *   3. powerfs_net_discover_volumes 从 Master 获取 volume 路由表 */
    {
        const char *maddr = (sbi->master_addr[0]) ? sbi->master_addr : NULL;
        __u16 mport = sbi->master_port ? sbi->master_port : 9334;

        /* 添加 Master 到连接池 */
        if (maddr) {
            char mbuf[256];
            char *mp, *mtok;
            strncpy(mbuf, maddr, sizeof(mbuf) - 1);
            mbuf[sizeof(mbuf) - 1] = '\0';
            mp = mbuf;
            while ((mtok = strsep(&mp, ",")) != NULL) {
                while (*mtok == ' ') mtok++;
                if (mtok[0] == '\0') continue;
                powerfs_net_add_server(mtok, mport,
                                      POWERFS_NET_SERVER_MASTER);
                pr_debug("powerfs: added master %s:%u\n", mtok, mport);
            }
        } else {
            pr_err("powerfs: master_addr not configured, cannot mount\n");
            return -EINVAL;
        }

        /* 从 Master 发现 filer 列表 */
        {
            int discovered = powerfs_net_discover_filers(maddr, mport);
            if (discovered > 0) {
                pr_info("powerfs: discovered %d filers via Master\n",
                        discovered);
            } else {
                pr_err("powerfs: Master filer discovery failed (%d), "
                       "cannot mount without filers\n", discovered);
                return -ENOLINK;
            }
        }
    }

    /* 初始化新连接池.
     *
     * 新架构 (per-conn 状态机 + shard 路由 + 事件驱动) 为唯一路径.
     * 断连检测由 sk_state_change 回调 + TCP keepalive 取代, 无需健康监控线程. */
    {
        const char *maddr = sbi->master_addr[0] ? sbi->master_addr : NULL;
        __u16 mport = sbi->master_port ? sbi->master_port : 9334;
        int pool_ret = powerfs_conn_pool_init(maddr, mport);
        if (pool_ret != 0) {
            pr_err("powerfs: connection pool init failed (%d)\n", pool_ret);
            return pool_ret;
        }
        pr_debug("powerfs: new connection pool initialized (sk callback + keepalive)\n");

        /* 从 Master GetTopology 发现 volume 路由表 (volume_id → conn_idx).
         * 失败不挂载失败: filer 元数据仍可用, 数据读写等 volume 上线后恢复. */
        if (maddr) {
            int vol_ret = powerfs_net_discover_volumes(maddr, mport);
            if (vol_ret < 0) {
                pr_warn("powerfs: discover_volumes failed: %d "
                        "(volume data IO will fail until routes established)\n",
                        vol_ret);
            } else {
                pr_debug("powerfs: volume routes discovered via Master\n");
            }
        }
    }

    pr_debug("powerfs: fill_super done, root ino=%lu\n", root->i_ino);
    pr_debug("powerfs: powerfs_net pool initialized (Delta Sync ready)\n");
    return 0;
}

/* ========== kill_sb_super: 卸载清理 ========== */

void powerfs_kill_sb_super(struct super_block *sb)
{
    struct powerfs_sb_info *sbi = POWERFS_SB_INFO(sb);

    pr_debug("powerfs: kill_sb_super\n");

    /* 清除全局超级块指针 */
    if (g_powerfs_sb == sb)
        g_powerfs_sb = NULL;

    /* 1. 先 sync 脏 inode (网络仍可用, write_inode 能同步 size 到 Filer,
     *    writepage 异步 work 也会执行网络写).
     *    如果先关闭网络, write_inode 的 setattr 会失败, inode 保持 dirty,
     *    evict_inodes 无法驱逐, 导致 umount 挂起或内存泄漏. */
    sync_filesystem(sb);

    /* 1b. Phase 3: 设置 shutting_down 标志.
     *    lease_renew_work_func 检查此标志, 避免在 destroy_workqueue(lease_wq)
     *    期间重新排队导致 flush 循环. 必须在销毁 lease_wq 之前设置. */
    if (sbi)
        sbi->shutting_down = true;

    /* 2. 销毁 writeback workqueue (Stage C).
     *    sync_filesystem 已触发 writeback 并等待 PageWriteback 清除,
     *    此时 workqueue 中所有 work 应已完成. destroy_workqueue 会
     *    drain 剩余 work (若有), 然后销毁. 必须在关闭网络前销毁,
     *    否则 work_fn 中的 powerfs_net_write 访问已关闭的网络. */
    if (sbi && sbi->writeback_wq) {
        destroy_workqueue(sbi->writeback_wq);
        sbi->writeback_wq = NULL;
    }
    /* 等待所有 wpw 的 call_rcu 回调完成 (work_struct 延迟释放).
     * destroy_workqueue 已确保 work_fn 全部执行完, 但 wpw 内存通过
     * call_rcu 异步释放. rcu_barrier 确保卸载时无残留内存. */
    rcu_barrier();

    /* 2b. 销毁 lease 续约工作队列.
     *    必须在关闭网络前销毁 (renew_work_fn 发网络请求).
     *    destroy_workqueue 会 flush 所有 pending delayed_work,
     *    之后 evict_inode 的 cancel_delayed_work_sync 仅取消定时器, 安全. */
    if (sbi && sbi->lease_wq) {
        destroy_workqueue(sbi->lease_wq);
        sbi->lease_wq = NULL;
    }

    /* 2c. 销毁 inode 刷新工作队列 (NOTIFY → getattr) */
    if (powerfs_refresh_wq) {
        destroy_workqueue(powerfs_refresh_wq);
        powerfs_refresh_wq = NULL;
    }
    /* 等待 refresh_work 的 call_rcu 回调完成 (与 wpw 相同的延迟释放模式) */
    rcu_barrier();

    /* 3. 设置 stopping 标志: 让 send_request 立即返回 -ENOTCONN,
     *    阻止 reconnect_work 在 g_pool 清零后访问野指针. */
    powerfs_net_set_stopping();

    /* 4. 关闭所有连接 (g_pool 会被清零) */
    powerfs_net_pool_cleanup();

    if (sbi) {
        kfree(sbi);
        sb->s_fs_info = NULL;
    }

    /* 5. kill_anon_super 会再次 sync (但已无脏数据) + shrink_dcache + evict_inodes */
    kill_anon_super(sb);
}

/*
 * powerfs_create_root - 创建根目录 inode
 */
struct inode *powerfs_create_root(struct super_block *sb)
{
    struct powerfs_inode_info *pi;
    struct inode *root;

    root = powerfs_new_inode(sb, S_IFDIR | 0755,
                              POWERFS_ROOT_INO, 0, "/");
    if (!root)
        return NULL;

    pi = POWERFS_I(root);

    /* 根目录的父目录是自己.
     * dir_complete=false: first readdir fetches from Filer to get any
     * pre-existing entries. The root may have files from previous mounts. */
    pi->parent_ino = POWERFS_ROOT_INO;
    WRITE_ONCE(pi->dir_complete, false);
    WRITE_ONCE(pi->dir_lease_expire, 0);

    /* 根目录设置 uid/gid 为 0 */
    root->i_uid = GLOBAL_ROOT_UID;
    root->i_gid = GLOBAL_ROOT_GID;

    pr_debug("powerfs: root inode created, ino=%lu\n", root->i_ino);
    return root;
}
