//! Stage 4 集成测试 — 验证 net_handler 调用 cap_manager 的集成模式.
//!
//! 聚焦 Stage 4 新增的 net_handler 层逻辑 (不直接构造 FilerNetHandler,
//! 因为它依赖 MetaShardManager/Raft; 而是模拟 net_handler 的调用序列,
//! 验证 cap_manager 作为 thin wrapper 在 net_handler 调用模式下的正确性):
//!
//! - **S4.1** 双向 client_id map 填充 + on_disconnect 反查 (cap_client_id_map)
//! - **S4.2** sweep loop: GATHER 超时 force-reclaim + promote 下发
//! - **S4.3** acquire_xlock GATHER dispatch + await (setattr/xattr 路径)
//! - **S4.4** handle_cap_release 复用 push_cap_upgrade_notify
//! - **S4.5** setattr/xattr 加锁 (Auth/Xattr/Nest) 与 File 锁不互斥
//! - **S4.6** on_disconnect 多 inode promote 下发
//! - **S4.7** sweep loop + on_disconnect 复用 push_cap_upgrade_notify 模式
//!
//! 测试使用 CapturingRevoker 捕获 recall 推送, 模拟 net_handler 的
//! recall_holder dispatch + push_cap_upgrade_notify 下发.

use powerfs_filer::cap_manager::{CapManager, CapRevoker, CapSet};
use powerfs_filer::lock_arbiter::{LockAcquireResult, LockArbiter, LockType};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ==================== 测试辅助: 捕获 recall + upgrade 推送 ====================

/// 测试用 CapRevoker — 记录所有 recall 调用供断言.
/// 模拟 net_handler 的 NetCapRevoker (实际推 CapRecallNotify 给客户端).
#[derive(Debug, Default)]
struct CapturingRevoker {
    recalls: Mutex<Vec<(u64, String, String, CapSet, CapSet, u64)>>,
}

impl CapturingRevoker {
    fn count(&self) -> usize {
        self.recalls.lock().unwrap().len()
    }
}

impl CapRevoker for CapturingRevoker {
    fn recall(
        &self,
        inode: u64,
        holder: &str,
        token: &str,
        caps_to_recall: CapSet,
        retained_caps: CapSet,
        new_epoch: u64,
    ) -> Result<(), String> {
        self.recalls.lock().unwrap().push((
            inode,
            holder.to_string(),
            token.to_string(),
            caps_to_recall,
            retained_caps,
            new_epoch,
        ));
        Ok(())
    }
}

/// 模拟 net_handler 的双向 client_id 映射 (string ↔ u64).
/// handle_cap_open_grant 填充, on_disconnect 反查, push_cap_upgrade_notify 正查.
#[derive(Debug, Default)]
struct ClientIdMap {
    string_to_net: Mutex<HashMap<String, u64>>,
    net_to_string: Mutex<HashMap<u64, String>>,
}

impl ClientIdMap {
    /// 模拟 handle_cap_open_grant 的双向 map 填充.
    fn insert(&self, string_cid: &str, net_cid: u64) {
        self.string_to_net
            .lock()
            .unwrap()
            .insert(string_cid.to_string(), net_cid);
        self.net_to_string
            .lock()
            .unwrap()
            .insert(net_cid, string_cid.to_string());
    }

    /// 模拟 push_cap_upgrade_notify 的 string→u64 正查.
    fn lookup_net(&self, string_cid: &str) -> Option<u64> {
        self.string_to_net.lock().unwrap().get(string_cid).copied()
    }

    /// 模拟 on_disconnect 的 u64→string 反查.
    fn lookup_string(&self, net_cid: u64) -> Option<String> {
        self.net_to_string.lock().unwrap().get(&net_cid).cloned()
    }

    /// 模拟 on_disconnect 清理双向 map.
    fn remove(&self, net_cid: u64) {
        if let Some(s) = self.net_to_string.lock().unwrap().remove(&net_cid) {
            self.string_to_net.lock().unwrap().remove(&s);
        }
    }
}

/// 模拟 net_handler 的 push_cap_upgrade_notify:
/// 正查 string→u64, 记录升级通知 (实际推 CapUpgradeNotify 给客户端).
#[derive(Debug, Default)]
struct UpgradeNotifyCapture {
    notifies: Mutex<Vec<(u64, String, u64, CapSet)>>,
}

impl UpgradeNotifyCapture {
    fn push(&self, inode: u64, survivor: &str, new_sn: u64, caps: CapSet) {
        self.notifies
            .lock()
            .unwrap()
            .push((inode, survivor.to_string(), new_sn, caps));
    }
    fn snapshot(&self) -> Vec<(u64, String, u64, CapSet)> {
        self.notifies.lock().unwrap().clone()
    }
    fn count(&self) -> usize {
        self.notifies.lock().unwrap().len()
    }
}

// ==================== S4.1: 双向 client_id map 填充 + on_disconnect 反查 ====================

#[test]
fn s4_1_client_id_map_bidirectional_lookup() {
    let mgr = CapManager::new();
    let map = ClientIdMap::default();

    // 模拟 handle_cap_open_grant: 多 client 注册, net_cid 分配
    let _r1 = mgr.open_grant(100, "fuse-1", true);
    map.insert("fuse-1", 5001);
    let _r2 = mgr.open_grant(100, "fuse-2", false);
    map.insert("fuse-2", 5002);
    let _r3 = mgr.open_grant(200, "fuse-1", true);
    // fuse-1 在两个 inode 都有锁, 但 net_cid 相同 (同连接)

    // 正查 (push_cap_upgrade_notify 用)
    assert_eq!(map.lookup_net("fuse-1"), Some(5001));
    assert_eq!(map.lookup_net("fuse-2"), Some(5002));
    assert_eq!(map.lookup_net("fuse-3"), None, "未注册的 client 返回 None");

    // 反查 (on_disconnect 用)
    assert_eq!(map.lookup_string(5001), Some("fuse-1".to_string()));
    assert_eq!(map.lookup_string(5002), Some("fuse-2".to_string()));
    assert_eq!(map.lookup_string(9999), None, "未注册的 net_cid 返回 None");

    // 模拟 on_disconnect: fuse-2 断连 → 反查 → evict → 清理 map
    let string_cid = map.lookup_string(5002).unwrap();
    assert_eq!(string_cid, "fuse-2");
    let (changed, _promote) = mgr.evict_session_full(&string_cid);
    assert!(!changed.is_empty(), "fuse-2 有锁, changed 非空");
    // evict fuse-2 (reader) 后剩 fuse-1 (已 LONER), 不触发 promote (已 full)
    // (promote_to_loner 仅对 caps 不全的 holder bump sn, fuse-1 已 EXCL)
    map.remove(5002);
    assert_eq!(map.lookup_net("fuse-2"), None, "清理后正查 None");
    assert_eq!(map.lookup_string(5002), None, "清理后反查 None");
    assert_eq!(map.lookup_net("fuse-1"), Some(5001), "fuse-1 仍在");
}

// ==================== S4.2: sweep loop lease 过期 force-reclaim + promote 下发 ====================

#[test]
fn s4_2_sweep_loop_force_reclaim_promotes_loner() {
    // 场景: C1 LONER + C2 reader → SHARED. C1 的 lease 先过期 (错开 open 时间),
    // tick → garbage_collect 清理 C1 → holders=[C2] → promote C2 to LONER.
    // (tick 的 promote 检查仅在 garbage_collect 返回 cleaned 时触发,
    //  GATHER 超时 gather_timeout 只唤醒 waiter, 不触发 promote)
    let arbiter = Arc::new(LockArbiter::new_for_test(
        Duration::from_secs(10),    // 长 recall_timeout, 不触发 GATHER 超时
        Duration::from_millis(100), // 短 lease_duration, 快速过期
    ));
    let revoker = Arc::new(CapturingRevoker::default());
    let mgr = CapManager::new()
        .with_arbiter(arbiter)
        .with_revoker(revoker.clone());

    // C1 open RDWR → LONER (lease 100ms, expire_at = T0+100)
    let r1 = mgr.open_grant(200, "C1", true);
    assert!(r1.granted_caps.is_exclusive(), "C1 LONER");

    // 错开 50ms, 让 C2 的 lease 比 C1 晚 50ms 过期
    std::thread::sleep(Duration::from_millis(50));

    // C2 open RDONLY → SHARED (C2 expire_at = T0+50+100 = T0+150)
    let _r2 = mgr.open_grant(200, "C2", false);

    // sleep 60ms → T0+110: C1 过期 (T0+100 < T0+110), C2 仍有效 (T0+150 > T0+110)
    std::thread::sleep(Duration::from_millis(60));
    let promotes = mgr.drain_expired_recalls();

    // C1 lease 过期被 garbage_collect 清理, holders 剩 C2 → promote C2
    assert_eq!(promotes.len(), 1, "C2 promoted to LONER");
    let (p_inode, p_lt, p_survivor, p_sn, p_caps) = &promotes[0];
    assert_eq!(*p_inode, 200);
    assert_eq!(*p_lt, LockType::File, "File 锁 promote");
    assert_eq!(p_survivor, "C2", "C2 is the survivor");
    assert!(p_caps.is_exclusive(), "C2 升级为 EXCLUSIVE");
    assert!(*p_sn > 0, "new sn for fencing");

    // 模拟 push_cap_upgrade_notify: 正查 map, 推 CapUpgradeNotify
    let notify_cap = UpgradeNotifyCapture::default();
    let map = ClientIdMap::default();
    map.insert("C2", 6002);
    for (inode, lt, survivor, new_sn, caps) in &promotes {
        if *lt == LockType::File {
            if let Some(net_cid) = map.lookup_net(survivor) {
                notify_cap.push(*inode, survivor, *new_sn, *caps);
                let _ = net_cid;
            }
        }
    }
    assert_eq!(notify_cap.count(), 1, "1 CapUpgradeNotify pushed");
    let n = &notify_cap.snapshot()[0];
    assert_eq!(n.1, "C2");
    assert!(n.3.is_exclusive());
}

// ==================== S4.3: acquire_xlock GATHER dispatch + await ====================

#[tokio::test]
async fn s4_3_acquire_xlock_gather_dispatch_and_await() {
    let arbiter = Arc::new(LockArbiter::new_for_test(
        Duration::from_secs(10), // 长 timeout, 让我们手动 ACK
        Duration::from_secs(30),
    ));
    let revoker = Arc::new(CapturingRevoker::default());
    let mgr = CapManager::new()
        .with_arbiter(arbiter)
        .with_revoker(revoker.clone());

    // C1 持 Auth 锁 (模拟 setattr 路径): xlock Auth
    let r1 = mgr.arbiter().xlock(300, LockType::Auth, "net-C1");
    assert!(r1.sn > 0, "C1 xlock Auth granted");

    // C2 想要 Auth 锁 (冲突) → xlock_async 返回 Waiting
    let acquire = mgr.arbiter().xlock_async(300, LockType::Auth, "net-C2");
    match acquire {
        LockAcquireResult::Granted(_) => {
            panic!("C2 should NOT get Auth immediately — C1 holds it");
        }
        LockAcquireResult::Waiting { recall_tasks, rx } => {
            // 模拟 net_handler acquire_xlock: dispatch recall 给 C1
            assert!(!recall_tasks.is_empty(), "C1 must be recalled");
            for t in &recall_tasks {
                let token = mgr.make_token(300, &t.client_id, t.sn);
                // recall_holder 推 CapRecallNotify 给 C1
                mgr.recall_holder(
                    300,
                    &t.client_id,
                    &token,
                    t.caps_to_recall,
                    t.retained_caps,
                    t.new_epoch,
                )
                .unwrap();
            }
            assert!(revoker.count() >= 1, "recall dispatched to C1");

            // C1 ACK recall — GATHER 完成, C1 释放 Auth 锁, waiter 被唤醒.
            // recall_tasks[0].sn 是 C1 的 sn.
            let c1_sn = recall_tasks[0].sn;
            let ack_token = mgr.make_token(300, "net-C1", c1_sn);
            // cap_manager.recall_ack 遍历所有 LockType, 命中 Auth 的 GATHER.
            let _ = mgr.recall_ack(300, "net-C1", &ack_token);

            // GATHER 完成 → waiter 被唤醒 → await rx 获得 sn
            match rx.await {
                Ok(g) => {
                    assert!(g.sn > 0, "C2 got Auth sn after GATHER");
                    assert_eq!(g.client_id, "net-C2");
                    // 完成操作后, C2 必须释放 Auth 锁
                    mgr.arbiter().unlock(300, LockType::Auth, g.sn);
                }
                Err(_) => panic!("waiter should be woken after GATHER"),
            }
        }
    }
}

// ==================== S4.4: handle_cap_release 复用 push_cap_upgrade_notify ====================

#[test]
fn s4_4_release_cap_reuses_push_upgrade_notify() {
    let mgr = CapManager::new();

    // C1 LONER + C2 reader → SHARED
    let r1 = mgr.open_grant(400, "C1", true);
    let _r2 = mgr.open_grant(400, "C2", false);

    // C1 release → promote C2 to LONER → UpgradeTask
    let up = mgr.release_cap(400, "C1", &r1.token).unwrap();
    assert!(up.is_some(), "C2 promoted");
    let upgrade = up.unwrap();
    assert_eq!(upgrade.holder, "C2");
    assert!(upgrade.granted_caps.is_exclusive());

    // 模拟 net_handler handle_cap_release 复用 push_cap_upgrade_notify:
    // 正查 map, 推 CapUpgradeNotify 给 survivor (C2)
    let map = ClientIdMap::default();
    map.insert("C1", 7001);
    map.insert("C2", 7002);
    let notify_cap = UpgradeNotifyCapture::default();

    let net_cid = map.lookup_net(&upgrade.holder);
    assert_eq!(net_cid, Some(7002), "C2 的 net_cid 正查成功");
    notify_cap.push(400, &upgrade.holder, upgrade.sn, upgrade.granted_caps);

    assert_eq!(notify_cap.count(), 1);
    let n = &notify_cap.snapshot()[0];
    assert_eq!(n.1, "C2");
    assert!(n.3.is_exclusive(), "upgrade to EXCLUSIVE");
}

// ==================== S4.5: setattr/xattr 加锁与 File 锁不互斥 ====================

#[test]
fn s4_5_setattr_xattr_locks_independent_of_file_lock() {
    let mgr = CapManager::new();

    // C1 持 File EXCL (LONER writer)
    let r1 = mgr.open_grant(500, "C1", true);
    assert!(r1.granted_caps.is_exclusive(), "C1 File LONER");

    // C2 xlock Auth (setattr 路径) — 不同 lock_type, 不冲突
    let r2 = mgr.arbiter().xlock(500, LockType::Auth, "net-C2");
    assert!(r2.sn > 0, "C2 Auth xlock granted immediately (不冲突 File)");
    mgr.arbiter().unlock(500, LockType::Auth, r2.sn);

    // C2 xlock Xattr (setxattr 路径) — 不同 lock_type, 不冲突
    let r3 = mgr.arbiter().xlock(500, LockType::Xattr, "net-C2");
    assert!(r3.sn > 0, "C2 Xattr xlock granted (不冲突 File/Auth)");
    mgr.arbiter().unlock(500, LockType::Xattr, r3.sn);

    // C2 xlock Nest (setattr_meta 路径) — 不同 lock_type
    let r4 = mgr.arbiter().xlock(500, LockType::Nest, "net-C2");
    assert!(r4.sn > 0, "C2 Nest xlock granted (不冲突 File/Auth/Xattr)");
    mgr.arbiter().unlock(500, LockType::Nest, r4.sn);

    // 反向: C1 持 Auth 锁, C2 open_grant File — 不冲突
    let mgr2 = CapManager::new();
    let _ra = mgr2.arbiter().xlock(501, LockType::Auth, "net-C1");
    let rfile = mgr2.open_grant(501, "C2", true);
    assert!(
        rfile.granted_caps.is_exclusive(),
        "C2 File LONER granted (Auth 锁不阻塞 File)"
    );
}

// ==================== S4.6: on_disconnect 多 inode promote 下发 ====================

#[test]
fn s4_6_on_disconnect_multi_inode_promote_dispatch() {
    let mgr = CapManager::new();
    let map = ClientIdMap::default();
    let notify_cap = UpgradeNotifyCapture::default();

    // 场景: C1 在 inode 600/601 都是 LONER, 各有一个 reader (C2/C3)
    // evict C1 后, C2 和 C3 各自升级为 LONER (2 个 promote)
    let _r1a = mgr.open_grant(600, "C1", true);
    let _r2a = mgr.open_grant(600, "C2", false); // C1 LONER + C2 reader
    let _r1b = mgr.open_grant(601, "C1", true);
    let _r3b = mgr.open_grant(601, "C3", false); // C1 LONER + C3 reader

    map.insert("C1", 8001);
    map.insert("C2", 8002);
    map.insert("C3", 8003);

    // 模拟 on_disconnect(net_cid=8001):
    let string_cid = map.lookup_string(8001).unwrap();
    assert_eq!(string_cid, "C1");
    let (_changed, promote_tasks) = mgr.evict_session_full(&string_cid);

    // C1 在两个 inode 都是 LONER, evict 后各剩 1 个 reader → 2 个 promote
    assert_eq!(promote_tasks.len(), 2, "2 survivors promoted");
    let survivors: Vec<String> = promote_tasks
        .iter()
        .map(|(_, _, s, _, _)| s.clone())
        .collect();
    assert!(
        survivors.contains(&"C2".to_string()),
        "C2 promoted on inode 600"
    );
    assert!(
        survivors.contains(&"C3".to_string()),
        "C3 promoted on inode 601"
    );

    // 模拟 push_cap_upgrade_notify: 对 File 锁的 promote 下发
    for (inode, lt, survivor, new_sn, caps) in &promote_tasks {
        if *lt == LockType::File {
            if map.lookup_net(survivor).is_some() {
                notify_cap.push(*inode, survivor, *new_sn, *caps);
            }
        }
    }
    assert_eq!(notify_cap.count(), 2, "2 CapUpgradeNotify pushed");

    // 清理 map
    map.remove(8001);
    assert_eq!(map.lookup_net("C1"), None);
}

// ==================== S4.7: sweep + on_disconnect 复用 push_cap_upgrade_notify 模式 ====================

#[test]
fn s4_7_sweep_and_disconnect_reuse_push_notify_pattern() {
    // 验证 force_reclaim_expired_cap_recalls 和 on_disconnect 都通过
    // 相同的 push_cap_upgrade_notify 模式下发 promote (DRY 原则)
    let arbiter = Arc::new(LockArbiter::new_for_test(
        Duration::from_secs(10),    // 长 recall_timeout
        Duration::from_millis(100), // 短 lease_duration
    ));
    let mgr = CapManager::new().with_arbiter(arbiter);
    let notify_cap = UpgradeNotifyCapture::default();
    let map = ClientIdMap::default();

    // 场景 A: lease 过期 promote (sweep loop 路径)
    // C1 LONER + C4 reader → SHARED; C1 lease 先过期 → promote C4.
    let _r1 = mgr.open_grant(700, "C1", true); // C1 LONER (expire T0+100)
    std::thread::sleep(Duration::from_millis(50));
    let _r4 = mgr.open_grant(700, "C4", false); // C4 reader (expire T0+150)
    map.insert("C1", 9001);
    map.insert("C4", 9004);

    // sleep 60ms → T0+110: C1 过期, C4 仍有效
    std::thread::sleep(Duration::from_millis(60));
    let sweep_promotes = mgr.drain_expired_recalls();
    for (inode, lt, survivor, new_sn, caps) in &sweep_promotes {
        if *lt == LockType::File && map.lookup_net(survivor).is_some() {
            notify_cap.push(*inode, survivor, *new_sn, *caps);
        }
    }

    // 场景 B: on_disconnect promote (evict 路径)
    let _r3 = mgr.open_grant(701, "C3", true); // C3 LONER
    let _r5 = mgr.open_grant(701, "C5", false); // C3 LONER + C5 reader
    map.insert("C3", 9003);
    map.insert("C5", 9005);

    let string_cid = map.lookup_string(9003).unwrap();
    let (_changed, evict_promotes) = mgr.evict_session_full(&string_cid);
    for (inode, lt, survivor, new_sn, caps) in &evict_promotes {
        if *lt == LockType::File && map.lookup_net(survivor).is_some() {
            notify_cap.push(*inode, survivor, *new_sn, *caps);
        }
    }

    // 两条路径都通过相同模式下发 promote
    // sweep: 1 个 (C4 升级); evict: 1 个 (C5 升级)
    let total = notify_cap.count();
    assert!(
        total >= 2,
        "至少 2 个 CapUpgradeNotify (sweep + evict), got {}",
        total
    );
    let survivors: Vec<String> = notify_cap.snapshot().iter().map(|n| n.1.clone()).collect();
    assert!(
        survivors.contains(&"C4".to_string()) || survivors.contains(&"C5".to_string()),
        "survivors should contain C4 or C5, got {:?}",
        survivors
    );
}

// ==================== S4.8: acquire_xlock 立即授予 (无冲突) ====================

#[tokio::test]
async fn s4_8_acquire_xlock_granted_immediately_when_no_conflict() {
    let mgr = CapManager::new();

    // 无 holder → xlock_async 立即 Granted
    let acquire = mgr.arbiter().xlock_async(800, LockType::Auth, "net-C1");
    match acquire {
        LockAcquireResult::Granted(g) => {
            assert!(g.sn > 0, "granted immediately");
            assert_eq!(g.client_id, "net-C1");
            // 完成操作后释放
            mgr.arbiter().unlock(800, LockType::Auth, g.sn);
        }
        LockAcquireResult::Waiting { .. } => {
            panic!("should be Granted when no holder");
        }
    }
}

// ==================== S4.9: recall_ack 遍历所有 LockType ====================

#[test]
fn s4_9_recall_ack_iterates_all_lock_types() {
    // 验证 cap_manager::recall_ack 遍历所有 LockType (Stage 3 修复的 bug):
    // 非 File 锁的 GATHER 也能被 recall_ack 推进
    let arbiter = Arc::new(LockArbiter::new_for_test(
        Duration::from_secs(10),
        Duration::from_secs(30),
    ));
    let revoker = Arc::new(CapturingRevoker::default());
    let mgr = CapManager::new()
        .with_arbiter(arbiter)
        .with_revoker(revoker.clone());

    // C1 xlock Auth
    let r1 = mgr.arbiter().xlock(900, LockType::Auth, "net-C1");
    assert!(r1.sn > 0);

    // C2 xlock_async Auth → Waiting, recall C1
    let acquire = mgr.arbiter().xlock_async(900, LockType::Auth, "net-C2");
    let recall_tasks = match acquire {
        LockAcquireResult::Granted(_) => panic!("should be Waiting"),
        LockAcquireResult::Waiting { recall_tasks, .. } => recall_tasks,
    };
    assert!(!recall_tasks.is_empty(), "C1 Auth recalled");

    // recall_ack 用 C1 的 sn (从 recall_tasks 取)
    let c1_sn = recall_tasks[0].sn;
    let token = mgr.make_token(900, "net-C1", c1_sn);
    // cap_manager.recall_ack 遍历所有 LockType, 命中 Auth 的 GATHER
    let result = mgr.recall_ack(900, "net-C1", &token);
    assert!(result.is_ok(), "recall_ack succeeds for Auth lock");
}
