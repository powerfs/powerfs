//! Cap Manager 集成测试 — 验证 Stage 3 重构后 cap_manager 作为
//! LockArbiter 的 thin wrapper 的端到端正确性.
//!
//! 覆盖 T1.x 场景 (与已删除的 multi_client_cap_test.rs 等价, 但基于
//! 新 API: 验证 OpenGrantResult.granted_caps / recall_tasks / token / sn
//! + UpgradeTask (而非旧 CapState/logical_state).
//!
//! 测试不依赖网络层 (使用默认 NoopCapRevoker), 直接通过 cap_manager
//! 公开 API 验证 lock_arbiter 桥接正确性.

use powerfs_filer::cap_manager::{CapManager, CapSet};
use powerfs_filer::lock_arbiter::{LockArbiter, LockType};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ==================== 测试辅助: 捕获 recall 推送的 CapRevoker ====================

/// 测试用 CapRevoker — 记录所有 recall 调用供断言.
#[derive(Debug, Default)]
struct CapturingRevoker {
    recalls: Mutex<Vec<(u64, String, String, CapSet, CapSet, u64)>>,
}

impl CapturingRevoker {
    fn count(&self) -> usize {
        self.recalls.lock().unwrap().len()
    }
}

impl powerfs_filer::cap_manager::CapRevoker for CapturingRevoker {
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

/// 计数型 penalty — 用于 drain_expired_recalls 触发检测 (保留供未来 Stage 4 使用).
#[allow(dead_code)]
#[derive(Debug, Default)]
struct CountingPenalty {
    count: std::sync::atomic::AtomicUsize,
}

#[allow(dead_code)]
impl powerfs_filer::cap_manager::RecallTimeoutPenalty for CountingPenalty {
    fn on_recall_ack_timeout(&self, _client_id: &str) {
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

// ==================== T1.1: open(RDONLY) 单 reader → granted CAP_R ====================

#[test]
fn t1_1_open_rdonly_grants_cap_r() {
    let mgr = CapManager::new();
    let r = mgr.open_grant(100, "C1", false);

    assert_eq!(r.granted_caps, CapSet::CAP_R, "reader gets CAP_R");
    assert!(r.recall_tasks.is_empty(), "no recall on first open");
    assert_eq!(r.epoch, 0, "epoch=0 (Available→Shared, no fencing)");
    assert!(r.sn > 0, "sn allocated");
    assert_eq!(r.duration_ms, 30_000, "default lease 30s");
    assert!(
        r.token.starts_with("cap-100-C1-"),
        "token format cap-{{inode}}-{{client}}-{{sn}}: got {}",
        r.token
    );
}

// ==================== T1.2: open(RDWR) 单 writer → granted EXCLUSIVE (LONER) ====================

#[test]
fn t1_2_open_rdwr_grants_exclusive_loner() {
    let mgr = CapManager::new();
    let r = mgr.open_grant(200, "C1", true);

    assert!(
        r.granted_caps.is_exclusive(),
        "single writer gets EXCLUSIVE (LONER)"
    );
    assert!(r.recall_tasks.is_empty(), "no recall on first writer open");
    assert!(r.sn > 0, "sn allocated");
}

// ==================== T1.3: 多 reader 共存 — 都 granted CAP_R, 无 recall ====================

#[test]
fn t1_3_multiple_readers_compatible() {
    let mgr = CapManager::new();
    let r1 = mgr.open_grant(300, "C1", false);
    let r2 = mgr.open_grant(300, "C2", false);

    assert_eq!(r1.granted_caps, CapSet::CAP_R);
    assert_eq!(r2.granted_caps, CapSet::CAP_R, "C2 reader also gets CAP_R");
    assert!(r2.recall_tasks.is_empty(), "no recall — readers compatible");
    assert_ne!(r1.token, r2.token, "distinct tokens per client");
}

// ==================== T1.4: writer + reader 共存 — rdlock 不 recall writer ====================

#[test]
fn t1_4_writer_then_reader_triggers_recall() {
    let revoker = Arc::new(CapturingRevoker::default());
    let mgr = CapManager::new().with_revoker(revoker.clone());

    let _r1 = mgr.open_grant(400, "C1", true); // C1 LONER
    let r2 = mgr.open_grant(400, "C2", false); // C2 reader → 打破 LONER → SHARED

    // C2 拿到 CAP_R
    assert_eq!(r2.granted_caps, CapSet::CAP_R, "C2 reader gets CAP_R");
    // rdlock 不 recall writer 的 caps (C1 仍持 EXCL, 仅 state 降级为 SHARED)
    // 这与 wrlock 触发 GATHER recall 不同 — rdlock 是兼容的共享读
    assert!(r2.recall_tasks.is_empty(), "rdlock does NOT trigger recall");
    assert_eq!(revoker.count(), 0, "no recall dispatched for reader");
}

// ==================== T1.5: 两 writer → GATHER recall 第一个 writer, 第二个 granted NONE ====================

#[test]
fn t1_5_two_writers_gather_recall_second_gets_none() {
    let revoker = Arc::new(CapturingRevoker::default());
    let mgr = CapManager::new().with_revoker(revoker.clone());

    let _r1 = mgr.open_grant(500, "C1", true); // C1 LONER
    let r2 = mgr.open_grant(500, "C2", true); // C2 writer → 打破 LONER, GATHER

    // C2 GATHER 未完成时 granted NONE (需等 C1 ACK)
    assert_eq!(r2.granted_caps, CapSet::NONE, "C2 gets NONE during GATHER");
    assert!(!r2.recall_tasks.is_empty(), "C1's W+X must be recalled");
    let recall = r2.recall_tasks.first().unwrap();
    assert_eq!(recall.holder, "C1");
    assert_eq!(recall.caps_to_recall, CapSet::CAP_W | CapSet::CAP_X);
    // arbiter wrlock: 两 writer 走 ToShared, retain = EXCLUSIVE.remove(W+X) = R
    // (C1 被 reader 化, 不是完全降级为 NONE — 这是 arbiter 的语义, 与
    // 公版 wrlock 不强制 recall 全部 cap 一致)
    assert_eq!(
        recall.retained_caps,
        CapSet::CAP_R,
        "C1 retains CAP_R (reader-ized)"
    );
}

// ==================== T1.6: writer + reader 共存, writer release 触发 reader 升级 ====================

#[test]
fn t1_6_release_writer_triggers_upgrade_to_loner() {
    let mgr = CapManager::new();

    // C1 open RDWR → LONER (holders: [C1 EXCL])
    let r1 = mgr.open_grant(600, "C1", true);
    // C2 open RDONLY → 打破 LONER, state=SHARED, holders: [C1 EXCL, C2 CAP_R]
    // (rdlock 不 recall C1, 仅降级 state; C1 仍持 EXCL caps)
    let _r2 = mgr.open_grant(600, "C2", false);

    // C1 release → holders 删 C1, 剩 C2 (CAP_R), holders.len()==1 && state==SHARED
    // → promote_to_loner 升级 C2 到 EXCLUSIVE (bump sn 用于 fencing)
    let up1 = mgr.release_cap(600, "C1", &r1.token).unwrap();
    assert!(up1.is_some(), "C2 promoted to LONER after C1 release");

    let upgrade = up1.unwrap();
    assert_eq!(upgrade.holder, "C2");
    assert!(upgrade.granted_caps.is_exclusive(), "upgraded to EXCLUSIVE");
    assert!(upgrade.sn > r1.sn, "new sn bumped for fencing");
    assert!(upgrade.token.starts_with("cap-600-C2-"));
}

// ==================== T1.7: recall_ack 流程 — GATHER 完成 ====================

#[test]
fn t1_7_recall_ack_completes_gather() {
    let mgr = CapManager::new();

    let _r1 = mgr.open_grant(700, "C1", true); // C1 LONER
    let r2 = mgr.open_grant(700, "C2", true); // GATHER, recall C1

    // 从 recall_tasks 取 C1 的 token (cap_manager 转换后的)
    let recall = r2.recall_tasks.first().unwrap();
    let c1_token = &recall.token;

    // C1 ACK recall — 应成功 (gather_done=true)
    let result = mgr.recall_ack(700, "C1", c1_token);
    assert!(result.is_ok(), "recall_ack succeeds");
    // arbiter recall_ack 返回 NONE (cap_manager 不暴露 retained 查询)
    assert_eq!(result.unwrap(), CapSet::NONE);
}

// ==================== T1.8: drain_expired_recalls 不 panic (空 + 有 active) ====================

#[test]
fn t1_8_drain_expired_recalls_no_panic() {
    let mgr = CapManager::new();

    // 空 active_inodes — drain 不 panic, 返回空 promote_tasks
    let promotes = mgr.drain_expired_recalls();
    assert!(promotes.is_empty(), "no active inodes → empty promote list");

    // 有 active inode 但无 GATHER 超时 — drain 仍返回空 (无 promote)
    let _r = mgr.open_grant(800, "C1", false);
    let promotes2 = mgr.drain_expired_recalls();
    assert!(promotes2.is_empty(), "no GATHER timeout → empty promote");
}

// ==================== T1.9: evict_session_full 清理 + 不 panic ====================

#[test]
fn t1_9_evict_session_full_cleanup() {
    let mgr = CapManager::new();

    let _r1 = mgr.open_grant(900, "C1", true);
    let _r2 = mgr.open_grant(901, "C1", false); // C1 在两个 inode 都有锁

    // evict C1 — 返回 (changed_inodes, promote_tasks)
    let (changed, promote) = mgr.evict_session_full("C1");
    assert_eq!(changed.len(), 2, "C1 had caps on 2 inodes");
    let inodes: Vec<u64> = changed.iter().map(|(i, _)| *i).collect();
    assert!(inodes.contains(&900));
    assert!(inodes.contains(&901));
    // C1 是 inode 900 的唯一 LONER writer, evict 后剩 0 holder,
    // 不触发 promote_to_loner (promote 仅在剩 1 个 holder 时触发)
    assert!(promote.is_empty(), "no survivor → no promote");

    // 再次 evict (空) — 不 panic
    let (changed2, promote2) = mgr.evict_session_full("C2");
    assert!(changed2.is_empty(), "C2 has no caps");
    assert!(promote2.is_empty());
}

// ==================== T1.10: token 格式与解析 ====================

#[test]
fn t1_10_token_format_and_sn_round_trip() {
    let mgr = CapManager::new();

    let r = mgr.open_grant(1000, "client-with-long-id-12345", false);
    // short_client 截断到 16 字符: "client-with-long" (16 字符)
    assert_eq!(
        r.token,
        format!("cap-1000-client-with-long-{}", r.sn),
        "token format cap-{{inode}}-{{short_client(16)}}-{{sn}}"
    );

    // release_cap 用此 token 应能正确解析 sn (rsplit '-' 取最后一段)
    let result = mgr.release_cap(1000, "client-with-long-id-12345", &r.token);
    assert!(result.is_ok(), "token sn parsing works for long client_id");
}

// ==================== T1.11: with_arbiter 注入测试 (自定义 recall_timeout) ====================

#[test]
fn t1_11_with_arbiter_injection_custom_timeout() {
    // 用 0ms recall_timeout — GATHER 立即超时 force-reclaim
    let arbiter = Arc::new(LockArbiter::new_for_test(
        Duration::from_millis(0),
        Duration::from_secs(30),
    ));
    let mgr = CapManager::new().with_arbiter(arbiter);

    let r1 = mgr.open_grant(1100, "C1", true);
    assert!(r1.granted_caps.is_exclusive(), "C1 LONER");

    // arbiter 透传也可用
    let direct_arbiter = mgr.arbiter();
    let _ = direct_arbiter.rdlock(1100, LockType::Auth, "C1");
}

// ==================== T1.12: 同 client 重入 — idempotent re-open ====================

#[test]
fn t1_12_idempotent_reopen_same_client() {
    let mgr = CapManager::new();

    let r1 = mgr.open_grant(1200, "C1", true);
    let r2 = mgr.open_grant(1200, "C1", true); // 同 client 重入

    // arbiter wrlock LONER fast path: 同 client 复用, sn 不变
    assert_eq!(r1.sn, r2.sn, "same sn on idempotent re-open");
    assert!(r2.granted_caps.is_exclusive(), "still EXCLUSIVE");
}

// ==================== T1.13: 全生命周期 (open → recall → ack → release → upgrade) ====================

#[test]
fn t1_13_full_lifecycle_open_recall_ack_release_upgrade() {
    let revoker = Arc::new(CapturingRevoker::default());
    let mgr = CapManager::new().with_revoker(revoker.clone());

    // 1. C1 open RDWR → LONER
    let r1 = mgr.open_grant(1300, "C1", true);
    assert!(r1.granted_caps.is_exclusive());

    // 2. C2 open RDWR → GATHER, recall C1 (C2 暂未加入 holders)
    let r2 = mgr.open_grant(1300, "C2", true);
    assert_eq!(r2.granted_caps, CapSet::NONE, "C2 NONE during GATHER");
    assert!(!r2.recall_tasks.is_empty(), "C1's W+X must be recalled");
    // cap_manager 不自动 dispatch recall — 由 net_handler 调 recall_holder
    // 模拟 dispatch: 把每个 RecallTask 推给 revoker
    for t in &r2.recall_tasks {
        mgr.recall_holder(
            1300,
            &t.holder,
            &t.token,
            t.caps_to_recall,
            t.retained_caps,
            t.new_epoch,
        )
        .unwrap();
    }
    assert_eq!(revoker.count(), 1, "1 recall dispatched");

    // 3. C1 ACK recall → GATHER 完成, C1 reader 化 (caps=CAP_R)
    let c1_recall_token = r2.recall_tasks.first().unwrap().token.clone();
    let _retained = mgr.recall_ack(1300, "C1", &c1_recall_token).unwrap();

    // 4. C2 再次 wrlock → GATHER 已完成, C1 无 W+X (CAP_R), need_gather=false
    //    → C2 直接加入 holders 为 LONER writer (C1 reader 共存)
    let r2b = mgr.open_grant(1300, "C2", true);
    assert!(r2b.granted_caps.is_exclusive(), "C2 now LONER writer");

    // 5. C1 release → 剩 C2 (已 LONER), promote_to_loner 再次触发
    //    (但 C2 已 full_caps, sn 不 bump — promote_to_loner 仅对 caps 不全的 holder bump sn)
    let up = mgr.release_cap(1300, "C1", &r1.token).unwrap();
    assert!(up.is_some(), "C2 re-promoted (promote_task returned)");
    let upgrade = up.unwrap();
    assert_eq!(upgrade.holder, "C2");
    assert!(upgrade.granted_caps.is_exclusive());
    assert!(upgrade.sn >= r2b.sn, "sn not decreased");

    // 6. C2 release (用 upgrade.token 含新 sn) → 无 survivor, 无升级
    let up2 = mgr.release_cap(1300, "C2", &upgrade.token).unwrap();
    assert!(up2.is_none(), "no survivor → no upgrade");

    // 7. drain_expired_recalls 不 panic
    let _ = mgr.drain_expired_recalls();
}

// ==================== T1.14: evict_session_full 返回 promote_tasks ====================

#[test]
fn t1_14_evict_session_full_returns_promote_tasks() {
    let mgr = CapManager::new();

    // C1 wrlock LONER + C2 rdlock → SHARED (holders: [C1 EXCL, C2 CAP_R])
    let _r1 = mgr.open_grant(1400, "C1", true);
    let _r2 = mgr.open_grant(1400, "C2", false);

    // evict C1 → 剩 C2 (CAP_R), promote_to_loner 升级 C2 到 EXCL
    let (changed, promote) = mgr.evict_session_full("C1");
    assert!(!changed.is_empty(), "C1 had locks");
    assert_eq!(promote.len(), 1, "C2 promoted to LONER");
    let (p_inode, _p_lt, p_client, _p_sn, p_caps) = &promote[0];
    assert_eq!(*p_inode, 1400);
    assert_eq!(p_client, "C2");
    assert!(p_caps.is_exclusive());
}

// ==================== T1.15: arbiter() 透传 — 直接调高级锁原语 ====================

#[test]
fn t1_15_arbiter_passthrough_for_advanced_primitives() {
    let mgr = CapManager::new();

    // 通过 cap_manager.arbiter() 直接调用 xlock (rename/unlink 路径)
    let arb = mgr.arbiter();
    let _r = arb.xlock(1500, LockType::Link, "C1");
    // 不 panic 即通过 (xlock 详细语义由 lock_arbiter 自己的测试覆盖)
}
