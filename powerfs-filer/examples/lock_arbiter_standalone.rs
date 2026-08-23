//! LockArbiter 独立二进制验证
//!
//! 演示 4 套锁状态机 + 锁原语 + GATHER + Loner 升级 + Quiesce + 会话销毁
//! 所有场景不依赖文件系统, 仅基于 lock_arbiter 模块独立运行验证。
//!
//! 运行:
//! ```sh
//! cargo run --example lock_arbiter_standalone
//! ```

use powerfs_filer::cap_manager::CapSet;
use powerfs_filer::lock_arbiter::{LockAcquireResult, LockArbiter, LockType};
use std::sync::Arc;
use std::time::Duration;

fn caps_str(c: CapSet) -> String {
    let mut s = String::new();
    if c.has_r() {
        s.push('R');
    }
    if c.has_w() {
        if !s.is_empty() {
            s.push('|');
        }
        s.push('W');
    }
    if c.has_x() {
        if !s.is_empty() {
            s.push('|');
        }
        s.push('X');
    }
    if s.is_empty() {
        "(none)".to_string()
    } else {
        s
    }
}

fn section(title: &str) {
    println!("\n========== {} ==========", title);
}

fn pass(name: &str) {
    println!("[PASS] {}", name);
}

#[tokio::main]
async fn main() {
    println!("PowerFS LockArbiter 独立验证 (不依赖文件系统)");
    println!("==============================================");

    scenario_1_state_machines().await;
    scenario_2_wrlock_conflict_gather().await;
    scenario_3_gather_timeout().await;
    scenario_4_loner_full_cycle().await;
    scenario_5_quiesce().await;
    scenario_6_evict_client().await;
    scenario_7_async_wait().await;
    scenario_8_dirty_cap().await;
    scenario_9_sn_fencing().await;

    println!("\n==============================================");
    println!("所有场景验证完成 (详见各 [PASS] 标记)");
}

async fn scenario_1_state_machines() {
    section("§1 4 套锁状态机");

    // LocalLock (Snap)
    let a = LockArbiter::new();
    assert!(a.local_lock(1, LockType::Snap));
    assert!(!a.local_lock(1, LockType::Snap));
    a.local_unlock(1, LockType::Snap);
    assert!(a.local_lock(1, LockType::Snap));
    pass("LocalLock AVAILABLE ⇄ LOCK");

    // SimpleLock (Auth)
    let a = LockArbiter::new();
    let r = a.rdlock(2, LockType::Auth, "C1");
    assert!(r.granted_caps.has_r());
    let r2 = a.rdlock(2, LockType::Auth, "C2");
    assert!(r2.granted_caps.has_r());
    pass("SimpleLock AVAILABLE → SHARED (多 reader)");

    // FileLock (File)
    let a = LockArbiter::new();
    let r = a.wrlock(3, LockType::File, "C1");
    assert!(r.granted_caps.is_exclusive());
    pass("FileLock AVAILABLE → LONER (单 writer, 全套 cap)");

    // ScatterLock (Dft)
    let a = LockArbiter::new();
    let r1 = a.scatter_wrlock(4, LockType::Dft, "C1");
    let r2 = a.scatter_wrlock(4, LockType::Dft, "C2");
    assert_eq!(r1.granted_caps, CapSet::NONE);
    assert_eq!(r2.granted_caps, CapSet::NONE);
    pass("ScatterLock AVAILABLE → DSCATTER (多方共享写, 无 cap)");
}

async fn scenario_2_wrlock_conflict_gather() {
    section("§2 wrlock 冲突 → GATHER → recall_ack → SHARED");

    let a = Arc::new(LockArbiter::new());
    let r1 = a.wrlock(100, LockType::File, "C1");
    println!("  C1 wrlock → LONER, caps={}", caps_str(r1.granted_caps));

    let r2 = a.wrlock(100, LockType::File, "C2");
    println!(
        "  C2 wrlock → GATHER, recall_tasks={}",
        r2.recall_tasks.len()
    );
    assert!(!r2.recall_tasks.is_empty());

    let recall = &r2.recall_tasks[0];
    println!(
        "  recall_task: recall={} retain={}",
        caps_str(recall.caps_to_recall),
        caps_str(recall.retained_caps)
    );
    assert!(recall.caps_to_recall.has_w());
    assert!(recall.caps_to_recall.has_x());
    assert!(recall.retained_caps.has_r());

    let done = a.recall_ack(100, LockType::File, "C1", r1.sn);
    assert!(done);
    println!("  C1 recall_ack → GATHER 完成");

    let caps = a.get_eval_issued(100, LockType::File);
    println!("  最终 eval_issued={}", caps_str(caps));
    pass("wrlock 冲突 → GATHER → ACK → SHARED");
}

async fn scenario_3_gather_timeout() {
    section("§3 GATHER 超时 force-reclaim");

    let a = LockArbiter::new_for_test(Duration::from_millis(50), Duration::from_secs(30));
    let _r1 = a.wrlock(200, LockType::File, "C1");
    println!("  C1 wrlock → LONER");

    let r2 = a.wrlock(200, LockType::File, "C2");
    println!("  C2 wrlock → GATHER (recall_timeout=50ms)");
    assert!(!r2.recall_tasks.is_empty());

    println!("  不 ACK, 等 60ms 触发超时...");
    tokio::time::sleep(Duration::from_millis(60)).await;

    let r3 = a.wrlock(200, LockType::File, "C2");
    println!(
        "  C2 重试 wrlock → force-reclaim 后 granted={}",
        caps_str(r3.granted_caps)
    );
    assert!(!r3.granted_caps.is_empty());
    pass("GATHER 超时 → force-reclaim → 授予");
}

async fn scenario_4_loner_full_cycle() {
    section("§4 Loner 完整循环 (打破 → 降级 → 重入 → 升级)");

    let a = Arc::new(LockArbiter::new());
    let r1 = a.wrlock(300, LockType::File, "C1");
    let sn_v1 = r1.sn;
    println!(
        "  1. C1 wrlock → LONER, sn={}, caps={}",
        sn_v1,
        caps_str(r1.granted_caps)
    );

    let rx = match a.wrlock_async(300, LockType::File, "C2") {
        LockAcquireResult::Waiting { recall_tasks: _, rx } => rx,
        LockAcquireResult::Granted(_) => panic!("应 Waiting"),
    };
    println!("  2. C2 wrlock_async → GATHER → Waiting");

    a.recall_ack(300, LockType::File, "C1", sn_v1);
    println!("  3. C1 ACK → C1 降级为 reader (CAP_R)");

    let c2 = tokio::time::timeout(Duration::from_secs(2), rx)
        .await
        .expect("超时")
        .expect("sender drop");
    println!(
        "  4. C2 被唤醒, 加入为 LONER holder, sn={}, caps={}",
        c2.sn,
        caps_str(c2.granted_caps)
    );

    a.unlock(300, LockType::File, c2.sn);
    println!("  5. C2 unlock → C1 升级回 LONER (bump sn)");

    let caps = a.get_eval_issued(300, LockType::File);
    println!("  最终 eval_issued={}", caps_str(caps));
    assert!(caps.has_w() && caps.has_x());
    assert!(!a.sn_valid(300, LockType::File, sn_v1));
    println!("  旧 sn={} 已失效 (bump fencing)", sn_v1);
    pass("Loner 完整循环 (打破→降级→重入→升级)");
}

async fn scenario_5_quiesce() {
    section("§5 Quiesce 静默协议");

    let a = LockArbiter::new();
    let r1 = a.wrlock(400, LockType::File, "C1");
    let r2 = a.rdlock(400, LockType::Auth, "C1");
    println!("  C1 持有 File(wrlock) + Auth(rdlock)");

    let tasks = a.quiesce(400);
    println!("  quiesce → recall_tasks={}", tasks.len());
    assert!(!tasks.is_empty());

    assert!(!a.quiesce_complete(400));
    println!("  quiesce_complete (未 ACK) = false");

    for t in &tasks {
        a.recall_ack(400, t.lock_type, "C1", t.sn);
    }
    println!("  全部 ACK 完成");

    assert!(a.quiesce_complete(400));
    println!("  quiesce_complete = true");
    let _ = (r1, r2);
    pass("Quiesce 协议 (recall + ACK + complete)");
}

async fn scenario_6_evict_client() {
    section("§6 会话销毁 evict_client");

    let a = Arc::new(LockArbiter::new());
    let r1 = a.wrlock(500, LockType::File, "C1");
    println!("  C1 wrlock → LONER, sn={}", r1.sn);

    let rx = match a.wrlock_async(500, LockType::File, "C2") {
        LockAcquireResult::Waiting { recall_tasks: _, rx } => rx,
        LockAcquireResult::Granted(_) => panic!("应 Waiting"),
    };
    a.recall_ack(500, LockType::File, "C1", r1.sn);
    let c2 = tokio::time::timeout(Duration::from_secs(2), rx)
        .await
        .expect("超时")
        .expect("sender drop");
    println!("  C2 加入 holders (sn={})", c2.sn);

    // evict C2 → C1 升级
    let (changed, promotes) = a.evict_client("C2");
    println!(
        "  evict C2: changed={} promotes={}",
        changed.len(),
        promotes.len()
    );
    assert!(!changed.is_empty());
    assert!(!promotes.is_empty());
    let (_, _, client, _new_sn, caps) = &promotes[0];
    println!("  promote: client={} caps={}", client, caps_str(*caps));
    assert_eq!(client, "C1");
    assert!(caps.has_w());

    let caps_c1 = a.get_eval_issued(500, LockType::File);
    println!("  C1 升级后 eval_issued={}", caps_str(caps_c1));
    assert!(caps_c1.has_w());
    pass("evict_client → Loner 升级");
}

async fn scenario_7_async_wait() {
    section("§7 异步等待 (oneshot channel)");

    let a = Arc::new(LockArbiter::new());
    match a.wrlock_async(600, LockType::File, "C1") {
        LockAcquireResult::Granted(r) => {
            println!(
                "  C1 wrlock_async → Granted (无冲突), caps={}",
                caps_str(r.granted_caps)
            );
        }
        _ => panic!("应 Granted"),
    }

    let rx = match a.wrlock_async(600, LockType::File, "C2") {
        LockAcquireResult::Waiting { recall_tasks: _, rx } => rx,
        LockAcquireResult::Granted(_) => panic!("应 Waiting"),
    };
    println!("  C2 wrlock_async → Waiting (注册 oneshot)");

    // 不会自动唤醒, 因为没有 recall_ack 触发 GATHER 完成
    // 演示: 100ms 超时
    let result = tokio::time::timeout(Duration::from_millis(100), rx).await;
    assert!(result.is_err(), "未 ACK 时不应唤醒");
    println!("  C2 等待 100ms 未被唤醒 (符合预期, GATHER 未完成)");

    pass("异步等待机制 (oneshot channel)");
}

async fn scenario_8_dirty_cap() {
    section("§8 dirty cap 管理");

    let a = LockArbiter::new();
    let r = a.wrlock(700, LockType::File, "C1");
    println!("  C1 wrlock → LONER, sn={}", r.sn);

    a.mark_dirty(700, LockType::File, "C1", CapSet::CAP_W);
    println!("  mark_dirty CAP_W");

    let dirty = a.get_dirty_clients(700);
    println!("  dirty_clients: {:?}", dirty);
    assert_eq!(dirty.len(), 1);
    assert_eq!(dirty[0].0, "C1");
    assert!(dirty[0].2.has_w());

    a.flush_dirty(700, LockType::File, "C1");
    let dirty2 = a.get_dirty_clients(700);
    println!("  flush_dirty → dirty={}", dirty2.len());
    assert!(dirty2.is_empty());

    let _ = r;
    pass("dirty cap 追踪 (mark_dirty + flush_dirty)");
}

async fn scenario_9_sn_fencing() {
    section("§9 sn fencing");

    let a = LockArbiter::new();
    let r = a.wrlock(800, LockType::File, "C1");
    println!("  C1 wrlock → sn={}", r.sn);
    assert!(a.sn_valid(800, LockType::File, r.sn));

    a.fence_epoch(800, LockType::File);
    println!("  fence_epoch → force-reclaim all");
    assert!(!a.sn_valid(800, LockType::File, r.sn));
    println!("  sn={} 失效", r.sn);
    pass("sn fencing (epoch bump 失效旧 sn)");
}
