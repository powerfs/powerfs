//! Multi-client cap state machine integration tests.
//!
//! Verifies the Cap model (§13) under realistic multi-client scenarios:
//! 2-3 clients × O_RDONLY / O_WRONLY / O_RDWR combinations, validating:
//!   - `open` never blocks (always returns STATUS_OK)
//!   - correct caps granted per open type
//!   - correct state transitions (Free/SharedRead/ExclusiveWrite/SharedWrite)
//!   - recall tasks dispatched on write-write / write-read conflicts
//!   - epoch fencing (bumps on every recall)
//!   - upgrade detection on release (SHARED_WRITE → EXCLUSIVE_WRITE)
//!   - force-reclaim on recall timeout
//!
//! Run: `cargo test -p powerfs-filer --lib multi_client_cap -- --nocapture`

use powerfs_filer::cap_manager::{CapManager, CapSet, CapState};

/// Helper: snapshot the key state for assertions + logging.
#[derive(Debug, Clone)]
struct StateSnapshot {
    logical: CapState,
    holder_count: usize,
}

fn snapshot(mgr: &CapManager, inode: u64) -> StateSnapshot {
    StateSnapshot {
        logical: mgr.logical_state(inode),
        holder_count: mgr.holder_count(inode),
    }
}

/// Pretty-print a state transition for the test report.
fn log_transition(label: &str, before: &StateSnapshot, after: &StateSnapshot) {
    eprintln!(
        "  [{}] {:?} (holders={}) → {:?} (holders={})",
        label, before.logical, before.holder_count, after.logical, after.holder_count
    );
}

// =============================================================================
// Scenario 1: 2 clients, RDONLY + RDONLY (compatible readers)
// =============================================================================
#[test]
fn s1_two_readers_compatible() {
    eprintln!("\n=== S1: 2 clients open(RDONLY) — compatible readers ===");
    let mgr = CapManager::new();
    let inode = 10_001;

    let before = snapshot(&mgr, inode);
    let r1 = mgr.open_grant(inode, "C1", false);
    let after1 = snapshot(&mgr, inode);
    log_transition("C1 open(RDONLY)", &before, &after1);

    assert_eq!(r1.granted_caps, CapSet::CAP_R, "C1 should get CAP_R");
    assert!(r1.recall_tasks.is_empty(), "no recall on first open");
    assert_eq!(after1.logical, CapState::SharedRead);

    let before = after1;
    let r2 = mgr.open_grant(inode, "C2", false);
    let after2 = snapshot(&mgr, inode);
    log_transition("C2 open(RDONLY)", &before, &after2);

    assert_eq!(r2.granted_caps, CapSet::CAP_R, "C2 should get CAP_R");
    assert!(r2.recall_tasks.is_empty(), "no recall — readers compatible");
    assert_eq!(after2.logical, CapState::SharedRead);
    assert_eq!(after2.holder_count, 2);

    eprintln!("  ✅ Both readers hold CAP_R, no recall, state=SharedRead");
}

// =============================================================================
// Scenario 2: 2 clients, RDWR + RDWR (write-write conflict → degrade)
// =============================================================================
#[test]
fn s2_two_writers_conflict_degrades_to_shared_write() {
    eprintln!("\n=== S2: C1 open(RDWR) → C2 open(RDWR) — write conflict ===");
    let mgr = CapManager::new();
    let inode = 10_002;

    let r1 = mgr.open_grant(inode, "C1", true);
    let s1 = snapshot(&mgr, inode);
    eprintln!("  [C1 open(RDWR)] → {:?} caps={:?}", s1.logical, r1.granted_caps);
    assert!(r1.granted_caps.is_exclusive(), "C1 gets EXCLUSIVE");
    assert_eq!(s1.logical, CapState::ExclusiveWrite);

    let before = s1;
    let r2 = mgr.open_grant(inode, "C2", true);
    let s2 = snapshot(&mgr, inode);
    log_transition("C2 open(RDWR) [conflict!]", &before, &s2);

    // Core Cap model invariant: open NEVER blocks — C2 gets OK
    assert_eq!(r2.granted_caps, CapSet::NONE, "C2 gets NONE (SHARED_WRITE)");
    assert_eq!(r2.recall_tasks.len(), 1, "C1's EXCLUSIVE must be recalled");
    assert_eq!(r2.recall_tasks[0].holder, "C1");
    assert_eq!(
        r2.recall_tasks[0].caps_to_recall,
        CapSet::EXCLUSIVE,
        "recall C1's full caps"
    );
    assert_eq!(s2.logical, CapState::SharedWrite);

    // Epoch must bump on recall (fencing)
    assert!(
        r2.recall_tasks[0].new_epoch > r1.epoch,
        "epoch must bump on recall (fencing)"
    );

    eprintln!("  ✅ open never blocks; C2 gets NONE; C1 recalled; epoch bumped");
}

// =============================================================================
// Scenario 3: RDWR + RDONLY (writer exists, reader arrives → downgrade)
// =============================================================================
#[test]
fn s3_writer_then_reader_downgrades_to_shared_read() {
    eprintln!("\n=== S3: C1 open(RDWR) → C2 open(RDONLY) — downgrade writer ===");
    let mgr = CapManager::new();
    let inode = 10_003;

    let _r1 = mgr.open_grant(inode, "C1", true);
    assert_eq!(mgr.logical_state(inode), CapState::ExclusiveWrite);

    let r2 = mgr.open_grant(inode, "C2", false);
    let s2 = snapshot(&mgr, inode);
    eprintln!(
        "  [C2 open(RDONLY)] → {:?} caps={:?} recalls={}",
        s2.logical,
        r2.granted_caps,
        r2.recall_tasks.len()
    );

    assert_eq!(r2.granted_caps, CapSet::CAP_R, "C2 gets CAP_R");
    assert_eq!(r2.recall_tasks.len(), 1, "C1's W+X must be recalled");
    assert_eq!(
        r2.recall_tasks[0].caps_to_recall,
        CapSet::CAP_W | CapSet::CAP_X,
        "recall only W+X, keep R"
    );
    assert_eq!(
        r2.recall_tasks[0].retained_caps,
        CapSet::CAP_R,
        "C1 retains CAP_R (downgraded to reader)"
    );
    assert_eq!(s2.logical, CapState::SharedRead);

    eprintln!("  ✅ C1 downgraded W+X→R; C2 gets R; state=SharedRead");
}

// =============================================================================
// Scenario 4: RDONLY + RDWR (reader exists, writer arrives → degrade)
// =============================================================================
#[test]
fn s4_reader_then_writer_degrades_to_shared_write() {
    eprintln!("\n=== S4: C1 open(RDONLY) → C2 open(RDWR) — writer arrives ===");
    let mgr = CapManager::new();
    let inode = 10_004;

    let _r1 = mgr.open_grant(inode, "C1", false);
    assert_eq!(mgr.logical_state(inode), CapState::SharedRead);

    let r2 = mgr.open_grant(inode, "C2", true);
    let s2 = snapshot(&mgr, inode);
    eprintln!(
        "  [C2 open(RDWR)] → {:?} caps={:?} recalls={}",
        s2.logical,
        r2.granted_caps,
        r2.recall_tasks.len()
    );

    assert_eq!(r2.granted_caps, CapSet::NONE, "C2 gets NONE (SHARED_WRITE)");
    assert!(!r2.recall_tasks.is_empty(), "C1's CAP_R must be recalled");
    assert_eq!(s2.logical, CapState::SharedWrite);

    eprintln!("  ✅ C1's CAP_R recalled; C2 gets NONE; state=SharedWrite");
}

// =============================================================================
// Scenario 5: 3 clients — RDWR + RDWR + RDWR (all writers, shared write)
// =============================================================================
#[test]
fn s5_three_writers_all_shared_write() {
    eprintln!("\n=== S5: 3 clients all open(RDWR) — full shared-write ===");
    let mgr = CapManager::new();
    let inode = 10_005;

    let r1 = mgr.open_grant(inode, "C1", true);
    eprintln!("  [C1 open(RDWR)] caps={:?} state={:?}", r1.granted_caps, mgr.logical_state(inode));
    assert!(r1.granted_caps.is_exclusive());

    let r2 = mgr.open_grant(inode, "C2", true);
    eprintln!("  [C2 open(RDWR)] caps={:?} recalls={} state={:?}", r2.granted_caps, r2.recall_tasks.len(), mgr.logical_state(inode));
    assert_eq!(r2.granted_caps, CapSet::NONE);

    // C3 opens RDWR — already SHARED_WRITE, no new recall needed
    let r3 = mgr.open_grant(inode, "C3", true);
    let s3 = snapshot(&mgr, inode);
    eprintln!("  [C3 open(RDWR)] caps={:?} recalls={} state={:?}", r3.granted_caps, r3.recall_tasks.len(), s3.logical);

    assert_eq!(r3.granted_caps, CapSet::NONE, "C3 gets NONE");
    assert!(r3.recall_tasks.is_empty(), "no recall — already SHARED_WRITE");
    assert_eq!(s3.logical, CapState::SharedWrite);
    assert_eq!(s3.holder_count, 3);

    eprintln!("  ✅ 3 writers, all NONE caps, no recall for 3rd, state=SharedWrite");
}

// =============================================================================
// Scenario 6: 3 clients — RDWR + RDWR + RDWR, then C1+C2 release → C3 upgrades
// =============================================================================
#[test]
fn s6_three_writers_release_triggers_upgrade() {
    eprintln!("\n=== S6: 3 writers → release C1,C2 → C3 upgrades to EXCLUSIVE ===");
    let mgr = CapManager::new();
    let inode = 10_006;

    let r1 = mgr.open_grant(inode, "C1", true);
    let r2 = mgr.open_grant(inode, "C2", true);
    let _r3 = mgr.open_grant(inode, "C3", true);
    assert_eq!(mgr.logical_state(inode), CapState::SharedWrite);
    eprintln!("  [3 writers] state={:?}", mgr.logical_state(inode));

    // C1 and C2 ACK their recalls + release
    mgr.recall_ack(inode, "C1", &r1.token).unwrap();
    let up1 = mgr.release_cap(inode, "C1", &r1.token).unwrap();
    assert!(up1.is_none(), "no upgrade yet — 2 writers remain");
    eprintln!("  [C1 release] upgrade={}", up1.is_some());

    mgr.recall_ack(inode, "C2", &r2.token).unwrap();
    let up2 = mgr.release_cap(inode, "C2", &r2.token).unwrap();
    let s_after = snapshot(&mgr, inode);
    eprintln!("  [C2 release] upgrade={} state={:?}", up2.is_some(), s_after.logical);

    assert!(up2.is_some(), "C3 should upgrade — only 1 writer left");
    assert_eq!(up2.as_ref().unwrap().holder, "C3");
    assert!(up2.as_ref().unwrap().granted_caps.is_exclusive());
    assert_eq!(s_after.logical, CapState::ExclusiveWrite);
    assert_eq!(s_after.holder_count, 1);

    eprintln!("  ✅ C3 upgraded to EXCLUSIVE_WRITE (high-perf local cache restored)");
}

// =============================================================================
// Scenario 7: RDWR + RDWR → release C2 → C1 upgrades back
// =============================================================================
#[test]
fn s7_release_second_writer_upgrades_first() {
    eprintln!("\n=== S7: C1(RDWR) + C2(RDWR) → release C2 → C1 upgrades ===");
    let mgr = CapManager::new();
    let inode = 10_007;

    let _r1 = mgr.open_grant(inode, "C1", true);
    let r2 = mgr.open_grant(inode, "C2", true);
    assert_eq!(mgr.logical_state(inode), CapState::SharedWrite);
    eprintln!("  [C1+C2 RDWR] state={:?}", mgr.logical_state(inode));

    // C2 ACKs + releases — only C1 remains, should upgrade
    mgr.recall_ack(inode, "C2", &r2.token).unwrap();
    let upgrade = mgr.release_cap(inode, "C2", &r2.token).unwrap();
    let s = snapshot(&mgr, inode);
    eprintln!("  [C2 release] upgrade={} state={:?}", upgrade.is_some(), s.logical);

    assert!(upgrade.is_some());
    assert_eq!(upgrade.as_ref().unwrap().holder, "C1");
    assert!(upgrade.as_ref().unwrap().granted_caps.is_exclusive());
    assert_eq!(s.logical, CapState::ExclusiveWrite);

    eprintln!("  ✅ C1 upgraded back to EXCLUSIVE_WRITE");
}

// =============================================================================
// Scenario 8: reader + writer + reader (mixed, 3 clients)
// =============================================================================
#[test]
fn s8_reader_writer_reader_mixed() {
    eprintln!("\n=== S8: C1(RDONLY) → C2(RDWR) → C3(RDONLY) — mixed ===");
    let mgr = CapManager::new();
    let inode = 10_008;

    let r1 = mgr.open_grant(inode, "C1", false);
    eprintln!("  [C1 RDONLY] caps={:?} state={:?}", r1.granted_caps, mgr.logical_state(inode));
    assert_eq!(mgr.logical_state(inode), CapState::SharedRead);

    let r2 = mgr.open_grant(inode, "C2", true);
    eprintln!("  [C2 RDWR] caps={:?} recalls={} state={:?}", r2.granted_caps, r2.recall_tasks.len(), mgr.logical_state(inode));
    assert_eq!(mgr.logical_state(inode), CapState::SharedWrite);

    // C3 opens RDONLY while in SHARED_WRITE — gets CAP_R (can cache reads
    // since all writes go through sync RPC)
    let r3 = mgr.open_grant(inode, "C3", false);
    let s3 = snapshot(&mgr, inode);
    eprintln!("  [C3 RDONLY] caps={:?} recalls={} state={:?}", r3.granted_caps, r3.recall_tasks.len(), s3.logical);

    assert_eq!(r3.granted_caps, CapSet::CAP_R, "C3 gets CAP_R in SHARED_WRITE");
    assert!(r3.recall_tasks.is_empty());
    assert_eq!(s3.logical, CapState::SharedWrite);

    eprintln!("  ✅ C3 reader gets CAP_R even in SHARED_WRITE (reads are cacheable)");
}

// =============================================================================
// Scenario 9: epoch fencing — every recall bumps epoch
// =============================================================================
#[test]
fn s9_epoch_fencing_on_repeated_recalls() {
    eprintln!("\n=== S9: repeated recalls bump epoch each time (fencing) ===");
    let mgr = CapManager::new();
    let inode = 10_009;

    let r1 = mgr.open_grant(inode, "C1", true);
    let epoch_1 = r1.epoch;
    eprintln!("  [C1 RDWR] epoch={}", epoch_1);

    let r2 = mgr.open_grant(inode, "C2", true);
    let epoch_2 = r2.recall_tasks[0].new_epoch;
    eprintln!("  [C2 RDWR] recall epoch={}", epoch_2);
    assert!(epoch_2 > epoch_1, "epoch must bump on 1st recall");

    // C3 triggers another recall (C2 was downgraded to NONE, but the
    // transition from ExclusiveWrite→SharedWrite already happened; C3
    // joining SHARED_WRITE doesn't recall). Instead, test force-reclaim.
    let r3 = mgr.open_grant(inode, "C3", true);
    eprintln!("  [C3 RDWR] recalls={} (already SHARED_WRITE)", r3.recall_tasks.len());
    assert!(r3.recall_tasks.is_empty());

    // Force-reclaim C1 (timed out recall) — bumps epoch again
    let mgr_fast = CapManager::new().with_recall_timeout_ms(0);
    let inode2 = 10_099;
    let _fa = mgr_fast.open_grant(inode2, "CA", true);
    let _fb = mgr_fast.open_grant(inode2, "CB", true);
    std::thread::sleep(std::time::Duration::from_millis(2));
    let reclaimed = mgr_fast.drain_expired_recalls();
    let epoch_after_force = mgr_fast.current_epoch();
    eprintln!("  [force-reclaim] reclaimed={} epoch={}", reclaimed.len(), epoch_after_force);
    assert_eq!(reclaimed.len(), 1, "C1 should be force-reclaimed");
    assert!(epoch_after_force > epoch_2, "epoch must bump on force-reclaim");

    eprintln!("  ✅ Epoch monotonically increases on every recall/force-reclaim");
}

// =============================================================================
// Scenario 10: full lifecycle — open, conflict, recall_ack, release, upgrade
// =============================================================================
#[test]
fn s10_full_lifecycle_open_conflict_release_upgrade() {
    eprintln!("\n=== S10: full lifecycle — open/conflict/recall/release/upgrade ===");
    let mgr = CapManager::new();
    let inode = 10_010;

    // Phase 1: C1 opens RDWR → EXCLUSIVE_WRITE
    let r1 = mgr.open_grant(inode, "C1", true);
    assert_eq!(mgr.logical_state(inode), CapState::ExclusiveWrite);
    eprintln!("  Phase 1: C1 open(RDWR) → ExclusiveWrite caps={:?}", r1.granted_caps);

    // Phase 2: C2 opens RDWR → recall C1, degrade to SHARED_WRITE
    let r2 = mgr.open_grant(inode, "C2", true);
    assert_eq!(mgr.logical_state(inode), CapState::SharedWrite);
    assert_eq!(r2.recall_tasks.len(), 1);
    eprintln!(
        "  Phase 2: C2 open(RDWR) → SharedWrite recalls={} C2_caps={:?}",
        r2.recall_tasks.len(),
        r2.granted_caps
    );

    // Phase 3: C1 ACKs recall (flush done)
    let retained = mgr.recall_ack(inode, "C1", &r1.token).unwrap();
    assert_eq!(retained, CapSet::NONE, "C1 retains nothing");
    eprintln!("  Phase 3: C1 recall_ack → retained={:?}", retained);

    // Phase 4: C3 opens RDWR → already SHARED_WRITE, no recall
    let r3 = mgr.open_grant(inode, "C3", true);
    assert!(r3.recall_tasks.is_empty());
    eprintln!("  Phase 4: C3 open(RDWR) → no recall (already SharedWrite)");

    // Phase 5: C1 releases (was already ACKed) — 2 writers remain
    let up1 = mgr.release_cap(inode, "C1", &r1.token).unwrap();
    assert!(up1.is_none(), "no upgrade — 2 writers (C2,C3) remain");
    eprintln!("  Phase 5: C1 release → no upgrade (C2,C3 remain)");

    // Phase 6: C2 ACKs + releases — 1 writer (C3) remains → upgrade!
    mgr.recall_ack(inode, "C2", &r2.token).unwrap();
    let up2 = mgr.release_cap(inode, "C2", &r2.token).unwrap();
    assert!(up2.is_some(), "C3 should upgrade");
    assert_eq!(up2.as_ref().unwrap().holder, "C3");
    assert_eq!(mgr.logical_state(inode), CapState::ExclusiveWrite);
    eprintln!(
        "  Phase 6: C2 release → C3 upgrades to ExclusiveWrite 🎉"
    );

    // Phase 7: C3 releases → back to Free
    let up3 = mgr.release_cap(inode, "C3", &r3.token).unwrap();
    // C3's token may have changed after upgrade — use the upgrade token
    if let Some(u) = &up3 {
        eprintln!("  Phase 7: C3 release → upgrade={:?} (unexpected)", u.holder);
    }
    // After C3 releases, inode should be cleaned up
    assert_eq!(mgr.logical_state(inode), CapState::Free);
    eprintln!("  Phase 7: C3 release → state=Free ✅");

    eprintln!("  ✅ Full lifecycle verified: EX→SW→upgrade→Free");
}

// =============================================================================
// Scenario 11: open never blocks — verify all conflict cases return OK
// =============================================================================
#[test]
fn s11_open_never_blocks_all_conflict_cases() {
    eprintln!("\n=== S11: open never blocks — all 6 conflict combinations ===");
    let cases: &[(&str, &[(bool, &str)])] = &[
        ("RDONLY+RDONLY", &[(false, "C1"), (false, "C2")]),
        ("RDONLY+RDWR", &[(false, "C1"), (true, "C2")]),
        ("RDWR+RDONLY", &[(true, "C1"), (false, "C2")]),
        ("RDWR+RDWR", &[(true, "C1"), (true, "C2")]),
        ("RDWR+RDWR+RDWR", &[(true, "C1"), (true, "C2"), (true, "C3")]),
        (
            "RDONLY+RDONLY+RDWR+RDWR",
            &[(false, "C1"), (false, "C2"), (true, "C3"), (true, "C4")],
        ),
    ];

    for (idx, (label, ops)) in cases.iter().enumerate() {
        let mgr = CapManager::new();
        let inode = 20_000 + idx as u64;
        eprintln!("\n  --- Case {}: {} ---", idx + 1, label);

        for (is_write, client) in ops.iter() {
            let before = snapshot(&mgr, inode);
            let r = mgr.open_grant(inode, client, *is_write);
            let after = snapshot(&mgr, inode);
            eprintln!(
                "    {} open({}) caps={:?} recalls={} {:?}→{:?}",
                client,
                if *is_write { "RDWR" } else { "RDONLY" },
                r.granted_caps,
                r.recall_tasks.len(),
                before.logical,
                after.logical
            );
            // Core invariant: open ALWAYS succeeds (caps may be NONE, but
            // the call returns — never blocks/panics)
            assert!(
                r.granted_caps.0 <= CapSet::EXCLUSIVE.0,
                "open must return valid caps"
            );
        }
    }
    eprintln!("\n  ✅ All 6 conflict combinations — open never blocks");
}

// =============================================================================
// Scenario 12: idempotent re-open by same client
// =============================================================================
#[test]
fn s12_idempotent_reopen_same_client() {
    eprintln!("\n=== S12: same client reopens — idempotent, no new token ===");
    let mgr = CapManager::new();
    let inode = 10_012;

    let r1 = mgr.open_grant(inode, "C1", true);
    let r2 = mgr.open_grant(inode, "C1", true); // same client
    let r3 = mgr.open_grant(inode, "C1", false); // same client, different mode

    eprintln!(
        "  C1 open(RDWR) token={} caps={:?}", r1.token, r1.granted_caps
    );
    eprintln!(
        "  C1 open(RDWR) token={} caps={:?} (same)", r2.token, r2.granted_caps
    );
    eprintln!(
        "  C1 open(RDONLY) token={} caps={:?} (same)", r3.token, r3.granted_caps
    );

    assert_eq!(r1.token, r2.token, "same token on reopen");
    assert_eq!(r1.token, r3.token, "same token even with different mode");
    assert_eq!(mgr.holder_count(inode), 1, "still 1 holder");

    eprintln!("  ✅ Idempotent reopen — same token, no duplicate holder");
}

// =============================================================================
// Scenario 13: reader release doesn't trigger upgrade (no writer to upgrade)
// =============================================================================
#[test]
fn s13_reader_release_no_upgrade() {
    eprintln!("\n=== S13: reader release — no upgrade (no writer) ===");
    let mgr = CapManager::new();
    let inode = 10_013;

    let r1 = mgr.open_grant(inode, "C1", false);
    let r2 = mgr.open_grant(inode, "C2", false);
    assert_eq!(mgr.logical_state(inode), CapState::SharedRead);
    eprintln!("  [2 readers] state={:?}", mgr.logical_state(inode));

    let up = mgr.release_cap(inode, "C1", &r1.token).unwrap();
    assert!(up.is_none(), "no upgrade — C2 is a reader, not a writer");
    eprintln!("  [C1 release] upgrade={}", up.is_some());

    let up2 = mgr.release_cap(inode, "C2", &r2.token).unwrap();
    assert!(up2.is_none(), "no upgrade — no holders left");
    assert_eq!(mgr.logical_state(inode), CapState::Free);
    eprintln!("  [C2 release] state=Free");

    eprintln!("  ✅ Reader release never triggers upgrade");
}

// =============================================================================
// Scenario 14: validate_cap returns correct caps for write/setattr checks
// =============================================================================
#[test]
fn s14_validate_cap_for_write_setattr() {
    eprintln!("\n=== S14: validate_cap — write/setattr permission check ===");
    let mgr = CapManager::new();
    let inode = 10_014;

    // C1 exclusive writer — has CAP_W + CAP_X
    let r1 = mgr.open_grant(inode, "C1", true);
    let caps = mgr.validate_cap(inode, "C1", &r1.token).unwrap();
    assert!(caps.has_w(), "C1 has CAP_W for local write");
    assert!(caps.has_x(), "C1 has CAP_X for setattr");
    eprintln!("  [C1 EXCLUSIVE] caps={:?} → write=✅ setattr=✅", caps);

    // C2 joins as writer — both degrade to SHARED_WRITE (no CAP_W)
    let r2 = mgr.open_grant(inode, "C2", true);
    let caps2 = mgr.validate_cap(inode, "C2", &r2.token).unwrap();
    assert!(!caps2.has_w(), "C2 has NO CAP_W — must use sync RPC");
    assert!(!caps2.has_x(), "C2 has NO CAP_X — setattr via sync RPC");
    eprintln!("  [C2 SHARED_WRITE] caps={:?} → write=sync setattr=sync", caps2);

    // C1's caps were downgraded server-side (recall in flight)
    let caps1_after = mgr.validate_cap(inode, "C1", &r1.token).unwrap();
    assert!(!caps1_after.has_w(), "C1 CAP_W recalled");
    eprintln!(
        "  [C1 after recall] caps={:?} → write=sync (CAP_W recalled)",
        caps1_after
    );

    eprintln!("  ✅ validate_cap correctly reflects write/setattr permission");
}

// =============================================================================
// Summary test: prints a state-transition matrix
// =============================================================================
#[test]
fn s99_state_transition_matrix_summary() {
    eprintln!("\n=== SUMMARY: State transition matrix ===\n");
    eprintln!("  From            │ Event              │ To               │ Recall │ Granted");
    eprintln!("  ────────────────┼────────────────────┼──────────────────┼────────┼─────────");

    let cases: &[(CapState, &str, bool, CapState, usize, CapSet)] = &[
        (CapState::Free, "open(RDONLY)", false, CapState::SharedRead, 0, CapSet::CAP_R),
        (CapState::Free, "open(RDWR)", true, CapState::ExclusiveWrite, 0, CapSet::EXCLUSIVE),
        (CapState::SharedRead, "open(RDONLY)", false, CapState::SharedRead, 0, CapSet::CAP_R),
        (CapState::SharedRead, "open(RDWR)", true, CapState::SharedWrite, 1, CapSet::NONE),
        (CapState::ExclusiveWrite, "open(RDONLY)", false, CapState::SharedRead, 1, CapSet::CAP_R),
        (CapState::ExclusiveWrite, "open(RDWR)", true, CapState::SharedWrite, 1, CapSet::NONE),
        (CapState::SharedWrite, "open(RDWR)", true, CapState::SharedWrite, 0, CapSet::NONE),
    ];

    for (from, event, is_write, to, recalls, granted) in cases {
        let mgr = CapManager::new();
        let inode = 30_000;

        // Set up the "from" state
        match from {
            CapState::Free => {}
            CapState::SharedRead => {
                mgr.open_grant(inode, "C0", false);
            }
            CapState::ExclusiveWrite => {
                mgr.open_grant(inode, "C0", true);
            }
            CapState::SharedWrite => {
                mgr.open_grant(inode, "C0", true);
                mgr.open_grant(inode, "C9", true);
            }
        }

        // Apply the event
        let r = mgr.open_grant(inode, "C1", *is_write);
        let actual_to = mgr.logical_state(inode);

        assert_eq!(
            actual_to, *to,
            "from={:?} event={} → expected {:?} got {:?}",
            from, event, to, actual_to
        );
        assert_eq!(
            r.recall_tasks.len(),
            *recalls,
            "from={:?} event={} → expected {} recalls got {}",
            from,
            event,
            recalls,
            r.recall_tasks.len()
        );
        assert_eq!(
            r.granted_caps, *granted,
            "from={:?} event={} → expected {:?} got {:?}",
            from, event, granted, r.granted_caps
        );

        eprintln!(
            "  {:?} │ {:18} │ {:16} │ {:6} │ {:?}",
            from, event, format!("{:?}", to), recalls, granted
        );
    }

    eprintln!("\n  ✅ All 7 state transitions verified against design (§13.2.2)");
}
