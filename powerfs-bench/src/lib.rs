//! powerfs-bench: Criterion micro-benchmarks for PowerFS lock hot paths.
//!
//! This crate is the **phase-3 performance baseline** described in
//! `docs/lock-optimization-plan.md` §四 (step 7) and §五 ("优化方向 — 性能
//! 基线后决定"). It does NOT ship in the runtime build; its only purpose
//! is to produce reproducible latency / throughput numbers that drive
//! phase-4 optimization prioritization.
//!
//! # Bench targets (each is a separate `[[bench]]` entry)
//!
//! | target           | covers                                                |
//! |------------------|------------------------------------------------------|
//! | `client_cache`   | `ClientLeaseState` get/put/sweep, `FuseLockManager`  |
//! |                  | acquire (cache hit + miss) / release, `LockMetrics`. |
//! | `server_lease`   | `MemoryLeaseStore::acquire` (clean + conflict),      |
//! |                  | `release`, `renew`, `InodeLeaseManager::acquire`.   |
//! | `health_fencer`  | `ClientHealth::check` (Allow/Throttle/Quarantine),   |
//! |                  | `Fencer::register` / `validate`.                    |
//! | `codec`          | `powerfs_lock_net::encode_frame` / `decode_frame`   |
//! |                  | for Acquire / Grant / Release / Revoke.              |
//!
//! # Running
//!
//! ```bash
//! # All benches, short warm-up + sample for quick local check:
//! cargo bench --package powerfs-bench -- --warm-up-time 1 --measurement-time 2
//!
//! # Just one target:
//! cargo bench --package powerfs-bench --bench client_cache
//!
//! # Save a baseline for later comparison:
//! cargo bench --package powerfs-bench -- --save-baseline phase3
//! cargo bench --package powerfs-bench -- --baseline phase3   # compare
//! ```
//!
//! # Notes on noise
//!
//! These benches measure synchronous in-process hot paths only. They do
//! NOT include RPC, disk, or Raft I/O — that overhead is governed by the
//! network stack and the Raft log, which are out of scope for the
//! lock-modularization baseline. Criterion's HTML report (`target/criterion/`)
//! gives per-iteration P50/P95/P99 and outliers; those are the numbers we
//! use to decide which optimization (Early Grant, Lockify, …) goes first.

// Currently no runtime API — the bench harnesses in `benches/` link
// directly to the dependency crates. This module exists so the crate has
// a `[lib]` target (required for workspace integration and so that
// `cargo check --package powerfs-bench` exercises the dep graph).
