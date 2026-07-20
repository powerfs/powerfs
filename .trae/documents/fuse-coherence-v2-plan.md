# FUSE Coherence v2 Missing Features - Implementation Plan (Enterprise Version)

## Repo Research Conclusion

Based on codebase analysis of `powerfs-fuse-enterprise`:

**Already Implemented (Server-side):**

* `master.proto`: `LeaseRenewRequest/Response`, `JOB_COMPLETE` event, `job_id` field ✓

* `directory_tree.rs`: `renew_lease()`, `complete_job()` publishes notification ✓

* `server.rs`: `renew_lease` gRPC handler ✓

* `powerfs-fuse-core/src/client.rs`: `renew_lease()` async + sync methods ✓

**Already Implemented (Enterprise Cache):**

* `cache.rs`: `clear_all()`, `path_generations`, `update_path_generation()`, `get_path_generation()` ✓

**Missing Implementation (Enterprise FUSE):**

1. **Feature 1**: Lease auto-renewal loop and tracking (`fuser_fs.rs`)
2. **Feature 2**: Handle `JOB_COMPLETE` notifications for batch invalidation (`fuser_fs.rs`)
3. **Feature 3**: Generation validation in lookup (`fuser_fs.rs`)
4. **Feature 4**: Job-level lease sharing with `job_id` filtering (`fuser_fs.rs`, `fuse.rs`)

## Files to Edit

| File                                              | Changes                                     |
| ------------------------------------------------- | ------------------------------------------- |
| `powerfs-fuse-enterprise/src/fuser_fs.rs`         | Main implementation file for all 4 features |
| `powerfs-fuse-enterprise/src/fuse.rs`             | Add `job_id` environment variable support   |
| `powerfs-fuse-enterprise/src/cache.rs`            | Additional tests                            |
| `powerfs-master/tests/coherence_phase3_test.rs`   | Job completion and job\_id tests            |
| `powerfs-master/tests/coherence_failover_test.rs` | Lease renewal tests                         |

## Implementation Steps

### Step 1: Feature 2 - Job Completion Batch Invalidation (P1)

**File:** **`powerfs-fuse-enterprise/src/fuser_fs.rs`**

1. Modify the metadata subscription loop (around line 1543) to handle `JOB_COMPLETE` event:

   * When `notification.event_type == 4` (JOB\_COMPLETE), call `meta.try_invalidate_local_cache_entry("/")` or similar batch invalidation

   * The `cache.rs` already has `clear_all()` implemented

### Step 2: Feature 3 - Lookup Generation Validation (P2)

**File:** **`powerfs-fuse-enterprise/src/fuser_fs.rs`**

1. In the metadata subscription loop, add `cache.update_path_generation(&notification.path, notification.generation)` at the beginning of each notification processing

2. In the `lookup()` method, when cache hit occurs:

   * Get the cached entry's generation

   * Get the path generation from `cache.get_path_generation(&lookup_path)`

   * If path generation > cached generation, skip cache and fetch from master

### Step 3: Feature 4 - Job-level Lease Sharing (P2)

**File:** **`powerfs-fuse-enterprise/src/fuser_fs.rs`**

1. Add `job_id: String` field to `PowerFsFuserFs` struct

2. Modify `new()` to accept `job_id` parameter

3. Modify metadata subscription loop to filter notifications:

   * If notification has `job_id` and it matches client's `job_id`, don't skip invalidation

   * This allows job-level lease sharing

**File:** **`powerfs-fuse-enterprise/src/fuse.rs`**

1. Add `job_id` to `FuseApp` struct
2. Read `job_id` from environment variable `POWERFS_JOB_ID` in `new()`
3. Pass `job_id` to `PowerFsFs` constructor

### Step 4: Feature 1 - Lease Auto-renewal (P0)

**File:** **`powerfs-fuse-enterprise/src/fuser_fs.rs`**

1. Add `LeaseInfo` struct:

   ```rust
   struct LeaseInfo {
       lease_id: String,
       path: String,
       duration_ms: u64,
       acquired_at: std::time::Instant,
   }
   ```

2. Add `leases: Arc<RwLock<HashMap<u64, Vec<LeaseInfo>>>>` field to `PowerFsFuserFs`

3. Modify `open()` to:

   * Store `LeaseInfo` instead of just lease\_id

   * Change duration from 300000ms to 30000ms (30 seconds)

4. Modify `release()` to:

   * Pop `LeaseInfo` from leases map

   * Release lease using lease\_id

5. Add `lease_renewal_loop()` async function:

   * Runs every 5 seconds

   * Checks all leases; renews when remaining time < 1/3 of duration

6. Spawn `lease_renewal_loop()` in `FuserApp::run()`

## Potential Dependencies & Considerations

1. **Thread Safety**: All modifications must maintain proper locking with `RwLock`
2. **Deadlock Avoidance**: Use non-blocking operations in notification handlers
3. **Performance**: Generation validation adds minimal overhead to lookup path
4. **Backward Compatibility**: Empty `job_id` means no job-level sharing (default behavior)
5. **OR-Set Architecture**: Enterprise uses OR-Set weak consistency, so cache invalidation must work with OR-Set semantics

## Risk Handling

1. **Lease Renewal Failure**: Implement retry logic with exponential backoff
2. **Cache Clear Race**: Ensure `clear_all()` is thread-safe (already uses RwLock)
3. **Notification Loss**: Metadata subscription loop automatically reconnects on failure
4. **Job ID Mismatch**: Log warnings when job\_id filtering causes skipped invalidations
5. **OR-Set Conflict**: Ensure cache invalidation doesn't interfere with OR-Set conflict resolution

## Verification Plan

```bash
# 1. Compilation
cargo check -p powerfs-fuse-enterprise

# 2. Format and clippy
cargo fmt --all
cargo clippy -p powerfs-fuse-enterprise --tests -- -D warnings

# 3. Run coherence tests
cargo test --package powerfs-fuse-enterprise --test coherence_phase0_test --test coherence_phase1_test
cargo test --package powerfs-master --test coherence_phase2_test --test coherence_phase3_test --test coherence_failover_test

# 4. Full test suite
cargo test --all
```

## Expected Result

* Existing 71 tests pass

* New tests added for each feature

* Total: \~86 tests passing

