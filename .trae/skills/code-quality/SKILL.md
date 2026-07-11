---
name: "code-quality"
description: "Performs comprehensive Rust code quality checks including cargo check, fmt, clippy, tests, and build. Invoke after code changes, before commits, or to ensure GitHub Actions pass."
---

# Code Quality Check

This skill performs comprehensive Rust code quality checks to ensure the codebase meets production standards and passes GitHub Actions CI.

## GitHub Actions Requirements (Hard Gate)

The CI workflow (`.github/workflows/rust.yml`) enforces strict quality gates. **All checks must pass with ZERO errors and ZERO warnings.** A single warning or formatting diff will fail the entire CI pipeline and block PR merging.

### CI Quality Gates (in execution order)

| Step | CI Command | Requirement |
|------|-----------|-------------|
| 1. Formatting | `cargo fmt --all -- --check` | No formatting diffs |
| 2. Linting | `cargo clippy --all -- -D warnings` | Zero warnings (warnings treated as errors) |
| 3. Build | `cargo build --all --verbose` | Compiles without errors or warnings |
| 4. All tests | `cargo test --all --verbose` | All tests pass (0 failures) |
| 5. Coherence tests | Specific test packages (see below) | All coherence/failover tests pass |

### Coherence Tests (CI Step 5)

```bash
cargo test --package powerfs-fuse --test coherence_phase0_test --verbose
cargo test --package powerfs-fuse --test coherence_phase1_test --verbose
cargo test --package powerfs-master --test coherence_phase2_test --verbose
cargo test --package powerfs-master --test coherence_phase3_test --verbose
cargo test --package powerfs-master --test coherence_failover_test --verbose
cargo test --package powerfs-master --test master_outage_e2e_test --verbose -- --test-threads=1
```

## Local Verification Commands

Run all checks sequentially to match CI:

```bash
# 1. Format check (use exact CI command)
cargo fmt --all -- --check

# 2. Clippy (warnings as errors, no --tests flag to match CI)
cargo clippy --all -- -D warnings

# 3. Build (required before tests for integration tests that spawn binaries)
cargo build --all

# 4. Run all tests
cargo test --all

# 5. Run coherence tests
cargo test --package powerfs-master --test coherence_phase2_test
cargo test --package powerfs-master --test coherence_failover_test
cargo test --package powerfs-master --test master_outage_e2e_test -- --test-threads=1
```

For stricter local checks (includes test code in clippy):

```bash
cargo clippy --all --tests -- -D warnings
```

## Error Handling

1. **Formatting issues**: Run `cargo fmt --all` to auto-fix. Note: generated protobuf code (`**/volume_proto/*.rs`) is also checked by fmt — run `cargo fmt --all` after regenerating protobuf files.

2. **Clippy warnings**: Fix manually. For functions with >7 parameters, add `#[allow(clippy::too_many_arguments)]`. Do NOT use broad `#![allow]` at crate level.

3. **Test compilation errors**: Common after API signature changes. When adding parameters to public methods, update ALL call sites including test files:
   - `powerfs-master/tests/coherence_phase2_test.rs`
   - `powerfs-master/tests/coherence_failover_test.rs`
   - `powerfs-master/tests/filer_api_test.rs`
   - `powerfs-master/tests/master_outage_e2e_test.rs`

4. **Integration test failures** (`No such file or directory`): Run `cargo build --all` first. Integration tests in `powerfs-fuse/tests/` spawn real binaries (`target/debug/powerfs`) and require them to exist.

5. **Build errors**: Fix compilation issues. Check that all API call sites match current signatures.

## When to Invoke

- After making code changes
- Before committing changes
- Before pushing to remote
- To verify code quality before PR submission
- When GitHub Actions fails
- After modifying public API signatures (check all call sites)
- After regenerating protobuf files
- As part of development workflow before moving to next phase

## Requirements

- Rust toolchain installed (stable recommended)
- System dependencies: `protobuf-compiler`, `libfuse-dev`, `fuse3`, `pkg-config`, `libssl-dev`
- `/dev/fuse` permissions: `sudo chmod 666 /dev/fuse` (for FUSE integration tests)
- All checks must pass with **NO errors and NO warnings**
- Tests must pass 100% (0 failures)
- Build must complete without warnings

## Verification Checklist

After running all checks, ensure:
- [ ] `cargo fmt --all -- --check` — no output (clean)
- [ ] `cargo clippy --all -- -D warnings` — "Finished" with no warnings
- [ ] `cargo build --all` — "Finished" with no warnings
- [ ] `cargo test --all` — all test suites show "0 failed"
- [ ] Coherence tests pass (including `--test-threads=1` for master_outage_e2e_test)

## Common Pitfalls

1. **Method signature changes**: When adding a parameter to a public method (e.g., `create_entry`, `update_entry`, `delete_entry`), ALL test files must be updated. Use `grep -rn "method_name" powerfs-*/tests/` to find all call sites.

2. **Generated code formatting**: Protobuf-generated files (`*.rs` in `volume_proto/` directories) are checked by `cargo fmt`. Always run `cargo fmt --all` after regenerating.

3. **Integration test ordering**: `master_outage_e2e_test` must run with `--test-threads=1` to avoid port conflicts and state interference.

4. **Build before test**: Integration tests spawn real binaries. Run `cargo build --all` before `cargo test --all` to ensure binaries exist.
