---
name: "powerfs-ci-local"
description: "Run PowerFS pre-PR local validation through cargo check, fmt, clippy, tests, and build. Use this skill whenever the user wants to validate a branch before opening or submitting a PR, run local CI, check changes before PR, reproduce GitHub Actions locally, or force a full pre-submit verification."
---

# PowerFS Pre-PR Local Validation

Use this skill to validate PowerFS code before submitting a PR. It runs the same checks as GitHub Actions to ensure your changes will pass CI.

## Default Entry Point

When the user asks for any of the following, run the validation commands:

- 提交 PR 前本地验证
- run ci test
- run local CI
- check my branch before PR
- reproduce CI locally
- validate changes

## Standard Validation Workflow

Run all checks sequentially:

```bash
# 1. Code formatting check
cargo fmt --check --all

# 2. Clippy linting (warnings as errors)
cargo clippy --all -- -D warnings

# 3. Compilation check
cargo check --all

# 4. Run all tests
cargo test --workspace

# 5. Build verification
cargo build --all
```

## Common Options

**Fix formatting issues:**
```bash
cargo fmt --all
```

**Run tests for a specific package:**
```bash
cargo test -p powerfs-monitor
```

**Build in release mode:**
```bash
cargo build --all --release
```

## How To Interpret Results

- ✅ **PASSED**: The check completed successfully
- ❌ **FAILED**: The check failed and needs investigation
- ⚠️ **WARNING**: Clippy warnings exist (will fail with `-D warnings`)

## Targeted Reruns For Debugging

Use targeted reruns only after the full validation identifies a failing area, or when the user explicitly asks for a smaller scope.

**Rerun a specific test:**
```bash
cargo test -p powerfs-monitor --test auth_test
```

**Check only specific packages:**
```bash
cargo check -p powerfs-monitor -p powerfs-core
```

## Current Coverage

Included by default:

- ✅ Code formatting (`cargo fmt`)
- ✅ Linting (`cargo clippy`)
- ✅ Compilation (`cargo check`)
- ✅ Unit tests (`cargo test`)
- ✅ Build verification (`cargo build`)

## Notes For The Agent

- Prefer running all checks instead of individual commands
- Summarize failing checks with specific error messages
- If formatting fails, run `cargo fmt --all` to auto-fix
- If clippy warns, fix the issues manually as clippy provides suggestions
- For test failures, isolate and rerun the specific failing test
