---
name: "code-quality"
description: "Performs comprehensive Rust code quality checks including cargo check, fmt, clippy, tests, and build. Invoke after code changes, before commits, or to ensure GitHub Actions pass."
---

# Code Quality Check

This skill performs comprehensive Rust code quality checks to ensure the codebase meets production standards and passes GitHub Actions.

## Checks Performed

1. **cargo check**: Compilation check to detect syntax and type errors
2. **cargo fmt --check --all**: Code formatting check
3. **cargo clippy --all -- -D warnings**: Linting with warnings treated as errors
4. **cargo test --workspace**: Run all tests
5. **cargo build**: Full build verification

## Usage

Run all checks sequentially:

```bash
cargo check --all && \
cargo fmt --check --all && \
cargo clippy --all -- -D warnings && \
cargo test --workspace && \
cargo build --all
```

## Error Handling

1. **Formatting issues**: Run `cargo fmt --all` to auto-fix
2. **Clippy warnings**: Fix the issues manually, clippy provides suggestions
3. **Test failures**: Debug and fix the failing tests
4. **Build errors**: Fix compilation issues

## When to Invoke

- After making code changes
- Before committing changes
- Before pushing to remote
- To verify code quality before PR submission
- When GitHub Actions fails
- As part of development workflow before moving to next phase

## Requirements

- Rust toolchain installed (stable recommended)
- All checks must pass with no warnings or errors
- Tests must pass 100%

## Verification

After running all checks, ensure:
- ✅ `cargo check` completes without errors
- ✅ `cargo fmt --check` shows no formatting issues
- ✅ `cargo clippy` shows no warnings (with -D warnings)
- ✅ All tests pass
- ✅ `cargo build` completes successfully
