#!/bin/bash

set -e

TEST_SUITE="all"
VERBOSE_LOG=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --suite|-s)
            TEST_SUITE="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE_LOG=true
            shift
            ;;
        --help|-h)
            cat <<EOF
Usage: run-tests.sh [OPTIONS]

Run tests against an already-started PowerFS environment.

Prerequisites:
  1. docker/scripts/start-cluster.sh
  2. docker/scripts/start-fuse.sh

Options:
  --suite, -s NAME    Test suite: all|mount|basic|rfs|volume|posix|concurrent|coherence|fs|sync|minimal|manual
  --verbose, -v       Show full test output
  --help, -h          Show this help
EOF
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
START_TIME=$(date +%s)

log_info()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $1"; }
log_warn()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] $1"; }
log_error() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $1"; }

log_info "========================================"
log_info "    PowerFS Test Runner"
log_info "========================================"
log_info "  Test suite: $TEST_SUITE"
log_info "  Verbose:    $VERBOSE_LOG"
log_info ""

# ============================================================
# Environment checks
# ============================================================

log_info "[1/3] Checking environment..."

if ! docker inspect fuse-1 >/dev/null 2>&1; then
    log_error "Container 'fuse-1' is not running."
    log_error ""
    log_error "Please start the environment first:"
    log_error "  docker/scripts/start-cluster.sh"
    log_error "  docker/scripts/start-fuse.sh"
    exit 2
fi

# 检查 powerfs-fuse 进程是否存活（非 zombie）
FUSE_PID=$(docker exec fuse-1 pgrep -x powerfs-fuse 2>/dev/null || echo "")
FUSE_ZOMBIE=$(docker exec fuse-1 pgrep -x -z powerfs-fuse 2>/dev/null || echo "")
if [ -z "$FUSE_PID" ]; then
    if [ -n "$FUSE_ZOMBIE" ]; then
        log_error "powerfs-fuse process is dead (zombie) in container 'fuse-1'."
    else
        log_error "powerfs-fuse process is not running in container 'fuse-1'."
    fi
    log_error ""
    log_error "Please restart FUSE client:"
    log_error "  docker/scripts/start-fuse.sh"
    exit 2
fi
log_info "  [OK] powerfs-fuse process is alive (pid=$FUSE_PID)"

if ! docker exec fuse-1 mount 2>/dev/null | grep -E "on /mnt/powerfs type fuse" >/dev/null; then
    log_error "FUSE is not mounted at /mnt/powerfs in container 'fuse-1'."
    log_error ""
    log_error "Please check FUSE client status:"
    log_error "  docker logs fuse-1"
    log_error "  docker exec fuse-1 mount | grep powerfs"
    log_error ""
    log_error "Or restart FUSE client:"
    log_error "  docker/scripts/start-fuse.sh"
    exit 2
fi
log_info "  [OK] FUSE is mounted at /mnt/powerfs"

# 读写探针测试：必须加超时，防止 FUSE 守护进程死掉时卡死容器
if ! timeout 5 docker exec fuse-1 bash -c "echo probe > /mnt/powerfs/.run-tests-probe && cat /mnt/powerfs/.run-tests-probe && rm /mnt/powerfs/.run-tests-probe" >/dev/null 2>&1; then
    log_error "FUSE read/write test failed (or timed out after 5s) at /mnt/powerfs."
    log_error "This usually means the FUSE daemon is stuck or dead."
    log_error ""
    log_error "Please restart FUSE client:"
    log_error "  docker/scripts/start-fuse.sh"
    exit 2
fi
log_info "  [OK] FUSE read/write verified"

log_info ""

# ============================================================
# Run tests
# ============================================================

log_info "[2/3] Running tests..."
log_info ""

TEST_RESULTS=()
FAILED_TESTS=()

run_test() {
    local test_name="$1"
    local test_args="$2"

    log_info "  ──────────────────────────────────────"
    log_info "  Running: $test_name"
    log_info "  ──────────────────────────────────────"

    local t0=$(date +%s)
    local output
    output=$(docker exec fuse-1 bash -c "cd /app && POWERFS_DOCKER_TEST=1 POWERFS_MOUNT=/mnt/powerfs cargo test $test_args -- --test-threads=1 2>&1" || true)
    local code=$?
    local t1=$(date +%s)
    local elapsed=$((t1 - t0))

    if [ "$VERBOSE_LOG" = true ]; then
        echo "$output"
    fi

    if [ $code -eq 0 ]; then
        log_info "  [OK] $test_name passed ($elapsed s)"
        TEST_RESULTS+=("$test_name: PASS")
    else
        log_error "  [FAIL] $test_name failed ($elapsed s)"
        echo "$output" | tail -10 | while IFS= read -r line; do
            log_error "    $line"
        done
        TEST_RESULTS+=("$test_name: FAIL")
        FAILED_TESTS+=("$test_name")
    fi
}

run_manual_test() {
    local test_name="$1"
    local test_command="$2"

    log_info "  ──────────────────────────────────────"
    log_info "  Running: $test_name"
    log_info "  ──────────────────────────────────────"

    local t0=$(date +%s)
    local output
    output=$(docker exec fuse-1 bash -c "$test_command" 2>&1 || true)
    local code=$?
    local t1=$(date +%s)
    local elapsed=$((t1 - t0))

    if [ "$VERBOSE_LOG" = true ]; then
        echo "$output"
    fi

    if [ $code -eq 0 ]; then
        log_info "  [OK] $test_name passed ($elapsed s)"
        TEST_RESULTS+=("$test_name: PASS")
    else
        log_error "  [FAIL] $test_name failed ($elapsed s)"
        TEST_RESULTS+=("$test_name: FAIL")
        FAILED_TESTS+=("$test_name")
    fi
}

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "mount" ]; then
    run_test "Mount Verification" "--manifest-path powerfs-fuse/Cargo.toml --test mount_verification_test"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "basic" ]; then
    run_test "FUSE Basic Operations" "--manifest-path powerfs-fuse/Cargo.toml --test fuse_basic_test"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "rfs" ]; then
    run_test "RFS Tester Integration" "--manifest-path powerfs-fuse/Cargo.toml --test rfs_tester_fuse_test"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "volume" ]; then
    run_test "Volume Integration" "--manifest-path powerfs-fuse/Cargo.toml --test volume_integration_test"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "volume" ]; then
    run_test "Volume Verification" "--manifest-path powerfs-fuse/Cargo.toml --test volume_verification_test"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "posix" ]; then
    run_test "POSIX Compliance" "--manifest-path powerfs-fuse/Cargo.toml --test posix_tests"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "concurrent" ]; then
    run_test "Concurrent Consistency" "--manifest-path powerfs-fuse/Cargo.toml --test concurrent_consistency"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "fs" ]; then
    run_test "File System" "--manifest-path powerfs-fuse/Cargo.toml --test fs_test"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "coherence" ]; then
    run_test "Coherence Phase 0" "--manifest-path powerfs-fuse/Cargo.toml --test coherence_phase0_test"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "coherence" ]; then
    run_test "Coherence Phase 1" "--manifest-path powerfs-fuse/Cargo.toml --test coherence_phase1_test"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "sync" ]; then
    run_test "Sync" "--manifest-path powerfs-fuse/Cargo.toml --test sync_test"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "minimal" ]; then
    run_test "FUSE Minimal" "--manifest-path powerfs-fuse/Cargo.toml --test fuse_minimal_test"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "manual" ]; then
    log_info ""
    log_info "  Running manual verification tests..."

    run_manual_test "Directory Creation and Visibility" "mkdir -p /mnt/powerfs/manual_test_dir && ls /mnt/powerfs/ | grep -q manual_test_dir && echo 'Directory visible'"
    run_manual_test "File Creation and Read" "echo 'test content' > /mnt/powerfs/manual_test_dir/test.txt && cat /mnt/powerfs/manual_test_dir/test.txt | grep -q 'test content'"
    run_manual_test "Directory Listing" "ls /mnt/powerfs/manual_test_dir/ | grep -q test.txt"
    run_manual_test "Cleanup" "rm /mnt/powerfs/manual_test_dir/test.txt && rmdir /mnt/powerfs/manual_test_dir && ! ls /mnt/powerfs/ | grep -q manual_test_dir"
fi

# ============================================================
# Summary
# ============================================================

log_info ""
log_info "[3/3] Test Results Summary"
log_info "============================="

PASS_COUNT=0
FAIL_COUNT=0

for result in "${TEST_RESULTS[@]}"; do
    if echo "$result" | grep -q "PASS"; then
        echo "  ✓ $result"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        echo "  ✗ $result"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
done

log_info ""
log_info "  Total:   ${#TEST_RESULTS[@]} tests"
log_info "  Passed:  $PASS_COUNT"
log_info "  Failed:  $FAIL_COUNT"

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
    log_info ""
    log_error "Failed tests:"
    for test in "${FAILED_TESTS[@]}"; do
        log_error "  - $test"
    done
fi

log_info ""
END_TIME=$(date +%s)
log_info "Test run completed in $((END_TIME - START_TIME)) seconds"

if [ $FAIL_COUNT -gt 0 ]; then
    exit 1
fi
exit 0
