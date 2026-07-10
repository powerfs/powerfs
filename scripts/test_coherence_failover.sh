#!/bin/bash
# Master Failover Test: Verify lease mechanism behavior during Master restart
# This script tests the epoch-based lease invalidation mechanism

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

FUSE1_CONTAINER="fuse-1"
FUSE2_CONTAINER="fuse-2"
MASTER_CONTAINER="master-1"
FUSE_MOUNT="/mnt/powerfs"

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
FAILED_TESTS=()

log_info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_warn() {
    echo "[WARN] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2
}

test_start() {
    TEST_NAME="$1"
    TEST_START_TIME=$(date +%s%N)
    echo ""
    echo "=============================================="
    echo "  TEST: $TEST_NAME"
    echo "=============================================="
}

test_pass() {
    local end_time=$(date +%s%N)
    local duration=$(( (end_time - TEST_START_TIME) / 1000000 ))
    echo "[PASS] $TEST_NAME (${duration}ms)"
    PASS_COUNT=$((PASS_COUNT + 1))
}

test_fail() {
    local reason="$1"
    local end_time=$(date +%s%N)
    local duration=$(( (end_time - TEST_START_TIME) / 1000000 ))
    echo "[FAIL] $TEST_NAME (${duration}ms)"
    echo "       Reason: $reason"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED_TESTS+=("$TEST_NAME: $reason")
}

test_skip() {
    local reason="$1"
    echo "[SKIP] $TEST_NAME"
    echo "       Reason: $reason"
    SKIP_COUNT=$((SKIP_COUNT + 1))
}

check_container_running() {
    local container=$1
    docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null | grep -q true
}

exec_fuse1() {
    docker exec "$FUSE1_CONTAINER" "$@"
}

exec_fuse2() {
    docker exec "$FUSE2_CONTAINER" "$@"
}

# Restart Master container
restart_master() {
    log_info "Restarting Master container..."
    docker restart "$MASTER_CONTAINER" >/dev/null 2>&1
    sleep 3

    local max_wait=30
    local attempt=0
    while [ $attempt -lt $max_wait ]; do
        if docker exec "$MASTER_CONTAINER" powerfs-master status 2>/dev/null | grep -q "OK"; then
            log_info "Master is ready"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    log_error "Master failed to become ready after restart"
    return 1
}

# ============================================================
# Test 1: Lease survives normal operation
# ============================================================

test_lease_normal_operation() {
    test_start "Lease acquired and released normally"
    local test_dir="$FUSE_MOUNT/failover_normal_$$"
    local test_file="$test_dir/file.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null
    exec_fuse1 bash -c "echo 'test' > '$test_file'" 2>/dev/null
    sleep 0.5

    local content
    content=$(exec_fuse2 cat "$test_file" 2>/dev/null || echo "")
    if [ "$content" = "test" ]; then
        test_pass
    else
        test_fail "Cannot read file: '$content'"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

# ============================================================
# Test 2: Master restart clears all leases
# ============================================================

test_master_restart_clears_leases() {
    test_start "Master restart clears all leases"

    local test_dir="$FUSE_MOUNT/failover_clear_$$"
    local test_file="$test_dir/leased.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null
    exec_fuse1 bash -c "echo 'v1' > '$test_file'" 2>/dev/null
    sleep 0.5

    # Client1 opens file (acquires lease)
    exec_fuse1 cat "$test_file" >/dev/null 2>&1
    sleep 0.5

    # Restart Master
    if ! restart_master; then
        test_skip "Master restart failed"
        exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
        return
    fi

    # After restart, Master has no leases
    # Client2 should be able to modify the file
    exec_fuse2 bash -c "echo 'v2' > '$test_file'" 2>/dev/null
    sleep 2

    local content
    content=$(exec_fuse1 cat "$test_file" 2>/dev/null || echo "")

    if [ "$content" = "v2" ]; then
        test_pass
    else
        test_fail "Client1 sees stale data: '$content' (expected 'v2')"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

# ============================================================
# Test 3: Cache consistency after Master restart
# ============================================================

test_cache_consistency_after_restart() {
    test_start "Cache consistency maintained after Master restart"

    local test_dir="$FUSE_MOUNT/failover_consistency_$$"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null
    sleep 0.5

    # Restart Master
    if ! restart_master; then
        test_skip "Master restart failed"
        exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
        return
    fi

    # After restart, create files and verify consistency
    local file1="$test_dir/post_restart_1.txt"
    local file2="$test_dir/post_restart_2.txt"

    exec_fuse1 bash -c "echo 'file1' > '$file1'" 2>/dev/null
    exec_fuse2 bash -c "echo 'file2' > '$file2'" 2>/dev/null
    sleep 2

    local c1
    local c2
    c1=$(exec_fuse2 cat "$file1" 2>/dev/null || echo "")
    c2=$(exec_fuse1 cat "$file2" 2>/dev/null || echo "")

    if [ "$c1" = "file1" ] && [ "$c2" = "file2" ]; then
        test_pass
    else
        test_fail "Cross-client visibility failed: file1='$c1', file2='$c2'"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

# ============================================================
# Test 4: New leases work after Master restart
# ============================================================

test_new_leases_after_restart() {
    test_start "New leases can be acquired after Master restart"

    local test_dir="$FUSE_MOUNT/failover_new_lease_$$"
    local test_file="$test_dir/new_lease.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null
    exec_fuse1 bash -c "echo 'initial' > '$test_file'" 2>/dev/null
    sleep 0.5

    # Restart Master
    if ! restart_master; then
        test_skip "Master restart failed"
        exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
        return
    fi

    # After restart, client opens file (should get new lease)
    local content
    content=$(exec_fuse1 cat "$test_file" 2>/dev/null || echo "")

    if [ "$content" = "initial" ]; then
        test_pass
    else
        test_fail "Cannot read file after restart: '$content'"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

# ============================================================
# Test 5: Multiple restarts stability
# ============================================================

test_multiple_restarts_stability() {
    test_start "Multiple Master restarts maintain consistency"

    local test_dir="$FUSE_MOUNT/failover_multi_$$"
    local test_file="$test_dir/counter.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null
    exec_fuse1 bash -c "echo '0' > '$test_file'" 2>/dev/null
    sleep 0.5

    local failures=0

    for i in 1 2 3; do
        if ! restart_master; then
            test_skip "Master restart #$i failed"
            exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
            return
        fi

        local expected="$i"
        exec_fuse1 bash -c "echo '$i' > '$test_file'" 2>/dev/null
        sleep 2

        local content
        content=$(exec_fuse2 cat "$test_file" 2>/dev/null || echo "")

        if [ "$content" != "$expected" ]; then
            failures=$((failures + 1))
            log_warn "Iteration $i: expected '$expected', got '$content'"
        fi
    done

    if [ $failures -eq 0 ]; then
        test_pass
    else
        test_fail "$failures/3 iterations failed"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

# ============================================================
# Test 6: Subscription recovers after Master restart
# ============================================================

test_subscription_recovers_after_restart() {
    test_start "Metadata subscription recovers after Master restart"

    local test_dir="$FUSE_MOUNT/failover_sub_$$"
    local test_file="$test_dir/sub_test.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null
    sleep 0.5

    # Restart Master
    if ! restart_master; then
        test_skip "Master restart failed"
        exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
        return
    fi

    # Wait for subscription to reconnect
    sleep 10

    # Create file on fuse1, verify visible on fuse2
    exec_fuse1 bash -c "echo 'after_restart' > '$test_file'" 2>/dev/null
    sleep 3

    local content
    content=$(exec_fuse2 cat "$test_file" 2>/dev/null || echo "")

    if [ "$content" = "after_restart" ]; then
        test_pass
    else
        test_fail "Subscription not recovered: '$content'"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

# ============================================================
# Print summary
# ============================================================

print_summary() {
    echo ""
    echo "=============================================="
    echo "  MASTER FAILOVER TEST SUMMARY"
    echo "=============================================="
    echo "  Passed:  $PASS_COUNT"
    echo "  Failed:  $FAIL_COUNT"
    echo "  Skipped: $SKIP_COUNT"
    echo ""
    if [ "$FAIL_COUNT" -gt 0 ]; then
        echo "Failed tests:"
        for failed in "${FAILED_TESTS[@]}"; do
            echo "  - $failed"
        done
        echo ""
        return 1
    fi
    echo "All tests passed!"
    return 0
}

# ============================================================
# Main
# ============================================================

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  PowerFS Master Failover Lease Behavior Test Suite     ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

log_info "Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed"
    exit 1
fi

for container in "$FUSE1_CONTAINER" "$FUSE2_CONTAINER" "$MASTER_CONTAINER"; do
    if ! check_container_running "$container"; then
        log_error "Container $container is not running"
        log_error "Please start the Docker cluster first: bash docker/scripts/start-fuse.sh"
        exit 1
    fi
done

log_info "  All containers are running"

# Run tests
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Master Failover Lease Behavior Tests"
echo "═══════════════════════════════════════════════════════════"

test_lease_normal_operation
test_master_restart_clears_leases
test_cache_consistency_after_restart
test_new_leases_after_restart
test_multiple_restarts_stability
test_subscription_recovers_after_restart

print_summary
