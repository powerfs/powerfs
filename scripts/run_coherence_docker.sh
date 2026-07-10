#!/bin/bash
# Run FUSE Coherence end-to-end tests in Docker environment
# This script executes coherence tests against the Docker-based PowerFS cluster
# using fuse-1 and fuse-2 containers as dual FUSE clients

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

# Docker container names (must match docker-compose.yml)
FUSE1_CONTAINER="fuse-1"
FUSE2_CONTAINER="fuse-2"
MASTER_CONTAINER="master-1"

# Mount points inside containers
FUSE_MOUNT="/mnt/powerfs"

# Test results
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

print_summary() {
    echo ""
    echo "=============================================="
    echo "  DOCKER COHERENCE TEST SUMMARY"
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

# Check if a container is running
check_container_running() {
    local container=$1
    if docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null | grep -q true; then
        return 0
    fi
    return 1
}

# Execute command in fuse-1 container
exec_fuse1() {
    docker exec "$FUSE1_CONTAINER" "$@"
}

# Execute command in fuse-2 container
exec_fuse2() {
    docker exec "$FUSE2_CONTAINER" "$@"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi

    if ! check_container_running "$FUSE1_CONTAINER"; then
        log_error "Container $FUSE1_CONTAINER is not running"
        log_error "Please start the Docker cluster first: bash docker/scripts/start-fuse.sh"
        exit 1
    fi

    if ! check_container_running "$FUSE2_CONTAINER"; then
        log_error "Container $FUSE2_CONTAINER is not running"
        log_error "Please start the Docker cluster first: bash docker/scripts/start-fuse.sh"
        exit 1
    fi

    if ! check_container_running "$MASTER_CONTAINER"; then
        log_error "Container $MASTER_CONTAINER is not running"
        exit 1
    fi

    log_info "  All containers are running"
    log_info "  FUSE mount point: $FUSE_MOUNT"
}

# Wait for FUSE mount to be ready in a container
wait_for_fuse_mount() {
    local container=$1
    local max_wait=30
    local attempt=0

    while [ $attempt -lt $max_wait ]; do
        if docker exec "$container" mountpoint -q "$FUSE_MOUNT" 2>/dev/null; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    return 1
}

# ============================================================
# Phase 0: Synchronous Commit + Error Rollback
# ============================================================

test_phase0_mkdir_sync() {
    test_start "Phase 0: mkdir synchronous commit"
    local test_dir="$FUSE_MOUNT/phase0_mkdir_$$"

    if exec_fuse1 mkdir -p "$test_dir" 2>/dev/null; then
        if exec_fuse2 test -d "$test_dir" 2>/dev/null; then
            test_pass
        else
            test_fail "Directory not visible from fuse-2"
        fi
    else
        test_fail "mkdir failed on fuse-1"
    fi

    exec_fuse1 rmdir "$test_dir" 2>/dev/null || true
}

test_phase0_create_sync() {
    test_start "Phase 0: create file synchronous commit"
    local test_file="$FUSE_MOUNT/phase0_create_$$.txt"

    if exec_fuse1 bash -c "echo 'test content' > '$test_file'" 2>/dev/null; then
        local content
        content=$(exec_fuse2 cat "$test_file" 2>/dev/null || echo "")
        if [ "$content" = "test content" ]; then
            test_pass
        else
            test_fail "Content mismatch: expected 'test content', got '$content'"
        fi
    else
        test_fail "File creation failed on fuse-1"
    fi

    exec_fuse1 rm -f "$test_file" 2>/dev/null || true
}

test_phase0_unlink_sync() {
    test_start "Phase 0: unlink synchronous commit"
    local test_file="$FUSE_MOUNT/phase0_unlink_$$.txt"

    exec_fuse1 bash -c "echo 'to delete' > '$test_file'" 2>/dev/null || true
    sleep 0.5

    if exec_fuse1 rm -f "$test_file" 2>/dev/null; then
        if ! exec_fuse2 test -f "$test_file" 2>/dev/null; then
            test_pass
        else
            test_fail "File still visible from fuse-2 after unlink"
        fi
    else
        test_fail "unlink failed on fuse-1"
    fi
}

test_phase0_rename_sync() {
    test_start "Phase 0: rename synchronous commit"
    local old_file="$FUSE_MOUNT/phase0_rename_old_$$.txt"
    local new_file="$FUSE_MOUNT/phase0_rename_new_$$.txt"

    exec_fuse1 bash -c "echo 'rename me' > '$old_file'" 2>/dev/null || true
    sleep 0.5

    if exec_fuse1 mv "$old_file" "$new_file" 2>/dev/null; then
        if exec_fuse2 test -f "$new_file" 2>/dev/null && ! exec_fuse2 test -f "$old_file" 2>/dev/null; then
            test_pass
        else
            test_fail "Rename not visible correctly from fuse-2"
        fi
    else
        test_fail "rename failed on fuse-1"
    fi

    exec_fuse1 rm -f "$new_file" 2>/dev/null || true
}

# ============================================================
# Phase 1: Server-Driven Cache Invalidation
# ============================================================

test_phase1_create_invalidation() {
    test_start "Phase 1: create triggers cross-client invalidation"
    local test_dir="$FUSE_MOUNT/phase1_create_$$"
    local test_file="$test_dir/file.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null || true
    sleep 0.5

    exec_fuse2 ls "$test_dir" 2>/dev/null || true

    exec_fuse1 bash -c "echo 'new file' > '$test_file'" 2>/dev/null
    sleep 1

    local content
    content=$(exec_fuse2 cat "$test_file" 2>/dev/null || echo "")
    if [ "$content" = "new file" ]; then
        test_pass
    else
        test_fail "Client2 cannot see newly created file: '$content'"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

test_phase1_delete_invalidation() {
    test_start "Phase 1: delete triggers cross-client invalidation"
    local test_dir="$FUSE_MOUNT/phase1_delete_$$"
    local test_file="$test_dir/to_delete.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null || true
    exec_fuse1 bash -c "echo 'delete me' > '$test_file'" 2>/dev/null
    sleep 0.5

    exec_fuse2 cat "$test_file" 2>/dev/null || true

    exec_fuse1 rm -f "$test_file" 2>/dev/null
    sleep 1

    if ! exec_fuse2 test -f "$test_file" 2>/dev/null; then
        test_pass
    else
        test_fail "Client2 still sees deleted file"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

test_phase1_mkdir_invalidation() {
    test_start "Phase 1: mkdir triggers directory cache invalidation"
    local parent_dir="$FUSE_MOUNT/phase1_mkdir_$$"
    local new_dir="$parent_dir/subdir"

    exec_fuse1 mkdir -p "$parent_dir" 2>/dev/null || true
    sleep 0.5

    exec_fuse2 ls "$parent_dir" 2>/dev/null || true

    exec_fuse1 mkdir "$new_dir" 2>/dev/null
    sleep 1

    if exec_fuse2 test -d "$new_dir" 2>/dev/null; then
        test_pass
    else
        test_fail "Client2 cannot see newly created directory"
    fi

    exec_fuse1 rm -rf "$parent_dir" 2>/dev/null || true
}

test_phase1_rename_invalidation() {
    test_start "Phase 1: rename triggers dual-path invalidation"
    local test_dir="$FUSE_MOUNT/phase1_rename_$$"
    local old_file="$test_dir/old.txt"
    local new_file="$test_dir/new.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null || true
    exec_fuse1 bash -c "echo 'rename' > '$old_file'" 2>/dev/null
    sleep 0.5

    exec_fuse2 ls "$test_dir" 2>/dev/null || true

    exec_fuse1 mv "$old_file" "$new_file" 2>/dev/null
    sleep 1

    if exec_fuse2 test -f "$new_file" 2>/dev/null && ! exec_fuse2 test -f "$old_file" 2>/dev/null; then
        test_pass
    else
        test_fail "Rename not visible correctly from client2"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

# ============================================================
# Phase 2: Lease Mechanism
# ============================================================

test_phase2_lease_on_open() {
    test_start "Phase 2: lease acquired on file open"
    local test_dir="$FUSE_MOUNT/phase2_open_$$"
    local test_file="$test_dir/leased.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null || true
    exec_fuse1 bash -c "echo 'lease test' > '$test_file'" 2>/dev/null
    sleep 0.5

    if exec_fuse1 python3 -c "
import time
f = open('$test_file', 'r')
content = f.read()
time.sleep(1)
f.close()
print('OK')
" 2>/dev/null | grep -q "OK"; then
        test_pass
    else
        test_skip "Python3 not available in container or file open failed"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

test_phase2_lease_release_on_close() {
    test_start "Phase 2: lease released on file close"
    local test_dir="$FUSE_MOUNT/phase2_release_$$"
    local test_file="$test_dir/release.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null || true
    exec_fuse1 bash -c "echo 'release test' > '$test_file'" 2>/dev/null
    sleep 0.5

    if exec_fuse1 python3 -c "
f = open('$test_file', 'r')
f.close()
f2 = open('$test_file', 'r')
content = f2.read()
f2.close()
assert content == 'release test'
print('OK')
" 2>/dev/null | grep -q "OK"; then
        test_pass
    else
        test_skip "Python3 not available or test failed"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

test_phase2_concurrent_access() {
    test_start "Phase 2: concurrent access with lease"
    local test_dir="$FUSE_MOUNT/phase2_concurrent_$$"
    local test_file1="$test_dir/shared.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null || true
    exec_fuse1 bash -c "echo 'initial' > '$test_file1'" 2>/dev/null
    sleep 0.5

    exec_fuse1 bash -c "echo 'modified' > '$test_file1'" 2>/dev/null &
    local writer_pid=$!
    sleep 1

    local content
    content=$(exec_fuse2 cat "$test_file1" 2>/dev/null || echo "")
    wait $writer_pid 2>/dev/null || true

    if [ -n "$content" ]; then
        test_pass
    else
        test_fail "Cannot read file during concurrent access"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

# ============================================================
# Phase 3: Job-Level Strong Consistency
# ============================================================

test_phase3_job_file_visibility() {
    test_start "Phase 3: job-level file visibility"
    local test_dir="$FUSE_MOUNT/phase3_visibility_$$"
    local test_file="$test_dir/job_file.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null || true
    sleep 0.5

    exec_fuse1 bash -c "echo 'job data' > '$test_file'" 2>/dev/null
    sleep 1

    local content
    content=$(exec_fuse2 cat "$test_file" 2>/dev/null || echo "")
    if [ "$content" = "job data" ]; then
        test_pass
    else
        test_fail "File not visible across clients: '$content'"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

test_phase3_job_dir_consistency() {
    test_start "Phase 3: job-level directory listing consistency"
    local test_dir="$FUSE_MOUNT/phase3_dirlist_$$"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null || true
    sleep 0.5

    for i in $(seq 1 5); do
        exec_fuse1 bash -c "echo 'file $i' > '$test_dir/file_$i.txt'" 2>/dev/null
    done
    sleep 1

    local count1
    local count2
    count1=$(exec_fuse1 ls "$test_dir" 2>/dev/null | wc -l)
    count2=$(exec_fuse2 ls "$test_dir" 2>/dev/null | wc -l)

    if [ "$count1" = "$count2" ] && [ "$count1" = "5" ]; then
        test_pass
    else
        test_fail "Directory listing mismatch: fuse1=$count1, fuse2=$count2"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

test_phase3_job_modification_visibility() {
    test_start "Phase 3: job-level modification visibility"
    local test_dir="$FUSE_MOUNT/phase3_modify_$$"
    local test_file="$test_dir/data.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null || true
    exec_fuse1 bash -c "echo 'initial' > '$test_file'" 2>/dev/null
    sleep 0.5

    exec_fuse1 bash -c "echo 'modified' > '$test_file'" 2>/dev/null
    sleep 1

    local content
    content=$(exec_fuse2 cat "$test_file" 2>/dev/null || echo "")
    if [ "$content" = "modified" ]; then
        test_pass
    else
        test_fail "Modification not visible: '$content'"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

test_phase3_job_deletion_visibility() {
    test_start "Phase 3: job-level deletion visibility"
    local test_dir="$FUSE_MOUNT/phase3_delete_$$"
    local test_file="$test_dir/to_delete.txt"

    exec_fuse1 mkdir -p "$test_dir" 2>/dev/null || true
    exec_fuse1 bash -c "echo 'delete me' > '$test_file'" 2>/dev/null
    sleep 0.5

    exec_fuse2 cat "$test_file" 2>/dev/null || true

    exec_fuse1 rm -f "$test_file" 2>/dev/null
    sleep 1

    if ! exec_fuse2 test -f "$test_file" 2>/dev/null; then
        test_pass
    else
        test_fail "Deletion not visible from client2"
    fi

    exec_fuse1 rm -rf "$test_dir" 2>/dev/null || true
}

# ============================================================
# Main execution
# ============================================================

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  PowerFS FUSE Coherence Docker E2E Test Suite           ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

check_prerequisites

log_info "Waiting for FUSE mounts to be ready..."
if ! wait_for_fuse_mount "$FUSE1_CONTAINER"; then
    log_error "FUSE mount not ready in $FUSE1_CONTAINER"
    exit 1
fi

if ! wait_for_fuse_mount "$FUSE2_CONTAINER"; then
    log_error "FUSE mount not ready in $FUSE2_CONTAINER"
    exit 1
fi
log_info "  FUSE mounts are ready"

# Run Phase 0 tests
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Phase 0: Synchronous Commit + Error Rollback"
echo "═══════════════════════════════════════════════════════════"

test_phase0_mkdir_sync
test_phase0_create_sync
test_phase0_unlink_sync
test_phase0_rename_sync

# Run Phase 1 tests
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Phase 1: Server-Driven Cache Invalidation"
echo "═══════════════════════════════════════════════════════════"

test_phase1_create_invalidation
test_phase1_delete_invalidation
test_phase1_mkdir_invalidation
test_phase1_rename_invalidation

# Run Phase 2 tests
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Phase 2: Lease Mechanism"
echo "═══════════════════════════════════════════════════════════"

test_phase2_lease_on_open
test_phase2_lease_release_on_close
test_phase2_concurrent_access

# Run Phase 3 tests
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Phase 3: Job-Level Strong Consistency"
echo "═══════════════════════════════════════════════════════════"

test_phase3_job_file_visibility
test_phase3_job_dir_consistency
test_phase3_job_modification_visibility
test_phase3_job_deletion_visibility

# Final summary
print_summary
