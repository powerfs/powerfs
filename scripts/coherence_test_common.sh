#!/bin/bash
# Common utilities for FUSE coherence end-to-end tests

set -e

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
FAILED_TESTS=()

TEST_START_TIME=""
TEST_NAME=""

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

assert_eq() {
    local expected="$1"
    local actual="$2"
    local desc="$3"
    if [ "$expected" != "$actual" ]; then
        test_fail "$desc: expected '$expected', got '$actual'"
        return 1
    fi
    return 0
}

assert_file_exists() {
    local path="$1"
    if [ ! -f "$path" ]; then
        test_fail "File does not exist: $path"
        return 1
    fi
    return 0
}

assert_file_not_exists() {
    local path="$1"
    if [ -f "$path" ]; then
        test_fail "File should not exist: $path"
        return 1
    fi
    return 0
}

assert_dir_exists() {
    local path="$1"
    if [ ! -d "$path" ]; then
        test_fail "Directory does not exist: $path"
        return 1
    fi
    return 0
}

assert_dir_not_exists() {
    local path="$1"
    if [ -d "$path" ]; then
        test_fail "Directory should not exist: $path"
        return 1
    fi
    return 0
}

assert_file_content() {
    local path="$1"
    local expected="$2"
    local actual
    actual=$(cat "$path" 2>/dev/null || true)
    if [ "$actual" != "$expected" ]; then
        test_fail "Content mismatch for $path: expected '$expected', got '$actual'"
        return 1
    fi
    return 0
}

print_summary() {
    echo ""
    echo "=============================================="
    echo "  TEST SUMMARY"
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

setup_test_env() {
    SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
    PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

    MOUNT_DIR="${MOUNT_DIR:-/tmp/powerfs-coherence-test}"
    MASTER_DIR="${MASTER_DIR:-/tmp/powerfs-coherence-master}"
    VOLUME_DIR="${VOLUME_DIR:-/tmp/powerfs-coherence-volume}"

    MASTER_PORT="${MASTER_PORT:-9460}"
    MASTER_GRPC_PORT=$((MASTER_PORT + 1))
    VOLUME_PORT="${VOLUME_PORT:-8197}"
    VOLUME_HTTP_PORT=$((VOLUME_PORT + 1))

    MASTER_PID=""
    VOLUME_PID=""
    FUSE_PID=""

    cd "$PROJECT_ROOT"
}

cleanup_test_env() {
    log_info "Cleaning up test environment..."

    if mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
        fusermount -uz "$MOUNT_DIR" 2>/dev/null || umount -f "$MOUNT_DIR" 2>/dev/null || true
        sleep 0.5
    fi

    [ -n "$FUSE_PID" ] && kill -TERM "$FUSE_PID" 2>/dev/null || true
    [ -n "$VOLUME_PID" ] && kill -TERM "$VOLUME_PID" 2>/dev/null || true
    [ -n "$MASTER_PID" ] && kill -TERM "$MASTER_PID" 2>/dev/null || true

    pkill -9 -f "powerfs-fuse" 2>/dev/null || true
    pkill -9 -f "powerfs-volume" 2>/dev/null || true
    pkill -9 -f "powerfs master" 2>/dev/null || true

    sleep 1

    rm -rf "$MOUNT_DIR" 2>/dev/null || true
    rm -rf "$MASTER_DIR" 2>/dev/null || true
    rm -rf "$VOLUME_DIR" 2>/dev/null || true

    log_info "Cleanup complete"
}

build_binaries() {
    log_info "Building binaries..."
    cargo build -p powerfs-server -p powerfs-volume -p powerfs-fuse 2>&1 | tail -3
    log_info "Build complete"
}

start_master() {
    log_info "Starting Master server on port $MASTER_PORT..."
    mkdir -p "$MASTER_DIR"

    "$PROJECT_ROOT/target/debug/powerfs" \
        --log-level warn \
        master \
        --port "$MASTER_PORT" \
        --dir "$MASTER_DIR" \
        > /tmp/coherence-test-master.log 2>&1 &
    MASTER_PID=$!

    sleep 3

    if ! kill -0 "$MASTER_PID" 2>/dev/null; then
        log_error "Master failed to start"
        cat /tmp/coherence-test-master.log
        return 1
    fi

    log_info "Master started (PID: $MASTER_PID)"
}

start_volume() {
    log_info "Starting Volume server on port $VOLUME_PORT..."
    mkdir -p "$VOLUME_DIR"

    "$PROJECT_ROOT/target/debug/powerfs-volume" \
        --grpc-address "0.0.0.0:$VOLUME_PORT" \
        --http-port "$VOLUME_HTTP_PORT" \
        --node-id coherence-test-node \
        --master-address "localhost:$MASTER_PORT" \
        --data-dir "$VOLUME_DIR" \
        > /tmp/coherence-test-volume.log 2>&1 &
    VOLUME_PID=$!

    sleep 3

    if ! kill -0 "$VOLUME_PID" 2>/dev/null; then
        log_error "Volume server failed to start"
        cat /tmp/coherence-test-volume.log
        return 1
    fi

    log_info "Volume server started (PID: $VOLUME_PID)"
}

start_fuse() {
    log_info "Starting FUSE mount at $MOUNT_DIR..."
    mkdir -p "$MOUNT_DIR"

    "$PROJECT_ROOT/target/debug/powerfs-fuse" \
        --master "localhost:$MASTER_PORT" \
        --mount-point "$MOUNT_DIR" \
        --collection default \
        --replication 000 \
        > /tmp/coherence-test-fuse.log 2>&1 &
    FUSE_PID=$!

    sleep 4

    if ! kill -0 "$FUSE_PID" 2>/dev/null; then
        log_error "FUSE failed to start"
        cat /tmp/coherence-test-fuse.log
        return 1
    fi

    if ! mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
        log_error "FUSE mount not ready"
        return 1
    fi

    log_info "FUSE started (PID: $FUSE_PID)"
}

start_all_services() {
    start_master
    start_volume
    start_fuse
}

restart_fuse() {
    log_info "Restarting FUSE mount..."

    if mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
        fusermount -uz "$MOUNT_DIR" 2>/dev/null || true
        sleep 0.5
    fi

    [ -n "$FUSE_PID" ] && kill -TERM "$FUSE_PID" 2>/dev/null || true
    sleep 1

    start_fuse
}
