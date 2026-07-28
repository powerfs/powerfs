#!/bin/bash
# Common utilities for PowerFS test scripts
# Source this file: source "$SCRIPT_DIR/lib/common.sh"

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
FAILED_TESTS=()

TEST_START_TIME=""
TEST_NAME=""
TEST_HAS_FAILURES=0

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*" >&2
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"
}

# Test framework functions
test_start() {
    TEST_NAME="$1"
    TEST_START_TIME=$(date +%s%N)
    TEST_HAS_FAILURES=0
    echo ""
    echo "=============================================="
    echo "  TEST: $TEST_NAME"
    echo "=============================================="
}

test_pass() {
    local end_time=$(date +%s%N)
    local duration=$(( (end_time - TEST_START_TIME) / 1000000 ))
    if [ "$TEST_HAS_FAILURES" -eq 0 ]; then
        echo -e "${GREEN}[PASS]${NC} $TEST_NAME (${duration}ms)"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        echo -e "${RED}[FAIL]${NC} $TEST_NAME (${duration}ms) - assertion failures"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        FAILED_TESTS+=("$TEST_NAME: assertion failures")
    fi
}

test_fail() {
    local reason="$1"
    local end_time=$(date +%s%N)
    local duration=$(( (end_time - TEST_START_TIME) / 1000000 ))
    echo -e "${RED}[ASSERT FAIL]${NC} $TEST_NAME ($duration ms)"
    echo "       Reason: $reason"
    TEST_HAS_FAILURES=1
    FAILED_TESTS+=("$TEST_NAME: $reason")
}

test_skip() {
    local reason="$1"
    echo -e "${YELLOW}[SKIP]${NC} $TEST_NAME"
    echo "       Reason: $reason"
    SKIP_COUNT=$((SKIP_COUNT + 1))
}

# Assertion functions (do NOT return non-zero to avoid triggering set -e)
assert_eq() {
    local expected="$1"
    local actual="$2"
    local desc="$3"
    if [ "$expected" != "$actual" ]; then
        test_fail "$desc: expected '$expected', got '$actual'"
    fi
}

assert_file_exists() {
    local path="$1"
    if [ ! -f "$path" ]; then
        test_fail "File does not exist: $path"
    fi
}

assert_file_not_exists() {
    local path="$1"
    if [ -f "$path" ]; then
        test_fail "File should not exist: $path"
    fi
}

assert_dir_exists() {
    local path="$1"
    if [ ! -d "$path" ]; then
        test_fail "Directory does not exist: $path"
    fi
}

assert_dir_not_exists() {
    local path="$1"
    if [ -d "$path" ]; then
        test_fail "Directory should not exist: $path"
    fi
}

assert_file_content() {
    local path="$1"
    local expected="$2"
    local actual
    actual=$(cat "$path" 2>/dev/null || true)
    if [ "$actual" != "$expected" ]; then
        test_fail "Content mismatch for $path: expected '$expected', got '$actual'"
    fi
}

# Print test summary
print_summary() {
    echo ""
    echo "=============================================="
    echo "  TEST SUMMARY"
    echo "=============================================="
    echo -e "  ${GREEN}Passed:  $PASS_COUNT${NC}"
    echo -e "  ${RED}Failed:  $FAIL_COUNT${NC}"
    echo -e "  ${YELLOW}Skipped: $SKIP_COUNT${NC}"
    echo ""
    if [ "$FAIL_COUNT" -gt 0 ]; then
        echo "Failed tests:"
        for failed in "${FAILED_TESTS[@]}"; do
            echo "  - $failed"
        done
        echo ""
        return 1
    fi
    echo -e "${GREEN}All tests passed!${NC}"
    return 0
}

# Environment setup and teardown
setup_test_env() {
    local project_root="${PROJECT_ROOT:-$(cd "$(dirname "$0")/../../.." && pwd)}"
    SCRIPT_DIR="${SCRIPT_DIR:-$project_root}"
    
    MOUNT_DIR="${MOUNT_DIR:-/tmp/powerfs-test}"
    MASTER_DIR="${MASTER_DIR:-/tmp/powerfs-test-master}"
    VOLUME_DIR="${VOLUME_DIR:-/tmp/powerfs-test-volume}"
    FILER_DIR="${FILER_DIR:-/tmp/powerfs-test-filer}"

    # Master ports
    MASTER_PORT="${MASTER_PORT:-9333}"        # gRPC/API port
    MASTER_NET_PORT="${MASTER_NET_PORT:-9334}" # powerfs-net binary protocol port

    # Volume ports
    VOLUME_PORT="${VOLUME_PORT:-8081}"        # gRPC port
    VOLUME_HTTP_PORT="${VOLUME_HTTP_PORT:-8080}" # HTTP port
    VOLUME_NET_PORT="${VOLUME_NET_PORT:-8082}" # powerfs-net binary protocol port

    # Filer ports
    FILER_S3_PORT="${FILER_S3_PORT:-8888}"    # S3 API port
    FILER_GRPC_PORT="${FILER_GRPC_PORT:-8889}" # gRPC port
    FILER_NET_PORT="${FILER_NET_PORT:-8890}"  # powerfs-net binary protocol port

    MASTER_PID=""
    VOLUME_PID=""
    FILER_PID=""
    FUSE_PID=""

    BINARY_PREFIX="${BINARY_PREFIX:-$project_root/target/release}"

    cd "$project_root"
}

cleanup_test_env() {
    log_info "Cleaning up test environment..."

    # Unmount FUSE
    if mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
        fusermount -uz "$MOUNT_DIR" 2>/dev/null || umount -f "$MOUNT_DIR" 2>/dev/null || true
        sleep 0.5
    fi

    # Kill processes
    [ -n "$FUSE_PID" ] && kill -TERM "$FUSE_PID" 2>/dev/null || true
    [ -n "$FILER_PID" ] && kill -TERM "$FILER_PID" 2>/dev/null || true
    [ -n "$VOLUME_PID" ] && kill -TERM "$VOLUME_PID" 2>/dev/null || true
    [ -n "$MASTER_PID" ] && kill -TERM "$MASTER_PID" 2>/dev/null || true

    # Force kill any remaining
    sleep 1
    pkill -9 -f "powerfs-fuse" 2>/dev/null || true
    pkill -9 -f "powerfs-filer" 2>/dev/null || true
    pkill -9 -f "powerfs-volume" 2>/dev/null || true
    pkill -9 -f "powerfs-master" 2>/dev/null || true

    sleep 1

    # Cleanup directories
    rm -rf "$MOUNT_DIR" 2>/dev/null || true
    rm -rf "$MASTER_DIR" 2>/dev/null || true
    rm -rf "$VOLUME_DIR" 2>/dev/null || true
    rm -rf "$FILER_DIR" 2>/dev/null || true

    log_info "Cleanup complete"
}

# Build binaries
build_binaries() {
    local build_type="${1:-release}"
    log_info "Building $build_type binaries..."
    
    if [ "$build_type" = "release" ]; then
        cargo build --release -p powerfs-master -p powerfs-filer -p powerfs-volume -p powerfs-fuse 2>&1 | tail -3
        BINARY_PREFIX="$PROJECT_ROOT/target/release"
    else
        cargo build -p powerfs-master -p powerfs-filer -p powerfs-volume -p powerfs-fuse 2>&1 | tail -3
        BINARY_PREFIX="$PROJECT_ROOT/target/debug"
    fi
    
    log_info "Build complete"
}

# Start individual services
start_master() {
    log_info "Starting Master server on port $MASTER_PORT..."
    mkdir -p "$MASTER_DIR"

    "$BINARY_PREFIX/powerfs-master" \
        --port "$MASTER_PORT" \
        --net-port "$MASTER_NET_PORT" \
        --dir "$MASTER_DIR" \
        > /tmp/powerfs-test-master.log 2>&1 &
    MASTER_PID=$!

    sleep 3

    if ! kill -0 "$MASTER_PID" 2>/dev/null; then
        log_error "Master failed to start"
        cat /tmp/powerfs-test-master.log
        return 1
    fi

    log_info "Master started (PID: $MASTER_PID)"
}

start_volume() {
    log_info "Starting Volume server on port $VOLUME_PORT..."
    mkdir -p "$VOLUME_DIR"

    "$BINARY_PREFIX/powerfs-volume" \
        --grpc-address "0.0.0.0:$VOLUME_PORT" \
        --http-port "$VOLUME_HTTP_PORT" \
        --net-port "$VOLUME_NET_PORT" \
        --node-id test-node \
        --master-address "localhost:$MASTER_PORT" \
        --data-dir "$VOLUME_DIR" \
        > /tmp/powerfs-test-volume.log 2>&1 &
    VOLUME_PID=$!

    sleep 3

    if ! kill -0 "$VOLUME_PID" 2>/dev/null; then
        log_error "Volume server failed to start"
        cat /tmp/powerfs-test-volume.log
        return 1
    fi

    log_info "Volume server started (PID: $VOLUME_PID)"
}

start_filer() {
    log_info "Starting Filer server on port $FILER_NET_PORT..."
    mkdir -p "$FILER_DIR"

    "$BINARY_PREFIX/powerfs-filer" \
        --port "$FILER_S3_PORT" \
        --grpc-port "$FILER_GRPC_PORT" \
        --net-port "$FILER_NET_PORT" \
        --master "localhost:$MASTER_PORT" \
        --data-dir "$FILER_DIR" \
        > /tmp/powerfs-test-filer.log 2>&1 &
    FILER_PID=$!

    sleep 3

    if ! kill -0 "$FILER_PID" 2>/dev/null; then
        log_error "Filer server failed to start"
        cat /tmp/powerfs-test-filer.log
        return 1
    fi

    log_info "Filer server started (PID: $FILER_PID)"
}

start_fuse() {
    log_info "Starting FUSE mount at $MOUNT_DIR..."
    mkdir -p "$MOUNT_DIR"

    "$BINARY_PREFIX/powerfs-fuse" \
        --master "localhost" \
        --master-net-port "$MASTER_NET_PORT" \
        --volume-net-port "$VOLUME_NET_PORT" \
        --filer-addr "localhost" \
        --filer-net-port "$FILER_NET_PORT" \
        --mount-point "$MOUNT_DIR" \
        --collection default \
        --replication 000 \
        > /tmp/powerfs-test-fuse.log 2>&1 &
    FUSE_PID=$!

    sleep 5

    if ! kill -0 "$FUSE_PID" 2>/dev/null; then
        log_error "FUSE failed to start"
        cat /tmp/powerfs-test-fuse.log
        return 1
    fi

    if ! mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
        log_error "FUSE mount not ready"
        return 1
    fi

    # Wait for Filer Raft leader election to complete
    # Check if Filer is ready by trying a simple lookup
    log_info "Waiting for Filer to be ready (Raft leader election)..."
    local retries=0
    while [ $retries -lt 10 ]; do
        if mkdir -p "$MOUNT_DIR/.ready_check" 2>/dev/null; then
            rmdir "$MOUNT_DIR/.ready_check" 2>/dev/null
            log_info "Filer is ready"
            break
        fi
        retries=$((retries + 1))
        sleep 1
    done

    if [ $retries -ge 10 ]; then
        log_warn "Filer may not be fully ready, proceeding anyway"
    fi

    log_info "FUSE started (PID: $FUSE_PID)"
}

start_all_services() {
    start_master
    start_volume
    start_filer
    start_fuse
}

# Restart FUSE only
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

# Check prerequisites
check_prerequisites() {
    local missing=0

    if ! command -v cargo &> /dev/null; then
        log_error "cargo is not installed"
        missing=1
    fi

    if ! command -v fusermount &> /dev/null && ! command -v fusermount3 &> /dev/null; then
        log_error "fusermount is not installed (install fuse3)"
        missing=1
    fi

    if ! command -v fio &> /dev/null; then
        log_warn "fio is not installed (install fio for performance tests)"
    fi

    if [ ! -f "$PROJECT_ROOT/Cargo.toml" ]; then
        log_error "Cargo.toml not found in $PROJECT_ROOT"
        missing=1
    fi

    if [ "$missing" -eq 1 ]; then
        return 1
    fi

    log_success "Prerequisites check passed"
    return 0
}
