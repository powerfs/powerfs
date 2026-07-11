#!/bin/bash

set -e

BUILD_IMAGES=false
TEST_SUITE="all"
VERBOSE_LOG=false
SKIP_CLEANUP=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build|-b)
            BUILD_IMAGES=true
            shift
            ;;
        --suite|-s)
            TEST_SUITE="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE_LOG=true
            shift
            ;;
        --skip-cleanup)
            SKIP_CLEANUP=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

DOCKER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROJECT_DIR=$(cd "$DOCKER_DIR/.." && pwd)
HOST_IP=$(hostname -I | awk '{print $1}')
START_TIME=$(date +%s)

log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $1"
}

log_warn() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] $1"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $1"
}

log_debug() {
    if [ "$VERBOSE_LOG" = true ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [DEBUG] $1"
    fi
}

log_info "========================================"
log_info "    PowerFS Docker Test Environment"
log_info "========================================"
log_info ""
log_info "Configuration:"
log_info "  Host IP:         $HOST_IP"
log_info "  Build images:    $BUILD_IMAGES"
log_info "  Test suite:      $TEST_SUITE"
log_info "  Verbose mode:    $VERBOSE_LOG"
log_info "  Docker dir:      $DOCKER_DIR"
log_info "  Project dir:     $PROJECT_DIR"
log_info ""

cleanup() {
    if [ "$SKIP_CLEANUP" = true ]; then
        log_info "Skipping cleanup (--skip-cleanup)"
        return
    fi
    
    log_info "Cleaning up test environment..."
    
    log_debug "Stopping containers..."
    docker compose -f "$DOCKER_DIR/docker-compose.test.yml" down --remove-orphans 2>/dev/null || true
    
    log_info "Cleanup completed"
}

trap cleanup EXIT

log_info "[PRE-CHECK] Performing environment pre-checks..."

if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed or not in PATH"
    exit 1
fi
log_info "  [OK] Docker is available"

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    log_error "Docker Compose is not available"
    exit 1
fi
log_info "  [OK] Docker Compose is available"

log_info "[PRE-CHECK] Pre-checks completed"
log_info ""

if [ "$BUILD_IMAGES" = true ]; then
    log_info "[1/4] Building Docker images..."
    cd "$DOCKER_DIR"
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

    log_info "  Building production image..."
    cd "$PROJECT_DIR"
    if cargo build --release --bin powerfs --bin powerfs-volume --bin powerfs-monitor --bin powerfs-fuse 2>&1 | tail -5; then
        log_info "  [OK] Rust binaries built successfully"
    else
        log_error "  [FAIL] Failed to build Rust binaries"
        exit 2
    fi

    cd "$DOCKER_DIR"
    if docker compose -f "$DOCKER_DIR/docker-compose.test.yml" build 2>&1 | tail -5; then
        log_info "[OK] Docker images built successfully"
    else
        log_error "[FAIL] Failed to build Docker images"
        exit 2
    fi
else
    log_info "[1/4] Using existing Docker images..."
    log_info "  Use --build or -b flag to rebuild images"
fi

log_info ""
log_info "[2/4] Starting test infrastructure..."
log_debug "Running: docker compose -f $DOCKER_DIR/docker-compose.test.yml up -d"

if docker compose -f "$DOCKER_DIR/docker-compose.test.yml" up -d; then
    log_info "  [OK] All containers started"
else
    log_error "  [FAIL] Failed to start containers"
    exit 2
fi

log_info "  Waiting for services to be ready..."
timeout=60
attempt=0
while [ $timeout -gt 0 ]; do
    attempt=$((attempt + 1))
    
    REDIS_READY=$(docker exec powerfs-test-redis redis-cli ping 2>/dev/null | grep -q PONG && echo "true" || echo "false")
    MASTER_READY=$(nc -z localhost 9333 2>/dev/null && echo "true" || echo "false")
    VOLUME_READY=$(nc -z localhost 8080 2>/dev/null && echo "true" || echo "false")
    
    log_debug "  Attempt $attempt: Redis=$REDIS_READY, Master=$MASTER_READY, Volume=$VOLUME_READY"
    
    if [ "$REDIS_READY" = "true" ] && [ "$MASTER_READY" = "true" ] && [ "$VOLUME_READY" = "true" ]; then
        log_info "  [OK] All infrastructure services ready after $attempt attempts"
        break
    fi
    
    if [ $((attempt % 10)) -eq 0 ]; then
        log_warn "  Services not ready yet (attempt $attempt/$timeout)"
    fi
    
    sleep 1
    timeout=$((timeout - 1))
done

if [ $timeout -eq 0 ]; then
    log_error "  [ERROR] Infrastructure failed to start within timeout"
    log_error "  Check logs: docker logs powerfs-test-master"
    exit 2
fi

log_info "  Waiting for FUSE to mount..."
sleep 5

if docker exec powerfs-test-fuse mount | grep -q "powerfs on /tmp/powerfs-test/mount"; then
    log_info "  [OK] FUSE is mounted"
else
    log_warn "  [WARN] FUSE may not be mounted properly"
    log_warn "  Check: docker logs powerfs-test-fuse"
fi

log_info ""
log_info "[3/4] Running tests inside FUSE container..."

TEST_RESULTS=()

run_test() {
    local test_name="$1"
    local test_args="$2"
    
    log_info "  Running $test_name..."
    log_debug "  Args: $test_args"
    
    START_TIME=$(date +%s)
    
    if docker exec powerfs-test-fuse bash -c "cd /app && POWERFS_DOCKER_TEST=1 POWERFS_MOUNT=/tmp/powerfs-test/mount cargo test $test_args -- --test-threads=1 2>&1" | tail -5; then
        END_TIME=$(date +%s)
        ELAPSED=$((END_TIME - START_TIME))
        log_info "  [OK] $test_name passed ($ELAPSED seconds)"
        TEST_RESULTS+=("$test_name: PASS")
        return 0
    else
        END_TIME=$(date +%s)
        ELAPSED=$((END_TIME - START_TIME))
        log_error "  [FAIL] $test_name failed ($ELAPSED seconds)"
        TEST_RESULTS+=("$test_name: FAIL")
        return 1
    fi
}

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "rfs" ]; then
    run_test "RFS Tester Tests" "--manifest-path powerfs-fuse/Cargo.toml --test rfs_tester_fuse_test"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "posix" ]; then
    run_test "POSIX Tests" "--manifest-path powerfs-fuse/Cargo.toml --test posix_tests"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "concurrent" ]; then
    run_test "Concurrent Consistency Tests" "--manifest-path powerfs-fuse/Cargo.toml --test concurrent_consistency"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "master" ]; then
    run_test "Master Integration Tests" "--manifest-path powerfs-master/Cargo.toml --test rfs_tester_integration"
fi

if [ "$TEST_SUITE" = "all" ] || [ "$TEST_SUITE" = "volume" ]; then
    run_test "Volume Integration Tests" "--manifest-path powerfs-fuse/Cargo.toml --test volume_integration_test"
fi

log_info ""
log_info "[4/4] Test Results Summary"
log_info "============================="

PASS_COUNT=0
FAIL_COUNT=0

for result in "${TEST_RESULTS[@]}"; do
    log_info "  $result"
    if echo "$result" | grep -q "PASS"; then
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
done

log_info ""
log_info "  Total: ${#TEST_RESULTS[@]} tests"
log_info "  Passed: $PASS_COUNT"
log_info "  Failed: $FAIL_COUNT"
log_info ""

END_TIME=$(date +%s)
ELAPSED_TIME=$((END_TIME - START_TIME))

log_info "Test run completed in $ELAPSED_TIME seconds"

if [ $FAIL_COUNT -gt 0 ]; then
    log_error "Some tests failed. Check logs for details."
    exit 1
else
    log_info "All tests passed!"
    exit 0
fi