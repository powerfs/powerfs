#!/bin/bash
# Master Failover End-to-End Test Script
#
# Verifies all failover fixes across a complete outage-recovery cycle:
#   1. Epoch mechanism: epoch increments on restart, old leases invalidated
#   2. Lease renewal: new leases acquired after restart are renewable
#   3. JOB_COMPLETE batch invalidation: notification reaches clients via gRPC stream
#   4. Generation tracking: metadata changes publish incrementing generation numbers
#
# Usage:
#   ./run_failover_e2e.sh                      # Run all tests
#   ./run_failover_e2e.sh --clean              # Clean up before running
#   ./run_failover_e2e.sh --check              # Only check prerequisites
#   ./run_failover_e2e.sh --stop               # Stop any running tests and exit

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

LOG_FILE="$PROJECT_ROOT/target/failover_e2e_test.log"
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
FAILED_TESTS=()

print_banner() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║  PowerFS Master Failover End-to-End Test Suite                      ║"
    echo "║  Verifies: Epoch | Lease Renewal | JOB_COMPLETE | Generation        ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝"
    echo ""
}

log_info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"
}

log_warn() {
    echo "[WARN] $(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"
}

log_error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE" >&2
}

test_start() {
    TEST_NAME="$1"
    TEST_START_TIME=$(date +%s%N)
    echo ""
    echo "──────────────────────────────────────────────────────────────────────"
    echo "  TEST: $TEST_NAME"
    echo "──────────────────────────────────────────────────────────────────────" | tee -a "$LOG_FILE"
}

test_pass() {
    local end_time=$(date +%s%N)
    local duration=$(( (end_time - TEST_START_TIME) / 1000000 ))
    echo "[PASS] $TEST_NAME (${duration}ms)" | tee -a "$LOG_FILE"
    PASS_COUNT=$((PASS_COUNT + 1))
}

test_fail() {
    local reason="$1"
    local end_time=$(date +%s%N)
    local duration=$(( (end_time - TEST_START_TIME) / 1000000 ))
    echo "[FAIL] $TEST_NAME (${duration}ms)" | tee -a "$LOG_FILE"
    echo "       Reason: $reason" | tee -a "$LOG_FILE"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED_TESTS+=("$TEST_NAME: $reason")
}

test_skip() {
    local reason="$1"
    echo "[SKIP] $TEST_NAME" | tee -a "$LOG_FILE"
    echo "       Reason: $reason" | tee -a "$LOG_FILE"
    SKIP_COUNT=$((SKIP_COUNT + 1))
}

print_summary() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║  FAILOVER E2E TEST SUMMARY                                          ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    echo "  Passed:  $PASS_COUNT" | tee -a "$LOG_FILE"
    echo "  Failed:  $FAIL_COUNT" | tee -a "$LOG_FILE"
    echo "  Skipped: $SKIP_COUNT" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    
    if [ "$FAIL_COUNT" -gt 0 ]; then
        echo "Failed tests:" | tee -a "$LOG_FILE"
        for failed in "${FAILED_TESTS[@]}"; do
            echo "  - $failed" | tee -a "$LOG_FILE"
        done
        echo "" | tee -a "$LOG_FILE"
        return 1
    fi
    
    echo "✓ All failover tests passed!" | tee -a "$LOG_FILE"
    echo "✓ Epoch mechanism: Working correctly" | tee -a "$LOG_FILE"
    echo "✓ Lease renewal: Working correctly" | tee -a "$LOG_FILE"
    echo "✓ JOB_COMPLETE: Working correctly" | tee -a "$LOG_FILE"
    echo "✓ Generation tracking: Working correctly" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    return 0
}

cleanup_leftover_processes() {
    echo ""
    echo "=== Cleaning up leftover processes ===" | tee -a "$LOG_FILE"
    
    local leftover_count=0
    
    for proc in "cargo test" "rustc" "sync_test" "coherence_phase" "master_outage"; do
        pids=$(pgrep -f "$proc" 2>/dev/null)
        for pid in $pids; do
            echo "Killing leftover process $pid ($proc)" | tee -a "$LOG_FILE"
            kill -9 "$pid" 2>/dev/null || true
            leftover_count=$((leftover_count + 1))
        done
    done
    
    if [ $leftover_count -eq 0 ]; then
        echo "No leftover processes found" | tee -a "$LOG_FILE"
    else
        echo "Killed $leftover_count leftover processes" | tee -a "$LOG_FILE"
        sleep 1
    fi
}

check_prerequisites() {
    echo "=== Checking prerequisites ===" | tee -a "$LOG_FILE"
    
    if ! command -v cargo &> /dev/null; then
        log_error "cargo is not installed"
        return 1
    fi
    echo "  ✓ cargo is installed" | tee -a "$LOG_FILE"
    
    if [ ! -f "$PROJECT_ROOT/Cargo.toml" ]; then
        log_error "Cargo.toml not found in project root"
        return 1
    fi
    echo "  ✓ Project root found" | tee -a "$LOG_FILE"
    
    echo "" | tee -a "$LOG_FILE"
    return 0
}

run_test() {
    local test_name="$1"
    local test_filter="$2"
    
    test_start "$test_name"
    
    if ! cargo test --manifest-path "$PROJECT_ROOT/powerfs-master/Cargo.toml" \
        --test master_outage_e2e_test \
        "$test_filter" \
        -- --test-threads=1 --nocapture 2>&1 | tee -a "$LOG_FILE"; then
        test_fail "Test execution failed"
        return 1
    fi
    
    test_pass
    return 0
}

main() {
    mkdir -p "$PROJECT_ROOT/target"
    echo "=== PowerFS Failover E2E Test - $(date '+%Y-%m-%d %H:%M:%S') ===" > "$LOG_FILE"
    
    print_banner
    
    if [ "$1" = "--clean" ]; then
        cleanup_leftover_processes
        echo ""
    elif [ "$1" = "--stop" ]; then
        cleanup_leftover_processes
        echo "Stopped"
        exit 0
    elif [ "$1" = "--check" ]; then
        check_prerequisites
        exit $?
    fi
    
    if ! check_prerequisites; then
        exit 1
    fi
    
    cleanup_leftover_processes
    
    echo ""
    echo "══════════════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
    echo "  Running Master Outage End-to-End Tests" | tee -a "$LOG_FILE"
    echo "══════════════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
    
    run_test "Epoch increments on restart" "test_master_outage_epoch_increments_on_restart"
    run_test "Old lease invalidated by epoch mismatch" "test_master_outage_old_lease_invalidated_by_epoch"
    run_test "New lease renewable after restart" "test_master_outage_new_lease_renewable_after_restart"
    run_test "Multiple restarts epoch stability" "test_multiple_master_restarts_epoch_stability"
    
    echo ""
    echo "══════════════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
    echo "  Running gRPC Wire-Level Tests" | tee -a "$LOG_FILE"
    echo "══════════════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
    
    run_test "Lease lifecycle via gRPC" "test_lease_lifecycle_via_grpc"
    run_test "JOB_COMPLETE notification via gRPC" "test_job_complete_notification_via_grpc"
    run_test "Complete non-existent job returns error" "test_complete_nonexistent_job_returns_error"
    run_test "Generation increments on metadata changes" "test_generation_increments_on_metadata_changes"
    
    cleanup_leftover_processes
    
    print_summary
    exit $?
}

main "$@"
