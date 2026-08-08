#!/bin/bash
# =============================================================================
# PowerFS P1 Protocol Validation — FUSE Container Test Script
#
# Validates the 6-layer protocol validation (R1-R5) implemented in P1:
#   Layer 1: Frame header invariant check (RX_HDR_INVARIANT)
#   Layer 2: Response size hard limit (RX_TRUNCATE / -E2BIG)
#   Layer 3: Per-msg_type expected size check (RX_SIZE_ANOMALY)
#   Layer 4: TLV required field check (RX_MISSING_FIELD)
#   Layer 5: Buffer truncation detection
#   Layer 6: Diagnostic log specification
#
# Test cases (from docs/file-layout-design.md §10.3.5):
#   T1:  Normal lookup response (< 4KB) — no warnings
#   T2:  Normal getattr response (with chunks) — no warnings
#   T8:  fio sequential read/write 256MB — no regression
#   T9:  fio random read/write 256MB — no regression
#   T10: High-concurrency file creation (100 threads)
#   T-log: Protocol validation log verification
#
# Usage:
#   ./scripts/test_p1_fuse.sh                  # Run all tests
#   ./scripts/test_p1_fuse.sh --quick          # Quick smoke test (smaller IO)
#   ./scripts/test_p1_fuse.sh --log-only       # Only check protocol logs
#   ./scripts/test_p1_fuse.sh --fio-only       # Only run fio tests
#
# Prerequisites:
#   - Test cluster running: ./docker/start_test_env.sh --wait
#   - FUSE mounted at /mnt/fuse inside fuse-test container
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULT_DIR="/tmp/powerfs/test/p1-$(date +%Y%m%d_%H%M%S)"
FUSE_CONTAINER="fuse-test"
FUSE_MOUNT="/mnt/fuse"

QUICK=0
LOG_ONLY=0
FIO_ONLY=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick)     QUICK=1 ;;
        --log-only)  LOG_ONLY=1 ;;
        --fio-only)  FIO_ONLY=1 ;;
        --help|-h)
            echo "Usage: $0 [--quick] [--log-only] [--fio-only]"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

# Color output
if [ -t 1 ]; then
    R='\033[0;31m'; G='\033[0;32m'; Y='\033[0;33m'
    B='\033[0;34m'; C='\033[0;36m'; N='\033[0m'
else
    R=''; G=''; Y=''; B=''; C=''; N=''
fi
log_info()  { echo -e "${B}[INFO]${N}  $(date +%H:%M:%S) $*"; }
log_pass()  { echo -e "${G}[PASS]${N}  $*"; }
log_fail()  { echo -e "${R}[FAIL]${N}  $*"; }
log_error() { echo -e "${R}[ERROR]${N} $*"; }
log_warn()  { echo -e "${Y}[WARN]${N} $*"; }
log_step()  { echo -e "\n${C}━━━ $* ━━━${N}"; }

# Test counters
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

record_pass() { PASS_COUNT=$((PASS_COUNT + 1)); log_pass "$1"; }
record_fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); log_fail "$1"; }
record_skip() { SKIP_COUNT=$((SKIP_COUNT + 1)); log_warn "SKIP: $1"; }

# ========== Pre-flight ==========
preflight() {
    log_step "Pre-flight Checks"

    # Check fuse-test container is running
    if ! docker ps --format '{{.Names}}' | grep -q "^${FUSE_CONTAINER}$"; then
        log_error "Container '$FUSE_CONTAINER' not running"
        log_info "Start it with: ./docker/start_test_env.sh --wait"
        exit 1
    fi
    record_pass "Container $FUSE_CONTAINER running"

    # Check FUSE mount
    if ! docker exec "$FUSE_CONTAINER" sh -c "mount | grep -q $FUSE_MOUNT" 2>/dev/null; then
        log_error "FUSE not mounted at $FUSE_MOUNT inside $FUSE_CONTAINER"
        log_info "Check container logs: docker logs $FUSE_CONTAINER"
        exit 1
    fi
    record_pass "FUSE mounted at $FUSE_MOUNT"

    # Create result directory
    mkdir -p "$RESULT_DIR"
    log_info "Results: $RESULT_DIR"
}

# ========== Helper: exec inside fuse container ==========
fuse_exec() {
    docker exec "$FUSE_CONTAINER" "$@"
}

# ========== Helper: capture fuse logs ==========
capture_fuse_logs() {
    local label="$1"
    docker logs "$FUSE_CONTAINER" > "$RESULT_DIR/fuse_logs_${label}.txt" 2>&1 || true
}

# ========== Helper: check for protocol validation log prefixes ==========
check_log_prefixes() {
    local log_file="$1"
    local label="$2"

    log_info "Checking protocol validation logs ($label)..."

    local found_invariant found_truncate found_anomaly found_missing
    # Note: grep -c returns exit code 1 when count is 0, so we use `|| true` to avoid `|| echo 0` appending a second "0"
    found_invariant=$(grep -c "RX_HDR_INVARIANT" "$log_file" 2>/dev/null || true)
    found_truncate=$(grep -c "RX_TRUNCATE" "$log_file" 2>/dev/null || true)
    found_anomaly=$(grep -c "RX_SIZE_ANOMALY" "$log_file" 2>/dev/null || true)
    found_missing=$(grep -c "RX_MISSING_FIELD" "$log_file" 2>/dev/null || true)
    # Default to 0 if empty (file not found or no matches)
    found_invariant=${found_invariant:-0}
    found_truncate=${found_truncate:-0}
    found_anomaly=${found_anomaly:-0}
    found_missing=${found_missing:-0}

    log_info "  RX_HDR_INVARIANT:  $found_invariant occurrences"
    log_info "  RX_TRUNCATE:       $found_truncate occurrences"
    log_info "  RX_SIZE_ANOMALY:   $found_anomaly occurrences"
    log_info "  RX_MISSING_FIELD:  $found_missing occurrences"

    # Under normal operation, no error-level prefixes should appear.
    # If any appear, it indicates either:
    #   - A real protocol violation (bug)
    #   - A test scenario that deliberately triggered them
    if [ "$found_invariant" -gt 0 ] || [ "$found_truncate" -gt 0 ] || [ "$found_missing" -gt 0 ]; then
        log_warn "Protocol validation errors detected in logs (may be expected if fault injection was run)"
    fi
}

# ========== T1: Normal lookup (< 4KB) ==========
test_t1_normal_lookup() {
    log_step "T1: Normal Lookup Response (< 4KB)"

    # ls triggers lookup operations
    if fuse_exec ls "$FUSE_MOUNT/" > "$RESULT_DIR/t1_ls.txt" 2>&1; then
        record_pass "T1: ls / succeeded"
    else
        record_fail "T1: ls / failed"
        cat "$RESULT_DIR/t1_ls.txt"
        return
    fi

    # stat triggers getattr
    if fuse_exec stat "$FUSE_MOUNT/" > "$RESULT_DIR/t1_stat.txt" 2>&1; then
        record_pass "T1: stat / succeeded"
    else
        record_fail "T1: stat / failed"
        cat "$RESULT_DIR/t1_stat.txt"
        return
    fi

    # Verify no RX_ error logs for normal operations
    capture_fuse_logs "t1"
    local errors
    errors=$(grep -E "RX_HDR_INVARIANT|RX_TRUNCATE|RX_MISSING_FIELD" "$RESULT_DIR/fuse_logs_t1.txt" 2>/dev/null | wc -l)
    if [ "$errors" -eq 0 ]; then
        record_pass "T1: No protocol validation errors for normal lookup"
    else
        record_fail "T1: $errors protocol validation errors detected for normal lookup"
        grep -E "RX_HDR_INVARIANT|RX_TRUNCATE|RX_MISSING_FIELD" "$RESULT_DIR/fuse_logs_t1.txt" | head -5
    fi
}

# ========== T2: Normal getattr with chunks ==========
test_t2_normal_getattr() {
    log_step "T2: Normal Getattr Response (with chunks)"

    # Create a test file with data to populate chunks
    local test_file="$FUSE_MOUNT/t2_test_file.bin"
    local test_size=1048576  # 1MB

    if fuse_exec dd if=/dev/urandom of="$test_file" bs=1M count=1 2>/dev/null; then
        record_pass "T2: Created 1MB test file"
    else
        record_fail "T2: Failed to create test file"
        return
    fi

    # stat should trigger getattr with chunks metadata
    if fuse_exec stat "$test_file" > "$RESULT_DIR/t2_stat.txt" 2>&1; then
        record_pass "T2: stat on file with chunks succeeded"
    else
        record_fail "T2: stat on file with chunks failed"
        cat "$RESULT_DIR/t2_stat.txt"
        fuse_exec rm -f "$test_file"
        return
    fi

    # Read the file to trigger read path
    if fuse_exec dd if="$test_file" of=/dev/null bs=1M count=1 2>/dev/null; then
        record_pass "T2: Read 1MB file with chunks succeeded"
    else
        record_fail "T2: Read 1MB file with chunks failed"
    fi

    # Verify no protocol errors
    capture_fuse_logs "t2"
    local errors
    errors=$(grep -E "RX_HDR_INVARIANT|RX_TRUNCATE|RX_MISSING_FIELD" "$RESULT_DIR/fuse_logs_t2.txt" 2>/dev/null | wc -l)
    if [ "$errors" -eq 0 ]; then
        record_pass "T2: No protocol validation errors for getattr with chunks"
    else
        record_fail "T2: $errors protocol validation errors detected"
    fi

    # Cleanup
    fuse_exec rm -f "$test_file"
}

# ========== T8: fio sequential read/write ==========
test_t8_fio_sequential() {
    log_step "T8: fio Sequential Read/Write"

    local size="256M"
    [ "$QUICK" -eq 1 ] && size="64M"

    local fio_job="$RESULT_DIR/t8_seq.fio"
    cat > "$fio_job" <<EOF
[global]
ioengine=libaio
direct=1
bs=1M
size=$size
rw=rw
rwmixread=70
directory=$FUSE_MOUNT
runtime=30
time_based=1
group_reporting=1
numjobs=1

[seq_rw]
filename=t8_seq_test.bin
EOF

    log_info "Running fio sequential test (size=$size, runtime=30s)..."
    if docker cp "$fio_job" "$FUSE_CONTAINER:/tmp/t8_seq.fio" && \
       fuse_exec fio /tmp/t8_seq.fio > "$RESULT_DIR/t8_fio_output.txt" 2>&1; then
        record_pass "T8: fio sequential test completed"

        # Extract performance metrics
        local read_iops write_iops
        read_iops=$(grep -oP 'read.*?IOPS=\K[\d.]+' "$RESULT_DIR/t8_fio_output.txt" | head -1 || echo "N/A")
        write_iops=$(grep -oP 'write.*?IOPS=\K[\d.]+' "$RESULT_DIR/t8_fio_output.txt" | head -1 || echo "N/A")
        log_info "  Read IOPS:  $read_iops"
        log_info "  Write IOPS: $write_iops"
    else
        record_fail "T8: fio sequential test failed"
        cat "$RESULT_DIR/t8_fio_output.txt" | tail -20
    fi

    # Verify no protocol errors during heavy IO
    capture_fuse_logs "t8"
    local errors
    errors=$(grep -E "RX_HDR_INVARIANT|RX_TRUNCATE|RX_MISSING_FIELD" "$RESULT_DIR/fuse_logs_t8.txt" 2>/dev/null | wc -l)
    if [ "$errors" -eq 0 ]; then
        record_pass "T8: No protocol validation errors during sequential IO"
    else
        record_fail "T8: $errors protocol validation errors during sequential IO"
        grep -E "RX_HDR_INVARIANT|RX_TRUNCATE|RX_MISSING_FIELD" "$RESULT_DIR/fuse_logs_t8.txt" | head -5
    fi

    # Cleanup
    fuse_exec rm -f "$FUSE_MOUNT/t8_seq_test.bin"
}

# ========== T9: fio random read/write ==========
test_t9_fio_random() {
    log_step "T9: fio Random Read/Write"

    local size="256M"
    [ "$QUICK" -eq 1 ] && size="64M"

    local fio_job="$RESULT_DIR/t9_rand.fio"
    cat > "$fio_job" <<EOF
[global]
ioengine=libaio
direct=1
bs=4K
size=$size
rw=randrw
rwmixread=70
directory=$FUSE_MOUNT
runtime=30
time_based=1
group_reporting=1
numjobs=4
iodepth=16

[rand_rw]
filename=t9_rand_test.bin
EOF

    log_info "Running fio random test (size=$size, bs=4K, jobs=4, runtime=30s)..."
    if docker cp "$fio_job" "$FUSE_CONTAINER:/tmp/t9_rand.fio" && \
       fuse_exec fio /tmp/t9_rand.fio > "$RESULT_DIR/t9_fio_output.txt" 2>&1; then
        record_pass "T9: fio random test completed"

        local read_iops write_iops
        read_iops=$(grep -oP 'read.*?IOPS=\K[\d.]+' "$RESULT_DIR/t9_fio_output.txt" | head -1 || echo "N/A")
        write_iops=$(grep -oP 'write.*?IOPS=\K[\d.]+' "$RESULT_DIR/t9_fio_output.txt" | head -1 || echo "N/A")
        log_info "  Read IOPS:  $read_iops"
        log_info "  Write IOPS: $write_iops"
    else
        record_fail "T9: fio random test failed"
        cat "$RESULT_DIR/t9_fio_output.txt" | tail -20
    fi

    # Verify no protocol errors
    capture_fuse_logs "t9"
    local errors
    errors=$(grep -E "RX_HDR_INVARIANT|RX_TRUNCATE|RX_MISSING_FIELD" "$RESULT_DIR/fuse_logs_t9.txt" 2>/dev/null | wc -l)
    if [ "$errors" -eq 0 ]; then
        record_pass "T9: No protocol validation errors during random IO"
    else
        record_fail "T9: $errors protocol validation errors during random IO"
    fi

    # Cleanup
    fuse_exec rm -f "$FUSE_MOUNT/t9_rand_test.bin"
}

# ========== T10: High-concurrency file creation ==========
test_t10_concurrent_create() {
    log_step "T10: High-Concurrency File Creation"

    local num_files=100
    [ "$QUICK" -eq 1 ] && num_files=20

    local test_dir="$FUSE_MOUNT/t10_concurrent"
    fuse_exec mkdir -p "$test_dir"

    log_info "Creating $num_files files concurrently..."
    local script="$RESULT_DIR/t10_create.sh"
    cat > "$script" <<EOF
#!/bin/sh
for i in \$(seq 1 $num_files); do
    echo "file \$i" > "$test_dir/file_\$i.txt" &
done
wait
echo "Created $num_files files"
ls "$test_dir" | wc -l
EOF
    chmod +x "$script"
    docker cp "$script" "$FUSE_CONTAINER:/tmp/t10_create.sh"

    local output
    output=$(fuse_exec sh /tmp/t10_create.sh 2>&1)
    local created_count
    created_count=$(echo "$output" | tail -1)

    if [ "$created_count" -eq "$num_files" ]; then
        record_pass "T10: Created $num_files files concurrently"
    else
        record_fail "T10: Expected $num_files files, got $created_count"
    fi

    # Verify file contents are correct
    local verify_count
    verify_count=$(fuse_exec sh -c "grep -l 'file 1' $test_dir/file_*.txt 2>/dev/null | wc -l")
    if [ "$verify_count" -gt 0 ]; then
        record_pass "T10: File contents verified"
    else
        record_fail "T10: File content verification failed"
    fi

    # Check for protocol errors
    capture_fuse_logs "t10"
    local errors
    errors=$(grep -E "RX_HDR_INVARIANT|RX_TRUNCATE|RX_MISSING_FIELD" "$RESULT_DIR/fuse_logs_t10.txt" 2>/dev/null | wc -l)
    if [ "$errors" -eq 0 ]; then
        record_pass "T10: No protocol validation errors during concurrent create"
    else
        record_fail "T10: $errors protocol validation errors during concurrent create"
    fi

    # Cleanup
    fuse_exec rm -rf "$test_dir"
}

# ========== T-log: Protocol log verification ==========
test_log_verification() {
    log_step "T-log: Protocol Validation Log Verification"

    capture_fuse_logs "final"

    local log_file="$RESULT_DIR/fuse_logs_final.txt"

    # Check that the FUSE client is running with debug logging
    if grep -q "powerfs-fuse" "$log_file" 2>/dev/null; then
        record_pass "FUSE client logs available"
    else
        record_skip "T-log: FUSE client logs not found (may use different log format)"
    fi

    # Verify all 4 log prefixes are defined in the codebase
    log_info "Verifying log prefix definitions in powerfs-net/src/protocol.rs..."
    local proto_file="$PROJECT_ROOT/powerfs-net/src/protocol.rs"
    local prefix_count=0
    for prefix in "RX_HDR_INVARIANT" "RX_TRUNCATE" "RX_SIZE_ANOMALY" "RX_MISSING_FIELD"; do
        if grep -q "LOG_PREFIX_${prefix}" "$proto_file" 2>/dev/null; then
            prefix_count=$((prefix_count + 1))
        fi
    done

    if [ "$prefix_count" -eq 4 ]; then
        record_pass "T-log: All 4 diagnostic log prefixes defined (Layer 6)"
    else
        record_fail "T-log: Only $prefix_count/4 log prefixes defined"
    fi

    # Summary of log occurrences across all test phases
    log_info "Protocol validation log summary (all phases):"
    check_log_prefixes "$log_file" "final"
}

# ========== Summary ==========
show_summary() {
    echo ""
    echo -e "${C}╔══════════════════════════════════════════════════════════╗${N}"
    echo -e "${C}║  P1 Protocol Validation Test Summary                     ${N}"
    echo -e "${C}╚══════════════════════════════════════════════════════════╝${N}"
    echo ""
    echo -e "  ${G}Passed:${N} $PASS_COUNT"
    echo -e "  ${R}Failed:${N} $FAIL_COUNT"
    echo -e "  ${Y}Skipped:${N} $SKIP_COUNT"
    echo ""

    if [ "$FAIL_COUNT" -eq 0 ]; then
        echo -e "  ${G}✓ All tests passed — P1 protocol validation working correctly${N}"
    else
        echo -e "  ${R}✗ $FAIL_COUNT test(s) failed — review logs in $RESULT_DIR${N}"
    fi
    echo ""
    echo "  Detailed results: $RESULT_DIR"
    echo ""

    # Save summary to result dir
    cat > "$RESULT_DIR/summary.txt" <<EOF
P1 Protocol Validation Test Summary
Date: $(date)
Result: PASS=$PASS_COUNT FAIL=$FAIL_COUNT SKIP=$SKIP_COUNT
Result directory: $RESULT_DIR
EOF
}

# ========== Main ==========
main() {
    echo ""
    echo -e "${C}╔══════════════════════════════════════════════════════════╗${N}"
    echo -e "${C}║  PowerFS P1 Protocol Validation — FUSE Test              ${N}"
    echo -e "${C}╚══════════════════════════════════════════════════════════╝${N}"
    echo ""
    echo -e "  ${B}Container:${N}    $FUSE_CONTAINER"
    echo -e "  ${B}Mount point:${N}  $FUSE_MOUNT"
    echo -e "  ${B}Quick mode:${N}   $([ "$QUICK" -eq 1 ] && echo 'yes' || echo 'no')"
    echo -e "  ${B}Result dir:${N}   $RESULT_DIR"
    echo ""

    preflight

    if [ "$LOG_ONLY" -eq 0 ] && [ "$FIO_ONLY" -eq 0 ]; then
        test_t1_normal_lookup
        test_t2_normal_getattr
    fi

    if [ "$LOG_ONLY" -eq 0 ]; then
        test_t8_fio_sequential
        test_t9_fio_random

        if [ "$FIO_ONLY" -eq 0 ]; then
            test_t10_concurrent_create
        fi
    fi

    test_log_verification

    show_summary

    [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
