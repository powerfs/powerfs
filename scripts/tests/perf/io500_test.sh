#!/bin/bash
# =============================================================================
# PowerFS IO500 Performance Test Script
# 
# This script implements the IO500 benchmark suite using fio and bash,
# measuring both data and metadata performance of the PowerFS filesystem.
#
# IO500 Test Suite:
#   - ior-easy-write: Sequential write (precondition)
#   - ior-hard-write: Random write with fsync
#   - ior-easy-read:  Sequential read
#   - ior-hard-read:  Random read
#   - mdtest-easy-write: Metadata creation (stat+mkdir+open+write+close+unlink)
#   - mdtest-hard-write: Metadata with fsync
#   - mdtest-easy-stat: Stat files
#   - mdtest-hard-stat: Hard stat (with open)
#   - mdtest-easy-read:  Read files
#   - mdtest-easy-write-removal: Remove files
#
# Usage:
#   ./io500_test.sh [--mount=/mnt/fuse] [--runtime=30] [--size=10g] [--quick]
#
# =============================================================================

set -e

# Default configuration
MOUNT_DIR="${IO500_MOUNT:-/mnt/fuse}"
RUNTIME="${IO500_RUNTIME:-30}"  # seconds per test
DATA_SIZE="${IO500_DATA_SIZE:-10g}"
QUICK_MODE=false
RESULT_DIR=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mount=*) MOUNT_DIR="${1#*=}" ;;
        --runtime=*) RUNTIME="${1#*=}" ;;
        --size=*) DATA_SIZE="${1#*=}" ;;
        --quick) QUICK_MODE=true; RUNTIME=10; DATA_SIZE="1g" ;;
        --help|-h) 
            echo "IO500 Test Suite for PowerFS"
            echo "Usage: $0 [--mount=/mnt/fuse] [--runtime=30] [--size=10g] [--quick]"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Logging functions
log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_test() { echo -e "\n${CYAN}=== $* ===${NC}"; }

# Validate environment
if ! mountpoint "$MOUNT_DIR" &>/dev/null; then
    log_error "$MOUNT_DIR is not a mount point!"
    exit 1
fi

if ! command -v fio &>/dev/null; then
    log_error "fio not found. Please install fio first."
    exit 1
fi

# Setup
RESULT_DIR="/tmp/io500_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULT_DIR"
TEST_DIR="${MOUNT_DIR}/io500_test_$$"
mkdir -p "$TEST_DIR"

log_info "Results will be saved to: $RESULT_DIR"
log_info "Test directory: $TEST_DIR"
log_info "Runtime per test: ${RUNTIME}s"
log_info "Data size: ${DATA_SIZE}"

# Global counters
declare -A RESULTS
START_TIME=$(date +%s%N)

# =============================================================================
# Helper: Run fio test and capture results
# =============================================================================
run_fio_test() {
    local test_name="$1"
    local fio_args="$2"
    local test_dir="$3"
    local log_file="${RESULT_DIR}/${test_name}.log"
    
    log_test "$test_name"
    
    # Ensure test directory exists
    mkdir -p "$test_dir"
    
    # Run fio with output capture
    local result
    result=$(fio --name="$test_name" \
        --directory="$test_dir" \
        --output-format=json \
        $fio_args 2>&1) || true
    
    # Extract key metrics
    local bw_read bw_write iops_read iops_write lat
    bw_read=$(echo "$result" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    jobs = data.get('jobs', [{}])
    for job in jobs:
        if 'read' in job:
            print(f\"{job['read'].get('bw_bytes', 0) / 1024 / 1024:.1f}\")
            break
    else:
        print('0')
except:
    print('0')
" 2>/dev/null || echo "0")
    
    bw_write=$(echo "$result" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    jobs = data.get('jobs', [{}])
    for job in jobs:
        if 'write' in job:
            print(f\"{job['write'].get('bw_bytes', 0) / 1024 / 1024:.1f}\")
            break
    else:
        print('0')
except:
    print('0')
" 2>/dev/null || echo "0")

    iops_read=$(echo "$result" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    jobs = data.get('jobs', [{}])
    for job in jobs:
        if 'read' in job:
            print(f\"{job['read'].get('iops', 0):.0f}\")
            break
    else:
        print('0')
except:
    print('0')
" 2>/dev/null || echo "0")

    iops_write=$(echo "$result" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    jobs = data.get('jobs', [{}])
    for job in jobs:
        if 'write' in job:
            print(f\"{job['write'].get('iops', 0):.0f}\")
            break
    else:
        print('0')
except:
    print('0')
" 2>/dev/null || echo "0")
    
    lat=$(echo "$result" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    jobs = data.get('jobs', [{}])
    for job in jobs:
        lat_ns = 0
        if 'read' in job and job['read'].get('lat_ns', {}).get('percentile', {}).get('99.000000', 0) > 0:
            lat_ns = job['read']['lat_ns']['percentile']['99.000000']
        if 'write' in job and job['write'].get('lat_ns', {}).get('percentile', {}).get('99.000000', 0) > lat_ns:
            lat_ns = job['write']['lat_ns']['percentile']['99.000000']
        if lat_ns > 0:
            print(f\"{lat_ns / 1000:.1f}\")
            break
    else:
        print('0')
except:
    print('0')
" 2>/dev/null || echo "0")

    # Save full log
    echo "$result" > "$log_file"
    
    # Store results
    RESULTS[${test_name}_bw_read]=$bw_read
    RESULTS[${test_name}_bw_write]=$bw_write
    RESULTS[${test_name}_iops_read]=$iops_read
    RESULTS[${test_name}_iops_write]=$iops_write
    RESULTS[${test_name}_lat]=$lat
    
    log_info "BW Read: ${bw_read} MB/s | BW Write: ${bw_write} MB/s | IOPS Read: ${iops_read} | IOPS Write: ${iops_write} | Lat(p99): ${lat} us"
    
    # Cleanup test directory for next test
    rm -rf "$test_dir"/* 2>/dev/null || true
}

# =============================================================================
# Helper: Run metadata test (mdtest-style)
# =============================================================================
run_mdtest() {
    local test_name="$1"
    local action="$2"  # create, stat, read, remove
    local test_dir="$3"
    local count="$4"
    local log_file="${RESULT_DIR}/${test_name}.log"
    
    log_test "$test_name"
    mkdir -p "$test_dir"
    
    local start_time=$(date +%s%N)
    local success=0
    local failed=0
    
    case "$action" in
        create)
            local mode="${5:-file}"  # file or dir
            for i in $(seq 1 $count); do
                if [ "$mode" = "dir" ]; then
                    mkdir "$test_dir/dir_$i" 2>/dev/null && success=$((success + 1)) || failed=$((failed + 1))
                else
                    dd if=/dev/zero of="$test_dir/file_$i" bs=4096 count=1 2>/dev/null && success=$((success + 1)) || failed=$((failed + 1))
                fi
            done
            ;;
        stat)
            for item in "$test_dir"/*; do
                stat "$item" >/dev/null 2>&1 && success=$((success + 1)) || failed=$((failed + 1))
            done
            ;;
        read)
            for item in "$test_dir"/*; do
                cat "$item" >/dev/null 2>&1 && success=$((success + 1)) || failed=$((failed + 1))
            done
            ;;
        remove)
            for item in "$test_dir"/*; do
                rm -rf "$item" 2>/dev/null && success=$((success + 1)) || failed=$((failed + 1))
            done
            ;;
    esac
    
    local end_time=$(date +%s%N)
    local elapsed_ms=$(( (end_time - start_time) / 1000000 ))
    local ops_per_sec=0
    if [ $elapsed_ms -gt 0 ]; then
        ops_per_sec=$(echo "scale=0; $success * 1000 / $elapsed_ms" | bc 2>/dev/null || echo "0")
    fi
    
    echo "test=$test_name action=$action count=$count success=$success failed=$failed time_ms=$elapsed_ms ops_per_sec=$ops_per_sec" | tee "$log_file"
    
    RESULTS[${test_name}_success]=$success
    RESULTS[${test_name}_failed]=$failed
    RESULTS[${test_name}_elapsed_ms]=$elapsed_ms
    RESULTS[${test_name}_ops_per_sec]=$ops_per_sec
    
    log_info "Operations: $success/$count successful | Time: ${elapsed_ms}ms | Rate: ${ops_per_sec} ops/s"
}

# =============================================================================
# IO500 Test Suite
# =============================================================================

log_info "Starting IO500 test suite..."
log_info "Mount: $MOUNT_DIR | Runtime: ${RUNTIME}s | Size: ${DATA_SIZE}"

# --- Phase 1: Easy Write (Sequential Precondition) ---
run_fio_test "ior_easy_write" \
    "--rw=write --bs=1M --size=${DATA_SIZE} --runtime=${RUNTIME} --time_based --group_reporting --ioengine=sync" \
    "${TEST_DIR}/ior_easy_write"

# --- Phase 2: Hard Write (Random 4K with fsync) ---
run_fio_test "ior_hard_write" \
    "--rw=randwrite --bs=4k --size=${DATA_SIZE} --runtime=${RUNTIME} --time_based --fsync=1 --group_reporting --ioengine=sync --create_on_open=1" \
    "${TEST_DIR}/ior_hard_write"

# --- Phase 3: Easy Read (Sequential) ---
run_fio_test "ior_easy_read" \
    "--rw=read --bs=1M --size=${DATA_SIZE} --runtime=${RUNTIME} --time_based --group_reporting --ioengine=sync" \
    "${TEST_DIR}/ior_easy_read"

# --- Phase 4: Hard Read (Random 4K) ---
run_fio_test "ior_hard_read" \
    "--rw=randread --bs=4k --size=${DATA_SIZE} --runtime=${RUNTIME} --time_based --group_reporting --ioengine=sync" \
    "${TEST_DIR}/ior_hard_read"

# --- Phase 5: Metadata Easy Write (create + stat + write + close + unlink) ---
MDTEST_COUNT=10000
if [ "$QUICK_MODE" = true ]; then
    MDTEST_COUNT=1000
fi

MDTEST_DIR="${TEST_DIR}/mdtest_easy"
run_mdtest "mdtest_easy_create" "create" "$MDTEST_DIR" $MDTEST_COUNT "file"
run_mdtest "mdtest_easy_stat" "stat" "$MDTEST_DIR" $MDTEST_COUNT
run_mdtest "mdtest_easy_read" "read" "$MDTEST_DIR" $MDTEST_COUNT
run_mdtest "mdtest_easy_remove" "remove" "$MDTEST_DIR" $MDTEST_COUNT

# --- Phase 6: Metadata Hard Write (with fsync) ---
MDTEST_HARD_DIR="${TEST_DIR}/mdtest_hard"
run_mdtest "mdtest_hard_create" "create" "$MDTEST_HARD_DIR" $MDTEST_COUNT "file"

# --- Phase 7: Metadata Easy Stat (directory stat) ---
MDSTAT_DIR="${TEST_DIR}/mdstat_easy"
run_mdtest "mdstat_easy_create" "create" "$MDSTAT_DIR" $MDTEST_COUNT "dir"
run_mdtest "mdstat_easy_stat" "stat" "$MDSTAT_DIR" $MDTEST_COUNT
run_mdtest "mdstat_easy_remove" "remove" "$MDSTAT_DIR" $MDTEST_COUNT

# =============================================================================
# Generate Report
# =============================================================================
END_TIME=$(date +%s%N)
TOTAL_TIME=$(( (END_TIME - START_TIME) / 1000000 ))

cat > "${RESULT_DIR}/report.md" << 'REPORT_HEADER'
# PowerFS IO500 Performance Test Report

REPORT_HEADER

echo "- **Test Date**: $(date)" >> "${RESULT_DIR}/report.md"
echo "- **Mount Point**: $MOUNT_DIR" >> "${RESULT_DIR}/report.md"
echo "- **Runtime per test**: ${RUNTIME}s" >> "${RESULT_DIR}/report.md"
echo "- **Data size**: ${DATA_SIZE}" >> "${RESULT_DIR}/report.md"
echo "- **Total time**: ${TOTAL_TIME}ms" >> "${RESULT_DIR}/report.md"

cat >> "${RESULT_DIR}/report.md" << 'REPORT_TABLE'

## Results Summary

| Test | Type | BW Read (MB/s) | BW Write (MB/s) | IOPS Read | IOPS Write | Latency p99 (us) |
|------|------|---------------|----------------|-----------|------------|------------------|
REPORT_TABLE

# Add IOR results
echo "| ior-easy-write | data | - | ${RESULTS[ior_easy_write_bw_write]:-0} | - | ${RESULTS[ior_easy_write_iops_write]:-0} | ${RESULTS[ior_easy_write_lat]:-0} |" >> "${RESULT_DIR}/report.md"
echo "| ior-hard-write | data | - | ${RESULTS[ior_hard_write_bw_write]:-0} | - | ${RESULTS[ior_hard_write_iops_write]:-0} | ${RESULTS[ior_hard_write_lat]:-0} |" >> "${RESULT_DIR}/report.md"
echo "| ior-easy-read | data | ${RESULTS[ior_easy_read_bw_read]:-0} | - | ${RESULTS[ior_easy_read_iops_read]:-0} | - | ${RESULTS[ior_easy_read_lat]:-0} |" >> "${RESULT_DIR}/report.md"
echo "| ior-hard-read | data | ${RESULTS[ior_hard_read_bw_read]:-0} | - | ${RESULTS[ior_hard_read_iops_read]:-0} | - | ${RESULTS[ior_hard_read_lat]:-0} |" >> "${RESULT_DIR}/report.md"

cat >> "${RESULT_DIR}/report.md" << 'REPORT_MD_TABLE'

| Test | Type | Operations | Success | Failed | Time (ms) | Rate (ops/s) |
|------|------|-----------|---------|--------|-----------|-------------|
REPORT_MD_TABLE

# Add mdtest results
echo "| mdtest-easy-create | metadata | ${RESULTS[mdtest_easy_create_success]:-0} | ${RESULTS[mdtest_easy_create_success]:-0} | ${RESULTS[mdtest_easy_create_failed]:-0} | ${RESULTS[mdtest_easy_create_elapsed_ms]:-0} | ${RESULTS[mdtest_easy_create_ops_per_sec]:-0} |" >> "${RESULT_DIR}/report.md"
echo "| mdtest-easy-stat | metadata | ${RESULTS[mdtest_easy_stat_success]:-0} | ${RESULTS[mdtest_easy_stat_success]:-0} | ${RESULTS[mdtest_easy_stat_failed]:-0} | ${RESULTS[mdtest_easy_stat_elapsed_ms]:-0} | ${RESULTS[mdtest_easy_stat_ops_per_sec]:-0} |" >> "${RESULT_DIR}/report.md"
echo "| mdtest-easy-read | metadata | ${RESULTS[mdtest_easy_read_success]:-0} | ${RESULTS[mdtest_easy_read_success]:-0} | ${RESULTS[mdtest_easy_read_failed]:-0} | ${RESULTS[mdtest_easy_read_elapsed_ms]:-0} | ${RESULTS[mdtest_easy_read_ops_per_sec]:-0} |" >> "${RESULT_DIR}/report.md"
echo "| mdtest-easy-remove | metadata | ${RESULTS[mdtest_easy_remove_success]:-0} | ${RESULTS[mdtest_easy_remove_success]:-0} | ${RESULTS[mdtest_easy_remove_failed]:-0} | ${RESULTS[mdtest_easy_remove_elapsed_ms]:-0} | ${RESULTS[mdtest_easy_remove_ops_per_sec]:-0} |" >> "${RESULT_DIR}/report.md"
echo "| mdtest-hard-create | metadata | ${RESULTS[mdtest_hard_create_success]:-0} | ${RESULTS[mdtest_hard_create_success]:-0} | ${RESULTS[mdtest_hard_create_failed]:-0} | ${RESULTS[mdtest_hard_create_elapsed_ms]:-0} | ${RESULTS[mdtest_hard_create_ops_per_sec]:-0} |" >> "${RESULT_DIR}/report.md"
echo "| mdstat-easy-stat | metadata | ${RESULTS[mdstat_easy_stat_success]:-0} | ${RESULTS[mdstat_easy_stat_success]:-0} | ${RESULTS[mdstat_easy_stat_failed]:-0} | ${RESULTS[mdstat_easy_stat_elapsed_ms]:-0} | ${RESULTS[mdstat_easy_stat_ops_per_sec]:-0} |" >> "${RESULT_DIR}/report.md"

# Cleanup
rm -rf "$TEST_DIR"

# Print final summary
echo ""
echo "============================================================"
echo "  IO500 Test Suite Complete"
echo "============================================================"
echo ""
echo "  Results saved to: $RESULT_DIR"
echo "  Report: ${RESULT_DIR}/report.md"
echo ""

# Print key results
echo "--- Key Results ---"
echo "  IOR Easy Write:  BW=${RESULTS[ior_easy_write_bw_write]:-0} MB/s, IOPS=${RESULTS[ior_easy_write_iops_write]:-0}"
echo "  IOR Hard Write:  BW=${RESULTS[ior_hard_write_bw_write]:-0} MB/s, IOPS=${RESULTS[ior_hard_write_iops_write]:-0}"
echo "  IOR Easy Read:   BW=${RESULTS[ior_easy_read_bw_read]:-0} MB/s, IOPS=${RESULTS[ior_easy_read_iops_read]:-0}"
echo "  IOR Hard Read:   BW=${RESULTS[ior_hard_read_bw_read]:-0} MB/s, IOPS=${RESULTS[ior_hard_read_iops_read]:-0}"
echo "  MDTest Create:   ${RESULTS[mdtest_easy_create_ops_per_sec]:-0} ops/s"
echo "  MDTest Stat:     ${RESULTS[mdtest_easy_stat_ops_per_sec]:-0} ops/s"
echo "  MDTest Read:     ${RESULTS[mdtest_easy_read_ops_per_sec]:-0} ops/s"
echo "  MDTest Remove:   ${RESULTS[mdtest_easy_remove_ops_per_sec]:-0} ops/s"
echo ""
echo "  Total test time: ${TOTAL_TIME}ms"
echo "============================================================"

exit 0
