#!/bin/bash
# PowerFS Performance Benchmark Tests
# Uses fio for comprehensive performance testing

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

source "$PROJECT_ROOT/scripts/lib/common.sh"

# Configuration
IO_ENGINE="${IO_ENGINE:-sync}"
FIO_FSYNC="${FIO_FSYNC:-0}"
BUILD_FIRST="${BUILD_FIRST:-true}"

print_usage() {
    cat << EOF
PowerFS Performance Benchmark

Usage: $0 [OPTIONS]

Options:
  --engine=ENGINE   IO engine: sync (default), libaio, io_uring
  --fsync=N         fsync interval (0=disabled, 1=every IO)
  --no-build        Skip building binaries
  --help            Show this help message

EOF
}

parse_args() {
    for arg in "$@"; do
        case "$arg" in
            --engine=*) IO_ENGINE="${arg#*=}" ;;
            --fsync=*) FIO_FSYNC="${arg#*=}" ;;
            --no-build) BUILD_FIRST=false ;;
            --help|-h) print_usage; exit 0 ;;
            *) log_error "Unknown option: $1"; print_usage; exit 1 ;;
        esac
    done
}

# Test functions
run_fio_test() {
    local test_name="$1"
    local test_desc="$2"
    local rw="$3"
    local bs="$4"
    local size="$5"
    local numjobs="$6"
    local fsync_val="$7"
    
    echo ""
    echo "=== Test: $test_name ==="
    echo "  Desc: $test_desc"
    echo "  Engine: $IO_ENGINE | fsync: $fsync_val | Jobs: $numjobs | BS: $bs"
    
    local fsync_opt=""
    if [ "$fsync_val" != "0" ]; then
        fsync_opt="--fsync=$fsync_val"
    fi
    
    fio --name="$test_name" \
        --ioengine="$IO_ENGINE" \
        --rw="$rw" \
        --bs="$bs" \
        --size="$size" \
        $fsync_opt \
        --directory="$MOUNT_DIR" \
        --numjobs="$numjobs" \
        --group_reporting
}

run_bench_tests() {
    echo ""
    echo "============================================================"
    echo "  PowerFS Performance Benchmark"
    echo "  Engine: $IO_ENGINE | fsync: $FIO_FSYNC"
    echo "============================================================"
    echo ""
    
    # Sequential write
    run_fio_test "seq_write" \
        "Sequential Write (1MB block)" \
        "write" "1M" "100M" "1" "$FIO_FSYNC"
    
    # Sequential read
    run_fio_test "seq_read" \
        "Sequential Read (1MB block)" \
        "read" "1M" "100M" "1" "0"
    
    # Random write
    run_fio_test "rand_write" \
        "Random Write (4KB block)" \
        "randwrite" "4K" "100M" "1" "$FIO_FSYNC"
    
    # Random read
    run_fio_test "rand_read" \
        "Random Read (4KB block)" \
        "randread" "4K" "100M" "1" "0"
    
    # Mixed read/write
    run_fio_test "mixed_rw" \
        "Mixed Read/Write (70%/30%, 4KB block)" \
        "randrw" "4K" "100M" "1" "$FIO_FSYNC"
    
    # Multi-thread tests
    echo ""
    echo "--- Multi-thread Tests (4 threads) ---"
    echo ""
    
    run_fio_test "mt_seq_write" \
        "Multi-thread Sequential Write" \
        "write" "1M" "50M" "4" "$FIO_FSYNC"
    
    run_fio_test "mt_rand_read" \
        "Multi-thread Random Read" \
        "randread" "4K" "50M" "4" "0"
}

# Simple benchmark tests (without fio)
run_simple_bench() {
    echo ""
    echo "============================================================"
    echo "  Simple Benchmark (without fio)"
    echo "============================================================"
    echo ""
    
    local TEST_DIR="$MOUNT_DIR/bench_test"
    mkdir -p "$TEST_DIR"
    
    # Test 1: Small file write
    echo "--- Small file write test (20 files x 4KB) ---"
    local start_time=$(date +%s%N)
    for i in $(seq 1 20); do
        dd if=/dev/zero of="$TEST_DIR/small_$i.txt" bs=4096 count=1 conv=fsync 2>/dev/null
    done
    local end_time=$(date +%s%N)
    local elapsed_s=$(echo "scale=3; ($end_time - $start_time) / 1000000000" | bc)
    local throughput=$(echo "scale=2; (20 * 4096) / ($elapsed_s * 1024 * 1024)" | bc)
    echo "  Time: ${elapsed_s}s, Throughput: ${throughput} MB/s"
    
    # Test 2: Small file read
    echo ""
    echo "--- Small file read test ---"
    start_time=$(date +%s%N)
    for i in $(seq 1 20); do
        cat "$TEST_DIR/small_$i.txt" > /dev/null
    done
    end_time=$(date +%s%N)
    elapsed_s=$(echo "scale=3; ($end_time - $start_time) / 1000000000" | bc)
    throughput=$(echo "scale=2; (20 * 4096) / ($elapsed_s * 1024 * 1024)" | bc)
    echo "  Time: ${elapsed_s}s, Throughput: ${throughput} MB/s"
    
    # Test 3: Large file write
    echo ""
    echo "--- Large file write test (4MB) ---"
    start_time=$(date +%s%N)
    dd if=/dev/zero of="$TEST_DIR/large.bin" bs=1M count=4 conv=fsync 2>/dev/null
    end_time=$(date +%s%N)
    elapsed_s=$(echo "scale=3; ($end_time - $start_time) / 1000000000" | bc)
    throughput=$(echo "scale=2; 4 / $elapsed_s" | bc)
    echo "  Time: ${elapsed_s}s, Throughput: ${throughput} MB/s"
    
    # Test 4: Large file read
    echo ""
    echo "--- Large file read test ---"
    start_time=$(date +%s%N)
    cat "$TEST_DIR/large.bin" > /dev/null
    end_time=$(date +%s%N)
    elapsed_s=$(echo "scale=3; ($end_time - $start_time) / 1000000000" | bc)
    throughput=$(echo "scale=2; 4 / $elapsed_s" | bc)
    echo "  Time: ${elapsed_s}s, Throughput: ${throughput} MB/s"
    
    # Test 5: Directory operations
    echo ""
    echo "--- Directory operations test ---"
    local dir_count=50
    start_time=$(date +%s%N)
    for i in $(seq 1 $dir_count); do
        mkdir "$TEST_DIR/dir_$i"
    done
    end_time=$(date +%s%N)
    elapsed_ms=$(( (end_time - $start_time) / 1000000 ))
    echo "  Created $dir_count dirs in ${elapsed_ms}ms ($(( dir_count * 1000 / (elapsed_ms + 1) )) ops/s)"
    
    start_time=$(date +%s%N)
    for i in $(seq 1 $dir_count); do
        ls "$TEST_DIR/dir_$i" > /dev/null
    done
    end_time=$(date +%s%N)
    elapsed_ms=$(( (end_time - $start_time) / 1000000 ))
    echo "  Listed $dir_count dirs in ${elapsed_ms}ms ($(( dir_count * 1000 / (elapsed_ms + 1) )) ops/s)"
    
    start_time=$(date +%s%N)
    for i in $(seq 1 $dir_count); do
        rmdir "$TEST_DIR/dir_$i"
    done
    end_time=$(date +%s%N)
    elapsed_ms=$(( (end_time - $start_time) / 1000000 ))
    echo "  Deleted $dir_count dirs in ${elapsed_ms}ms ($(( dir_count * 1000 / (elapsed_ms + 1) )) ops/s)"
    
    # Cleanup
    rm -rf "$TEST_DIR"
}

# Main
main() {
    parse_args "$@"
    
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║     PowerFS Performance Benchmark                        ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
    
    # Setup
    setup_test_env
    cleanup_test_env
    
    # Build
    if [ "$BUILD_FIRST" = "true" ]; then
        build_binaries "release"
    fi
    
    # Start services
    start_all_services
    
    # Run benchmarks
    if command -v fio &> /dev/null; then
        log_info "Using fio for comprehensive testing..."
        run_bench_tests
    else
        log_warn "fio not found, running simple benchmarks only"
        run_simple_bench
    fi
    
    # Cleanup
    cleanup_test_env
    
    echo ""
    log_success "Performance benchmark completed!"
}

main "$@"
