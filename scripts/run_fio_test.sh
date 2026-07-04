#!/bin/bash
set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")
MOUNT_DIR="/tmp/powerfs-fio-test"
MASTER_DIR="/tmp/powerfs-fio-master"
VOLUME_DIR="/tmp/powerfs-fio-volume"

MASTER_PORT=9370
VOLUME_PORT=8099

IO_ENGINE="sync"
FIO_FSYNC="0"

cd "$PROJECT_ROOT"

print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --engine=ENGINE  IO engine: sync (default), libaio, io_uring"
    echo "  --fsync=N        Number of I/Os between fsync (0=disabled, 1=every IO)"
    echo "  --no-fsync       Shortcut for --fsync=0 (cached writes)"
    echo "  --force-fsync    Shortcut for --fsync=1 (persistent writes)"
    echo "  --no-build       Skip building release binaries"
    echo "  --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                          # Default: sync engine, no fsync"
    echo "  $0 --engine=libaio          # Use libaio engine, no fsync"
    echo "  $0 --engine=io_uring        # Use io_uring engine, no fsync"
    echo "  $0 --force-fsync            # sync engine with fsync=1"
    echo "  $0 --engine=libaio --fsync=1000 # libaio, fsync every 1000 I/Os"
}

parse_args() {
    BUILD=true
    for arg in "$@"; do
        case "$arg" in
            --engine=*)
                IO_ENGINE="${arg#*=}"
                ;;
            --fsync=*)
                FIO_FSYNC="${arg#*=}"
                ;;
            --no-fsync)
                FIO_FSYNC="0"
                ;;
            --force-fsync)
                FIO_FSYNC="1"
                ;;
            --no-build)
                BUILD=false
                ;;
            --help)
                print_usage
                exit 0
                ;;
            *)
                echo "Unknown argument: $arg"
                print_usage
                exit 1
                ;;
        esac
    done
}

cleanup() {
    echo "=== Cleanup ==="
    
    if mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
        fusermount -uz "$MOUNT_DIR" 2>/dev/null || umount -f "$MOUNT_DIR" 2>/dev/null || true
        sleep 0.5
        rm -rf "$MOUNT_DIR" 2>/dev/null || true
    elif [ -d "$MOUNT_DIR" ]; then
        rm -rf "$MOUNT_DIR" 2>/dev/null || true
    fi
    
    pkill -9 -f "powerfs master" 2>/dev/null || true
    pkill -9 -f "powerfs-volume" 2>/dev/null || true
    pkill -9 -f "powerfs-fuse" 2>/dev/null || true
    
    sleep 1
    
    rm -rf "$MASTER_DIR" 2>/dev/null || true
    rm -rf "$VOLUME_DIR" 2>/dev/null || true
    
    echo "Cleanup done"
}

start_services() {
    echo "=== Starting Master ==="
    "$PROJECT_ROOT/target/release/powerfs" \
        --log-level debug \
        master \
        --port "$MASTER_PORT" \
        --dir "$MASTER_DIR" > /tmp/fio-test-master.log 2>&1 &
    MASTER_PID=$!
    
    sleep 3
    
    echo "=== Starting Volume ==="
    "$PROJECT_ROOT/target/release/powerfs-volume" \
        --grpc-address "0.0.0.0:$VOLUME_PORT" \
        --http-port 8100 \
        --node-id fio-test-node \
        --master-address "localhost:$MASTER_PORT" \
        --data-dir "$VOLUME_DIR" > /tmp/fio-test-volume.log 2>&1 &
    VOLUME_PID=$!
    
    sleep 3
    
    echo "=== Starting FUSE ==="
    mkdir -p "$MOUNT_DIR"
    "$PROJECT_ROOT/target/release/powerfs-fuse" \
        --master "localhost:$MASTER_PORT" \
        --mount-point "$MOUNT_DIR" \
        --collection default \
        --replication 000 > /tmp/fio-test-fuse.log 2>&1 &
    FUSE_PID=$!
    
    sleep 4
    
    echo "Master PID: $MASTER_PID"
    echo "Volume PID: $VOLUME_PID"
    echo "FUSE PID: $FUSE_PID"
    
    export MASTER_PID VOLUME_PID FUSE_PID
}

stop_services() {
    echo "=== Stopping FUSE ==="
    kill -TERM "$FUSE_PID" 2>/dev/null || true
    sleep 1
    fusermount -uz "$MOUNT_DIR" 2>/dev/null || true
    sleep 0.5
    
    echo "=== Stopping Volume ==="
    kill -TERM "$VOLUME_PID" 2>/dev/null || true
    sleep 1
    
    echo "=== Stopping Master ==="
    kill -TERM "$MASTER_PID" 2>/dev/null || true
    sleep 1
    
    echo "Services stopped"
}

run_fio_test() {
    local test_name="$1"
    local test_desc="$2"
    local rw="$3"
    local bs="$4"
    local size="$5"
    local numjobs="$6"
    local fsync_val="$7"
    
    echo ""
    echo "=== Test $test_name: $test_desc ==="
    echo "  Engine: $IO_ENGINE | fsync: $fsync_val | Jobs: $numjobs | BS: $bs"
    
    local fsync_opt=""
    if [ "$fsync_val" != "0" ]; then
        fsync_opt="--fsync=$fsync_val"
    fi
    
    fio --name="$test_name" --ioengine="$IO_ENGINE" --rw="$rw" --bs="$bs" --size="$size" \
        $fsync_opt \
        --directory="$MOUNT_DIR" --numjobs="$numjobs" --group_reporting
}

run_tests() {
    echo ""
    echo "=== Running FIO Performance Tests ==="
    echo "  IO Engine: $IO_ENGINE"
    echo "  fsync: ${FIO_FSYNC} (0=disabled, 1=every IO, N=every N I/Os)"
    echo ""
    
    run_fio_test "seq_write" "Sequential Write (1MB block)" "write" "1M" "100M" "1" "$FIO_FSYNC"
    run_fio_test "seq_read" "Sequential Read (1MB block)" "read" "1M" "100M" "1" "0"
    
    run_fio_test "rand_write" "Random Write (4KB block)" "randwrite" "4K" "100M" "1" "$FIO_FSYNC"
    run_fio_test "rand_read" "Random Read (4KB block)" "randread" "4K" "100M" "1" "0"
    
    run_fio_test "mixed_rw" "Mixed Read/Write (70%/30%, 4KB block)" "randrw" "4K" "100M" "1" "$FIO_FSYNC"
    
    echo ""
    echo "=== Running Multi-thread Tests (4 threads) ==="
    echo ""
    
    run_fio_test "mt_seq_write" "Multi-thread Sequential Write (1MB block)" "write" "1M" "50M" "4" "$FIO_FSYNC"
    run_fio_test "mt_rand_read" "Multi-thread Random Read (4KB block)" "randread" "4K" "50M" "4" "0"
}

main() {
    parse_args "$@"
    
    cleanup
    
    if $BUILD; then
        echo "=== Building release binaries ==="
        cargo build --release -p powerfs-server -p powerfs-volume -p powerfs-fuse 2>&1 | tail -1
    fi
    
    echo ""
    echo "=== Starting services ==="
    start_services
    
    run_tests
    
    echo ""
    echo "=== ALL FIO PERFORMANCE TESTS COMPLETE ==="
    
    stop_services
    cleanup
}

main "$@"