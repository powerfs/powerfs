#!/bin/bash
set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")
MOUNT_DIR="/tmp/powerfs-persistence-test"
MASTER_DIR="/tmp/powerfs-persistence-master"
VOLUME_DIR="/tmp/powerfs-persistence-volume"

MASTER_PORT=9340
VOLUME_PORT=8090

cd "$PROJECT_ROOT"

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
        --log-level warn \
        master \
        --port "$MASTER_PORT" \
        --dir "$MASTER_DIR" > /tmp/persistence-master.log 2>&1 &
    MASTER_PID=$!
    
    sleep 3
    
    if ! kill -0 "$MASTER_PID" 2>/dev/null; then
        echo "ERROR: Master failed to start"
        cat /tmp/persistence-master.log
        exit 1
    fi
    
    echo "=== Starting Volume ==="
    "$PROJECT_ROOT/target/release/powerfs-volume" \
        --grpc-address "0.0.0.0:$VOLUME_PORT" \
        --http-port 8091 \
        --node-id persistence-node \
        --master-address "localhost:$MASTER_PORT" \
        --data-dir "$VOLUME_DIR" > /tmp/persistence-volume.log 2>&1 &
    VOLUME_PID=$!
    
    sleep 3
    
    if ! kill -0 "$VOLUME_PID" 2>/dev/null; then
        echo "ERROR: Volume failed to start"
        cat /tmp/persistence-volume.log
        exit 1
    fi
    
    echo "=== Starting FUSE ==="
    mkdir -p "$MOUNT_DIR"
    "$PROJECT_ROOT/target/release/powerfs-fuse" \
        --master "localhost:$MASTER_PORT" \
        --mount-point "$MOUNT_DIR" \
        --collection default \
        --replication 000 > /tmp/persistence-fuse.log 2>&1 &
    FUSE_PID=$!
    
    sleep 4
    
    if ! kill -0 "$FUSE_PID" 2>/dev/null; then
        echo "ERROR: FUSE failed to start"
        cat /tmp/persistence-fuse.log
        exit 1
    fi
    
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

create_test_data() {
    echo "=== Creating test data ==="
    
    mkdir -p "$MOUNT_DIR/dir1"
    mkdir -p "$MOUNT_DIR/dir1/subdir"
    mkdir -p "$MOUNT_DIR/dir2"
    
    echo "Hello World" > "$MOUNT_DIR/file1.txt"
    echo "Test data with special chars" > "$MOUNT_DIR/file2.txt"
    echo -n "binary data" > "$MOUNT_DIR/file3.bin"
    
    dd if=/dev/urandom bs=1K count=100 2>/dev/null > "$MOUNT_DIR/large_file.bin"
    
    echo "nested file" > "$MOUNT_DIR/dir1/nested.txt"
    echo "deeply nested" > "$MOUNT_DIR/dir1/subdir/deep.txt"
    echo "dir2 file" > "$MOUNT_DIR/dir2/file.txt"
    
    ln -s file1.txt "$MOUNT_DIR/link1.txt"
    
    echo "Test data created:"
    ls -laR "$MOUNT_DIR"
}

verify_test_data() {
    echo "=== Verifying test data ==="
    
    local all_ok=0
    
    if [ ! -d "$MOUNT_DIR/dir1" ]; then
        echo "FAIL: dir1 missing"
        all_ok=1
    fi
    
    if [ ! -d "$MOUNT_DIR/dir1/subdir" ]; then
        echo "FAIL: dir1/subdir missing"
        all_ok=1
    fi
    
    if [ ! -d "$MOUNT_DIR/dir2" ]; then
        echo "FAIL: dir2 missing"
        all_ok=1
    fi
    
    if [ ! -f "$MOUNT_DIR/file1.txt" ]; then
        echo "FAIL: file1.txt missing"
        all_ok=1
    elif [ "$(cat "$MOUNT_DIR/file1.txt")" != "Hello World" ]; then
        echo "FAIL: file1.txt content mismatch"
        all_ok=1
    else
        echo "OK: file1.txt"
    fi
    
    if [ ! -f "$MOUNT_DIR/file2.txt" ]; then
        echo "FAIL: file2.txt missing"
        all_ok=1
    elif [ "$(cat "$MOUNT_DIR/file2.txt")" != "Test data with special chars" ]; then
        echo "FAIL: file2.txt content mismatch"
        all_ok=1
    else
        echo "OK: file2.txt"
    fi
    
    if [ ! -f "$MOUNT_DIR/file3.bin" ]; then
        echo "FAIL: file3.bin missing"
        all_ok=1
    elif [ "$(cat "$MOUNT_DIR/file3.bin")" != "binary data" ]; then
        echo "FAIL: file3.bin content mismatch"
        all_ok=1
    else
        echo "OK: file3.bin"
    fi
    
    if [ ! -f "$MOUNT_DIR/large_file.bin" ]; then
        echo "FAIL: large_file.bin missing"
        all_ok=1
    elif [ "$(stat -c %s "$MOUNT_DIR/large_file.bin")" -ne 102400 ]; then
        echo "FAIL: large_file.bin size mismatch"
        all_ok=1
    else
        echo "OK: large_file.bin (size: $(stat -c %s "$MOUNT_DIR/large_file.bin"))"
    fi
    
    if [ ! -f "$MOUNT_DIR/dir1/nested.txt" ]; then
        echo "FAIL: dir1/nested.txt missing"
        all_ok=1
    elif [ "$(cat "$MOUNT_DIR/dir1/nested.txt")" != "nested file" ]; then
        echo "FAIL: dir1/nested.txt content mismatch"
        all_ok=1
    else
        echo "OK: dir1/nested.txt"
    fi
    
    if [ ! -f "$MOUNT_DIR/dir1/subdir/deep.txt" ]; then
        echo "FAIL: dir1/subdir/deep.txt missing"
        all_ok=1
    elif [ "$(cat "$MOUNT_DIR/dir1/subdir/deep.txt")" != "deeply nested" ]; then
        echo "FAIL: dir1/subdir/deep.txt content mismatch"
        all_ok=1
    else
        echo "OK: dir1/subdir/deep.txt"
    fi
    
    if [ ! -f "$MOUNT_DIR/dir2/file.txt" ]; then
        echo "FAIL: dir2/file.txt missing"
        all_ok=1
    elif [ "$(cat "$MOUNT_DIR/dir2/file.txt")" != "dir2 file" ]; then
        echo "FAIL: dir2/file.txt content mismatch"
        all_ok=1
    else
        echo "OK: dir2/file.txt"
    fi
    
    if [ ! -L "$MOUNT_DIR/link1.txt" ]; then
        echo "FAIL: link1.txt missing"
        all_ok=1
    elif [ "$(readlink "$MOUNT_DIR/link1.txt")" != "file1.txt" ]; then
        echo "FAIL: link1.txt target mismatch"
        all_ok=1
    else
        echo "OK: link1.txt -> $(readlink "$MOUNT_DIR/link1.txt")"
    fi
    
    echo ""
    echo "Files after restart:"
    ls -laR "$MOUNT_DIR"
    
    return $all_ok
}

main() {
    cleanup
    
    echo "=== Building release binaries ==="
    cargo build --release -p powerfs-server -p powerfs-volume -p powerfs-fuse 2>&1 | tail -3
    
    echo ""
    echo "=== PHASE 1: Create test data ==="
    start_services
    create_test_data
    stop_services
    
    sleep 2
    
    echo ""
    echo "=== PHASE 2: Verify persistence after restart ==="
    start_services
    
    if verify_test_data; then
        echo ""
        echo "=== ALL PERSISTENCE TESTS PASSED ==="
        cleanup
        exit 0
    else
        echo ""
        echo "=== PERSISTENCE TEST FAILED ==="
        cleanup
        exit 1
    fi
}

main "$@"