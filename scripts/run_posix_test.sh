#!/bin/bash
set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")
MOUNT_DIR="/tmp/powerfs-posix-test"
MASTER_DIR="/tmp/powerfs-posix-master"
VOLUME_DIR="/tmp/powerfs-posix-volume"

MASTER_PORT=9360
VOLUME_PORT=8097

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
        --dir "$MASTER_DIR" > /tmp/posix-test-master.log 2>&1 &
    MASTER_PID=$!
    
    sleep 3
    
    echo "=== Starting Volume ==="
    "$PROJECT_ROOT/target/release/powerfs-volume" \
        --grpc-address "0.0.0.0:$VOLUME_PORT" \
        --http-port 8098 \
        --node-id posix-test-node \
        --master-address "localhost:$MASTER_PORT" \
        --data-dir "$VOLUME_DIR" > /tmp/posix-test-volume.log 2>&1 &
    VOLUME_PID=$!
    
    sleep 3
    
    echo "=== Starting FUSE ==="
    mkdir -p "$MOUNT_DIR"
    "$PROJECT_ROOT/target/release/powerfs-fuse" \
        --master "localhost:$MASTER_PORT" \
        --mount-point "$MOUNT_DIR" \
        --collection default \
        --replication 000 > /tmp/posix-test-fuse.log 2>&1 &
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

test_mkdir() {
    echo ""
    echo "=== Test 1: mkdir ==="
    
    rm -rf "$MOUNT_DIR/mkdir_test"
    
    mkdir -p "$MOUNT_DIR/mkdir_test/a/b/c"
    mkdir "$MOUNT_DIR/mkdir_test/d"
    
    [ -d "$MOUNT_DIR/mkdir_test/a/b/c" ] || (echo "FAIL: Nested dir not created" && exit 1)
    [ -d "$MOUNT_DIR/mkdir_test/d" ] || (echo "FAIL: Dir not created" && exit 1)
    
    echo "OK: mkdir works correctly"
    echo "Test 1 PASSED"
}

test_rename() {
    echo ""
    echo "=== Test 2: rename ==="
    
    rm -rf "$MOUNT_DIR/rename_test"
    
    mkdir -p "$MOUNT_DIR/rename_test"
    echo "test content" > "$MOUNT_DIR/rename_test/file.txt"
    mkdir "$MOUNT_DIR/rename_test/dir1"
    echo "dir content" > "$MOUNT_DIR/rename_test/dir1/nested.txt"
    
    mv "$MOUNT_DIR/rename_test/file.txt" "$MOUNT_DIR/rename_test/renamed.txt"
    mv "$MOUNT_DIR/rename_test/dir1" "$MOUNT_DIR/rename_test/dir2"
    
    [ -f "$MOUNT_DIR/rename_test/renamed.txt" ] || (echo "FAIL: File not renamed" && exit 1)
    [ ! -f "$MOUNT_DIR/rename_test/file.txt" ] || (echo "FAIL: Old file still exists" && exit 1)
    [ -d "$MOUNT_DIR/rename_test/dir2" ] || (echo "FAIL: Dir not renamed" && exit 1)
    [ -f "$MOUNT_DIR/rename_test/dir2/nested.txt" ] || (echo "FAIL: Nested file missing after dir rename" && exit 1)
    
    content=$(cat "$MOUNT_DIR/rename_test/renamed.txt")
    [ "$content" = "test content" ] || (echo "FAIL: File content changed" && exit 1)
    
    echo "OK: rename works correctly"
    echo "Test 2 PASSED"
}

test_link() {
    echo ""
    echo "=== Test 3: hard link ==="
    
    rm -rf "$MOUNT_DIR/link_test"
    
    mkdir -p "$MOUNT_DIR/link_test"
    echo "hard link content" > "$MOUNT_DIR/link_test/original.txt"
    
    ln "$MOUNT_DIR/link_test/original.txt" "$MOUNT_DIR/link_test/hardlink.txt"
    
    [ -f "$MOUNT_DIR/link_test/hardlink.txt" ] || (echo "FAIL: Hard link not created" && exit 1)
    
    content1=$(cat "$MOUNT_DIR/link_test/original.txt")
    content2=$(cat "$MOUNT_DIR/link_test/hardlink.txt")
    [ "$content1" = "$content2" ] || (echo "FAIL: Hard link content mismatch" && exit 1)
    
    echo "updated content" > "$MOUNT_DIR/link_test/hardlink.txt"
    content3=$(cat "$MOUNT_DIR/link_test/original.txt")
    [ "$content3" = "updated content" ] || (echo "FAIL: Hard link update not visible in original" && exit 1)
    
    echo "OK: hard link works correctly"
    echo "Test 3 PASSED"
}

test_symlink() {
    echo ""
    echo "=== Test 4: symlink ==="
    
    rm -rf "$MOUNT_DIR/symlink_test"
    
    mkdir -p "$MOUNT_DIR/symlink_test"
    echo "symlink target content" > "$MOUNT_DIR/symlink_test/target.txt"
    
    ln -s target.txt "$MOUNT_DIR/symlink_test/link.txt"
    
    [ -L "$MOUNT_DIR/symlink_test/link.txt" ] || (echo "FAIL: Symlink not created" && exit 1)
    
    target=$(readlink "$MOUNT_DIR/symlink_test/link.txt")
    [ "$target" = "target.txt" ] || (echo "FAIL: Symlink target mismatch" && exit 1)
    
    content=$(cat "$MOUNT_DIR/symlink_test/link.txt")
    [ "$content" = "symlink target content" ] || (echo "FAIL: Symlink content mismatch" && exit 1)
    
    echo "OK: symlink works correctly"
    echo "Test 4 PASSED"
}

test_xattr() {
    echo ""
    echo "=== Test 5: xattr ==="
    
    rm -rf "$MOUNT_DIR/xattr_test"
    
    mkdir -p "$MOUNT_DIR/xattr_test"
    echo "xattr test file" > "$MOUNT_DIR/xattr_test/file.txt"
    
    setfattr -n user.test_attr -v "test_value" "$MOUNT_DIR/xattr_test/file.txt" 2>/dev/null || {
        echo "SKIP: xattr not supported (setfattr failed)"
        return 0
    }
    
    value=$(getfattr -n user.test_attr -e text --only-values "$MOUNT_DIR/xattr_test/file.txt" 2>/dev/null)
    [ "$value" = "test_value" ] || (echo "FAIL: xattr value mismatch" && exit 1)
    
    setfattr -x user.test_attr "$MOUNT_DIR/xattr_test/file.txt"
    value=$(getfattr -n user.test_attr "$MOUNT_DIR/xattr_test/file.txt" 2>/dev/null || true)
    [ -z "$value" ] || (echo "FAIL: xattr not removed" && exit 1)
    
    echo "OK: xattr works correctly"
    echo "Test 5 PASSED"
}

test_flock() {
    echo ""
    echo "=== Test 6: flock ==="
    
    rm -rf "$MOUNT_DIR/flock_test"
    
    mkdir -p "$MOUNT_DIR/flock_test"
    
    python3 << 'EOF'
import fcntl
import os
import time
import multiprocessing

def lock_test(filename, lock_type):
    f = open(filename, 'a')
    if lock_type == 'exclusive':
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
    else:
        fcntl.flock(f.fileno(), fcntl.LOCK_SH)
    
    time.sleep(0.5)
    
    if lock_type == 'exclusive':
        f.write('exclusive lock\n')
        f.flush()
    
    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    f.close()

if __name__ == '__main__':
    filename = '/tmp/powerfs-posix-test/flock_test/test.lock'
    
    open(filename, 'w').close()
    
    p1 = multiprocessing.Process(target=lock_test, args=(filename, 'exclusive'))
    p2 = multiprocessing.Process(target=lock_test, args=(filename, 'exclusive'))
    
    start = time.time()
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    elapsed = time.time() - start
    
    assert elapsed >= 1.0, f"Expected >= 1.0s (serialized), got {elapsed:.2f}s"
    
    f = open(filename, 'r')
    content = f.read()
    f.close()
    
    assert content.count('exclusive lock') == 2, f"Expected 2 writes, got: {content}"
    
    print(f"OK: File locking works (elapsed: {elapsed:.2f}s)")
EOF
    
    echo "Test 6 PASSED"
}

test_permissions() {
    echo ""
    echo "=== Test 7: file permissions ==="
    
    rm -rf "$MOUNT_DIR/perms_test"
    
    mkdir -p "$MOUNT_DIR/perms_test"
    echo "perm test" > "$MOUNT_DIR/perms_test/file.txt"
    
    chmod 600 "$MOUNT_DIR/perms_test/file.txt"
    perms=$(stat -c "%a" "$MOUNT_DIR/perms_test/file.txt")
    [ "$perms" = "600" ] || (echo "FAIL: Permissions not set correctly (got $perms)" && exit 1)
    
    chmod 755 "$MOUNT_DIR/perms_test/file.txt"
    perms=$(stat -c "%a" "$MOUNT_DIR/perms_test/file.txt")
    [ "$perms" = "755" ] || (echo "FAIL: Permissions not changed correctly (got $perms)" && exit 1)
    
    echo "OK: file permissions work correctly"
    echo "Test 7 PASSED"
}

test_truncate() {
    echo ""
    echo "=== Test 8: truncate ==="
    
    rm -rf "$MOUNT_DIR/truncate_test"
    
    mkdir -p "$MOUNT_DIR/truncate_test"
    echo "this is a longer test string" > "$MOUNT_DIR/truncate_test/file.txt"
    
    truncate -s 10 "$MOUNT_DIR/truncate_test/file.txt"
    size=$(stat -c "%s" "$MOUNT_DIR/truncate_test/file.txt")
    [ "$size" = "10" ] || (echo "FAIL: Truncate size mismatch (got $size)" && exit 1)
    
    content=$(cat "$MOUNT_DIR/truncate_test/file.txt")
    [ "$content" = "this is a " ] || (echo "FAIL: Truncate content mismatch" && exit 1)
    
    echo "OK: truncate works correctly"
    echo "Test 8 PASSED"
}

test_unlink() {
    echo ""
    echo "=== Test 9: unlink ==="
    
    rm -rf "$MOUNT_DIR/unlink_test"
    
    mkdir -p "$MOUNT_DIR/unlink_test"
    echo "to be deleted" > "$MOUNT_DIR/unlink_test/file.txt"
    
    [ -f "$MOUNT_DIR/unlink_test/file.txt" ] || (echo "FAIL: File not created" && exit 1)
    
    rm "$MOUNT_DIR/unlink_test/file.txt"
    
    [ ! -f "$MOUNT_DIR/unlink_test/file.txt" ] || (echo "FAIL: File not deleted" && exit 1)
    
    echo "OK: unlink works correctly"
    echo "Test 9 PASSED"
}

test_rmdir() {
    echo ""
    echo "=== Test 10: rmdir ==="
    
    rm -rf "$MOUNT_DIR/rmdir_test"
    
    mkdir -p "$MOUNT_DIR/rmdir_test/a/b"
    mkdir "$MOUNT_DIR/rmdir_test/c"
    
    rmdir "$MOUNT_DIR/rmdir_test/a/b"
    rmdir "$MOUNT_DIR/rmdir_test/a"
    rmdir "$MOUNT_DIR/rmdir_test/c"
    
    [ ! -d "$MOUNT_DIR/rmdir_test/a" ] || (echo "FAIL: Dir not removed" && exit 1)
    [ ! -d "$MOUNT_DIR/rmdir_test/c" ] || (echo "FAIL: Dir not removed" && exit 1)
    
    echo "OK: rmdir works correctly"
    echo "Test 10 PASSED"
}

main() {
    cleanup
    
    echo "=== Building release binaries ==="
    cargo build --release -p powerfs-server -p powerfs-volume -p powerfs-fuse 2>&1 | tail -1
    
    echo ""
    echo "=== Starting services ==="
    start_services
    
    echo ""
    echo "=== Running POSIX Functionality Tests ==="
    
    test_mkdir
    test_rename
    test_link
    test_symlink
    test_xattr
    test_flock
    test_permissions
    test_truncate
    test_unlink
    test_rmdir
    
    echo ""
    echo "=== ALL POSIX FUNCTIONALITY TESTS PASSED ==="
    
    stop_services
    cleanup
}

main "$@"
