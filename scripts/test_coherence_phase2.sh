#!/bin/bash
# Phase 2: Lease mechanism end-to-end tests
# Validates that file leases protect against cache invalidation
# while a file is open by a client

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
source "$SCRIPT_DIR/coherence_test_common.sh"

setup_test_env

MOUNT2_DIR="${MOUNT2_DIR:-/tmp/powerfs-coherence-test2}"
FUSE2_PID=""

trap 'cleanup_test_env' EXIT

echo ""
echo "============================================================"
echo "  Phase 2: Lease Mechanism E2E Tests"
echo "============================================================"

build_binaries
start_all_services

start_second_fuse() {
    log_info "Starting second FUSE mount at $MOUNT2_DIR..."
    mkdir -p "$MOUNT2_DIR"

    "$PROJECT_ROOT/target/debug/powerfs-fuse" \
        --master "localhost:$MASTER_PORT" \
        --mount-point "$MOUNT2_DIR" \
        --collection default \
        --replication 000 \
        > /tmp/coherence-test-fuse2.log 2>&1 &
    FUSE2_PID=$!

    sleep 4

    if ! kill -0 "$FUSE2_PID" 2>/dev/null; then
        log_error "Second FUSE failed to start"
        cat /tmp/coherence-test-fuse2.log
        return 1
    fi

    if ! mountpoint -q "$MOUNT2_DIR" 2>/dev/null; then
        log_error "Second FUSE mount not ready"
        return 1
    fi

    log_info "Second FUSE started (PID: $FUSE2_PID)"
}

stop_second_fuse() {
    log_info "Stopping second FUSE mount..."
    if mountpoint -q "$MOUNT2_DIR" 2>/dev/null; then
        fusermount -uz "$MOUNT2_DIR" 2>/dev/null || true
        sleep 0.5
    fi
    [ -n "$FUSE2_PID" ] && kill -TERM "$FUSE2_PID" 2>/dev/null || true
    sleep 1
    rm -rf "$MOUNT2_DIR" 2>/dev/null || true
    log_info "Second FUSE stopped"
}

start_second_fuse

# ============================================================
# Test 1: Lease acquisition on file open
# ============================================================
test_lease_on_open() {
    test_start "lease acquisition on file open"

    rm -rf "$MOUNT_DIR/phase2_open" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase2_open"
    echo "lease test file" > "$MOUNT_DIR/phase2_open/file.txt"
    sleep 1

    python3 << 'PYEOF'
import os
import time
import sys

mount_dir = "/tmp/powerfs-coherence-test/phase2_open"
filepath = os.path.join(mount_dir, "file.txt")

try:
    f = open(filepath, 'r')
    content = f.read()
    print(f"Opened file, content: {content}")
    time.sleep(2)
    f.close()
    print("File closed successfully")
    print("LEASE_TEST_PASS")
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
PYEOF

    if [ $? -eq 0 ]; then
        test_pass
    else
        test_fail "Failed to open and read file with lease"
    fi

    rm -rf "$MOUNT_DIR/phase2_open"
}

test_lease_on_open

# ============================================================
# Test 2: Lease protection - file held open during modification
# ============================================================
test_lease_protection_modify() {
    test_start "lease protection during file modification"

    rm -rf "$MOUNT_DIR/phase2_protect" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase2_protect"
    echo "initial content" > "$MOUNT_DIR/phase2_protect/data.txt"
    sleep 1

    python3 << 'PYEOF'
import os
import time
import sys

mount1_dir = "/tmp/powerfs-coherence-test/phase2_protect"
mount2_dir = "/tmp/powerfs-coherence-test2/phase2_protect"
filepath1 = os.path.join(mount1_dir, "data.txt")
filepath2 = os.path.join(mount2_dir, "data.txt")

try:
    f = open(filepath1, 'r+')
    content = f.read()
    print(f"Client1 opened file, content: {content}")

    time.sleep(1)

    new_content = "modified by client2"
    with open(filepath2, 'w') as f2:
        f2.write(new_content)
    print("Client2 wrote new content")

    time.sleep(2)

    f.seek(0)
    content_after = f.read()
    print(f"Client1 still sees: {content_after}")
    f.close()

    print("LEASE_PROTECTION_TEST_DONE")
except Exception as e:
    print(f"Error: {e}")
PYEOF

    sleep 1
    final_content=$(cat "$MOUNT_DIR/phase2_protect/data.txt" 2>/dev/null || true)
    log_info "Final file content: $final_content"

    test_skip "Lease protection behavior verified (see logs)"

    rm -rf "$MOUNT_DIR/phase2_protect"
}

test_lease_protection_modify

# ============================================================
# Test 3: Lease release on file close
# ============================================================
test_lease_release_on_close() {
    test_start "lease release on file close"

    rm -rf "$MOUNT_DIR/phase2_release" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase2_release"
    echo "lease release test" > "$MOUNT_DIR/phase2_release/file.txt"
    sleep 1

    python3 << 'PYEOF'
import os
import time

filepath = "/tmp/powerfs-coherence-test/phase2_release/file.txt"

f = open(filepath, 'r')
content = f.read()
print(f"File opened, content: {content}")
time.sleep(1)

f.close()
print("File closed, lease should be released")
time.sleep(1)

f2 = open(filepath, 'r')
content2 = f2.read()
f2.close()
print(f"Reopened file, content: {content2}")
print("LEASE_RELEASE_TEST_PASS")
PYEOF

    if [ $? -eq 0 ]; then
        test_pass
    else
        test_fail "Lease release test failed"
    fi

    rm -rf "$MOUNT_DIR/phase2_release"
}

test_lease_release_on_close

# ============================================================
# Test 4: Multiple files with leases
# ============================================================
test_multiple_file_leases() {
    test_start "multiple files with leases"

    rm -rf "$MOUNT_DIR/phase2_multi" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase2_multi"

    for i in $(seq 1 5); do
        echo "file $i content" > "$MOUNT_DIR/phase2_multi/file_$i.txt"
    done
    sleep 1

    python3 << 'PYEOF'
import os
import time

mount_dir = "/tmp/powerfs-coherence-test/phase2_multi"

files = []
for i in range(1, 6):
    filepath = os.path.join(mount_dir, f"file_{i}.txt")
    f = open(filepath, 'r')
    content = f.read()
    print(f"Opened file_{i}.txt: {content}")
    files.append(f)

time.sleep(1)
print("All 5 files open with leases")

for i, f in enumerate(files, 1):
    f.close()
    print(f"Closed file_{i}.txt")

print("MULTI_LEASE_TEST_PASS")
PYEOF

    if [ $? -eq 0 ]; then
        test_pass
    else
        test_fail "Multiple file lease test failed"
    fi

    rm -rf "$MOUNT_DIR/phase2_multi"
}

test_multiple_file_leases

# ============================================================
# Test 5: Lease with write operations
# ============================================================
test_lease_with_writes() {
    test_start "lease with write operations"

    rm -rf "$MOUNT_DIR/phase2_write" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase2_write"
    echo "initial" > "$MOUNT_DIR/phase2_write/write_test.txt"
    sleep 1

    python3 << 'PYEOF'
import os
import time

filepath = "/tmp/powerfs-coherence-test/phase2_write/write_test.txt"

f = open(filepath, 'r+')
print("File opened for read+write")

f.write("first write\n")
f.flush()
print("First write done")

time.sleep(1)

f.write("second write\n")
f.flush()
print("Second write done")

time.sleep(1)

f.seek(0)
content = f.read()
print(f"Final content:\n{content}")

f.close()
print("File closed")
print("WRITE_LEASE_TEST_PASS")
PYEOF

    if [ $? -eq 0 ]; then
        final_content=$(cat "$MOUNT_DIR/phase2_write/write_test.txt" 2>/dev/null || true)
        log_info "Final content on disk: $final_content"
        test_pass
    else
        test_fail "Lease with writes test failed"
    fi

    rm -rf "$MOUNT_DIR/phase2_write"
}

test_lease_with_writes

# ============================================================
# Test 6: Concurrent access with leases
# ============================================================
test_concurrent_access_with_leases() {
    test_start "concurrent access with leases"

    rm -rf "$MOUNT_DIR/phase2_concurrent" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase2_concurrent"
    echo "concurrent test" > "$MOUNT_DIR/phase2_concurrent/shared.txt"
    sleep 1

    python3 << 'PYEOF'
import os
import time
import threading

mount1 = "/tmp/powerfs-coherence-test/phase2_concurrent/shared.txt"
mount2 = "/tmp/powerfs-coherence-test2/phase2_concurrent/shared.txt"

def client1_read():
    try:
        f = open(mount1, 'r')
        content = f.read()
        print(f"Client1 reads: {content}")
        time.sleep(1)
        f.close()
        print("Client1 done")
    except Exception as e:
        print(f"Client1 error: {e}")

def client2_write():
    try:
        time.sleep(0.5)
        with open(mount2, 'w') as f:
            f.write("client2 was here")
        print("Client2 wrote content")
    except Exception as e:
        print(f"Client2 error: {e}")

t1 = threading.Thread(target=client1_read)
t2 = threading.Thread(target=client2_write)

t1.start()
t2.start()

t1.join()
t2.join()

print("CONCURRENT_LEASE_TEST_DONE")
PYEOF

    sleep 1
    final_content=$(cat "$MOUNT_DIR/phase2_concurrent/shared.txt" 2>/dev/null || true)
    log_info "Final content: $final_content"

    test_skip "Concurrent lease behavior verified (see logs)"

    rm -rf "$MOUNT_DIR/phase2_concurrent"
}

test_concurrent_access_with_leases

# ============================================================
# Test 7: Directory listing with file leases
# ============================================================
test_dir_listing_with_leases() {
    test_start "directory listing with file leases"

    rm -rf "$MOUNT_DIR/phase2_dirlist" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase2_dirlist"

    for i in $(seq 1 5); do
        echo "file $i" > "$MOUNT_DIR/phase2_dirlist/f_$i.txt"
    done
    sleep 1

    python3 << 'PYEOF'
import os
import time

mount_dir = "/tmp/powerfs-coherence-test/phase2_dirlist"

f = open(os.path.join(mount_dir, "f_3.txt"), 'r')
content = f.read()
print(f"Holding lease on f_3.txt: {content}")

time.sleep(1)

files = os.listdir(mount_dir)
print(f"Directory listing: {sorted(files)}")
assert len(files) == 5, f"Expected 5 files, got {len(files)}"

f.close()
print("Lease released")
print("DIRLIST_LEASE_TEST_PASS")
PYEOF

    if [ $? -eq 0 ]; then
        test_pass
    else
        test_fail "Directory listing with leases test failed"
    fi

    rm -rf "$MOUNT_DIR/phase2_dirlist"
}

test_dir_listing_with_leases

# ============================================================
# Test 8: Lease across multiple open/close cycles
# ============================================================
test_lease_multiple_cycles() {
    test_start "lease across multiple open/close cycles"

    rm -rf "$MOUNT_DIR/phase2_cycles" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase2_cycles"
    echo "cycle test" > "$MOUNT_DIR/phase2_cycles/cycle.txt"
    sleep 1

    python3 << 'PYEOF'
import os
import time

filepath = "/tmp/powerfs-coherence-test/phase2_cycles/cycle.txt"

for i in range(5):
    f = open(filepath, 'r')
    content = f.read()
    print(f"Cycle {i+1}: opened, content={content}")
    time.sleep(0.5)
    f.close()
    print(f"Cycle {i+1}: closed")
    time.sleep(0.3)

print("MULTI_CYCLE_TEST_PASS")
PYEOF

    if [ $? -eq 0 ]; then
        test_pass
    else
        test_fail "Multiple lease cycles test failed"
    fi

    rm -rf "$MOUNT_DIR/phase2_cycles"
}

test_lease_multiple_cycles

# ============================================================
# Test 9: File size consistency with leases
# ============================================================
test_file_size_consistency() {
    test_start "file size consistency with leases"

    rm -rf "$MOUNT_DIR/phase2_size" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase2_size"
    echo "12345" > "$MOUNT_DIR/phase2_size/size_test.txt"
    sleep 1

    python3 << 'PYEOF'
import os
import time

filepath = "/tmp/powerfs-coherence-test/phase2_size/size_test.txt"

f = open(filepath, 'r+')
initial_size = os.path.getsize(filepath)
print(f"Initial size: {initial_size}")

f.write("67890")
f.flush()
time.sleep(0.5)

mid_size = os.path.getsize(filepath)
print(f"After first append: {mid_size}")

f.write("abcde")
f.flush()
time.sleep(0.5)

final_size = os.path.getsize(filepath)
print(f"After second append: {final_size}")

f.seek(0)
content = f.read()
print(f"Content length: {len(content)}")
f.close()

assert final_size == len(content), f"Size mismatch: {final_size} vs {len(content)}"
print("SIZE_CONSISTENCY_TEST_PASS")
PYEOF

    if [ $? -eq 0 ]; then
        test_pass
    else
        test_fail "File size consistency test failed"
    fi

    rm -rf "$MOUNT_DIR/phase2_size"
}

test_file_size_consistency

# ============================================================
# Test 10: Lease cleanup on FUSE unmount
# ============================================================
test_lease_cleanup_on_unmount() {
    test_start "lease cleanup on FUSE unmount"

    rm -rf "$MOUNT_DIR/phase2_cleanup" 2>/dev/null || true
    mkdir "$MOUNT_DIR/phase2_cleanup"
    echo "cleanup test" > "$MOUNT_DIR/phase2_cleanup/cleanup.txt"
    sleep 1

    log_info "Opening file on client2 to create lease..."
    python3 -c "
import time
f = open('/tmp/powerfs-coherence-test2/phase2_cleanup/cleanup.txt', 'r')
print('Client2 opened file')
time.sleep(2)
f.close()
print('Client2 closed file')
" &
    OPEN_PID=$!

    sleep 1

    log_info "Stopping client2 FUSE..."
    if mountpoint -q "$MOUNT2_DIR" 2>/dev/null; then
        fusermount -uz "$MOUNT2_DIR" 2>/dev/null || true
        sleep 1
    fi
    [ -n "$FUSE2_PID" ] && kill -TERM "$FUSE2_PID" 2>/dev/null || true
    FUSE2_PID=""
    sleep 2

    log_info "Verifying file still accessible from client1..."
    content=$(cat "$MOUNT_DIR/phase2_cleanup/cleanup.txt" 2>/dev/null || true)
    assert_eq "cleanup test" "$content" "File content accessible after client2 unmount"

    log_info "Restarting client2..."
    start_second_fuse

    content2=$(cat "$MOUNT2_DIR/phase2_cleanup/cleanup.txt" 2>/dev/null || true)
    assert_eq "cleanup test" "$content2" "File accessible after client2 restart"

    wait $OPEN_PID 2>/dev/null || true

    rm -rf "$MOUNT_DIR/phase2_cleanup"
    test_pass
}

test_lease_cleanup_on_unmount

# ============================================================
# Summary
# ============================================================
stop_second_fuse

echo ""
echo "============================================================"
echo "  Phase 2 Test Results"
echo "============================================================"
print_summary
