#!/bin/bash
set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")
MOUNT_DIR="/tmp/powerfs-open-test"
MASTER_DIR="/tmp/powerfs-open-master"
VOLUME_DIR="/tmp/powerfs-open-volume"

MASTER_PORT=9350
VOLUME_PORT=8095

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
        --dir "$MASTER_DIR" > /tmp/open-test-master.log 2>&1 &
    MASTER_PID=$!
    
    sleep 3
    
    echo "=== Starting Volume ==="
    "$PROJECT_ROOT/target/release/powerfs-volume" \
        --grpc-address "0.0.0.0:$VOLUME_PORT" \
        --http-port 8096 \
        --node-id open-test-node \
        --master-address "localhost:$MASTER_PORT" \
        --data-dir "$VOLUME_DIR" > /tmp/open-test-volume.log 2>&1 &
    VOLUME_PID=$!
    
    sleep 3
    
    echo "=== Starting FUSE ==="
    mkdir -p "$MOUNT_DIR"
    "$PROJECT_ROOT/target/release/powerfs-fuse" \
        --master "localhost:$MASTER_PORT" \
        --mount-point "$MOUNT_DIR" \
        --collection default \
        --replication 000 > /tmp/open-test-fuse.log 2>&1 &
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

test_single_open() {
    echo ""
    echo "=== Test 1: Single process open (1-to-1) ==="
    
    rm -f "$MOUNT_DIR/single_open_test.txt"
    
    python3 << 'EOF'
import os

f = open('/tmp/powerfs-open-test/single_open_test.txt', 'w')
f.write('Hello from single process\n')
f.flush()
f.close()

f = open('/tmp/powerfs-open-test/single_open_test.txt', 'r')
content = f.read()
f.close()

assert content == 'Hello from single process\n', f"Expected 'Hello from single process\\n', got '{content}'"
print("OK: Single process open/read/write works correctly")
EOF
    
    echo "Test 1 PASSED"
}

test_multi_open() {
    echo ""
    echo "=== Test 2: Multiple processes open same file (N-to-1) ==="
    
    rm -f "$MOUNT_DIR/multi_open_test.txt"
    
    python3 << 'EOF'
import os
import time
import multiprocessing

def writer(id, barrier):
    f = open('/tmp/powerfs-open-test/multi_open_test.txt', 'a')
    barrier.wait()
    time.sleep(0.1 * id)
    f.write(f'Writer {id} says hello\n')
    f.flush()
    time.sleep(0.5)
    f.close()
    return f"Writer {id} completed"

if __name__ == '__main__':
    open('/tmp/powerfs-open-test/multi_open_test.txt', 'w').close()
    
    barrier = multiprocessing.Barrier(3)
    processes = []
    for i in range(3):
        p = multiprocessing.Process(target=writer, args=(i, barrier))
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join()
    
    f = open('/tmp/powerfs-open-test/multi_open_test.txt', 'r')
    content = f.read()
    f.close()
    
    assert 'Writer 0' in content, "Writer 0 output missing"
    assert 'Writer 1' in content, "Writer 1 output missing"
    assert 'Writer 2' in content, "Writer 2 output missing"
    
    print(f"File content:\n{content}")
    print("OK: Multiple processes can open and write to same file")
EOF
    
    echo "Test 2 PASSED"
}

test_shared_write_visibility() {
    echo ""
    echo "=== Test 3: Shared write visibility ==="
    
    rm -f "$MOUNT_DIR/shared_visibility_test.txt"
    
    python3 << 'EOF'
import os
import time
import multiprocessing

def writer_process(filename, event):
    f = open(filename, 'w')
    f.write('initial content')
    f.flush()
    event.wait()
    f.seek(0)
    f.write('updated content')
    f.flush()
    time.sleep(0.2)
    f.close()

def reader_process(filename, event):
    f = open(filename, 'r')
    content = f.read()
    assert content == 'initial content', f"Expected 'initial content', got '{content}'"
    event.set()
    time.sleep(0.5)
    f.seek(0)
    content = f.read()
    assert content == 'updated content', f"Expected 'updated content', got '{content}'"
    f.close()
    print("OK: Reader saw writer's update")

if __name__ == '__main__':
    filename = '/tmp/powerfs-open-test/shared_visibility_test.txt'
    
    event = multiprocessing.Event()
    
    writer = multiprocessing.Process(target=writer_process, args=(filename, event))
    reader = multiprocessing.Process(target=reader_process, args=(filename, event))
    
    writer.start()
    time.sleep(0.5)
    reader.start()
    
    writer.join()
    reader.join()
    
    f = open(filename, 'r')
    final_content = f.read()
    f.close()
    assert final_content == 'updated content', f"Expected 'updated content', got '{final_content}'"
    print("OK: Final file content is correct")
EOF
    
    echo "Test 3 PASSED"
}

test_concurrent_read_write() {
    echo ""
    echo "=== Test 4: Concurrent read and write ==="
    
    rm -f "$MOUNT_DIR/concurrent_test.txt"
    
    python3 << 'EOF'
import os
import time
import multiprocessing

def writer_process(filename):
    f = open(filename, 'w')
    for i in range(100):
        f.write(f'Line {i}\n')
        f.flush()
        time.sleep(0.001)
    f.close()

def reader_process(filename):
    time.sleep(0.05)
    f = open(filename, 'r')
    lines = f.readlines()
    f.close()
    print(f"Reader read {len(lines)} lines")
    assert len(lines) > 0, "No lines read"
    return len(lines)

if __name__ == '__main__':
    filename = '/tmp/powerfs-open-test/concurrent_test.txt'
    
    writer = multiprocessing.Process(target=writer_process, args=(filename,))
    readers = []
    
    writer.start()
    
    for _ in range(5):
        r = multiprocessing.Process(target=reader_process, args=(filename,))
        readers.append(r)
        r.start()
    
    writer.join()
    
    for r in readers:
        r.join()
    
    f = open(filename, 'r')
    lines = f.readlines()
    f.close()
    
    assert len(lines) == 100, f"Expected 100 lines, got {len(lines)}"
    print("OK: Concurrent read/write works correctly")
EOF
    
    echo "Test 4 PASSED"
}

main() {
    cleanup
    
    echo "=== Building release binaries ==="
    cargo build --release -p powerfs-server -p powerfs-volume -p powerfs-fuse 2>&1 | tail -1
    
    echo ""
    echo "=== Starting services ==="
    start_services
    
    echo ""
    echo "=== Running Open Semantics Tests ==="
    
    test_single_open
    test_multi_open
    test_shared_write_visibility
    test_concurrent_read_write
    
    echo ""
    echo "=== ALL OPEN SEMANTICS TESTS PASSED ==="
    
    stop_services
    cleanup
}

main "$@"
