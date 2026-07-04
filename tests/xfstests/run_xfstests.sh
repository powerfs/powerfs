#!/bin/bash
set -e

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
TEST_DIR=${TEST_DIR:-/mnt/powerfs}
SCRATCH_MNT=${SCRATCH_MNT:-/mnt/powerfs-scratch}

echo "=== PowerFS xfstests Runner ==="
echo "Project root: $PROJECT_ROOT"
echo "Test dir: $TEST_DIR"
echo "Scratch mount: $SCRATCH_MNT"
echo ""

if ! command -v xfstests &> /dev/null; then
    echo "WARNING: xfstests not installed. Installing..."
    echo "  On Ubuntu: sudo apt install xfstests-bpf"
    echo "  On Fedora: sudo dnf install xfstests"
    echo "  From source: https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git"
    exit 0
fi

echo "=== Building PowerFS ==="
cd "$PROJECT_ROOT"
cargo build --release -p powerfs-server -p powerfs-volume -p powerfs-fuse 2>&1 | tail -5

echo ""
echo "=== Starting PowerFS services ==="
MASTER_DIR=$(mktemp -d)
VOLUME_DIR=$(mktemp -d)
MASTER_PORT=9333
VOLUME_GRPC_PORT=8081
VOLUME_HTTP_PORT=8080

./target/release/powerfs master \
    --port $MASTER_PORT \
    --dir "$MASTER_DIR" \
    2>&1 > /tmp/powerfs-master-xfstests.log &
MASTER_PID=$!

sleep 3

./target/release/powerfs-volume \
    --grpc-address "127.0.0.1:$VOLUME_GRPC_PORT" \
    --http-port $VOLUME_HTTP_PORT \
    --node-id "xfstests-node" \
    --master-address "127.0.0.1:$MASTER_PORT" \
    --data-dir "$VOLUME_DIR" \
    2>&1 > /tmp/powerfs-volume-xfstests.log &
VOLUME_PID=$!

sleep 3

echo ""
echo "=== Mounting PowerFS ==="
mkdir -p "$TEST_DIR"
./target/release/powerfs-fuse \
    --master "127.0.0.1:$MASTER_PORT" \
    --mount-point "$TEST_DIR" \
    2>&1 > /tmp/powerfs-fuse-xfstests.log &
FUSE_PID=$!

sleep 3

echo ""
echo "=== Running xfstests ==="
echo "Services:"
echo "  Master PID: $MASTER_PID"
echo "  Volume PID: $VOLUME_PID"
echo "  FUSE PID: $FUSE_PID"
echo ""

if [ -z "$1" ]; then
    echo "Running generic test group..."
    xfstests -c "$SCRIPT_DIR/powerfs.conf" -g generic 2>&1 | tee /tmp/powerfs-xfstests-output.log
else
    echo "Running test group: $1"
    xfstests -c "$SCRIPT_DIR/powerfs.conf" -g "$1" 2>&1 | tee /tmp/powerfs-xfstests-output.log
fi

echo ""
echo "=== Cleaning up ==="
fusermount -u "$TEST_DIR" 2>/dev/null || umount "$TEST_DIR" 2>/dev/null || true
kill $FUSE_PID 2>/dev/null || true
kill $VOLUME_PID 2>/dev/null || true
kill $MASTER_PID 2>/dev/null || true

rm -rf "$MASTER_DIR" "$VOLUME_DIR"

echo "Done. Logs saved to /tmp/powerfs-*-xfstests*.log"
