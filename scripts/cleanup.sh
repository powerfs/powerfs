#!/bin/bash
# PowerFS Quick Cleanup Script
# Simple cleanup - delegates to full cleanup in env/cleanup.sh

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

echo "=== PowerFS Quick Cleanup ==="
echo ""

# Use the full cleanup script
source "$PROJECT_ROOT/scripts/lib/common.sh"

setup_test_env
cleanup_test_env

# Also clean up common mount points
for mp in /tmp/powerfs-perf-test /tmp/powerfs-bench-mount /mnt/powerfs/test; do
    if mountpoint -q "$mp" 2>/dev/null; then
        echo "Unmounting $mp..."
        fusermount -uz "$mp" 2>/dev/null || umount -f "$mp" 2>/dev/null || true
    fi
done

# Kill any remaining processes
pkill -9 -f "powerfs-master" 2>/dev/null || true
pkill -9 -f "powerfs-filer" 2>/dev/null || true
pkill -9 -f "powerfs-volume" 2>/dev/null || true
pkill -9 -f "powerfs-fuse" 2>/dev/null || true
pkill -9 -f "powerfs-s3" 2>/dev/null || true
pkill -9 -f "powerfs-monitor" 2>/dev/null || true

# Clean up temp directories
rm -rf /tmp/powerfs-* 2>/dev/null || true

echo ""
echo "=== Cleanup complete ==="
echo "For full cleanup including Docker, use: $PROJECT_ROOT/scripts/env/cleanup.sh --docker"
