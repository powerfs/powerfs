#!/bin/bash
set -e

MOUNT_DIR="${1:-/tmp/powerfs-bench-mount}"
MASTER_DIR="${2:-/tmp/powerfs-bench-master}"
VOLUME_DIR="${3:-/tmp/powerfs-bench-volume}"

cleanup_processes() {
    echo "Cleaning up powerfs processes..."
    
    pkill -f "powerfs master" 2>/dev/null || true
    pkill -f "powerfs-volume" 2>/dev/null || true
    pkill -f "powerfs-fuse" 2>/dev/null || true
    
    sleep 1
    
    for pid in $(pgrep -f "powerfs" 2>/dev/null); do
        kill -9 "$pid" 2>/dev/null || true
    done
    
    sleep 0.5
}

cleanup_mount() {
    echo "Cleaning up mount points..."
    
    if mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
        fusermount -uz "$MOUNT_DIR" 2>/dev/null || umount -f "$MOUNT_DIR" 2>/dev/null || true
        sleep 0.5
        rm -rf "$MOUNT_DIR" 2>/dev/null || true
    elif [ -d "$MOUNT_DIR" ]; then
        rm -rf "$MOUNT_DIR" 2>/dev/null || true
    fi
}

cleanup_dirs() {
    echo "Cleaning up data directories..."
    
    rm -rf "$MASTER_DIR" 2>/dev/null || true
    rm -rf "$VOLUME_DIR" 2>/dev/null || true
}

cleanup_ports() {
    echo "Cleaning up leftover port listeners..."
    
    for port in 9333 9334 9335 8080 8081 8082 8083; do
        pid=$(ss -tlnp 2>/dev/null | grep ":$port " | grep -oE 'pid=[0-9]+' | cut -d= -f2 | head -1)
        if [ -n "$pid" ]; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
}

main() {
    echo "=== PowerFS Cleanup Script ==="
    
    cleanup_mount
    cleanup_processes
    cleanup_ports
    cleanup_dirs
    
    echo "=== Cleanup complete ==="
}

main "$@"