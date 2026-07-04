#!/bin/bash

POWERFS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LOG_DIR="$POWERFS_DIR/logs"
DATA_DIR="$POWERFS_DIR/data"

stop_service() {
    local name=$1
    local pid_file="$LOG_DIR/${name}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping $name (PID: $pid)..."
            kill -TERM "$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null
            sleep 2
            pkill -P "$pid" 2>/dev/null || true
            if kill -0 "$pid" 2>/dev/null; then
                echo "  Failed to stop $name"
            else
                echo "  $name stopped"
                rm -f "$pid_file"
            fi
        else
            echo "$name is not running"
            rm -f "$pid_file"
        fi
    else
        echo "$name PID file not found"
    fi
}

unmount_fuse() {
    local mount_point="$DATA_DIR/fuse/mount"
    if mountpoint -q "$mount_point" 2>/dev/null; then
        echo "Unmounting FUSE at $mount_point..."
        fusermount -u "$mount_point" 2>/dev/null || umount -f "$mount_point" 2>/dev/null
        if mountpoint -q "$mount_point" 2>/dev/null; then
            echo "  Failed to unmount FUSE"
        else
            echo "  FUSE unmounted"
        fi
    fi
}

echo "========================================"
echo "        Stopping PowerFS Cluster"
echo "========================================"
echo ""

stop_service "frontend"
stop_service "monitor"
stop_service "volume"
stop_service "master"
stop_service "fuse"

unmount_fuse

echo ""
echo "All services stopped."
echo "Log files are still available in: $LOG_DIR"
