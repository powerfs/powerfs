#!/bin/bash

set -e

FUSE_MOUNT="/tmp/powerfs-posix-test"
TEST_DATA_DIR="/tmp/powerfs-test-data"

echo "Cleaning up test environment..."

fusermount3 -u "$FUSE_MOUNT" > /dev/null 2>&1 || true
fusermount3 -zu "$FUSE_MOUNT" > /dev/null 2>&1 || true

pkill -9 -f "powerfs master" > /dev/null 2>&1 || true
pkill -9 -f "powerfs-volume" > /dev/null 2>&1 || true
pkill -9 -f "powerfs fuse" > /dev/null 2>&1 || true

rm -rf "$FUSE_MOUNT"
rm -rf "$TEST_DATA_DIR"

sleep 1

echo "Running tests..."
cargo test "$@"