#!/bin/bash
echo "=== Force cleanup ==="

for pid in $(lsof -ti :9340 2>/dev/null); do
    echo "Killing process $pid on port 9340"
    kill -9 "$pid" 2>/dev/null || true
done

for pid in $(lsof -ti :8090 2>/dev/null); do
    echo "Killing process $pid on port 8090"
    kill -9 "$pid" 2>/dev/null || true
done

sleep 2

rm -rf /tmp/powerfs-persistence-master /tmp/powerfs-persistence-volume /tmp/powerfs-persistence-test 2>/dev/null || true

echo "Cleanup done"
