#!/bin/bash

echo "=== Stopping PowerFS Master Cluster ==="

echo "Stopping master nodes..."
pkill -f 'powerfs master' || true

echo "Waiting for processes to terminate..."
sleep 2

echo "Checking remaining processes..."
remaining=$(ps aux | grep powerfs | grep -v grep | grep -v docker)
if [ -n "$remaining" ]; then
    echo "Warning: Some powerfs processes still running:"
    echo "$remaining"
else
    echo "All powerfs master processes stopped"
fi

echo "=== Cluster stopped! ==="
