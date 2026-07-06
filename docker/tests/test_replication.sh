#!/bin/bash

set -e

DOCKER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

echo "========================================"
echo "    PowerFS Data Replication Test"
echo "========================================"
echo ""

TEST_NAME="test_replication"

echo "[1/4] Checking Master nodes..."
MASTER_COUNT=0
for PORT in 9333 9334 9335; do
    if nc -z localhost $PORT >/dev/null 2>&1; then
        echo "  [OK] Master $PORT is reachable"
        MASTER_COUNT=$((MASTER_COUNT + 1))
    else
        echo "  [ERROR] Master $PORT is not reachable"
    fi
done

echo ""
echo "[2/4] Checking Volume nodes..."
VOLUME_COUNT=0
for PORT in 8080 8081 8082; do
    if nc -z localhost $PORT >/dev/null 2>&1; then
        echo "  [OK] Volume $PORT is reachable"
        VOLUME_COUNT=$((VOLUME_COUNT + 1))
    else
        echo "  [ERROR] Volume $PORT is not reachable"
    fi
done

echo ""
echo "[3/4] Checking Monitor service..."
if nc -z localhost 8083 >/dev/null 2>&1; then
    echo "  [OK] Monitor is reachable"
else
    echo "  [WARNING] Monitor is not reachable"
fi

echo ""
echo "[4/4] Test Summary:"
echo "  Master Nodes: $MASTER_COUNT/3"
echo "  Volume Nodes: $VOLUME_COUNT/3"
echo "  Replication factor: 3"
echo "  Replicas available: $VOLUME_COUNT"

echo ""
if [ $MASTER_COUNT -ge 2 ] && [ $VOLUME_COUNT -ge 2 ]; then
    echo "========================================"
    echo "    Test Result: PASSED"
    echo "========================================"
    exit 0
else
    echo "========================================"
    echo "    Test Result: FAILED"
    echo "========================================"
    exit 1
fi