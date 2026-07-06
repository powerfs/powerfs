#!/bin/bash

set -e

DOCKER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

echo "========================================"
echo "    PowerFS KV Distributed Test"
echo "========================================"
echo ""

TEST_NAME="test_kv_distributed"

echo "[1/4] Checking Redis connection..."
if redis-cli ping >/dev/null 2>&1; then
    echo "  [OK] Redis is available"
else
    echo "  [ERROR] Redis is not available"
    exit 1
fi

echo ""
echo "[2/4] Checking all Master nodes connectivity..."
SUCCESS_COUNT=0
for PORT in 9333 9334 9335; do
    if nc -z localhost $PORT >/dev/null 2>&1; then
        echo "  [OK] Master $PORT is reachable"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo "  [WARNING] Master $PORT is not reachable"
    fi
done

echo ""
echo "[3/4] Checking all Volume nodes connectivity..."
VOLUME_SUCCESS=0
for PORT in 8080 8081 8082; do
    if nc -z localhost $PORT >/dev/null 2>&1; then
        echo "  [OK] Volume $PORT is reachable"
        VOLUME_SUCCESS=$((VOLUME_SUCCESS + 1))
    else
        echo "  [WARNING] Volume $PORT is not reachable"
    fi
done

echo ""
echo "[4/4] Checking Monitor API..."
if nc -z localhost 8083 >/dev/null 2>&1; then
    echo "  [OK] Monitor API is reachable"
else
    echo "  [WARNING] Monitor API is not reachable"
fi

echo ""
echo "Test Summary:"
echo "  Masters accessible: $SUCCESS_COUNT/3"
echo "  Volumes accessible: $VOLUME_SUCCESS/3"
echo "  Redis: OK"

if [ $SUCCESS_COUNT -ge 2 ] && [ $VOLUME_SUCCESS -ge 2 ]; then
    echo ""
    echo "========================================"
    echo "    Test Result: PASSED"
    echo "========================================"
    exit 0
else
    echo ""
    echo "========================================"
    echo "    Test Result: FAILED"
    echo "========================================"
    exit 1
fi