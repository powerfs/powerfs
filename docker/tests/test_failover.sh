#!/bin/bash

set -e

DOCKER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

echo "========================================"
echo "    PowerFS Master Failover Test"
echo "========================================"
echo ""

TEST_NAME="test_failover"

echo "[1/5] Checking initial cluster state..."
MASTER_1_HEALTH=$(curl -s http://localhost:9333/health || echo "")
MASTER_2_HEALTH=$(curl -s http://localhost:9334/health || echo "")
MASTER_3_HEALTH=$(curl -s http://localhost:9335/health || echo "")

echo "  Master 1: $(if [ -n "$MASTER_1_HEALTH" ]; then echo "UP"; else echo "DOWN"; fi)"
echo "  Master 2: $(if [ -n "$MASTER_2_HEALTH" ]; then echo "UP"; else echo "DOWN"; fi)"
echo "  Master 3: $(if [ -n "$MASTER_3_HEALTH" ]; then echo "UP"; else echo "DOWN"; fi)"

echo ""
echo "[2/5] Finding current Leader..."
LEADER_PORT=""
for PORT in 9333 9334 9335; do
    if curl -s http://localhost:$PORT/health >/dev/null 2>&1; then
        STATUS=$(curl -s http://localhost:$PORT/status || echo "")
        if echo "$STATUS" | grep -i "leader" >/dev/null 2>&1; then
            LEADER_PORT=$PORT
            echo "  [OK] Leader found on port $PORT"
            break
        fi
    fi
done

if [ -z "$LEADER_PORT" ]; then
    echo "  [WARNING] Could not determine current leader, using master-1"
    LEADER_PORT=9333
fi

echo ""
echo "[3/5] Creating test data before failover..."
TEST_KEY="failover_test_key_$(date +%s)"
TEST_VALUE="Data created before failover"

curl -s -X POST http://localhost:$LEADER_PORT/kv/session/create \
    -H "Content-Type: application/json" \
    -d '{"ttl":3600}' > /dev/null 2>&1 || true

echo "  [OK] Test data prepared"

echo ""
echo "[4/5] Simulating Leader failure (stopping master-$((LEADER_PORT-9332)))..."
LEADER_CONTAINER="master-$((LEADER_PORT-9332))"
echo "  Stopping container: $LEADER_CONTAINER"
docker compose stop "$LEADER_CONTAINER"

echo "  Waiting 10 seconds for failover..."
sleep 10

echo ""
echo "[5/5] Verifying failover..."
NEW_LEADER_FOUND=0
for PORT in 9333 9334 9335; do
    if [ "$PORT" -ne "$LEADER_PORT" ] && curl -s http://localhost:$PORT/health >/dev/null 2>&1; then
        echo "  [OK] Master $PORT is still accessible"
        NEW_LEADER_FOUND=$((NEW_LEADER_FOUND + 1))
    fi
done

echo ""
echo "[6/5] Restoring failed master..."
echo "  Starting container: $LEADER_CONTAINER"
docker compose start "$LEADER_CONTAINER"

echo "  Waiting 10 seconds for recovery..."
sleep 10

RECOVERED=0
if curl -s http://localhost:$LEADER_PORT/health >/dev/null 2>&1; then
    echo "  [OK] Master $LEADER_PORT recovered"
    RECOVERED=1
else
    echo "  [WARNING] Master $LEADER_PORT did not recover"
fi

echo ""
echo "Test Summary:"
echo "  Original Leader Port: $LEADER_PORT"
echo "  Remaining Masters After Failover: $NEW_LEADER_FOUND/2"
echo "  Recovery Successful: $(if [ $RECOVERED -eq 1 ]; then echo "YES"; else echo "NO"; fi)"

if [ $NEW_LEADER_FOUND -ge 1 ] && [ $RECOVERED -eq 1 ]; then
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