#!/bin/bash

set -e

DOCKER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

echo "========================================"
echo "    PowerFS Load Balance Test"
echo "========================================"
echo ""

TEST_NAME="test_load_balance"
NUM_REQUESTS=100

echo "[1/4] Generating test requests..."
echo "  Sending $NUM_REQUESTS assign requests across all master nodes"

REQUEST_COUNTS=(0 0 0)
ERROR_COUNTS=(0 0 0)

echo ""
echo "[2/4] Distributing requests..."
for i in $(seq 1 $NUM_REQUESTS); do
    MASTER_INDEX=$((i % 3))
    PORT=$((9333 + MASTER_INDEX))
    
    if curl -s -X POST http://localhost:$PORT/assign \
        -H "Content-Type: application/json" \
        -d '{"replication":"001","ttl":3600}' > /dev/null 2>&1; then
        REQUEST_COUNTS[$MASTER_INDEX]=$((REQUEST_COUNTS[$MASTER_INDEX] + 1))
    else
        ERROR_COUNTS[$MASTER_INDEX]=$((ERROR_COUNTS[$MASTER_INDEX] + 1))
    fi
    
    if [ $((i % 20)) -eq 0 ]; then
        echo "  Progress: $i/$NUM_REQUESTS"
    fi
done

echo ""
echo "[3/4] Analyzing distribution..."
echo "  Request Distribution:"
echo "    Master 1 (9333): ${REQUEST_COUNTS[0]} requests, ${ERROR_COUNTS[0]} errors"
echo "    Master 2 (9334): ${REQUEST_COUNTS[1]} requests, ${ERROR_COUNTS[1]} errors"
echo "    Master 3 (9335): ${REQUEST_COUNTS[2]} requests, ${ERROR_COUNTS[2]} errors"

TOTAL_ERRORS=$((ERROR_COUNTS[0] + ERROR_COUNTS[1] + ERROR_COUNTS[2]))
SUCCESS_RATE=$(( (NUM_REQUESTS - TOTAL_ERRORS) * 100 / NUM_REQUESTS ))

echo ""
echo "  Success Rate: $SUCCESS_RATE%"

echo ""
echo "[4/4] Checking Volume distribution..."
VOLUME_STATS=""
for PORT in 8080 8081 8082; do
    if curl -s http://localhost:$PORT/health >/dev/null 2>&1; then
        echo "  [OK] Volume $PORT is healthy"
    else
        echo "  [WARNING] Volume $PORT is not accessible"
    fi
done

echo ""
echo "Test Summary:"
echo "  Total Requests: $NUM_REQUESTS"
echo "  Total Errors: $TOTAL_ERRORS"
echo "  Success Rate: $SUCCESS_RATE%"

if [ $SUCCESS_RATE -ge 90 ]; then
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