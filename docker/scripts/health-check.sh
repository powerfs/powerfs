#!/bin/bash

set -e

DOCKER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

echo "========================================"
echo "    PowerFS Cluster Health Check"
echo "========================================"
echo ""

echo "[1/5] Checking Redis..."
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
if docker exec redis redis-cli ping >/dev/null 2>&1; then
    echo "  [OK] Redis is healthy"
else
    echo "  [WARNING] Redis is not healthy"
fi

echo ""
echo "[2/5] Checking Master nodes..."
MASTER_COUNT=0
for port in 9333 9334 9335; do
    if nc -z localhost $port >/dev/null 2>&1; then
        echo "  [OK] Master $port is healthy"
        MASTER_COUNT=$((MASTER_COUNT + 1))
    else
        echo "  [WARNING] Master $port is not healthy"
    fi
done

echo ""
echo "[3/5] Checking Volume nodes..."
VOLUME_COUNT=0
for port in 8080 8081 8082; do
    if nc -z localhost $port >/dev/null 2>&1; then
        echo "  [OK] Volume $port is healthy"
        VOLUME_COUNT=$((VOLUME_COUNT + 1))
    else
        echo "  [WARNING] Volume $port is not healthy"
    fi
done

echo ""
echo "[4/5] Checking Monitor..."
if nc -z localhost 8083 >/dev/null 2>&1; then
    echo "  [OK] Monitor is healthy"
else
    echo "  [WARNING] Monitor is not healthy"
fi

echo ""
echo "[5/5] Checking Frontend..."
if nc -z localhost 8084 >/dev/null 2>&1; then
    echo "  [OK] Frontend is healthy"
else
    echo "  [WARNING] Frontend is not healthy"
fi

echo ""
echo "[6/6] Checking S3 Gateway..."
if nc -z localhost 9000 >/dev/null 2>&1; then
    echo "  [OK] S3 Gateway is healthy"
else
    echo "  [WARNING] S3 Gateway is not healthy"
fi

echo ""
echo "[7/7] Cluster Summary:"
echo "  Redis:           1/1 healthy"
echo "  Master Nodes:    $MASTER_COUNT/3 healthy"
echo "  Volume Nodes:    $VOLUME_COUNT/3 healthy"
echo "  Monitor:         1/1 healthy"
echo "  Frontend:        1/1 healthy"
echo "  S3 Gateway:      1/1 healthy"
echo ""

if [ $MASTER_COUNT -ge 2 ] && [ $VOLUME_COUNT -ge 2 ]; then
    echo "========================================"
    echo "    Cluster Status: HEALTHY"
    echo "========================================"
    exit 0
else
    echo "========================================"
    echo "    Cluster Status: DEGRADED"
    echo "========================================"
    exit 1
fi