#!/bin/bash

set -e

DOCKER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROJECT_DIR=$(cd "$DOCKER_DIR/.." && pwd)

echo "========================================"
echo "    Starting PowerFS Multi-Node Cluster"
echo "========================================"
echo ""

echo "[1/4] Building Docker images..."
cd "$DOCKER_DIR"
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
docker compose build 2>&1 | tail -5
echo "[OK] Images built"

echo ""
echo "[2/4] Starting Redis..."
docker compose up -d redis

echo "  Waiting for redis to be ready..."
timeout=30
while [ $timeout -gt 0 ]; do
    if docker exec redis redis-cli ping 2>/dev/null | grep -q PONG; then
        echo "  [OK] Redis ready"
        break
    fi
    sleep 1
    timeout=$((timeout - 1))
done

if [ $timeout -eq 0 ]; then
    echo "  [ERROR] Redis failed to start"
    exit 2
fi

echo ""
echo "[3/4] Starting Master nodes..."
docker compose up -d master-1

echo "  Waiting for master-1 to be ready..."
timeout=60
while [ $timeout -gt 0 ]; do
    if nc -z localhost 9333 >/dev/null 2>&1; then
        echo "  [OK] master-1 ready"
        break
    fi
    sleep 1
    timeout=$((timeout - 1))
done

if [ $timeout -eq 0 ]; then
    echo "  [ERROR] master-1 failed to start"
    exit 2
fi

docker compose up -d master-2

echo "  Waiting for master-2 to be ready..."
timeout=60
while [ $timeout -gt 0 ]; do
    if nc -z localhost 9334 >/dev/null 2>&1; then
        echo "  [OK] master-2 ready"
        break
    fi
    sleep 1
    timeout=$((timeout - 1))
done

if [ $timeout -eq 0 ]; then
    echo "  [WARNING] master-2 failed to start"
fi

docker compose up -d master-3

echo "  Waiting for master-3 to be ready..."
timeout=60
while [ $timeout -gt 0 ]; do
    if nc -z localhost 9335 >/dev/null 2>&1; then
        echo "  [OK] master-3 ready"
        break
    fi
    sleep 1
    timeout=$((timeout - 1))
done

if [ $timeout -eq 0 ]; then
    echo "  [WARNING] master-3 failed to start"
fi

echo ""
echo "[4/4] Starting Volume nodes..."
docker compose up -d volume-1 volume-2 volume-3

echo "  Waiting for volumes to register..."
sleep 5

echo ""
echo "[5/4] Starting Monitor..."
docker compose up -d monitor

echo "  Waiting for monitor to be ready..."
timeout=30
while [ $timeout -gt 0 ]; do
    if nc -z localhost 8083 >/dev/null 2>&1; then
        echo "  [OK] monitor ready"
        break
    fi
    sleep 1
    timeout=$((timeout - 1))
done

echo ""
echo "[6/4] Starting Frontend..."
docker compose up -d frontend

echo "  Waiting for frontend to be ready..."
timeout=30
while [ $timeout -gt 0 ]; do
    if nc -z localhost 8084 >/dev/null 2>&1; then
        echo "  [OK] frontend ready"
        break
    fi
    sleep 1
    timeout=$((timeout - 1))
done

echo ""
echo "[7/4] Starting S3 Server..."
docker compose up -d s3

echo "  Waiting for S3 server to be ready..."
timeout=30
while [ $timeout -gt 0 ]; do
    if nc -z localhost 9000 >/dev/null 2>&1; then
        echo "  [OK] S3 server ready"
        break
    fi
    sleep 1
    timeout=$((timeout - 1))
done

echo ""
echo "========================================"
echo "    Cluster Started Successfully!"
echo "========================================"
echo ""
echo "Service Addresses:"
echo "  Redis:           localhost:6379"
echo "  Master 1:        localhost:9333"
echo "  Master 2:        localhost:9334"
echo "  Master 3:        localhost:9335"
echo "  Volume 1:        localhost:8080"
echo "  Volume 2:        localhost:8081"
echo "  Volume 3:        localhost:8082"
echo "  Monitor API:     localhost:8083"
echo "  Monitor UI:      http://localhost:8084"
echo "  S3 Server:       localhost:9000"
echo ""
echo "S3 Compatible Endpoint:"
echo "  http://localhost:9000"
echo ""
echo "To stop the cluster, run:"
echo "  docker/scripts/stop-cluster.sh"
echo ""
echo "To check health status, run:"
echo "  docker/scripts/health-check.sh"
