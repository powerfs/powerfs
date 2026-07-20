#!/bin/bash

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DEMO_DIR=$(dirname "$SCRIPT_DIR")

echo "============================================"
echo "      PowerFS Demo Environment Setup         "
echo "============================================"

echo ""
echo "1. Creating FUSE mount directories..."
mkdir -p /tmp/powerfs-demo/fuse1 /tmp/powerfs-demo/fuse2

echo ""
echo "2. Building Docker images..."
cd "$DEMO_DIR/.."
docker compose -f "$DEMO_DIR/docker-compose.demo.yml" build

echo ""
echo "3. Starting demo environment..."
docker compose -f "$DEMO_DIR/docker-compose.demo.yml" up -d

echo ""
echo "4. Waiting for services to start..."
echo "   This may take 30-60 seconds..."
sleep 10

echo ""
echo "============================================"
echo "      Demo Environment Ready!                "
echo "============================================"
echo ""
echo "Services:"
echo "  - Frontend (Monitor UI):    http://localhost:8084"
echo "  - S3 API:                   http://localhost:9000"
echo "  - Master Nodes:             localhost:9333, 9334, 9335"
echo "  - Volume Nodes:             localhost:8080, 8081, 8082"
echo "  - Redis:                    localhost:6379"
echo "  - FUSE Mount 1:             /tmp/powerfs-demo/fuse1"
echo "  - FUSE Mount 2:             /tmp/powerfs-demo/fuse2"
echo ""
echo "Run benchmarks:"
echo "  cd $DEMO_DIR && ./scripts/run-benchmarks.sh"
echo ""
echo "Stop demo:"
echo "  cd $DEMO_DIR && ./scripts/stop-demo.sh"
echo ""