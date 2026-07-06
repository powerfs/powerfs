#!/bin/bash

set -e

DOCKER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

echo "========================================"
echo "    Stopping PowerFS Multi-Node Cluster"
echo "========================================"
echo ""

echo "[1/3] Stopping containers..."
cd "$DOCKER_DIR"
docker compose down 2>&1 | tail -3
echo "[OK] Containers stopped"

echo ""
echo "[2/3] Cleaning up unused resources..."
docker system prune -f 2>&1 | tail -1
echo "[OK] Resources cleaned"

echo ""
echo "[3/3] Removing volumes (optional)..."
read -p "Remove all persistent volumes? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker compose down -v 2>&1 | tail -1
    echo "[OK] Volumes removed"
else
    echo "[SKIP] Volumes preserved"
fi

echo ""
echo "========================================"
echo "    Cluster Stopped Successfully!"
echo "========================================"
echo ""
echo "To restart the cluster, run:"
echo "  docker/scripts/start-cluster.sh"