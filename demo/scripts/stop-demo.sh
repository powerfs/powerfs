#!/bin/bash

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DEMO_DIR=$(dirname "$SCRIPT_DIR")

echo "============================================"
echo "      Stopping PowerFS Demo Environment      "
echo "============================================"

docker compose -f "$DEMO_DIR/docker-compose.demo.yml" down

echo ""
echo "Demo environment stopped."
echo ""