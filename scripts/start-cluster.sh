#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
POWERFS_BIN="${POWERFS_BIN:-$PROJECT_ROOT/target/debug/powerfs}"
DATA_DIR="${DATA_DIR:-$SCRIPT_DIR/data}"
LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/logs}"
NODE_COUNT="${NODE_COUNT:-3}"
BASE_PORT="${BASE_PORT:-9333}"

echo "=== PowerFS Master Cluster Starter ==="
echo "Nodes: $NODE_COUNT"
echo "Base port: $BASE_PORT"
echo "Data dir: $DATA_DIR"
echo "Log dir: $LOG_DIR"
echo ""

mkdir -p "$DATA_DIR"
mkdir -p "$LOG_DIR"

if [ ! -f "$POWERFS_BIN" ]; then
    echo "Building powerfs..."
    cd "$PROJECT_ROOT" && cargo build
fi

PEER_ARGS=""
for i in $(seq 1 $NODE_COUNT); do
    RAFT_PORT=$((BASE_PORT + i))
    PEER_ARGS="$PEER_ARGS -p localhost:$RAFT_PORT"
done

echo "Raft peers: $PEER_ARGS"
echo ""

for i in $(seq 1 $NODE_COUNT); do
    NODE_ID=$i
    PORT=$((BASE_PORT + i))
    NODE_DATA_DIR="$DATA_DIR/node$i"
    NODE_LOG_FILE="$LOG_DIR/node$i.log"

    mkdir -p "$NODE_DATA_DIR"

    echo "Starting Master Node $NODE_ID..."
    echo "  Port: $PORT"
    echo "  Data: $NODE_DATA_DIR"
    echo "  Log:  $NODE_LOG_FILE"

    "$POWERFS_BIN" master \
        -i "$NODE_ID" \
        -P "$PORT" \
        -D "$NODE_DATA_DIR" \
        --ip "0.0.0.0" \
        $PEER_ARGS \
        > "$NODE_LOG_FILE" 2>&1 &

    echo "  PID: $!"
    echo ""

    sleep 2
done

echo "=== Cluster started! ==="
echo "To stop: pkill -f 'powerfs master'"
echo "To check logs: tail -f $LOG_DIR/*.log"
