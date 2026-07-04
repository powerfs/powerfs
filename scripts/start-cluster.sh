#!/bin/bash

set -e

POWERFS_BIN="${POWERFS_BIN:-./target/debug/powerfs-server}"
DATA_DIR="${DATA_DIR:-./data}"
LOG_DIR="${LOG_DIR:-./logs}"
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
    echo "Building powerfs-server..."
    cargo build -p powerfs-server
fi

for i in $(seq 1 $NODE_COUNT); do
    NODE_ID=$i
    HTTP_PORT=$((BASE_PORT + i))
    GRPC_PORT=$((BASE_PORT + 10 + i))
    RAFT_PORT=$((BASE_PORT + 20 + i))
    NODE_DATA_DIR="$DATA_DIR/node$i"
    NODE_LOG_FILE="$LOG_DIR/node$i.log"

    mkdir -p "$NODE_DATA_DIR"

    echo "Starting Master Node $NODE_ID..."
    echo "  HTTP: http://localhost:$HTTP_PORT"
    echo "  gRPC: http://localhost:$GRPC_PORT"
    echo "  Raft: http://localhost:$RAFT_PORT"
    echo "  Data: $NODE_DATA_DIR"
    echo "  Log:  $NODE_LOG_FILE"

    $POWERFS_BIN master \
        --id "$NODE_ID" \
        --http-address "0.0.0.0:$HTTP_PORT" \
        --grpc-address "0.0.0.0:$GRPC_PORT" \
        --raft-address "0.0.0.0:$RAFT_PORT" \
        --data-dir "$NODE_DATA_DIR" \
        --log-file "$NODE_LOG_FILE" \
        &

    echo "  PID: $!"
    echo ""

    sleep 1
done

echo "=== Cluster started! ==="
echo "To stop: pkill -f powerfs-server"
echo "To check logs: tail -f $LOG_DIR/*.log"