#!/bin/bash

set -e

POWERFS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LOG_DIR="$POWERFS_DIR/logs"
DATA_DIR="$POWERFS_DIR/data"

mkdir -p "$LOG_DIR"
mkdir -p "$DATA_DIR/master"
mkdir -p "$DATA_DIR/volume"
mkdir -p "$DATA_DIR/monitor"
mkdir -p "$DATA_DIR/fuse"

REDIS_URL="redis://localhost:6379"

echo "========================================"
echo "        PowerFS Cluster Launcher"
echo "========================================"
echo ""
echo "PowerFS Directory: $POWERFS_DIR"
echo "Log Directory:     $LOG_DIR"
echo "Data Directory:    $DATA_DIR"
echo "Redis URL:         $REDIS_URL"
echo ""

check_redis() {
    if ! redis-cli ping > /dev/null 2>&1; then
        echo "Redis not running, starting Redis..."
        sudo systemctl start redis-server || (echo "Failed to start Redis" && exit 1)
        sleep 2
    fi
    echo "[OK] Redis is running"
}

wait_for_port() {
    local host=$1
    local port=$2
    local timeout=${3:-30}
    local start=$(date +%s)
    
    echo -n "  Waiting for $host:$port..."
    while ! nc -z "$host" "$port" 2>/dev/null; do
        local now=$(date +%s)
        if [ $((now - start)) -ge $timeout ]; then
            echo " TIMEOUT"
            return 1
        fi
        sleep 1
        echo -n "."
    done
    echo " OK"
    return 0
}

start_master() {
    echo "Starting Master Service..."
    cd "$POWERFS_DIR"
    
    REDIS_URL="$REDIS_URL" \
    cargo run --bin powerfs -- --log-level info master \
        --port 9333 \
        --dir "$DATA_DIR/master" \
        --ip 0.0.0.0 \
        > "$LOG_DIR/master.log" 2>&1 &
    
    MASTER_PID=$!
    echo "Master PID: $MASTER_PID"
    echo "$MASTER_PID" > "$LOG_DIR/master.pid"
    
    wait_for_port "localhost" "9333" 45
}

start_volume() {
    echo "Starting Volume Service..."
    cd "$POWERFS_DIR"
    
    REDIS_URL="$REDIS_URL" \
    cargo run --bin powerfs-volume -- \
        --grpc-address 0.0.0.0:8080 \
        --http-port 8080 \
        --node-id volume-server-1 \
        --data-center default \
        --rack default \
        --master-address localhost:9333 \
        --data-dir "$DATA_DIR/volume" \
        --volume-size 1073741824 \
        > "$LOG_DIR/volume.log" 2>&1 &
    
    VOLUME_PID=$!
    echo "Volume PID: $VOLUME_PID"
    echo "$VOLUME_PID" > "$LOG_DIR/volume.pid"
    
    wait_for_port "localhost" "8080" 45
}

start_monitor() {
    echo "Starting Monitor Service..."
    cd "$POWERFS_DIR"
    
    REDIS_URL="$REDIS_URL" \
    cargo run --bin powerfs-monitor -- \
        --addr 0.0.0.0:8081 \
        --redis-url "$REDIS_URL" \
        > "$LOG_DIR/monitor.log" 2>&1 &
    
    MONITOR_PID=$!
    echo "Monitor PID: $MONITOR_PID"
    echo "$MONITOR_PID" > "$LOG_DIR/monitor.pid"
    
    wait_for_port "localhost" "8081" 30
}

start_frontend() {
    echo "Starting Frontend..."
    cd "$POWERFS_DIR/powerfs-monitor-frontend"
    
    pnpm run dev --host --port 5173 \
        > "$LOG_DIR/frontend.log" 2>&1 &
    
    FRONTEND_PID=$!
    echo "Frontend PID: $FRONTEND_PID"
    echo "$FRONTEND_PID" > "$LOG_DIR/frontend.pid"
    
    wait_for_port "localhost" "5173" 30
}

start_client() {
    echo "Starting FUSE Client..."
    cd "$POWERFS_DIR"
    
    mkdir -p "$DATA_DIR/fuse/mount"
    
    cargo run --bin powerfs-fuse -- \
        --mount-point "$DATA_DIR/fuse/mount" \
        --master http://localhost:9333 \
        --log-level info \
        > "$LOG_DIR/fuse.log" 2>&1 &
    
    FUSE_PID=$!
    echo "FUSE PID: $FUSE_PID"
    echo "$FUSE_PID" > "$LOG_DIR/fuse.pid"
    
    sleep 3
}

check_services() {
    echo ""
    echo "Checking service status..."
    
    local master_up=false
    local volume_up=false
    local monitor_up=false
    local frontend_up=false
    local fuse_up=false
    
    for i in {1..10}; do
        if curl -s http://localhost:9333/ > /dev/null 2>&1; then
            master_up=true
            echo "[OK] Master Service is running on http://localhost:9333"
        fi
        
        if curl -s http://localhost:8080/ > /dev/null 2>&1; then
            volume_up=true
            echo "[OK] Volume Service is running on http://localhost:8080"
        fi
        
        if curl -s http://localhost:8081/api/metrics/cluster > /dev/null 2>&1; then
            monitor_up=true
            echo "[OK] Monitor Service is running on http://localhost:8081"
        fi
        
        if curl -s http://localhost:5173/ > /dev/null 2>&1; then
            frontend_up=true
            echo "[OK] Frontend is running on http://localhost:5173"
        fi
        
        if [ -f "$LOG_DIR/fuse.pid" ]; then
            local fuse_pid=$(cat "$LOG_DIR/fuse.pid")
            if kill -0 "$fuse_pid" 2>/dev/null; then
                fuse_up=true
                echo "[OK] FUSE Client is running (PID: $fuse_pid)"
            fi
        fi
        
        if $master_up && $volume_up && $monitor_up && $frontend_up; then
            break
        fi
        
        sleep 2
    done
    
    echo ""
    if $master_up && $volume_up && $monitor_up && $frontend_up; then
        echo "========================================"
        echo "    All Services Started Successfully!"
        echo "========================================"
        echo ""
        echo "Service Addresses:"
        echo "  Master Service:     http://localhost:9333"
        echo "  Volume Service:     http://localhost:8080"
        echo "  Monitor API:        http://localhost:8081"
        echo "  Monitor Frontend:   http://localhost:5173"
        echo "                      http://192.168.3.52:5173"
        if $fuse_up; then
            echo "  FUSE Mount:         $DATA_DIR/fuse/mount"
        fi
        echo ""
        echo "Log Files:"
        echo "  Master:    $LOG_DIR/master.log"
        echo "  Volume:    $LOG_DIR/volume.log"
        echo "  Monitor:   $LOG_DIR/monitor.log"
        echo "  Frontend:  $LOG_DIR/frontend.log"
        if $fuse_up; then
            echo "  FUSE:      $LOG_DIR/fuse.log"
        fi
        echo ""
        echo "To stop all services, run:"
        echo "  ./stop-cluster.sh"
        echo ""
    else
        echo "========================================"
        echo "    Some services failed to start!"
        echo "========================================"
        echo ""
        echo "Check log files for details:"
        echo "  Master:    tail -f $LOG_DIR/master.log"
        echo "  Volume:    tail -f $LOG_DIR/volume.log"
        echo "  Monitor:   tail -f $LOG_DIR/monitor.log"
        echo "  Frontend:  tail -f $LOG_DIR/frontend.log"
        echo ""
    fi
}

echo "1. Checking Redis..."
check_redis

echo ""
echo "2. Starting Master Service..."
start_master

echo ""
echo "3. Starting Volume Service..."
start_volume

echo ""
echo "4. Starting Monitor Service..."
start_monitor

echo ""
echo "5. Starting Frontend..."
start_frontend

echo ""
echo "6. Starting FUSE Client..."
start_client

check_services
