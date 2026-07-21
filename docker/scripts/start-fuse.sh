#!/bin/bash

set -e

BUILD_IMAGES=false
REBUILD_CODE=false
VERBOSE_LOG=false
ENTERPRISE_FUSE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build|-b)
            BUILD_IMAGES=true
            shift
            ;;
        --rebuild|-bb)
            REBUILD_CODE=true
            BUILD_IMAGES=true
            shift
            ;;
        --verbose|-v)
            VERBOSE_LOG=true
            shift
            ;;
        --enterprise)
            ENTERPRISE_FUSE=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

DOCKER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROJECT_DIR=$(cd "$DOCKER_DIR/.." && pwd)
HOST_IP=$(hostname -I | awk '{print $1}')
START_TIME=$(date +%s)

log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $1"
}

log_warn() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] $1"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $1"
}

log_debug() {
    if [ "$VERBOSE_LOG" = true ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [DEBUG] $1"
    fi
}

log_info "========================================"
log_info "    Starting PowerFS FUSE Test Environment"
log_info "========================================"
log_info ""
log_info "Configuration:"
log_info "  Host IP:         $HOST_IP"
log_info "  Build images:    $BUILD_IMAGES"
log_info "  Rebuild code:    $REBUILD_CODE"
log_info "  Verbose mode:    $VERBOSE_LOG"
log_info "  Enterprise FUSE: $ENTERPRISE_FUSE"
log_info "  Docker dir:      $DOCKER_DIR"
log_info "  Project dir:     $PROJECT_DIR"
log_info ""

log_info "[PRE-CHECK] Performing environment pre-checks..."

if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed or not in PATH"
    log_error "Please install Docker and try again"
    exit 1
fi
log_info "  [OK] Docker is available"

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    log_error "Docker Compose is not available"
    log_error "Please install Docker Compose and try again"
    exit 1
fi
log_info "  [OK] Docker Compose is available"

if ! command -v nc &> /dev/null; then
    log_warn "netcat (nc) is not installed, using alternative port checking method"
    PORT_CHECK_CMD="bash -c '</dev/tcp/localhost/%PORT%' 2>/dev/null"
else
    PORT_CHECK_CMD="nc -z localhost %PORT%"
fi
log_info "  [OK] Port checking command is available"

if [ ! -f "$DOCKER_DIR/docker-compose.yml" ]; then
    log_error "docker-compose.yml not found in $DOCKER_DIR"
    exit 1
fi
log_info "  [OK] docker-compose.yml exists"

log_info "  Creating FUSE mount directories..."
mkdir -p /tmp/powerfs/fuse1
mkdir -p /tmp/powerfs/fuse2
log_info "  [OK] Mount directories created: /tmp/powerfs/fuse1, /tmp/powerfs/fuse2"

log_info "[PRE-CHECK] Pre-checks completed"
log_info ""

if [ "$BUILD_IMAGES" = true ]; then
    log_info "[1/8] Building Docker images..."
    cd "$DOCKER_DIR"
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

    log_info "  Step 1: Cleaning up old containers and volumes..."
    docker compose -f "$DOCKER_DIR/docker-compose.yml" down -v 2>&1 | tail -5 || true
    log_info "  [OK] Old containers and volumes cleaned up"

    log_info "  Step 2: Removing old Docker images..."
    IMAGES_TO_REMOVE=("powerfs:latest" "powerfs-frontend:latest")
    for img in "${IMAGES_TO_REMOVE[@]}"; do
        if docker images -q "$img" | grep -q .; then
            log_debug "  Removing $img..."
            docker rmi -f "$img" 2>&1 | tail -1 || true
        fi
    done
    log_info "  [OK] Old Docker images removed"

    if [ "$REBUILD_CODE" = true ]; then
        log_info "  Step 3: Building Rust binaries..."
        cd "$PROJECT_DIR"
        
        log_info "  Step 3a: Cleaning old build artifacts..."
        if cargo clean 2>&1 | tail -2; then
            log_info "  [OK] Build artifacts cleaned"
        else
            log_warn "  [WARN] Failed to clean build artifacts (may be in use)"
        fi
        
        BUILD_TIME=$(date '+%Y-%m-%d %H:%M:%S')
        log_info "  Step 3b: Starting build at $BUILD_TIME..."
        
        BUILD_CMD="cargo build --release --bin powerfs --bin powerfs-volume --bin powerfs-monitor"
        
        if [ "$ENTERPRISE_FUSE" = true ]; then
            log_debug "  Running: $BUILD_CMD --bin powerfs-fuse --features powerfs-fuse/enterprise"
            BUILD_CMD="$BUILD_CMD --bin powerfs-fuse --features powerfs-fuse/enterprise"
        else
            log_debug "  Running: $BUILD_CMD --bin powerfs-fuse"
            BUILD_CMD="$BUILD_CMD --bin powerfs-fuse"
        fi
        
        if $BUILD_CMD 2>&1 | tail -10; then
            log_info "  [OK] Rust binaries built successfully"
        else
            log_error "  [FAIL] Failed to build Rust binaries"
            log_error "  Check cargo build output for detailed errors"
            exit 2
        fi
    else
        log_info "  Step 3: Skipping Rust code rebuild (use -bb flag to rebuild)"
    fi

    log_info "  Step 4: Building Docker image..."
    cd "$DOCKER_DIR"
    log_debug "  Running: docker compose -f $DOCKER_DIR/docker-compose.yml build"

    if docker compose -f "$DOCKER_DIR/docker-compose.yml" build 2>&1 | tail -10; then
        log_info "[OK] Docker images built successfully"
    else
        log_error "[FAIL] Failed to build Docker images"
        log_error "Check docker compose build output for detailed errors"
        exit 2
    fi
else
    log_info "[1/8] Using existing Docker images..."
    log_info "  Use --build or -b flag to rebuild images"
    
    log_info "  Verifying required images exist..."
    IMAGES_NEEDED=("powerfs:latest" "powerfs-frontend:latest")
    for img in "${IMAGES_NEEDED[@]}"; do
        if docker images -q "$img" | grep -q .; then
            log_info "  [OK] Image exists: $img"
        else
            log_warn "  [WARN] Image not found: $img"
            log_warn "  Consider running with -b flag to build images"
        fi
    done
    log_info "[OK] Using existing images"
fi

log_info ""
log_info "[2/8] Starting Redis..."
log_debug "  Running: docker compose up -d redis"

if docker compose -f "$DOCKER_DIR/docker-compose.yml" up -d redis; then
    log_info "  [OK] Redis container started"
else
    log_error "  [FAIL] Failed to start Redis container"
    log_error "  Check: docker logs redis"
    exit 2
fi

log_info "  Waiting for Redis to be ready..."
timeout=30
attempt=0
while [ $timeout -gt 0 ]; do
    attempt=$((attempt + 1))
    log_debug "  Attempt $attempt/$timeout: Checking Redis connection..."
    
    if docker exec redis redis-cli ping 2>/dev/null | grep -q PONG; then
        log_info "  [OK] Redis ready after $attempt attempts"
        break
    fi
    
    if [ $((attempt % 5)) -eq 0 ]; then
        log_warn "  [WARN] Redis not ready yet (attempt $attempt/$timeout)"
        log_debug "  Redis container status: $(docker inspect -f '{{.State.Status}}' redis 2>/dev/null || echo 'unknown')"
    fi
    
    sleep 1
    timeout=$((timeout - 1))
done

if [ $timeout -eq 0 ]; then
    log_error "  [ERROR] Redis failed to start within timeout"
    log_error "  Container status: $(docker inspect -f '{{.State.Status}}' redis 2>/dev/null || echo 'unknown')"
    log_error "  Last 20 lines of Redis logs:"
    docker logs redis 2>/dev/null | tail -20 || true
    exit 2
fi

log_info ""
log_info "[3/8] Starting Master nodes..."

MASTER_NODES=("master-1" "master-2" "master-3")
MASTER_PORTS=("9333" "9334" "9335")
MASTER_REQUIRED=("true" "false" "false")

for i in "${!MASTER_NODES[@]}"; do
    node=${MASTER_NODES[$i]}
    port=${MASTER_PORTS[$i]}
    required=${MASTER_REQUIRED[$i]}
    
    log_info "  Starting $node..."
    log_debug "  Running: docker compose -f $DOCKER_DIR/docker-compose.yml up -d --no-deps $node"

    if docker compose -f "$DOCKER_DIR/docker-compose.yml" up -d --no-deps "$node"; then
        log_info "  [OK] $node container started"
    else
        if [ "$required" = "true" ]; then
            log_error "  [FAIL] Failed to start $node (required)"
            log_error "  Check: docker logs $node"
            exit 2
        else
            log_warn "  [WARN] Failed to start $node (optional)"
            continue
        fi
    fi
    
    log_info "  Waiting for $node to be ready on port $port..."
    timeout=60
    attempt=0
    while [ $timeout -gt 0 ]; do
        attempt=$((attempt + 1))
        log_debug "  Attempt $attempt/$timeout: Checking port $port..."
        
        if nc -z localhost "$port" >/dev/null 2>&1; then
            log_info "  [OK] $node ready on port $port after $attempt attempts"
            break
        fi
        
        if [ $((attempt % 10)) -eq 0 ]; then
            log_warn "  [WARN] $node not ready yet (attempt $attempt/$timeout)"
            log_debug "  Container status: $(docker inspect -f '{{.State.Status}}' "$node" 2>/dev/null || echo 'unknown')"
        fi
        
        sleep 1
        timeout=$((timeout - 1))
    done
    
    if [ $timeout -eq 0 ]; then
        if [ "$required" = "true" ]; then
            log_error "  [ERROR] $node failed to start within timeout"
            log_error "  Container status: $(docker inspect -f '{{.State.Status}}' "$node" 2>/dev/null || echo 'unknown')"
            log_error "  Last 20 lines of $node logs:"
            docker logs "$node" 2>/dev/null | tail -20 || true
            exit 2
        else
            log_warn "  [WARN] $node may not be running properly"
            log_warn "  Check: docker logs $node"
        fi
    fi
done

log_info ""
log_info "[4/8] Starting Volume nodes..."
log_debug "  Running: docker compose -f $DOCKER_DIR/docker-compose.yml up -d --no-deps volume-1 volume-2 volume-3"

if docker compose -f "$DOCKER_DIR/docker-compose.yml" up -d --no-deps volume-1 volume-2 volume-3; then
    log_info "  [OK] Volume containers started"
else
    log_error "  [FAIL] Failed to start Volume containers"
    exit 2
fi

log_info "  Waiting for volumes to register with master..."
wait_time=5
for i in $(seq 1 $wait_time); do
    log_debug "  Waiting... ($i/$wait_time)"
    sleep 1
done

log_info "  Verifying Volume nodes status..."
VOLUME_NODES=("volume-1" "volume-2" "volume-3")
VOLUME_PORTS=("8080" "8081" "8082")

for i in "${!VOLUME_NODES[@]}"; do
    node=${VOLUME_NODES[$i]}
    port=${VOLUME_PORTS[$i]}
    
    if nc -z localhost "$port" >/dev/null 2>&1; then
        log_info "  [OK] $node is listening on port $port"
    else
        log_warn "  [WARN] $node is not listening on port $port"
        log_debug "  Container status: $(docker inspect -f '{{.State.Status}}' "$node" 2>/dev/null || echo 'unknown')"
    fi
done

log_info ""
log_info "[5/9] Starting Filer..."
log_debug "  Running: docker compose -f $DOCKER_DIR/docker-compose.yml up -d --no-deps filer"

if docker compose -f "$DOCKER_DIR/docker-compose.yml" up -d --no-deps filer; then
    log_info "  [OK] Filer container started"
else
    log_error "  [FAIL] Failed to start Filer container"
    exit 2
fi

log_info "  Waiting for Filer to be ready..."
timeout=30
attempt=0
while [ $timeout -gt 0 ]; do
    attempt=$((attempt + 1))
    log_debug "  Attempt $attempt/$timeout: Checking port 8888..."
    
    if nc -z localhost 8888 >/dev/null 2>&1; then
        log_info "  [OK] Filer ready on port 8888 after $attempt attempts"
        break
    fi
    
    if [ $((attempt % 10)) -eq 0 ]; then
        log_warn "  [WARN] Filer not ready yet (attempt $attempt/$timeout)"
    fi
    
    sleep 1
    timeout=$((timeout - 1))
done

if [ $timeout -eq 0 ]; then
    log_warn "  [WARN] Filer may not be ready"
    log_warn "  Check: docker logs filer"
fi

log_info ""
log_info "[6/9] Starting S3 Backend..."
log_debug "  Running: docker compose -f $DOCKER_DIR/docker-compose.yml up -d --no-deps s3"

if docker compose -f "$DOCKER_DIR/docker-compose.yml" up -d --no-deps s3; then
    log_info "  [OK] S3 container started"
else
    log_error "  [FAIL] Failed to start S3 container"
    exit 2
fi

log_info "  Waiting for S3 backend to be ready..."
timeout=30
attempt=0
while [ $timeout -gt 0 ]; do
    attempt=$((attempt + 1))
    log_debug "  Attempt $attempt/$timeout: Checking port 9000..."
    
    if nc -z localhost 9000 >/dev/null 2>&1; then
        log_info "  [OK] S3 backend ready on port 9000 after $attempt attempts"
        break
    fi
    
    if [ $((attempt % 10)) -eq 0 ]; then
        log_warn "  [WARN] S3 not ready yet (attempt $attempt/$timeout)"
    fi
    
    sleep 1
    timeout=$((timeout - 1))
done

if [ $timeout -eq 0 ]; then
    log_warn "  [WARN] S3 backend may not be ready"
    log_warn "  Check: docker logs s3"
fi

log_info ""
log_info "[7/9] Starting Monitor..."
log_debug "  Running: docker compose -f $DOCKER_DIR/docker-compose.yml up -d --no-deps monitor"

if docker compose -f "$DOCKER_DIR/docker-compose.yml" up -d --no-deps monitor; then
    log_info "  [OK] Monitor container started"
else
    log_error "  [FAIL] Failed to start Monitor container"
    exit 2
fi

log_info "  Waiting for Monitor to be ready..."
timeout=30
attempt=0
while [ $timeout -gt 0 ]; do
    attempt=$((attempt + 1))
    log_debug "  Attempt $attempt/$timeout: Checking port 8083..."
    
    if nc -z localhost 8083 >/dev/null 2>&1; then
        log_info "  [OK] Monitor ready on port 8083 after $attempt attempts"
        break
    fi
    
    if [ $((attempt % 10)) -eq 0 ]; then
        log_warn "  [WARN] Monitor not ready yet (attempt $attempt/$timeout)"
    fi
    
    sleep 1
    timeout=$((timeout - 1))
done

if [ $timeout -eq 0 ]; then
    log_warn "  [WARN] Monitor may not be ready"
    log_warn "  Check: docker logs monitor"
fi

log_info ""
log_info "[8/9] Starting Frontend..."
log_debug "  Running: docker compose -f $DOCKER_DIR/docker-compose.yml up -d --no-deps frontend"

if docker compose -f "$DOCKER_DIR/docker-compose.yml" up -d --no-deps frontend; then
    log_info "  [OK] Frontend container started"
else
    log_error "  [FAIL] Failed to start Frontend container"
    exit 2
fi

log_info "  Waiting for Frontend to be ready..."
timeout=30
attempt=0
while [ $timeout -gt 0 ]; do
    attempt=$((attempt + 1))
    log_debug "  Attempt $attempt/$timeout: Checking port 8084..."
    
    if nc -z localhost 8084 >/dev/null 2>&1; then
        log_info "  [OK] Frontend ready on port 8084 after $attempt attempts"
        break
    fi
    
    if [ $((attempt % 10)) -eq 0 ]; then
        log_warn "  [WARN] Frontend not ready yet (attempt $attempt/$timeout)"
    fi
    
    sleep 1
    timeout=$((timeout - 1))
done

if [ $timeout -eq 0 ]; then
    log_warn "  [WARN] Frontend may not be ready"
    log_warn "  Check: docker logs frontend"
fi

log_info ""
log_info "[9/9] Starting FUSE Clients..."
log_debug "  Running: docker compose -f $DOCKER_DIR/docker-compose.yml up -d --no-deps fuse-1 fuse-2"

if docker compose -f "$DOCKER_DIR/docker-compose.yml" up -d --no-deps fuse-1 fuse-2; then
    log_info "  [OK] FUSE client containers started"
else
    log_error "  [FAIL] Failed to start FUSE client containers"
    exit 2
fi

log_info "  Waiting for FUSE clients to mount..."
wait_time=3
for i in $(seq 1 $wait_time); do
    log_debug "  Waiting... ($i/$wait_time)"
    sleep 1
done

log_info "  Checking FUSE client status..."
timeout=30
attempt=0
FUSE1_OK=false
FUSE2_OK=false

while [ $timeout -gt 0 ]; do
    attempt=$((attempt + 1))
    
    FUSE1_RUNNING=$(docker inspect -f '{{.State.Running}}' fuse-1 2>/dev/null || echo "false")
    FUSE2_RUNNING=$(docker inspect -f '{{.State.Running}}' fuse-2 2>/dev/null || echo "false")
    
    log_debug "  Attempt $attempt/$timeout: fuse-1=$FUSE1_RUNNING, fuse-2=$FUSE2_RUNNING"
    
    if [ "$FUSE1_RUNNING" = "true" ]; then
        FUSE1_OK=true
    fi
    if [ "$FUSE2_RUNNING" = "true" ]; then
        FUSE2_OK=true
    fi
    
    if [ "$FUSE1_OK" = "true" ] && [ "$FUSE2_OK" = "true" ]; then
        log_info "  [OK] Both FUSE clients are running"
        break
    fi
    
    if [ $((attempt % 10)) -eq 0 ]; then
        log_warn "  [WARN] FUSE clients not ready yet (attempt $attempt/$timeout)"
        if [ "$FUSE1_RUNNING" != "true" ]; then
            log_debug "  fuse-1 status: $(docker inspect -f '{{.State.Status}}' fuse-1 2>/dev/null || echo 'unknown')"
        fi
        if [ "$FUSE2_RUNNING" != "true" ]; then
            log_debug "  fuse-2 status: $(docker inspect -f '{{.State.Status}}' fuse-2 2>/dev/null || echo 'unknown')"
        fi
    fi
    
    sleep 1
    timeout=$((timeout - 1))
done

if [ "$FUSE1_OK" != "true" ]; then
    log_warn "  [WARN] FUSE client 1 may not be running properly"
    log_warn "  Check logs: docker logs fuse-1"
fi

if [ "$FUSE2_OK" != "true" ]; then
    log_warn "  [WARN] FUSE client 2 may not be running properly"
    log_warn "  Check logs: docker logs fuse-2"
fi

END_TIME=$(date +%s)
ELAPSED_TIME=$((END_TIME - START_TIME))

log_info ""
log_info "========================================"
log_info "    FUSE Test Environment Started!"
log_info "========================================"
log_info ""
log_info "Total startup time: $ELAPSED_TIME seconds"
if [ -n "$BUILD_TIME" ]; then
    log_info "Build timestamp:    $BUILD_TIME"
fi
log_info ""
log_info "=== Service Status Summary ==="

ALL_SERVICES=(
    "Redis|6379|redis"
    "Master 1|9333|master-1"
    "Master 2|9334|master-2"
    "Master 3|9335|master-3"
    "Volume 1|8080|volume-1"
    "Volume 2|8081|volume-2"
    "Volume 3|8082|volume-3"
    "Filer|8888|filer"
    "S3 Backend|9000|s3"
    "Monitor API|8083|monitor"
    "Frontend|8084|frontend"
    "FUSE Client 1|-|fuse-1"
    "FUSE Client 2|-|fuse-2"
)

for service in "${ALL_SERVICES[@]}"; do
    IFS='|' read -r name port container <<< "$service"
    
    STATUS="UNKNOWN"
    if docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null | grep -q true; then
        STATUS="RUNNING"
        if [ "$port" != "-" ]; then
            if nc -z localhost "$port" >/dev/null 2>&1; then
                STATUS="READY"
            else
                STATUS="RUNNING (port not ready)"
            fi
        fi
    else
        STATUS="NOT RUNNING"
    fi
    
    if [ "$STATUS" = "READY" ]; then
        log_info "  $name:      [OK] $STATUS"
    elif [ "$STATUS" = "RUNNING" ]; then
        log_info "  $name:      [OK] $STATUS"
    else
        log_warn "  $name:      [WARN] $STATUS"
    fi
done

log_info ""
log_info "=== Service Addresses ==="
log_info "  Redis:           $HOST_IP:6379"
log_info "  Master 1:        $HOST_IP:9333"
log_info "  Master 2:        $HOST_IP:9334"
log_info "  Master 3:        $HOST_IP:9335"
log_info "  Volume 1:        $HOST_IP:8080"
log_info "  Volume 2:        $HOST_IP:8081"
log_info "  Volume 3:        $HOST_IP:8082"
log_info "  Filer:           $HOST_IP:8888"
log_info "  S3 Backend:      $HOST_IP:9000"
log_info "  Monitor API:     $HOST_IP:8083"
log_info "  Monitor UI:      http://$HOST_IP:8084"
log_info ""
log_info "=== FUSE Mount Points ==="
log_info "  FUSE Client 1:   /tmp/powerfs/fuse1"
log_info "  FUSE Client 2:   /tmp/powerfs/fuse2"
log_info ""
log_info "=== S3 Credentials ==="
log_info "  Endpoint:        http://$HOST_IP:9000"
log_info "  Access Key:      powerfs"
log_info "  Secret Key:      powerfs123"
log_info ""
log_info "=== Quick Commands ==="
log_info "  Stop environment:         docker/scripts/stop-cluster.sh"
log_info "  Test FUSE:                echo 'Hello PowerFS' > /tmp/powerfs/fuse1/test.txt"
log_info "  Verify cross-client:      cat /tmp/powerfs/fuse2/test.txt"
log_info "  Check all logs:           docker compose logs"
log_info "  Check specific:           docker logs <container_name>"
log_info ""
log_info "=== Build Options ==="
log_info "  Build images (cleanup + build):  docker/scripts/start-fuse.sh -b"
log_info "  Rebuild code + images:           docker/scripts/start-fuse.sh -bb"
log_info "  Build with enterprise FUSE:      docker/scripts/start-fuse.sh -bb --enterprise"
log_info ""
log_info "========================================"