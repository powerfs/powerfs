#!/bin/bash
# PowerFS CRDT 测试环境管理脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_COMPOSE_FILE="$PROJECT_ROOT/docker/docker-compose.crdt-test.yml"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }

# 打印使用说明
usage() {
    echo "PowerFS CRDT 测试环境管理"
    echo ""
    echo "用法: $0 <command>"
    echo ""
    echo "命令:"
    echo "  build     构建 Docker 镜像"
    echo "  start     启动测试环境"
    echo "  stop      停止测试环境"
    echo "  status    查看环境状态"
    echo "  logs      查看容器日志"
    echo "  test      运行集成测试"
    echo "  stress    运行压力测试"
    echo "  clean     清理所有数据"
    echo "  help      显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 build    # 构建镜像"
    echo "  $0 start    # 启动环境"
    echo "  $0 status   # 查看状态"
    echo "  $0 test     # 运行集成测试"
    echo "  $0 stress   # 运行压力测试"
    echo "  $0 stop     # 停止环境"
}

# 构建镜像
build() {
    log_step "构建 PowerFS Docker 镜像..."

    cd "$PROJECT_ROOT"

    # 检查是否需要先编译
    if [ ! -f "target/release/powerfs-filer" ]; then
        log_warn "未找到 release 构建产物，开始编译..."
        log_step "运行 cargo build --release..."
        cargo build --release --bin powerfs-filer
    fi

    # 构建 Docker 镜像
    docker compose -f "$DOCKER_COMPOSE_FILE" build

    log_info "构建完成"
}

# 启动环境
start() {
    log_step "启动 PowerFS CRDT 测试环境..."

    # 创建本地挂载点
    mkdir -p /tmp/powerfs/crdt-fuse1
    mkdir -p /tmp/powerfs/crdt-fuse2

    # 启动服务
    docker compose -f "$DOCKER_COMPOSE_FILE" up -d

    log_step "等待服务就绪..."
    sleep 10

    # 检查容器状态
    check_status

    log_info "环境启动完成"
    echo ""
    echo "服务地址:"
    echo "  Filer-1 API:  http://localhost:18888"
    echo "  Filer-2 API:  http://localhost:28888"
    echo "  Filer-3 API:  http://localhost:38888"
    echo ""
    echo "CRDT 管理接口:"
    echo "  /admin/crdt/overview           - CRDT 概览"
    echo "  /admin/crdt/shards/<id>        - 分片 OR-Set 状态"
    echo "  /admin/crdt/shards/<id>/dirs/<dir_ino> - 目录 OR-Set 详情"
    echo "  /admin/crdt/cleanup?ttl=24     - 清理 Tombstone"
    echo ""
    echo "挂载点:"
    echo "  FUSE-1: /tmp/powerfs/crdt-fuse1"
    echo "  FUSE-2: /tmp/powerfs/crdt-fuse2"
}

# 停止环境
stop() {
    log_step "停止 PowerFS CRDT 测试环境..."

    # 卸载 FUSE
    for mount in /tmp/powerfs/crdt-fuse1 /tmp/powerfs/crdt-fuse2; do
        if mountpoint -q "$mount" 2>/dev/null; then
            log_step "卸载 $mount..."
            fusermount -u "$mount" 2>/dev/null || true
        fi
    done

    # 停止容器
    docker compose -f "$DOCKER_COMPOSE_FILE" down

    log_info "环境已停止"
}

# 查看状态
check_status() {
    log_step "检查容器状态..."

    docker compose -f "$DOCKER_COMPOSE_FILE" ps
}

# 查看日志
logs() {
    local service=$1
    if [ -n "$service" ]; then
        docker compose -f "$DOCKER_COMPOSE_FILE" logs -f "$service"
    else
        docker compose -f "$DOCKER_COMPOSE_FILE" logs -f
    fi
}

# 运行集成测试
run_test() {
    log_step "运行 CRDT 集成测试..."

    # 等待 FUSE 挂载就绪
    for i in $(seq 1 30); do
        if mountpoint -q /tmp/powerfs/crdt-fuse1 2>/dev/null && \
           mountpoint -q /tmp/powerfs/crdt-fuse2 2>/dev/null; then
            log_info "FUSE 挂载就绪"
            break
        fi
        log_warn "等待 FUSE 挂载... ($i/30)"
        sleep 2
    done

    # 运行测试
    python3 "$SCRIPT_DIR/crdt_integration_test.py"
}

# 运行压力测试
run_stress() {
    log_step "运行 CRDT 压力测试..."

    # 等待 FUSE 挂载就绪
    for i in $(seq 1 30); do
        if mountpoint -q /tmp/powerfs/crdt-fuse1 2>/dev/null && \
           mountpoint -q /tmp/powerfs/crdt-fuse2 2>/dev/null; then
            log_info "FUSE 挂载就绪"
            break
        fi
        log_warn "等待 FUSE 挂载... ($i/30)"
        sleep 2
    done

    # 运行测试
    python3 "$SCRIPT_DIR/crdt_stress_test.py" "$@"
}

# 清理数据
clean() {
    log_step "清理所有测试数据..."

    # 停止环境
    stop

    # 清理 Docker volumes
    docker compose -f "$DOCKER_COMPOSE_FILE" down -v

    # 清理本地挂载点
    rm -rf /tmp/powerfs/crdt-fuse1
    rm -rf /tmp/powerfs/crdt-fuse2

    log_info "清理完成"
}

# 主入口
case "${1:-help}" in
    build)
        build
        ;;
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        check_status
        ;;
    logs)
        logs "${2:-}"
        ;;
    test)
        run_test
        ;;
    stress)
        shift
        run_stress "$@"
        ;;
    clean)
        clean
        ;;
    help|*)
        usage
        ;;
esac
