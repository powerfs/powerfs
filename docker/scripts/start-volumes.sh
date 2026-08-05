#!/bin/bash
#
# start-volumes.sh - 启动/重启 Volume Server 并验证 powerfs-net 监听
#
# Phase 1: Volume server 启用 powerfs-net 直连监听 (net_port)
# 数据路径改造: 内核客户端将直连 Volume Server (WriteNeedle/ReadNeedle)
#
# 用法:
#   ./start-volumes.sh              # 重启 volume 容器 (使用现有镜像)
#   ./start-volumes.sh --build      # 重新编译 volume 二进制并重建镜像
#   ./start-volumes.sh --check      # 仅检查, 不重启

set -e

# ========== 参数解析 ==========
BUILD=false
CHECK_ONLY=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build|-b)
            BUILD=true
            shift
            ;;
        --check)
            CHECK_ONLY=true
            shift
            ;;
        *)
            echo "未知参数: $1"
            echo "用法: $0 [--build|--check]"
            exit 1
            ;;
    esac
done

# ========== 路径 ==========
DOCKER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROJECT_DIR=$(cd "$DOCKER_DIR/.." && pwd)

cd "$DOCKER_DIR"

# ========== Volume 配置 ==========
# 三个 volume server 的容器内 IP + 端口
# gRPC 端口: 8080 (所有 volume 相同, 不同 IP)
# net_port:  8901/8902/8903 (powerfs-net 直连协议)
VOLUMES=(
    "volume-1|172.30.0.21|8901|8091"
    "volume-2|172.30.0.22|8902|8092"
    "volume-3|172.30.0.23|8903|8093"
)

echo "========================================"
echo "    PowerFS Volume Server 启动脚本"
echo "========================================"
echo ""

if [ "$CHECK_ONLY" = true ]; then
    echo "[模式] 仅检查, 不重启"
else
    # ========== 可选: 重新编译 ==========
    if [ "$BUILD" = true ]; then
        echo "[1/4] 编译 Volume 二进制..."
        cd "$PROJECT_DIR"
        source "$HOME/.cargo/env" 2>/dev/null || true
        cargo build --release --bin powerfs-volume 2>&1 | tail -5
        echo "  [OK] powerfs-volume 编译完成"

        echo "  重建 Docker 镜像..."
        cd "$DOCKER_DIR"
        docker compose build volume-1 volume-2 volume-3 2>&1 | tail -3
        echo "  [OK] 镜像重建完成"
    else
        echo "[1/4] 使用现有镜像 (加 --build 重新编译)"
    fi

    # ========== 重启 Volume 容器 ==========
    echo ""
    echo "[2/4] 重启 Volume 容器..."
    docker compose up -d --force-recreate --no-deps volume-1 volume-2 volume-3
    echo "  [OK] 容器已重启"
fi

# ========== 等待 net_port 就绪 ==========
echo ""
echo "[3/4] 等待 powerfs-net 端口就绪..."

wait_for_port() {
    local name=$1
    local ip=$2
    local port=$3
    local timeout=30
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        if docker exec "$name" bash -c "nc -z $ip $port" 2>/dev/null; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

ALL_READY=true
for entry in "${VOLUMES[@]}"; do
    IFS='|' read -r name ip net_port http_port <<< "$entry"
    container="volume-$(echo $name | cut -d'-' -f2)"

    # 检查 gRPC 端口 (8080)
    if wait_for_port "$container" "$ip" 8080 2>/dev/null; then
        grpc_ok="OK"
    else
        grpc_ok="FAIL"
        ALL_READY=false
    fi

    # 检查 net_port (powerfs-net)
    if wait_for_port "$container" "$ip" "$net_port" 2>/dev/null; then
        net_ok="OK"
    else
        net_ok="FAIL"
        ALL_READY=false
    fi

    printf "  %-12s  gRPC(8080): %-4s  net(%s): %-4s  HTTP(%s)\n" \
        "$name" "$grpc_ok" "$net_port" "$net_ok" "$http_port"
done

if [ "$ALL_READY" = false ]; then
    echo ""
    echo "  [WARNING] 部分端口未就绪, 检查容器日志:"
    echo "    docker logs volume-1 --tail 30"
    echo "    docker logs volume-2 --tail 30"
    echo "    docker logs volume-3 --tail 30"
fi

# ========== 验证 Master 注册的 net_port ==========
echo ""
echo "[4/4] 验证 Master 拓扑 (volume net_port 注册)..."

# 通过 Master HTTP API 查询 volume 拓扑
MASTER_ADDR="localhost:9333"
TOPO=$(curl -s "http://$MASTER_ADDR/cluster/status" 2>/dev/null || echo "")

if echo "$TOPO" | grep -q "net_port" 2>/dev/null; then
    echo "  [OK] Master 拓扑包含 net_port 信息"
    # 提取 volume server 的 net_port
    echo "$TOPO" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    vs = data.get('volume_servers', data.get('VolumeServers', []))
    for s in vs:
        ip = s.get('ip', s.get('Ip', '?'))
        np = s.get('net_port', s.get('NetPort', '?'))
        vid = s.get('id', s.get('Id', '?'))
        print(f'  volume-server: id={vid} ip={ip} net_port={np}')
except:
    print('  (无法解析拓扑 JSON, 原始输出:)')
    print(sys.stdin.read())
" 2>/dev/null || echo "  (拓扑解析失败, 原始: $TOPO)"
else
    echo "  [INFO] Master 拓扑未返回 net_port (可能 API 路径不同)"
    echo "  手动验证: curl http://$MASTER_ADDR/cluster/status | grep net_port"
fi

# ========== 汇总 ==========
echo ""
echo "========================================"
echo "    Volume Server 状态汇总"
echo "========================================"
echo ""
echo "Volume 直连地址 (供内核客户端使用):"
for entry in "${VOLUMES[@]}"; do
    IFS='|' read -r name ip net_port http_port <<< "$entry"
    echo "  $name: ${ip}:${net_port} (powerfs-net)"
done
echo ""
echo "Volume gRPC 地址 (管理接口):"
for entry in "${VOLUMES[@]}"; do
    IFS='|' read -r name ip net_port http_port <<< "$entry"
    echo "  $name: ${ip}:8080 (gRPC), ${ip}:${http_port} (HTTP)"
done
echo ""
echo "日志查看:"
echo "  docker logs volume-1 -f --tail 50"
echo "  docker logs volume-2 -f --tail 50"
echo "  docker logs volume-3 -f --tail 50"
echo ""
