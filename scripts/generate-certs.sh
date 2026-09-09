#!/usr/bin/env bash
# =============================================================================
# PowerFS TLS 证书生成脚本
# =============================================================================
# 通过 master CA 服务 (powerfs-cli cert) 生成完整证书套件：
#   - CA 证书 (ca.crt) — 分发给所有节点验证 leaf 证书
#   - 存储节点证书 (filer-*.crt, volume-server-*.crt) — filer/volume 作为
#     master 客户端 mTLS
#   - 客户端证书 (kernel-client-*.crt, fuse-client-*.crt) — FUSE/kernel 客户端
#     mount 时 mTLS，绑定源 IP + 挂载目录
#
# 前置条件:
#   1. Master 节点运行且 ca_dir 已配置 (见 docker/config/master.toml: ca_dir=...)
#      Master 首次启动会自动生成 CA (ca.crt + ca.key) 持久化到 ca_dir
#   2. powerfs-cli 已编译: cargo build --release -p powerfs-cli
#   3. 可达 master admin API (metrics_port, 默认 9300)
#
# 用法:
#   # 单节点 RDMA 拓扑 (docker-compose.rdma.yml, host network 192.168.100.3)
#   ./scripts/generate-certs.sh
#
#   # 三节点 HA 拓扑 (docker-compose.yml, bridge 172.30.0.x)
#   ./scripts/generate-certs.sh --topology three --master-api 172.30.0.11:9300
#
#   # 生产环境 (输出到 gitignored docker/certs/, 不入库)
#   ./scripts/generate-certs.sh --output-dir docker/certs
#
# 缺省输出 docker/certs-default/ 提交到 git 作为开发/测试缺省证书。
# 生产部署请用 --output-dir docker/certs/ 重新生成, 并参考
# docker/certs-default/README.md 部署文档替换证书。
# =============================================================================
set -euo pipefail

# --- 缺省参数 ---
MASTER_API="${MASTER_API:-192.168.100.3:9300}"
ADMIN_TOKEN="${ADMIN_TOKEN:-powerfs-admin-test}"
OUTPUT_DIR="${OUTPUT_DIR:-docker/certs-default}"
TOPOLOGY="single"
MOUNT_DIR="${MOUNT_DIR:-/mnt/powerfs}"

# 自动探测 powerfs-cli (CLI 参数优先级最高, 然后是 repo-root/target/release/powerfs-cli,
# 然后是 CWD/target/release/powerfs-cli, 最后 PATH)
if [ -z "${CLI:-}" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
    if [ -x "${REPO_ROOT}/target/release/powerfs-cli" ]; then
        CLI="${REPO_ROOT}/target/release/powerfs-cli"
    elif [ -x "target/release/powerfs-cli" ]; then
        CLI="target/release/powerfs-cli"
    elif command -v powerfs-cli &>/dev/null; then
        CLI="$(command -v powerfs-cli)"
    else
        CLI="target/release/powerfs-cli"  # 保留原缺省让后置 error 生效
    fi
fi

# 单节点 RDMA: 所有存储节点共享 host IP 192.168.100.3
SINGLE_NODE_IP="${SINGLE_NODE_IP:-192.168.100.3}"
# VM kernel-client 源 IP (ib0 IPoIB 优先, eth0 docker-net 兜底)
SINGLE_VM1_IPS=(192.168.100.100 172.30.0.100)
SINGLE_VM2_IPS=(192.168.100.101 172.30.0.101)

# 三节点 HA: 各服务独立 IP (bridge 172.30.0.x)
# IP 必须与 docker-compose.yml 的 ipv4_address 一致, 否则 master 证书验证拒绝.
THREE_FILER_IDS=(filer-1 filer-2 filer-3)
THREE_FILER_IPS=(172.30.0.31 172.30.0.32 172.30.0.33)
THREE_VOLUME_IDS=(volume-server-1 volume-server-2 volume-server-3 volume-server-4 volume-server-5 volume-server-6)
THREE_VOLUME_IPS=(172.30.0.21 172.30.0.22 172.30.0.23 172.30.0.24 172.30.0.25 172.30.0.26)
THREE_FUSE_IDS=(fuse-client-1 fuse-client-2)
THREE_FUSE_IPS=(172.30.0.41 172.30.0.42)

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]
  --topology single|three   拓扑 (默认 single)
  --master-api HOST:PORT    master admin API (默认 192.168.100.3:9300)
  --admin-token TOKEN       admin token (默认 powerfs-admin-test)
  --output-dir DIR          输出目录 (默认 docker/certs-default)
  --mount-dir DIR           客户端绑定挂载目录 (默认 /mnt/powerfs)
  --cli PATH                powerfs-cli 路径 (默认 target/release/powerfs-cli)
  -h, --help                显示帮助
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --topology) TOPOLOGY="$2"; shift 2;;
        --master-api) MASTER_API="$2"; shift 2;;
        --admin-token) ADMIN_TOKEN="$2"; shift 2;;
        --output-dir) OUTPUT_DIR="$2"; shift 2;;
        --mount-dir) MOUNT_DIR="$2"; shift 2;;
        --cli) CLI="$2"; shift 2;;
        -h|--help) usage;;
        *) echo "Unknown option: $1" >&2; usage;;
    esac
done

# --- 前置检查 ---
if [[ ! -x "$CLI" ]]; then
    echo "ERROR: powerfs-cli not found at $CLI" >&2
    echo "  Run: cargo build --release -p powerfs-cli" >&2
    exit 1
fi

# 探测 master 是否可达
if ! curl -sk -m 3 -o /dev/null -w "" "https://${MASTER_API}/api/cert/ca" \
       -H "Authorization: Bearer ${ADMIN_TOKEN}" 2>/dev/null; then
    # /api/cert/ca 可能 401 (没 token) 但只要 TCP 通即可; 失败则 master 未启动
    if ! curl -sk -m 3 -o /dev/null "http://${MASTER_API}/" 2>/dev/null \
       && ! curl -sk -m 3 -o /dev/null "https://${MASTER_API}/" 2>/dev/null; then
        echo "ERROR: master admin API unreachable at ${MASTER_API}" >&2
        echo "  1. Ensure master running with ca_dir configured" >&2
        echo "  2. Check metrics_port in master.toml" >&2
        exit 1
    fi
fi

mkdir -p "$OUTPUT_DIR"
# 清理上轮生成的 leaf 证书 (ca.crt + *.crt + *.key).
# 保留 README.md / .gitkeep / client_registry.json (master 维护).
# 必要时 sudo: master 容器以 root 写入, 当前用户可能无权删除.
clean_file() {
    local f="$1"
    [[ -e "$f" ]] || return 0
    if ! rm -f "$f" 2>/dev/null; then
        echo "  (sudo rm $f — root-owned from previous master run)"
        sudo rm -f "$f"
    fi
}
clean_file "$OUTPUT_DIR/ca.crt"
shopt -s nullglob
for f in "$OUTPUT_DIR"/*.crt "$OUTPUT_DIR"/*.key; do
    [[ "$f" == *ca.crt || "$f" == *ca.key ]] && continue
    clean_file "$f"
done
shopt -u nullglob

echo "============================================================"
echo "Generating PowerFS TLS certificates"
echo "  topology:  $TOPOLOGY"
echo "  master:    $MASTER_API"
echo "  output:    $OUTPUT_DIR"
echo "  mount-dir: $MOUNT_DIR"
echo "============================================================"

# 辅助: 构造 --san-ip 参数串
san_ip_args() {
    local args=""
    for ip in "$@"; do
        args+=" --san-ip $ip"
    done
    echo "$args"
}

# --- 1. 获取 CA 证书 ---
echo
echo "=== [1/3] Fetching CA certificate ==="
"$CLI" cert init-ca --master-api "$MASTER_API" --admin-token "$ADMIN_TOKEN" -o "$OUTPUT_DIR"

# --- 2. 签发存储节点证书 ---
echo
echo "=== [2/3] Signing storage node certificates ==="
sign_node() {
    local node_id="$1" ip="$2"
    echo "  - $node_id (san-ip=$ip)"
    "$CLI" cert sign-node \
        --node-id "$node_id" \
        --master-api "$MASTER_API" \
        --admin-token "$ADMIN_TOKEN" \
        --san-ip "$ip" \
        -o "$OUTPUT_DIR"
}

if [[ "$TOPOLOGY" == "single" ]]; then
    sign_node filer-1 "$SINGLE_NODE_IP"
    sign_node volume-server-1 "$SINGLE_NODE_IP"
    sign_node volume-server-2 "$SINGLE_NODE_IP"
    sign_node volume-server-3 "$SINGLE_NODE_IP"
elif [[ "$TOPOLOGY" == "three" ]]; then
    for i in "${!THREE_FILER_IDS[@]}"; do
        sign_node "${THREE_FILER_IDS[i]}" "${THREE_FILER_IPS[i]}"
    done
    for i in "${!THREE_VOLUME_IDS[@]}"; do
        sign_node "${THREE_VOLUME_IDS[i]}" "${THREE_VOLUME_IPS[i]}"
    done
else
    echo "ERROR: unknown topology '$TOPOLOGY' (use single|three)" >&2
    exit 1
fi

# --- 3. 签发客户端证书 ---
echo
echo "=== [3/3] Signing client certificates ==="
sign_client() {
    local client_name="$1"; shift
    local -a ips=("$@")
    local args; args=$(san_ip_args "${ips[@]}")
    echo "  - $client_name (san-ips=${ips[*]})"
    # shellcheck disable=SC2086
    "$CLI" cert sign-client \
        --client-name "$client_name" \
        --master-api "$MASTER_API" \
        --admin-token "$ADMIN_TOKEN" \
        $args \
        --mount-dir "$MOUNT_DIR" \
        -o "$OUTPUT_DIR"
}

if [[ "$TOPOLOGY" == "single" ]]; then
    sign_client kernel-client-1 "${SINGLE_VM1_IPS[@]}"
    sign_client kernel-client-2 "${SINGLE_VM2_IPS[@]}"
elif [[ "$TOPOLOGY" == "three" ]]; then
    # kernel-client (VM, 假设 VM eth0 172.30.0.100/101)
    sign_client kernel-client-1 172.30.0.100
    sign_client kernel-client-2 172.30.0.101
    # fuse-client (FUSE 用户态, 各自 IP)
    for i in "${!THREE_FUSE_IDS[@]}"; do
        sign_client "${THREE_FUSE_IDS[i]}" "${THREE_FUSE_IPS[i]}"
    done
fi

# --- 汇总 ---
echo
echo "============================================================"
echo "Done. Generated certificates:"
ls -la "$OUTPUT_DIR"
echo "============================================================"
echo

# --- 完整性检查 (master ca_dir 挂载自 OUTPUT_DIR 时 master 自动写入) ---
check_file() {
    local f="$1"
    if [[ ! -s "$OUTPUT_DIR/$f" ]]; then
        echo "  WARN: $f missing or empty in $OUTPUT_DIR"
        return 1
    fi
    return 0
}

echo "完整性检查:"
MISSING=0
for f in ca.crt client_registry.json; do
    check_file "$f" || MISSING=$((MISSING+1))
done
# ca.key 仅在 OUTPUT_DIR == master ca_dir 挂载点时存在 (缺省入库场景)
# 生产模式 (OUTPUT_DIR=docker/certs) 不应有 ca.key, 该字段是 master 私钥.
if [[ "$OUTPUT_DIR" == *"certs-default"* ]]; then
    check_file "ca.key" || MISSING=$((MISSING+1))
fi
if [[ $MISSING -gt 0 ]]; then
    echo
    echo "  WARN: $MISSING file(s) missing — see above."
    echo "  If OUTPUT_DIR is NOT the master ca_dir mount point (e.g. docker/certs/),"
    echo "  ca.key + client_registry.json will not be present — that is expected for"
    echo "  production (master holds them internally on its own volume)."
fi

echo
echo "部署提示:"
echo "  - $OUTPUT_DIR/ca.crt              分发给所有节点 (验证 leaf 证书用)"
echo "  - 存储节点 (filer/volume): 部署 <node-id>.crt + <node-id>.key 到 /etc/powerfs/certs,"
echo "    filer.toml/volume.toml 已配置 ca_crt/client_crt/client_key"
echo "  - 客户端 (kernel/fuse): 部署 ca.crt + <client-name>.crt + <client-name>.key,"
echo "    mount 时通过 ca_crt/client_crt/client_key 选项指定"
echo
echo "缺省证书入库 (dev/test):"
echo "  - 当前 OUTPUT_DIR=$OUTPUT_DIR"
echo "  - 如果是 docker/certs-default/, 可直接 git add 入库"
echo "  - docker-compose 已挂载 ./certs-default 到 master /data/master/ca 和服务 /etc/powerfs/certs"
echo "  - 拉取代码即可 docker compose up, 缺省证书自动生效"
echo
echo "生产部署:"
echo "  - 用 --output-dir docker/certs 重新生成 (该目录被 .gitignore 忽略)"
echo "  - 生产证书不入库, 每个环境独立生成"
echo "  - admin_token 请从 master.toml 读取, 切勿使用缺省 token"
echo "  - 详细流程见 docker/certs-default/README.md"

# --- 权限修正: master 容器以 root 写入 ca.key + client_registry.json ---
# 若当前用户非 root, 提示 chown 以便 git add 入库.
if [[ "$(id -u)" -ne 0 ]]; then
    ROOT_OWNED=()
    while IFS= read -r f; do
        ROOT_OWNED+=("$f")
    done < <(find "$OUTPUT_DIR" -maxdepth 1 -user root -type f 2>/dev/null)
    if [[ ${#ROOT_OWNED[@]} -gt 0 ]]; then
        echo
        echo "============================================================"
        echo "权限修正 (master 容器以 root 写入以下文件):"
        printf '  %s\n' "${ROOT_OWNED[@]}"
        echo "  当前用户 $(whoami) 无法 git add 这些文件。"
        echo "  请执行:"
        echo "    sudo chown $(id -u):$(id -g) ${ROOT_OWNED[*]}"
        echo "  然后再 git add $OUTPUT_DIR/"
        echo "============================================================"
    fi
fi
