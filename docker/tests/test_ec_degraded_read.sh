#!/bin/bash
###############################################################################
# test_ec_degraded_read.sh — P6 EC 降级读集成回归测试
#
# 在容器环境中验证 EC(4+2) 编码文件的降级读取:
#   1. 写入文件 → scrubber 自动转换为 EC (Replicated → EC)
#   2. 基线读取校验
#   3. 删除 1 个数据分片 → 降级读成功 (parity 重建 1 个)
#   4. 删除 2 个数据分片 → 降级读成功 (parity 重建 2 个, 达到 EC(4+2) 容错上限)
#   5. 删除 3 个分片 → 读取失败 EIO (超过 parity 容错能力, 不可恢复)
#   6. fio 性能回归 (正常读 vs 降级读)
#
# 分片丢失模拟: 通过 powerfs-testutil delete-needle 删除 volume 上的 needle,
#   使对应 shard 读取失败, 触发 FUSE EC 降级读路径 (data+parity 重建).
#
# 用法:
#   ./test_ec_degraded_read.sh                  # 假设集群已启动, 仅跑测试
#   ./test_ec_degraded_read.sh --start-cluster  # 先启动 6-volume 集群再测试
#   ./test_ec_degraded_read.sh --start-cluster --cleanup  # 测试后关闭集群
#   ./test_ec_degraded_read.sh --build           # 重新编译二进制 + 重建镜像
#
# 环境变量:
#   EC_DATA_SHARDS     EC 数据分片数 (默认 4)
#   EC_PARITY_SHARDS   EC 校验分片数 (默认 2)
#   SCAN_INTERVAL      scrubber 扫描间隔秒 (默认 10, 测试用快值)
#   TEST_FILE_SIZE_MB  测试文件大小 MB (默认 8)
###############################################################################

set -euo pipefail

# ============================================================================
# 配置
# ============================================================================
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DOCKER_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
PROJECT_DIR=$(cd "$DOCKER_DIR/.." && pwd)

EC_DATA_SHARDS="${EC_DATA_SHARDS:-4}"
EC_PARITY_SHARDS="${EC_PARITY_SHARDS:-2}"
SCAN_INTERVAL="${SCAN_INTERVAL:-10}"
TEST_FILE_SIZE_MB="${TEST_FILE_SIZE_MB:-8}"
FUSE_CONTAINER="fuse-1"
FILER_CONTAINER="filer-1"
MOUNT_POINT="/mnt/powerfs"
TEST_DIR="${MOUNT_POINT}/ec_test"
TESTUTIL="/app/powerfs-testutil"

# 容器内的 volume net 端口 (统一 8901)
VOLUME_NET_PORT=8901

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 测试计数
PASS=0
FAIL=0
FAILED_TESTS=()

# 参数
START_CLUSTER=false
CLEANUP=false
BUILD=false

# ============================================================================
# 辅助函数
# ============================================================================
log_info()  { echo -e "${BLUE}[INFO]${NC} $(date '+%H:%M:%S') $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $(date '+%H:%M:%S') $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%H:%M:%S') $*" >&2; }
log_ok()    { echo -e "${GREEN}[OK]${NC} $(date '+%H:%M:%S') $*"; }

test_start() {
    echo ""
    echo "=============================================="
    echo "  TEST: $1"
    echo "=============================================="
}

test_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    PASS=$((PASS + 1))
}

test_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    echo "       Reason: $2"
    FAIL=$((FAIL + 1))
    FAILED_TESTS+=("$1: $2")
}

# 在 fuse 容器内执行命令
fuse_exec() {
    docker exec "$FUSE_CONTAINER" "$@"
}

# 等待 fuse 挂载就绪
wait_fuse_ready() {
    log_info "等待 FUSE 挂载就绪..."
    local retries=0
    while [ $retries -lt 60 ]; do
        if docker exec "$FUSE_CONTAINER" test -d "$MOUNT_POINT" 2>/dev/null && \
           docker exec "$FUSE_CONTAINER" mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
            # 尝试创建目录确认 Filer 已就绪
            if docker exec "$FUSE_CONTAINER" mkdir -p "$TEST_DIR" 2>/dev/null; then
                log_ok "FUSE 挂载就绪"
                return 0
            fi
        fi
        retries=$((retries + 1))
        sleep 2
    done
    log_error "FUSE 挂载超时"
    return 1
}

# 重启 fuse 容器以清除 chunk_cache
restart_fuse_clear_cache() {
    log_info "重启 $FUSE_CONTAINER 以清除 chunk_cache..."
    docker restart "$FUSE_CONTAINER" >/dev/null 2>&1
    wait_fuse_ready || { log_error "FUSE 重启后无法就绪"; return 1; }
    # 额外等待拓扑同步, 确保 volume 路由表已填充
    sleep 3
    log_ok "chunk_cache 已清除"
}

# 等待 EC 转换完成, 解析分片位置
# 参数: $1 = inode, $2 = 超时秒
# 输出: 写入全局数组 SHARD_KIND, SHARD_INDEX, SHARD_VOL, SHARD_NEEDLE, SHARD_ADDR
SHARD_KIND=(); SHARD_INDEX=(); SHARD_VOL=(); SHARD_NEEDLE=(); SHARD_ADDR=()
wait_ec_conversion() {
    local inode="$1"
    local timeout="${2:-180}"
    log_info "等待 inode $inode 的 EC 转换 (超时 ${timeout}s)..."

    local elapsed=0
    local log_line=""
    while [ $elapsed -lt $timeout ]; do
        # 查找匹配 inode 的 EC converted 日志行
        log_line=$(docker logs "$FILER_CONTAINER" 2>&1 | \
            grep "inode ${inode} EC converted" | tail -1 || true)
        if [ -n "$log_line" ]; then
            log_ok "检测到 EC 转换完成"
            log_info "日志: $log_line"
            parse_shards "$log_line"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
        if [ $((elapsed % 30)) -eq 0 ]; then
            log_warn "仍在等待... (${elapsed}s/$timeout)"
        fi
    done
    log_error "EC 转换超时 (${timeout}s)"
    log_error "最近 filer 日志:"
    docker logs "$FILER_CONTAINER" 2>&1 | tail -20 | sed 's/^/  /'
    return 1
}

# 解析分片位置日志行
# 格式: ...shards=[D[0]:vol=5 needle=0x1234@172.30.0.21:8901, D[1]:...]
# 使用纯 bash 参数展开 (不依赖 grep -oP, 保证容器内兼容)
parse_shards() {
    local log_line="$1"
    SHARD_KIND=(); SHARD_INDEX=(); SHARD_VOL=(); SHARD_NEEDLE=(); SHARD_ADDR=()

    # 提取 shards=[...] 部分: 去掉 "shards=[" 之前的所有内容, 再去掉最后的 "]"
    local shards_str="${log_line##*shards=\[}"
    shards_str="${shards_str%]*}"
    if [ -z "$shards_str" ]; then
        log_error "无法解析分片信息"
        return 1
    fi

    # 按逗号分割
    IFS=',' read -ra shard_arr <<< "$shards_str"
    for shard in "${shard_arr[@]}"; do
        # 去除首尾空格
        shard="${shard#"${shard%%[![:space:]]*}"}"  # 去前导空格
        shard="${shard%"${shard##*[![:space:]]}"}"   # 去尾部空格
        [ -z "$shard" ] && continue

        # 解析: D[0]:vol=5 needle=0x1234@172.30.0.21:8901
        local kind idx vol needle addr
        kind="${shard:0:1}"                          # 首字符 D 或 P
        idx="${shard#*[}"                            # 去掉 "D[" 前缀
        idx="${idx%%]*}"                             # 去掉 "]" 及之后内容
        vol="${shard#*vol=}"                         # 去掉 "vol=" 之前
        vol="${vol%% *}"                             # 去掉空格及之后
        needle="${shard#*needle=}"                   # 去掉 "needle=" 之前
        needle="${needle%%@*}"                       # 去掉 "@" 及之后
        addr="${shard##*@}"                          # 取 "@" 之后的所有内容

        SHARD_KIND+=("$kind")
        SHARD_INDEX+=("$idx")
        SHARD_VOL+=("$vol")
        SHARD_NEEDLE+=("$needle")
        SHARD_ADDR+=("$addr")

        log_info "  分片 ${kind}[${idx}]: vol=${vol} needle=${needle} addr=${addr}"
    done

    local total=${#SHARD_KIND[@]}
    local data_count=0
    local parity_count=0
    for k in "${SHARD_KIND[@]}"; do
        [ "$k" = "D" ] && data_count=$((data_count + 1))
        [ "$k" = "P" ] && parity_count=$((parity_count + 1))
    done
    log_ok "共 ${total} 个分片 (${data_count} data + ${parity_count} parity)"

    if [ "$data_count" -ne "$EC_DATA_SHARDS" ] || [ "$parity_count" -ne "$EC_PARITY_SHARDS" ]; then
        log_error "分片数不匹配: 期望 ${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}, 实际 ${data_count}+${parity_count}"
        return 1
    fi
    return 0
}

# 删除指定分片的 needle
# 参数: shard_index (在 SHARD_* 数组中的下标)
delete_shard() {
    local idx="$1"
    local kind="${SHARD_KIND[$idx]}"
    local sidx="${SHARD_INDEX[$idx]}"
    local vol="${SHARD_VOL[$idx]}"
    local needle="${SHARD_NEEDLE[$idx]}"
    local addr="${SHARD_ADDR[$idx]}"

    log_info "删除分片 ${kind}[${sidx}]: vol=${vol} needle=${needle} @ ${addr}"
    if docker exec "$FUSE_CONTAINER" "$TESTUTIL" delete-needle \
        --addr "$addr" --volume-id "$vol" --needle-id "$needle" 2>&1; then
        log_ok "分片 ${kind}[${sidx}] 已删除"
        return 0
    else
        log_error "分片 ${kind}[${sidx}] 删除失败"
        return 1
    fi
}

# 读取文件并校验 checksum
# 参数: $1 = 文件路径, $2 = 期望的 sha256
# 返回: 0=校验通过, 1=校验失败, 2=读取错误
verify_read() {
    local file_path="$1"
    local expected_sha="$2"
    local actual_sha

    actual_sha=$(docker exec "$FUSE_CONTAINER" sha256sum "$file_path" 2>/dev/null | awk '{print $1}')
    local exit_code=$?

    if [ $exit_code -ne 0 ] || [ -z "$actual_sha" ]; then
        log_error "读取失败 (exit=$exit_code) — 可能是 EIO (不可恢复)"
        return 2
    fi

    if [ "$actual_sha" = "$expected_sha" ]; then
        log_ok "读取成功, checksum 匹配"
        return 0
    else
        log_error "checksum 不匹配: 期望 ${expected_sha:0:16}... 实际 ${actual_sha:0:16}..."
        return 1
    fi
}

# 获取数据分片的数组下标列表
get_data_shard_indices() {
    local indices=()
    for i in "${!SHARD_KIND[@]}"; do
        if [ "${SHARD_KIND[$i]}" = "D" ]; then
            indices+=("$i")
        fi
    done
    echo "${indices[@]}"
}

# ============================================================================
# 参数解析
# ============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --start-cluster) START_CLUSTER=true; shift ;;
            --cleanup)       CLEANUP=true; shift ;;
            --build)         BUILD=true; shift ;;
            --help|-h)
                head -30 "$0"
                exit 0
                ;;
            *) shift ;;
        esac
    done
}

# ============================================================================
# 集群管理
# ============================================================================
build_and_image() {
    log_info "编译 Rust 二进制 (含 powerfs-testutil)..."
    cd "$PROJECT_DIR"
    source "$HOME/.cargo/env" 2>/dev/null || true
    cargo build --release \
        --bin powerfs-master --bin powerfs-filer --bin powerfs-s3 \
        --bin powerfs-volume --bin powerfs-monitor --bin powerfs-fuse \
        --bin powerfs-testutil 2>&1 | tail -5
    log_ok "二进制编译完成"

    log_info "重建 Docker 镜像..."
    cd "$DOCKER_DIR"
    docker compose build 2>&1 | tail -5
    log_ok "Docker 镜像构建完成"
}

start_cluster() {
    log_info "启动 6-volume 集群 (EC=${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}, scan=${SCAN_INTERVAL}s)..."

    # 设置 EC 环境变量 (docker-compose 会透传到 filer 容器)
    export POWERFS_EC_DATA_SHARDS="$EC_DATA_SHARDS"
    export POWERFS_EC_PARITY_SHARDS="$EC_PARITY_SHARDS"
    export POWERFS_SCRUBBER_SCAN_INTERVAL="$SCAN_INTERVAL"
    export POWERFS_EC_MIN_FILE_SIZE=0
    export POWERFS_SCRUBBER_MAX_INODES=50

    cd "$DOCKER_DIR"
    docker compose down --remove-orphans 2>/dev/null || true

    # 使用 start-cluster.sh 启动 (它处理了服务依赖顺序和健康检查)
    bash "$DOCKER_DIR/scripts/start-cluster.sh" 2>&1 | tail -30

    # 额外启动 volume-4/5/6 (start-cluster.sh 只启动 volume-1/2/3)
    log_info "启动 volume-4/5/6..."
    docker compose up -d --no-deps volume-4 volume-5 volume-6 2>&1

    # 等待 volume-4/5/6 注册到 master
    log_info "等待 volume-4/5/6 注册..."
    local retries=0
    while [ $retries -lt 30 ]; do
        local ready=0
        for vol in volume-4 volume-5 volume-6; do
            docker inspect -f '{{.State.Running}}' "$vol" 2>/dev/null | grep -q true && ready=$((ready + 1))
        done
        if [ $ready -eq 3 ]; then
            log_ok "volume-4/5/6 已就绪"
            break
        fi
        retries=$((retries + 1))
        sleep 2
    done

    # 等待 volume-4/5/6 注册到 master (额外等待拓扑同步)
    sleep 5

    wait_fuse_ready
}

stop_cluster() {
    log_info "停止集群..."
    cd "$DOCKER_DIR"
    docker compose down --remove-orphans 2>/dev/null || true
    log_ok "集群已停止"
}

# ============================================================================
# 测试用例
# ============================================================================

# Phase 2: 写入测试文件并等待 EC 转换
TEST_FILE=""
TEST_INODE=""
BASELINE_SHA=""

phase_write_and_convert() {
    test_start "Phase 2: 写入测试文件 + 等待 EC 转换"

    TEST_FILE="${TEST_DIR}/ec_file_$(date +%s).dat"
    log_info "写入 ${TEST_FILE_SIZE_MB}MB 测试文件: $TEST_FILE"

    # 用 dd 生成确定性数据 (urandom), 记录 checksum
    fuse_exec dd if=/dev/urandom of="$TEST_FILE" bs=1M count="$TEST_FILE_SIZE_MB" 2>&1 | tail -1
    fuse_exec sync

    # 获取 inode
    TEST_INODE=$(fuse_exec stat -c '%i' "$TEST_FILE")
    log_info "文件 inode: $TEST_INODE"

    # 基线 checksum (此时文件是 Replicated/PendingReplicated, 走正常读路径)
    BASELINE_SHA=$(fuse_exec sha256sum "$TEST_FILE" | awk '{print $1}')
    log_info "基线 checksum: ${BASELINE_SHA:0:32}..."

    # 等待 EC 转换
    if ! wait_ec_conversion "$TEST_INODE" 180; then
        test_fail "Phase 2" "EC 转换超时"
        return 1
    fi

    test_pass "Phase 2: 文件写入 + EC 转换完成"
    return 0
}

# Phase 4: 基线读取 (EC 转换后, 清除缓存, 正常读)
phase_baseline_read() {
    test_start "Phase 4: 基线读取 (EC 转换后, 全部分片完好)"

    restart_fuse_clear_cache || { test_fail "Phase 4" "FUSE 重启失败"; return 1; }

    if verify_read "$TEST_FILE" "$BASELINE_SHA"; then
        test_pass "Phase 4: 基线读取成功 (EC 全分片正常读)"
    else
        test_fail "Phase 4" "基线读取校验失败"
        return 1
    fi
    return 0
}

# Phase 5: 降级读 — 丢失 1 个数据分片
phase_degraded_1() {
    test_start "Phase 5: 降级读 — 丢失 1 个数据分片"

    restart_fuse_clear_cache || { test_fail "Phase 5" "FUSE 重启失败"; return 1; }

    # 删除第一个数据分片
    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -lt 1 ]; then
        test_fail "Phase 5" "无数据分片可删除"
        return 1
    fi

    delete_shard "${data_indices[0]}" || { test_fail "Phase 5" "分片删除失败"; return 1; }

    # 读取 — 应该成功 (1 个数据分片由 parity 重建)
    if verify_read "$TEST_FILE" "$BASELINE_SHA"; then
        test_pass "Phase 5: 降级读成功 (1 个数据分片丢失, parity 重建)"
    else
        test_fail "Phase 5" "降级读失败 (1 个分片丢失应可恢复)"
        return 1
    fi
    return 0
}

# Phase 6: 降级读 — 丢失 2 个数据分片 (EC(4+2) 容错上限)
phase_degraded_2() {
    test_start "Phase 6: 降级读 — 丢失 2 个数据分片 (容错上限)"

    restart_fuse_clear_cache || { test_fail "Phase 6" "FUSE 重启失败"; return 1; }

    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -lt 2 ]; then
        test_fail "Phase 6" "数据分片不足 2 个"
        return 1
    fi

    # 第一个分片已在 Phase 5 删除, 删除第二个数据分片
    # 注意: 删除是持久的, Phase 5 删的分片仍然不存在
    delete_shard "${data_indices[1]}" || { test_fail "Phase 6" "分片删除失败"; return 1; }

    # 读取 — 应该成功 (2 个数据分片由 2 个 parity 重建, 达到容错上限)
    if verify_read "$TEST_FILE" "$BASELINE_SHA"; then
        test_pass "Phase 6: 降级读成功 (2 个数据分片丢失, 达到 EC(${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}) 容错上限)"
    else
        test_fail "Phase 6" "降级读失败 (2 个分片丢失在容错范围内)"
        return 1
    fi
    return 0
}

# Phase 7: 不可恢复 — 丢失 3 个分片 (超过 parity 容错)
phase_unrecoverable() {
    test_start "Phase 7: 不可恢复 — 丢失 3 个分片 (超过容错)"

    restart_fuse_clear_cache || { test_fail "Phase 7" "FUSE 重启失败"; return 1; }

    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -lt 3 ]; then
        test_fail "Phase 7" "数据分片不足 3 个"
        return 1
    fi

    # 删除第三个数据分片 (前两个已在 Phase 5/6 删除)
    delete_shard "${data_indices[2]}" || { test_fail "Phase 7" "分片删除失败"; return 1; }

    # 读取 — 应该失败 (3 个分片丢失 > 2 parity, 不可恢复, 返回 EIO)
    log_info "尝试读取 (预期失败 EIO)..."
    local result
    if docker exec "$FUSE_CONTAINER" sha256sum "$TEST_FILE" >/dev/null 2>&1; then
        # 读取成功了 — 不应该
        test_fail "Phase 7" "读取应失败但成功了 (3 分片丢失应不可恢复)"
    else
        local exit_code=$?
        log_ok "读取按预期失败 (exit=$exit_code)"
        # 验证是 EIO (exit code 5) 或读取错误
        if [ $exit_code -ne 0 ]; then
            test_pass "Phase 7: 不可恢复场景正确返回错误 (3 分片丢失 > ${EC_PARITY_SHARDS} parity)"
        else
            test_fail "Phase 7" "未预期的退出码"
        fi
    fi
    return 0
}

# Phase 8: fio 性能回归
phase_fio_regression() {
    test_start "Phase 8: fio 性能回归 (正常读 vs 降级读)"

    # 检查/安装 fio
    if ! docker exec "$FUSE_CONTAINER" which fio >/dev/null 2>&1; then
        log_info "安装 fio..."
        if ! docker exec "$FUSE_CONTAINER" bash -c "apt-get update -qq && apt-get install -y -qq fio" >/dev/null 2>&1; then
            log_warn "fio 安装失败, 跳过性能回归"
            test_fail "Phase 8" "fio 不可用"
            return 0
        fi
    fi
    log_ok "fio 可用"

    # 写一个新的测试文件用于 fio (避免受分片删除影响)
    local fio_file="${TEST_DIR}/fio_ec_$(date +%s).dat"
    log_info "写入 fio 测试文件: $fio_file"
    fuse_exec dd if=/dev/urandom of="$fio_file" bs=1M count="$TEST_FILE_SIZE_MB" 2>/dev/null
    local fio_inode
    fio_inode=$(fuse_exec stat -c '%i' "$fio_file")

    # 等待 EC 转换
    if ! wait_ec_conversion "$fio_inode" 180; then
        test_fail "Phase 8" "fio 文件 EC 转换超时"
        return 0
    fi

    # fio 读取作业 (顺序读)
    local fio_job="/tmp/ec_read_test.fio"
    cat > "$fio_job" << EOF
[global]
ioengine=libaio
direct=1
runtime=10
time_based=1
group_reporting=1
filename=${fio_file}

[seq-read]
rw=read
bs=1m
numjobs=1
iodepth=4
EOF

    # 拷贝 fio 作业到容器
    docker cp "$fio_job" "${FUSE_CONTAINER}:/tmp/ec_read_test.fio"

    # 1. 正常读 (全分片完好)
    restart_fuse_clear_cache || { test_fail "Phase 8" "FUSE 重启失败"; return 0; }
    log_info "fio 正常读 (全分片完好)..."
    local normal_bw
    normal_bw=$(docker exec "$FUSE_CONTAINER" fio /tmp/ec_read_test.fio 2>&1 | \
        grep -oP 'READ:.*bw=\K[0-9.]+[KMG]?B/s' | head -1 || echo "N/A")
    log_ok "正常读带宽: $normal_bw"

    # 2. 降级读 (1 分片丢失)
    restart_fuse_clear_cache || { test_fail "Phase 8" "FUSE 重启失败"; return 0; }
    # 重新解析分片 (wait_ec_conversion 已填充 SHARD_*)
    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -ge 1 ]; then
        delete_shard "${data_indices[0]}" 2>/dev/null || true
    fi

    log_info "fio 降级读 (1 分片丢失)..."
    local degraded_bw
    degraded_bw=$(docker exec "$FUSE_CONTAINER" fio /tmp/ec_read_test.fio 2>&1 | \
        grep -oP 'READ:.*bw=\K[0-9.]+[KMG]?B/s' | head -1 || echo "N/A")
    log_ok "降级读带宽: $degraded_bw"

    echo ""
    echo "  fio 性能对比:"
    echo "    正常读:   $normal_bw"
    echo "    降级读:   $degraded_bw (1 分片由 parity 重建)"

    test_pass "Phase 8: fio 性能回归完成"
    rm -f "$fio_job"
    return 0
}

# ============================================================================
# 主流程
# ============================================================================
main() {
    parse_args "$@"

    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║  P6 EC 降级读集成回归测试                                ║"
    echo "║  EC=${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}  scan=${SCAN_INTERVAL}s  file=${TEST_FILE_SIZE_MB}MB          ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""

    # 编译 + 构建镜像
    if [ "$BUILD" = true ]; then
        build_and_image
    fi

    # 启动集群
    if [ "$START_CLUSTER" = true ]; then
        start_cluster
    else
        # 检查集群是否已运行
        if ! docker inspect -f '{{.State.Running}}' "$FUSE_CONTAINER" 2>/dev/null | grep -q true; then
            log_error "$FUSE_CONTAINER 未运行, 请用 --start-cluster 启动集群"
            exit 1
        fi
        wait_fuse_ready
    fi

    # 确认 testutil 可用
    if ! docker exec "$FUSE_CONTAINER" test -f "$TESTUTIL" 2>/dev/null; then
        log_error "$TESTUTIL 不存在于 $FUSE_CONTAINER, 请用 --build 重新构建镜像"
        exit 1
    fi
    log_ok "powerfs-testutil 可用"

    # 确认 volume 数量 >= data+parity
    local volume_count
    volume_count=$(docker ps --format '{{.Names}}' | grep -c '^volume-' || true)
    local total_shards=$((EC_DATA_SHARDS + EC_PARITY_SHARDS))
    log_info "运行中的 volume 容器: ${volume_count}, EC 需要: ${total_shards}"
    if [ "$volume_count" -lt "$total_shards" ]; then
        log_error "volume 数量不足: ${volume_count} < ${total_shards}"
        log_error "请用 --start-cluster --build 启动完整 6-volume 集群"
        exit 1
    fi

    # 执行测试
    phase_write_and_convert
    phase_baseline_read
    phase_degraded_1
    phase_degraded_2
    phase_unrecoverable
    phase_fio_regression

    # 清理测试文件
    log_info "清理测试文件..."
    docker exec "$FUSE_CONTAINER" rm -rf "$TEST_DIR" 2>/dev/null || true

    # 汇总
    echo ""
    echo "=============================================="
    echo "  测试汇总"
    echo "=============================================="
    echo -e "  ${GREEN}通过: $PASS${NC}"
    echo -e "  ${RED}失败: $FAIL${NC}"
    if [ "$FAIL" -gt 0 ]; then
        echo ""
        echo "失败用例:"
        for failed in "${FAILED_TESTS[@]}"; do
            echo "  - $failed"
        done
    fi
    echo ""

    # 关闭集群
    if [ "$CLEANUP" = true ]; then
        stop_cluster
    fi

    if [ "$FAIL" -gt 0 ]; then
        exit 1
    fi
    exit 0
}

main "$@"
