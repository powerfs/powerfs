#!/bin/bash
###############################################################################
# test_ec_reliability.sh — PowerFS EC 容错/可靠性综合测试
#
# 在 Docker 容器环境中验证 PowerFS EC(4+2) 的容错能力:
#   1. Filer 故障 + 恢复 (Raft 多数派存活, 读写继续)
#   2. Volume 故障 + EC 降级读 (1 个数据分片丢失, parity 重建)
#   3. 多 Volume 故障 (EC 容量上限: 2 个数据分片丢失)
#   4. Filer + Volume 并发故障 (降级读 + Filer 可用性降低)
#   5. 持续运行压力测试 (短暂中断 + 多文件读写)
#
# 用法:
#   ./test_ec_reliability.sh                  # 假设集群已运行
#   ./test_ec_reliability.sh --start-cluster  # 先启动集群再测试
#   ./test_ec_reliability.sh --cleanup        # 测试后关闭集群
#   ./test_ec_reliability.sh --start-cluster --cleanup
#
# 环境变量:
#   EC_DATA_SHARDS     EC 数据分片数 (默认 4)
#   EC_PARITY_SHARDS   EC 校验分片数 (默认 2)
#   SCAN_INTERVAL      scrubber 扫描间隔秒 (默认 10)
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

FUSE_CONTAINER="fuse-1"
MOUNT_POINT="/mnt/powerfs"
TEST_DIR="${MOUNT_POINT}/ec_reliability_test"
TESTUTIL="/app/powerfs-testutil"

# Filer 容器 (Raft 组)
FILERS=("filer-1" "filer-2" "filer-3")

# Volume 容器
VOLUMES=("volume-1" "volume-2" "volume-3" "volume-4" "volume-5" "volume-6")

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

# ============================================================================
# 日志/测试辅助
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

# ============================================================================
# FUSE 辅助
# ============================================================================

# 在 fuse 容器内执行命令
fuse_exec() {
    docker exec "$FUSE_CONTAINER" "$@"
}

# 等待 FUSE 挂载就绪
wait_fuse_ready() {
    log_info "等待 FUSE 挂载就绪..."
    local retries=0
    while [ $retries -lt 60 ]; do
        if docker exec "$FUSE_CONTAINER" test -d "$MOUNT_POINT" 2>/dev/null && \
           docker exec "$FUSE_CONTAINER" mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
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

# ============================================================================
# 文件操作辅助
# ============================================================================

# 全局变量: write_test_file 的输出
WRITE_FILE_PATH=""
WRITE_FILE_INODE=""
WRITE_FILE_SHA=""

# 写入测试文件
# 参数: $1 = 文件大小 MB, $2 = 文件名前缀
# 输出: WRITE_FILE_PATH, WRITE_FILE_INODE, WRITE_FILE_SHA
write_test_file() {
    local size_mb="$1"
    local name="$2"
    WRITE_FILE_PATH="${TEST_DIR}/${name}_$(date +%s).dat"
    log_info "写入 ${size_mb}MB 测试文件: $WRITE_FILE_PATH"
    fuse_exec dd if=/dev/urandom of="$WRITE_FILE_PATH" bs=1M count="$size_mb" 2>&1 | tail -1
    fuse_exec sync
    WRITE_FILE_INODE=$(fuse_exec stat -c '%i' "$WRITE_FILE_PATH")
    WRITE_FILE_SHA=$(fuse_exec sha256sum "$WRITE_FILE_PATH" | awk '{print $1}')
    log_info "inode=$WRITE_FILE_INODE  sha256=${WRITE_FILE_SHA:0:32}..."
}

# 校验文件 sha256
# 参数: $1 = 文件路径, $2 = 期望的 sha256
# 返回: 0=匹配, 1=不匹配, 2=读取失败
verify_sha256() {
    local file_path="$1"
    local expected_sha="$2"
    local actual_sha=""
    actual_sha=$(docker exec "$FUSE_CONTAINER" sha256sum "$file_path" 2>/dev/null | awk '{print $1}') || true
    if [ -z "$actual_sha" ]; then
        log_error "读取失败 (无法获取 checksum): $(basename "$file_path")"
        return 2
    fi
    if [ "$actual_sha" = "$expected_sha" ]; then
        log_ok "checksum 匹配: $(basename "$file_path")"
        return 0
    else
        log_error "checksum 不匹配: 期望 ${expected_sha:0:16}... 实际 ${actual_sha:0:16}..."
        return 1
    fi
}

# ============================================================================
# EC 转换辅助
# ============================================================================

# 全局数组: G0 分片信息
SHARD_KIND=(); SHARD_INDEX=(); SHARD_VOL=(); SHARD_NEEDLE=(); SHARD_ADDR=()

# 等待 EC 转换完成并解析 G0 分片
# 参数: $1 = inode, $2 = 超时秒
wait_ec_conversion() {
    local inode="$1"
    local timeout="${2:-180}"
    log_info "等待 inode $inode 的 EC 转换 (超时 ${timeout}s)..."

    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        local log_line=""
        # 搜索所有 filer 日志 (Raft leader 可能是任意 filer)
        for filer in "${FILERS[@]}"; do
            log_line=$(docker logs "$filer" 2>&1 | grep "inode ${inode} EC converted" | tail -1 || true)
            [ -n "$log_line" ] && break
        done
        if [ -n "$log_line" ]; then
            log_ok "检测到 EC 转换完成"
            log_info "日志: $log_line"
            parse_g0_shards "$log_line"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
        if [ $((elapsed % 30)) -eq 0 ]; then
            log_warn "仍在等待... (${elapsed}s/$timeout)"
        fi
    done
    log_error "EC 转换超时 (${timeout}s)"
    for filer in "${FILERS[@]}"; do
        log_error "最近 $filer 日志:"
        docker logs "$filer" 2>&1 | tail -10 | sed 's/^/    /'
    done
    return 1
}

# 解析 G0=[...] 分片信息
# 日志格式: ...| G0=[D[0]:vol=5 needle=0x1234@172.30.0.21:8901, D[1]:...]
parse_g0_shards() {
    local log_line="$1"
    SHARD_KIND=(); SHARD_INDEX=(); SHARD_VOL=(); SHARD_NEEDLE=(); SHARD_ADDR=()

    # 提取 G0=[...] 内容
    local shards_str="${log_line##*G0=\[}"
    shards_str="${shards_str%]*}"
    if [ -z "$shards_str" ]; then
        log_error "无法解析 G0 分片信息"
        return 1
    fi

    # 按逗号分割
    IFS=',' read -ra shard_arr <<< "$shards_str"
    for shard in "${shard_arr[@]}"; do
        # 去除首尾空格
        shard="${shard#"${shard%%[![:space:]]*}"}"
        shard="${shard%"${shard##*[![:space:]]}"}"
        [ -z "$shard" ] && continue

        # 解析: D[0]:vol=5 needle=0x1234@172.30.0.21:8901
        local kind idx vol needle addr
        kind="${shard:0:1}"                          # 首字符 D 或 P
        idx="${shard#*[}"                            # 去掉 "D[" 前缀
        idx="${idx%%]*}"                             # 去掉 "]" 及之后
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

        log_info "  G0 分片 ${kind}[${idx}]: vol=${vol} needle=${needle} addr=${addr}"
    done

    local total=${#SHARD_KIND[@]}
    local data_count=0 parity_count=0
    for k in "${SHARD_KIND[@]}"; do
        [ "$k" = "D" ] && data_count=$((data_count + 1))
        [ "$k" = "P" ] && parity_count=$((parity_count + 1))
    done
    log_ok "G0 共 ${total} 个分片 (${data_count} data + ${parity_count} parity)"

    if [ "$data_count" -ne "$EC_DATA_SHARDS" ] || [ "$parity_count" -ne "$EC_PARITY_SHARDS" ]; then
        log_error "分片数不匹配: 期望 ${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}, 实际 ${data_count}+${parity_count}"
        return 1
    fi
    return 0
}

# 获取 G0 数据分片的数组下标列表
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
# 容器管理辅助
# ============================================================================

container_running() {
    docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null | grep -q true
}

stop_container() {
    local container="$1"
    log_info "停止容器: $container"
    docker stop "$container" >/dev/null 2>&1 || true
    log_ok "已停止: $container"
}

start_container() {
    local container="$1"
    log_info "启动容器: $container"
    docker start "$container" >/dev/null 2>&1 || true
    log_ok "已启动: $container"
}

# 等待容器就绪 (运行中 + 额外等待注册/Raft 同步)
# 参数: $1 = 容器名, $2 = 类型 (filer/volume), $3 = 超时秒
wait_container_ready() {
    local container="$1"
    local type="${2:-volume}"
    local timeout="${3:-30}"
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        if container_running "$container"; then
            # 额外等待注册/Raft 同步
            if [ "$type" = "filer" ]; then
                log_info "等待 Raft 同步 (7s)..."
                sleep 7
            else
                log_info "等待 volume 注册到 master (7s)..."
                sleep 7
            fi
            log_ok "容器就绪: $container"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    log_error "容器 $container 启动超时"
    return 1
}

# 确保所有 filer 和 volume 容器运行中 (测试间安全网)
ensure_all_running() {
    log_info "确保所有容器运行中..."
    local started=0
    for c in "${FILERS[@]}" "${VOLUMES[@]}"; do
        if ! container_running "$c"; then
            log_warn "$c 未运行, 正在启动..."
            docker start "$c" >/dev/null 2>&1 || true
            started=$((started + 1))
        fi
    done
    if [ $started -gt 0 ]; then
        log_info "等待 ${started} 个容器恢复 (8s)..."
        sleep 8
    fi
    log_ok "所有容器已确认运行"
}

# ============================================================================
# Volume IP → 容器名映射
# ============================================================================

# 172.30.0.21 → volume-1, 172.30.0.22 → volume-2, ..., 172.30.0.26 → volume-6
# 规则: IP 最后一个 octet - 20 = volume 编号
vol_ip_to_name() {
    local ip="$1"
    local last_octet="${ip##*.}"
    local vol_num=$((last_octet - 20))
    echo "volume-${vol_num}"
}

# 从分片地址 (172.30.0.21:8901) 获取 volume 容器名
# 参数: $1 = 分片在 SHARD_* 数组中的下标
get_volume_for_shard() {
    local idx="$1"
    local addr="${SHARD_ADDR[$idx]}"
    local ip="${addr%%:*}"  # 去掉端口
    vol_ip_to_name "$ip"
}

# ============================================================================
# 集群管理
# ============================================================================

start_cluster() {
    log_info "启动集群 (EC=${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}, scan=${SCAN_INTERVAL}s)..."
    export POWERFS_EC_DATA_SHARDS="$EC_DATA_SHARDS"
    export POWERFS_EC_PARITY_SHARDS="$EC_PARITY_SHARDS"
    export POWERFS_SCRUBBER_SCAN_INTERVAL="$SCAN_INTERVAL"
    export POWERFS_EC_MIN_FILE_SIZE=0
    export POWERFS_SCRUBBER_MAX_INODES=50

    cd "$DOCKER_DIR"
    docker compose down --remove-orphans 2>/dev/null || true
    bash "$DOCKER_DIR/scripts/start-cluster.sh" 2>&1 | tail -30

    # 启动 volume-4/5/6 (start-cluster.sh 可能只启动 volume-1/2/3)
    log_info "启动 volume-4/5/6..."
    docker compose up -d --no-deps volume-4 volume-5 volume-6 2>&1 || true

    # 等待 volume-4/5/6 注册
    local retries=0
    while [ $retries -lt 30 ]; do
        local ready=0
        for vol in volume-4 volume-5 volume-6; do
            container_running "$vol" && ready=$((ready + 1))
        done
        if [ $ready -eq 3 ]; then
            break
        fi
        retries=$((retries + 1))
        sleep 2
    done
    sleep 5  # 等待拓扑同步

    wait_fuse_ready
}

stop_cluster() {
    log_info "停止集群..."
    cd "$DOCKER_DIR"
    docker compose down --remove-orphans 2>/dev/null || true
    log_ok "集群已停止"
}

# ============================================================================
# 测试场景 1: Filer 故障 + 恢复
# ============================================================================
test_filer_failure() {
    test_start "场景 1: Filer 故障 + 恢复 (Raft 多数派存活)"

    # 写入第一个文件, 等待 EC 转换
    write_test_file 32 "filer_fail_1"
    local file1_path="$WRITE_FILE_PATH"
    local file1_sha="$WRITE_FILE_SHA"
    local file1_inode="$WRITE_FILE_INODE"

    if ! wait_ec_conversion "$file1_inode" 180; then
        test_fail "场景1-EC转换" "第一个文件 EC 转换超时"
        return 1
    fi
    test_pass "场景1-EC转换" "第一个文件 EC 转换完成"

    # 停止 filer-2 (Raft 多数派仍存活: filer-1 + filer-3)
    stop_container "filer-2"
    sleep 3  # 等待 FUSE 客户端检测故障并重连

    # 验证读取仍正常 (sha256 匹配)
    log_info "验证 filer-2 停机后读取第一个文件..."
    if verify_sha256 "$file1_path" "$file1_sha"; then
        test_pass "场景1-降级读取" "filer-2 停机后读取成功 (Raft 多数派存活)"
    else
        test_fail "场景1-降级读取" "filer-2 停机后读取失败"
        start_container "filer-2"
        wait_container_ready "filer-2" filer || true
        return 1
    fi

    # 在 filer 停机期间写入第二个文件
    log_info "在 filer-2 停机期间写入第二个文件..."
    write_test_file 32 "filer_fail_2"
    local file2_path="$WRITE_FILE_PATH"
    local file2_sha="$WRITE_FILE_SHA"
    log_ok "第二个文件写入成功"

    # 验证第二个文件读取
    if verify_sha256 "$file2_path" "$file2_sha"; then
        test_pass "场景1-降级写入" "filer-2 停机期间写入+读取成功"
    else
        test_fail "场景1-降级写入" "filer-2 停机期间写入文件读取失败"
        start_container "filer-2"
        wait_container_ready "filer-2" filer || true
        return 1
    fi

    # 恢复 filer-2, 等待 Raft 同步
    start_container "filer-2"
    wait_container_ready "filer-2" filer || true

    # 验证两个文件仍可读取
    log_info "验证 filer-2 恢复后两个文件均可读取..."
    local all_ok=true
    verify_sha256 "$file1_path" "$file1_sha" || all_ok=false
    verify_sha256 "$file2_path" "$file2_sha" || all_ok=false

    if $all_ok; then
        test_pass "场景1-恢复" "filer-2 恢复后两个文件均读取成功 (Raft 同步完成)"
    else
        test_fail "场景1-恢复" "filer-2 恢复后文件读取失败"
        return 1
    fi

    # 清理测试文件
    fuse_exec rm -f "$file1_path" "$file2_path" 2>/dev/null || true
    return 0
}

# ============================================================================
# 测试场景 2: Volume 故障 + EC 降级读
# ============================================================================
test_volume_failure() {
    test_start "场景 2: Volume 故障 + EC 降级读 (1 分片丢失)"

    # 写入文件, 等待 EC 转换
    write_test_file 32 "vol_fail"
    local file_path="$WRITE_FILE_PATH"
    local file_sha="$WRITE_FILE_SHA"
    local file_inode="$WRITE_FILE_INODE"

    if ! wait_ec_conversion "$file_inode" 180; then
        test_fail "场景2-EC转换" "EC 转换超时"
        return 1
    fi
    test_pass "场景2-EC转换" "EC 转换完成"

    # 找到 G0 D[0] 所在的 volume 容器
    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -lt 1 ]; then
        test_fail "场景2" "无数据分片可定位"
        return 1
    fi

    local target_vol
    target_vol=$(get_volume_for_shard "${data_indices[0]}")
    log_info "G0 D[0] 位于 $target_vol, 停止该 volume 模拟分片丢失"

    # 停止该 volume
    stop_container "$target_vol"

    # 重启 fuse 清除缓存, 验证降级读 (1 分片丢失, parity 重建)
    restart_fuse_clear_cache || {
        test_fail "场景2" "FUSE 重启失败"
        start_container "$target_vol"
        wait_container_ready "$target_vol" volume || true
        return 1
    }

    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景2-降级读" "1 个 volume 停机后 EC 降级读成功 (parity 重建 1 分片)"
    else
        test_fail "场景2-降级读" "EC 降级读失败 (1 分片丢失应可恢复)"
        start_container "$target_vol"
        wait_container_ready "$target_vol" volume || true
        return 1
    fi

    # 恢复 volume
    start_container "$target_vol"
    wait_container_ready "$target_vol" volume || true

    # 重启 fuse, 验证正常读
    restart_fuse_clear_cache || {
        test_fail "场景2" "FUSE 重启失败"
        return 1
    }

    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景2-恢复" "volume 恢复后正常读成功"
    else
        test_fail "场景2-恢复" "恢复后读取失败"
        return 1
    fi

    # 清理测试文件
    fuse_exec rm -f "$file_path" 2>/dev/null || true
    return 0
}

# ============================================================================
# 测试场景 3: 多 Volume 故障 (EC 容量上限)
# ============================================================================
test_multi_volume_failure() {
    test_start "场景 3: 多 Volume 故障 (EC 容量上限: 2 分片丢失)"

    # 写入文件, 等待 EC 转换
    write_test_file 32 "multi_vol_fail"
    local file_path="$WRITE_FILE_PATH"
    local file_sha="$WRITE_FILE_SHA"
    local file_inode="$WRITE_FILE_INODE"

    if ! wait_ec_conversion "$file_inode" 180; then
        test_fail "场景3-EC转换" "EC 转换超时"
        return 1
    fi
    test_pass "场景3-EC转换" "EC 转换完成"

    # 找到 G0 D[0] 和 D[1] 所在的不同 volume 容器
    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -lt 2 ]; then
        test_fail "场景3" "数据分片不足 2 个"
        return 1
    fi

    local vol1 vol2
    vol1=$(get_volume_for_shard "${data_indices[0]}")
    # 找一个与 vol1 不同的 volume (确保停止 2 个不同 volume = 2 个不同分片丢失)
    vol2=""
    for i in "${data_indices[@]:1}"; do
        local v
        v=$(get_volume_for_shard "$i")
        if [ "$v" != "$vol1" ]; then
            vol2="$v"
            break
        fi
    done
    if [ -z "$vol2" ]; then
        log_warn "所有数据分片位于同一 volume, 使用第二个分片所在 volume"
        vol2=$(get_volume_for_shard "${data_indices[1]}")
    fi

    log_info "G0 D[0] 位于 $vol1, D[1] 位于 $vol2, 同时停止两个 volume"

    # 同时停止两个 volume
    stop_container "$vol1"
    stop_container "$vol2"

    # 重启 fuse, 验证降级读 (2 分片丢失, 达到 EC(4+2) 容错上限)
    restart_fuse_clear_cache || {
        test_fail "场景3" "FUSE 重启失败"
        start_container "$vol1"
        start_container "$vol2"
        wait_container_ready "$vol1" volume || true
        wait_container_ready "$vol2" volume || true
        return 1
    }

    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景3-降级读" "2 个 volume 停机后 EC 降级读成功 (达到容错上限, 2 parity 重建)"
    else
        test_fail "场景3-降级读" "EC 降级读失败 (2 分片丢失在容错范围内)"
        start_container "$vol1"
        start_container "$vol2"
        wait_container_ready "$vol1" volume || true
        wait_container_ready "$vol2" volume || true
        return 1
    fi

    # 恢复两个 volume
    start_container "$vol1"
    start_container "$vol2"
    wait_container_ready "$vol1" volume || true
    wait_container_ready "$vol2" volume || true

    # 重启 fuse, 验证正常读
    restart_fuse_clear_cache || {
        test_fail "场景3" "FUSE 重启失败"
        return 1
    }

    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景3-恢复" "两个 volume 恢复后正常读成功"
    else
        test_fail "场景3-恢复" "恢复后读取失败"
        return 1
    fi

    # 清理测试文件
    fuse_exec rm -f "$file_path" 2>/dev/null || true
    return 0
}

# ============================================================================
# 测试场景 4: Filer + Volume 并发故障
# ============================================================================
test_concurrent_failure() {
    test_start "场景 4: Filer + Volume 并发故障"

    # 写入文件, 等待 EC 转换
    write_test_file 32 "concurrent_fail"
    local file_path="$WRITE_FILE_PATH"
    local file_sha="$WRITE_FILE_SHA"
    local file_inode="$WRITE_FILE_INODE"

    if ! wait_ec_conversion "$file_inode" 180; then
        test_fail "场景4-EC转换" "EC 转换超时"
        return 1
    fi
    test_pass "场景4-EC转换" "EC 转换完成"

    # 找到 G0 D[0] 所在的 volume
    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -lt 1 ]; then
        test_fail "场景4" "无数据分片可定位"
        return 1
    fi
    local target_vol
    target_vol=$(get_volume_for_shard "${data_indices[0]}")

    log_info "同时停止 filer-2 和 $target_vol (并发故障)"

    # 同时停止 filer 和 volume (并发故障)
    stop_container "filer-2"
    stop_container "$target_vol"
    sleep 3  # 等待系统检测故障

    # 重启 fuse, 验证降级读 + Filer 可用性降低
    restart_fuse_clear_cache || {
        test_fail "场景4" "FUSE 重启失败"
        start_container "filer-2"
        start_container "$target_vol"
        wait_container_ready "filer-2" filer || true
        wait_container_ready "$target_vol" volume || true
        return 1
    }

    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景4-并发降级" "Filer + Volume 并发故障时 EC 降级读成功 (多派存活 + parity 重建)"
    else
        test_fail "场景4-并发降级" "并发故障时读取失败"
        start_container "filer-2"
        start_container "$target_vol"
        wait_container_ready "filer-2" filer || true
        wait_container_ready "$target_vol" volume || true
        return 1
    fi

    # 恢复 filer 和 volume
    start_container "filer-2"
    start_container "$target_vol"
    wait_container_ready "filer-2" filer || true
    wait_container_ready "$target_vol" volume || true

    # 重启 fuse, 验证恢复后正常读
    restart_fuse_clear_cache || {
        test_fail "场景4" "FUSE 重启失败"
        return 1
    }

    if verify_sha256 "$file_path" "$file_sha"; then
        test_pass "场景4-恢复" "并发故障恢复后正常读成功"
    else
        test_fail "场景4-恢复" "恢复后读取失败"
        return 1
    fi

    # 清理测试文件
    fuse_exec rm -f "$file_path" 2>/dev/null || true
    return 0
}

# ============================================================================
# 测试场景 5: 持续运行压力测试
# ============================================================================
test_sustained_operation() {
    test_start "场景 5: 持续运行压力测试 (短暂中断 + 多文件)"

    local files=()
    local shas=()
    local i

    # 写入 5 个文件 (每个 8MB), 等待全部 EC 转换
    for i in 1 2 3 4 5; do
        write_test_file 8 "sustained_$i"
        files+=("$WRITE_FILE_PATH")
        shas+=("$WRITE_FILE_SHA")
        if ! wait_ec_conversion "$WRITE_FILE_INODE" 180; then
            test_fail "场景5-EC转换" "文件 $i EC 转换超时"
            for f in "${files[@]}"; do
                fuse_exec rm -f "$f" 2>/dev/null || true
            done
            return 1
        fi
    done
    test_pass "场景5-EC转换" "5 个文件全部写入并 EC 转换完成"

    # 3 次迭代: 读取所有文件 + 短暂中断 volume-3
    local iteration
    for iteration in 1 2 3; do
        log_info "--- 迭代 $iteration/3: 读取 + 短暂中断 volume-3 ---"

        # 后台读取所有 5 个文件 (创建并发 I/O 负载)
        (
            for f in "${files[@]}"; do
                docker exec "$FUSE_CONTAINER" sha256sum "$f" >/dev/null 2>&1 || exit 1
            done
            exit 0
        ) &
        local read_pid=$!

        # 短暂中断: 停止并重启 volume-3
        docker stop volume-3 >/dev/null 2>&1 || true
        sleep 2  # 短暂中断窗口
        docker start volume-3 >/dev/null 2>&1 || true
        sleep 6  # 等待 volume 重新注册

        # 等待后台读取完成
        local read_status=0
        wait "$read_pid" 2>/dev/null || read_status=$?

        if [ "$read_status" -eq 0 ]; then
            log_ok "迭代 $iteration: 后台读取全部成功"
        else
            log_warn "迭代 $iteration: 后台读取部分失败 (中断期间预期行为, 最终验证以恢复后为准)"
        fi
    done

    # 最终验证: 重启 fuse 清除缓存, 校验所有 5 个文件
    restart_fuse_clear_cache || {
        test_fail "场景5" "FUSE 重启失败"
        for f in "${files[@]}"; do
            fuse_exec rm -f "$f" 2>/dev/null || true
        done
        return 1
    }

    local all_ok=true
    for i in "${!files[@]}"; do
        verify_sha256 "${files[$i]}" "${shas[$i]}" || all_ok=false
    done

    if $all_ok; then
        test_pass "场景5-持续运行" "3 次中断后 5 个文件全部读取成功 (sha256 匹配)"
    else
        test_fail "场景5-持续运行" "中断恢复后部分文件读取失败"
    fi

    # 清理测试文件
    for f in "${files[@]}"; do
        fuse_exec rm -f "$f" 2>/dev/null || true
    done
    return 0
}

# ============================================================================
# 参数解析
# ============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --start-cluster) START_CLUSTER=true; shift ;;
            --cleanup)       CLEANUP=true; shift ;;
            --help|-h)
                head -30 "$0"
                exit 0
                ;;
            *) shift ;;
        esac
    done
}

# ============================================================================
# 主流程
# ============================================================================
main() {
    parse_args "$@"

    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║  PowerFS EC 容错/可靠性综合测试                          ║"
    echo "║  EC=${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}  scan=${SCAN_INTERVAL}s                              ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""

    # 启动集群
    if [ "$START_CLUSTER" = true ]; then
        start_cluster
    else
        # 检查集群是否已运行
        if ! container_running "$FUSE_CONTAINER"; then
            log_error "$FUSE_CONTAINER 未运行, 请用 --start-cluster 启动集群"
            exit 1
        fi
        wait_fuse_ready
    fi

    # 确认 testutil 可用 (诊断功能, 非必需)
    if ! docker exec "$FUSE_CONTAINER" test -f "$TESTUTIL" 2>/dev/null; then
        log_warn "$TESTUTIL 不存在于 $FUSE_CONTAINER (诊断功能不可用, 测试仍可继续)"
    else
        log_ok "powerfs-testutil 可用"
    fi

    # 确认 volume 数量 >= data+parity
    local volume_count=0
    for v in "${VOLUMES[@]}"; do
        container_running "$v" && volume_count=$((volume_count + 1))
    done
    local total_shards=$((EC_DATA_SHARDS + EC_PARITY_SHARDS))
    log_info "运行中的 volume 容器: ${volume_count}/${#VOLUMES[@]}, EC 需要: ${total_shards}"
    if [ "$volume_count" -lt "$total_shards" ]; then
        log_error "volume 数量不足: ${volume_count} < ${total_shards}"
        exit 1
    fi

    # 确认所有 filer 运行中
    local filer_count=0
    for f in "${FILERS[@]}"; do
        container_running "$f" && filer_count=$((filer_count + 1))
    done
    log_info "运行中的 filer 容器: ${filer_count}/${#FILERS[@]}"
    if [ "$filer_count" -lt 2 ]; then
        log_error "filer 数量不足: ${filer_count} < 2 (Raft 需要多数派)"
        exit 1
    fi

    # 执行所有测试 (每个测试间确保容器恢复)
    log_info "开始执行 5 个测试场景..."

    test_filer_failure || true
    ensure_all_running

    test_volume_failure || true
    ensure_all_running

    test_multi_volume_failure || true
    ensure_all_running

    test_concurrent_failure || true
    ensure_all_running

    test_sustained_operation || true
    ensure_all_running

    # 清理测试目录
    log_info "清理测试文件..."
    fuse_exec rm -rf "$TEST_DIR" 2>/dev/null || true

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
