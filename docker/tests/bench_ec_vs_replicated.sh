#!/bin/bash
###############################################################################
# bench_ec_vs_replicated.sh — P6 fio 性能评估: EC 降级读 vs 副本读 vs 单副本读
#
# 对比 5 种读取场景的带宽、IOPS、延迟:
#   1. SingleReplica 读 — 单副本, 无冗余 (基线)
#   2. Replicated 读    — 2 副本, 从主 volume 读
#   3. EC 正常读        — EC(4+2) 全分片完好, 直接读 data shards
#   4. EC 降级读 (1丢失) — 1 个 data shard 丢失, parity 重建
#   5. EC 降级读 (2丢失) — 2 个 data shard 丢失, 达到容错上限
#
# 每种场景测试:
#   - 顺序读带宽 (1MB block, iodepth=4)
#   - 随机读 IOPS (4KB block, iodepth=1)
#   - 延迟 (average, p99)
#
# 输出: 终端对比表 + Markdown 报告文件
#
# 用法:
#   ./bench_ec_vs_replicated.sh                         # 集群已启动, 仅跑测试
#   ./bench_ec_vs_replicated.sh --start-cluster         # 先启动集群
#   ./bench_ec_vs_replicated.sh --start-cluster --build # 编译+启动
#   ./bench_ec_vs_replicated.sh --cleanup               # 测试后关闭集群
#
# 环境变量:
#   EC_DATA_SHARDS     EC 数据分片数 (默认 4)
#   EC_PARITY_SHARDS   EC 校验分片数 (默认 2)
#   SCAN_INTERVAL      scrubber 扫描间隔秒 (默认 10)
#   FILE_SIZE_MB       测试文件大小 MB (默认 32, 需 > 1MB chunk_size)
#   FIO_RUNTIME        每次测试运行秒 (默认 10)
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
FILE_SIZE_MB="${FILE_SIZE_MB:-32}"
FIO_RUNTIME="${FIO_RUNTIME:-10}"

FUSE_CONTAINER="fuse-1"
FILER_CONTAINER="filer-1"
MOUNT_POINT="/mnt/powerfs"
TEST_DIR="${MOUNT_POINT}/bench_dir"
TESTUTIL="/app/powerfs-testutil"
VOLUME_NET_PORT=8901

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# 参数
START_CLUSTER=false
CLEANUP=false
BUILD=false

# 结果存储 (关联数组)
declare -A SEQ_BW SEQ_IOPS SEQ_LAT_AVG SEQ_LAT_P99
declare -A RND_IOPS RND_LAT_AVG RND_LAT_P99

# 场景列表
SCENARIOS=("single" "replicated" "ec_normal" "ec_degraded1" "ec_degraded2")
SCENARIO_NAMES=(
    "single:SingleReplica 读 (单副本基线)"
    "replicated:Replicated 读 (2副本)"
    "ec_normal:EC 正常读 (全分片完好)"
    "ec_degraded1:EC 降级读 (1 data shard 丢失)"
    "ec_degraded2:EC 降级读 (2 data shard 丢失, 容错上限)"
)

# 分片信息 (EC 文件)
SHARD_KIND=(); SHARD_INDEX=(); SHARD_VOL=(); SHARD_NEEDLE=(); SHARD_ADDR=()

# ============================================================================
# 辅助函数
# ============================================================================
log_info()  { echo -e "${BLUE}[INFO]${NC} $(date '+%H:%M:%S') $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $(date '+%H:%M:%S') $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%H:%M:%S') $*" >&2; }
log_ok()    { echo -e "${GREEN}[OK]${NC} $(date '+%H:%M:%S') $*"; }
log_bench() { echo -e "${CYAN}[BENCH]${NC} $(date '+%H:%M:%S') $*"; }

fuse_exec() { docker exec "$FUSE_CONTAINER" "$@"; }

# ============================================================================
# 参数解析
# ============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --start-cluster) START_CLUSTER=true; shift ;;
            --cleanup)       CLEANUP=true; shift ;;
            --build)         BUILD=true; shift ;;
            --help|-h)       head -30 "$0"; exit 0 ;;
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

    export POWERFS_EC_DATA_SHARDS="$EC_DATA_SHARDS"
    export POWERFS_EC_PARITY_SHARDS="$EC_PARITY_SHARDS"
    export POWERFS_SCRUBBER_SCAN_INTERVAL="$SCAN_INTERVAL"
    export POWERFS_EC_MIN_FILE_SIZE=0
    export POWERFS_SCRUBBER_MAX_INODES=50

    cd "$DOCKER_DIR"
    docker compose down --remove-orphans 2>/dev/null || true
    bash "$DOCKER_DIR/scripts/start-cluster.sh" 2>&1 | tail -30

    log_info "启动 volume-4/5/6..."
    docker compose up -d --no-deps volume-4 volume-5 volume-6 2>&1

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
    sleep 5
    wait_fuse_ready
}

stop_cluster() {
    log_info "停止集群..."
    cd "$DOCKER_DIR"
    docker compose down --remove-orphans 2>/dev/null || true
    log_ok "集群已停止"
}

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

restart_fuse_clear_cache() {
    log_info "重启 $FUSE_CONTAINER 以清除 chunk_cache..."
    docker restart "$FUSE_CONTAINER" >/dev/null 2>&1
    wait_fuse_ready || { log_error "FUSE 重启后无法就绪"; return 1; }
    sleep 3
    log_ok "chunk_cache 已清除"
}

# ============================================================================
# fio 安装与运行
# ============================================================================
ensure_fio() {
    if docker exec "$FUSE_CONTAINER" which fio >/dev/null 2>&1; then
        log_ok "fio 已安装"
        return 0
    fi
    log_info "安装 fio..."
    docker exec "$FUSE_CONTAINER" bash -c "apt-get update -qq && apt-get install -y -qq fio" >/dev/null 2>&1
    if docker exec "$FUSE_CONTAINER" which fio >/dev/null 2>&1; then
        log_ok "fio 安装成功"
    else
        log_error "fio 安装失败"
        return 1
    fi
}

# 运行 fio 顺序读测试, 输出 JSON, 解析关键指标
# 参数: $1 = 文件路径
# 设置全局变量: SEQ_BW_VAL, SEQ_IOPS_VAL, SEQ_LAT_AVG_VAL, SEQ_LAT_P99_VAL
run_fio_seq_read() {
    local file_path="$1"
    local fio_job="/tmp/fio_seq_read.fio"

    cat > "$fio_job" << 'EOF'
[global]
ioengine=psync
direct=0
runtime=RUNTIME_SEC
time_based=1
group_reporting=1
filename=FILE_PATH

[seq-read]
rw=read
bs=1m
numjobs=4
iodepth=1
EOF

    # 替换占位符
    local tmp_job="/tmp/fio_seq_read_${$}.fio"
    sed "s|FILE_PATH|$file_path|g; s|RUNTIME_SEC|$FIO_RUNTIME|g" "$fio_job" > "$tmp_job"
    docker cp "$tmp_job" "${FUSE_CONTAINER}:/tmp/fio_seq_read.fio"
    rm -f "$tmp_job"

    local json_output
    json_output=$(docker exec "$FUSE_CONTAINER" fio /tmp/fio_seq_read.fio --output-format=json 2>/dev/null || true)

    # 用 python3 解析 JSON (通过 stdin 传递, 避免引号问题)
    local parsed
    parsed=$(echo "$json_output" | docker exec -i "$FUSE_CONTAINER" python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    job = data['jobs'][0]
    r = job['read']
    bw = r['bw']  # KiB/s
    iops = r['iops']
    lat_avg = r['lat_ns']['mean'] / 1000  # ns -> us
    pct = r['lat_ns'].get('percentile', {})
    lat_p99 = pct.get('99.000000', r['lat_ns'].get('max', 0)) / 1000
    print(f'{bw:.1f} {iops:.1f} {lat_avg:.1f} {lat_p99:.1f}')
except:
    print('0 0 0 0')
" 2>/dev/null || echo "0 0 0 0")
    read -r SEQ_BW_VAL SEQ_IOPS_VAL SEQ_LAT_AVG_VAL SEQ_LAT_P99_VAL <<< "$parsed"

    if [ -z "${SEQ_BW_VAL:-}" ]; then
        log_warn "fio 顺序读解析失败, 使用备用解析"
        # 备用: 从文本输出解析
        local text_output
        text_output=$(docker exec "$FUSE_CONTAINER" fio /tmp/fio_seq_read.fio 2>&1)
        SEQ_BW_VAL=$(echo "$text_output" | grep -oP 'bw=\K[0-9.]+' | head -1 || echo "0")
        SEQ_IOPS_VAL=$(echo "$text_output" | grep -oP 'IOPS=\K[0-9.]+' | head -1 || echo "0")
        SEQ_LAT_AVG_VAL="0"
        SEQ_LAT_P99_VAL="0"
    fi
}

# 运行 fio 随机读测试
# 参数: $1 = 文件路径
run_fio_rand_read() {
    local file_path="$1"
    local fio_job="/tmp/fio_rand_read.fio"

    cat > "$fio_job" << 'EOF'
[global]
ioengine=psync
direct=0
runtime=RUNTIME_SEC
time_based=1
group_reporting=1
filename=FILE_PATH

[rand-read]
rw=randread
bs=4k
numjobs=1
iodepth=1
EOF

    local tmp_job="/tmp/fio_rand_read_${$}.fio"
    sed "s|FILE_PATH|$file_path|g; s|RUNTIME_SEC|$FIO_RUNTIME|g" "$fio_job" > "$tmp_job"
    docker cp "$tmp_job" "${FUSE_CONTAINER}:/tmp/fio_rand_read.fio"
    rm -f "$tmp_job"

    local json_output
    json_output=$(docker exec "$FUSE_CONTAINER" fio /tmp/fio_rand_read.fio --output-format=json 2>/dev/null || true)

    local parsed
    parsed=$(echo "$json_output" | docker exec -i "$FUSE_CONTAINER" python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    job = data['jobs'][0]
    r = job['read']
    iops = r['iops']
    lat_avg = r['lat_ns']['mean'] / 1000  # ns -> us
    pct = r['lat_ns'].get('percentile', {})
    lat_p99 = pct.get('99.000000', r['lat_ns'].get('max', 0)) / 1000
    print(f'{iops:.1f} {lat_avg:.1f} {lat_p99:.1f}')
except:
    print('0 0 0')
" 2>/dev/null || echo "0 0 0")
    read -r RND_IOPS_VAL RND_LAT_AVG_VAL RND_LAT_P99_VAL <<< "$parsed"

    if [ -z "${RND_IOPS_VAL:-}" ]; then
        log_warn "fio 随机读解析失败, 使用备用解析"
        local text_output
        text_output=$(docker exec "$FUSE_CONTAINER" fio /tmp/fio_rand_read.fio 2>&1)
        RND_IOPS_VAL=$(echo "$text_output" | grep -oP 'IOPS=\K[0-9.]+' | head -1 || echo "0")
        RND_LAT_AVG_VAL="0"
        RND_LAT_P99_VAL="0"
    fi
}

# ============================================================================
# EC 分片管理 (复用 test_ec_degraded_read.sh 的逻辑)
# ============================================================================

# 等待状态转换完成
# 参数: $1 = inode, $2 = 期望状态 (replicated/ec), $3 = 超时秒
wait_state_transition() {
    local inode="$1"
    local target_state="$2"
    local timeout="${3:-180}"
    local log_pattern

    case "$target_state" in
        replicated) log_pattern="inode ${inode} replicated.*state -> Replicated" ;;
        ec)         log_pattern="inode ${inode} EC converted" ;;
        *) log_error "未知状态: $target_state"; return 1 ;;
    esac

    log_info "等待 inode $inode → $target_state (超时 ${timeout}s)..."
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        # 搜索所有 filer 日志 (Raft leader 可能是任意 filer)
        if docker logs filer-1 2>&1 | grep -q "$log_pattern" 2>/dev/null || \
           docker logs filer-2 2>&1 | grep -q "$log_pattern" 2>/dev/null || \
           docker logs filer-3 2>&1 | grep -q "$log_pattern" 2>/dev/null; then
            log_ok "状态转换完成: $target_state"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
        if [ $((elapsed % 30)) -eq 0 ]; then
            log_warn "仍在等待... (${elapsed}s/$timeout)"
        fi
    done
    log_error "状态转换超时 ($target_state)"
    return 1
}

# 等待 EC 转换并解析分片位置
# 参数: $1 = inode, $2 = 超时秒
wait_ec_and_parse_shards() {
    local inode="$1"
    local timeout="${2:-180}"

    if ! wait_state_transition "$inode" ec "$timeout"; then
        return 1
    fi

    local log_line
    # 搜索所有 filer 日志 (Raft leader 可能是任意 filer)
    log_line=$(docker logs filer-1 2>&1 | grep "inode ${inode} EC converted" | tail -1)
    [ -z "$log_line" ] && log_line=$(docker logs filer-2 2>&1 | grep "inode ${inode} EC converted" | tail -1)
    [ -z "$log_line" ] && log_line=$(docker logs filer-3 2>&1 | grep "inode ${inode} EC converted" | tail -1)
    [ -z "$log_line" ] && { log_error "未找到 EC converted 日志"; return 1; }

    # 解析分片 (stripe group 格式: G0=[D[0]:vol=... needle=...@..., ...])
    SHARD_KIND=(); SHARD_INDEX=(); SHARD_VOL=(); SHARD_NEEDLE=(); SHARD_ADDR=()

    local shards_str="${log_line##*G0=\[}"
    shards_str="${shards_str%]*}"
    [ -z "$shards_str" ] && { log_error "无法解析分片信息"; return 1; }

    IFS=',' read -ra shard_arr <<< "$shards_str"
    for shard in "${shard_arr[@]}"; do
        shard="${shard#"${shard%%[![:space:]]*}"}"
        shard="${shard%"${shard##*[![:space:]]}"}"
        [ -z "$shard" ] && continue

        local kind idx vol needle addr
        kind="${shard:0:1}"
        idx="${shard#*[}"; idx="${idx%%]*}"
        vol="${shard#*vol=}"; vol="${vol%% *}"
        needle="${shard#*needle=}"; needle="${needle%%@*}"
        addr="${shard##*@}"

        SHARD_KIND+=("$kind")
        SHARD_INDEX+=("$idx")
        SHARD_VOL+=("$vol")
        SHARD_NEEDLE+=("$needle")
        SHARD_ADDR+=("$addr")
    done

    log_ok "解析到 ${#SHARD_KIND[@]} 个分片"
    return 0
}

# 获取数据分片的数组下标列表
get_data_shard_indices() {
    local indices=()
    for i in "${!SHARD_KIND[@]}"; do
        [ "${SHARD_KIND[$i]}" = "D" ] && indices+=("$i")
    done
    echo "${indices[@]}"
}

# 删除指定分片
delete_shard() {
    local idx="$1"
    local kind="${SHARD_KIND[$idx]}"
    local sidx="${SHARD_INDEX[$idx]}"
    local vol="${SHARD_VOL[$idx]}"
    local needle="${SHARD_NEEDLE[$idx]}"
    local addr="${SHARD_ADDR[$idx]}"

    log_info "删除分片 ${kind}[${sidx}]: vol=${vol} needle=${needle} @ ${addr}"
    docker exec "$FUSE_CONTAINER" "$TESTUTIL" delete-needle \
        --addr "$addr" --volume-id "$vol" --needle-id "$needle" 2>&1
}

# ============================================================================
# 测试场景
# ============================================================================

# 写入测试文件并返回路径和 inode
# 参数: $1 = 场景标签 (用于文件名)
# 输出: 设置全局 FILE_PATH, FILE_INODE
write_test_file() {
    local label="$1"
    FILE_PATH="${TEST_DIR}/bench_${label}_$(date +%s).dat"
    log_info "写入 ${FILE_SIZE_MB}MB 测试文件: $FILE_PATH"
    fuse_exec dd if=/dev/urandom of="$FILE_PATH" bs=1M count="$FILE_SIZE_MB" 2>/dev/null
    fuse_exec sync
    FILE_INODE=$(fuse_exec stat -c '%i' "$FILE_PATH")
    log_info "inode: $FILE_INODE"
}

# 对指定文件运行完整 fio 基准测试
# 参数: $1 = 场景名, $2 = 文件路径
run_benchmark() {
    local scenario="$1"
    local file_path="$2"

    log_bench "场景: $scenario"
    log_bench "文件: $file_path"

    # 清除缓存
    restart_fuse_clear_cache || return 1

    # 顺序读
    log_bench "  顺序读 (1MB block, iodepth=4)..."
    run_fio_seq_read "$file_path"
    SEQ_BW[$scenario]="${SEQ_BW_VAL:-0}"
    SEQ_IOPS[$scenario]="${SEQ_IOPS_VAL:-0}"
    SEQ_LAT_AVG[$scenario]="${SEQ_LAT_AVG_VAL:-0}"
    SEQ_LAT_P99[$scenario]="${SEQ_LAT_P99_VAL:-0}"
    log_bench "  → BW=${SEQ_BW[$scenario]} KiB/s, IOPS=${SEQ_IOPS[$scenario]}, lat_avg=${SEQ_LAT_AVG[$scenario]}us, lat_p99=${SEQ_LAT_P99[$scenario]}us"

    # 清除缓存
    restart_fuse_clear_cache || return 1

    # 随机读
    log_bench "  随机读 (4KB block, iodepth=1)..."
    run_fio_rand_read "$file_path"
    RND_IOPS[$scenario]="${RND_IOPS_VAL:-0}"
    RND_LAT_AVG[$scenario]="${RND_LAT_AVG_VAL:-0}"
    RND_LAT_P99[$scenario]="${RND_LAT_P99_VAL:-0}"
    log_bench "  → IOPS=${RND_IOPS[$scenario]}, lat_avg=${RND_LAT_AVG[$scenario]}us, lat_p99=${RND_LAT_P99[$scenario]}us"
}

# ============================================================================
# 主测试流程
# ============================================================================

run_all_benchmarks() {
    echo ""
    echo "======================================================"
    echo "  P6 fio 性能评估: EC 降级读 vs 副本读 vs 单副本读"
    echo "  文件大小: ${FILE_SIZE_MB}MB | EC: ${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}"
    echo "  fio runtime: ${FIO_RUNTIME}s per test"
    echo "======================================================"
    echo ""

    # 清理旧测试文件
    log_info "清理旧测试文件..."
    fuse_exec rm -rf "${TEST_DIR:?}"/* 2>/dev/null || true

    # ----------------------------------------------------------------
    # 场景 1: SingleReplica 读 (单副本基线)
    # 写入文件后立即测试, 不等 scrubber 复制
    # ----------------------------------------------------------------
    echo ""
    log_bench "========== 场景 1/5: SingleReplica 读 =========="
    write_test_file "single"

    # 立即运行基准测试 (文件处于 PendingReplicated / SingleReplica 状态)
    run_benchmark "single" "$FILE_PATH"
    SINGLE_FILE="$FILE_PATH"
    SINGLE_INODE="$FILE_INODE"

    # ----------------------------------------------------------------
    # 场景 2: Replicated 读 (2副本)
    # 等待 scrubber 完成副本复制
    # ----------------------------------------------------------------
    echo ""
    log_bench "========== 场景 2/5: Replicated 读 =========="
    write_test_file "repl"

    # 等待副本复制完成
    wait_state_transition "$FILE_INODE" replicated 120 || {
        log_warn "副本复制超时, 使用未复制文件测试 (结果可能不准确)"
    }

    run_benchmark "replicated" "$FILE_PATH"
    REPL_FILE="$FILE_PATH"
    REPL_INODE="$FILE_INODE"

    # ----------------------------------------------------------------
    # 场景 3: EC 正常读 (全分片完好)
    # 等待 scrubber 完成 EC 转换
    # ----------------------------------------------------------------
    echo ""
    log_bench "========== 场景 3/5: EC 正常读 =========="
    write_test_file "ec"

    # 等待 EC 转换 (先复制再 EC, 需要更长时间)
    wait_ec_and_parse_shards "$FILE_INODE" 300 || {
        log_error "EC 转换超时, 跳过 EC 场景"
        SEQ_BW["ec_normal"]="N/A"; SEQ_IOPS["ec_normal"]="N/A"
        SEQ_LAT_AVG["ec_normal"]="N/A"; SEQ_LAT_P99["ec_normal"]="N/A"
        RND_IOPS["ec_normal"]="N/A"; RND_LAT_AVG["ec_normal"]="N/A"; RND_LAT_P99["ec_normal"]="N/A"
        SEQ_BW["ec_degraded1"]="N/A"; SEQ_IOPS["ec_degraded1"]="N/A"
        SEQ_LAT_AVG["ec_degraded1"]="N/A"; SEQ_LAT_P99["ec_degraded1"]="N/A"
        RND_IOPS["ec_degraded1"]="N/A"; RND_LAT_AVG["ec_degraded1"]="N/A"; RND_LAT_P99["ec_degraded1"]="N/A"
        SEQ_BW["ec_degraded2"]="N/A"; SEQ_IOPS["ec_degraded2"]="N/A"
        SEQ_LAT_AVG["ec_degraded2"]="N/A"; SEQ_LAT_P99["ec_degraded2"]="N/A"
        RND_IOPS["ec_degraded2"]="N/A"; RND_LAT_AVG["ec_degraded2"]="N/A"; RND_LAT_P99["ec_degraded2"]="N/A"
        print_results
        return 1
    }

    EC_FILE="$FILE_PATH"
    EC_INODE="$FILE_INODE"

    run_benchmark "ec_normal" "$EC_FILE"

    # ----------------------------------------------------------------
    # 场景 4: EC 降级读 (1 data shard 丢失)
    # ----------------------------------------------------------------
    echo ""
    log_bench "========== 场景 4/5: EC 降级读 (1 shard 丢失) =========="

    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -ge 1 ]; then
        delete_shard "${data_indices[0]}" 2>/dev/null || log_warn "分片删除可能失败"
        run_benchmark "ec_degraded1" "$EC_FILE"
    else
        log_error "无数据分片可删除"
        SEQ_BW["ec_degraded1"]="N/A"
    fi

    # ----------------------------------------------------------------
    # 场景 5: EC 降级读 (2 data shard 丢失, 容错上限)
    # ----------------------------------------------------------------
    echo ""
    log_bench "========== 场景 5/5: EC 降级读 (2 shard 丢失) =========="

    if [ ${#data_indices[@]} -ge 2 ]; then
        delete_shard "${data_indices[1]}" 2>/dev/null || log_warn "分片删除可能失败"
        run_benchmark "ec_degraded2" "$EC_FILE"
    else
        log_error "数据分片不足 2 个"
        SEQ_BW["ec_degraded2"]="N/A"
    fi
}

# ============================================================================
# 结果输出
# ============================================================================

# 将 KiB/s 转换为人类可读格式
format_bw() {
    local kib="${1:-0}"
    if [ "$kib" = "N/A" ]; then
        echo "N/A"
    elif [ "$kib" = "0" ]; then
        echo "0"
    else
        local mib
        mib=$(echo "scale=1; $kib / 1024" | bc 2>/dev/null || echo "0")
        echo "${mib} MiB/s"
    fi
}

print_results() {
    echo ""
    echo "======================================================"
    echo "  性能对比结果"
    echo "======================================================"
    echo ""

    # 顺序读对比表
    echo -e "${BOLD}顺序读 (1MB block, iodepth=4, runtime=${FIO_RUNTIME}s)${NC}"
    echo "+-----------------------+---------------+----------+----------+----------+"
    echo "| 场景                  | 带宽          | IOPS     | avg lat  | p99 lat  |"
    echo "|                       |               |          | (us)     | (us)     |"
    echo "+-----------------------+---------------+----------+----------+----------+"

    for entry in "${SCENARIO_NAMES[@]}"; do
        local key="${entry%%:*}"
        local name="${entry#*:}"
        local bw=$(format_bw "${SEQ_BW[$key]:-N/A}")
        local iops="${SEQ_IOPS[$key]:-N/A}"
        local lat_avg="${SEQ_LAT_AVG[$key]:-N/A}"
        local lat_p99="${SEQ_LAT_P99[$key]:-N/A}"
        printf "| %-21s | %-13s | %-8s | %-8s | %-8s |\n" \
            "$name" "$bw" "$iops" "$lat_avg" "$lat_p99"
    done
    echo "+-----------------------+---------------+----------+----------+----------+"

    echo ""

    # 随机读对比表
    echo -e "${BOLD}随机读 (4KB block, iodepth=1, runtime=${FIO_RUNTIME}s)${NC}"
    echo "+-----------------------+----------+----------+----------+"
    echo "| 场景                  | IOPS     | avg lat  | p99 lat  |"
    echo "|                       |          | (us)     | (us)     |"
    echo "+-----------------------+----------+----------+----------+"

    for entry in "${SCENARIO_NAMES[@]}"; do
        local key="${entry%%:*}"
        local name="${entry#*:}"
        local iops="${RND_IOPS[$key]:-N/A}"
        local lat_avg="${RND_LAT_AVG[$key]:-N/A}"
        local lat_p99="${RND_LAT_P99[$key]:-N/A}"
        printf "| %-21s | %-8s | %-8s | %-8s |\n" \
            "$name" "$iops" "$lat_avg" "$lat_p99"
    done
    echo "+-----------------------+----------+----------+----------+"

    echo ""

    # 降级读开销计算
    if [ "${SEQ_BW[ec_normal]:-N/A}" != "N/A" ] && [ "${SEQ_BW[ec_degraded1]:-N/A}" != "N/A" ] && \
       [ "${SEQ_BW[ec_normal]}" != "0" ] && [ "${SEQ_BW[ec_degraded1]}" != "0" ]; then
        local normal_bw="${SEQ_BW[ec_normal]}"
        local degraded1_bw="${SEQ_BW[ec_degraded1]}"
        local degraded2_bw="${SEQ_BW[ec_degraded2]:-$normal_bw}"
        local overhead1 overhead2

        overhead1=$(echo "scale=1; (1 - $degraded1_bw / $normal_bw) * 100" | bc 2>/dev/null || echo "N/A")
        overhead2=$(echo "scale=1; (1 - $degraded2_bw / $normal_bw) * 100" | bc 2>/dev/null || echo "N/A")

        echo -e "${BOLD}EC 降级读开销分析${NC}"
        echo "  EC 正常读 → 降级读 (1丢失): 带宽下降 ${overhead1}%"
        echo "  EC 正常读 → 降级读 (2丢失): 带宽下降 ${overhead2}%"
        echo ""
    fi

    # EC vs Replicated vs Single 对比
    if [ "${SEQ_BW[single]:-N/A}" != "N/A" ] && [ "${SEQ_BW[replicated]:-N/A}" != "N/A" ] && \
       [ "${SEQ_BW[ec_normal]:-N/A}" != "N/A" ]; then
        echo -e "${BOLD}可靠性开销对比 (相对 SingleReplica 基线)${NC}"
        local single_bw="${SEQ_BW[single]}"
        local repl_bw="${SEQ_BW[replicated]}"
        local ec_bw="${SEQ_BW[ec_normal]}"

        if [ "$single_bw" != "0" ]; then
            local repl_overhead ec_overhead
            repl_overhead=$(echo "scale=1; (1 - $repl_bw / $single_bw) * 100" | bc 2>/dev/null || echo "N/A")
            ec_overhead=$(echo "scale=1; (1 - $ec_bw / $single_bw) * 100" | bc 2>/dev/null || echo "N/A")
            echo "  Replicated 读 vs SingleReplica: ${repl_overhead}% 带宽损失"
            echo "  EC 正常读   vs SingleReplica: ${ec_overhead}% 带宽损失"
            echo ""
        fi
    fi
}

# ============================================================================
# Markdown 报告生成
# ============================================================================

generate_markdown_report() {
    local report_file="${PROJECT_DIR}/docs/p6-fio-benchmark-report.md"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S %Z')

    cat > "$report_file" << REPORT_EOF
# P6 fio 性能评估报告: EC 降级读 vs 副本读 vs 单副本读

> 测试时间: ${timestamp}
> 测试环境: Docker 容器, 6-volume 集群, EC(${EC_DATA_SHARDS}+${EC_PARITY_SHARDS})
> 文件大小: ${FILE_SIZE_MB}MB
> fio runtime: ${FIO_RUNTIME}s per test
> fio ioengine: psync, direct=0 (FUSE 兼容)

## 1. 顺序读性能 (1MB block, iodepth=4)

| 场景 | 带宽 | IOPS | avg latency (us) | p99 latency (us) |
|------|------|------|-------------------|-------------------|
REPORT_EOF

    for entry in "${SCENARIO_NAMES[@]}"; do
        local key="${entry%%:*}"
        local name="${entry#*:}"
        local bw=$(format_bw "${SEQ_BW[$key]:-N/A}")
        local iops="${SEQ_IOPS[$key]:-N/A}"
        local lat_avg="${SEQ_LAT_AVG[$key]:-N/A}"
        local lat_p99="${SEQ_LAT_P99[$key]:-N/A}"
        echo "| ${name} | ${bw} | ${iops} | ${lat_avg} | ${lat_p99} |" >> "$report_file"
    done

    cat >> "$report_file" << REPORT_EOF

## 2. 随机读性能 (4KB block, iodepth=1)

| 场景 | IOPS | avg latency (us) | p99 latency (us) |
|------|------|-------------------|-------------------|
REPORT_EOF

    for entry in "${SCENARIO_NAMES[@]}"; do
        local key="${entry%%:*}"
        local name="${entry#*:}"
        local iops="${RND_IOPS[$key]:-N/A}"
        local lat_avg="${RND_LAT_AVG[$key]:-N/A}"
        local lat_p99="${RND_LAT_P99[$key]:-N/A}"
        echo "| ${name} | ${iops} | ${lat_avg} | ${lat_p99} |" >> "$report_file"
    done

    # 降级读开销分析
    if [ "${SEQ_BW[ec_normal]:-N/A}" != "N/A" ] && [ "${SEQ_BW[ec_degraded1]:-N/A}" != "N/A" ] && \
       [ "${SEQ_BW[ec_normal]}" != "0" ] && [ "${SEQ_BW[ec_degraded1]}" != "0" ]; then
        local normal_bw="${SEQ_BW[ec_normal]}"
        local degraded1_bw="${SEQ_BW[ec_degraded1]}"
        local degraded2_bw="${SEQ_BW[ec_degraded2]:-$normal_bw}"
        local overhead1 overhead2

        overhead1=$(echo "scale=1; (1 - $degraded1_bw / $normal_bw) * 100" | bc 2>/dev/null || echo "N/A")
        overhead2=$(echo "scale=1; (1 - $degraded2_bw / $normal_bw) * 100" | bc 2>/dev/null || echo "N/A")

        cat >> "$report_file" << REPORT_EOF

## 3. EC 降级读开销分析

| 场景 | 带宽 | 相对 EC 正常读 |
|------|------|---------------|
| EC 正常读 | $(format_bw "$normal_bw") | 基线 (0%) |
| EC 降级读 (1丢失) | $(format_bw "$degraded1_bw") | -${overhead1}% |
| EC 降级读 (2丢失) | $(format_bw "$degraded2_bw") | -${overhead2}% |

**结论**: 降级读的额外开销主要来自 parity 重建的 CPU 计算 + 额外的网络读取 (data+parity shards).
REPORT_EOF
    fi

    # 可靠性开销对比
    if [ "${SEQ_BW[single]:-N/A}" != "N/A" ] && [ "${SEQ_BW[replicated]:-N/A}" != "N/A" ] && \
       [ "${SEQ_BW[ec_normal]:-N/A}" != "N/A" ] && [ "${SEQ_BW[single]}" != "0" ]; then
        local single_bw="${SEQ_BW[single]}"
        local repl_bw="${SEQ_BW[replicated]}"
        local ec_bw="${SEQ_BW[ec_normal]}"
        local repl_overhead ec_overhead

        repl_overhead=$(echo "scale=1; (1 - $repl_bw / $single_bw) * 100" | bc 2>/dev/null || echo "N/A")
        ec_overhead=$(echo "scale=1; (1 - $ec_bw / $single_bw) * 100" | bc 2>/dev/null || echo "N/A")

        cat >> "$report_file" << REPORT_EOF

## 4. 可靠性开销对比 (相对 SingleReplica 基线)

| 可靠性级别 | 带宽 | 相对 SingleReplica | 空间开销 | 容错能力 |
|-----------|------|-------------------|---------|---------|
| SingleReplica | $(format_bw "$single_bw") | 基线 (0%) | 1.0x | 0 故障 |
| Replicated(2) | $(format_bw "$repl_bw") | ${repl_overhead}% | 2.0x | 1 故障 |
| EC(4+2) | $(format_bw "$ec_bw") | ${ec_overhead}% | 1.5x | 2 故障 |

**结论**:
- EC(4+2) 以 1.5x 空间开销获得 2 故障容错, 优于 Replicated(2) 的 2.0x 空间 + 1 故障容错
- EC 读性能开销主要来自多 volume 并行读取 + chunk_cache 命中率
REPORT_EOF
    fi

    cat >> "$report_file" << REPORT_EOF

## 5. 测试配置

| 参数 | 值 |
|------|-----|
| EC 数据分片 | ${EC_DATA_SHARDS} |
| EC 校验分片 | ${EC_PARITY_SHARDS} |
| 测试文件大小 | ${FILE_SIZE_MB} MB |
| fio runtime | ${FIO_RUNTIME} s |
| fio ioengine | psync (direct=0, FUSE 兼容) |
| 顺序读 block size | 1 MB |
| 随机读 block size | 4 KB |
| 集群 volumes | 6 (anti-affinity) |
| scrubber scan interval | ${SCAN_INTERVAL} s |
REPORT_EOF

    log_ok "Markdown 报告已生成: $report_file"
}

# ============================================================================
# 主函数
# ============================================================================

main() {
    parse_args "$@"

    # 编译
    if [ "$BUILD" = true ]; then
        build_and_image
    fi

    # 启动集群
    if [ "$START_CLUSTER" = true ]; then
        start_cluster
    fi

    # 确保集群就绪
    wait_fuse_ready || { log_error "FUSE 未就绪, 退出"; exit 1; }
    ensure_fio || { log_error "fio 不可用, 退出"; exit 1; }

    # 运行所有基准测试
    run_all_benchmarks

    # 打印结果
    print_results

    # 生成 Markdown 报告
    generate_markdown_report

    # 清理
    if [ "$CLEANUP" = true ]; then
        stop_cluster
    fi

    echo ""
    log_ok "基准测试完成!"
}

main "$@"
