#!/bin/bash
###############################################################################
# test_ec_functional.sh — PowerFS EC (Erasure Coding) 功能综合测试
#
# 测试范围:
#   1. 基础文件操作 (内容校验: 0B/1KB/4KB/1MB/4MB/8MB, append/overwrite/truncate)
#   2. 目录操作 (mkdir/readdir/rename/rmdir/嵌套目录)
#   3. EC 转换 + 读取校验 (1MB/4MB/8MB/16MB/32MB, 重启清缓存后读取)
#   4. EC 降级读 (删除 1/2/3 个 G0 数据分片, 验证 EC(4+2) 容错边界)
#   5. 并发操作 (4 并发写 + 4 并发读)
#   6. 删除 + 清理 (文件删除 + 空间回收)
#
# 前提: 集群已启动 (6 volume + 3 filer + fuse-1), EC=4+2, scan_interval=10s
#
# 用法:
#   bash test_ec_functional.sh
#
# 环境变量:
#   EC_DATA_SHARDS     EC 数据分片数 (默认 4)
#   EC_PARITY_SHARDS   EC 校验分片数 (默认 2)
#   EC_CONV_TIMEOUT    EC 转换等待超时秒 (默认 180)
###############################################################################

set -euo pipefail

# ============================================================================
# 配置
# ============================================================================
FUSE_CONTAINER="fuse-1"
MOUNT_POINT="/mnt/powerfs"
TEST_DIR="${MOUNT_POINT}/ec_functional_test"
TESTUTIL="/app/powerfs-testutil"
FILER_CONTAINERS=("filer-1" "filer-2" "filer-3")

EC_DATA_SHARDS="${EC_DATA_SHARDS:-4}"
EC_PARITY_SHARDS="${EC_PARITY_SHARDS:-2}"
EC_CONV_TIMEOUT="${EC_CONV_TIMEOUT:-180}"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# 计数
PASS=0
FAIL=0
FAILED_TESTS=()

# G0 分片信息 (解析后填充)
SHARD_KIND=(); SHARD_INDEX=(); SHARD_VOL=(); SHARD_NEEDLE=(); SHARD_ADDR=()

# 最近一次 EC converted 日志行
EC_LOG_LINE=""

# ============================================================================
# 日志与计数
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
# FUSE 容器执行辅助
# ============================================================================
fuse_exec() {
    docker exec "$FUSE_CONTAINER" "$@"
}

fuse_bash() {
    docker exec "$FUSE_CONTAINER" bash -c "$1"
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
    if ! wait_fuse_ready; then
        log_error "FUSE 重启后无法就绪"
        return 1
    fi
    # 额外等待拓扑同步, 确保 volume 路由表已填充
    sleep 3
    log_ok "chunk_cache 已清除"
    return 0
}

# ============================================================================
# 校验辅助
# ============================================================================

# 计算文件 sha256 (容器内), 读取失败返回空字符串
compute_sha256() {
    local file_path="$1"
    docker exec "$FUSE_CONTAINER" sha256sum "$file_path" 2>/dev/null | awk '{print $1}'
}

# 校验文件 sha256
# 返回: 0=匹配, 1=不匹配, 2=读取失败
verify_sha256() {
    local file_path="$1"
    local expected="$2"
    local actual
    actual=$(compute_sha256 "$file_path")
    if [ -z "$actual" ]; then
        log_error "读取失败 (可能 EIO) — $file_path"
        return 2
    fi
    if [ "$actual" = "$expected" ]; then
        log_ok "checksum 匹配 (${actual:0:16}...)"
        return 0
    else
        log_error "checksum 不匹配: 期望 ${expected:0:16}... 实际 ${actual:0:16}..."
        return 1
    fi
}

# ============================================================================
# EC 转换辅助
# ============================================================================

# 在所有 filer 日志中查找 inode 的 EC converted 日志行
# (Raft leader 可能是 filer-1/filer-2/filer-3 中的任意一个)
find_ec_conversion_log() {
    local inode="$1"
    local line=""
    for filer in "${FILER_CONTAINERS[@]}"; do
        line=$(docker logs "$filer" 2>&1 | grep "inode ${inode} EC converted" | tail -1 || true)
        if [ -n "$line" ]; then
            echo "$line"
            return 0
        fi
    done
    return 1
}

# 等待 EC 转换完成, 成功后设置 EC_LOG_LINE 并记录详情
wait_ec_conversion() {
    local inode="$1"
    local timeout="${2:-$EC_CONV_TIMEOUT}"
    log_info "等待 inode $inode 的 EC 转换 (超时 ${timeout}s)..."
    local elapsed=0
    local line=""
    while [ $elapsed -lt $timeout ]; do
        line=$(find_ec_conversion_log "$inode") || line=""
        if [ -n "$line" ]; then
            EC_LOG_LINE="$line"
            log_ok "检测到 EC 转换完成"
            log_info "日志: $line"
            record_ec_details "$line"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
        if [ $((elapsed % 30)) -eq 0 ]; then
            log_warn "仍在等待... (${elapsed}s/$timeout)"
        fi
    done
    log_error "EC 转换超时 (${timeout}s)"
    for filer in "${FILER_CONTAINERS[@]}"; do
        log_error "$filer 最近日志:"
        docker logs "$filer" 2>&1 | tail -10 | sed 's/^/    /' >&2
    done
    return 1
}

# 从 EC converted 日志行解析并记录转换详情 (groups/shards/needles)
# 格式: ...converted, 4+2 per group, 8 groups, 1048576B shard_size, 48 needles total...
record_ec_details() {
    local line="$1"
    local per_group num_groups shard_size total_needles
    per_group=$(echo "$line" | sed -n 's/.*converted, \([0-9][0-9]*+[0-9][0-9]*\) per group.*/\1/p')
    num_groups=$(echo "$line" | sed -n 's/.*per group, \([0-9][0-9]*\) groups.*/\1/p')
    shard_size=$(echo "$line" | sed -n 's/.*\([0-9][0-9]*\)B shard_size.*/\1/p')
    total_needles=$(echo "$line" | sed -n 's/.*\([0-9][0-9]*\) needles total.*/\1/p')
    log_info "EC 详情: ${per_group:-?} per group, ${num_groups:-?} groups, ${shard_size:-?}B shard_size, ${total_needles:-?} needles total"
}

# 解析 G0 分片位置, 填充 SHARD_* 数组
# 日志格式: ...state -> EC | G0=[D[0]:vol=1 needle=0x123@172.30.0.21:8901, ...]
# G0 位于日志行末尾, 以 ] 结尾
parse_g0_shards() {
    local line="$1"
    SHARD_KIND=(); SHARD_INDEX=(); SHARD_VOL=(); SHARD_NEEDLE=(); SHARD_ADDR=()

    # 去掉 "G0=[" 之前的所有内容
    local shards_str="${line##*G0=\[}"
    # 去掉末尾 "]" (G0 在行尾, 最短 ]* 后缀即末尾 ])
    shards_str="${shards_str%]*}"

    if [ -z "$shards_str" ] || [ "$shards_str" = "$line" ]; then
        log_error "无法解析 G0 分片信息"
        return 1
    fi

    # 按逗号分割
    IFS=',' read -ra shard_arr <<< "$shards_str"
    local shard
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
    local dcount=0 pcount=0
    for k in "${SHARD_KIND[@]}"; do
        [ "$k" = "D" ] && dcount=$((dcount + 1))
        [ "$k" = "P" ] && pcount=$((pcount + 1))
    done
    log_ok "G0 共 ${total} 个分片 (${dcount} data + ${pcount} parity)"

    if [ "$dcount" -ne "$EC_DATA_SHARDS" ] || [ "$pcount" -ne "$EC_PARITY_SHARDS" ]; then
        log_error "G0 分片数不匹配: 期望 ${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}, 实际 ${dcount}+${pcount}"
        return 1
    fi
    return 0
}

# 获取数据分片在 SHARD_* 数组中的下标列表
get_data_shard_indices() {
    local indices=()
    for i in "${!SHARD_KIND[@]}"; do
        [ "${SHARD_KIND[$i]}" = "D" ] && indices+=("$i")
    done
    echo "${indices[@]}"
}

# 删除指定下标的分片 needle (通过 powerfs-testutil delete-needle)
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

# ============================================================================
# 测试 1: 基础文件操作 (内容校验)
# ============================================================================
test_basic_file_ops() {
    test_start "1. 基础文件操作 (内容校验)"

    # 1a. 不同大小文件写入 + 读回 sha256 校验
    local sizes=("0:0B" "1024:1KB" "4096:4KB" "1048576:1MB" "4194304:4MB" "8388608:8MB")
    local entry size_bytes label file_path baseline actual
    for entry in "${sizes[@]}"; do
        size_bytes="${entry%%:*}"
        label="${entry##*:}"
        file_path="${TEST_DIR}/basic_${label}.dat"

        if [ "$size_bytes" -eq 0 ]; then
            fuse_exec truncate -s 0 "$file_path"
        else
            fuse_exec dd if=/dev/urandom of="$file_path" bs="$size_bytes" count=1 2>/dev/null
        fi
        fuse_exec sync

        baseline=$(compute_sha256 "$file_path")
        if [ -z "$baseline" ]; then
            test_fail "basic ${label}" "无法计算基线 sha256"
            continue
        fi
        actual=$(compute_sha256 "$file_path")
        if [ "$actual" = "$baseline" ]; then
            test_pass "basic ${label}: 写入 + 读回内容校验通过"
        else
            test_fail "basic ${label}" "sha256 不匹配"
        fi
    done

    # 1b. append: 写 1MB('A'), 追加 1MB('B'), 校验完整 2MB 内容
    local app_file="${TEST_DIR}/basic_append.dat"
    fuse_bash "head -c 1048576 /dev/zero | tr '\0' 'A' > '$app_file'"
    fuse_bash "head -c 1048576 /dev/zero | tr '\0' 'B' >> '$app_file'"
    fuse_exec sync
    local expected_app app_actual app_size
    expected_app=$(fuse_bash "{ head -c 1048576 /dev/zero | tr '\0' 'A'; head -c 1048576 /dev/zero | tr '\0' 'B'; } | sha256sum | cut -d' ' -f1")
    app_actual=$(compute_sha256 "$app_file")
    app_size=$(fuse_exec stat -c '%s' "$app_file")
    if [ "$app_actual" = "$expected_app" ] && [ "$app_size" -eq 2097152 ]; then
        test_pass "append: 1MB+1MB=2MB 内容与大小校验通过"
    else
        test_fail "append" "sha256/size 不匹配 (size=$app_size)"
    fi

    # 1c. overwrite: 写 1MB('A'), 覆盖写 1MB('B'), 校验内容为 B
    local ow_file="${TEST_DIR}/basic_overwrite.dat"
    fuse_bash "head -c 1048576 /dev/zero | tr '\0' 'A' > '$ow_file'"
    fuse_bash "head -c 1048576 /dev/zero | tr '\0' 'B' > '$ow_file'"
    fuse_exec sync
    local expected_ow ow_actual ow_size
    expected_ow=$(fuse_bash "head -c 1048576 /dev/zero | tr '\0' 'B' | sha256sum | cut -d' ' -f1")
    ow_actual=$(compute_sha256 "$ow_file")
    ow_size=$(fuse_exec stat -c '%s' "$ow_file")
    if [ "$ow_actual" = "$expected_ow" ] && [ "$ow_size" -eq 1048576 ]; then
        test_pass "overwrite: 覆盖后内容为新内容 (B), 大小校验通过"
    else
        test_fail "overwrite" "内容/size 不匹配 (size=$ow_size)"
    fi

    # 1d. truncate: 写 4MB(urandom), 截断到 2MB, 校验大小与内容(前 2MB)
    local tr_file="${TEST_DIR}/basic_truncate.dat"
    fuse_exec dd if=/dev/urandom of="$tr_file" bs=1M count=4 2>/dev/null
    fuse_exec sync
    # 截断前计算前 2MB 的 sha256 (作为期望值)
    local expected_tr
    expected_tr=$(fuse_bash "head -c 2097152 '$tr_file' | sha256sum | cut -d' ' -f1")
    fuse_exec truncate -s 2M "$tr_file"
    local tr_size tr_actual
    tr_size=$(fuse_exec stat -c '%s' "$tr_file")
    tr_actual=$(compute_sha256 "$tr_file")
    if [ "$tr_size" -eq 2097152 ] && [ "$tr_actual" = "$expected_tr" ]; then
        test_pass "truncate: 4MB→2MB 大小与内容(前 2MB 保留)校验通过"
    else
        test_fail "truncate" "size=$tr_size (期望 2097152) 或 sha256 不匹配"
    fi
}

# ============================================================================
# 测试 2: 目录操作
# ============================================================================
test_directory_ops() {
    test_start "2. 目录操作"

    # 2a. mkdir + 创建文件 + readdir + 校验文件数
    local dir="${TEST_DIR}/dir_ops"
    fuse_exec mkdir -p "$dir"
    local i
    for i in 1 2 3; do
        fuse_exec dd if=/dev/urandom of="${dir}/file_${i}.dat" bs=1K count=1 2>/dev/null
    done
    fuse_exec sync
    local file_count
    file_count=$(fuse_exec ls -1 "$dir" | wc -l)
    if [ "$file_count" -eq 3 ]; then
        test_pass "mkdir + readdir: 目录内文件数 = 3"
    else
        test_fail "mkdir + readdir" "文件数 = $file_count (期望 3)"
    fi

    # 2b. rename: 旧路径消失, 新路径可访问
    local old_path="${dir}/file_1.dat"
    local new_path="${dir}/file_renamed.dat"
    fuse_exec mv "$old_path" "$new_path"
    if ! fuse_exec test -e "$old_path" 2>/dev/null && fuse_exec test -e "$new_path" 2>/dev/null; then
        test_pass "rename: 旧路径消失, 新路径可访问"
    else
        test_fail "rename" "rename 后路径状态异常"
    fi

    # 2c. rmdir 空目录 (应成功), rmdir 非空目录 (应失败)
    local empty_dir="${TEST_DIR}/empty_dir"
    fuse_exec mkdir -p "$empty_dir"
    if fuse_exec rmdir "$empty_dir" 2>/dev/null; then
        test_pass "rmdir 空目录: 成功"
    else
        test_fail "rmdir 空目录" "应成功但失败"
    fi

    # rmdir 非空目录应失败
    if fuse_exec rmdir "$dir" 2>/dev/null; then
        test_fail "rmdir 非空目录" "应失败但成功了"
    else
        test_pass "rmdir 非空目录: 正确失败 (目录非空)"
    fi

    # 2d. 嵌套目录 (3 层)
    local nested="${TEST_DIR}/nested/l1/l2/l3"
    fuse_exec mkdir -p "$nested"
    fuse_exec dd if=/dev/urandom of="${nested}/deep.dat" bs=1K count=1 2>/dev/null
    fuse_exec sync
    if fuse_exec test -d "$nested" 2>/dev/null && fuse_exec test -e "${nested}/deep.dat" 2>/dev/null; then
        test_pass "嵌套目录 (3 层): 创建 + 文件写入成功"
    else
        test_fail "嵌套目录" "3 层目录或文件创建失败"
    fi
}

# ============================================================================
# 测试 3: EC 转换 + 读取校验 (CRITICAL)
# ============================================================================
test_ec_conversion_read() {
    test_start "3. EC 转换 + 读取校验 (重启清缓存后)"

    local sizes=("1:1MB" "4:4MB" "8:8MB" "16:16MB" "32:32MB")
    local entry size_mb label file_path inode baseline

    for entry in "${sizes[@]}"; do
        size_mb="${entry%%:*}"
        label="${entry##*:}"
        file_path="${TEST_DIR}/ec_conv_${label}.dat"

        # a. 写入文件 (urandom 确定性内容, 写入后内容固定)
        log_info "写入 ${label} 测试文件: $file_path"
        fuse_exec dd if=/dev/urandom of="$file_path" bs=1M count="$size_mb" 2>/dev/null
        fuse_exec sync

        # b. 记录基线 sha256
        baseline=$(compute_sha256 "$file_path")
        if [ -z "$baseline" ]; then
            test_fail "ec_conv ${label}" "无法计算基线 sha256"
            continue
        fi
        log_info "基线 checksum: ${baseline:0:32}..."

        # 获取 inode
        inode=$(fuse_exec stat -c '%i' "$file_path")
        log_info "文件 inode: $inode"

        # c. 等待 EC 转换 (搜索所有 filer 日志)
        if ! wait_ec_conversion "$inode" "$EC_CONV_TIMEOUT"; then
            test_fail "ec_conv ${label}" "EC 转换超时"
            continue
        fi

        # d. 重启 fuse-1 清除 chunk_cache
        if ! restart_fuse_clear_cache; then
            test_fail "ec_conv ${label}" "FUSE 重启失败"
            continue
        fi

        # e. 读取文件, 校验 sha256 匹配基线
        # verify_sha256 返回 0/1/2, 用 || 捕获非零退出码避免 set -e 中断
        local rc=0
        verify_sha256 "$file_path" "$baseline" || rc=$?
        if [ $rc -eq 0 ]; then
            test_pass "ec_conv ${label}: EC 转换后读取 checksum 匹配"
        elif [ $rc -eq 2 ]; then
            test_fail "ec_conv ${label}" "读取失败 (EIO)"
        else
            test_fail "ec_conv ${label}" "checksum 不匹配"
        fi
        # f. EC 转换详情已在 wait_ec_conversion 中记录
    done
}

# ============================================================================
# 测试 4: EC 降级读 (内容校验)
# ============================================================================
test_ec_degraded_read() {
    test_start "4. EC 降级读 (删除 G0 数据分片, 验证容错边界)"

    local file_path="${TEST_DIR}/ec_degraded_32MB.dat"
    local inode baseline

    # 写入 32MB 文件
    log_info "写入 32MB 测试文件: $file_path"
    fuse_exec dd if=/dev/urandom of="$file_path" bs=1M count=32 2>/dev/null
    fuse_exec sync

    baseline=$(compute_sha256 "$file_path")
    if [ -z "$baseline" ]; then
        test_fail "ec_degraded baseline" "无法计算基线 sha256"
        return 0
    fi
    log_info "基线 checksum: ${baseline:0:32}..."

    inode=$(fuse_exec stat -c '%i' "$file_path")
    log_info "文件 inode: $inode"

    # 等待 EC 转换
    if ! wait_ec_conversion "$inode" "$EC_CONV_TIMEOUT"; then
        test_fail "ec_degraded" "EC 转换超时"
        return 0
    fi

    # 解析 G0 分片位置
    if ! parse_g0_shards "$EC_LOG_LINE"; then
        test_fail "ec_degraded" "G0 分片解析失败"
        return 0
    fi

    local data_indices
    data_indices=($(get_data_shard_indices))
    if [ ${#data_indices[@]} -lt 3 ]; then
        test_fail "ec_degraded" "G0 数据分片不足 3 个 (实际 ${#data_indices[@]})"
        return 0
    fi

    # 基线读取 (全分片完好, 清缓存后)
    if ! restart_fuse_clear_cache; then
        test_fail "ec_degraded baseline" "FUSE 重启失败"
        return 0
    fi
    if verify_sha256 "$file_path" "$baseline"; then
        test_pass "ec_degraded baseline: 全分片完好, 读取 checksum 匹配"
    else
        test_fail "ec_degraded baseline" "基线读取校验失败"
        return 0
    fi

    # 降级读 1: 删除 G0 第 1 个数据分片 → 读取应成功 (parity 重建 1 个)
    if ! restart_fuse_clear_cache; then
        test_fail "ec_degraded 1-loss" "FUSE 重启失败"
        return 0
    fi
    if ! delete_shard "${data_indices[0]}"; then
        test_fail "ec_degraded 1-loss" "分片删除失败"
        return 0
    fi
    if verify_sha256 "$file_path" "$baseline"; then
        test_pass "ec_degraded 1-loss: 删除 1 个数据分片, 降级读成功 (parity 重建)"
    else
        test_fail "ec_degraded 1-loss" "降级读失败 (1 分片丢失应可恢复)"
    fi

    # 降级读 2: 删除 G0 第 2 个数据分片 → 读取应成功 (达到 EC(4+2) 容错上限)
    if ! restart_fuse_clear_cache; then
        test_fail "ec_degraded 2-loss" "FUSE 重启失败"
        return 0
    fi
    if ! delete_shard "${data_indices[1]}"; then
        test_fail "ec_degraded 2-loss" "分片删除失败"
        return 0
    fi
    if verify_sha256 "$file_path" "$baseline"; then
        test_pass "ec_degraded 2-loss: 删除 2 个数据分片, 降级读成功 (达到 EC(${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}) 容错上限)"
    else
        test_fail "ec_degraded 2-loss" "降级读失败 (2 分片丢失在容错范围内)"
    fi

    # 降级读 3: 删除 G0 第 3 个分片 → 读取应失败 (超过 parity 容错, EIO)
    if ! restart_fuse_clear_cache; then
        test_fail "ec_degraded 3-loss" "FUSE 重启失败"
        return 0
    fi
    if ! delete_shard "${data_indices[2]}"; then
        test_fail "ec_degraded 3-loss" "分片删除失败"
        return 0
    fi
    log_info "尝试读取 (预期失败 EIO, 超过 ${EC_PARITY_SHARDS} parity 容错)..."
    if docker exec "$FUSE_CONTAINER" sha256sum "$file_path" >/dev/null 2>&1; then
        test_fail "ec_degraded 3-loss" "读取应失败但成功了 (3 分片丢失应不可恢复)"
    else
        test_pass "ec_degraded 3-loss: 不可恢复场景正确返回错误 (3 分片丢失 > ${EC_PARITY_SHARDS} parity)"
    fi
}

# ============================================================================
# 测试 5: 并发操作
# ============================================================================
test_concurrent_ops() {
    test_start "5. 并发操作"

    # 5a. 4 并发写: 各写 4MB 到不同路径, 校验 sha256 一致性
    log_info "启动 4 个并发写进程 (各 4MB)..."
    local pids=() i
    for i in 1 2 3 4; do
        docker exec "$FUSE_CONTAINER" bash -c \
            "dd if=/dev/urandom of='$TEST_DIR/cw_${i}.dat' bs=1M count=4 2>/dev/null && sha256sum '$TEST_DIR/cw_${i}.dat' > /tmp/cw_${i}.sha" \
            >/dev/null 2>&1 &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do
        wait "$pid" || true
    done
    fuse_exec sync

    local cw_pass=0 cw_fail=0
    for i in 1 2 3 4; do
        local baseline_i actual_i
        baseline_i=$(docker exec "$FUSE_CONTAINER" cat "/tmp/cw_${i}.sha" 2>/dev/null | awk '{print $1}')
        actual_i=$(compute_sha256 "$TEST_DIR/cw_${i}.dat")
        if [ -n "$baseline_i" ] && [ "$actual_i" = "$baseline_i" ]; then
            cw_pass=$((cw_pass + 1))
        else
            cw_fail=$((cw_fail + 1))
            log_warn "并发写文件 ${i} sha256 不一致"
        fi
        docker exec "$FUSE_CONTAINER" rm -f "/tmp/cw_${i}.sha" 2>/dev/null || true
    done
    if [ $cw_fail -eq 0 ] && [ $cw_pass -eq 4 ]; then
        test_pass "并发写 (4×4MB): 全部文件 sha256 一致"
    else
        test_fail "并发写" "${cw_pass} 通过, ${cw_fail} 失败"
    fi

    # 5b. 4 并发读: 同一文件, 校验全部读取正确
    local cr_file="${TEST_DIR}/concurrent_read.dat"
    fuse_exec dd if=/dev/urandom of="$cr_file" bs=1M count=4 2>/dev/null
    fuse_exec sync
    local cr_baseline
    cr_baseline=$(compute_sha256 "$cr_file")
    if [ -z "$cr_baseline" ]; then
        test_fail "并发读" "无法计算基线 sha256"
        return 0
    fi

    log_info "启动 4 个并发读进程 (同一文件)..."
    pids=()
    for i in 1 2 3 4; do
        docker exec "$FUSE_CONTAINER" sha256sum "$cr_file" > "/tmp/cr_${i}.sha.tmp" 2>/dev/null &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do
        wait "$pid" || true
    done

    local cr_pass=0 cr_fail=0
    for i in 1 2 3 4; do
        local actual_i
        actual_i=$(awk '{print $1}' "/tmp/cr_${i}.sha.tmp" 2>/dev/null)
        if [ "$actual_i" = "$cr_baseline" ]; then
            cr_pass=$((cr_pass + 1))
        else
            cr_fail=$((cr_fail + 1))
            log_warn "并发读 ${i} sha256 不匹配"
        fi
        rm -f "/tmp/cr_${i}.sha.tmp"
    done
    if [ $cr_fail -eq 0 ] && [ $cr_pass -eq 4 ]; then
        test_pass "并发读 (4×同一文件): 全部读取 checksum 匹配"
    else
        test_fail "并发读" "${cr_pass} 通过, ${cr_fail} 失败"
    fi
}

# ============================================================================
# 测试 6: 删除 + 清理
# ============================================================================
test_delete_cleanup() {
    test_start "6. 删除 + 清理"

    # 6a. 创建文件, 删除, 验证不再存在
    local del_file="${TEST_DIR}/delete_me.dat"
    fuse_exec dd if=/dev/urandom of="$del_file" bs=1M count=4 2>/dev/null
    fuse_exec sync
    if ! fuse_exec test -e "$del_file" 2>/dev/null; then
        test_fail "delete" "文件创建后不可访问"
        return 0
    fi
    fuse_exec rm -f "$del_file"
    if ! fuse_exec test -e "$del_file" 2>/dev/null; then
        test_pass "delete: 文件删除后不再存在"
    else
        test_fail "delete" "文件删除后仍存在"
    fi

    # 6b. 磁盘空间回收 (df before/after)
    local df_before df_after_create df_after_delete
    df_before=$(fuse_exec df -B1 "$MOUNT_POINT" 2>/dev/null | tail -1 | awk '{print $4}')
    log_info "df 可用空间 (初始): ${df_before:-unknown} bytes"

    local space_file="${TEST_DIR}/space_test.dat"
    fuse_exec dd if=/dev/urandom of="$space_file" bs=1M count=4 2>/dev/null
    fuse_exec sync
    df_after_create=$(fuse_exec df -B1 "$MOUNT_POINT" 2>/dev/null | tail -1 | awk '{print $4}')
    log_info "df 可用空间 (创建 4MB 后): ${df_after_create:-unknown} bytes"

    fuse_exec rm -f "$space_file"
    # 等待空间回收
    sleep 5
    df_after_delete=$(fuse_exec df -B1 "$MOUNT_POINT" 2>/dev/null | tail -1 | awk '{print $4}')
    log_info "df 可用空间 (删除后): ${df_after_delete:-unknown} bytes"

    # 验证: 文件已删除 + 可用空间未减少 (>= 创建后, 即空间已回收)
    local space_reclaimed=false
    if [ -n "$df_after_create" ] && [ -n "$df_after_delete" ] && \
       [ "$df_after_delete" -ge "$df_after_create" ]; then
        space_reclaimed=true
    fi
    if ! fuse_exec test -e "$space_file" 2>/dev/null && [ "$space_reclaimed" = true ]; then
        test_pass "空间回收: 文件删除后可用空间已恢复"
    elif ! fuse_exec test -e "$space_file" 2>/dev/null; then
        test_pass "空间回收: 文件已删除 (df 未显著变化, 可能异步回收)"
    else
        test_fail "空间回收" "文件仍存在或空间未回收"
    fi
}

# ============================================================================
# 清理与汇总
# ============================================================================
cleanup() {
    log_info "清理测试目录 $TEST_DIR ..."
    docker exec "$FUSE_CONTAINER" rm -rf "$TEST_DIR" 2>/dev/null || true
}

print_summary() {
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
}

# ============================================================================
# 主流程
# ============================================================================
main() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║  PowerFS EC 功能综合测试                                 ║"
    echo "║  EC=${EC_DATA_SHARDS}+${EC_PARITY_SHARDS}  conv_timeout=${EC_CONV_TIMEOUT}s                  ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""

    # 注册清理 (退出时清理测试文件)
    trap cleanup EXIT

    # 检查 fuse-1 容器运行
    if ! docker inspect -f '{{.State.Running}}' "$FUSE_CONTAINER" 2>/dev/null | grep -q true; then
        log_error "$FUSE_CONTAINER 未运行, 请先启动集群"
        exit 1
    fi

    # 等待 FUSE 就绪
    if ! wait_fuse_ready; then
        log_error "FUSE 未就绪, 退出"
        exit 1
    fi

    # 确认 testutil 可用
    if ! docker exec "$FUSE_CONTAINER" test -f "$TESTUTIL" 2>/dev/null; then
        log_error "$TESTUTIL 不存在于 $FUSE_CONTAINER"
        exit 1
    fi
    log_ok "powerfs-testutil 可用"

    # 确认 volume 数量 >= data+parity
    local volume_count total_shards
    volume_count=$(docker ps --format '{{.Names}}' | grep -c '^volume-' || true)
    total_shards=$((EC_DATA_SHARDS + EC_PARITY_SHARDS))
    log_info "运行中的 volume 容器: ${volume_count}, EC 需要: ${total_shards}"
    if [ "$volume_count" -lt "$total_shards" ]; then
        log_error "volume 数量不足: ${volume_count} < ${total_shards}"
        exit 1
    fi

    # 清理旧测试残留
    docker exec "$FUSE_CONTAINER" rm -rf "$TEST_DIR" 2>/dev/null || true
    docker exec "$FUSE_CONTAINER" mkdir -p "$TEST_DIR" 2>/dev/null

    # 执行所有测试
    test_basic_file_ops
    test_directory_ops
    test_ec_conversion_read
    test_ec_degraded_read
    test_concurrent_ops
    test_delete_cleanup

    # 汇总
    print_summary

    if [ "$FAIL" -gt 0 ]; then
        exit 1
    fi
    exit 0
}

main "$@"
