#!/usr/bin/env bash
# ================================================================
# PowerFS 功能测试统一断言库
#
# 所有功能测试脚本 source 此库，确保 PASS/FAIL 判定标准一致。
# 核心原则：不只看命令 exit code，必须验证 stat 字段 + MD5 内容一致性。
#
# 用法：
#   source tests/lib/assertions.sh
#   CONTAINER=fuse-1
#   assert_stat "size check" /path '%s' "100"
# ================================================================
set -u

PASS=0; FAIL=0; SKIP=0
C_RED='\033[0;31m'; C_GREEN='\033[0;32m'; C_YELLOW='\033[1;33m'
C_CYAN='\033[0;36m'; C_RESET='\033[0m'

pass() { PASS=$((PASS+1)); echo -e "  ${C_GREEN}[PASS]${C_RESET} $1"; }
skip() { SKIP=$((SKIP+1)); echo -e "  ${C_YELLOW}[SKIP]${C_RESET} $1"; }
section() { echo ""; echo -e "${C_CYAN}━━━ $1 ━━━${C_RESET}"; }

fail() {
    FAIL=$((FAIL+1))
    echo -e "  ${C_RED}[FAIL]${C_RESET} $1"
    if [[ -n "${2:-}" ]]; then echo -e "    ${C_RED}expected:${C_RESET} $2"; fi
    if [[ -n "${3:-}" ]]; then echo -e "    ${C_RED}actual:${C_RESET}   $3"; fi
}

# ---- 核心断言 ----

# 断言两个值完全相等
assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        pass "$desc (='$actual')"
    else
        fail "$desc" "$expected" "$actual"
        return 1
    fi
}

# 断言命令执行成功 (exit code == 0)
assert_ok() {
    local desc="$1"; shift
    if "$@" >/dev/null 2>&1; then
        pass "$desc"
    else
        fail "$desc (command failed)" "exit 0" "exit $?"
        return 1
    fi
}

# 断言命令执行失败 (exit code != 0)
assert_fail() {
    local desc="$1"; shift
    if "$@" >/dev/null 2>&1; then
        fail "$desc (should have failed)" "exit != 0" "exit 0"
        return 1
    else
        pass "$desc"
    fi
}

# 断言文件/目录存在（在 CONTAINER 内检查）
assert_exists() {
    local desc="$1" path="$2"
    if docker exec "${CONTAINER:-fuse-1}" test -e "$path" 2>/dev/null; then
        pass "$desc (exists: $path)"
    else
        fail "$desc" "$path exists" "$path missing"
        return 1
    fi
}

# 断言文件/目录不存在（在 CONTAINER 内检查）
assert_not_exists() {
    local desc="$1" path="$2"
    if docker exec "${CONTAINER:-fuse-1}" test ! -e "$path" 2>/dev/null; then
        pass "$desc (absent: $path)"
    else
        fail "$desc" "$path absent" "$path still exists"
        return 1
    fi
}

# 断言 MD5 一致（单容器内）
# 用法: assert_md5_match "desc" /path "expected_md5"
assert_md5_match() {
    local desc="$1" path="$2" expected_md5="$3"
    local actual_md5
    actual_md5=$(docker exec "${CONTAINER:-fuse-1}" md5sum "$path" 2>/dev/null | awk '{print $1}')
    if [[ "$actual_md5" == "$expected_md5" ]]; then
        pass "$desc (md5=${actual_md5:0:12}...)"
    else
        fail "$desc" "$expected_md5" "$actual_md5"
        return 1
    fi
}

# 断言 stat 字段
# 用法: assert_stat "desc" /path '%s %a %u %g %h' "100 644 0 0 1"
assert_stat() {
    local desc="$1" path="$2" fmt="$3" expected="$4"
    local actual
    actual=$(docker exec "${CONTAINER:-fuse-1}" stat -c "$fmt" "$path" 2>/dev/null | tr -d '\r')
    if [[ "$actual" == "$expected" ]]; then
        pass "$desc (stat='$actual')"
    else
        fail "$desc" "$expected" "$actual"
        return 1
    fi
}

# 断言跨容器 MD5 一致
# 用法: assert_md5_cross "desc" /path container_a container_b
assert_md5_cross() {
    local desc="$1" path="$2" container_a="$3" container_b="$4"
    local md5_a md5_b
    md5_a=$(docker exec "$container_a" md5sum "$path" 2>/dev/null | awk '{print $1}')
    md5_b=$(docker exec "$container_b" md5sum "$path" 2>/dev/null | awk '{print $1}')
    if [[ -n "$md5_a" && "$md5_a" == "$md5_b" ]]; then
        pass "$desc (both=${md5_a:0:12}...)"
    else
        fail "$desc" "md5 match" "$container_a=$md5_a, $container_b=$md5_b"
        return 1
    fi
}

# ---- 环境辅助 ----

# 在指定容器内 drop page cache（跨客户端读取前必须调用）
drop_cache() {
    docker exec "$1" sh -c 'sync; echo 2 > /proc/sys/vm/drop_caches' 2>/dev/null || true
}

# 在容器内执行命令（简化写法）
exec_in() {
    docker exec "${1:-${CONTAINER:-fuse-1}}" sh -c "${2:?missing command}"
}

# ---- 日志检查 ----

# 检查容器日志中是否有 error/panic/deadlock
# 用法: check_logs_clean "stage name" container_name
check_logs_clean() {
    local stage="$1" container="${2:-filer-1}"
    local errors
    errors=$(docker logs "$container" 2>&1 | tail -200 | \
        grep -iE 'panic|deadlock|unwrap.*None|thread.*crashed' | \
        grep -v 'grep' || true)
    if [[ -n "$errors" ]]; then
        fail "$stage: $container log has critical errors"
        echo "$errors" | head -10 | sed 's/^/    /'
    else
        pass "$stage: $container log clean"
    fi
}

# ---- 汇总 ----

print_summary() {
    echo ""
    echo -e "${C_CYAN}━━━ Summary ━━━${C_RESET}"
    echo -e "  ${C_GREEN}PASS${C_RESET}: $PASS"
    echo -e "  ${C_RED}FAIL${C_RESET}: $FAIL"
    echo -e "  ${C_YELLOW}SKIP${C_RESET}: $SKIP"
    if [[ $FAIL -gt 0 ]]; then
        echo -e "  ${C_RED}RESULT: FAILED${C_RESET}"
        return 1
    else
        echo -e "  ${C_GREEN}RESULT: ALL PASS${C_RESET}"
        return 0
    fi
}

# ---- 前置检查 ----

# 验证所有核心容器运行中 + 挂载点可访问 + 基础读写 + 跨客户端可见
preflight() {
    section "Preflight: Environment Check"

    local containers=(master-1 master-2 master-3 filer-1 filer-2 filer-3 \
                      volume-1 volume-2 volume-3 fuse-1 fuse-2)
    for c in "${containers[@]}"; do
        if docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -q true; then
            : # running
        else
            fail "container $c running" "true" "false/missing"
            return 1
        fi
    done
    pass "all core containers running"

    # 挂载点
    docker exec fuse-1 test -d /mnt/powerfs 2>/dev/null || { fail "fuse-1 mount" "exists" "missing"; return 1; }
    docker exec fuse-2 test -d /mnt/powerfs 2>/dev/null || { fail "fuse-2 mount" "exists" "missing"; return 1; }
    pass "both fuse mount points accessible"

    # 基础 round-trip
    local pf_file="/mnt/powerfs/.preflight_$$"
    docker exec fuse-1 sh -c "echo preflight > '$pf_file'" 2>/dev/null
    local result
    result=$(docker exec fuse-1 cat "$pf_file" 2>/dev/null | tr -d '\r\n')
    if [[ "$result" == "preflight" ]]; then
        pass "basic write+read round-trip"
    else
        fail "basic write+read" "preflight" "$result"
        return 1
    fi

    # 跨客户端可见性
    drop_cache fuse-2
    result=$(docker exec fuse-2 cat "$pf_file" 2>/dev/null | tr -d '\r\n')
    if [[ "$result" == "preflight" ]]; then
        pass "cross-client visibility (fuse-1 → fuse-2)"
    else
        fail "cross-client visibility" "preflight" "$result"
        return 1
    fi

    # 清理
    docker exec fuse-1 rm -f "$pf_file" 2>/dev/null
    echo ""
}
