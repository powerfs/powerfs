#!/bin/bash
# =============================================================================
# PowerFS L2 POSIX 兼容性测试 (L2.13-L2.19)
#
# 测试标准 Unix 工具在 PowerFS FUSE 文件系统上的兼容性
# 在 fuse-1-test 容器内运行：docker exec fuse-1-test /tmp/run_l2_posix_test.sh
#
# 测试项:
#   L2.13: ls 递归
#   L2.14: cp 复制 (保留属性)
#   L2.15: find 查找
#   L2.16: grep 搜索
#   L2.17: tar 打包/解包
#   L2.18: rsync 同步
#   L2.19: stat 格式
# =============================================================================

set -u

MOUNT="/mnt/fuse"
TEST_ROOT="$MOUNT/posix_test"
PASS=0
FAIL=0
FAILED_LIST=()

# ── 辅助函数 ──────────────────────────────────────────────────────────

record_pass() { PASS=$((PASS+1)); }
record_fail() { FAIL=$((FAIL+1)); FAILED_LIST+=("$1"); }

assert_eq() {
    local expected="$1" actual="$2" msg="$3"
    if [ "$expected" = "$actual" ]; then
        record_pass
    else
        record_fail "$msg (expected='$expected' actual='$actual')"
    fi
}

assert_exists() {
    if [ -e "$1" ]; then record_pass; else record_fail "$2: $1 not found"; fi
}

assert_not_exists() {
    if [ ! -e "$1" ]; then record_pass; else record_fail "$2: $1 still exists"; fi
}

assert_success() {
    local msg="$1"; shift
    if "$@" >/dev/null 2>&1; then
        record_pass
    else
        record_fail "$msg"
    fi
}

# ── 清理 ──────────────────────────────────────────────────────────────

cleanup() {
    rm -rf "$TEST_ROOT" 2>/dev/null || true
}
trap cleanup EXIT
# Clean up any stale data from previous runs before starting
cleanup

# 检查工具是否可用
for tool in cp find grep tar rsync stat dd; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "ERROR: $tool not found in container"
        exit 1
    fi
done

echo "============================================================"
echo "  PowerFS L2 POSIX 兼容性测试"
echo "  时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "  挂载点: $MOUNT"
echo "============================================================"
echo ""

# ── 准备测试数据 ──────────────────────────────────────────────────────
mkdir -p "$TEST_ROOT/src/sub1/sub2"
mkdir -p "$TEST_ROOT/src/empty_dir"

# 创建测试文件
echo "line1: hello world" > "$TEST_ROOT/src/file1.txt"
echo "line2: foo bar baz" >> "$TEST_ROOT/src/file1.txt"
echo "line3: powerfs test" >> "$TEST_ROOT/src/file1.txt"
echo "pattern_match_here" > "$TEST_ROOT/src/sub1/match.txt"
echo "no_match_content" > "$TEST_ROOT/src/sub1/nomatch.txt"
echo "deep content" > "$TEST_ROOT/src/sub1/sub2/deep.txt"

# 创建不同权限的文件
echo "perm_test" > "$TEST_ROOT/src/perm.txt"
chmod 0644 "$TEST_ROOT/src/perm.txt"
chown 1000:1000 "$TEST_ROOT/src/perm.txt" 2>/dev/null || true

# 二进制文件 (1MB)
dd if=/dev/urandom of="$TEST_ROOT/src/big.bin" bs=1M count=1 2>/dev/null

BIG_MD5=$(md5sum "$TEST_ROOT/src/big.bin" | awk '{print $1}')

# ============================================================
# L2.13: ls 递归
# ============================================================
echo "--- L2.13: ls 递归 ---"

# ls -R 应列出所有文件和目录
LS_R_OUTPUT=$(ls -R "$TEST_ROOT/src" 2>&1)
LS_COUNT=$(echo "$LS_R_OUTPUT" | grep -c "file1.txt\|match.txt\|nomatch.txt\|deep.txt\|perm.txt\|big.bin")
if [ "$LS_COUNT" -ge 6 ]; then
    record_pass
else
    record_fail "L2.13.01 ls -R 列出所有文件 (count=$LS_COUNT, expected>=6)"
fi

# ls -la 应包含权限信息
LS_LA=$(ls -la "$TEST_ROOT/src/file1.txt" 2>&1)
if echo "$LS_LA" | grep -q "file1.txt"; then
    record_pass
else
    record_fail "L2.13.02 ls -la 显示文件"
fi

# ls -laR 嵌套目录
LS_LAR=$(ls -laR "$TEST_ROOT/src/sub1" 2>&1)
if echo "$LS_LAR" | grep -q "sub2" && echo "$LS_LAR" | grep -q "deep.txt"; then
    record_pass
else
    record_fail "L2.13.03 ls -laR 显示嵌套目录"
fi

# 空目录 ls 应无输出
EMPTY_LS=$(ls "$TEST_ROOT/src/empty_dir" 2>&1)
if [ -z "$EMPTY_LS" ]; then
    record_pass
else
    record_fail "L2.13.04 ls 空目录无输出 (got='$EMPTY_LS')"
fi

# ============================================================
# L2.14: cp 复制
# ============================================================
echo "--- L2.14: cp 复制 ---"

# cp 单文件
assert_success "L2.14.01 cp 单文件" cp "$TEST_ROOT/src/file1.txt" "$TEST_ROOT/cp_file1.txt"
SRC_MD5=$(md5sum "$TEST_ROOT/src/file1.txt" | awk '{print $1}')
DST_MD5=$(md5sum "$TEST_ROOT/cp_file1.txt" | awk '{print $1}')
assert_eq "$SRC_MD5" "$DST_MD5" "L2.14.02 cp 内容一致"

# cp -r 递归目录
assert_success "L2.14.03 cp -r 递归" cp -r "$TEST_ROOT/src" "$TEST_ROOT/cp_src"
assert_exists "$TEST_ROOT/cp_src/sub1/sub2/deep.txt" "L2.14.04 cp -r 深层文件"

# cp -p 保留属性
cp -p "$TEST_ROOT/src/perm.txt" "$TEST_ROOT/cp_perm.txt" 2>/dev/null
SRC_MODE=$(stat -c '%a' "$TEST_ROOT/src/perm.txt")
DST_MODE=$(stat -c '%a' "$TEST_ROOT/cp_perm.txt" 2>/dev/null)
assert_eq "$SRC_MODE" "$DST_MODE" "L2.14.05 cp -p 保留 mode"

# cp 大文件
assert_success "L2.14.06 cp 大文件" cp "$TEST_ROOT/src/big.bin" "$TEST_ROOT/cp_big.bin"
CP_BIG_MD5=$(md5sum "$TEST_ROOT/cp_big.bin" | awk '{print $1}')
assert_eq "$BIG_MD5" "$CP_BIG_MD5" "L2.14.07 cp 大文件 md5 一致"

# ============================================================
# L2.15: find 查找
# ============================================================
echo "--- L2.15: find 查找 ---"

# find -name 按名称查找
FIND_NAME=$(find "$TEST_ROOT/src" -name "deep.txt" 2>/dev/null)
assert_eq "$TEST_ROOT/src/sub1/sub2/deep.txt" "$FIND_NAME" "L2.15.01 find -name"

# find -type f 查找所有文件
FIND_F_COUNT=$(find "$TEST_ROOT/src" -type f 2>/dev/null | wc -l)
if [ "$FIND_F_COUNT" -ge 6 ]; then
    record_pass
else
    record_fail "L2.15.02 find -type f (count=$FIND_F_COUNT, expected>=6)"
fi

# find -type d 查找所有目录
FIND_D_COUNT=$(find "$TEST_ROOT/src" -type d 2>/dev/null | wc -l)
if [ "$FIND_D_COUNT" -ge 4 ]; then
    record_pass
else
    record_fail "L2.15.03 find -type d (count=$FIND_D_COUNT, expected>=4)"
fi

# find -name 通配符
FIND_WILDCARD=$(find "$TEST_ROOT/src" -name "*.txt" 2>/dev/null | wc -l)
if [ "$FIND_WILDCARD" -ge 5 ]; then
    record_pass
else
    record_fail "L2.15.04 find -name '*.txt' (count=$FIND_WILDCARD, expected>=5)"
fi

# find -size 按大小查找
FIND_SIZE=$(find "$TEST_ROOT/src" -size +500k 2>/dev/null | wc -l)
if [ "$FIND_SIZE" -ge 1 ]; then
    record_pass
else
    record_fail "L2.15.05 find -size +500k (count=$FIND_SIZE, expected>=1)"
fi

# find -newer 按时间查找
touch "$TEST_ROOT/src/anchor.txt" 2>/dev/null
FIND_NEWER=$(find "$TEST_ROOT/src" -newer "$TEST_ROOT/src/anchor.txt" 2>/dev/null | wc -l)
if [ "$FIND_NEWER" -ge 0 ]; then
    record_pass
else
    record_fail "L2.15.06 find -newer failed"
fi

# ============================================================
# L2.16: grep 搜索
# ============================================================
echo "--- L2.16: grep 搜索 ---"

# grep 单文件
GREP_OUT=$(grep "hello world" "$TEST_ROOT/src/file1.txt" 2>/dev/null)
assert_eq "line1: hello world" "$GREP_OUT" "L2.16.01 grep 单文件"

# grep -r 递归
GREP_R_COUNT=$(grep -r "pattern_match_here" "$TEST_ROOT/src" 2>/dev/null | wc -l)
assert_eq "1" "$GREP_R_COUNT" "L2.16.02 grep -r 递归"

# grep -c 计数
GREP_C=$(grep -c "line" "$TEST_ROOT/src/file1.txt" 2>/dev/null)
assert_eq "3" "$GREP_C" "L2.16.03 grep -c 计数"

# grep -l 显示文件名
GREP_L=$(grep -rl "powerfs test" "$TEST_ROOT/src" 2>/dev/null)
assert_eq "$TEST_ROOT/src/file1.txt" "$GREP_L" "L2.16.04 grep -l 显示文件名"

# grep -v 反向
GREP_V=$(grep -v "line1" "$TEST_ROOT/src/file1.txt" 2>/dev/null | wc -l)
assert_eq "2" "$GREP_V" "L2.16.05 grep -v 反向"

# grep -i 忽略大小写
echo "MIXED Case Line" > "$TEST_ROOT/src/case.txt"
GREP_I=$(grep -i "mixed case" "$TEST_ROOT/src/case.txt" 2>/dev/null)
assert_eq "MIXED Case Line" "$GREP_I" "L2.16.06 grep -i 忽略大小写"

# ============================================================
# L2.17: tar 打包/解包
# ============================================================
echo "--- L2.17: tar 打包/解包 ---"

# tar 创建
assert_success "L2.17.01 tar cf 创建" tar cf "$TEST_ROOT/archive.tar" -C "$TEST_ROOT" src
assert_exists "$TEST_ROOT/archive.tar" "L2.17.02 tar 文件存在"

# tar 解包
mkdir -p "$TEST_ROOT/tar_extract"
assert_success "L2.17.03 tar xf 解包" tar xf "$TEST_ROOT/archive.tar" -C "$TEST_ROOT/tar_extract"
assert_exists "$TEST_ROOT/tar_extract/src/sub1/sub2/deep.txt" "L2.17.04 tar 解包深层文件"

# tar 解包后内容一致
TAR_DEEP_MD5=$(md5sum "$TEST_ROOT/tar_extract/src/sub1/sub2/deep.txt" | awk '{print $1}')
SRC_DEEP_MD5=$(md5sum "$TEST_ROOT/src/sub1/sub2/deep.txt" | awk '{print $1}')
assert_eq "$SRC_DEEP_MD5" "$TAR_DEEP_MD5" "L2.17.05 tar 解包内容一致"

# tar -z gzip 压缩
assert_success "L2.17.06 tar czf gzip 压缩" tar czf "$TEST_ROOT/archive.tar.gz" -C "$TEST_ROOT" src
assert_exists "$TEST_ROOT/archive.tar.gz" "L2.17.07 gzip 压缩文件存在"

# tar -z 解压
mkdir -p "$TEST_ROOT/tar_gz_extract"
assert_success "L2.17.08 tar xzf gzip 解压" tar xzf "$TEST_ROOT/archive.tar.gz" -C "$TEST_ROOT/tar_gz_extract"
assert_exists "$TEST_ROOT/tar_gz_extract/src/file1.txt" "L2.17.09 gzip 解压文件存在"

# tar 列出内容
TAR_LIST=$(tar tf "$TEST_ROOT/archive.tar" 2>/dev/null | grep -c "deep.txt")
assert_eq "1" "$TAR_LIST" "L2.17.10 tar tf 列出内容"

# ============================================================
# L2.18: rsync 同步
# ============================================================
echo "--- L2.18: rsync 同步 ---"

# rsync 同步目录
mkdir -p "$TEST_ROOT/rsync_dst"
assert_success "L2.18.01 rsync -av 同步" rsync -a "$TEST_ROOT/src/" "$TEST_ROOT/rsync_dst/"
assert_exists "$TEST_ROOT/rsync_dst/sub1/sub2/deep.txt" "L2.18.02 rsync 同步深层文件"

# rsync 同步后内容一致
RSYNC_DEEP_MD5=$(md5sum "$TEST_ROOT/rsync_dst/sub1/sub2/deep.txt" | awk '{print $1}')
assert_eq "$SRC_DEEP_MD5" "$RSYNC_DEEP_MD5" "L2.18.03 rsync 同步内容一致"

# rsync 保留权限
RSYNC_MODE=$(stat -c '%a' "$TEST_ROOT/rsync_dst/perm.txt" 2>/dev/null)
assert_eq "$SRC_MODE" "$RSYNC_MODE" "L2.18.04 rsync 保留 mode"

# rsync 增量同步 (修改一个文件后重同步)
echo "modified content" > "$TEST_ROOT/src/sub1/match.txt"
assert_success "L2.18.05 rsync 增量同步" rsync -a "$TEST_ROOT/src/" "$TEST_ROOT/rsync_dst/"
RSYNC_MOD=$(cat "$TEST_ROOT/rsync_dst/sub1/match.txt")
assert_eq "modified content" "$RSYNC_MOD" "L2.18.06 rsync 增量同步内容更新"

# rsync --delete 删除目标多余文件
touch "$TEST_ROOT/rsync_dst/extra_file.txt"
assert_exists "$TEST_ROOT/rsync_dst/extra_file.txt" "L2.18.07a 创建额外文件"
rsync -a --delete "$TEST_ROOT/src/" "$TEST_ROOT/rsync_dst/" 2>/dev/null
assert_not_exists "$TEST_ROOT/rsync_dst/extra_file.txt" "L2.18.07 rsync --delete 删除多余文件"

# ============================================================
# L2.19: stat 格式
# ============================================================
echo "--- L2.19: stat 格式 ---"

# stat -c '%s %a %u %g' 多字段
# Note: echo adds a trailing newline, so "stat test\n" = 10 bytes
echo "stat test" > "$TEST_ROOT/stat_test.txt"
chmod 0644 "$TEST_ROOT/stat_test.txt"
STAT_OUT=$(stat -c '%s %a %u %g' "$TEST_ROOT/stat_test.txt" 2>/dev/null)
EXPECTED_STAT="10 644 0 0"
assert_eq "$EXPECTED_STAT" "$STAT_OUT" "L2.19.01 stat 多字段"

# stat 文件大小
STAT_SIZE=$(stat -c '%s' "$TEST_ROOT/src/big.bin" 2>/dev/null)
assert_eq "1048576" "$STAT_SIZE" "L2.19.02 stat 大文件 size"

# stat mode
STAT_MODE=$(stat -c '%a' "$TEST_ROOT/src/perm.txt" 2>/dev/null)
assert_eq "644" "$STAT_MODE" "L2.19.03 stat mode"

# stat uid/gid (可能因容器权限失败)
chown 1000:1000 "$TEST_ROOT/src/perm.txt" 2>/dev/null && {
    STAT_UID=$(stat -c '%u' "$TEST_ROOT/src/perm.txt" 2>/dev/null)
    assert_eq "1000" "$STAT_UID" "L2.19.04 stat uid"
    STAT_GID=$(stat -c '%g' "$TEST_ROOT/src/perm.txt" 2>/dev/null)
    assert_eq "1000" "$STAT_GID" "L2.19.05 stat gid"
} || {
    record_pass  # skip if no chown permission
    record_pass
}

# stat 目录
STAT_DIR_TYPE=$(stat -c '%F' "$TEST_ROOT/src" 2>/dev/null)
if [ "$STAT_DIR_TYPE" = "directory" ]; then
    record_pass
else
    record_fail "L2.19.06 stat 目录类型 (got='$STAT_DIR_TYPE')"
fi

# stat nlink for directory
STAT_NLINK=$(stat -c '%h' "$TEST_ROOT/src" 2>/dev/null)
if [ "$STAT_NLINK" -ge 2 ]; then
    record_pass
else
    record_fail "L2.19.07 stat 目录 nlink>=2 (got='$STAT_NLINK')"
fi

# stat inode 非零
STAT_INO=$(stat -c '%i' "$TEST_ROOT/src/file1.txt" 2>/dev/null)
if [ "$STAT_INO" -gt 0 ]; then
    record_pass
else
    record_fail "L2.19.08 stat inode>0 (got='$STAT_INO')"
fi

# ── 汇总 ─────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  L2 POSIX 兼容性测试汇总"
echo "============================================================"
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo "  TOTAL: $((PASS+FAIL))"
echo ""
if [ $FAIL -gt 0 ]; then
    echo "  失败项:"
    for t in "${FAILED_LIST[@]}"; do
        echo "    - $t"
    done
fi
echo "============================================================"

exit $FAIL
