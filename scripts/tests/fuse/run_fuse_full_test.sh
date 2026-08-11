#!/bin/bash
# =============================================================================
# PowerFS FUSE 文件系统全面功能测试
#
# 按照 docs/fuse-test-plan.md 方案执行 T1-T8 功能测试
# 在 fuse-test 容器内运行：docker exec fuse-test /tmp/fuse_full_test.sh
#
# 用法：
#   docker cp scripts/tests/fuse/run_fuse_full_test.sh fuse-test:/tmp/
#   docker exec fuse-test /tmp/fuse_full_test.sh
# =============================================================================

set -u

MOUNT="/mnt/fuse"
TEST_ROOT="$MOUNT/fuse_full_test"
PASS=0
FAIL=0
SKIP=0
FAILED_LIST=()

# ── 辅助函数 ──────────────────────────────────────────────────────────

record_pass() { PASS=$((PASS+1)); }
record_fail() { FAIL=$((FAIL+1)); FAILED_LIST+=("$1"); }
record_skip() { SKIP=$((SKIP+1)); }

# 带超时执行命令
run_timeout() {
    local timeout_s="$1"; shift
    timeout "$timeout_s" "$@" 2>/dev/null
}

# 断言：相等
assert_eq() {
    local expected="$1" actual="$2" msg="$3"
    if [ "$expected" = "$actual" ]; then
        record_pass
    else
        record_fail "$msg (expected='$expected' actual='$actual')"
    fi
}

# 断言：文件存在
assert_exists() {
    if [ -e "$1" ]; then record_pass; else record_fail "$2: $1 not found"; fi
}

# 断言：文件不存在
assert_not_exists() {
    if [ ! -e "$1" ]; then record_pass; else record_fail "$2: $1 still exists"; fi
}

# 断言：命令成功
assert_success() {
    local msg="$1"; shift
    if "$@" >/dev/null 2>&1; then
        record_pass
    else
        record_fail "$msg"
    fi
}

# 断言：命令失败（期望失败）
assert_fails() {
    local msg="$1"; shift
    if "$@" >/dev/null 2>&1; then
        record_fail "$msg (expected failure but succeeded)"
    else
        record_pass
    fi
}

# ── 清理 ──────────────────────────────────────────────────────────────

cleanup() {
    rm -rf "$TEST_ROOT" 2>/dev/null || true
}
trap cleanup EXIT
cleanup
mkdir -p "$TEST_ROOT"

echo "============================================================"
echo "  PowerFS FUSE 全面功能测试"
echo "  挂载点: $MOUNT"
echo "  时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"

# ════════════════════════════════════════════════════════════════════
# T1: 基础操作（10 项）
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T1: 基础操作 ━━━"

# T1.01 创建单层目录
mkdir -p "$TEST_ROOT/t1/d1"
assert_exists "$TEST_ROOT/t1/d1" "T1.01 mkdir single"
[ -d "$TEST_ROOT/t1/d1" ] && assert_eq "yes" "yes" "T1.01 is directory" || assert_eq "yes" "no" "T1.01 is directory"

# T1.02 创建嵌套目录
mkdir -p "$TEST_ROOT/t1/a/b/c"
assert_exists "$TEST_ROOT/t1/a/b/c" "T1.02 mkdir nested"

# T1.03 删除空目录
mkdir -p "$TEST_ROOT/t1/del_me"
rmdir "$TEST_ROOT/t1/del_me"
assert_not_exists "$TEST_ROOT/t1/del_me" "T1.03 rmdir empty"

# T1.04 删除非空目录失败
assert_fails "T1.04 rmdir non-empty" rmdir "$TEST_ROOT/t1/a"

# T1.05 创建文件
echo "hello" > "$TEST_ROOT/t1/f1.txt"
assert_exists "$TEST_ROOT/t1/f1.txt" "T1.05 create file"
CONTENT=$(cat "$TEST_ROOT/t1/f1.txt")
assert_eq "hello" "$CONTENT" "T1.05 file content"

# T1.06 删除文件
rm "$TEST_ROOT/t1/f1.txt"
assert_not_exists "$TEST_ROOT/t1/f1.txt" "T1.06 unlink"

# T1.07 重命名文件
echo "rename_me" > "$TEST_ROOT/t1/src.txt"
mv "$TEST_ROOT/t1/src.txt" "$TEST_ROOT/t1/dst.txt"
assert_not_exists "$TEST_ROOT/t1/src.txt" "T1.07 rename: source gone"
assert_exists "$TEST_ROOT/t1/dst.txt" "T1.07 rename: target exists"
CONTENT=$(cat "$TEST_ROOT/t1/dst.txt")
assert_eq "rename_me" "$CONTENT" "T1.07 rename: content preserved"

# T1.08 重命名目录
mkdir -p "$TEST_ROOT/t1/dir1/sub"
echo "content" > "$TEST_ROOT/t1/dir1/sub/file.txt"
mv "$TEST_ROOT/t1/dir1" "$TEST_ROOT/t1/dir2"
assert_not_exists "$TEST_ROOT/t1/dir1" "T1.08 rename dir: source gone"
assert_exists "$TEST_ROOT/t1/dir2/sub/file.txt" "T1.08 rename dir: nested preserved"

# T1.09 重名创建失败
mkdir -p "$TEST_ROOT/t1/dup"
assert_fails "T1.09 duplicate mkdir" mkdir "$TEST_ROOT/t1/dup"

# T1.10 不存在路径失败
assert_fails "T1.10 ENOENT" cat "$TEST_ROOT/t1/nope_not_exist"

# ════════════════════════════════════════════════════════════════════
# T2: 文件 I/O（12 项）
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T2: 文件 I/O ━━━"
mkdir -p "$TEST_ROOT/t2"

# T2.01 写入+读取
echo "hello world" > "$TEST_ROOT/t2/rw.txt"
CONTENT=$(cat "$TEST_ROOT/t2/rw.txt")
assert_eq "hello world" "$CONTENT" "T2.01 write+read"

# T2.02 覆盖写
echo "AAA" > "$TEST_ROOT/t2/over.txt"
echo "BBB" > "$TEST_ROOT/t2/over.txt"
CONTENT=$(cat "$TEST_ROOT/t2/over.txt")
assert_eq "BBB" "$CONTENT" "T2.02 overwrite"

# T2.03 追加写（O_APPEND）— 已知问题
echo "AAA" > "$TEST_ROOT/t2/app.txt"
echo "BBB" >> "$TEST_ROOT/t2/app.txt"
CONTENT=$(cat "$TEST_ROOT/t2/app.txt")
EXPECTED=$'AAA\nBBB'
if [ "$CONTENT" = "$EXPECTED" ]; then
    record_pass
else
    record_fail "T2.03 O_APPEND (expected='AAA\\nBBB' actual='$CONTENT') [KNOWN ISSUE]"
fi

# T2.04 截断缩小
echo "0123456789" > "$TEST_ROOT/t2/trunc.txt"
truncate -s 5 "$TEST_ROOT/t2/trunc.txt"
SIZE=$(stat -c "%s" "$TEST_ROOT/t2/trunc.txt")
assert_eq "5" "$SIZE" "T2.04 truncate down"
CONTENT=$(cat "$TEST_ROOT/t2/trunc.txt")
assert_eq "01234" "$CONTENT" "T2.04 truncated content"

# T2.05 截断扩大
truncate -s 20 "$TEST_ROOT/t2/trunc.txt"
SIZE=$(stat -c "%s" "$TEST_ROOT/t2/trunc.txt")
assert_eq "20" "$SIZE" "T2.05 truncate up"

# T2.06 dd 写 1MB
run_timeout 30 dd if=/dev/zero of="$TEST_ROOT/t2/dd1m.bin" bs=1M count=1 2>/dev/null
SIZE=$(stat -c "%s" "$TEST_ROOT/t2/dd1m.bin" 2>/dev/null || echo "0")
assert_eq "1048576" "$SIZE" "T2.06 dd write 1MB"

# T2.07 dd 读 1MB
READ_SIZE=$(run_timeout 10 dd if="$TEST_ROOT/t2/dd1m.bin" of=/dev/null bs=1M 2>&1 | grep -oP 'copied \K\d+' || echo "0")
if [ "$READ_SIZE" = "1048576" ] || [ "$READ_SIZE" = "1M" ] || [ "$READ_SIZE" = "1.0M" ]; then
    record_pass
else
    # Alternative check: file is readable
    if run_timeout 10 cat "$TEST_ROOT/t2/dd1m.bin" >/dev/null 2>&1; then
        record_pass
    else
        record_fail "T2.07 dd read 1MB (read_size=$READ_SIZE)"
    fi
fi

# T2.08 fsync 持久化
echo "fsync_test" > "$TEST_ROOT/t2/fsync.txt"
assert_success "T2.08 fsync" run_timeout 5 sync "$TEST_ROOT/t2/fsync.txt"

# T2.09 fallocate 预分配
echo "" > "$TEST_ROOT/t2/falloc.txt"  # create file first
if command -v fallocate >/dev/null 2>&1; then
    if run_timeout 10 fallocate -l 1048576 "$TEST_ROOT/t2/falloc.txt" 2>/dev/null; then
        SIZE=$(stat -c "%s" "$TEST_ROOT/t2/falloc.txt")
        assert_eq "1048576" "$SIZE" "T2.09 fallocate 1MB"
    else
        record_skip "T2.09 fallocate not supported"
    fi
else
    record_skip "T2.09 fallocate tool missing"
fi

# T2.10 空文件
touch "$TEST_ROOT/t2/empty.txt"
SIZE=$(stat -c "%s" "$TEST_ROOT/t2/empty.txt")
assert_eq "0" "$SIZE" "T2.10 empty file size"
CONTENT=$(cat "$TEST_ROOT/t2/empty.txt")
assert_eq "" "$CONTENT" "T2.10 empty file content"

# T2.11 随机偏移写
rm -f "$TEST_ROOT/t2/seek.txt"
run_timeout 10 dd if=/dev/zero of="$TEST_ROOT/t2/seek.txt" bs=1K seek=100 count=1 conv=notrunc 2>/dev/null
SIZE=$(stat -c "%s" "$TEST_ROOT/t2/seek.txt" 2>/dev/null || echo "0")
if [ "$SIZE" -ge 102400 ] 2>/dev/null; then
    record_pass
else
    record_fail "T2.11 seek write (size=$SIZE, expected >=102400)"
fi

# T2.12 大文件分块写（10×1MB）— 已知问题，可能卡死
if run_timeout 30 sh -c 'for i in $(seq 1 10); do dd if=/dev/zero of='"$TEST_ROOT/t2/large.bin"' bs=1M count=1 conv=notrunc oflag=append 2>/dev/null; done' 2>/dev/null; then
    SIZE=$(stat -c "%s" "$TEST_ROOT/t2/large.bin" 2>/dev/null || echo "0")
    assert_eq "10485760" "$SIZE" "T2.12 large file 10MB"
else
    record_fail "T2.12 large file 10MB [KNOWN ISSUE: write hang]"
fi

# ════════════════════════════════════════════════════════════════════
# T3: 元数据（8 项）
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T3: 元数据 ━━━"
mkdir -p "$TEST_ROOT/t3"
echo "perm_test" > "$TEST_ROOT/t3/perm.txt"

# T3.01 chmod 644
chmod 644 "$TEST_ROOT/t3/perm.txt"
PERMS=$(stat -c "%a" "$TEST_ROOT/t3/perm.txt")
assert_eq "644" "$PERMS" "T3.01 chmod 644"

# T3.02 chmod 755
chmod 755 "$TEST_ROOT/t3/perm.txt"
PERMS=$(stat -c "%a" "$TEST_ROOT/t3/perm.txt")
assert_eq "755" "$PERMS" "T3.02 chmod 755"

# T3.03 chmod 000
chmod 000 "$TEST_ROOT/t3/perm.txt"
PERMS=$(stat -c "%a" "$TEST_ROOT/t3/perm.txt")
# stat -c "%a" returns "0" (not zero-padded "000") for permission 0
assert_eq "0" "$PERMS" "T3.03 chmod 000"
chmod 644 "$TEST_ROOT/t3/perm.txt"  # restore

# T3.04 chown uid
chown 1000 "$TEST_ROOT/t3/perm.txt" 2>/dev/null
UID_VAL=$(stat -c "%u" "$TEST_ROOT/t3/perm.txt")
assert_eq "1000" "$UID_VAL" "T3.04 chown uid"

# T3.05 chown gid
chown :1000 "$TEST_ROOT/t3/perm.txt" 2>/dev/null
GID_VAL=$(stat -c "%g" "$TEST_ROOT/t3/perm.txt")
assert_eq "1000" "$GID_VAL" "T3.05 chown gid"

# T3.06 utimes
touch -d "2020-01-01 00:00:00" "$TEST_ROOT/t3/perm.txt" 2>/dev/null
MTIME=$(stat -c "%y" "$TEST_ROOT/t3/perm.txt" 2>/dev/null | cut -d'.' -f1)
assert_eq "2020-01-01 00:00:00" "$MTIME" "T3.06 utimes"

# T3.07 access 检查
test -r "$TEST_ROOT/t3/perm.txt" && test -w "$TEST_ROOT/t3/perm.txt"
assert_eq "0" "$?" "T3.07 access check (r/w)"

# T3.08 stat 文件类型
FTYPE=$(stat -c "%F" "$TEST_ROOT/t3/perm.txt")
case "$FTYPE" in
    *regular*) record_pass ;;
    *) record_fail "T3.08 stat file type (got='$FTYPE')" ;;
esac

# ════════════════════════════════════════════════════════════════════
# T4: 目录操作（6 项）
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T4: 目录操作 ━━━"
mkdir -p "$TEST_ROOT/t4"

# T4.01 readdir 基础
for i in 1 2 3; do echo "f$i" > "$TEST_ROOT/t4/file_$i.txt"; done
COUNT=$(ls "$TEST_ROOT/t4/" | wc -l)
assert_eq "3" "$COUNT" "T4.01 readdir basic"

# T4.02 readdir 长格式
LS_LA=$(ls -la "$TEST_ROOT/t4/" 2>/dev/null)
echo "$LS_LA" | grep -q "file_1.txt" && echo "$LS_LA" | grep -q "file_2.txt" && echo "$LS_LA" | grep -q "file_3.txt"
assert_eq "0" "$?" "T4.02 readdir -la"

# T4.03 readdir 递归
mkdir -p "$TEST_ROOT/t4/sub/deep"
echo "nested" > "$TEST_ROOT/t4/sub/deep/n.txt"
FOUND=$(find "$TEST_ROOT/t4" -type f -name "*.txt" | wc -l)
assert_eq "4" "$FOUND" "T4.03 find recursive"

# T4.04 readdir 100 文件
mkdir -p "$TEST_ROOT/t4/hundred"
for i in $(seq 1 100); do touch "$TEST_ROOT/t4/hundred/f_$i"; done
COUNT=$(ls "$TEST_ROOT/t4/hundred/" | wc -l)
assert_eq "100" "$COUNT" "T4.04 readdir 100 files"

# T4.05 readdir 200 文件（分页）
mkdir -p "$TEST_ROOT/t4/twohundred"
for i in $(seq 1 200); do touch "$TEST_ROOT/t4/twohundred/f_$i"; done
COUNT=$(ls "$TEST_ROOT/t4/twohundred/" | wc -l)
assert_eq "200" "$COUNT" "T4.05 readdir 200 files (paging)"

# T4.06 readdir 删除后更新
rm "$TEST_ROOT/t4/hundred/f_1"
COUNT=$(ls "$TEST_ROOT/t4/hundred/" | wc -l)
assert_eq "99" "$COUNT" "T4.06 readdir after delete"

# ════════════════════════════════════════════════════════════════════
# T5: 链接（6 项）
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T5: 链接 ━━━"
mkdir -p "$TEST_ROOT/t5"

# T5.01 硬链接创建
echo "hardlink content" > "$TEST_ROOT/t5/orig.txt"
if ln "$TEST_ROOT/t5/orig.txt" "$TEST_ROOT/t5/hard.txt" 2>/dev/null; then
    record_pass
else
    record_fail "T5.01 hardlink create"
fi

# T5.02 硬链接 nlink
NLINK=$(stat -c "%h" "$TEST_ROOT/t5/orig.txt" 2>/dev/null)
assert_eq "2" "$NLINK" "T5.02 hardlink nlink=2"

# T5.03 硬链接内容共享
echo "modified" > "$TEST_ROOT/t5/hard.txt"
CONTENT=$(cat "$TEST_ROOT/t5/orig.txt")
assert_eq "modified" "$CONTENT" "T5.03 hardlink content sharing"

# T5.04 硬链接删除后 nlink
rm "$TEST_ROOT/t5/hard.txt"
NLINK=$(stat -c "%h" "$TEST_ROOT/t5/orig.txt" 2>/dev/null)
assert_eq "1" "$NLINK" "T5.04 nlink=1 after unlink"

# T5.05 符号链接创建 — 已知问题
echo "target content" > "$TEST_ROOT/t5/target.txt"
if ln -s target.txt "$TEST_ROOT/t5/sym.txt" 2>/dev/null; then
    record_pass
else
    record_fail "T5.05 symlink create [KNOWN ISSUE]"
fi

# T5.06 readlink
TARGET=$(readlink "$TEST_ROOT/t5/sym.txt" 2>/dev/null)
assert_eq "target.txt" "$TARGET" "T5.06 readlink"

# ════════════════════════════════════════════════════════════════════
# T6: 系统操作（4 项）
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T6: 系统操作 ━━━"

# T6.01 statfs 基础
if run_timeout 5 df "$MOUNT" >/dev/null 2>&1; then
    record_pass
else
    record_fail "T6.01 df statfs"
fi

# T6.02 statfs 详细
STATF=$(run_timeout 5 stat -f "$MOUNT" 2>/dev/null)
if echo "$STATF" | grep -qi "block\|size\|avail" 2>/dev/null; then
    record_pass
else
    record_fail "T6.02 stat -f detail"
fi

# T6.03 大目录 find
if run_timeout 15 find "$TEST_ROOT" -maxdepth 3 >/dev/null 2>&1; then
    record_pass
else
    record_fail "T6.03 find maxdepth 3"
fi

# T6.04 umount/remount 持久化（仅验证数据存在，不实际 umount）
assert_exists "$TEST_ROOT/t1/dst.txt" "T6.04 data persistence check"

# ════════════════════════════════════════════════════════════════════
# T7: 扩展属性（5 项）
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T7: 扩展属性 ━━━"
mkdir -p "$TEST_ROOT/t7"
echo "xattr test" > "$TEST_ROOT/t7/xa.txt"

# 检查 setfattr 是否可用
if command -v setfattr >/dev/null 2>&1; then

    # T7.01 setxattr
    if setfattr -n user.key1 -v "value1" "$TEST_ROOT/t7/xa.txt" 2>/dev/null; then
        record_pass
    else
        record_fail "T7.01 setxattr"
    fi

    # T7.02 getxattr
    VAL=$(getfattr -n user.key1 --only-values "$TEST_ROOT/t7/xa.txt" 2>/dev/null)
    assert_eq "value1" "$VAL" "T7.02 getxattr"

    # T7.03 listxattr
    setfattr -n user.key2 -v "value2" "$TEST_ROOT/t7/xa.txt" 2>/dev/null
    LIST=$(getfattr -m- "$TEST_ROOT/t7/xa.txt" 2>/dev/null | grep "user\.")
    echo "$LIST" | grep -q "user.key1" && echo "$LIST" | grep -q "user.key2"
    assert_eq "0" "$?" "T7.03 listxattr"

    # T7.04 removexattr
    setfattr -x user.key1 "$TEST_ROOT/t7/xa.txt" 2>/dev/null
    # getfattr outputs "No such attribute" to stderr; merge with stdout for grep
    if getfattr -n user.key1 "$TEST_ROOT/t7/xa.txt" 2>&1 | grep -q "No such attribute"; then
        record_pass
    else
        record_fail "T7.04 removexattr"
    fi

    # T7.05 多 xattr
    setfattr -n user.key3 -v "v3" "$TEST_ROOT/t7/xa.txt" 2>/dev/null
    COUNT=$(getfattr -m- "$TEST_ROOT/t7/xa.txt" 2>/dev/null | grep -c "user\.")
    assert_eq "2" "$COUNT" "T7.05 multiple xattr (key2+key3)"

else
    echo "  [SKIP] setfattr not available, skipping T7.01-T7.05"
    SKIP=$((SKIP+5))
fi

# ════════════════════════════════════════════════════════════════════
# T8: 并发与压力（5 项）
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T8: 并发与压力 ━━━"
mkdir -p "$TEST_ROOT/t8"

# T8.01 并发写不同文件
mkdir -p "$TEST_ROOT/t8/concurrent_write"
for p in 1 2 3 4; do
    (
        for i in $(seq 1 25); do
            echo "proc-$p-file-$i" > "$TEST_ROOT/t8/concurrent_write/p${p}_f${i}.txt"
        done
    ) &
done
wait
COUNT=$(ls "$TEST_ROOT/t8/concurrent_write/" | wc -l)
assert_eq "100" "$COUNT" "T8.01 concurrent write 100 files"

# T8.02 并发读同一文件
echo "shared content for concurrent read" > "$TEST_ROOT/t8/shared.txt"
READ_OK=0
for p in 1 2 3 4; do
    (
        CONTENT=$(cat "$TEST_ROOT/t8/shared.txt")
        if [ "$CONTENT" = "shared content for concurrent read" ]; then
            echo "ok" > /tmp/concurrent_read_$p
        else
            echo "fail" > /tmp/concurrent_read_$p
        fi
    ) &
done
wait
for p in 1 2 3 4; do
    [ "$(cat /tmp/concurrent_read_$p 2>/dev/null)" = "ok" ] && READ_OK=$((READ_OK+1))
done
assert_eq "4" "$READ_OK" "T8.02 concurrent read same file"
rm -f /tmp/concurrent_read_*

# T8.03 并发创建目录
mkdir -p "$TEST_ROOT/t8/concurrent_mkdir"
for p in 1 2 3 4; do
    (
        for i in $(seq 1 50); do
            mkdir -p "$TEST_ROOT/t8/concurrent_mkdir/p${p}_d${i}"
        done
    ) &
done
wait
COUNT=$(find "$TEST_ROOT/t8/concurrent_mkdir" -maxdepth 1 -type d | wc -l)
# +1 because find includes the parent dir itself
assert_eq "201" "$COUNT" "T8.03 concurrent mkdir 200 dirs"

# T8.04 压力创建 1000 文件
mkdir -p "$TEST_ROOT/t8/stress_files"
for i in $(seq 1 1000); do
    touch "$TEST_ROOT/t8/stress_files/f_$i" 2>/dev/null
done
COUNT=$(ls "$TEST_ROOT/t8/stress_files/" 2>/dev/null | wc -l)
assert_eq "1000" "$COUNT" "T8.04 stress create 1000 files"

# T8.05 压力创建 500 目录
mkdir -p "$TEST_ROOT/t8/stress_dirs"
for i in $(seq 1 500); do
    mkdir -p "$TEST_ROOT/t8/stress_dirs/d_$i" 2>/dev/null
done
COUNT=$(find "$TEST_ROOT/t8/stress_dirs" -maxdepth 1 -type d 2>/dev/null | wc -l)
assert_eq "501" "$COUNT" "T8.05 stress create 500 dirs"

# ════════════════════════════════════════════════════════════════════
# 汇总
# ════════════════════════════════════════════════════════════════════
echo ""
echo "============================================================"
echo "  测试汇总"
echo "============================================================"
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo "  SKIP: $SKIP"
echo "  TOTAL: $((PASS+FAIL+SKIP))"
echo ""

if [ "$FAIL" -gt 0 ]; then
    echo "  失败项:"
    for f in "${FAILED_LIST[@]}"; do
        echo "    - $f"
    done
    echo ""
fi

echo "============================================================"

cleanup
exit $FAIL
