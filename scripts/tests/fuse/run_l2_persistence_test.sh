#!/bin/bash
# =============================================================================
# PowerFS L2 持久化测试 (在宿主机运行, 通过 docker exec 操作容器)
#
# 测试场景: 写数据 → 重启 FUSE → 验证数据/属性一致性
#
# 测试项:
#   P1: 文件内容持久化 (小文件 + 大文件)
#   P2: 文件属性持久化 (mode/uid/gid/mtime)
#   P3: 目录结构持久化 (嵌套目录)
#   P4: 硬链接持久化 (nlink + 内容共享)
#   P5: 符号链接持久化 (target + 内容读取)
#   P6: 空文件持久化
#   P7: 扩展属性持久化
#   P8: 覆盖写后持久化
# =============================================================================

set -u
set +H          # 禁用历史扩展, 避免 ! 被误解释 (测试数据含 ! 字符)
PASS=0
FAIL=0
FAILED_TESTS=()

CONTAINER="fuse-1-test"
TEST_DIR="/mnt/fuse/l2_persist_test"

log()  { echo "[$(date '+%H:%M:%S')] $1"; }
ok()   { echo "  OK: $1"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); FAILED_TESTS+=("$1"); }

assert_eq() {
    local expected="$1" actual="$2" name="$3"
    if [ "$expected" = "$actual" ]; then
        ok "$name"
    else
        fail "$name (expected=[$expected] actual=[$actual])"
    fi
}

# 在容器内执行命令 (禁用历史扩展, 避免 ! 被误解释)
in_container() {
    docker exec "$CONTAINER" bash -c "set +H; $1"
}

echo "============================================================"
echo "  PowerFS L2 持久化测试"
echo "  时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
echo ""

# ── 准备测试数据 ──
log "准备测试数据..."
in_container "rm -rf $TEST_DIR && mkdir -p $TEST_DIR"

# P1: 文件内容持久化
echo "--- P1: 文件内容持久化 ---"
in_container "echo 'Hello, PowerFS Persistence Test!' > $TEST_DIR/small.txt"
SMALL_MD5=$(in_container "md5sum $TEST_DIR/small.txt" | awk '{print $1}')

in_container "dd if=/dev/urandom of=$TEST_DIR/large.bin bs=1M count=1 2>/dev/null"
LARGE_MD5=$(in_container "md5sum $TEST_DIR/large.bin" | awk '{print $1}')

# P2: 文件属性持久化
echo "--- P2: 文件属性持久化 ---"
in_container "echo 'attr test' > $TEST_DIR/attr.txt"
in_container "chmod 0644 $TEST_DIR/attr.txt"
in_container "chown 1000:1000 $TEST_DIR/attr.txt" 2>/dev/null
in_container "touch -d '2020-06-15 12:00:00' $TEST_DIR/attr.txt"
ATTR_MODE=$(in_container "stat -c '%a' $TEST_DIR/attr.txt")
ATTR_UID=$(in_container "stat -c '%u' $TEST_DIR/attr.txt")
ATTR_GID=$(in_container "stat -c '%g' $TEST_DIR/attr.txt")
ATTR_MTIME=$(in_container "stat -c '%Y' $TEST_DIR/attr.txt")

# P3: 目录结构持久化
echo "--- P3: 目录结构持久化 ---"
in_container "mkdir -p $TEST_DIR/dir1/dir2/dir3"
in_container "echo 'deep file' > $TEST_DIR/dir1/dir2/dir3/deep.txt"
DEEP_MD5=$(in_container "md5sum $TEST_DIR/dir1/dir2/dir3/deep.txt" | awk '{print $1}')

# P4: 硬链接持久化
echo "--- P4: 硬链接持久化 ---"
in_container "echo 'hardlink content' > $TEST_DIR/orig.txt"
in_container "ln $TEST_DIR/orig.txt $TEST_DIR/hard.txt"
HL_NLINK=$(in_container "stat -c '%h' $TEST_DIR/orig.txt")
HL_MD5=$(in_container "md5sum $TEST_DIR/orig.txt" | awk '{print $1}')

# P5: 符号链接持久化
echo "--- P5: 符号链接持久化 ---"
in_container "echo 'symlink target' > $TEST_DIR/target.txt"
in_container "ln -s target.txt $TEST_DIR/sym.txt"
SYM_TARGET=$(in_container "readlink $TEST_DIR/sym.txt")

# P6: 空文件持久化
echo "--- P6: 空文件持久化 ---"
in_container "touch $TEST_DIR/empty.txt"
EMPTY_SIZE=$(in_container "stat -c '%s' $TEST_DIR/empty.txt")

# P7: 扩展属性持久化
echo "--- P7: 扩展属性持久化 ---"
in_container "echo 'xattr test' > $TEST_DIR/xattr.txt"
in_container "setfattr -n user.test_key -v 'test_value' $TEST_DIR/xattr.txt" 2>/dev/null
XATTR_VAL=$(in_container "getfattr -n user.test_key --only-values $TEST_DIR/xattr.txt" 2>/dev/null)

# P8: 覆盖写后持久化
echo "--- P8: 覆盖写后持久化 ---"
in_container "echo 'first content' > $TEST_DIR/overwrite.txt"
in_container "echo 'second content' > $TEST_DIR/overwrite.txt"
in_container "echo 'final content' > $TEST_DIR/overwrite.txt"
OW_MD5=$(in_container "md5sum $TEST_DIR/overwrite.txt" | awk '{print $1}')

echo ""
log "测试数据准备完成"
echo "  small.txt:     $SMALL_MD5"
echo "  large.bin:     $LARGE_MD5"
echo "  attr.txt:      mode=$ATTR_MODE uid=$ATTR_UID gid=$ATTR_GID mtime=$ATTR_MTIME"
echo "  deep.txt:      $DEEP_MD5"
echo "  orig.txt:      nlink=$HL_NLINK md5=$HL_MD5"
echo "  sym.txt:       target=$SYM_TARGET"
echo "  empty.txt:     size=$EMPTY_SIZE"
echo "  xattr.txt:     user.test_key=$XATTR_VAL"
echo "  overwrite.txt: $OW_MD5"
echo ""

# ── 健全性检查: 确保数据准备成功 (md5 不能为空, 否则后续比对是假阳性) ──
SANITY_FAIL=0
[ -z "$SMALL_MD5" ] && { fail "P0.00 sanity: small.txt md5 empty (data prep failed)"; SANITY_FAIL=1; }
[ -z "$LARGE_MD5" ] && { fail "P0.00 sanity: large.bin md5 empty (data prep failed)"; SANITY_FAIL=1; }
[ -z "$DEEP_MD5" ]  && { fail "P0.00 sanity: deep.txt md5 empty (data prep failed)"; SANITY_FAIL=1; }
[ -z "$HL_MD5" ]    && { fail "P0.00 sanity: orig.txt md5 empty (data prep failed)"; SANITY_FAIL=1; }
[ -z "$OW_MD5" ]    && { fail "P0.00 sanity: overwrite.txt md5 empty (data prep failed)"; SANITY_FAIL=1; }
if [ $SANITY_FAIL -gt 0 ]; then
    echo "============================================================"
    echo "  错误: 测试数据准备失败, 终止测试"
    echo "============================================================"
    exit 1
fi

# ── 重启 FUSE ──
log "重启 FUSE 客户端..."
docker restart "$CONTAINER" > /dev/null 2>&1

log "等待 FUSE 重新挂载..."
for i in $(seq 1 30); do
    if docker exec "$CONTAINER" mount 2>/dev/null | grep -q "/mnt/fuse"; then
        log "FUSE 已重新挂载"
        break
    fi
    sleep 1
done

# 额外等待 2s 让客户端初始化完成
sleep 2

echo ""
echo "============================================================"
echo "  验证持久化结果"
echo "============================================================"
echo ""

# P1: 验证文件内容
echo "--- P1: 验证文件内容 ---"
SMALL_MD5_AFTER=$(in_container "md5sum $TEST_DIR/small.txt 2>/dev/null" | awk '{print $1}')
assert_eq "$SMALL_MD5" "$SMALL_MD5_AFTER" "P1.01 small.txt content persistent"

LARGE_MD5_AFTER=$(in_container "md5sum $TEST_DIR/large.bin 2>/dev/null" | awk '{print $1}')
assert_eq "$LARGE_MD5" "$LARGE_MD5_AFTER" "P1.02 large.bin content persistent (1MB)"

# P2: 验证文件属性
echo "--- P2: 验证文件属性 ---"
ATTR_MODE_AFTER=$(in_container "stat -c '%a' $TEST_DIR/attr.txt 2>/dev/null")
assert_eq "$ATTR_MODE" "$ATTR_MODE_AFTER" "P2.01 mode persistent"

ATTR_UID_AFTER=$(in_container "stat -c '%u' $TEST_DIR/attr.txt 2>/dev/null")
assert_eq "$ATTR_UID" "$ATTR_UID_AFTER" "P2.02 uid persistent"

ATTR_GID_AFTER=$(in_container "stat -c '%g' $TEST_DIR/attr.txt 2>/dev/null")
assert_eq "$ATTR_GID" "$ATTR_GID_AFTER" "P2.03 gid persistent"

ATTR_MTIME_AFTER=$(in_container "stat -c '%Y' $TEST_DIR/attr.txt 2>/dev/null")
assert_eq "$ATTR_MTIME" "$ATTR_MTIME_AFTER" "P2.04 mtime persistent"

# P3: 验证目录结构
echo "--- P3: 验证目录结构 ---"
DEEP_MD5_AFTER=$(in_container "md5sum $TEST_DIR/dir1/dir2/dir3/deep.txt 2>/dev/null" | awk '{print $1}')
assert_eq "$DEEP_MD5" "$DEEP_MD5_AFTER" "P3.01 deep nested file content persistent"

DIR_EXISTS=$(in_container "test -d $TEST_DIR/dir1/dir2/dir3 && echo yes || echo no")
assert_eq "yes" "$DIR_EXISTS" "P3.02 deep nested directory exists"

# P4: 验证硬链接
echo "--- P4: 验证硬链接 ---"
HL_NLINK_AFTER=$(in_container "stat -c '%h' $TEST_DIR/orig.txt 2>/dev/null")
assert_eq "$HL_NLINK" "$HL_NLINK_AFTER" "P4.01 hardlink nlink persistent"

HL_MD5_AFTER=$(in_container "md5sum $TEST_DIR/orig.txt 2>/dev/null" | awk '{print $1}')
assert_eq "$HL_MD5" "$HL_MD5_AFTER" "P4.02 hardlink content persistent"

HARD_EXISTS=$(in_container "test -f $TEST_DIR/hard.txt && echo yes || echo no")
assert_eq "yes" "$HARD_EXISTS" "P4.03 hardlink file exists"

HARD_MD5=$(in_container "md5sum $TEST_DIR/hard.txt 2>/dev/null" | awk '{print $1}')
assert_eq "$HL_MD5" "$HARD_MD5" "P4.04 hardlink shares content with original"

# P5: 验证符号链接
echo "--- P5: 验证符号链接 ---"
SYM_TARGET_AFTER=$(in_container "readlink $TEST_DIR/sym.txt 2>/dev/null")
assert_eq "$SYM_TARGET" "$SYM_TARGET_AFTER" "P5.01 symlink target persistent"

SYM_CONTENT=$(in_container "cat $TEST_DIR/sym.txt 2>/dev/null")
assert_eq "symlink target" "$SYM_CONTENT" "P5.02 symlink content readable"

# P6: 验证空文件
echo "--- P6: 验证空文件 ---"
EMPTY_SIZE_AFTER=$(in_container "stat -c '%s' $TEST_DIR/empty.txt 2>/dev/null")
assert_eq "$EMPTY_SIZE" "$EMPTY_SIZE_AFTER" "P6.01 empty file size persistent"
assert_eq "0" "$EMPTY_SIZE_AFTER" "P6.02 empty file is actually empty"

# P7: 验证扩展属性
echo "--- P7: 验证扩展属性 ---"
XATTR_VAL_AFTER=$(in_container "getfattr -n user.test_key --only-values $TEST_DIR/xattr.txt 2>/dev/null")
assert_eq "$XATTR_VAL" "$XATTR_VAL_AFTER" "P7.01 xattr persistent"

# P8: 验证覆盖写
echo "--- P8: 验证覆盖写 ---"
OW_MD5_AFTER=$(in_container "md5sum $TEST_DIR/overwrite.txt 2>/dev/null" | awk '{print $1}')
assert_eq "$OW_MD5" "$OW_MD5_AFTER" "P8.01 overwrite content persistent"

OW_CONTENT=$(in_container "cat $TEST_DIR/overwrite.txt 2>/dev/null")
assert_eq "final content" "$OW_CONTENT" "P8.02 overwrite has final content"

# ── 汇总 ──
echo ""
echo "============================================================"
echo "  L2 持久化测试汇总"
echo "============================================================"
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo "  TOTAL: $((PASS+FAIL))"
echo ""
if [ $FAIL -gt 0 ]; then
    echo "  失败项:"
    for t in "${FAILED_TESTS[@]}"; do
        echo "    - $t"
    done
fi
echo "============================================================"
