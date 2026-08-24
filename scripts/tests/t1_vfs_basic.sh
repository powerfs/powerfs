#!/bin/bash
# PowerFS T1 VFS 基础操作测试 (FUSE 客户端)
# 测试: T1.1 文件CRUD / T1.2 目录操作 / T1.3 权限 / T1.4 特殊文件 / T1.5 边界 / T1.6 并发
# 用法: docker exec fuse-1 bash /tests/t1_vfs_basic.sh
# 或:   bash t1_vfs_basic.sh (在容器内直接运行)

set -euo pipefail

MNT="/mnt/powerfs"
PASS=0
FAIL=0
SKIP=0

ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
skip() { echo "  [SKIP] $1"; SKIP=$((SKIP+1)); }

# 清理测试目录
TESTDIR="${MNT}/t1_test_$$"
rm -rf "${TESTDIR}" 2>/dev/null || true
mkdir -p "${TESTDIR}"

echo "=========================================="
echo "PowerFS T1 VFS 基础操作测试"
echo "挂载点: ${MNT}"
echo "测试目录: ${TESTDIR}"
echo "时间: $(date)"
echo "=========================================="

# ============================================================
echo ""
echo "--- T1.1 文件 CRUD: create/write/read/close/stat/truncate ---"

# 1. 创建并写入文件
echo "hello powerfs" > "${TESTDIR}/file1.txt"
if [ "$(cat "${TESTDIR}/file1.txt")" = "hello powerfs" ]; then
    ok "create + write + read 一致"
else
    fail "create + write + read 不一致"
fi

# 2. stat 检查 size 和 mode
ACTUAL_SIZE=$(stat -c %s "${TESTDIR}/file1.txt")  # "hello powerfs\n" = 14 bytes
if [ "${ACTUAL_SIZE}" -eq 14 ]; then
    ok "stat size 正确 (${ACTUAL_SIZE} bytes)"
else
    fail "stat size 错误 (期望 14, 实际 ${ACTUAL_SIZE})"
fi

ACTUAL_MODE=$(stat -c %a "${TESTDIR}/file1.txt")
if [ "${ACTUAL_MODE}" = "644" ]; then
    ok "stat mode 正确 (${ACTUAL_MODE})"
else
    fail "stat mode 错误 (期望 644, 实际 ${ACTUAL_MODE})"
fi

# 3. truncate 扩展
truncate -s 1024 "${TESTDIR}/file1.txt"
NEW_SIZE=$(stat -c %s "${TESTDIR}/file1.txt")
if [ "${NEW_SIZE}" -eq 1024 ]; then
    ok "truncate 扩展到 1024 bytes"
else
    fail "truncate 扩展失败 (期望 1024, 实际 ${NEW_SIZE})"
fi

# 4. truncate 缩小
truncate -s 5 "${TESTDIR}/file1.txt"
SMALL_SIZE=$(stat -c %s "${TESTDIR}/file1.txt")
if [ "${SMALL_SIZE}" -eq 5 ]; then
    ok "truncate 缩小到 5 bytes"
else
    fail "truncate 缩小失败 (期望 5, 实际 ${SMALL_SIZE})"
fi

# 5. append 写入
echo "appended" >> "${TESTDIR}/file1.txt"
APPEND_SIZE=$(stat -c %s "${TESTDIR}/file1.txt")
if [ "${APPEND_SIZE}" -gt 5 ]; then
    ok "append 写入成功 (${APPEND_SIZE} bytes)"
else
    fail "append 写入失败"
fi

# 6. 覆盖写
echo "overwrite" > "${TESTDIR}/file1.txt"
OV_SIZE=$(stat -c %s "${TESTDIR}/file1.txt")
if [ "${OV_SIZE}" -eq 10 ]; then
    ok "覆盖写正确 (10 bytes)"
else
    fail "覆盖写错误 (期望 10, 实际 ${OV_SIZE})"
fi

# 7. 二进制写入 (dd)
dd if=/dev/urandom of="${TESTDIR}/bin1.dat" bs=4K count=1 2>/dev/null
BIN_MD5=$(md5sum "${TESTDIR}/bin1.dat" | awk '{print $1}')
BIN_SIZE=$(stat -c %s "${TESTDIR}/bin1.dat")
if [ "${BIN_SIZE}" -eq 4096 ]; then
    ok "二进制写入 4K (md5=${BIN_MD5:0:8})"
else
    fail "二进制写入失败 (期望 4096, 实际 ${BIN_SIZE})"
fi

# 8. 大文件写入 (1MB)
dd if=/dev/urandom of="${TESTDIR}/big1.dat" bs=1M count=1 2>/dev/null
BIG_MD5=$(md5sum "${TESTDIR}/big1.dat" | awk '{print $1}')
BIG_SIZE=$(stat -c %s "${TESTDIR}/big1.dat")
if [ "${BIG_SIZE}" -eq 1048576 ]; then
    ok "大文件写入 1MB (md5=${BIG_MD5:0:8})"
else
    fail "大文件写入失败 (期望 1048576, 实际 ${BIG_SIZE})"
fi

# 9. 读回校验 (md5 二次读一致)
BIG_MD5_2=$(md5sum "${TESTDIR}/big1.dat" | awk '{print $1}')
if [ "${BIG_MD5}" = "${BIG_MD5_2}" ]; then
    ok "多次读取 MD5 一致"
else
    fail "多次读取 MD5 不一致 (${BIG_MD5:0:8} vs ${BIG_MD5_2:0:8})"
fi

# 10. unlink 删除
rm "${TESTDIR}/file1.txt"
if [ ! -f "${TESTDIR}/file1.txt" ]; then
    ok "unlink 删除成功"
else
    fail "unlink 删除失败"
fi

# ============================================================
echo ""
echo "--- T1.2 目录操作: mkdir/rmdir/readdir/rename/unlink/symlink/hardlink ---"

# 1. mkdir 多级
mkdir -p "${TESTDIR}/dir1/subdir1/subdir2"
if [ -d "${TESTDIR}/dir1/subdir1/subdir2" ]; then
    ok "mkdir -p 多级目录"
else
    fail "mkdir -p 失败"
fi

# 2. readdir
mkdir -p "${TESTDIR}/dir2"
touch "${TESTDIR}/dir2/a.txt" "${TESTDIR}/dir2/b.txt" "${TESTDIR}/dir2/c.txt"
LS_COUNT=$(ls "${TESTDIR}/dir2" | wc -l)
if [ "${LS_COUNT}" -eq 3 ]; then
    ok "readdir 返回 ${LS_COUNT} 个条目"
else
    fail "readdir 条目数错误 (期望 3, 实际 ${LS_COUNT})"
fi

# 3. rename 文件
echo "data" > "${TESTDIR}/rename_src.txt"
if mv "${TESTDIR}/rename_src.txt" "${TESTDIR}/rename_dst.txt" 2>&1; then
    if [ -f "${TESTDIR}/rename_dst.txt" ] && [ ! -f "${TESTDIR}/rename_src.txt" ]; then
        ok "rename 文件"
    else
        fail "rename 文件失败 (mv 返回 0 但文件状态异常)"
    fi
else
    fail "rename 文件失败 (mv 返回非零)"
fi

# 4. rename 目录
mkdir -p "${TESTDIR}/dir3"
if mv "${TESTDIR}/dir3" "${TESTDIR}/dir3_renamed" 2>&1; then
    if [ -d "${TESTDIR}/dir3_renamed" ] && [ ! -d "${TESTDIR}/dir3" ]; then
        ok "rename 目录"
    else
        fail "rename 目录失败 (mv 返回 0 但目录状态异常)"
    fi
else
    fail "rename 目录失败 (mv 返回非零)"
fi

# 5. symlink
echo "target" > "${TESTDIR}/sym_target.txt"
ln -s "${TESTDIR}/sym_target.txt" "${TESTDIR}/sym_link.txt"
if [ -L "${TESTDIR}/sym_link.txt" ]; then
    LINK_TARGET=$(readlink "${TESTDIR}/sym_link.txt")
    if [ "${LINK_TARGET}" = "${TESTDIR}/sym_target.txt" ]; then
        ok "symlink 创建 + readlink 正确"
    else
        fail "symlink readlink 错误: ${LINK_TARGET}"
    fi
else
    fail "symlink 创建失败"
fi

# 6. hardlink
echo "hardlink data" > "${TESTDIR}/hard_src.txt"
if ln "${TESTDIR}/hard_src.txt" "${TESTDIR}/hard_link.txt" 2>&1; then
    SRC_NLINK=$(stat -c %h "${TESTDIR}/hard_src.txt")
    if [ "${SRC_NLINK}" -eq 2 ]; then
        HARD_MD5_SRC=$(md5sum "${TESTDIR}/hard_src.txt" | awk '{print $1}')
        HARD_MD5_LNK=$(md5sum "${TESTDIR}/hard_link.txt" | awk '{print $1}')
        if [ "${HARD_MD5_SRC}" = "${HARD_MD5_LNK}" ]; then
            ok "hardlink nlink=2 + 内容一致"
        else
            fail "hardlink 内容不一致"
        fi
    else
        fail "hardlink nlink 错误 (期望 2, 实际 ${SRC_NLINK})"
    fi
else
    fail "hardlink 创建失败 (ln 返回非零)"
fi

# 7. hardlink 删除源后存活
rm "${TESTDIR}/hard_src.txt"
if [ -f "${TESTDIR}/hard_link.txt" ]; then
    SURVIVE_NLINK=$(stat -c %h "${TESTDIR}/hard_link.txt")
    if [ "${SURVIVE_NLINK}" -eq 1 ]; then
        ok "hardlink 删除源后存活 nlink=1"
    else
        fail "hardlink 删除后 nlink 错误 (期望 1, 实际 ${SURVIVE_NLINK})"
    fi
else
    fail "hardlink 删除源后丢失"
fi

# 8. rmdir 空目录
rmdir "${TESTDIR}/dir1/subdir1/subdir2"
if [ ! -d "${TESTDIR}/dir1/subdir1/subdir2" ]; then
    ok "rmdir 空目录"
else
    fail "rmdir 失败"
fi

# 9. rmdir 非空目录应失败
mkdir -p "${TESTDIR}/nonempty"
touch "${TESTDIR}/nonempty/file.txt"
if rmdir "${TESTDIR}/nonempty" 2>/dev/null; then
    fail "rmdir 非空目录应失败但成功了"
else
    ok "rmdir 非空目录正确拒绝"
fi

# ============================================================
echo ""
echo "--- T1.3 权限测试: chmod/chown/utimes ---"

# 1. chmod
touch "${TESTDIR}/perm.txt"
chmod 600 "${TESTDIR}/perm.txt" 2>&1 || true
if [ "$(stat -c %a "${TESTDIR}/perm.txt")" = "600" ]; then
    ok "chmod 600"
else
    fail "chmod 600 失败 (实际 $(stat -c %a "${TESTDIR}/perm.txt"))"
fi

chmod 755 "${TESTDIR}/perm.txt"
if [ "$(stat -c %a "${TESTDIR}/perm.txt")" = "755" ]; then
    ok "chmod 755"
else
    fail "chmod 755 失败"
fi

# 2. chown (root → root, 换 uid)
chown 1:1 "${TESTDIR}/perm.txt" 2>/dev/null || true
ACTUAL_UID=$(stat -c %u "${TESTDIR}/perm.txt")
if [ "${ACTUAL_UID}" = "1" ]; then
    ok "chown uid=1"
else
    # FUSE 可能不支持 chown，标记为 SKIP
    skip "chown (FUSE 限制, uid=${ACTUAL_UID})"
fi

# 3. utimes (修改时间)
touch -a -m -d "2026-01-01 12:00:00" "${TESTDIR}/perm.txt"
ACTUAL_MTIME=$(stat -c %Y "${TESTDIR}/perm.txt")
EXPECTED_MTIME=$(date -d "2026-01-01 12:00:00" +%s)
if [ "${ACTUAL_MTIME}" = "${EXPECTED_MTIME}" ]; then
    ok "utimes mtime 正确"
else
    fail "utimes mtime 错误 (期望 ${EXPECTED_MTIME}, 实际 ${ACTUAL_MTIME})"
fi

# ============================================================
echo ""
echo "--- T1.4 特殊文件: mknod (fifo/sock) ---"

# 1. mkfifo
mkfifo "${TESTDIR}/test.fifo" 2>/dev/null
if [ -p "${TESTDIR}/test.fifo" ]; then
    ok "mkfifo 创建成功"
else
    fail "mkfifo 创建失败"
fi

# 2. unix socket
python3 -c "import socket; s=socket.socket(socket.AF_UNIX); s.bind('${TESTDIR}/test.sock')" 2>/dev/null || \
python -c "import socket; s=socket.socket(socket.AF_UNIX); s.bind('${TESTDIR}/test.sock')" 2>/dev/null || true
if [ -S "${TESTDIR}/test.sock" ]; then
    ok "unix socket 创建成功"
else
    skip "unix socket (python 不可用或 FUSE 不支持 mknod socket)"
fi

# ============================================================
echo ""
echo "--- T1.5 边界测试: 空文件/路径名/特殊字符 ---"

# 1. 空文件
touch "${TESTDIR}/empty.txt"
EMPTY_SIZE=$(stat -c %s "${TESTDIR}/empty.txt")
if [ "${EMPTY_SIZE}" -eq 0 ]; then
    ok "空文件 size=0"
else
    fail "空文件 size 错误 (${EMPTY_SIZE})"
fi

# 2. 空文件写入再清空
echo "temp" > "${TESTDIR}/empty.txt"
truncate -s 0 "${TESTDIR}/empty.txt"
ZERO_SIZE=$(stat -c %s "${TESTDIR}/empty.txt")
if [ "${ZERO_SIZE}" -eq 0 ]; then
    ok "truncate 清空 size=0"
else
    fail "truncate 清空失败 (${ZERO_SIZE})"
fi

# 3. 特殊字符文件名
touch "${TESTDIR}/file with spaces.txt" 2>/dev/null && ok "空格文件名" || fail "空格文件名失败"
touch "${TESTDIR}/中文文件.txt" 2>/dev/null && ok "中文文件名" || fail "中文文件名失败"
touch "${TESTDIR}/file(1).txt" 2>/dev/null && ok "括号文件名" || fail "括号文件名失败"

# 4. 255 字节文件名 (POSIX 上限)
LONG_NAME=$(python3 -c "print('a'*255)" 2>/dev/null || printf 'a%.0s' {1..255})
touch "${TESTDIR}/${LONG_NAME}" 2>/dev/null && ok "255字节文件名" || skip "255字节文件名 (可能超限)"

# 5. 256 字节文件名应失败
TOO_LONG=$(python3 -c "print('a'*256)" 2>/dev/null || printf 'a%.0s' {1..256})
if touch "${TESTDIR}/${TOO_LONG}" 2>/dev/null; then
    fail "256字节文件名应失败但成功了"
else
    ok "256字节文件名正确拒绝 (ENAMETOOLONG)"
fi

# ============================================================
echo ""
echo "--- T1.6 并发读写: 多进程同文件/不同文件 ---"

# 1. 并发写不同文件
for i in $(seq 1 8); do
    dd if=/dev/urandom of="${TESTDIR}/conc_${i}.dat" bs=64K count=1 2>/dev/null &
done
wait
CONC_OK=0
for i in $(seq 1 8); do
    SIZE=$(stat -c %s "${TESTDIR}/conc_${i}.dat" 2>/dev/null || echo 0)
    if [ "${SIZE}" -eq 65536 ]; then
        CONC_OK=$((CONC_OK+1))
    fi
done
if [ "${CONC_OK}" -eq 8 ]; then
    ok "8 进程并发写不同文件全部成功"
else
    fail "并发写不同文件 ${CONC_OK}/8 成功"
fi

# 2. 并发写同文件 (各写不同 offset)
for i in $(seq 0 7); do
    OFFSET=$((i * 4096))
    dd if=/dev/zero bs=4K count=1 seek="${i}" of="${TESTDIR}/samefile.dat" conv=notrunc 2>/dev/null &
done
wait
SAME_SIZE=$(stat -c %s "${TESTDIR}/samefile.dat")
if [ "${SAME_SIZE}" -ge 32768 ]; then
    # 验证每个 4K block 是否全零
    ALL_ZERO=true
    # 4096 bytes of zeros → md5 = 620f0b67a91f7f74151bc5be745b7110
    ZERO_BLOCK_MD5="620f0b67a91f7f74151bc5be745b7110"
    for i in $(seq 0 7); do
        BLOCK=$(dd if="${TESTDIR}/samefile.dat" bs=4K count=1 skip="${i}" 2>/dev/null | md5sum | awk '{print $1}')
        if [ "${BLOCK}" != "${ZERO_BLOCK_MD5}" ]; then
            ALL_ZERO=false
            break
        fi
    done
    if [ "${ALL_ZERO}" = "true" ]; then
        ok "8 进程并发写同文件不同 offset (size=${SAME_SIZE})"
    else
        fail "并发写同文件数据不一致"
    fi
else
    fail "并发写同文件 size 不足 (${SAME_SIZE})"
fi

# 3. 并发 mkdir
for i in $(seq 1 16); do
    mkdir -p "${TESTDIR}/conc_dir_${i}" &
done
wait
DIR_OK=0
for i in $(seq 1 16); do
    [ -d "${TESTDIR}/conc_dir_${i}" ] && DIR_OK=$((DIR_OK+1))
done
if [ "${DIR_OK}" -eq 16 ]; then
    ok "16 进程并发 mkdir 全部成功"
else
    fail "并发 mkdir ${DIR_OK}/16 成功"
fi

# ============================================================
# 清理
echo ""
echo "--- 清理测试目录 ---"
rm -rf "${TESTDIR}"

# ============================================================
echo ""
echo "=========================================="
echo "T1 测试结果汇总"
echo "  PASS: ${PASS}"
echo "  FAIL: ${FAIL}"
echo "  SKIP: ${SKIP}"
echo "=========================================="

if [ "${FAIL}" -gt 0 ]; then
    echo "❌ T1 测试未通过 (${FAIL} 个失败)"
    exit 1
else
    echo "✅ T1 测试全部通过 (PASS=${PASS}, SKIP=${SKIP})"
    exit 0
fi
