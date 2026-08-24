#!/bin/bash
# T2.4 rsync 数据完整性测试
# 验证 storage_mode 修复后 rsync 同步的文件内容正确（非空）
set -euo pipefail

MNT="/mnt/powerfs"
PASS=0
FAIL=0
SKIP=0

ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
skip() { echo "  [SKIP] $1"; SKIP=$((SKIP+1)); }

WORKDIR="/tmp/t2_4_work_$$"
SRC_DIR="${WORKDIR}/src"
TESTDIR="${MNT}/t2_4_test_$$"
RSYNC_DST="${TESTDIR}/rsync_dst"

rm -rf "${TESTDIR}" 2>/dev/null || true
rm -rf "${WORKDIR}" 2>/dev/null || true
mkdir -p "${SRC_DIR}" "${TESTDIR}" "${RSYNC_DST}"

echo "=========================================="
echo "T2.4 rsync 数据完整性测试"
echo "挂载点: ${MNT}"
echo "测试目录: ${TESTDIR}"
echo "时间: $(date)"
echo "=========================================="

# 生成源文件（不同大小覆盖 inline 和 flat 模式）
echo "--- 生成源文件 ---"

# 1. 小文件 (inline 模式, < 4KB)
echo "  - 100B inline files..."
for i in $(seq 1 20); do
    head -c 100 /dev/urandom | base64 > "${SRC_DIR}/small_${i}.txt"
done

# 2. 中等文件 (inline 模式边界, 3-8KB)
echo "  - 3-8KB boundary files..."
for i in $(seq 1 10); do
    SIZE=$(( (RANDOM % 6 + 3) * 1024 ))
    head -c ${SIZE} /dev/urandom | base64 > "${SRC_DIR}/mid_${i}.txt"
done

# 3. 大文件 (flat 模式, > 8KB)
echo "  - 50KB flat files..."
for i in $(seq 1 10); do
    head -c 51200 /dev/urandom | base64 > "${SRC_DIR}/large_${i}.txt"
done

# 4. 混合内容文件
echo "  - mixed content files..."
for i in $(seq 1 5); do
    { echo "header line $i"; head -c 10000 /dev/urandom | base64; echo "footer line $i"; } > "${SRC_DIR}/mixed_${i}.txt"
done

# 5. 嵌套目录
echo "  - nested directories..."
mkdir -p "${SRC_DIR}/sub1/sub2"
head -c 5000 /dev/urandom | base64 > "${SRC_DIR}/sub1/sub2/deep.txt"
head -c 20000 /dev/urandom | base64 > "${SRC_DIR}/sub1/file1.txt"
head -c 500 /dev/urandom | base64 > "${SRC_DIR}/sub1/sub2/nested.txt"

SRC_FILE_COUNT=$(find "${SRC_DIR}" -type f | wc -l)
echo "  源文件数: ${SRC_FILE_COUNT}"

# ============================================================
echo ""
echo "--- T2.4.1 rsync -a 同步 ---"
T_START=$(date +%s%N)
if rsync -a "${SRC_DIR}/" "${RSYNC_DST}/" 2>/dev/null; then
    T_END=$(date +%s%N)
    T_MS=$(( (T_END - T_START) / 1000000 ))
    ok "rsync -a 完成 (${T_MS} ms)"
else
    T_END=$(date +%s%N)
    T_MS=$(( (T_END - T_START) / 1000000 ))
    fail "rsync -a 失败 (${T_MS} ms)"
fi

# ============================================================
echo ""
echo "--- T2.4.2 文件数量一致性 ---"
DST_FILE_COUNT=$(find "${RSYNC_DST}" -type f | wc -l)
if [ "${SRC_FILE_COUNT}" -eq "${DST_FILE_COUNT}" ]; then
    ok "文件数一致 (${SRC_FILE_COUNT} = ${DST_FILE_COUNT})"
else
    fail "文件数不一致 (src=${SRC_FILE_COUNT} dst=${DST_FILE_COUNT})"
fi

# ============================================================
echo ""
echo "--- T2.4.3 文件内容完整性 (md5 校验) ---"
CONTENT_FAIL=0
TOTAL_CHECKED=0
EMPTY_FILES=0

while IFS= read -r src_file; do
    rel_path="${src_file#${SRC_DIR}/}"
    dst_file="${RSYNC_DST}/${rel_path}"

    if [ ! -f "${dst_file}" ]; then
        echo "  [FAIL] 缺失文件: ${rel_path}"
        CONTENT_FAIL=$((CONTENT_FAIL+1))
        continue
    fi

    src_md5=$(md5sum "${src_file}" | awk '{print $1}')
    dst_md5=$(md5sum "${dst_file}" | awk '{print $1}')
    src_size=$(stat -c '%s' "${src_file}")
    dst_size=$(stat -c '%s' "${dst_file}")

    TOTAL_CHECKED=$((TOTAL_CHECKED+1))

    if [ "${src_md5}" = "${dst_md5}" ]; then
        :
    else
        echo "  [FAIL] md5 不匹配: ${rel_path}"
        echo "         src: md5=${src_md5} size=${src_size}"
        echo "         dst: md5=${dst_md5} size=${dst_size}"
        CONTENT_FAIL=$((CONTENT_FAIL+1))

        if [ "${dst_size}" -eq 0 ] 2>/dev/null || [ "${dst_size}" = "0" ]; then
            EMPTY_FILES=$((EMPTY_FILES+1))
            echo "         *** 目标文件为空 (0 bytes) — storage_mode 推断 bug 特征 ***"
        fi
    fi
done < <(find "${SRC_DIR}" -type f)

if [ "${CONTENT_FAIL}" -eq 0 ]; then
    ok "全部 ${TOTAL_CHECKED} 个文件 md5 一致 (0 空文件)"
else
    fail "${CONTENT_FAIL}/${TOTAL_CHECKED} 个文件 md5 不匹配 (${EMPTY_FILES} 个空文件)"
fi

# ============================================================
echo ""
echo "--- T2.4.4 rsync --checksum 增量检查 ---"
T_CHECK_START=$(date +%s%N)
RSYNC_OUT=$(rsync -a --checksum --dry-run --itemize-changes "${SRC_DIR}/" "${RSYNC_DST}/" 2>/dev/null || true)
T_CHECK_END=$(date +%s%N)
T_CHECK_MS=$(( (T_CHECK_END - T_CHECK_START) / 1000000 ))

if [ -z "${RSYNC_OUT}" ]; then
    ok "rsync --checksum 无增量 (${T_CHECK_MS} ms)"
else
    fail "rsync --checksum 发现差异 (${T_CHECK_MS} ms):"
    echo "${RSYNC_OUT}" | head -10 | sed 's/^/    /'
fi

# ============================================================
echo ""
echo "--- T2.4.5 跨客户端读取验证 (fuse-2) ---"
# 从 fuse-2 读取部分文件，验证跨客户端数据一致性
CROSS_CLIENT_FAIL=0
CROSS_CLIENT_CHECKED=0

# 选择几个代表性文件进行跨客户端验证
for test_file in small_1.txt mid_1.txt large_1.txt mixed_1.txt sub1/sub2/deep.txt; do
    src_file="${SRC_DIR}/${test_file}"
    dst_file_on_fuse2="/mnt/powerfs${RSYNC_DST#${MNT}}/${test_file}"

    if docker exec fuse-2 test -f "${dst_file_on_fuse2}" 2>/dev/null; then
        src_md5=$(md5sum "${src_file}" | awk '{print $1}')
        dst_md5=$(docker exec fuse-2 md5sum "${dst_file_on_fuse2}" | awk '{print $1}')

        CROSS_CLIENT_CHECKED=$((CROSS_CLIENT_CHECKED+1))

        if [ "${src_md5}" = "${dst_md5}" ]; then
            :
        else
            echo "  [FAIL] 跨客户端 md5 不匹配: ${test_file}"
            echo "         src (fuse-1): ${src_md5}"
            echo "         dst (fuse-2):  ${dst_md5}"
            CROSS_CLIENT_FAIL=$((CROSS_CLIENT_FAIL+1))
        fi
    else
        echo "  [SKIP] 跨客户端文件不存在: ${test_file}"
    fi
done

if [ "${CROSS_CLIENT_FAIL}" -eq 0 ] && [ "${CROSS_CLIENT_CHECKED}" -gt 0 ]; then
    ok "跨客户端验证通过 (${CROSS_CLIENT_CHECKED} 个文件)"
else
    fail "跨客户端验证失败 (${CROSS_CLIENT_FAIL}/${CROSS_CLIENT_CHECKED})"
fi

# ============================================================
echo ""
echo "=========================================="
echo "T2.4 rsync 数据完整性测试结果"
echo "  PASS: ${PASS}"
echo "  FAIL: ${FAIL}"
echo "  SKIP: ${SKIP}"
echo "=========================================="

# 清理
rm -rf "${WORKDIR}" 2>/dev/null || true

if [ "${FAIL}" -eq 0 ]; then
    exit 0
else
    exit 1
fi
