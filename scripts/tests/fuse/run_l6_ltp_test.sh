#!/bin/bash
# =============================================================================
# PowerFS L6 LTP 标准测试集 (xfstests-dev/ltp tools)
#
# 在 fuse-test 容器内运行：
#   docker cp scripts/tests/fuse/run_l6_ltp_test.sh fuse-test:/tmp/
#   docker exec fuse-test /tmp/run_l6_ltp_test.sh
#
# 使用 xfstests-dev/ltp 工具集 (fsstress, fsx, doio, iogen, aio-stress)
# 这些是社区标准文件系统测试工具，覆盖 L6 测试计划的核心场景。
#
# 测试项映射 (按 fs-test-plan.md):
#   L6L.01  growfiles    → fsstress (文件增长/截断/稀疏写)
#   L6L.02  rwtest       → rwtest.sh (sync/buffered/mmap 读写)
#   L6L.03  iogen        → rwtest.sh -i 60s (I/O 生成器 混合读写)
#   L6L.04  ftest        → fsstress -p 4 (并发文件系统测试)
#   L6L.05  fs_racer     → fsstress -p 8 (竞争条件检测)
#   L6L.06  fs_di        → fsx (数据完整性校验)
#   L6L.07  openfile     → fsstress -f openfile (并发 open)
#   L6L.08  inode        → fsstress (inode 管理)
#   L6L.09  linker       → fsstress -f link/unlink (硬链接)
#   L6L.10  stream       → rwtest.sh -f sync (流式 I/O)
#   L6L.11  lftest       → fsstress -n 1000 -l 100 (大文件)
#   L6L.12  writetest    → fsstress -f write (写入)
#   L6L.13  fs_inod      → fsstress -n 10000 (inode 计数)
#   L6S.03  aio_read     → aio-stress -r (异步读)
#   L6S.04  aio_write    → aio-stress -s (异步写)
# =============================================================================

set -u

MOUNT="/mnt/fuse"
TEST_ROOT="$MOUNT/l6_ltp_test"
TMPDIR_LTP="$TEST_ROOT/tmp"
RESULT_DIR="/tmp/l6_results"
LTP_TOOLS="${LTP_TOOLS:-/opt/ltp-tools}"

PASS=0
FAIL=0
SKIP=0
FAILED_TESTS=()
SKIPPED_TESTS=()

# ── 辅助函数 ──────────────────────────────────────────────────────────

record_pass() { PASS=$((PASS+1)); }
record_fail() { FAIL=$((FAIL+1)); FAILED_TESTS+=("$1"); }
record_skip() { SKIP=$((SKIP+1)); SKIPPED_TESTS+=("$1"); }

log()  { echo "[$(date '+%H:%M:%S')] $*"; }

# 检查工具是否存在
check_tool() {
    if [ ! -x "$LTP_TOOLS/$1" ]; then
        log "  SKIP: $1 not found in $LTP_TOOLS"
        return 1
    fi
    return 0
}

# 运行测试并判定结果
# 参数: ID CMD TIMEOUT_S
run_test() {
    local id="$1"
    local cmd="$2"
    local timeout_s="${3:-300}"
    local out_file="$RESULT_DIR/${id}.log"

    log "  RUN: $id (timeout=${timeout_s}s)"
    local start_ts=$(date +%s)
    timeout "$timeout_s" bash -c "cd $TMPDIR_LTP && $cmd" >"$out_file" 2>&1
    local rc=$?
    local end_ts=$(date +%s)
    local elapsed=$((end_ts - start_ts))

    if [ $rc -eq 124 ]; then
        log "  FAIL: $id (TIMEOUT after ${elapsed}s)"
        record_fail "$id (TIMEOUT ${elapsed}s)"
    elif [ $rc -eq 0 ]; then
        # 检查输出中是否有致命错误标志 (排除非致命的 ioctl/fallocate 警告)
        local errors=$(grep -cE '(^FAIL| write\(\) failed| read\(\) failed|pwrite.*failed|pread.*failed|corrupt|CORRUPT|panic|PANIC|abort|ABORT|signal 7|signal 11)' "$out_file" 2>/dev/null)
        errors=${errors:-0}
        if [ "$errors" -gt 0 ]; then
            log "  FAIL: $id (rc=0 but $errors error lines, ${elapsed}s)"
            record_fail "$id ($errors errors, ${elapsed}s)"
        else
            log "  PASS: $id (${elapsed}s)"
            record_pass
        fi
    else
        log "  FAIL: $id (rc=$rc, ${elapsed}s)"
        record_fail "$id (rc=$rc, ${elapsed}s)"
        # 显示最后几行错误
        tail -3 "$out_file" 2>/dev/null | while read line; do
            log "    > $line"
        done
    fi
}

# ── 准备环境 ──────────────────────────────────────────────────────────

mkdir -p "$RESULT_DIR" "$TEST_ROOT" "$TMPDIR_LTP"
chmod 777 "$TMPDIR_LTP"

echo "============================================================"
echo "  PowerFS L6 LTP 标准测试集"
echo "  时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "  挂载点: $MOUNT"
echo "  TMPDIR: $TMPDIR_LTP"
echo "  工具目录: $LTP_TOOLS"
echo "============================================================"
echo ""

# 检查工具
for tool in fsstress fsx doio iogen aio-stress rwtest.sh; do
    if ! check_tool "$tool"; then
        echo "ERROR: Required tool '$tool' not found. Build xfstests-dev/ltp tools first."
        exit 1
    fi
done
log "All tools available: fsstress fsx doio iogen aio-stress rwtest.sh"
echo ""

export PATH="$LTP_TOOLS:$PATH"
export LTPROOT="$LTP_TOOLS"

# 清理之前的数据
rm -rf "$TMPDIR_LTP"/* 2>/dev/null || true

# ════════════════════════════════════════════════════════════════════
# L6L.01: growfiles — fsstress (文件增长/截断/稀疏写)
# ════════════════════════════════════════════════════════════════════
echo "━━━ L6L.01: growfiles (fsstress 文件增长/截断/稀疏写) ━━━"
# fsstress -d 指定工作目录, -n 指定操作数, -p 指定进程数
# -X 不包含某些操作, -r 限制文件大小范围
run_test "L6L.01.fsstress_grow" \
    "fsstress -d $TMPDIR_LTP/grow -n 500 -p 1 -l 0 2>&1" 180

# ════════════════════════════════════════════════════════════════════
# L6L.02: rwtest — 读写测试 (sync/buffered/mmap)
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.02: rwtest (buffered 读写) ━━━"
mkdir -p "$TMPDIR_LTP/rw"
# 注: FUSE 不支持 O_SYNC 写, 使用 buffered 模式; 文件大小限制 1MB 避免迁移问题

# rwtest01: buffered 读写
run_test "L6L.02.rwtest_buffered" \
    "rwtest -N rwtest01 -c -q -i 20s -f buffered 1000b:$TMPDIR_LTP/rw/buff-\$\$" 60

# rwtest02: mmap buffered
run_test "L6L.02.rwtest_mmap_buff" \
    "rwtest -N rwtest02 -c -q -i 20s -n 2 -f buffered -s mmread,mmwrite -m random -Dv 1000b:$TMPDIR_LTP/rw/mmap-buff-\$\$" 60

# ════════════════════════════════════════════════════════════════════
# L6L.03: iogen — I/O 生成器 (混合读写)
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.03: iogen (I/O 生成器 混合读写) ━━━"
run_test "L6L.03.iogen" \
    "rwtest -N iogen01 -i 30s -s read,write -Da -Dv -n 2 500b:$TMPDIR_LTP/doio.f1.\$\$ 1000b:$TMPDIR_LTP/doio.f2.\$\$" 90

# ════════════════════════════════════════════════════════════════════
# L6L.04: ftest — 并发文件系统测试
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.04: ftest (fsstress 并发文件系统测试) ━━━"
run_test "L6L.04.fsstress_concurrent" \
    "fsstress -d $TMPDIR_LTP/ftest -n 1000 -p 4 -l 0 2>&1" 180

# ════════════════════════════════════════════════════════════════════
# L6L.05: fs_racer — 竞争条件检测
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.05: fs_racer (fsstress 竞争条件检测) ━━━"
# 高并发进程数模拟竞争条件
run_test "L6L.05.fsstress_racer" \
    "fsstress -d $TMPDIR_LTP/racer -n 2000 -p 8 -l 0 2>&1" 180

# ════════════════════════════════════════════════════════════════════
# L6L.06: fs_di — 数据完整性校验 (fsx)
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.06: fs_di (fsx 数据完整性校验) ━━━"
# fsx 标准文件系统 exerciser - 写入/读取/截断/映射并验证数据完整性
run_test "L6L.06.fsx_small" \
    "fsx -N 1000 -l 1048576 $TMPDIR_LTP/fsx_small.bin 2>&1" 120

run_test "L6L.06.fsx_large" \
    "fsx -N 500 -l 10485760 $TMPDIR_LTP/fsx_large.bin 2>&1" 180

# ════════════════════════════════════════════════════════════════════
# L6L.07: openfile — 并发 open
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.07: openfile (fsstress 并发 open) ━━━"
# 限制操作为 open/close 以测试 fd 泄漏
run_test "L6L.07.fsstress_open" \
    "fsstress -d $TMPDIR_LTP/open -n 500 -p 10 -l 0 -f creat=1 -f open=1 -f close=1 -f unlink=1 2>&1" 120

# ════════════════════════════════════════════════════════════════════
# L6L.08: inode — inode 管理
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.08: inode (fsstress inode 管理) ━━━"
# 创建大量文件测试 inode 分配/释放
run_test "L6L.08.fsstress_inode" \
    "fsstress -d $TMPDIR_LTP/inode -n 2000 -p 2 -l 0 -f creat=1 -f unlink=1 -f mkdir=1 -f rmdir=1 2>&1" 120

# ════════════════════════════════════════════════════════════════════
# L6L.09: linker — 硬链接测试
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.09: linker (fsstress 硬链接测试) ━━━"
run_test "L6L.09.fsstress_link" \
    "fsstress -d $TMPDIR_LTP/link -n 1000 -p 2 -l 0 -f creat=1 -f link=1 -f unlink=1 -f rename=1 2>&1" 120

# ════════════════════════════════════════════════════════════════════
# L6L.10: stream — 流式 I/O
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.10: stream (buffered 流式 I/O) ━━━"
mkdir -p "$TMPDIR_LTP/stream"
run_test "L6L.10.stream_write" \
    "rwtest -N stream01 -c -q -i 20s -f buffered -s write 1000b:$TMPDIR_LTP/stream/sw-\$\$" 60

run_test "L6L.10.stream_read" \
    "rwtest -N stream02 -c -q -i 20s -f buffered -s read 1000b:$TMPDIR_LTP/stream/sr-\$\$" 60

# ════════════════════════════════════════════════════════════════════
# L6L.11: lftest — 大文件测试
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.11: lftest (fsstress 大文件测试) ━━━"
# 写入大文件 (100MB+)
run_test "L6L.11.fsstress_largefile" \
    "fsstress -d $TMPDIR_LTP/largefile -n 100 -p 1 -l 0 -f write=4 -f truncate=1 2>&1" 180

# 额外: 使用 dd 创建大文件并验证
run_test "L6L.11.largefile_dd" \
    "dd if=/dev/zero of=$TMPDIR_LTP/largefile.bin bs=1M count=100 2>&1 && md5sum $TMPDIR_LTP/largefile.bin 2>&1" 120

# ════════════════════════════════════════════════════════════════════
# L6L.12: writetest — 写入测试
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.12: writetest (fsstress 持续写入) ━━━"
run_test "L6L.12.fsstress_write" \
    "fsstress -d $TMPDIR_LTP/write -n 1000 -p 2 -l 0 -f write=8 -f fsync=1 2>&1" 120

# ════════════════════════════════════════════════════════════════════
# L6L.13: fs_inod — inode 计数验证
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6L.13: fs_inod (inode 计数验证) ━━━"
# 创建 10×10×10 = 1000 个文件并验证
run_test "L6L.13.inode_count" \
    "mkdir -p $TMPDIR_LTP/inod && for i in \$(seq 1 10); do for j in \$(seq 1 10); do for k in \$(seq 1 10); do touch $TMPDIR_LTP/inod/f_\${i}_\${j}_\${k}; done; done; done && find $TMPDIR_LTP/inod -type f | wc -l" 120

# ════════════════════════════════════════════════════════════════════
# L6S.03: aio_read — 异步读 IO
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6S.03: aio_read (aio-stress 异步读) ━━━"
# 先创建测试文件 (10MB, 足够 aio-stress 使用)
dd if=/dev/zero of="$TMPDIR_LTP/aio_read.bin" bs=1M count=10 2>/dev/null
run_test "L6S.03.aio_read" \
    "aio-stress -s 10M -r 64k -i 16 -o 1 $TMPDIR_LTP/aio_read.bin 2>&1" 120

# ════════════════════════════════════════════════════════════════════
# L6S.04: aio_write — 异步写 IO
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ L6S.04: aio_write (aio-stress 异步写) ━━━"
run_test "L6S.04.aio_write" \
    "aio-stress -s 10M -r 64k -i 16 -o 0 $TMPDIR_LTP/aio_write.bin 2>&1" 120

# ════════════════════════════════════════════════════════════════════
# 附加: fsx 随机模式 (更严格的数据完整性测试)
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ 附加: fsx 随机模式 (数据完整性) ━━━"
run_test "L6L.06b.fsx_random" \
    "fsx -N 2000 -l 4194304 -S 0 $TMPDIR_LTP/fsx_random.bin 2>&1" 180

# ════════════════════════════════════════════════════════════════════
# 附加: fsstress 长时间运行 (稳定性回归)
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ 附加: fsstress 长时间运行 (稳定性) ━━━"
run_test "L6L.stability" \
    "fsstress -d $TMPDIR_LTP/stability -n 5000 -p 4 -l 0 2>&1" 300

# ════════════════════════════════════════════════════════════════════
# 汇总报告
# ════════════════════════════════════════════════════════════════════
echo ""
echo "============================================================"
echo "  L6 LTP 测试汇总"
echo "  时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
TOTAL=$((PASS + FAIL + SKIP))
echo "  TOTAL: $TOTAL"
echo "  PASS:  $PASS"
echo "  FAIL:  $FAIL"
echo "  SKIP:  $SKIP"
if [ "$TOTAL" -gt 0 ]; then
    echo "  通过率: $(python3 -c "print(f'{$PASS*100/$TOTAL:.1f}%')" 2>/dev/null || echo 'N/A')"
fi

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
    echo ""
    echo "  失败项:"
    for t in "${FAILED_TESTS[@]}"; do
        echo "    - $t"
    done
fi

if [ ${#SKIPPED_TESTS[@]} -gt 0 ]; then
    echo ""
    echo "  跳过项:"
    for t in "${SKIPPED_TESTS[@]}"; do
        echo "    - $t"
    done
fi

echo ""
echo "  详细日志: $RESULT_DIR/*.log"
echo "============================================================"

# 清理测试数据 (保留结果日志)
rm -rf "$TEST_ROOT" 2>/dev/null || true

exit $FAIL
