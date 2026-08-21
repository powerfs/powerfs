#!/bin/bash
# =============================================================================
# PowerFS L3 性能测试 — fio 基准测试
#
# 在 fuse-1-test 容器内运行：
#   docker cp scripts/tests/fuse/run_l3_perf_test.sh fuse-1-test:/tmp/
#   docker exec fuse-1-test /tmp/run_l3_perf_test.sh
#
# 测试项 (按 fs-test-plan.md):
#   L3.01 顺序写   bs=1M, rw=write,  size=64M
#   L3.02 顺序读   bs=1M, rw=read,   size=64M
#   L3.03 随机写   bs=4K, rw=randwrite, size=64M
#   L3.04 随机读   bs=4K, rw=randread,  size=64M
#   L3.05 混合读写 bs=4K, rw=randrw, rwmix=70
#   L3.06 多线程写 bs=1M, numjobs=4
#   L3.07 多线程读 bs=4K, numjobs=4
#   L3.08 fsync影响 bs=4K, fsync=1
# =============================================================================

set -u
MOUNT="/mnt/fuse"
TEST_DIR="$MOUNT/l3_perf_test"
RESULT_DIR="/tmp/l3_results"
PASS=0
FAIL=0
FAILED_TESTS=()

record_pass() { PASS=$((PASS+1)); }
record_fail() { FAIL=$((FAIL+1)); FAILED_TESTS+=("$1"); }

# 从 fio JSON 输出中提取 BW (KB/s) — sum across all jobs
extract_bw_kbps() {
    echo "$1" | python3 -c "
import sys, json
data = json.load(sys.stdin)
total = 0
for j in data.get('jobs', []):
    if 'write' in j and j['write']['bw'] > 0:
        total += j['write']['bw']
    elif 'read' in j and j['read']['bw'] > 0:
        total += j['read']['bw']
print(total)
" 2>/dev/null
}

extract_iops() {
    echo "$1" | python3 -c "
import sys, json
data = json.load(sys.stdin)
total = 0.0
for j in data.get('jobs', []):
    if 'write' in j and j['write']['iops'] > 0:
        total += j['write']['iops']
    elif 'read' in j and j['read']['iops'] > 0:
        total += j['read']['iops']
print(f'{total:.2f}')
" 2>/dev/null
}

extract_lat_us() {
    echo "$1" | python3 -c "
import sys, json
data = json.load(sys.stdin)
j = data['jobs'][0]
lat = j['write']['lat_ns']['mean'] / 1000 if 'write' in j and j['write']['lat_ns']['mean'] > 0 else j['read']['lat_ns']['mean'] / 1000
print(f'{lat:.2f}')
" 2>/dev/null
}

# 格式化 BW 为人类可读
fmt_bw() {
    local kbps=$1
    if [ "$kbps" -ge 1048576 ]; then
        echo "$(python3 -c "print(f'{$kbps/1048576:.2f}')") GB/s"
    elif [ "$kbps" -ge 1024 ]; then
        echo "$(python3 -c "print(f'{$kbps/1024:.2f}')") MB/s"
    else
        echo "$kbps KB/s"
    fi
}

echo "============================================================"
echo "  PowerFS L3 性能测试 (fio)"
echo "  时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "  挂载点: $MOUNT"
echo "  fio: $(fio --version 2>&1)"
echo "============================================================"
echo ""

# 准备
mkdir -p "$TEST_DIR" "$RESULT_DIR"
rm -f "$TEST_DIR"/* 2>/dev/null

# ── L3.01: 顺序写 bs=1M ──────────────────────────────────────────────
echo "--- L3.01: 顺序写 (bs=1M, rw=write, size=64M) ---"
OUT=$(fio --name=l3_01_seq_write \
    --directory="$TEST_DIR" --filename=seq_write.bin \
    --rw=write --bs=1M --size=64M \
    --ioengine=psync --direct=0 \
    --time_based=0 --group_reporting \
    --output-format=json 2>/dev/null)
BW=$(extract_bw_kbps "$OUT")
LAT=$(extract_lat_us "$OUT")
if [ -n "$BW" ] && [ "$BW" -gt 0 ]; then
    echo "  BW=$(fmt_bw $BW)  lat=${LAT}us"
    record_pass
else
    echo "  FAIL: BW=$BW"
    record_fail "L3.01 顺序写"
fi

# ── L3.02: 顺序读 bs=1M ──────────────────────────────────────────────
echo "--- L3.02: 顺序读 (bs=1M, rw=read, size=64M) ---"
# 先确保有文件可读
dd if=/dev/urandom of="$TEST_DIR/seq_read.bin" bs=1M count=64 2>/dev/null
sync
OUT=$(fio --name=l3_02_seq_read \
    --directory="$TEST_DIR" --filename=seq_read.bin \
    --rw=read --bs=1M --size=64M \
    --ioengine=psync --direct=0 \
    --time_based=0 --group_reporting \
    --output-format=json 2>/dev/null)
BW=$(extract_bw_kbps "$OUT")
LAT=$(extract_lat_us "$OUT")
if [ -n "$BW" ] && [ "$BW" -gt 0 ]; then
    echo "  BW=$(fmt_bw $BW)  lat=${LAT}us"
    record_pass
else
    echo "  FAIL: BW=$BW"
    record_fail "L3.02 顺序读"
fi

# ── L3.03: 随机写 bs=4K ──────────────────────────────────────────────
echo "--- L3.03: 随机写 (bs=4K, rw=randwrite, size=4M) ---"
OUT=$(fio --name=l3_03_rand_write \
    --directory="$TEST_DIR" --filename=rand_write.bin \
    --rw=randwrite --bs=4K --size=4M \
    --ioengine=psync --direct=0 \
    --time_based=0 --group_reporting \
    --output-format=json 2>/dev/null)
IOPS=$(extract_iops "$OUT")
LAT=$(extract_lat_us "$OUT")
if [ -n "$IOPS" ] && [ "$(python3 -c "print($IOPS > 0)")" = "True" ]; then
    echo "  IOPS=$IOPS  lat=${LAT}us"
    record_pass
else
    echo "  FAIL: IOPS=$IOPS"
    record_fail "L3.03 随机写"
fi

# ── L3.04: 随机读 bs=4K ──────────────────────────────────────────────
echo "--- L3.04: 随机读 (bs=4K, rw=randread, size=4M) ---"
# 先准备文件
dd if=/dev/urandom of="$TEST_DIR/rand_read.bin" bs=1M count=4 2>/dev/null
sync
OUT=$(fio --name=l3_04_rand_read \
    --directory="$TEST_DIR" --filename=rand_read.bin \
    --rw=randread --bs=4K --size=4M \
    --ioengine=psync --direct=0 \
    --time_based=0 --group_reporting \
    --output-format=json 2>/dev/null)
IOPS=$(extract_iops "$OUT")
LAT=$(extract_lat_us "$OUT")
if [ -n "$IOPS" ] && [ "$(python3 -c "print($IOPS > 0)")" = "True" ]; then
    echo "  IOPS=$IOPS  lat=${LAT}us"
    record_pass
else
    echo "  FAIL: IOPS=$IOPS"
    record_fail "L3.04 随机读"
fi

# ── L3.05: 混合读写 bs=4K, rwmix=70 ──────────────────────────────────
echo "--- L3.05: 混合读写 (bs=4K, rw=randrw, rwmix=70) ---"
OUT=$(fio --name=l3_05_mixed \
    --directory="$TEST_DIR" --filename=mixed.bin \
    --rw=randrw --rwmixread=70 --bs=4K --size=4M \
    --ioengine=psync --direct=0 \
    --time_based=0 --group_reporting \
    --output-format=json 2>/dev/null)
# 混合读写: 提取总 IOPS
READ_IOPS=$(echo "$OUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['jobs'][0]['read']['iops']:.2f}\")" 2>/dev/null)
WRITE_IOPS=$(echo "$OUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['jobs'][0]['write']['iops']:.2f}\")" 2>/dev/null)
if [ -n "$READ_IOPS" ] && [ -n "$WRITE_IOPS" ]; then
    echo "  read_IOPS=$READ_IOPS  write_IOPS=$WRITE_IOPS"
    record_pass
else
    echo "  FAIL: read=$READ_IOPS write=$WRITE_IOPS"
    record_fail "L3.05 混合读写"
fi

# ── L3.06: 多线程写 bs=1M, numjobs=4 ─────────────────────────────────
echo "--- L3.06: 多线程写 (bs=1M, numjobs=4) ---"
OUT=$(fio --name=l3_06_mt_write \
    --directory="$TEST_DIR" --filename=mt_write.bin \
    --rw=write --bs=1M --size=16M --numjobs=4 \
    --ioengine=psync --direct=0 \
    --time_based=0 --group_reporting \
    --output-format=json 2>/dev/null)
BW=$(extract_bw_kbps "$OUT")
if [ -n "$BW" ] && [ "$BW" -gt 0 ]; then
    echo "  BW=$(fmt_bw $BW) (4 jobs)"
    record_pass
else
    echo "  FAIL: BW=$BW"
    record_fail "L3.06 多线程写"
fi

# ── L3.07: 多线程读 bs=4K, numjobs=4 ─────────────────────────────────
echo "--- L3.07: 多线程读 (bs=4K, numjobs=4) ---"
# 准备文件
dd if=/dev/urandom of="$TEST_DIR/mt_read.bin" bs=1M count=4 2>/dev/null
sync
OUT=$(fio --name=l3_07_mt_read \
    --directory="$TEST_DIR" --filename=mt_read.bin \
    --rw=randread --bs=4K --size=4M --numjobs=4 \
    --ioengine=psync --direct=0 \
    --time_based=0 --group_reporting \
    --output-format=json 2>/dev/null)
IOPS=$(extract_iops "$OUT")
if [ -n "$IOPS" ] && [ "$(python3 -c "print($IOPS > 0)")" = "True" ]; then
    echo "  IOPS=$IOPS (4 jobs)"
    record_pass
else
    echo "  FAIL: IOPS=$IOPS"
    record_fail "L3.07 多线程读"
fi

# ── L3.08: fsync 影响 bs=4K, fsync=1 ─────────────────────────────────
echo "--- L3.08: fsync 影响 (bs=4K, fsync=1) ---"
OUT=$(fio --name=l3_08_fsync \
    --directory="$TEST_DIR" --filename=fsync.bin \
    --rw=write --bs=4K --size=16M --fsync=1 \
    --ioengine=psync --direct=0 \
    --time_based=0 --group_reporting \
    --output-format=json 2>/dev/null)
IOPS=$(extract_iops "$OUT")
LAT=$(extract_lat_us "$OUT")
if [ -n "$IOPS" ] && [ "$(python3 -c "print($IOPS > 0)")" = "True" ]; then
    echo "  IOPS=$IOPS  lat=${LAT}us"
    record_pass
else
    echo "  FAIL: IOPS=$IOPS"
    record_fail "L3.08 fsync影响"
fi

# ── 清理 ──
rm -f "$TEST_DIR"/*.bin 2>/dev/null

# ── 汇总 ──
echo ""
echo "============================================================"
echo "  L3 性能测试汇总"
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
