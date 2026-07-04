#!/bin/bash
set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
MOUNT_DIR="/tmp/powerfs-bench-mount"
TEST_DIR="$MOUNT_DIR/bench_test"
REPORT_FILE="/tmp/powerfs_bench_report.txt"
MASTER_PORT=9333
VOLUME_GRPC_PORT=8081
VOLUME_HTTP_PORT=8080

echo "=== PowerFS FUSE 性能基准测试 ==="
echo "Project root: $PROJECT_ROOT"
echo "Mount dir: $MOUNT_DIR"
echo "Report: $REPORT_FILE"
echo ""

# Pre-cleanup: kill any leftover powerfs processes and unmount stale mountpoints
echo "=== 预清理 ==="
if mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
    fusermount -u "$MOUNT_DIR" 2>/dev/null || umount "$MOUNT_DIR" 2>/dev/null || true
    sleep 0.5
fi
for port in $MASTER_PORT $VOLUME_GRPC_PORT $VOLUME_HTTP_PORT; do
    pid=$(ss -tlnp 2>/dev/null | grep ":$port " | grep -oE 'pid=[0-9]+' | cut -d= -f2 | head -1)
    if [ -n "$pid" ]; then
        echo "Killing leftover process on port $port (PID $pid)"
        kill "$pid" 2>/dev/null || true
    fi
done
sleep 1

cleanup() {
    echo ""
    echo "=== 清理环境 ==="
    if mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
        fusermount -u "$MOUNT_DIR" 2>/dev/null || umount "$MOUNT_DIR" 2>/dev/null || true
        sleep 0.5
    fi
    kill $MASTER_PID 2>/dev/null || true
    kill $VOLUME_PID 2>/dev/null || true
    kill $FUSE_PID 2>/dev/null || true
    sleep 1
    rm -rf "$MASTER_DIR" "$VOLUME_DIR"
}

trap cleanup EXIT

echo "=== 编译项目 ==="
cd "$PROJECT_ROOT"
cargo build --release -p powerfs-server -p powerfs-volume -p powerfs-fuse 2>&1 | tail -3

echo ""
echo "=== 启动服务 ==="
MASTER_DIR=$(mktemp -d)
VOLUME_DIR=$(mktemp -d)

# 使用 powerfs 统一入口启动 Master
./target/release/powerfs master \
    --port $MASTER_PORT \
    --dir "$MASTER_DIR" \
    > /tmp/powerfs-bench-master.log 2>&1 &
MASTER_PID=$!
echo "Master PID: $MASTER_PID (port $MASTER_PORT)"

sleep 3

# 使用 powerfs-volume 启动 Volume Server
./target/release/powerfs-volume \
    --grpc-address "127.0.0.1:$VOLUME_GRPC_PORT" \
    --http-port $VOLUME_HTTP_PORT \
    --node-id "bench-node" \
    --master-address "127.0.0.1:$MASTER_PORT" \
    --data-dir "$VOLUME_DIR" \
    > /tmp/powerfs-bench-volume.log 2>&1 &
VOLUME_PID=$!
echo "Volume PID: $VOLUME_PID (grpc port $VOLUME_GRPC_PORT)"

sleep 3

mkdir -p "$MOUNT_DIR"

# 使用 powerfs-fuse 启动 FUSE 挂载
./target/release/powerfs-fuse \
    --master "127.0.0.1:$MASTER_PORT" \
    --mount-point "$MOUNT_DIR" \
    > /tmp/powerfs-bench-fuse.log 2>&1 &
FUSE_PID=$!
echo "FUSE PID: $FUSE_PID"

sleep 3

if ! mountpoint -q "$MOUNT_DIR"; then
    echo "错误: FUSE 挂载失败"
    echo "--- FUSE 日志 ---"
    cat /tmp/powerfs-bench-fuse.log
    echo "--- Master 日志 ---"
    cat /tmp/powerfs-bench-master.log
    exit 1
fi

mkdir -p "$TEST_DIR"

echo "" > "$REPORT_FILE"
echo "=========================================" >> "$REPORT_FILE"
echo "PowerFS FUSE 性能基准测试报告" >> "$REPORT_FILE"
echo "测试时间: $(date)" >> "$REPORT_FILE"
echo "=========================================" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

run_bench() {
    local name="$1"
    local cmd="$2"
    local desc="$3"
    
    echo ""
    echo "=== $name ==="
    echo "描述: $desc"
    
    local start_time=$(date +%s%N)
    eval "$cmd"
    local end_time=$(date +%s%N)
    local elapsed_ns=$((end_time - start_time))
    local elapsed_ms=$((elapsed_ns / 1000000))
    local elapsed_s=$(echo "scale=3; $elapsed_ns / 1000000000" | bc)
    
    echo "耗时: ${elapsed_s}s (${elapsed_ms}ms)"
    echo "" >> "$REPORT_FILE"
    echo "### $name" >> "$REPORT_FILE"
    echo "- 描述: $desc" >> "$REPORT_FILE"
    echo "- 耗时: ${elapsed_s}s" >> "$REPORT_FILE"
    
    LAST_ELAPSED_S=$elapsed_s
}

echo ""
echo "=== 开始性能测试 ==="

# 1. 小文件写入测试
SMALL_FILE_COUNT=20
SMALL_FILE_SIZE=4096
run_bench "小文件写入 (${SMALL_FILE_COUNT}个 x ${SMALL_FILE_SIZE}B)" \
    "for i in \$(seq 1 $SMALL_FILE_COUNT); do dd if=/dev/zero of=$TEST_DIR/small_\$i.txt bs=$SMALL_FILE_SIZE count=1 conv=fsync 2>/dev/null; done" \
    "创建 $SMALL_FILE_COUNT 个 $SMALL_FILE_SIZE 字节的小文件并 fsync"
SMALL_WRITE_THROUGHPUT=$(echo "scale=2; ($SMALL_FILE_COUNT * $SMALL_FILE_SIZE) / ($LAST_ELAPSED_S * 1024 * 1024)" | bc)
echo "- 吞吐量: ${SMALL_WRITE_THROUGHPUT} MB/s" >> "$REPORT_FILE"
echo "吞吐量: ${SMALL_WRITE_THROUGHPUT} MB/s"

# 2. 小文件读取测试
run_bench "小文件读取 (${SMALL_FILE_COUNT}个 x ${SMALL_FILE_SIZE}B)" \
    "for i in \$(seq 1 $SMALL_FILE_COUNT); do cat $TEST_DIR/small_\$i.txt > /dev/null; done" \
    "顺序读取 $SMALL_FILE_COUNT 个 $SMALL_FILE_SIZE 字节的小文件"
SMALL_READ_THROUGHPUT=$(echo "scale=2; ($SMALL_FILE_COUNT * $SMALL_FILE_SIZE) / ($LAST_ELAPSED_S * 1024 * 1024)" | bc)
echo "- 吞吐量: ${SMALL_READ_THROUGHPUT} MB/s" >> "$REPORT_FILE"
echo "吞吐量: ${SMALL_READ_THROUGHPUT} MB/s"

# 3. 大文件顺序写入测试
LARGE_FILE_SIZE=$((4 * 1024 * 1024))
LARGE_FILE_SIZE_MB=4
run_bench "大文件顺序写入 (${LARGE_FILE_SIZE_MB}MB)" \
    "dd if=/dev/zero of=$TEST_DIR/large_write.bin bs=1M count=$LARGE_FILE_SIZE_MB conv=fsync 2>/dev/null" \
    "顺序写入 $LARGE_FILE_SIZE_MB MB 的大文件"
LARGE_WRITE_THROUGHPUT=$(echo "scale=2; $LARGE_FILE_SIZE_MB / $LAST_ELAPSED_S" | bc)
echo "- 吞吐量: ${LARGE_WRITE_THROUGHPUT} MB/s" >> "$REPORT_FILE"
echo "吞吐量: ${LARGE_WRITE_THROUGHPUT} MB/s"

# 4. 大文件顺序读取测试
run_bench "大文件顺序读取 (${LARGE_FILE_SIZE_MB}MB)" \
    "cat $TEST_DIR/large_write.bin > /dev/null" \
    "顺序读取 $LARGE_FILE_SIZE_MB MB 的大文件"
LARGE_READ_THROUGHPUT=$(echo "scale=2; $LARGE_FILE_SIZE_MB / $LAST_ELAPSED_S" | bc)
echo "- 吞吐量: ${LARGE_READ_THROUGHPUT} MB/s" >> "$REPORT_FILE"
echo "吞吐量: ${LARGE_READ_THROUGHPUT} MB/s"

# 5. 大文件随机读取测试 (4KB blocks)
RAND_READ_COUNT=100
run_bench "随机读取 (${RAND_READ_COUNT}次 x 4KB)" \
    "for i in \$(seq 1 $RAND_READ_COUNT); do offset=\$((RANDOM * 4096 % $LARGE_FILE_SIZE)); dd if=$TEST_DIR/large_write.bin bs=4096 count=1 skip=\$((offset / 4096)) 2>/dev/null > /dev/null; done" \
    "随机读取 $RAND_READ_COUNT 次 4KB 数据块"
RAND_READ_IOPS=$(echo "scale=0; $RAND_READ_COUNT / $LAST_ELAPSED_S" | bc)
echo "- IOPS: $RAND_READ_IOPS" >> "$REPORT_FILE"
echo "IOPS: $RAND_READ_IOPS"

# 6. 目录操作测试
DIR_COUNT=20
run_bench "目录创建 ($DIR_COUNT 个)" \
    "for i in \$(seq 1 $DIR_COUNT); do mkdir $TEST_DIR/dir_\$i; done" \
    "创建 $DIR_COUNT 个子目录"

run_bench "目录列出 ($DIR_COUNT 个)" \
    "for i in \$(seq 1 $DIR_COUNT); do ls $TEST_DIR/dir_\$i > /dev/null; done" \
    "列出 $DIR_COUNT 个目录"

run_bench "目录删除 ($DIR_COUNT 个)" \
    "for i in \$(seq 1 $DIR_COUNT); do rmdir $TEST_DIR/dir_\$i; done" \
    "删除 $DIR_COUNT 个子目录"

# 7. 元数据操作测试
run_bench "文件 stat (${SMALL_FILE_COUNT}次)" \
    "for i in \$(seq 1 $SMALL_FILE_COUNT); do stat $TEST_DIR/small_\$i.txt > /dev/null; done" \
    "获取 $SMALL_FILE_COUNT 个文件的元数据"
STATS_PER_SEC=$(echo "scale=0; $SMALL_FILE_COUNT / $LAST_ELAPSED_S" | bc)
echo "- 每秒 stat 数: $STATS_PER_SEC" >> "$REPORT_FILE"
echo "每秒 stat 数: $STATS_PER_SEC"

# 8. 文件删除测试
run_bench "文件删除 (${SMALL_FILE_COUNT}个)" \
    "for i in \$(seq 1 $SMALL_FILE_COUNT); do rm $TEST_DIR/small_\$i.txt; done" \
    "删除 $SMALL_FILE_COUNT 个小文件"

echo ""
echo "========================================="
echo "测试完成！报告已保存到: $REPORT_FILE"
echo "========================================="
echo ""
cat "$REPORT_FILE"
