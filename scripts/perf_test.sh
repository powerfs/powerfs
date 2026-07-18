#!/bin/bash
set -e

MOUNT_DIR="/tmp/powerfs-perf-test"
TEST_DATA_DIR="/tmp/powerfs-perf-data"
MASTER_PORT=15000

# 清理旧环境
echo "[*] Cleaning up old environment..."
fusermount3 -u "$MOUNT_DIR" 2>/dev/null || true
pkill -9 -f "powerfs master" 2>/dev/null || true
pkill -9 -f "powerfs-volume" 2>/dev/null || true
pkill -9 -f "powerfs fuse" 2>/dev/null || true
rm -rf "$MOUNT_DIR" "$TEST_DATA_DIR"

# 准备环境
echo "[*] Preparing environment..."
mkdir -p "$MOUNT_DIR"
mkdir -p "$TEST_DATA_DIR/master"
mkdir -p "$TEST_DATA_DIR/volume1"

# 启动服务
echo "[*] Starting PowerFS services..."
./target/release/powerfs master --port $MASTER_PORT --dir "$TEST_DATA_DIR/master" --ip 127.0.0.1 &
MASTER_PID=$!

sleep 1

./target/release/powerfs volume --port 15001 --dir "$TEST_DATA_DIR/volume1" --master 127.0.0.1:$MASTER_PORT &
VOLUME_PID=$!

sleep 1

./target/release/powerfs fuse --dir "$MOUNT_DIR" --master 127.0.0.1:$MASTER_PORT &
FUSE_PID=$!

# 等待 FUSE 挂载
echo "[*] Waiting for FUSE mount..."
sleep 3

if ! mount | grep -q "$MOUNT_DIR"; then
    echo "[!] FUSE mount failed!"
    kill $MASTER_PID $VOLUME_PID $FUSE_PID
    exit 1
fi

echo "[*] FUSE mounted successfully!"

# ============================================
# 测试 1: 单线程基准测试
# ============================================
echo ""
echo "[=== Test 1: Single-threaded Benchmark ===]"
TEST_DIR="$MOUNT_DIR/test1"
mkdir -p "$TEST_DIR"

echo "[*] Running mkdir → create → write → unlink → rmdir..."
START=$(date +%s%N)
for i in {1..1000}; do
    mkdir "$TEST_DIR/dir$i"
    echo "hello" > "$TEST_DIR/dir$i/file.txt"
    rm "$TEST_DIR/dir$i/file.txt"
    rmdir "$TEST_DIR/dir$i"
done
END=$(date +%s%N)
ELAPSED=$((($END - $START) / 1000000))
THROUGHPUT=$(echo "scale=2; 5000 / ($ELAPSED / 1000)" | bc)
echo "[+] Time: ${ELAPSED}ms, Throughput: ${THROUGHPUT} ops/s"

rmdir "$TEST_DIR"

# ============================================
# 测试 2: 多线程并发测试
# ============================================
echo ""
echo "[=== Test 2: Multi-threaded Concurrent Test ===]"
NUM_THREADS=8
TEST_DIR="$MOUNT_DIR/test2"
mkdir -p "$TEST_DIR"

echo "[*] Running ${NUM_THREADS} threads concurrently..."
START=$(date +%s%N)

# 启动多个线程
pids=()
for t in {1..$NUM_THREADS}; do
    (
        for i in {1..100}; do
            mkdir "$TEST_DIR/thread${t}_dir${i}"
            echo "thread${t}" > "$TEST_DIR/thread${t}_dir${i}/file.txt"
            rm "$TEST_DIR/thread${t}_dir${i}/file.txt"
            rmdir "$TEST_DIR/thread${t}_dir${i}"
        done
    ) &
    pids+=($!)
done

# 等待所有线程完成
for pid in "${pids[@]}"; do
    wait $pid
done

END=$(date +%s%N)
ELAPSED=$((($END - $START) / 1000000))
TOTAL_OPS=$(($NUM_THREADS * 100 * 4))
THROUGHPUT=$(echo "scale=2; $TOTAL_OPS / ($ELAPSED / 1000)" | bc)
echo "[+] Time: ${ELAPSED}ms, Throughput: ${THROUGHPUT} ops/s"

rmdir "$TEST_DIR"

# ============================================
# 测试 3: 大目录拷贝测试
# ============================================
echo ""
echo "[=== Test 3: Large Directory Copy ===]"
TEST_DIR="$MOUNT_DIR/test3"
mkdir -p "$TEST_DIR"

echo "[*] Copying /usr/bin to $TEST_DIR..."
START=$(date +%s%N)
cp -prf /usr/bin "$TEST_DIR/" 2>/dev/null || true
END=$(date +%s%N)
ELAPSED=$((($END - $START) / 1000000))
NUM_FILES=$(find "$TEST_DIR/bin" -type f 2>/dev/null | wc -l)
echo "[+] Time: ${ELAPSED}ms, Files copied: ${NUM_FILES}"

rm -rf "$TEST_DIR"

# ============================================
# 测试 4: Rename 测试
# ============================================
echo ""
echo "[=== Test 4: Rename Test ===]"
TEST_DIR="$MOUNT_DIR/test4"
mkdir -p "$TEST_DIR"

echo "[*] Running rename operations..."
START=$(date +%s%N)

# 创建测试文件
for i in {1..500}; do
    echo "file$i" > "$TEST_DIR/file$i.txt"
done

# 同目录 rename
for i in {1..500}; do
    mv "$TEST_DIR/file$i.txt" "$TEST_DIR/renamed$i.txt"
done

# 跨目录 rename
mkdir "$TEST_DIR/subdir"
for i in {1..500}; do
    mv "$TEST_DIR/renamed$i.txt" "$TEST_DIR/subdir/file$i.txt"
done

END=$(date +%s%N)
ELAPSED=$((($END - $START) / 1000000))
THROUGHPUT=$(echo "scale=2; 1500 / ($ELAPSED / 1000)" | bc)
echo "[+] Time: ${ELAPSED}ms, Throughput: ${THROUGHPUT} ops/s"

rm -rf "$TEST_DIR"

# 清理
echo ""
echo "[*] Cleaning up..."
kill $MASTER_PID $VOLUME_PID $FUSE_PID
fusermount3 -u "$MOUNT_DIR" 2>/dev/null || true
rm -rf "$MOUNT_DIR" "$TEST_DATA_DIR"

echo "[*] Performance test completed!"