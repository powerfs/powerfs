#!/bin/bash

MASTER_ADDR="127.0.0.1:36697"
CLI_BIN="./target/release/powerfs-cli"
TEST_DIR1="/tmp/powerfs-posix-test/conflict-test"
TEST_DIR2="/tmp/powerfs-posix-test2/conflict-test"
MOUNT_POINT1="/tmp/powerfs-posix-test"
MOUNT_POINT2="/tmp/powerfs-posix-test2"

PASSED=0
FAILED=0

check_environment() {
    local check_name="$1"
    local timeout=5
    
    echo "[环境检查] $check_name ..."
    
    timeout $timeout df -h "$MOUNT_POINT1" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "[环境检查] ❌ df $MOUNT_POINT1 失败或超时"
        echo "[环境检查] ⚠️ 系统可能卡住，需要排查问题"
        return 1
    fi
    
    if [ -d "$MOUNT_POINT2" ]; then
        timeout $timeout df -h "$MOUNT_POINT2" > /dev/null 2>&1
        if [ $? -ne 0 ]; then
            echo "[环境检查] ❌ df $MOUNT_POINT2 失败或超时"
            echo "[环境检查] ⚠️ 系统可能卡住，需要排查问题"
            return 1
        fi
    fi
    
    echo "[环境检查] ✅ 系统正常"
    return 0
}

run_test() {
    local desc="$1"
    local condition="$2"
    if eval "$condition"; then
        echo "[测试] $desc ... ✅ 通过"
        ((PASSED++))
    else
        echo "[测试] $desc ... ❌ 失败"
        ((FAILED++))
    fi
}

count_conflicts() {
    $CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test 2>/dev/null | grep "^Total:" | awk '{print $2}' || echo 0
}

cleanup() {
    $CLI_BIN -m $MASTER_ADDR conflicts auto-resolve --path /conflict-test --policy aggressive 2>/dev/null || true
    rm -rf "$TEST_DIR1"/* "$TEST_DIR2"/* 2>/dev/null || true
    sleep 1
}

echo "=== PowerFS 冲突检测测试 ==="
check_environment "测试开始前" || exit 1

mkdir -p "$TEST_DIR1" "$TEST_DIR2"

echo ""
echo "=== 场景1: CreateCreate 冲突（目录）==="
check_environment "场景1开始前" || exit 1
cleanup

mkdir -p "$TEST_DIR1/scene1-dir"
mkdir -p "$TEST_DIR2/scene1-dir"
sleep 3

check_environment "场景1操作后" || exit 1

COUNT=$(count_conflicts)
run_test "应检测到 CreateCreate 冲突" "[ $COUNT -ge 1 ]"

echo "冲突详情:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test

echo ""
echo "=== 场景2: CreateCreate 冲突（文件）==="
check_environment "场景2开始前" || exit 1
cleanup

echo "content1" > "$TEST_DIR1/scene2-file.txt"
echo "content2" > "$TEST_DIR2/scene2-file.txt"
sleep 3

check_environment "场景2操作后" || exit 1

COUNT=$(count_conflicts)
run_test "应检测到 CreateCreate 冲突" "[ $COUNT -ge 1 ]"

echo "冲突详情:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test

echo ""
echo "=== 场景3: WriteWrite 冲突 ==="
check_environment "场景3开始前" || exit 1
cleanup

echo "initial" > "$TEST_DIR1/scene3-write.txt"
sleep 3

check_environment "场景3初始写入后" || exit 1

echo "client1 update" > "$TEST_DIR1/scene3-write.txt"
echo "client2 update" > "$TEST_DIR2/scene3-write.txt"
sleep 3

check_environment "场景3冲突写入后" || exit 1

COUNT=$(count_conflicts)
run_test "应检测到 WriteWrite 冲突" "[ $COUNT -ge 1 ]"

echo "冲突详情:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test

echo ""
echo "=== 场景4: 客户端同步验证 ==="
check_environment "场景4开始前" || exit 1
cleanup

echo "sync test" > "$TEST_DIR1/scene4-sync.txt"
sleep 5

check_environment "场景4文件创建后" || exit 1

if [ -f "$TEST_DIR2/scene4-sync.txt" ]; then
    run_test "客户端2应能看到文件" "true"
    CONTENT1=$(cat "$TEST_DIR1/scene4-sync.txt")
    CONTENT2=$(cat "$TEST_DIR2/scene4-sync.txt")
    if [ "$CONTENT1" = "$CONTENT2" ]; then
        run_test "文件内容应一致" "true"
    else
        run_test "文件内容应一致" "false"
    fi
else
    run_test "客户端2应能看到文件" "false"
    run_test "文件内容应一致" "false"
fi

rm -f "$TEST_DIR1/scene4-sync.txt"
sleep 5

check_environment "场景4文件删除后" || exit 1

if [ ! -f "$TEST_DIR2/scene4-sync.txt" ]; then
    run_test "客户端2应同步删除" "true"
else
    run_test "客户端2应同步删除" "false"
fi

echo ""
echo "=== 测试结果汇总 ==="
check_environment "测试结束前" || exit 1

echo "通过: $PASSED"
echo "失败: $FAILED"

if [ $FAILED -gt 0 ]; then
    exit 1
else
    echo "🎉 所有测试通过！"
    exit 0
fi