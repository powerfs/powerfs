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

cleanup_conflicts() {
    sleep 2
}

count_conflicts() {
    $CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test 2>/dev/null | grep "^Total:" | awk '{print $2}' || echo 0
}

get_conflict_type() {
    local name="$1"
    local types=$($CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test 2>/dev/null | \
    awk -v name="$name" '
        /^\[/ { 
            if (saved_type != "" && saved_name == name) {
                print saved_type
            }
            saved_type = ""
            saved_name = ""
            in_conflict = 1 
        }
        in_conflict && /^\[.*Type:/ { 
            match($0, /Type: ([^|]+)/, arr)
            if (arr[1] != "") {
                saved_type = arr[1]
                gsub(/^[ \t]+|[ \t]+$/, "", saved_type)
            }
        }
        in_conflict && /^  Base name:/ { 
            saved_name = $3
        }
        END {
            if (saved_type != "" && saved_name == name) {
                print saved_type
            }
        }
    ')
    
    if echo "$types" | grep -q "CreateCreate"; then
        echo "CreateCreate"
    elif echo "$types" | grep -q "WriteUnlink"; then
        echo "WriteUnlink"
    elif echo "$types" | grep -q "DeleteCreate"; then
        echo "DeleteCreate"
    elif echo "$types" | grep -q "RenameConflict"; then
        echo "RenameConflict"
    elif echo "$types" | grep -q "WriteWrite"; then
        echo "WriteWrite"
    else
        echo ""
    fi
}

wait_for_sync() {
    echo "等待 Delta Sync ($1s)..."
    sleep "$1"
}

wait_for_conflict_detection() {
    local max_wait="${1:-10}"
    local waited=0
    
    echo "等待冲突检测 ($1s)..."
    while [ $waited -lt $max_wait ]; do
        sleep 1
        ((waited++))
        local count=$($CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test 2>/dev/null | grep "^Total:" | awk '{print $2}' || echo 0)
        [ "$count" -gt 0 ] && echo "冲突已检测到" && return 0
    done
    echo "冲突检测超时"
    return 1
}

wait_for_file_sync() {
    local file="$1"
    local max_wait="${2:-10}"
    local waited=0
    
    echo "等待文件同步: $file (最多${max_wait}s)"
    while [ ! -f "$file" ] && [ $waited -lt $max_wait ]; do
        sleep 1
        ((waited++))
    done
    
    if [ -f "$file" ]; then
        echo "文件同步成功"
        return 0
    else
        echo "文件同步超时"
        return 1
    fi
}

echo "=== PowerFS 冲突检测测试 ==="
echo "Delta Sync 间隔: 10秒"
check_environment "测试开始前" || exit 1
echo ""

mkdir -p "$TEST_DIR1" "$TEST_DIR2"

echo "=== 场景1: CreateCreate 冲突（目录）==="
check_environment "场景1开始前" || exit 1
cleanup_conflicts

mkdir -p "$TEST_DIR1/scene1-dir"
mkdir -p "$TEST_DIR2/scene1-dir"
sync && wait_for_conflict_detection 10

check_environment "场景1操作后" || exit 1

SCENE1_COUNT=$(count_conflicts)
run_test "应检测到目录冲突" \
    '[ "$SCENE1_COUNT" -ge 1 ]'

SCENE1_TYPE=$(get_conflict_type "scene1-dir")
run_test "冲突类型为 CreateCreate" \
    '[ "$SCENE1_TYPE" = "CreateCreate" ]'

echo "冲突详情:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test
echo ""

echo "=== 场景2: CreateCreate 冲突（文件，内容相同）==="
check_environment "场景2开始前" || exit 1
cleanup_conflicts

echo "same content" > "$TEST_DIR1/scene2-same.txt"
echo "same content" > "$TEST_DIR2/scene2-same.txt"
sync && wait_for_sync 15

check_environment "场景2操作后" || exit 1

SCENE2_COUNT=$(count_conflicts)
run_test "应检测到文件冲突" \
    '[ "$SCENE2_COUNT" -ge 1 ]'

SCENE2_TYPE=$(get_conflict_type "scene2-same.txt")
run_test "冲突类型为 CreateCreate" \
    '[ "$SCENE2_TYPE" = "CreateCreate" ]'

echo "冲突详情:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test
echo ""

echo "=== 场景3: CreateCreate 冲突（文件，内容不同）==="
check_environment "场景3开始前" || exit 1
cleanup_conflicts

echo "content1" > "$TEST_DIR1/scene3-diff.txt"
echo "content2" > "$TEST_DIR2/scene3-diff.txt"
sync && wait_for_sync 15

check_environment "场景3操作后" || exit 1

SCENE3_COUNT=$(count_conflicts)
run_test "应检测到文件冲突" \
    '[ "$SCENE3_COUNT" -ge 1 ]'

SCENE3_TYPE=$(get_conflict_type "scene3-diff.txt")
run_test "冲突类型为 CreateCreate" \
    '[ "$SCENE3_TYPE" = "CreateCreate" ]'

echo "冲突详情:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test
echo ""

echo "=== 场景4: WriteWrite 冲突 ==="
check_environment "场景4开始前" || exit 1
cleanup_conflicts

echo "initial" > "$TEST_DIR1/scene4-write.txt"
wait_for_file_sync "$TEST_DIR2/scene4-write.txt" 10 || true
sync && wait_for_sync 3

check_environment "场景4初始写入后" || exit 1

echo "client1 update" > "$TEST_DIR1/scene4-write.txt"
echo "client2 update" > "$TEST_DIR2/scene4-write.txt"
sync && wait_for_sync 15

check_environment "场景4冲突写入后" || exit 1

SCENE4_COUNT=$(count_conflicts)
run_test "应检测到 WriteWrite 冲突" \
    '[ "$SCENE4_COUNT" -ge 1 ]'

echo "冲突详情:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test
echo ""

echo "=== 场景5: WriteUnlink 冲突 ==="
check_environment "场景5开始前" || exit 1
cleanup_conflicts

echo "initial" > "$TEST_DIR1/scene5-writeunlink.txt"
if wait_for_file_sync "$TEST_DIR2/scene5-writeunlink.txt" 10; then
    run_test "客户端2应能看到客户端1创建的文件" 'true'
    
    echo "client1 update" > "$TEST_DIR1/scene5-writeunlink.txt"
    rm -f "$TEST_DIR2/scene5-writeunlink.txt"
    sync && wait_for_sync 15
    
    check_environment "场景5操作后" || exit 1
    
    SCENE5_TYPE=$(get_conflict_type "scene5-writeunlink.txt")
    run_test "应检测到 WriteUnlink 冲突" \
        '[ "$SCENE5_TYPE" = "WriteUnlink" ]'
else
    run_test "客户端2应能看到客户端1创建的文件" 'false'
    run_test "应检测到 WriteUnlink 冲突" 'false'
fi

echo "冲突详情:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test
echo ""

echo "=== 场景6: DeleteCreate 冲突 ==="
check_environment "场景6开始前" || exit 1
cleanup_conflicts

echo "initial" > "$TEST_DIR1/scene6-deletecreate.txt"
if wait_for_file_sync "$TEST_DIR2/scene6-deletecreate.txt" 10; then
    run_test "客户端2应能看到初始文件" 'true'
    
    rm -f "$TEST_DIR1/scene6-deletecreate.txt"
    echo "client2 create" > "$TEST_DIR2/scene6-deletecreate.txt"
    sync && wait_for_sync 15
    
    check_environment "场景6操作后" || exit 1
    
    SCENE6_TYPE=$(get_conflict_type "scene6-deletecreate.txt")
    run_test "应检测到 DeleteCreate 冲突" \
        '[ "$SCENE6_TYPE" = "DeleteCreate" ]'
else
    run_test "客户端2应能看到初始文件" 'false'
    run_test "应检测到 DeleteCreate 冲突" 'false'
fi

echo "冲突详情:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test
echo ""

echo "=== 场景7: Rename 冲突 ==="
check_environment "场景7开始前" || exit 1
cleanup_conflicts

echo "target content" > "$TEST_DIR1/scene7-target.txt"
echo "src content" > "$TEST_DIR2/scene7-src.txt"
sync && wait_for_sync 3

check_environment "场景7初始创建后" || exit 1

TARGET_OK=false
SRC_OK=false
if [ -f "$TEST_DIR2/scene7-target.txt" ]; then TARGET_OK=true; fi
if [ -f "$TEST_DIR1/scene7-src.txt" ]; then SRC_OK=true; fi

run_test "客户端1应能看到目标文件" "$TARGET_OK"
run_test "客户端2应能看到源文件" "$SRC_OK"

if $TARGET_OK && $SRC_OK; then
    mv "$TEST_DIR1/scene7-src.txt" "$TEST_DIR1/scene7-target.txt" 2>/dev/null || true
    echo "client2 update" > "$TEST_DIR2/scene7-target.txt"
    sync && wait_for_sync 15
    
    check_environment "场景7冲突操作后" || exit 1
    
    SCENE7_TYPE=$(get_conflict_type "scene7-target.txt")
    run_test "应检测到 RenameConflict" \
        '[ "$SCENE7_TYPE" = "RenameConflict" ]'
else
    run_test "应检测到 RenameConflict" 'false'
fi

echo "冲突详情:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test
echo ""

echo "=== 场景8: CLI 冲突管理功能测试 ==="
check_environment "场景8开始前" || exit 1
cleanup_conflicts

echo "manual test" > "$TEST_DIR1/scene8-cli.txt"
echo "manual test 2" > "$TEST_DIR2/scene8-cli.txt"
sync && wait_for_sync 3

check_environment "场景8冲突创建后" || exit 1

SCENE8_COUNT=$(count_conflicts)
run_test "应检测到冲突（手动策略）" \
    '[ "$SCENE8_COUNT" -ge 1 ]'

echo "冲突列表:"
$CLI_BIN -m $MASTER_ADDR conflicts list --path /conflict-test

echo "执行自动解决..."
$CLI_BIN -m $MASTER_ADDR conflicts auto-resolve --path /conflict-test --policy aggressive
wait_for_sync 0.5

check_environment "场景8自动解决后" || exit 1

SCENE8_RESOLVED=$(count_conflicts)
run_test "自动解决后冲突数应为0" \
    '[ "$SCENE8_RESOLVED" -eq 0 ]'

if [ -f "$TEST_DIR1/scene8-cli.txt" ] || [ -f "$TEST_DIR2/scene8-cli.txt" ]; then
    run_test "文件应存在（aggressive 策略保留）" 'true'
else
    run_test "文件应存在（aggressive 策略保留）" 'false'
fi
echo ""

echo "=== 场景9: 合并策略设置和查询 ==="
check_environment "场景9开始前" || exit 1

$CLI_BIN -m $MASTER_ADDR conflicts set-policy --path /conflict-test --policy write-priority 2>/dev/null
run_test "设置 write-priority 成功" 'true'

$CLI_BIN -m $MASTER_ADDR conflicts set-policy --path /conflict-test --policy delete-priority 2>/dev/null
run_test "设置 delete-priority 成功" 'true'

$CLI_BIN -m $MASTER_ADDR conflicts set-policy --path /conflict-test --policy manual 2>/dev/null
run_test "设置 manual 成功" 'true'

$CLI_BIN -m $MASTER_ADDR conflicts set-policy --path /conflict-test --policy aggressive 2>/dev/null
run_test "设置 aggressive 成功" 'true'
echo ""

echo "=== 场景10: 客户端同步验证 ==="
check_environment "场景10开始前" || exit 1
cleanup_conflicts

echo "sync test content" > "$TEST_DIR1/scene10-sync.txt"

if wait_for_file_sync "$TEST_DIR2/scene10-sync.txt" 10; then
    run_test "客户端2应同步到文件" 'true'
    
    CONTENT1=$(cat "$TEST_DIR1/scene10-sync.txt" 2>/dev/null || echo "")
    CONTENT2=$(cat "$TEST_DIR2/scene10-sync.txt" 2>/dev/null || echo "")
    run_test "文件内容应一致" "[ \"$CONTENT1\" = \"$CONTENT2\" ]"
    
    check_environment "场景10文件同步后" || exit 1
    
    rm -f "$TEST_DIR1/scene10-sync.txt"
    wait_for_sync 5
    
    check_environment "场景10文件删除后" || exit 1
    
    if [ ! -f "$TEST_DIR2/scene10-sync.txt" ]; then
        run_test "客户端2应同步删除" 'true'
    else
        run_test "客户端2应同步删除" 'false'
    fi
else
    run_test "客户端2应同步到文件" 'false'
    run_test "文件内容应一致" 'false'
    run_test "客户端2应同步删除" 'false'
fi
echo ""

echo "=== 测试结果汇总 ==="
check_environment "测试结束前" || exit 1

echo "通过: $PASSED"
echo "失败: $FAILED"
echo "跳过: 0"

if [ $FAILED -gt 0 ]; then
    echo ""
    echo "⚠️ 部分测试失败，请检查日志"
    exit 1
else
    echo ""
    echo "🎉 所有测试通过！"
    exit 0
fi