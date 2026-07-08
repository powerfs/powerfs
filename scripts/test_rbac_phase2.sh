#!/usr/bin/env bash
# Phase 2 集成测试：资源归属与基础权限
# 验证 S3 bucket 归属权记录、alert 按 owner 过滤、ResourceOwnerStore 行为

set -u
cd /home/portion/powerfs

BIN=./target/debug/powerfs-monitor
AUTH_DB=/tmp/powerfs_auth_phase2_test.db
LOG_FILE=/tmp/powerfs_monitor_phase2_test.log
PID_FILE=/tmp/powerfs_monitor_phase2_test.pid
PORT=18082

# === 安全清理上次残留 ===
echo "[1/7] 清理上次残留..."
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "  停止旧 monitor 进程 PID=$OLD_PID"
        kill "$OLD_PID" 2>/dev/null || true
        sleep 1
        kill -9 "$OLD_PID" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
fi
if command -v fuser >/dev/null 2>&1; then
    fuser -k "${PORT}/tcp" 2>/dev/null || true
fi
rm -rf "$AUTH_DB"
rm -f "$LOG_FILE"

# === 编译 ===
echo "[2/7] 编译 monitor (debug)..."
if ! cargo build -p powerfs-monitor 2>&1 | tail -5; then
    echo "编译失败"
    exit 1
fi

# === 启动 monitor ===
echo "[3/7] 启动 monitor (port=$PORT)..."
$BIN \
    --addr "0.0.0.0:${PORT}" \
    --redis-url "redis://localhost:6379" \
    --s3-endpoint "http://localhost:9000" \
    --s3-backend-endpoint "http://localhost:9002" \
    --auth-db-path "$AUTH_DB" \
    --jwt-secret "test-secret-phase2" \
    --admin-username "admin" \
    --admin-password "admin12345" \
    > "$LOG_FILE" 2>&1 &
MONITOR_PID=$!
echo "$MONITOR_PID" > "$PID_FILE"
echo "  monitor PID=$MONITOR_PID"

# 等待启动
for i in $(seq 1 20); do
    if curl -sf -o /dev/null "http://127.0.0.1:${PORT}/api/auth/login" -X POST 2>/dev/null; then
        break
    fi
    if ! kill -0 "$MONITOR_PID" 2>/dev/null; then
        echo "  monitor 进程意外退出，日志："
        cat "$LOG_FILE"
        exit 1
    fi
    sleep 0.5
done

if ! kill -0 "$MONITOR_PID" 2>/dev/null; then
    echo "  monitor 启动失败"
    cat "$LOG_FILE"
    exit 1
fi
echo "  monitor 启动成功"

BASE="http://127.0.0.1:${PORT}"
PASS=0
FAIL=0

check() {
    local name="$1"
    local expected="$2"
    local actual="$3"
    if [ "$actual" = "$expected" ]; then
        echo "  [PASS] $name"
        PASS=$((PASS+1))
    else
        echo "  [FAIL] $name (expected=$expected actual=$actual)"
        FAIL=$((FAIL+1))
    fi
}

contains() {
    local name="$1"
    local needle="$2"
    local haystack="$3"
    if echo "$haystack" | grep -q "$needle"; then
        echo "  [PASS] $name"
        PASS=$((PASS+1))
    else
        echo "  [FAIL] $name (未找到 '$needle'，响应=$haystack)"
        FAIL=$((FAIL+1))
    fi
}

not_contains() {
    local name="$1"
    local needle="$2"
    local haystack="$3"
    if echo "$haystack" | grep -q "$needle"; then
        echo "  [FAIL] $name (不应包含 '$needle'，响应=$haystack)"
        FAIL=$((FAIL+1))
    else
        echo "  [PASS] $name"
        PASS=$((PASS+1))
    fi
}

# === 准备：admin 登录 ===
echo "[4/7] 准备测试用户..."
RESP=$(curl -s -X POST "$BASE/api/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin12345"}')
ADMIN_TOKEN=$(echo "$RESP" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
if [ -z "$ADMIN_TOKEN" ]; then
    echo "  [FAIL] admin 登录失败：$RESP"
    FAIL=$((FAIL+1))
    exit 1
fi
echo "  admin 登录成功"

# === 创建普通用户 alice ===
RESP=$(curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"username":"alice","password":"alicepass123","role":"user"}' \
    "$BASE/api/users")
contains "创建用户 alice" "alice" "$RESP"

# === alice 登录 ===
RESP=$(curl -s -X POST -H "Content-Type: application/json" \
    -d '{"username":"alice","password":"alicepass123"}' \
    "$BASE/api/auth/login")
ALICE_TOKEN=$(echo "$RESP" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
if [ -z "$ALICE_TOKEN" ]; then
    echo "  [FAIL] alice 登录失败：$RESP"
    FAIL=$((FAIL+1))
    exit 1
fi
echo "  alice 登录成功"

# === 测试 1: Alert 过滤 - admin 可见系统告警 ===
echo "[5/7] 测试 Alert 过滤..."
# 等待 alert engine 触发（默认规则可能未触发，但接口应可用）
RESP=$(curl -s -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/api/alerts")
# admin 应能成功访问 alerts 接口
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/api/alerts")
check "admin 访问 /api/alerts 应返回 200" "200" "$HTTP_CODE"

# === 测试 2: Alert 过滤 - 普通用户访问 alerts 接口 ===
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $ALICE_TOKEN" "$BASE/api/alerts")
check "alice 访问 /api/alerts 应返回 200" "200" "$HTTP_CODE"

# alice 查看告警，应只看到自己的（目前无归属告警，应返回空数组）
RESP=$(curl -s -H "Authorization: Bearer $ALICE_TOKEN" "$BASE/api/alerts")
contains "alice 查看告警返回成功标记" "\"code\":200" "$RESP"
contains "alice 无归属告警返回空数组" "\"data\":\[\]" "$RESP"

# === 测试 3: S3 bucket 列表 - 普通用户应只见自己的 bucket ===
echo "[6/7] 测试 S3 bucket 归属权..."
# 注意：S3 后端可能未运行，get_buckets 会返回空或错误，但接口应可访问
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/api/s3/buckets")
check "admin 访问 /api/s3/buckets 应返回 200" "200" "$HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $ALICE_TOKEN" "$BASE/api/s3/buckets")
check "alice 访问 /api/s3/buckets 应返回 200" "200" "$HTTP_CODE"

# === 测试 4: 创建 bucket（S3 后端未运行时返回错误，但归属记录不应被创建）===
RESP=$(curl -s -X POST -H "Authorization: Bearer $ALICE_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"name":"alice-bucket"}' \
    "$BASE/api/s3/buckets")
# S3 后端未运行时应返回 error code:500（验证接口可达且归属逻辑未误创建记录）
contains "alice 创建 bucket 接口可达" "\"code\":" "$RESP"
not_contains "S3 失败时不应记录归属" "\"code\":200" "$RESP"

# === 测试 5: 删除 bucket - 非 owner 应被拒 ===
# 由于 S3 后端未运行，无法真正创建 bucket，所以 delete 也会因 S3 失败
# 但如果 S3 运行，归属检查会先执行
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
    -H "Authorization: Bearer $ALICE_TOKEN" \
    "$BASE/api/s3/buckets/nonexistent-bucket")
# 由于 bucket 不存在且 alice 不是 owner，应返回 403（归属检查先于 S3 调用）
check "alice 删除非自己 bucket 应被拒(403)" "403" "$HTTP_CODE"

# === 测试 6: admin 删除任意 bucket（无归属检查）===
# admin 不受归属检查限制，但 S3 后端未运行会返回错误
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    "$BASE/api/s3/buckets/nonexistent-bucket")
# admin 跳过归属检查，直接调用 S3，S3 未运行返回错误（200 表示接口正常但 S3 失败）
echo "  admin 删除 bucket HTTP_CODE=$HTTP_CODE（S3 后端未运行时为 200+error）"

# === 测试 7: ResourceOwnerStore 单元测试 ===
echo "[7/7] 运行 ResourceOwnerStore 单元测试..."
if cargo test -p powerfs-monitor resource_owner 2>&1 | grep -q "test result: ok"; then
    echo "  [PASS] ResourceOwnerStore 单元测试通过"
    PASS=$((PASS+1))
else
    echo "  [FAIL] ResourceOwnerStore 单元测试失败"
    FAIL=$((FAIL+1))
fi

# === 清理 ===
echo "  停止 monitor..."
kill "$MONITOR_PID" 2>/dev/null || true
sleep 1
kill -9 "$MONITOR_PID" 2>/dev/null || true
rm -f "$PID_FILE"
rm -rf "$AUTH_DB"

echo ""
echo "=== Phase 2 测试结果汇总 ==="
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
if [ "$FAIL" -gt 0 ]; then
    echo "  日志：$LOG_FILE"
    exit 1
else
    rm -f "$LOG_FILE"
    echo "  全部通过"
    exit 0
fi
