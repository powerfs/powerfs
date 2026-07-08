#!/usr/bin/env bash
# Phase 1.6 集成测试：启动 monitor 并验证 RBAC 认证流程
# 安全地停止旧进程、启动新进程、运行测试、最后清理

set -u
cd /home/portion/powerfs

BIN=./target/debug/powerfs-monitor
AUTH_DB=/tmp/powerfs_auth_test.db
LOG_FILE=/tmp/powerfs_monitor_test.log
PID_FILE=/tmp/powerfs_monitor_test.pid
PORT=18081

# === 安全清理上次残留 ===
echo "[1/6] 清理上次残留..."
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
# 兜底：按端口查找并停止（仅针对测试端口 18081，不影响生产）
if command -v fuser >/dev/null 2>&1; then
    fuser -k "${PORT}/tcp" 2>/dev/null || true
fi
rm -rf "$AUTH_DB"
rm -f "$LOG_FILE"

# === 编译 ===
echo "[2/6] 编译 monitor (debug)..."
if ! cargo build -p powerfs-monitor 2>&1 | tail -5; then
    echo "编译失败"
    exit 1
fi

# === 启动 monitor ===
echo "[3/6] 启动 monitor (port=$PORT)..."
$BIN \
    --addr "0.0.0.0:${PORT}" \
    --redis-url "redis://localhost:6379" \
    --s3-endpoint "http://localhost:9000" \
    --s3-backend-endpoint "http://localhost:9002" \
    --auth-db-path "$AUTH_DB" \
    --jwt-secret "test-secret-please-change" \
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

# === 测试 1: 错误密码登录失败 ===
echo "[4/6] 测试认证流程..."
RESP=$(curl -s -X POST "$BASE/api/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"wrong"}')
contains "登录-错误密码应被拒" "Invalid username or password" "$RESP"

# === 测试 2: 正确密码登录成功 ===
RESP=$(curl -s -X POST "$BASE/api/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin12345"}')
contains "登录-正确密码返回 token" "\"token\":" "$RESP"
contains "登录-返回 refresh_token" "refresh_token" "$RESP"
contains "登录-返回用户信息" "username" "$RESP"
TOKEN=$(echo "$RESP" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
REFRESH=$(echo "$RESP" | sed -n 's/.*"refresh_token":"\([^"]*\)".*/\1/p')

if [ -z "$TOKEN" ]; then
    echo "  [FAIL] 无法提取 token，原始响应=$RESP"
    FAIL=$((FAIL+1))
fi

# === 测试 3: 未带 token 访问受保护路由被拒（401） ===
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/api/auth/me")
check "无 token 访问 /api/auth/me 应返回 401" "401" "$HTTP_CODE"

# === 测试 4: 携带 token 访问 /api/auth/me ===
RESP=$(curl -s -H "Authorization: Bearer $TOKEN" "$BASE/api/auth/me")
contains "携带 token 获取当前用户" "admin" "$RESP"

# === 测试 5: 列出用户（admin） ===
RESP=$(curl -s -H "Authorization: Bearer $TOKEN" "$BASE/api/users")
contains "admin 列出用户" "admin" "$RESP"

# === 测试 6: 创建新用户 ===
RESP=$(curl -s -X POST -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"username":"testuser","password":"testpass123","role":"user","email":"t@example.com"}' \
    "$BASE/api/users")
contains "创建用户 testuser" "testuser" "$RESP"
NEW_USER_ID=$(echo "$RESP" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')

# === 测试 7: 用 testuser 登录 ===
RESP=$(curl -s -X POST -H "Content-Type: application/json" \
    -d '{"username":"testuser","password":"testpass123"}' \
    "$BASE/api/auth/login")
contains "testuser 登录成功" "\"token\":" "$RESP"
USER_TOKEN=$(echo "$RESP" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')

# === 测试 8: 普通用户访问 /api/users 被拒（403） ===
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $USER_TOKEN" \
    "$BASE/api/users")
check "普通用户访问 /api/users 应返回 403" "403" "$HTTP_CODE"

# === 测试 9: 普通用户可获取自己信息 ===
RESP=$(curl -s -H "Authorization: Bearer $USER_TOKEN" "$BASE/api/auth/me")
contains "普通用户获取自己信息" "testuser" "$RESP"

# === 测试 10: 刷新 token ===
RESP=$(curl -s -X POST -H "Content-Type: application/json" \
    -d "{\"refresh_token\":\"$REFRESH\"}" \
    "$BASE/api/auth/refresh")
contains "刷新 token 成功" "\"token\":" "$RESP"

# === 测试 11: admin 更新 testuser 角色 ===
if [ -n "$NEW_USER_ID" ]; then
    RESP=$(curl -s -X PUT -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{"role":"admin"}' \
        "$BASE/api/users/$NEW_USER_ID")
    contains "更新用户角色为 admin" "admin" "$RESP"
fi

# === 测试 12: 删除用户 ===
if [ -n "$NEW_USER_ID" ]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
        -H "Authorization: Bearer $TOKEN" \
        "$BASE/api/users/$NEW_USER_ID")
    check "删除用户返回 200" "200" "$HTTP_CODE"
fi

# === 测试 13: 错误 token 应被拒 ===
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer invalid.token.here" \
    "$BASE/api/auth/me")
check "无效 token 应返回 401" "401" "$HTTP_CODE"

# === 清理 ===
echo "[5/6] 停止 monitor..."
kill "$MONITOR_PID" 2>/dev/null || true
sleep 1
kill -9 "$MONITOR_PID" 2>/dev/null || true
rm -f "$PID_FILE"
rm -rf "$AUTH_DB"

echo "[6/6] 测试结果汇总："
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
