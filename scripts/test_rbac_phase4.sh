#!/usr/bin/env bash
# Phase 4 集成测试：权限边界 + 认证安全
# T4-01: 权限边界测试（无 token、无效 token、跨用户资源访问）
# T4-02: 认证安全测试（错误密码、禁用用户、无效刷新令牌）

set -u
cd /home/portion/powerfs

BIN=./target/debug/powerfs-monitor
AUTH_DB=/tmp/powerfs_auth_phase4_test.db
LOG_FILE=/tmp/powerfs_monitor_phase4_test.log
PID_FILE=/tmp/powerfs_monitor_phase4_test.pid
PORT=18084

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

# === 清理残留 ===
echo "[1/9] 清理残留..."
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
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
echo "[2/9] 编译 monitor..."
if ! cargo build -p powerfs-monitor 2>&1 | tail -3; then
    echo "编译失败"
    exit 1
fi

# === 启动 monitor ===
echo "[3/9] 启动 monitor (port=$PORT)..."
$BIN \
    --addr "0.0.0.0:${PORT}" \
    --redis-url "redis://localhost:6379" \
    --s3-endpoint "http://localhost:9000" \
    --s3-backend-endpoint "http://localhost:9002" \
    --auth-db-path "$AUTH_DB" \
    --jwt-secret "phase4-secret" \
    --hmac-secret "phase4-hmac" \
    --admin-username "admin" \
    --admin-password "admin12345" \
    > "$LOG_FILE" 2>&1 &
MONITOR_PID=$!
echo "$MONITOR_PID" > "$PID_FILE"

for i in $(seq 1 20); do
    if curl -sf -o /dev/null "http://127.0.0.1:${PORT}/api/auth/login" -X POST 2>/dev/null; then
        break
    fi
    if ! kill -0 "$MONITOR_PID" 2>/dev/null; then
        echo "  monitor 意外退出"; cat "$LOG_FILE"; exit 1
    fi
    sleep 0.5
done
if ! kill -0 "$MONITOR_PID" 2>/dev/null; then
    echo "  monitor 启动失败"; cat "$LOG_FILE"; exit 1
fi
echo "  monitor 启动成功 PID=$MONITOR_PID"

BASE="http://127.0.0.1:${PORT}"

# === 准备用户 ===
echo "[4/9] 准备测试用户..."
RESP=$(curl -s -X POST "$BASE/api/auth/login" -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin12345"}')
ADMIN_TOKEN=$(echo "$RESP" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -z "$ADMIN_TOKEN" ] && { echo "  admin 登录失败"; exit 1; }
echo "  admin 登录成功"

# 创建普通用户 carol
curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -H "Content-Type: application/json" \
    -d '{"username":"carol","password":"carolpass123","role":"user"}' "$BASE/api/users" >/dev/null

RESP=$(curl -s -X POST "$BASE/api/auth/login" -H "Content-Type: application/json" \
    -d '{"username":"carol","password":"carolpass123"}')
CAROL_TOKEN=$(echo "$RESP" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -z "$CAROL_TOKEN" ] && { echo "  carol 登录失败"; exit 1; }
echo "  carol 登录成功"

# 创建第二个普通用户 dave
curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -H "Content-Type: application/json" \
    -d '{"username":"dave","password":"davepass123","role":"user"}' "$BASE/api/users" >/dev/null

RESP=$(curl -s -X POST "$BASE/api/auth/login" -H "Content-Type: application/json" \
    -d '{"username":"dave","password":"davepass123"}')
DAVE_TOKEN=$(echo "$RESP" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -z "$DAVE_TOKEN" ] && { echo "  dave 登录失败"; exit 1; }
echo "  dave 登录成功"

# === T4-01: 权限边界测试 ===
echo "[5/9] T4-01: 权限边界测试..."

# 测试 1: 无 token 访问受保护路由
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/api/auth/me")
check "无 token 访问 /api/auth/me 应返回 401" "401" "$HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/api/users")
check "无 token 访问 /api/users 应返回 401" "401" "$HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/api/roles")
check "无 token 访问 /api/roles 应返回 401" "401" "$HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/api/alerts")
check "无 token 访问 /api/alerts 应返回 401" "401" "$HTTP_CODE"

# 测试 2: 无效 Bearer 格式
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: NotBearer abc" "$BASE/api/auth/me")
check "非 Bearer 格式应返回 401" "401" "$HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer" "$BASE/api/auth/me")
check "Bearer 后无 token 应返回 401" "401" "$HTTP_CODE"

# 测试 3: 无效 token 字符串
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer invalidtoken123" "$BASE/api/auth/me")
check "无效 token 字符串应返回 401" "401" "$HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer " "$BASE/api/auth/me")
check "空 token 应返回 401" "401" "$HTTP_CODE"

# 测试 4: 使用其他服务的 JWT secret 签名的 token
# 用不同 secret 生成 token（通过另一个 monitor 实例）
# 简化：使用错误格式的 JWT
WRONG_TOKEN="eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0IiwidXNlcm5hbWUiOiJoYWNrZXIiLCJyb2xlIjoiYWRtaW4iLCJleHAiOjk5OTk5OTk5OTl9.invalid_signature"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $WRONG_TOKEN" "$BASE/api/auth/me")
check "伪造签名的 token 应返回 401" "401" "$HTTP_CODE"

# 测试 5: 普通用户不能访问用户管理 API
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $CAROL_TOKEN" "$BASE/api/users")
check "普通用户访问 /api/users 应返回 403" "403" "$HTTP_CODE"

# 测试 6: 普通用户不能创建用户
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Authorization: Bearer $CAROL_TOKEN" \
    -H "Content-Type: application/json" -d '{"username":"evil","password":"evilpass123"}' "$BASE/api/users")
check "普通用户创建用户应返回 403" "403" "$HTTP_CODE"

# 测试 7: 普通用户不能删除其他用户
# 获取 carol 的 user id
RESP=$(curl -s -H "Authorization: Bearer $CAROL_TOKEN" "$BASE/api/auth/me")
CAROL_ID=$(echo "$RESP" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
# 获取 dave 的 user id (通过 admin)
RESP=$(curl -s -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/api/users")
DAVE_ID=$(echo "$RESP" | sed -n 's/.*"id":"\([^"]*\)","username":"dave".*/\1/p')

if [ -n "$DAVE_ID" ]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
        -H "Authorization: Bearer $CAROL_TOKEN" "$BASE/api/users/$DAVE_ID")
    check "普通用户删除其他用户应返回 403" "403" "$HTTP_CODE"
else
    echo "  [SKIP] 无法获取 dave ID，跳过删除测试"
fi

# 测试 8: AccessKey 跨用户隔离
# carol 创建 AccessKey
RESP=$(curl -s -X POST -H "Authorization: Bearer $CAROL_TOKEN" "$BASE/api/s3/keys")
CAROL_KEY_ID=$(echo "$RESP" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')

# dave 列出自己的 key，不应包含 carol 的
RESP=$(curl -s -H "Authorization: Bearer $DAVE_TOKEN" "$BASE/api/s3/keys")
not_contains "dave 不应看到 carol 的 AccessKey" "$CAROL_KEY_ID" "$RESP"

# dave 删除 carol 的 key 应被拒
if [ -n "$CAROL_KEY_ID" ]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
        -H "Authorization: Bearer $DAVE_TOKEN" "$BASE/api/s3/keys/$CAROL_KEY_ID")
    check "dave 删除 carol 的 AccessKey 应返回 403" "403" "$HTTP_CODE"
fi

# === T4-02: 认证安全测试 ===
echo "[6/9] T4-02: 认证安全测试..."

# 测试 9: 错误密码登录
RESP=$(curl -s -X POST "$BASE/api/auth/login" -H "Content-Type: application/json" \
    -d '{"username":"carol","password":"wrongpassword"}')
contains "错误密码登录应失败" "Invalid username or password" "$RESP"
not_contains "错误密码不应返回 token" "\"token\":" "$RESP"

# 测试 10: 不存在的用户登录
RESP=$(curl -s -X POST "$BASE/api/auth/login" -H "Content-Type: application/json" \
    -d '{"username":"nonexistent","password":"anypassword"}')
contains "不存在用户登录应失败" "Invalid username or password" "$RESP"

# 测试 11: 空用户名/密码
RESP=$(curl -s -X POST "$BASE/api/auth/login" -H "Content-Type: application/json" \
    -d '{"username":"","password":""}')
# 应返回错误（可能 400 或 error message）
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE/api/auth/login" \
    -H "Content-Type: application/json" -d '{"username":"","password":""}')
# 空 username 在 UserStore 中查不到，应返回 error 而非 200 成功
contains "空用户名登录应失败" "Invalid username or password" "$RESP"

# 测试 12: 无效 refresh token
RESP=$(curl -s -X POST "$BASE/api/auth/refresh" -H "Content-Type: application/json" \
    -d '{"refresh_token":"invalid-refresh-token"}')
contains "无效 refresh token 应返回错误" "\"code\":500" "$RESP"

# 测试 13: 用 access token 作为 refresh token（应失败，签名密钥不同）
RESP=$(curl -s -X POST "$BASE/api/auth/refresh" -H "Content-Type: application/json" \
    -d "{\"refresh_token\":\"$CAROL_TOKEN\"}")
contains "access token 不能用作 refresh token" "\"code\":500" "$RESP"

# 测试 14: 禁用用户不能登录
echo "[7/9] 测试禁用用户..."
# 先用 admin 更新 carol 状态为 inactive
RESP=$(curl -s -X PUT -H "Authorization: Bearer $ADMIN_TOKEN" -H "Content-Type: application/json" \
    -d '{"status":"inactive"}' "$BASE/api/users/$CAROL_ID")
contains "禁用 carol 用户成功" "\"code\":200" "$RESP"

# carol 尝试登录应失败
RESP=$(curl -s -X POST "$BASE/api/auth/login" -H "Content-Type: application/json" \
    -d '{"username":"carol","password":"carolpass123"}')
contains "禁用用户登录应失败" "disabled or locked" "$RESP"
not_contains "禁用用户不应获得 token" "\"token\":" "$RESP"

# 测试 15: 禁用前的旧 token 仍有效（当前未实现 token 黑名单，这是已知行为）
# 恢复 carol 状态
curl -s -X PUT -H "Authorization: Bearer $ADMIN_TOKEN" -H "Content-Type: application/json" \
    -d '{"status":"active"}' "$BASE/api/users/$CAROL_ID" >/dev/null

# 测试 16: 重新启用后可以登录
RESP=$(curl -s -X POST "$BASE/api/auth/login" -H "Content-Type: application/json" \
    -d '{"username":"carol","password":"carolpass123"}')
contains "重新启用后可登录" "\"token\":" "$RESP"

# === T4-01 补充: 资源归属隔离 ===
echo "[8/9] 测试资源归属隔离..."

# carol 创建 AccessKey
RESP=$(curl -s -X POST -H "Authorization: Bearer $CAROL_TOKEN" "$BASE/api/s3/keys")
CAROL_NEW_KEY_ID=$(echo "$RESP" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
CAROL_NEW_ACCESS=$(echo "$RESP" | sed -n 's/.*"access_key":"\([^"]*\)".*/\1/p')

# dave 创建 AccessKey
RESP=$(curl -s -X POST -H "Authorization: Bearer $DAVE_TOKEN" "$BASE/api/s3/keys")
DAVE_KEY_ID=$(echo "$RESP" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
DAVE_ACCESS=$(echo "$RESP" | sed -n 's/.*"access_key":"\([^"]*\)".*/\1/p')

# carol 列出自己的 key，不应看到 dave 的
RESP=$(curl -s -H "Authorization: Bearer $CAROL_TOKEN" "$BASE/api/s3/keys")
contains "carol 看到自己的 key" "$CAROL_NEW_ACCESS" "$RESP"
not_contains "carol 不应看到 dave 的 key" "$DAVE_ACCESS" "$RESP"

# dave 列出自己的 key，不应看到 carol 的
RESP=$(curl -s -H "Authorization: Bearer $DAVE_TOKEN" "$BASE/api/s3/keys")
contains "dave 看到自己的 key" "$DAVE_ACCESS" "$RESP"
not_contains "dave 不应看到 carol 的 key" "$CAROL_NEW_ACCESS" "$RESP"

# admin 可以删除任意 key
RESP=$(curl -s -X DELETE -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/api/s3/keys/$DAVE_KEY_ID")
contains "admin 删除 dave 的 key 成功" "\"code\":200" "$RESP"

# === 清理和汇总 ===
echo "[9/9] 清理..."
echo "  停止 monitor..."
kill "$MONITOR_PID" 2>/dev/null || true
sleep 1
kill -9 "$MONITOR_PID" 2>/dev/null || true
rm -f "$PID_FILE"
rm -rf "$AUTH_DB"

echo ""
echo "=== Phase 4 测试结果汇总 ==="
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
