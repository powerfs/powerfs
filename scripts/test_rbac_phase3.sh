#!/usr/bin/env bash
# Phase 3 集成测试：RBAC 权限细化
# 验证角色管理 API、S3 AccessKey 多用户管理、权限边界

set -u
cd /home/portion/powerfs

BIN=./target/debug/powerfs-monitor
AUTH_DB=/tmp/powerfs_auth_phase3_test.db
LOG_FILE=/tmp/powerfs_monitor_phase3_test.log
PID_FILE=/tmp/powerfs_monitor_phase3_test.pid
PORT=18083

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

# === 清理上次残留 ===
echo "[1/8] 清理上次残留..."
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
echo "[2/8] 编译 monitor (debug)..."
if ! cargo build -p powerfs-monitor 2>&1 | tail -5; then
    echo "编译失败"
    exit 1
fi

# === 启动 monitor ===
echo "[3/8] 启动 monitor (port=$PORT)..."
$BIN \
    --addr "0.0.0.0:${PORT}" \
    --redis-url "redis://localhost:6379" \
    --s3-endpoint "http://localhost:9000" \
    --s3-backend-endpoint "http://localhost:9002" \
    --auth-db-path "$AUTH_DB" \
    --jwt-secret "test-secret-phase3" \
    --hmac-secret "test-hmac-phase3" \
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
echo "  monitor 启动成功 PID=$MONITOR_PID"

BASE="http://127.0.0.1:${PORT}"

# === 准备用户 ===
echo "[4/8] 准备测试用户..."
RESP=$(curl -s -X POST "$BASE/api/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin12345"}')
ADMIN_TOKEN=$(echo "$RESP" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
if [ -z "$ADMIN_TOKEN" ]; then
    echo "  [FAIL] admin 登录失败：$RESP"
    exit 1
fi
echo "  admin 登录成功"

# 创建普通用户 bob
RESP=$(curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"username":"bob","password":"bobpass123","role":"user"}' \
    "$BASE/api/users")
contains "创建用户 bob" "bob" "$RESP"

# bob 登录
RESP=$(curl -s -X POST -H "Content-Type: application/json" \
    -d '{"username":"bob","password":"bobpass123"}' \
    "$BASE/api/auth/login")
BOB_TOKEN=$(echo "$RESP" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
if [ -z "$BOB_TOKEN" ]; then
    echo "  [FAIL] bob 登录失败：$RESP"
    exit 1
fi
echo "  bob 登录成功"

# === 测试 1: 角色管理 - 默认角色 ===
echo "[5/8] 测试角色管理 API..."
RESP=$(curl -s -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/api/roles")
contains "admin 列出角色应返回成功" "\"code\":200" "$RESP"
contains "默认角色包含 admin" "\"name\":\"admin\"" "$RESP"
contains "默认角色包含 user" "\"name\":\"user\"" "$RESP"
contains "admin 角色拥有超级权限" "\"\\*\"" "$RESP"

# 提取 admin 角色 ID
ADMIN_ROLE_ID=$(echo "$RESP" | sed -n 's/.*"id":"\([^"]*\)","name":"admin".*/\1/p')
if [ -z "$ADMIN_ROLE_ID" ]; then
    echo "  [FAIL] 无法提取 admin 角色 ID"
    FAIL=$((FAIL+1))
fi

# === 测试 2: 角色管理 - 创建自定义角色 ===
RESP=$(curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"name":"auditor","description":"只读审计员","permissions":["s3:read","kv:read","alert:read"]}' \
    "$BASE/api/roles")
contains "创建 auditor 角色成功" "\"code\":200" "$RESP"
contains "auditor 角色名正确" "\"name\":\"auditor\"" "$RESP"
contains "auditor 角色描述正确" "\"只读审计员\"" "$RESP"
contains "auditor 角色权限正确" "\"s3:read\"" "$RESP"

# 提取 auditor 角色 ID
AUDITOR_ROLE_ID=$(echo "$RESP" | sed -n 's/.*"id":"\([^"]*\)","name":"auditor".*/\1/p')

# === 测试 3: 角色管理 - 获取单个角色 ===
RESP=$(curl -s -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/api/roles/$AUDITOR_ROLE_ID")
contains "获取 auditor 角色成功" "\"code\":200" "$RESP"
contains "获取角色 ID 匹配" "\"$AUDITOR_ROLE_ID\"" "$RESP"

# === 测试 4: 角色管理 - 更新角色 ===
RESP=$(curl -s -X PUT -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"description":"更新后的审计员","permissions":["s3:read","alert:read"]}' \
    "$BASE/api/roles/$AUDITOR_ROLE_ID")
contains "更新 auditor 角色成功" "\"code\":200" "$RESP"
contains "更新后描述正确" "\"更新后的审计员\"" "$RESP"
contains "更新后权限数量正确" "\"s3:read\"" "$RESP"

# === 测试 5: 角色管理 - 普通用户无权访问 ===
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $BOB_TOKEN" "$BASE/api/roles")
check "普通用户访问角色列表应返回 403" "403" "$HTTP_CODE"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Authorization: Bearer $BOB_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"name":"hacker","permissions":["*"]}' \
    "$BASE/api/roles")
check "普通用户创建角色应返回 403" "403" "$HTTP_CODE"

# === 测试 6: S3 AccessKey - 用户创建自己的密钥 ===
echo "[6/8] 测试 S3 AccessKey 多用户管理..."
RESP=$(curl -s -X POST -H "Authorization: Bearer $BOB_TOKEN" "$BASE/api/s3/keys")
contains "bob 创建 AccessKey 成功" "\"code\":200" "$RESP"
contains "返回 AccessKey" "\"access_key\"" "$RESP"
contains "返回明文 SecretKey" "\"secret_key\"" "$RESP"

# 提取 bob 的 access_key
BOB_ACCESS_KEY=$(echo "$RESP" | sed -n 's/.*"access_key":"\([^"]*\)".*/\1/p')
BOB_KEY_ID=$(echo "$RESP" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
if [ -z "$BOB_ACCESS_KEY" ] || [ -z "$BOB_KEY_ID" ]; then
    echo "  [FAIL] 无法提取 bob 的 access_key 或 id"
    FAIL=$((FAIL+1))
fi
echo "  bob 创建 AccessKey: $BOB_ACCESS_KEY"

# === 测试 7: S3 AccessKey - 用户列出自己的密钥 ===
RESP=$(curl -s -H "Authorization: Bearer $BOB_TOKEN" "$BASE/api/s3/keys")
contains "bob 列出 AccessKey 成功" "\"code\":200" "$RESP"
contains "列表包含 bob 的 access_key" "\"$BOB_ACCESS_KEY\"" "$RESP"
not_contains "列表不包含 secret_key_hash" "\"secret_key_hash\"" "$RESP"

# === 测试 8: S3 AccessKey - admin 也能创建和列出 ===
RESP=$(curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/api/s3/keys")
contains "admin 创建 AccessKey 成功" "\"code\":200" "$RESP"
ADMIN_ACCESS_KEY=$(echo "$RESP" | sed -n 's/.*"access_key":"\([^"]*\)".*/\1/p')
ADMIN_KEY_ID=$(echo "$RESP" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')

RESP=$(curl -s -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/api/s3/keys")
contains "admin 列出自己的 AccessKey" "\"$ADMIN_ACCESS_KEY\"" "$RESP"
# admin 只能看到自己的，不能看到 bob 的
not_contains "admin 不应看到 bob 的 AccessKey" "\"$BOB_ACCESS_KEY\"" "$RESP"

# === 测试 9: S3 AccessKey - 用户不能删除别人的密钥 ===
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
    -H "Authorization: Bearer $BOB_TOKEN" \
    "$BASE/api/s3/keys/$ADMIN_KEY_ID")
check "bob 删除 admin 的密钥应返回 403" "403" "$HTTP_CODE"

# === 测试 10: S3 AccessKey - admin 可以删除任意密钥 ===
RESP=$(curl -s -X DELETE -H "Authorization: Bearer $ADMIN_TOKEN" \
    "$BASE/api/s3/keys/$BOB_KEY_ID")
contains "admin 删除 bob 的密钥成功" "\"code\":200" "$RESP"

# 验证删除后 bob 列表中不再有该 key
RESP=$(curl -s -H "Authorization: Bearer $BOB_TOKEN" "$BASE/api/s3/keys")
not_contains "删除后 bob 列表中不再有该 key" "\"$BOB_ACCESS_KEY\"" "$RESP"

# === 测试 11: 权限边界 - 普通用户访问用户管理 API ===
echo "[7/8] 测试权限边界..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $BOB_TOKEN" "$BASE/api/users")
check "普通用户访问用户列表应返回 403" "403" "$HTTP_CODE"

# === 测试 12: 角色管理 - 删除角色 ===
echo "[8/8] 测试角色删除和单元测试..."
RESP=$(curl -s -X DELETE -H "Authorization: Bearer $ADMIN_TOKEN" \
    "$BASE/api/roles/$AUDITOR_ROLE_ID")
contains "删除 auditor 角色成功" "\"code\":200" "$RESP"

# 验证删除后获取应失败
RESP=$(curl -s -H "Authorization: Bearer $ADMIN_TOKEN" "$BASE/api/roles/$AUDITOR_ROLE_ID")
contains "删除后获取角色应返回错误" "\"code\":500" "$RESP"

# === 单元测试 ===
echo "  运行 RoleStore 单元测试..."
if cargo test -p powerfs-monitor role 2>&1 | grep -q "test result: ok"; then
    echo "  [PASS] RoleStore 单元测试通过"
    PASS=$((PASS+1))
else
    echo "  [FAIL] RoleStore 单元测试失败"
    FAIL=$((FAIL+1))
fi

echo "  运行 S3AccessKeyStore 单元测试..."
if cargo test -p powerfs-monitor s3_access_key 2>&1 | grep -q "test result: ok"; then
    echo "  [PASS] S3AccessKeyStore 单元测试通过"
    PASS=$((PASS+1))
else
    echo "  [FAIL] S3AccessKeyStore 单元测试失败"
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
echo "=== Phase 3 测试结果汇总 ==="
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
