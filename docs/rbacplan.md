# PowerFS RBAC 权限管理优化方案

## 1. 需求分析

### 1.1 业务需求概述

| 需求编号 | 需求描述 | 优先级 |
|---------|---------|--------|
| RQ-001 | 前端管理增加用户管理功能，支持用户注册、登录、修改密码 | 高 |
| RQ-002 | 实现 RBAC 角色权限控制，支持管理员和普通用户角色 | 高 |
| RQ-003 | 防止非法用户访问和操作，所有 API 需认证 | 高 |
| RQ-004 | KV 支持多用户管理，用户只能访问自己的 KV 数据 | 高 |
| RQ-005 | S3 支持多用户管理，用户只能访问自己的存储桶 | 高 |
| RQ-006 | 用户可以创建和管理自己的 S3 API AccessKey | 中 |
| RQ-007 | 管理员可以查看所有系统信息和管理所有用户 | 高 |
| RQ-008 | 普通用户只能查看和管理自己的 KV、S3 及相关告警 | 高 |

### 1.2 当前问题分析

| 问题编号 | 问题描述 | 影响范围 |
|---------|---------|---------|
| PROB-001 | 当前无用户概念，只有全局单一 access_key/secret_key | 全部 |
| PROB-002 | Monitor API 无认证，任何人都可以访问和操作 | 管理界面 |
| PROB-003 | KV 无多租户隔离，所有数据共享 | KV 模块 |
| PROB-004 | S3 Bucket 无归属概念，所有用户共享 | S3 模块 |
| PROB-005 | 告警无用户关联，无法区分用户级告警 | 监控模块 |

---

## 2. 总体架构设计

### 2.1 认证与授权架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            认证层 (Authentication)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │   JWT Token  │    │  RefreshToken│    │   Login API  │                   │
│  │  (访问令牌)   │    │  (刷新令牌)   │    │  (登录接口)   │                   │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                   │
│         │                   │                   │                          │
└─────────┼───────────────────┼───────────────────┼───────────────────────────┘
          ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            授权层 (Authorization)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │    User      │    │    Role      │    │  Permission  │                   │
│  │  (用户模型)   │    │  (角色模型)   │    │  (权限模型)   │                   │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                   │
│         │                   │                   │                          │
│         │ 1:N               │ 1:N               │ N:N                       │
│         ▼                   ▼                   ▼                          │
│  ┌──────────────────────────────────────────────┐                          │
│  │           Access Control Middleware          │                          │
│  │         (访问控制中间件)                      │                          │
│  └──────────────────────┬───────────────────────┘                          │
└─────────────────────────┼───────────────────────────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          资源层 (Resource)                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │    KV Data   │    │  S3 Bucket   │    │    Alert     │                   │
│  │  (KV数据)     │    │  (存储桶)     │    │  (告警)      │                   │
│  │  +owner      │    │  +owner      │    │  +owner      │                   │
│  └──────────────┘    └──────────────┘    └──────────────┘                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 认证流程

```
前端请求 ──→ 认证中间件 ──→ 验证JWT ──→ 解析用户信息 ──→ 注入请求上下文 ──→ 业务处理
                 │                                    │
                 └────────── 无效/过期 ────────────────┘
                                   │
                                   ▼
                          返回 401/403
```

---

## 3. 数据模型设计

### 3.1 用户模型 (User)

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | String (UUID) | 用户唯一标识 | 主键 |
| username | String | 用户名 | 唯一，非空 |
| password_hash | String | 密码哈希 | 非空 |
| email | String | 邮箱 | 可选 |
| phone | String | 手机号 | 可选 |
| role | String | 用户角色 | admin/user |
| status | String | 状态 | active/inactive/locked |
| created_at | DateTime | 创建时间 | 非空 |
| updated_at | DateTime | 更新时间 | 非空 |

### 3.2 角色模型 (Role)

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | String | 角色标识 | 主键 |
| name | String | 角色名称 | 唯一 |
| description | String | 角色描述 | 可选 |
| permissions | JSON | 权限列表 | 非空 |

### 3.3 权限模型 (Permission)

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | String | 权限标识 | 主键 |
| name | String | 权限名称 | 唯一 |
| resource | String | 资源类型 | kv/s3/system/alert |
| action | String | 操作类型 | read/write/delete/admin |

### 3.4 S3 AccessKey 模型 (S3AccessKey)

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | String (UUID) | 记录ID | 主键 |
| user_id | String | 所属用户ID | 外键 |
| access_key | String | AccessKey | 唯一，非空 |
| secret_key_hash | String | SecretKey哈希 | 非空 |
| status | String | 状态 | active/inactive |
| created_at | DateTime | 创建时间 | 非空 |
| last_used_at | DateTime | 最后使用时间 | 可选 |

### 3.5 资源归属模型 (ResourceOwner)

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | String (UUID) | 记录ID | 主键 |
| user_id | String | 用户ID | 外键 |
| resource_type | String | 资源类型 | kv_namespace/s3_bucket |
| resource_id | String | 资源标识 | 非空 |
| permissions | JSON | 权限列表 | 非空 |

---

## 4. 权限模型设计

### 4.1 角色定义

| 角色 | 说明 | 权限范围 |
|------|------|---------|
| admin | 系统管理员 | 所有资源的全部权限 |
| user | 普通用户 | 仅自己创建的资源的读写权限 |

### 4.2 权限矩阵

| 资源类型 | 操作 | Admin | User (Own) | User (Other) |
|---------|------|-------|------------|--------------|
| 用户管理 | 查看/创建/编辑/删除 | ✅ | ❌ | ❌ |
| 角色管理 | 查看/创建/编辑/删除 | ✅ | ❌ | ❌ |
| KV 命名空间 | 创建 | ✅ | ✅ | ❌ |
| KV 命名空间 | 查看 | ✅ | ✅ | ❌ |
| KV 数据 | 读写 | ✅ | ✅ | ❌ |
| S3 Bucket | 创建 | ✅ | ✅ | ❌ |
| S3 Bucket | 查看 | ✅ | ✅ | ❌ |
| S3 对象 | 读写 | ✅ | ✅ | ❌ |
| S3 AccessKey | 创建/管理 | ✅ | ✅(自己) | ❌ |
| 系统监控 | 查看 | ✅ | ❌ | ❌ |
| 告警 | 查看 | ✅ | ✅(自己) | ❌ |
| FUSE 挂载 | 管理 | ✅ | ❌ | ❌ |

---

## 5. API 改造方案

### 5.1 新增认证 API

| API 路径 | 方法 | 说明 | 认证要求 |
|----------|------|------|---------|
| `/api/auth/login` | POST | 用户登录 | 无需 |
| `/api/auth/logout` | POST | 用户登出 | JWT |
| `/api/auth/refresh` | POST | 刷新 Token | RefreshToken |
| `/api/auth/register` | POST | 用户注册 | Admin |

### 5.2 新增用户管理 API

| API 路径 | 方法 | 说明 | 认证要求 |
|----------|------|------|---------|
| `/api/users` | GET | 获取用户列表 | Admin |
| `/api/users/:id` | GET | 获取用户详情 | Admin/自己 |
| `/api/users` | POST | 创建用户 | Admin |
| `/api/users/:id` | PUT | 更新用户 | Admin/自己 |
| `/api/users/:id` | DELETE | 删除用户 | Admin |

### 5.3 新增角色管理 API

| API 路径 | 方法 | 说明 | 认证要求 |
|----------|------|------|---------|
| `/api/roles` | GET | 获取角色列表 | Admin |
| `/api/roles/:id` | GET | 获取角色详情 | Admin |
| `/api/roles` | POST | 创建角色 | Admin |
| `/api/roles/:id` | PUT | 更新角色 | Admin |
| `/api/roles/:id` | DELETE | 删除角色 | Admin |

### 5.4 S3 AccessKey API 改造

| API 路径 | 方法 | 说明 | 认证要求 |
|----------|------|------|---------|
| `/api/s3/keys` | GET | 获取自己的 AccessKey | JWT |
| `/api/s3/keys` | POST | 创建 AccessKey | JWT |
| `/api/s3/keys/:id` | DELETE | 删除 AccessKey | JWT |

### 5.5 KV API 改造

| API 路径 | 方法 | 说明 | 认证要求 |
|----------|------|------|---------|
| `/api/kv/namespaces` | GET | 获取自己的命名空间 | JWT |
| `/api/kv/namespaces` | POST | 创建命名空间 | JWT |
| `/api/kv/namespaces/:name` | DELETE | 删除命名空间 | JWT |

### 5.6 现有 API 认证要求变更

| API 路径 | 原认证 | 新认证 |
|----------|--------|--------|
| `/api/metrics/cluster` | 无 | Admin |
| `/api/metrics/nodes` | 无 | Admin |
| `/api/metrics/volumes` | 无 | Admin |
| `/api/metrics/kv` | 无 | Admin/自己 |
| `/api/metrics/s3` | 无 | Admin/自己 |
| `/api/s3/buckets` | 无 | JWT |
| `/api/alerts` | 无 | Admin/自己 |
| `/api/fuse/mounts` | 无 | Admin |

---

## 6. 核心模块改造

### 6.1 Monitor 服务改造

#### 6.1.1 新增认证中间件

```rust
pub struct AuthMiddleware;

impl<S> tower::Layer<S> for AuthMiddleware {
    type Service = AuthService<S>;
    
    fn layer(&self, service: S) -> Self::Service {
        AuthService { inner: service }
    }
}

pub struct AuthService<S> {
    inner: S,
}
```

#### 6.1.2 JWT 验证

```rust
pub struct JwtValidator {
    secret: String,
}

impl JwtValidator {
    pub fn validate(&self, token: &str) -> Result<Claims> {
        // 验证 JWT 签名
        // 解析用户信息
    }
}
```

### 6.2 S3 Gateway 改造

#### 6.2.1 多用户 AccessKey 支持

```rust
pub struct AccessKeyManager {
    db: sled::Db,
}

impl AccessKeyManager {
    pub async fn get_user_id(&self, access_key: &str) -> Option<String> {
        // 根据 access_key 查询用户 ID
    }
    
    pub async fn validate_key(&self, access_key: &str, secret_key: &str) -> bool {
        // 验证 AccessKey 和 SecretKey
    }
}
```

#### 6.2.2 资源归属检查

```rust
pub async fn check_bucket_owner(
    user_id: &str,
    bucket_name: &str,
    directory_tree: &dyn DirectoryTreeApi,
) -> bool {
    // 检查用户是否拥有该 bucket
}
```

### 6.3 KV 模块改造

#### 6.3.1 命名空间隔离

```rust
pub struct KVNamespaceManager {
    db: sled::Db,
}

impl KVNamespaceManager {
    pub async fn create_namespace(&self, user_id: &str, name: &str) -> Result<()> {
        // 创建用户专属命名空间
    }
    
    pub async fn get_namespaces(&self, user_id: &str) -> Vec<String> {
        // 获取用户的所有命名空间
    }
}
```

### 6.4 DirectoryTree 改造

#### 6.4.1 Entry 增加 owner 字段

```protobuf
message Entry {
    string name = 1;
    string directory = 2;
    FuseAttributes attributes = 3;
    repeated FileChunk chunks = 4;
    string hard_link_id = 5;
    int32 hard_link_counter = 6;
    map<string, bytes> extended = 7;
    int64 content_size = 8;
    int64 disk_size = 9;
    string ttl = 10;
    string symlink_target = 11;
    string owner = 12;  // 新增：资源归属用户ID
}
```

### 6.5 数据存储方案

#### 6.5.1 存储位置选择

**决策：使用独立的 RocksDB 实例存储认证数据**

| 数据类型 | 存储位置 | 说明 |
|----------|---------|------|
| User/Role/Permission | Master 的独立 RocksDB 实例 | 独立 DB 路径 `/data/master/auth.db`，通过 Raft 同步 |
| S3AccessKey | Master 的独立 RocksDB 实例 | 与 User 数据一同存储，独立备份 |
| ResourceOwner | Master 的独立 RocksDB 实例 | 资源归属关系，支持权限查询 |
| JWT Token | Redis | 黑名单存储，用于 Token 强制失效 |

**独立 RocksDB 实例设计：**
- **独立 DB 路径**：`/data/master/auth.db`（与现有 `/data/master/db` 分离）
- **独立 Column Family**：使用独立的 CF 存储认证数据
- **独立备份策略**：认证数据需要更频繁的备份（每小时）
- **独立 Raft 配置**：认证变更使用更高优先级的 Raft 配置

#### 6.5.2 Monitor 与 S3 Gateway 认证传递机制

**已确认：方案A（最终决策）**

```
前端 ──→ Monitor (JWT) ──→ 查询用户的 S3 AccessKey ──→ S3 Gateway (SigV4)
```

**流程说明：**
1. 前端携带 JWT 调用 Monitor API
2. Monitor 解析 JWT 获取用户 ID
3. Monitor **实时查询** Master 获取该用户的 S3 AccessKey（不缓存）
4. Monitor 使用用户的 AccessKey 进行 SigV4 签名
5. Monitor 将签名后的请求转发给 S3 Gateway
6. S3 Gateway 验证 SigV4 签名，确认用户身份和权限

**设计要点：**
- **不缓存 AccessKey**：每次请求实时查询，避免 AccessKey 被禁用后缓存失效问题
- **AccessKey 状态检查**：查询时同时检查 AccessKey 是否为 active 状态
- **故障降级**：AccessKey 查询失败时返回 500 错误，不使用缓存数据

**优势：**
- S3 Gateway 无需修改认证方式，继续使用 SigV4
- 用户可以通过 AWS CLI/SDK 直接访问 S3
- 权限检查在 S3 Gateway 端统一执行
- AccessKey 变更实时生效，无缓存延迟

### 6.6 Proto 兼容性说明

#### 6.6.1 向后兼容策略

由于 Protobuf 支持字段追加（backward compatible），增加 `owner` 字段不会影响现有客户端：

| 场景 | 兼容性 | 说明 |
|------|--------|------|
| 旧客户端写入 | 兼容 | 新字段默认空字符串 |
| 新客户端读取旧数据 | 兼容 | owner 字段为空，视为无归属 |
| 新客户端写入 | 兼容 | 新字段正常写入 |
| 旧客户端读取新数据 | 兼容 | 忽略新增字段 |

#### 6.6.2 迁移步骤

1. **编译新 Proto**：更新 `proto/powerfs.proto`，增加 owner 字段
2. **重新生成代码**：运行 `cargo build` 自动重新生成 gRPC 代码
3. **部署新服务**：先部署 Master，再部署 S3 Gateway 和 Volume
4. **数据迁移**：批量更新现有 Entry 的 owner 字段为 admin
5. **验证**：确保旧客户端仍能正常工作

### 6.7 存储专家审查

#### 6.7.1 密码安全

**推荐方案：使用 Argon2id 哈希算法**

| 项目 | 要求 |
|------|------|
| 算法 | Argon2id（拒绝 bcrypt） |
| 内存成本 | 至少 64MB |
| 时间成本 | 至少 3 次迭代 |
| 并行度 | 至少 4 线程 |

**安全理由：**
- Argon2id 是 2015 年 Password Hashing Competition 获胜者
- 相比 bcrypt，更能抵抗 GPU/ASIC 暴力破解
- 支持可配置的内存和时间成本，便于未来调整

#### 6.7.2 S3 AccessKey 安全

**SecretKey 存储策略：使用 HMAC-SHA256 哈希**

| 字段 | 存储方式 | 说明 |
|------|---------|------|
| access_key | 明文存储 | 需要用于查询和展示 |
| secret_key | HMAC-SHA256 哈希 | 永远不存储明文 |

**验证流程：**
```
用户输入 secret_key ──→ HMAC-SHA256(secret_key, salt) ──→ 与存储的哈希比较
```

**安全理由：**
- 如果数据库泄露，攻击者无法获取有效的 secret_key
- 与 S3 原生认证保持一致（AWS 也不存储明文 secret）

#### 6.7.3 Monitor AccessKey 查询策略

**强制要求：不缓存，实时查询**

| 场景 | 处理方式 |
|------|---------|
| 正常请求 | 每次实时查询 Master |
| AccessKey 被禁用 | 立即生效，无缓存延迟 |
| AccessKey 被删除 | 立即生效，返回 403 |
| Master 查询失败 | 返回 500 错误，不使用缓存 |

**安全理由：**
- 避免攻击者在 AccessKey 被禁用后仍能通过缓存访问资源
- 确保权限变更实时生效

#### 6.7.4 独立 RocksDB 备份策略

| 项目 | 配置 |
|------|------|
| 备份频率 | 每小时增量备份，每日全量备份 |
| 备份保留 | 保留 30 天备份 |
| 备份加密 | 使用 AES-256-GCM 加密备份文件 |
| 备份验证 | 每次备份后验证数据完整性 |

**备份路径：**
```
/data/master/auth_backups/
├── daily/          # 每日全量备份
│   └── auth.db-2026-07-07.tar.gz
└── hourly/         # 每小时增量备份
    └── auth.db-2026-07-07-1400.diff
```

#### 6.7.5 登录速率限制

**配置要求：**

| 限制项 | 阈值 |
|--------|------|
| 单 IP 每分钟尝试次数 | 最多 10 次 |
| 单用户每分钟尝试次数 | 最多 5 次 |
| 锁定时间 | 连续失败 5 次后锁定 15 分钟 |
| 全局每分钟尝试次数 | 最多 1000 次 |

**实现方式：**
- 使用 Redis 作为速率限制计数器
- 每次登录失败增加计数器
- 达到阈值后返回 429 错误

#### 6.7.6 JWT 黑名单 Redis TTL 策略

| 项目 | 配置 |
|------|------|
| Token 过期时间 | 15 分钟 |
| RefreshToken 过期时间 | 7 天 |
| Redis 黑名单 TTL | 与 Token 过期时间一致（15 分钟） |
| 强制登出 TTL | 7 天（覆盖 RefreshToken 有效期） |

**设计理由：**
- 正常过期的 Token 自动从 Redis 清理，无需手动删除
- 强制登出需要保留更长时间，防止旧 RefreshToken 被滥用

---

## 7. 前端改造方案

### 7.1 新增页面

| 页面路径 | 说明 | 访问权限 |
|----------|------|---------|
| `/login` | 登录页面 | 公开 |
| `/users` | 用户管理页面 | Admin |
| `/roles` | 角色管理页面 | Admin |

### 7.2 页面改造

| 页面路径 | 改造内容 |
|----------|---------|
| `/dashboard` | 管理员查看完整系统信息，普通用户查看个人资源概览 |
| `/s3` | 只显示当前用户的 bucket |
| `/kv` | 只显示当前用户的命名空间 |
| `/alerts` | 管理员查看所有告警，普通用户查看自己的告警 |

### 7.3 登录状态管理

```typescript
interface UserInfo {
    id: string;
    username: string;
    role: 'admin' | 'user';
    permissions: string[];
}

interface AuthState {
    user: UserInfo | null;
    token: string;
    refreshToken: string;
    isAuthenticated: boolean;
}
```

---

## 8. 分阶段实施计划

### Phase 1：认证体系搭建（1-2周）

| 任务 | 说明 | 负责人 |
|------|------|--------|
| T1-01 | 实现 JWT Token 生成和验证 | 后端 |
| T1-02 | 实现用户登录/登出 API | 后端 |
| T1-03 | 实现用户注册/管理 API | 后端 |
| T1-04 | 实现认证中间件 | 后端 |
| T1-05 | 前端登录页面开发 | 前端 |
| T1-06 | 前端路由守卫实现 | 前端 |

### Phase 2：资源归属与基础权限（2-3周）

| 任务 | 说明 | 负责人 |
|------|------|--------|
| T2-01 | Entry 增加 owner 字段 | 后端 |
| T2-02 | S3 Bucket 创建时记录 owner | 后端 |
| T2-03 | KV 命名空间增加 owner | 后端 |
| T2-04 | S3 列表接口过滤用户自己的 bucket | 后端 |
| T2-05 | KV 列表接口过滤用户自己的命名空间 | 后端 |
| T2-06 | 告警增加 owner 字段 | 后端 |

### Phase 3：RBAC 权限细化（1-2周）

| 任务 | 说明 | 负责人 |
|------|------|--------|
| T3-01 | 实现角色管理 API | 后端 |
| T3-02 | 实现权限检查中间件 | 后端 |
| T3-03 | S3 AccessKey 多用户管理 | 后端 |
| T3-04 | 管理员/普通用户界面区分 | 前端 |
| T3-05 | 用户管理页面开发 | 前端 |

### Phase 4：测试与优化（1周）

| 任务 | 说明 | 负责人 |
|------|------|--------|
| T4-01 | 权限边界测试 | 测试 |
| T4-02 | 认证安全测试 | 测试 |
| T4-03 | 性能优化 | 后端 |
| T4-04 | Bug 修复 | 全组 |

---

## 9. 数据迁移方案

### 9.1 现有数据处理

| 数据类型 | 处理方式 |
|----------|---------|
| 现有 Bucket | 默认归属 admin 用户 |
| 现有 KV 数据 | 默认归属 admin 用户 |
| 现有告警 | 默认归属 admin 用户 |
| 现有 AccessKey | 转换为 admin 用户的 AccessKey |

### 9.2 迁移步骤

1. **备份数据**：迁移前备份所有数据库
2. **添加字段**：为 Entry、KV 元数据、告警表添加 owner 字段
3. **批量更新**：将现有数据的 owner 设置为 admin
4. **验证**：验证迁移后数据完整性
5. **切换**：启用权限检查逻辑

---

## 10. 安全性考虑

### 10.1 密码安全

- 使用 bcrypt 或 Argon2 哈希算法
- 禁止明文存储密码
- 强制密码复杂度要求

### 10.2 Token 安全

- JWT 设置合理过期时间（建议 15-30 分钟）
- RefreshToken 存储在安全的 HTTP-only Cookie 中
- Token 泄露检测和自动失效机制

### 10.3 传输安全

- API 强制 HTTPS
- 敏感数据传输加密
- CSRF 防护

### 10.4 权限安全

- 最小权限原则
- 权限变更审计日志
- 定期权限审查机制

---

## 11. 验收标准

### 11.1 功能验收

| 验收项 | 验收标准 |
|--------|---------|
| 用户登录 | 正确用户名密码登录成功，错误提示明确 |
| 用户认证 | 未登录访问受保护页面跳转到登录页 |
| 用户管理 | 管理员可以创建、编辑、删除用户 |
| 角色权限 | 普通用户无法访问管理员页面 |
| S3 隔离 | 用户只能看到自己的 bucket |
| KV 隔离 | 用户只能访问自己的 KV 数据 |
| 告警隔离 | 用户只能看到自己的告警 |

### 11.2 安全验收

| 验收项 | 验收标准 |
|--------|---------|
| 密码安全 | 数据库中无明文密码 |
| Token 安全 | Token 过期后自动失效 |
| 权限绕过 | 无法通过修改请求参数绕过权限检查 |
| 注入攻击 | API 能抵御 SQL 注入等攻击 |

---

*文档版本：v1.0*  
*创建日期：2026-07-07*  
*最后更新：2026-07-07*
