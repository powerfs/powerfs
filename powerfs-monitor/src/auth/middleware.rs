use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Extension,
};
use std::sync::Arc;

use super::jwt::JwtValidator;
use super::kv_access_key::KVAccessKeyStore;
use super::role::RoleStore;
use super::user_store::UserStore;
use super::{Claims, UserRole};

pub struct AuthState {
    pub validator: JwtValidator,
    pub user_store: Arc<UserStore>,
    /// 长效 API Key 存储（用于 Python SDK / Agent 访问 Monitor API）
    pub api_key_store: Arc<KVAccessKeyStore>,
    /// HMAC 密钥，用于验证 API Key 的 secret 部分
    pub hmac_secret: String,
}

#[derive(Debug, Clone)]
pub struct CurrentUser {
    pub id: String,
    pub username: String,
    pub role: UserRole,
}

impl From<Claims> for CurrentUser {
    fn from(c: Claims) -> Self {
        let role = c.role.parse().unwrap_or(UserRole::User);
        Self {
            id: c.sub,
            username: c.username,
            role,
        }
    }
}

impl CurrentUser {
    pub fn is_admin(&self) -> bool {
        self.role == UserRole::Admin
    }

    /// 角色名称（用于在 RoleStore 中查找对应 Role）
    pub fn role_name(&self) -> &'static str {
        match self.role {
            UserRole::Admin => "admin",
            UserRole::User => "user",
        }
    }

    /// 检查用户是否拥有指定权限
    /// admin 角色隐式拥有全部权限
    pub fn has_permission(&self, role_store: &RoleStore, resource: &str, action: &str) -> bool {
        if self.is_admin() {
            return true;
        }
        match role_store.get_role_by_name(self.role_name()) {
            Ok(Some(role)) => role.has_permission(resource, action),
            _ => false,
        }
    }
}

/// 认证中间件 - 验证 JWT 或长效 API Key 并注入 CurrentUser
///
/// 支持两种认证方式：
/// 1. JWT token（短期，15 分钟过期，适合前端用户会话）
/// 2. 长效 API Key（格式 `pak_<access_key>_<secret_key>`，适合 Python SDK / Agent）
///
/// 当 Bearer token 不是有效 JWT 时，尝试作为 API Key 解析。
pub async fn auth_middleware(
    State(auth_state): State<Arc<AuthState>>,
    mut req: Request<axum::body::Body>,
    next: Next<axum::body::Body>,
) -> Response {
    let auth_header = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok());

    let token = match auth_header {
        Some(h) if h.starts_with("Bearer ") => &h[7..],
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                "Missing or invalid Authorization header",
            )
                .into_response();
        }
    };

    // 优先尝试 JWT 验证
    match auth_state.validator.validate_access_token(token) {
        Ok(claims) => {
            let current_user = CurrentUser::from(claims);
            req.extensions_mut().insert(current_user);
            next.run(req).await
        }
        Err(_) => {
            // JWT 验证失败，尝试作为长效 API Key 验证
            match verify_api_key(&auth_state, token) {
                Some(current_user) => {
                    req.extensions_mut().insert(current_user);
                    next.run(req).await
                }
                None => (StatusCode::UNAUTHORIZED, "Invalid token or API key").into_response(),
            }
        }
    }
}

/// 验证长效 API Key
///
/// API Key 格式：`pak_<access_key>_<secret_key>`
/// - access_key: 32 字符 hex（16 字节）
/// - secret_key: 64 字符 hex（32 字节）
///
/// 返回对应的 CurrentUser（角色从用户存储中查找，默认 admin）
fn verify_api_key(auth_state: &AuthState, token: &str) -> Option<CurrentUser> {
    // 解析 `pak_<access_key>_<secret_key>` 格式
    let rest = token.strip_prefix("pak_")?;
    let parts: Vec<&str> = rest.splitn(2, '_').collect();
    if parts.len() != 2 {
        return None;
    }
    let access_key = parts[0];
    let secret_key = parts[1];

    // 查找 access_key
    let stored = auth_state
        .api_key_store
        .find_by_access_key(access_key)
        .ok()??;

    // 检查状态
    if !stored.is_active() {
        return None;
    }

    // 验证 secret_key
    let expected_hash = super::hash_secret_key(secret_key, &auth_state.hmac_secret);
    if stored.secret_key_hash != expected_hash {
        return None;
    }

    // 查找用户信息以获取角色
    let role = auth_state
        .user_store
        .get_user_by_id(&stored.user_id)
        .ok()
        .flatten()
        .map(|u| u.role)
        .unwrap_or(UserRole::Admin);

    Some(CurrentUser {
        id: stored.user_id.clone(),
        username: String::new(),
        role,
    })
}

/// 管理员权限检查
pub async fn require_admin(
    Extension(user): Extension<CurrentUser>,
    req: Request<axum::body::Body>,
    next: Next<axum::body::Body>,
) -> Response {
    if !user.is_admin() {
        return (StatusCode::FORBIDDEN, "Admin permission required").into_response();
    }
    next.run(req).await
}
