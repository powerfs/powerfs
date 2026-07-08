use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Extension,
};
use std::sync::Arc;

use super::jwt::JwtValidator;
use super::role::RoleStore;
use super::user_store::UserStore;
use super::{Claims, UserRole};

pub struct AuthState {
    pub validator: JwtValidator,
    pub user_store: Arc<UserStore>,
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

/// 认证中间件 - 验证 JWT 并注入 CurrentUser
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

    match auth_state.validator.validate_access_token(token) {
        Ok(claims) => {
            let current_user = CurrentUser::from(claims);
            req.extensions_mut().insert(current_user);
            next.run(req).await
        }
        Err(e) => (StatusCode::UNAUTHORIZED, e).into_response(),
    }
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
