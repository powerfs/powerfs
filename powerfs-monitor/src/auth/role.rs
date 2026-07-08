use rocksdb::{ColumnFamily, DB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// 权限定义：资源:操作 格式，如 "user:read", "s3:write", "system:admin"
/// 通配符 "*" 表示全部权限
pub fn build_permission(resource: &str, action: &str) -> String {
    format!("{}:{}", resource, action)
}

/// 角色模型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub id: String,
    pub name: String,
    pub description: String,
    /// 权限列表，如 ["user:read", "s3:write"]；"*" 表示全部权限
    pub permissions: Vec<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl Role {
    pub fn new(name: &str, description: &str, permissions: Vec<String>) -> Self {
        let now = chrono::Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
            description: description.to_string(),
            permissions,
            created_at: now,
            updated_at: now,
        }
    }

    /// 检查角色是否拥有指定权限
    /// - "*" 通配符拥有全部权限
    /// - "resource:*" 拥有该资源的全部操作权限
    /// - "resource:action" 精确匹配
    pub fn has_permission(&self, resource: &str, action: &str) -> bool {
        let target = build_permission(resource, action);
        for p in &self.permissions {
            if p == "*" || p == &target {
                return true;
            }
            // resource:* 形式
            if let Some(res) = p.strip_suffix(":*") {
                if res == resource {
                    return true;
                }
            }
        }
        false
    }

    /// 是否为超级权限（拥有全部权限）
    pub fn is_super(&self) -> bool {
        self.permissions.iter().any(|p| p == "*")
    }
}

const CF_ROLES: &str = "roles";
const CF_ROLE_NAME_INDEX: &str = "role_name_index";

/// 角色存储，与 UserStore 共享同一个 auth.db RocksDB 实例
pub struct RoleStore {
    db: Arc<DB>,
}

impl RoleStore {
    pub fn from_user_store(user_store: &crate::auth::UserStore) -> Self {
        Self {
            db: user_store.db_handle(),
        }
    }

    fn cf_roles(&self) -> &ColumnFamily {
        self.db.cf_handle(CF_ROLES).expect("roles CF missing")
    }

    fn cf_name_index(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_ROLE_NAME_INDEX)
            .expect("role_name_index CF missing")
    }

    pub fn create_role(
        &self,
        name: &str,
        description: &str,
        permissions: Vec<String>,
    ) -> Result<Role, String> {
        if self.get_role_by_name(name)?.is_some() {
            return Err("Role name already exists".to_string());
        }
        let role = Role::new(name, description, permissions);
        self.save_role(&role)?;
        Ok(role)
    }

    pub fn save_role(&self, role: &Role) -> Result<(), String> {
        let key = role.id.as_bytes();
        let value = serde_json::to_vec(role).map_err(|e| format!("Serialize error: {}", e))?;

        self.db
            .put_cf(self.cf_roles(), key, &value)
            .map_err(|e| format!("RocksDB put error: {}", e))?;

        // 更新名称索引
        self.db
            .put_cf(
                self.cf_name_index(),
                role.name.as_bytes(),
                role.id.as_bytes(),
            )
            .map_err(|e| format!("RocksDB index put error: {}", e))?;

        Ok(())
    }

    pub fn get_role_by_id(&self, id: &str) -> Result<Option<Role>, String> {
        match self.db.get_cf(self.cf_roles(), id.as_bytes()) {
            Ok(Some(data)) => {
                let role: Role = serde_json::from_slice(&data)
                    .map_err(|e| format!("Deserialize error: {}", e))?;
                Ok(Some(role))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(format!("RocksDB get error: {}", e)),
        }
    }

    pub fn get_role_by_name(&self, name: &str) -> Result<Option<Role>, String> {
        match self.db.get_cf(self.cf_name_index(), name.as_bytes()) {
            Ok(Some(id_bytes)) => {
                let id = String::from_utf8(id_bytes)
                    .map_err(|e| format!("UTF-8 decode error: {}", e))?;
                self.get_role_by_id(&id)
            }
            Ok(None) => Ok(None),
            Err(e) => Err(format!("RocksDB index get error: {}", e)),
        }
    }

    pub fn list_roles(&self) -> Result<Vec<Role>, String> {
        let iter = self
            .db
            .iterator_cf(self.cf_roles(), rocksdb::IteratorMode::Start);
        let mut roles = Vec::new();
        for item in iter {
            let (_key, value) = item.map_err(|e| format!("Iterator error: {}", e))?;
            let role: Role =
                serde_json::from_slice(&value).map_err(|e| format!("Deserialize error: {}", e))?;
            roles.push(role);
        }
        roles.sort_by_key(|r| r.created_at);
        Ok(roles)
    }

    pub fn update_role(
        &self,
        id: &str,
        name: Option<String>,
        description: Option<String>,
        permissions: Option<Vec<String>>,
    ) -> Result<Role, String> {
        let mut role = self.get_role_by_id(id)?.ok_or("Role not found")?;

        if let Some(n) = name {
            // 检查名称唯一性
            if let Some(existing) = self.get_role_by_name(&n)? {
                if existing.id != role.id {
                    return Err("Role name already in use".to_string());
                }
            }
            // 删除旧名称索引
            self.db
                .delete_cf(self.cf_name_index(), role.name.as_bytes())
                .map_err(|e| format!("RocksDB index delete error: {}", e))?;
            role.name = n;
        }
        if let Some(d) = description {
            role.description = d;
        }
        if let Some(p) = permissions {
            role.permissions = p;
        }
        role.updated_at = chrono::Utc::now();
        self.save_role(&role)?;
        Ok(role)
    }

    pub fn delete_role(&self, id: &str) -> Result<bool, String> {
        let role = self.get_role_by_id(id)?;
        match role {
            Some(role) => {
                self.db
                    .delete_cf(self.cf_roles(), id.as_bytes())
                    .map_err(|e| format!("RocksDB delete error: {}", e))?;
                self.db
                    .delete_cf(self.cf_name_index(), role.name.as_bytes())
                    .map_err(|e| format!("RocksDB index delete error: {}", e))?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// 内置默认角色：admin 和 user
    pub fn ensure_default_roles(&self) -> Result<(), String> {
        if self.get_role_by_name("admin")?.is_none() {
            let admin = Role::new("admin", "系统管理员，拥有全部权限", vec!["*".to_string()]);
            self.save_role(&admin)?;
            log::info!("Created default admin role");
        }
        if self.get_role_by_name("user")?.is_none() {
            let user = Role::new(
                "user",
                "普通用户，仅拥有自己资源的读写权限",
                vec![
                    "s3:read".to_string(),
                    "s3:write".to_string(),
                    "s3:delete".to_string(),
                    "kv:read".to_string(),
                    "kv:write".to_string(),
                    "alert:read".to_string(),
                ],
            );
            self.save_role(&user)?;
            log::info!("Created default user role");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::user_store::UserStore;

    #[test]
    fn test_role_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let store = UserStore::new(tmp.path().to_str().unwrap()).unwrap();
        let role_store = RoleStore::from_user_store(&store);

        // create
        let role = role_store
            .create_role(
                "viewer",
                "只读角色",
                vec!["s3:read".to_string(), "kv:read".to_string()],
            )
            .unwrap();
        assert_eq!(role.name, "viewer");

        // get by id
        let fetched = role_store.get_role_by_id(&role.id).unwrap().unwrap();
        assert_eq!(fetched.name, "viewer");
        assert_eq!(fetched.permissions.len(), 2);

        // get by name
        let by_name = role_store.get_role_by_name("viewer").unwrap().unwrap();
        assert_eq!(by_name.id, role.id);

        // duplicate name
        assert!(role_store.create_role("viewer", "dup", vec![]).is_err());

        // list
        let roles = role_store.list_roles().unwrap();
        assert_eq!(roles.len(), 1);

        // update
        let updated = role_store
            .update_role(
                &role.id,
                Some("viewer2".to_string()),
                Some("updated".to_string()),
                Some(vec!["s3:read".to_string()]),
            )
            .unwrap();
        assert_eq!(updated.name, "viewer2");
        assert_eq!(updated.description, "updated");

        // permission check
        assert!(updated.has_permission("s3", "read"));
        assert!(!updated.has_permission("s3", "write"));
        assert!(!updated.has_permission("kv", "read"));

        // delete
        assert!(role_store.delete_role(&role.id).unwrap());
        assert!(role_store.get_role_by_id(&role.id).unwrap().is_none());
    }

    #[test]
    fn test_default_roles() {
        let tmp = tempfile::tempdir().unwrap();
        let store = UserStore::new(tmp.path().to_str().unwrap()).unwrap();
        let role_store = RoleStore::from_user_store(&store);

        role_store.ensure_default_roles().unwrap();

        let admin = role_store.get_role_by_name("admin").unwrap().unwrap();
        assert!(admin.is_super());
        assert!(admin.has_permission("anything", "whatever"));

        let user = role_store.get_role_by_name("user").unwrap().unwrap();
        assert!(!user.is_super());
        assert!(user.has_permission("s3", "read"));
        assert!(!user.has_permission("user", "read"));

        // 幂等
        role_store.ensure_default_roles().unwrap();
        let roles = role_store.list_roles().unwrap();
        assert_eq!(roles.len(), 2);
    }

    #[test]
    fn test_permission_wildcard() {
        let role = Role::new("test", "", vec!["s3:*".to_string()]);
        assert!(role.has_permission("s3", "read"));
        assert!(role.has_permission("s3", "write"));
        assert!(role.has_permission("s3", "delete"));
        assert!(!role.has_permission("kv", "read"));
    }
}
