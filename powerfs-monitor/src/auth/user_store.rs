use argon2::password_hash::{
    rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
};
use argon2::{Algorithm, Argon2, Params, Version};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, DB};
use std::sync::Arc;

use super::user::{User, UserRole, UserStatus};

fn create_argon2() -> Argon2<'static> {
    let params = Params::new(65536, 3, 4, None).expect("Invalid Argon2 params");
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

const CF_USERS: &str = "users";
const CF_USERNAME_INDEX: &str = "username_index";
const CF_RESOURCE_OWNERS: &str = "resource_owners";
const CF_ROLES: &str = "roles";
const CF_ROLE_NAME_INDEX: &str = "role_name_index";
const CF_S3_KEYS: &str = "s3_keys";
const CF_S3_KEY_USER_INDEX: &str = "s3_key_user_index";

pub struct UserStore {
    db: Arc<DB>,
}

impl UserStore {
    pub fn new(path: &str) -> Result<Self, String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_users = ColumnFamilyDescriptor::new(CF_USERS, Options::default());
        let cf_index = ColumnFamilyDescriptor::new(CF_USERNAME_INDEX, Options::default());
        let cf_resource_owners =
            ColumnFamilyDescriptor::new(CF_RESOURCE_OWNERS, Options::default());
        let cf_roles = ColumnFamilyDescriptor::new(CF_ROLES, Options::default());
        let cf_role_index = ColumnFamilyDescriptor::new(CF_ROLE_NAME_INDEX, Options::default());
        let cf_s3_keys = ColumnFamilyDescriptor::new(CF_S3_KEYS, Options::default());
        let cf_s3_key_index = ColumnFamilyDescriptor::new(CF_S3_KEY_USER_INDEX, Options::default());

        let db = DB::open_cf_descriptors(
            &opts,
            path,
            vec![
                cf_users,
                cf_index,
                cf_resource_owners,
                cf_roles,
                cf_role_index,
                cf_s3_keys,
                cf_s3_key_index,
            ],
        )
        .map_err(|e| format!("Failed to open auth RocksDB at {}: {}", path, e))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// 返回内部 DB 句柄，供其他存储（如 ResourceOwnerStore）共享同一 RocksDB 实例
    pub fn db_handle(&self) -> Arc<DB> {
        self.db.clone()
    }

    /// 资源归属 CF 句柄
    pub fn cf_resource_owners(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_RESOURCE_OWNERS)
            .expect("resource_owners CF missing")
    }

    fn cf_users(&self) -> &ColumnFamily {
        self.db.cf_handle(CF_USERS).expect("users CF missing")
    }

    fn cf_index(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_USERNAME_INDEX)
            .expect("username_index CF missing")
    }

    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        role: UserRole,
    ) -> Result<User, String> {
        if self.get_user_by_username(username)?.is_some() {
            return Err("Username already exists".to_string());
        }

        let salt = SaltString::generate(&mut OsRng);
        let argon2 = create_argon2();
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| format!("Failed to hash password: {}", e))?
            .to_string();

        let user = User {
            id: uuid::Uuid::new_v4().to_string(),
            username: username.to_string(),
            password_hash,
            password_salt: salt.to_string(),
            email: None,
            phone: None,
            role,
            status: UserStatus::Active,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        self.save_user(&user)?;
        Ok(user)
    }

    pub fn save_user(&self, user: &User) -> Result<(), String> {
        let key = user.id.as_bytes();
        let value = serde_json::to_vec(user).map_err(|e| format!("Serialize error: {}", e))?;

        self.db
            .put_cf(self.cf_users(), key, &value)
            .map_err(|e| format!("RocksDB put error: {}", e))?;

        // update username index
        self.db
            .put_cf(
                self.cf_index(),
                user.username.as_bytes(),
                user.id.as_bytes(),
            )
            .map_err(|e| format!("RocksDB index put error: {}", e))?;

        Ok(())
    }

    pub fn get_user_by_id(&self, id: &str) -> Result<Option<User>, String> {
        match self.db.get_cf(self.cf_users(), id.as_bytes()) {
            Ok(Some(data)) => {
                let user: User = serde_json::from_slice(&data)
                    .map_err(|e| format!("Deserialize error: {}", e))?;
                Ok(Some(user))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(format!("RocksDB get error: {}", e)),
        }
    }

    pub fn get_user_by_username(&self, username: &str) -> Result<Option<User>, String> {
        match self.db.get_cf(self.cf_index(), username.as_bytes()) {
            Ok(Some(user_id_bytes)) => {
                let user_id = String::from_utf8(user_id_bytes)
                    .map_err(|e| format!("UTF-8 decode error: {}", e))?;
                self.get_user_by_id(&user_id)
            }
            Ok(None) => Ok(None),
            Err(e) => Err(format!("RocksDB index get error: {}", e)),
        }
    }

    pub fn list_users(&self) -> Result<Vec<User>, String> {
        let iter = self
            .db
            .iterator_cf(self.cf_users(), rocksdb::IteratorMode::Start);
        let mut users = Vec::new();

        for item in iter {
            let (_key, value) = item.map_err(|e| format!("Iterator error: {}", e))?;
            let user: User =
                serde_json::from_slice(&value).map_err(|e| format!("Deserialize error: {}", e))?;
            users.push(user);
        }

        users.sort_by_key(|a| a.created_at);
        Ok(users)
    }

    pub fn delete_user(&self, id: &str) -> Result<bool, String> {
        if let Some(user) = self.get_user_by_id(id)? {
            self.db
                .delete_cf(self.cf_users(), id.as_bytes())
                .map_err(|e| format!("RocksDB delete error: {}", e))?;
            self.db
                .delete_cf(self.cf_index(), user.username.as_bytes())
                .map_err(|e| format!("RocksDB index delete error: {}", e))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn verify_password(&self, user: &User, password: &str) -> bool {
        let parsed_hash = match PasswordHash::new(&user.password_hash) {
            Ok(h) => h,
            Err(_) => return false,
        };
        create_argon2()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    }

    pub fn update_password(&self, user_id: &str, new_password: &str) -> Result<(), String> {
        let mut user = self.get_user_by_id(user_id)?.ok_or("User not found")?;

        let salt = SaltString::generate(&mut OsRng);
        let argon2 = create_argon2();
        let password_hash = argon2
            .hash_password(new_password.as_bytes(), &salt)
            .map_err(|e| format!("Failed to hash password: {}", e))?
            .to_string();

        user.password_hash = password_hash;
        user.password_salt = salt.to_string();
        user.updated_at = chrono::Utc::now();
        self.save_user(&user)
    }

    pub fn update_user(
        &self,
        user_id: &str,
        email: Option<String>,
        phone: Option<String>,
        status: Option<UserStatus>,
        role: Option<UserRole>,
    ) -> Result<User, String> {
        let mut user = self.get_user_by_id(user_id)?.ok_or("User not found")?;

        if let Some(e) = email {
            user.email = Some(e);
        }
        if let Some(p) = phone {
            user.phone = Some(p);
        }
        if let Some(s) = status {
            user.status = s;
        }
        if let Some(r) = role {
            user.role = r;
        }
        user.updated_at = chrono::Utc::now();
        self.save_user(&user)?;
        Ok(user)
    }

    pub fn ensure_admin_exists(
        &self,
        admin_username: &str,
        admin_password: &str,
    ) -> Result<(), String> {
        if self.get_user_by_username(admin_username)?.is_none() {
            self.create_user(admin_username, admin_password, UserRole::Admin)?;
            log::info!("Created default admin user: {}", admin_username);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let store = UserStore::new(tmp.path().to_str().unwrap()).unwrap();

        // create
        let user = store
            .create_user("alice", "password123", UserRole::User)
            .unwrap();
        assert_eq!(user.username, "alice");

        // get by username
        let fetched = store.get_user_by_username("alice").unwrap().unwrap();
        assert_eq!(fetched.id, user.id);

        // verify password
        assert!(store.verify_password(&fetched, "password123"));
        assert!(!store.verify_password(&fetched, "wrong"));

        // list
        let users = store.list_users().unwrap();
        assert_eq!(users.len(), 1);

        // delete
        assert!(store.delete_user(&user.id).unwrap());
        assert!(store.get_user_by_username("alice").unwrap().is_none());
    }
}
