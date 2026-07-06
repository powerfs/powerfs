use crossbeam::sync::ShardedLock;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub expires_at: Option<std::time::SystemTime>,
}

#[derive(Debug, Clone)]
pub struct AuthManager {
    credentials: Arc<ShardedLock<HashMap<String, Credentials>>>,
}

impl AuthManager {
    pub fn new() -> Self {
        AuthManager {
            credentials: Arc::new(ShardedLock::new(HashMap::new())),
        }
    }

    pub fn with_default_credentials(access_key: &str, secret_key: &str) -> Self {
        let manager = Self::new();
        manager.add_credentials(access_key, secret_key);
        manager
    }

    pub fn add_credentials(&self, access_key: &str, secret_key: &str) {
        self.add_credentials_with_expiry(access_key, secret_key, None);
    }

    pub fn add_credentials_with_expiry(
        &self,
        access_key: &str,
        secret_key: &str,
        expires_at: Option<std::time::SystemTime>,
    ) {
        self.credentials.write().unwrap().insert(
            access_key.to_string(),
            Credentials {
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                expires_at,
            },
        );
    }

    pub fn remove_credentials(&self, access_key: &str) {
        self.credentials.write().unwrap().remove(access_key);
    }

    pub fn get_credentials(&self, access_key: &str) -> Option<Credentials> {
        let creds = self.credentials.read().unwrap().get(access_key).cloned()?;

        if let Some(expires_at) = creds.expires_at {
            if std::time::SystemTime::now() > expires_at {
                return None;
            }
        }

        Some(creds)
    }

    pub fn load_from_env(&self) {
        if let Ok(access_key) = std::env::var("S3_ACCESS_KEY") {
            if let Ok(secret_key) = std::env::var("S3_SECRET_KEY") {
                self.add_credentials(&access_key, &secret_key);
            }
        }
    }

    pub fn load_from_map(&self, creds_map: &HashMap<String, String>) {
        for (access_key, secret_key) in creds_map {
            self.add_credentials(access_key, secret_key);
        }
    }

    pub fn list_access_keys(&self) -> Vec<String> {
        self.credentials.read().unwrap().keys().cloned().collect()
    }

    pub fn verify_sigv4(
        &self,
        auth_header: &str,
        method: &str,
        uri: &str,
        host: &str,
        date: &str,
        payload_hash: &str,
    ) -> bool {
        let parts: Vec<&str> = auth_header.split_whitespace().collect();
        if parts.len() != 2 || parts[0] != "AWS4-HMAC-SHA256" {
            return false;
        }

        let credential_parts: Vec<&str> = parts[1].split(',').collect();
        if credential_parts.len() < 2 {
            return false;
        }

        let access_key = extract_value(credential_parts[0], "Credential=");
        let signature = extract_value(credential_parts[1], "Signature=");

        let Some(creds) = self.get_credentials(&access_key) else {
            return false;
        };

        let signing_key = self.compute_signing_key(&creds.secret_key, date, "us-east-1", "s3");
        let string_to_sign = self.compute_string_to_sign(method, uri, host, date, payload_hash);

        let mut mac = HmacSha256::new_from_slice(&signing_key).unwrap();
        mac.update(string_to_sign.as_bytes());
        let computed_signature = hex::encode(mac.finalize().into_bytes());

        computed_signature == signature
    }

    fn compute_signing_key(
        &self,
        secret_key: &str,
        date: &str,
        region: &str,
        service: &str,
    ) -> Vec<u8> {
        let date_key = self.compute_hmac(format!("AWS4{}", secret_key).as_bytes(), date.as_bytes());
        let region_key = self.compute_hmac(&date_key, region.as_bytes());
        let service_key = self.compute_hmac(&region_key, service.as_bytes());
        self.compute_hmac(&service_key, "aws4_request".as_bytes())
    }

    fn compute_hmac(&self, key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    fn compute_string_to_sign(
        &self,
        method: &str,
        uri: &str,
        host: &str,
        date: &str,
        payload_hash: &str,
    ) -> String {
        format!(
            "AWS4-HMAC-SHA256\n{}\n{}/{}/s3/aws4_request\n{}\n{}\n{}",
            date,
            date.get(..8).unwrap_or(date),
            "us-east-1",
            self.compute_canonical_request(method, uri, host, date, payload_hash),
            hex::encode(
                self.compute_sha256(
                    self.compute_canonical_request(method, uri, host, date, payload_hash)
                        .as_bytes()
                )
            ),
            hex::encode(self.compute_sha256(payload_hash.as_bytes()))
        )
    }

    fn compute_canonical_request(
        &self,
        method: &str,
        uri: &str,
        host: &str,
        date: &str,
        payload_hash: &str,
    ) -> String {
        format!(
            "{} \n{}\n\nhost:{}\nx-amz-date:{}\n\nhost;x-amz-date\n{}",
            method, uri, host, date, payload_hash
        )
    }

    fn compute_sha256(&self, data: &[u8]) -> Vec<u8> {
        use sha2::Digest;
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}

fn extract_value(input: &str, prefix: &str) -> String {
    if let Some(value) = input.strip_prefix(prefix) {
        value.to_string()
    } else {
        input.to_string()
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_get_credentials() {
        let manager = AuthManager::new();
        manager.add_credentials("AKIAEXAMPLE", "secret123");

        let creds = manager.get_credentials("AKIAEXAMPLE");
        assert!(creds.is_some());
        assert_eq!(creds.unwrap().secret_key, "secret123");
    }

    #[test]
    fn test_remove_credentials() {
        let manager = AuthManager::new();
        manager.add_credentials("AKIAEXAMPLE", "secret123");
        manager.remove_credentials("AKIAEXAMPLE");

        let creds = manager.get_credentials("AKIAEXAMPLE");
        assert!(creds.is_none());
    }

    #[test]
    fn test_compute_hmac() {
        let manager = AuthManager::new();
        let key = b"test-key";
        let data = b"test-data";

        let result = manager.compute_hmac(key, data);
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_compute_sha256() {
        let manager = AuthManager::new();
        let data = b"test-data";

        let result = manager.compute_sha256(data);
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_extract_value() {
        let value = extract_value("Credential=AKIAEXAMPLE", "Credential=");
        assert_eq!(value, "AKIAEXAMPLE");

        let value = extract_value("Signature=abc123", "Signature=");
        assert_eq!(value, "abc123");
    }

    #[test]
    fn test_with_default_credentials() {
        let manager = AuthManager::with_default_credentials("AKIADEFAULT", "default-secret");

        let creds = manager.get_credentials("AKIADEFAULT");
        assert!(creds.is_some());
        assert_eq!(creds.unwrap().secret_key, "default-secret");
    }

    #[test]
    fn test_expired_credentials() {
        let manager = AuthManager::new();
        let expired_time = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
        manager.add_credentials_with_expiry("AKIAEXPIRED", "secret123", Some(expired_time));

        let creds = manager.get_credentials("AKIAEXPIRED");
        assert!(creds.is_none());
    }

    #[test]
    fn test_non_expired_credentials() {
        let manager = AuthManager::new();
        let future_time = std::time::SystemTime::now() + std::time::Duration::from_secs(3600);
        manager.add_credentials_with_expiry("AKIAFUTURE", "secret123", Some(future_time));

        let creds = manager.get_credentials("AKIAFUTURE");
        assert!(creds.is_some());
        assert_eq!(creds.unwrap().secret_key, "secret123");
    }

    #[test]
    fn test_load_from_map() {
        let manager = AuthManager::new();
        let mut creds_map = HashMap::new();
        creds_map.insert("AKIAMAP1".to_string(), "secret1".to_string());
        creds_map.insert("AKIAMAP2".to_string(), "secret2".to_string());

        manager.load_from_map(&creds_map);

        assert!(manager.get_credentials("AKIAMAP1").is_some());
        assert!(manager.get_credentials("AKIAMAP2").is_some());
        assert_eq!(
            manager.get_credentials("AKIAMAP1").unwrap().secret_key,
            "secret1"
        );
    }

    #[test]
    fn test_list_access_keys() {
        let manager = AuthManager::new();
        manager.add_credentials("AKIAKEY1", "secret1");
        manager.add_credentials("AKIAKEY2", "secret2");

        let keys = manager.list_access_keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"AKIAKEY1".to_string()));
        assert!(keys.contains(&"AKIAKEY2".to_string()));
    }
}
