use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// 全局唯一的请求 ID
///
/// 用于保证请求的幂等性。在分布式环境下，通过 RequestId 和 ClientIdentity 的组合，
/// 可以确保每个请求都有唯一的标识，服务端可以据此进行去重和重试。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct RequestId(pub String);

impl RequestId {
    /// 创建新的唯一请求 ID
    pub fn new() -> Self {
        // 使用 UUID v4 生成全局唯一 ID
        Self(Uuid::new_v4().to_string())
    }

    /// 从现有字符串创建 RequestId
    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    /// 获取字符串表示
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_unique() {
        let id1 = RequestId::new();
        let id2 = RequestId::new();
        assert_ne!(id1, id2);
        assert_ne!(id1.as_str(), id2.as_str());
    }

    #[test]
    fn test_request_id_format() {
        let id = RequestId::new();
        // UUID v4 format: 8-4-4-4-12 hex chars
        assert_eq!(id.as_str().len(), 36);
        assert!(id.as_str().contains('-'));
    }

    #[test]
    fn test_request_id_from_string() {
        let id = RequestId::from_string("custom-id-123".to_string());
        assert_eq!(id.as_str(), "custom-id-123");
    }

    #[test]
    fn test_request_id_display() {
        let id = RequestId::from_string("test-id".to_string());
        assert_eq!(format!("{}", id), "test-id");
    }
}
