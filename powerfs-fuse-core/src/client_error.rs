/// 客户端错误类型
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClientError {
    /// 网络层错误
    #[error("Network error: {0}")]
    Network(String),

    /// 服务端错误
    #[error("Server error: {0}")]
    Server(String),

    /// 请求超时
    #[error("Request timeout after {0:?}")]
    Timeout(std::time::Duration),

    /// 熔断器已打开
    #[error("Circuit breaker is open")]
    CircuitOpen,

    /// 客户端未就绪
    #[error("Client not ready: {0:?}")]
    ClientNotReady(String),

    /// 分片 Leader 未找到
    #[error("No leader for shard {0}")]
    NoShardLeader(u64),

    /// Volume 未找到
    #[error("Volume {0} not found")]
    VolumeNotFound(u64),

    /// 无效的请求类型
    #[error("Unsupported request kind: {0}")]
    UnsupportedRequest(String),

    /// 请求已取消
    #[error("Request cancelled")]
    Cancelled,

    /// 无有效 Lease
    #[error("No valid lease for write request")]
    NoValidLease,

    /// 队列已满
    #[error("Request queue is full (capacity: {0})")]
    QueueFull(usize),

    /// 无效的地址格式
    #[error("Invalid address format: {0}")]
    InvalidAddress(String),

    /// 内部错误
    #[error("Internal error: {0}")]
    Internal(String),
}

impl ClientError {
    /// 从 powerfs_net 的 NetError 转换
    pub fn from_net_error(e: powerfs_net::NetError) -> Self {
        match e {
            powerfs_net::NetError::Timeout => {
                ClientError::Timeout(std::time::Duration::from_secs(5))
            }
            powerfs_net::NetError::ServerError(msg) => ClientError::Server(msg),
            powerfs_net::NetError::Connection(msg) => ClientError::Network(msg),
            powerfs_net::NetError::InvalidResponse(msg) => ClientError::Internal(msg),
            _ => ClientError::Network(format!("Unknown net error: {}", e)),
        }
    }

    /// 判断是否为可重试错误
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ClientError::Timeout(_)
                | ClientError::Network(_)
                | ClientError::CircuitOpen
                | ClientError::NoShardLeader(_)
                | ClientError::VolumeNotFound(_)
        )
    }
}

impl From<powerfs_net::NetError> for ClientError {
    fn from(e: powerfs_net::NetError) -> Self {
        Self::from_net_error(e)
    }
}

/// 客户端 Result 类型
pub type ClientResult<T> = Result<T, ClientError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ClientError::Network("connection refused".to_string());
        assert_eq!(format!("{}", err), "Network error: connection refused");

        let err = ClientError::Timeout(std::time::Duration::from_secs(10));
        assert!(format!("{}", err).contains("timeout"));

        let err = ClientError::CircuitOpen;
        assert_eq!(format!("{}", err), "Circuit breaker is open");
    }

    #[test]
    fn test_retryable_errors() {
        assert!(ClientError::Timeout(std::time::Duration::from_secs(5)).is_retryable());
        assert!(ClientError::Network("test".to_string()).is_retryable());
        assert!(ClientError::CircuitOpen.is_retryable());
        assert!(ClientError::NoShardLeader(1).is_retryable());
        assert!(ClientError::VolumeNotFound(1).is_retryable());

        assert!(!ClientError::Server("test".to_string()).is_retryable());
        assert!(!ClientError::UnsupportedRequest("test".to_string()).is_retryable());
        assert!(!ClientError::Cancelled.is_retryable());
        assert!(!ClientError::NoValidLease.is_retryable());
    }

    #[test]
    fn test_from_net_error() {
        let net_err = powerfs_net::NetError::Timeout;
        let client_err: ClientError = net_err.into();
        assert!(matches!(client_err, ClientError::Timeout(_)));

        let net_err = powerfs_net::NetError::ServerError("test error".to_string());
        let client_err: ClientError = net_err.into();
        assert!(matches!(client_err, ClientError::Server(_)));
    }
}
