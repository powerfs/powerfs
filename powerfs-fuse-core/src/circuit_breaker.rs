use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// 熔断器状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// 闭合状态 (正常通行)
    Closed,
    /// 打开状态 (熔断中，拒绝所有请求)
    Open,
    /// 半开状态 (探测中，允许少量请求)
    HalfOpen,
}

impl CircuitState {
    pub fn as_str(&self) -> &str {
        match self {
            CircuitState::Closed => "Closed",
            CircuitState::Open => "Open",
            CircuitState::HalfOpen => "HalfOpen",
        }
    }
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// 熔断器配置
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// 连续失败次数阈值 (超过此值触发熔断)
    pub failure_threshold: u32,
    /// 熔断持续时间 (过此时间后进入 HalfOpen)
    pub recovery_timeout: std::time::Duration,
    /// HalfOpen 状态下允许的最大请求数
    pub half_open_max_requests: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout: std::time::Duration::from_secs(30),
            half_open_max_requests: 3,
        }
    }
}

/// 熔断器实现
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Mutex<CircuitBreakerInner>,
}

struct CircuitBreakerInner {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure_time: Option<Instant>,
    half_open_requests: u32,
    opened_at: Option<Instant>,
}

impl CircuitBreaker {
    /// 创建新的熔断器
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Mutex::new(CircuitBreakerInner {
                state: CircuitState::Closed,
                failure_count: 0,
                success_count: 0,
                last_failure_time: None,
                half_open_requests: 0,
                opened_at: None,
            }),
        }
    }

    /// 获取当前状态
    pub fn state(&self) -> CircuitState {
        let mut inner = self.state.lock().unwrap();
        self.check_state_transition(&mut inner);
        inner.state
    }

    /// 检查是否允许请求通过
    pub fn is_available(&self) -> bool {
        let mut inner = self.state.lock().unwrap();
        self.check_state_transition(&mut inner);

        match inner.state {
            CircuitState::Closed => true,
            CircuitState::HalfOpen => {
                // 允许少量请求通过，并立即占用一个名额
                if inner.half_open_requests < self.config.half_open_max_requests {
                    inner.half_open_requests += 1;
                    true
                } else {
                    false
                }
            }
            CircuitState::Open => false,
        }
    }

    /// 记录成功
    pub fn record_success(&self) {
        let mut inner = self.state.lock().unwrap();

        match inner.state {
            CircuitState::HalfOpen => {
                inner.success_count += 1;

                // 如果连续成功次数达到阈值，恢复到 Closed
                if inner.success_count >= self.config.half_open_max_requests {
                    inner.state = CircuitState::Closed;
                    inner.failure_count = 0;
                    inner.success_count = 0;
                    inner.half_open_requests = 0;
                    inner.opened_at = None;
                    log::info!("CircuitBreaker: HalfOpen -> Closed (success threshold reached)");
                }
            }
            CircuitState::Closed => {
                // 重置失败计数
                inner.failure_count = 0;
            }
            CircuitState::Open => {
                // 忽略，保持 Open 状态
            }
        }
    }

    /// 记录失败
    pub fn record_failure(&self) {
        let mut inner = self.state.lock().unwrap();
        self.check_state_transition(&mut inner);

        match inner.state {
            CircuitState::Closed => {
                inner.failure_count += 1;
                inner.last_failure_time = Some(Instant::now());
                inner.success_count = 0;

                // 检查是否达到失败阈值
                if inner.failure_count >= self.config.failure_threshold {
                    inner.state = CircuitState::Open;
                    inner.opened_at = Some(Instant::now());
                    inner.half_open_requests = 0;
                    log::warn!(
                        "CircuitBreaker: Closed -> Open (failure threshold reached: {})",
                        inner.failure_count
                    );
                }
            }
            CircuitState::HalfOpen => {
                // 任何失败都立即重新打开熔断器
                inner.state = CircuitState::Open;
                inner.failure_count = self.config.failure_threshold; // 确保达到阈值
                inner.last_failure_time = Some(Instant::now());
                inner.opened_at = Some(Instant::now());
                inner.half_open_requests = 0;
                log::warn!("CircuitBreaker: HalfOpen -> Open (failure in half-open state)");
            }
            CircuitState::Open => {
                // 忽略
            }
        }
    }

    /// 重置熔断器 (强制恢复到 Closed)
    pub fn reset(&self) {
        let mut inner = self.state.lock().unwrap();
        inner.state = CircuitState::Closed;
        inner.failure_count = 0;
        inner.success_count = 0;
        inner.half_open_requests = 0;
        inner.opened_at = None;
        log::info!("CircuitBreaker: Manually reset to Closed");
    }

    /// 检查并转换状态
    fn check_state_transition(&self, inner: &mut CircuitBreakerInner) {
        if inner.state == CircuitState::Open {
            // 检查是否到达恢复超时
            if let Some(opened_at) = inner.opened_at {
                if opened_at.elapsed() >= self.config.recovery_timeout {
                    inner.state = CircuitState::HalfOpen;
                    inner.half_open_requests = 0;
                    inner.success_count = 0;
                    log::info!("CircuitBreaker: Open -> HalfOpen (recovery timeout elapsed)");
                }
            }
        }
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }
}

/// 熔断器配置构建器
pub struct CircuitBreakerBuilder {
    config: CircuitBreakerConfig,
}

impl CircuitBreakerBuilder {
    pub fn new() -> Self {
        Self {
            config: CircuitBreakerConfig::default(),
        }
    }

    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.config.failure_threshold = threshold;
        self
    }

    pub fn with_recovery_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.config.recovery_timeout = timeout;
        self
    }

    pub fn with_half_open_max_requests(mut self, max: u32) -> Self {
        self.config.half_open_max_requests = max;
        self
    }

    pub fn build(self) -> CircuitBreaker {
        CircuitBreaker::new(self.config)
    }
}

impl Default for CircuitBreakerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A pool of circuit breakers, one per backend server address.
/// Provides precise fault isolation: only the failed server's requests are rejected.
pub struct CircuitBreakerPool {
    breakers: DashMap<String, Arc<CircuitBreaker>>,
    default_config: CircuitBreakerConfig,
}

impl CircuitBreakerPool {
    pub fn new(default_config: CircuitBreakerConfig) -> Self {
        Self {
            breakers: DashMap::new(),
            default_config,
        }
    }

    /// Check if the circuit for the given server address is available.
    /// Creates a new breaker if none exists for this address.
    pub fn check(&self, addr: &str) -> bool {
        self.get_or_create(addr).is_available()
    }

    /// Record a success for the given server address.
    pub fn record_success(&self, addr: &str) {
        self.get_or_create(addr).record_success();
    }

    /// Record a failure for the given server address.
    pub fn record_failure(&self, addr: &str) {
        self.get_or_create(addr).record_failure();
    }

    /// Get the current circuit state for the given server.
    pub fn state(&self, addr: &str) -> CircuitState {
        self.get_or_create(addr).state()
    }

    /// Reset the circuit for the given server.
    pub fn reset(&self, addr: &str) {
        self.get_or_create(addr).reset();
    }

    /// Remove the circuit for the given server (when server is decommissioned).
    pub fn remove(&self, addr: &str) {
        self.breakers.remove(addr);
    }

    /// Get or create a circuit breaker for the given address.
    fn get_or_create(&self, addr: &str) -> Arc<CircuitBreaker> {
        self.breakers
            .entry(addr.to_string())
            .or_insert_with(|| Arc::new(CircuitBreaker::new(self.default_config.clone())))
            .clone()
    }

    /// Get the number of tracked servers.
    pub fn len(&self) -> usize {
        self.breakers.len()
    }

    /// Check if the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.breakers.is_empty()
    }
}

impl Default for CircuitBreakerPool {
    fn default() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_initial_state_closed() {
        let cb = CircuitBreaker::default();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.is_available());
    }

    #[test]
    fn test_transitions_to_open_after_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        // 前 2 次失败不应触发熔断
        for _ in 0..2 {
            cb.record_failure();
        }
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.is_available());

        // 第 3 次失败触发熔断
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.is_available());
    }

    #[test]
    fn test_success_resets_failure_count() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        // 成功重置失败计数
        cb.record_success();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed); // 仍在 Closed，因为失败计数已重置
    }

    #[test]
    fn test_open_to_half_open_after_timeout() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            recovery_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        cb.record_failure(); // 触发熔断
        assert_eq!(cb.state(), CircuitState::Open);

        // 等待超时
        thread::sleep(Duration::from_millis(60));
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        assert!(cb.is_available());
    }

    #[test]
    fn test_half_open_success_transitions_to_closed() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            recovery_timeout: Duration::from_millis(10),
            half_open_max_requests: 2,
        };
        let cb = CircuitBreaker::new(config);

        // 触发熔断
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // 等待进入 HalfOpen
        thread::sleep(Duration::from_millis(20));
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // 两次成功恢复
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_failure_transitions_to_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            recovery_timeout: Duration::from_millis(10),
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        // 触发熔断
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // 等待进入 HalfOpen
        thread::sleep(Duration::from_millis(20));
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // 失败立即重新打开
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_half_open_limits_requests() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            recovery_timeout: Duration::from_millis(10),
            half_open_max_requests: 2,
        };
        let cb = CircuitBreaker::new(config);

        // 触发熔断
        cb.record_failure();
        cb.record_failure();

        // 等待进入 HalfOpen
        thread::sleep(Duration::from_millis(20));

        // 前 2 个请求应该通过
        assert!(cb.is_available());
        assert!(cb.is_available());

        // 第 3 个请求应该被拒绝
        assert!(!cb.is_available());
    }

    #[test]
    fn test_manual_reset() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.is_available());
    }

    #[test]
    fn test_builder_configuration() {
        let cb = CircuitBreakerBuilder::new()
            .with_failure_threshold(10)
            .with_recovery_timeout(Duration::from_secs(60))
            .with_half_open_max_requests(5)
            .build();

        assert_eq!(cb.state(), CircuitState::Closed);
    }

    // CircuitBreakerPool tests

    #[test]
    fn test_pool_isolation() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let pool = CircuitBreakerPool::new(config);

        let addr_a = "172.20.0.21:8080";
        let addr_b = "172.20.0.22:8080";

        // Both start available
        assert!(pool.check(addr_a));
        assert!(pool.check(addr_b));

        // Fail server A 3 times to trip its breaker
        for _ in 0..3 {
            pool.record_failure(addr_a);
        }

        // Server A should be open, but server B should still be available
        assert!(!pool.check(addr_a));
        assert!(pool.check(addr_b));
        assert_eq!(pool.state(addr_a), CircuitState::Open);
        assert_eq!(pool.state(addr_b), CircuitState::Closed);
    }

    #[test]
    fn test_pool_independent_recovery() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            recovery_timeout: Duration::from_millis(50),
            half_open_max_requests: 1,
        };
        let pool = CircuitBreakerPool::new(config);

        let addr_a = "172.20.0.21:8080";
        let addr_b = "172.20.0.22:8080";

        // Trip server A
        pool.record_failure(addr_a);
        pool.record_failure(addr_a);
        assert!(!pool.check(addr_a));

        // Trip server B
        pool.record_failure(addr_b);
        pool.record_failure(addr_b);
        assert!(!pool.check(addr_b));

        // Reset only server A
        pool.reset(addr_a);
        assert!(pool.check(addr_a));
        assert!(!pool.check(addr_b)); // B still open

        // Server A recovery does NOT affect server B
        assert_eq!(pool.state(addr_a), CircuitState::Closed);
        assert_eq!(pool.state(addr_b), CircuitState::Open);
    }

    #[test]
    fn test_pool_auto_create() {
        let pool = CircuitBreakerPool::default();

        // First check auto-creates a breaker
        assert!(pool.check("new-server:9999"));
        assert_eq!(pool.len(), 1);

        // Second check on same server reuses the breaker
        assert!(pool.check("new-server:9999"));
        assert_eq!(pool.len(), 1);

        // Different address creates a new breaker
        assert!(pool.check("another-server:8888"));
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn test_pool_remove() {
        let pool = CircuitBreakerPool::default();

        pool.check("server-a:8080");
        pool.check("server-b:8080");
        assert_eq!(pool.len(), 2);

        pool.remove("server-a:8080");
        assert_eq!(pool.len(), 1);

        // After removal, a new breaker is created on next check
        pool.check("server-a:8080");
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn test_pool_failure_recording() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            ..Default::default()
        };
        let pool = CircuitBreakerPool::new(config);

        let addr = "server:8080";

        // First failure doesn't trip
        pool.record_failure(addr);
        assert!(pool.check(addr));
        assert_eq!(pool.state(addr), CircuitState::Closed);

        // Second failure trips
        pool.record_failure(addr);
        assert!(!pool.check(addr));
        assert_eq!(pool.state(addr), CircuitState::Open);

        // Success on a different server doesn't affect the failed one
        pool.record_success("other:9090");
        assert_eq!(pool.state(addr), CircuitState::Open);
    }

    #[test]
    fn test_pool_empty() {
        let pool = CircuitBreakerPool::default();
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);

        pool.check("server:8080");
        assert!(!pool.is_empty());
    }
}
