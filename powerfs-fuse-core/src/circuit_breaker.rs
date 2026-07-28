use std::sync::Mutex;
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
}
