use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    NonRetryable(String),
    Retryable(String),
    LeaderChanged(String),
    RateLimited(Duration),
}

pub trait RetryPolicy: Sync + Send {
    fn max_retries(&self) -> usize;
    fn initial_delay(&self) -> Duration;
    fn max_delay(&self) -> Duration;
    fn backoff_factor(&self) -> f64;

    fn classify_error<E: std::fmt::Display>(&self, error: &E) -> ErrorKind {
        let msg = error.to_string();
        if msg.contains("not leader") || msg.contains("NotLeader") {
            ErrorKind::LeaderChanged(String::new())
        } else if msg.contains("invalid request")
            || msg.contains("not found")
            || msg.contains("already exists")
            || msg.contains("permission denied")
            || msg.contains("checksum mismatch")
            || msg.contains("out of space")
        {
            ErrorKind::NonRetryable(msg)
        } else if msg.contains("rate limited") {
            ErrorKind::RateLimited(Duration::from_secs(5))
        } else {
            ErrorKind::Retryable(msg)
        }
    }

    fn calculate_delay(&self, attempt: usize) -> Duration {
        let delay_nanos =
            self.initial_delay().as_nanos() as f64 * self.backoff_factor().powi(attempt as i32);
        let delay = Duration::from_nanos(delay_nanos as u64);
        std::cmp::min(delay, self.max_delay())
    }

    fn execute<T, E, F, Fut>(
        &self,
        mut func: F,
    ) -> impl std::future::Future<Output = Result<T, E>> + Send
    where
        T: Send + 'static,
        E: std::fmt::Display + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, E>> + Send + 'static,
    {
        async move {
            let mut attempt = 0;
            loop {
                match func().await {
                    Ok(result) => return Ok(result),
                    Err(e) => {
                        attempt += 1;
                        match self.classify_error(&e) {
                            ErrorKind::NonRetryable(_) => return Err(e),
                            ErrorKind::LeaderChanged(_) => {
                                continue;
                            }
                            ErrorKind::RateLimited(delay) => {
                                sleep(delay).await;
                                continue;
                            }
                            ErrorKind::Retryable(_) => {
                                if attempt > self.max_retries() {
                                    return Err(e);
                                }
                                let delay = self.calculate_delay(attempt);
                                log::warn!(
                                    "Retry attempt {} failed: {}, retrying in {:?}",
                                    attempt,
                                    e,
                                    delay
                                );
                                sleep(delay).await;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    max_retries: usize,
    initial_delay: Duration,
    max_delay: Duration,
    backoff_factor: f64,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        ExponentialBackoff {
            max_retries: 3,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            backoff_factor: 2.0,
        }
    }
}

impl ExponentialBackoff {
    pub fn new(
        max_retries: usize,
        initial_delay: Duration,
        max_delay: Duration,
        backoff_factor: f64,
    ) -> Self {
        ExponentialBackoff {
            max_retries,
            initial_delay,
            max_delay,
            backoff_factor,
        }
    }
}

impl RetryPolicy for ExponentialBackoff {
    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn initial_delay(&self) -> Duration {
        self.initial_delay
    }

    fn max_delay(&self) -> Duration {
        self.max_delay
    }

    fn backoff_factor(&self) -> f64 {
        self.backoff_factor
    }
}

#[derive(Debug, Clone)]
pub struct FixedDelay {
    max_retries: usize,
    delay: Duration,
}

impl Default for FixedDelay {
    fn default() -> Self {
        FixedDelay {
            max_retries: 3,
            delay: Duration::from_millis(500),
        }
    }
}

impl FixedDelay {
    pub fn new(max_retries: usize, delay: Duration) -> Self {
        FixedDelay { max_retries, delay }
    }
}

impl RetryPolicy for FixedDelay {
    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn initial_delay(&self) -> Duration {
        self.delay
    }

    fn max_delay(&self) -> Duration {
        self.delay
    }

    fn backoff_factor(&self) -> f64 {
        1.0
    }
}
