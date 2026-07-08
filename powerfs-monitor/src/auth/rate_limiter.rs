use redis::{AsyncCommands, Client};
use std::sync::Arc;

const IP_LIMIT: i64 = 10;
const USER_LIMIT: i64 = 5;
const LOCKOUT_ATTEMPTS: i64 = 5;
const LOCKOUT_DURATION_SECONDS: u64 = 900;
const WINDOW_SECONDS: i64 = 60;

pub struct RateLimiter {
    client: Arc<Client>,
}

impl RateLimiter {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    pub async fn check_login(&self, ip: &str, username: &str) -> Result<bool, String> {
        let mut conn = match self.client.get_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                log::warn!("Redis connection failed for rate limiter: {}", e);
                return Ok(true);
            }
        };

        let ip_key = format!("login:ip:{}", ip);
        let user_key = format!("login:user:{}", username);
        let lock_key = format!("login:lock:{}", username);

        if let Ok(lock) = conn.get::<_, Option<String>>(&lock_key).await {
            if lock.is_some() {
                return Ok(false);
            }
        }

        let ip_count: i64 = conn
            .incr(&ip_key, 1)
            .await
            .map_err(|e| format!("Redis incr error: {}", e))?;
        let user_count: i64 = conn
            .incr(&user_key, 1)
            .await
            .map_err(|e| format!("Redis incr error: {}", e))?;

        if ip_count == 1 {
            let _: redis::RedisResult<()> = conn.expire(&ip_key, WINDOW_SECONDS).await;
        }
        if user_count == 1 {
            let _: redis::RedisResult<()> = conn.expire(&user_key, WINDOW_SECONDS).await;
        }

        if user_count >= LOCKOUT_ATTEMPTS {
            let _: redis::RedisResult<()> =
                conn.set_ex(&lock_key, "1", LOCKOUT_DURATION_SECONDS).await;
            return Ok(false);
        }

        Ok(ip_count <= IP_LIMIT && user_count <= USER_LIMIT)
    }

    pub async fn reset_login(&self, username: &str) -> Result<(), String> {
        let mut conn = self.client.get_async_connection().await.map_err(|e| {
            log::warn!("Redis connection failed for rate limiter: {}", e);
            "Rate limit reset failed".to_string()
        })?;

        let user_key = format!("login:user:{}", username);
        let lock_key = format!("login:lock:{}", username);

        let _: redis::RedisResult<()> = conn.del(&user_key).await;
        let _: redis::RedisResult<()> = conn.del(&lock_key).await;

        Ok(())
    }
}
