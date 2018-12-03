//! Summary information about a single queue.

use redis::{self, FromRedisValue, from_redis_value};
use crate::models::Duration;

#[derive(Debug, PartialEq, Serialize)]
pub struct Summary {
    pub size: u64,
    pub timeout: Duration,
    pub heartbeat_timeout: Duration,
    pub expires_after: Duration,
}

impl FromRedisValue for Summary {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let (size, timeout, heartbeat_timeout, expires_after): (u64, Duration, Duration, Duration) =
            from_redis_value(v)?;
        Ok(Summary { size, timeout, heartbeat_timeout, expires_after })
    }
}