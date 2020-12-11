use redis::{self, from_redis_value, FromRedisValue, RedisResult};
use serde::{Deserialize, Serialize};

use crate::models::Duration;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct Settings {
    pub timeout: Duration,
    pub heartbeat_timeout: Duration,
    pub expires_after: Duration,
    pub retries: u64,
    pub retry_delays: Vec<Duration>,
}

impl FromRedisValue for Settings {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        let (timeout, heartbeat_timeout, expires_after, retries, retry_delays): (
            Duration,
            Duration,
            Duration,
            u64,
            Option<String>,
        ) = from_redis_value(v)?;
        let retry_delays = match retry_delays {
            Some(s) => serde_json::from_str(&s).unwrap(),
            None => Vec::new(),
        };
        Ok(Self {
            timeout,
            heartbeat_timeout,
            expires_after,
            retries,
            retry_delays,
        })
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300),
            heartbeat_timeout: Duration::from_secs(0),
            expires_after: Duration::from_secs(300),
            retries: 0,
            retry_delays: Vec::new(),
        }
    }
}
