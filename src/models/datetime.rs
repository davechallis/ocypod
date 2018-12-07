//! Defines custom `DateTime` type.

use std::fmt;

use chrono;
use redis::{self, FromRedisValue, ToRedisArgs, RedisResult};
use serde_derive::*;

/// Thin wrapper around a `chrono::DateTime<Utc>` with functions for custom (de)serialisation.
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize)]
pub struct DateTime(chrono::DateTime<chrono::Utc>);

impl DateTime {
    /// Get current UTC date/time.
    pub fn now() -> Self {
        DateTime(chrono::Utc::now())
    }

    /// Get number of seconds since another given date/time.
    pub fn seconds_since(&self, other: &DateTime) -> i64 {
        self.0.signed_duration_since(other.0).num_seconds()
    }
}

impl FromRedisValue for DateTime {
    ///  Parse an RFC3339 date string from Redis.
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        let dt: String = redis::from_redis_value(v)?;
        Ok(DateTime(chrono::DateTime::parse_from_rfc3339(&dt).unwrap().with_timezone(&chrono::Utc)))
    }
}

impl ToRedisArgs for DateTime {
    /// Format this struct as an RFC3339 date string for storage in Redis.
    fn write_redis_args(&self, out: &mut Vec<Vec<u8>>) {
        self.0.to_rfc3339().write_redis_args(out)
    }
}

impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.to_rfc3339())
    }
}
