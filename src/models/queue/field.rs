use redis::{self, FromRedisValue, RedisWrite, ToRedisArgs};
use std::fmt;
use std::str::FromStr;

const TIMEOUT_FIELD: &str = "timeout";
const HEARTBEAT_TIMEOUT_FIELD: &str = "heartbeat_timeout";
const EXPIRES_AFTER_FIELD: &str = "expires_after";
const RETRIES_FIELD: &str = "retries";
const RETRY_DELAYS_FIELD: &str = "retry_delays";

#[derive(Debug, PartialEq)]
pub enum Field {
    Timeout,
    HeartbeatTimeout,
    ExpiresAfter,
    Retries,
    RetryDelays,
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl AsRef<str> for Field {
    fn as_ref(&self) -> &str {
        match self {
            Field::Timeout => TIMEOUT_FIELD,
            Field::HeartbeatTimeout => HEARTBEAT_TIMEOUT_FIELD,
            Field::ExpiresAfter => EXPIRES_AFTER_FIELD,
            Field::Retries => RETRIES_FIELD,
            Field::RetryDelays => RETRY_DELAYS_FIELD,
        }
    }
}

impl FromStr for Field {
    type Err = ();

    fn from_str(s: &str) -> Result<Field, ()> {
        match s {
            TIMEOUT_FIELD => Ok(Field::Timeout),
            HEARTBEAT_TIMEOUT_FIELD => Ok(Field::HeartbeatTimeout),
            EXPIRES_AFTER_FIELD => Ok(Field::ExpiresAfter),
            RETRIES_FIELD => Ok(Field::Retries),
            RETRY_DELAYS_FIELD => Ok(Field::RetryDelays),
            _ => Err(()),
        }
    }
}

impl ToRedisArgs for Field {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        self.as_ref().write_redis_args(out)
    }
}

impl FromRedisValue for Field {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let s: String = String::from_redis_value(v)?;
        Self::from_str(&s).map_err(|_| (redis::ErrorKind::TypeError, "Invalid queue field").into())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Ensure all fields correctly map to/from the same strings.
    #[test]
    fn field_to_from_str() {
        let all_fields = &[
            Field::Timeout,
            Field::HeartbeatTimeout,
            Field::ExpiresAfter,
            Field::Retries,
            Field::RetryDelays,
        ];

        for field in all_fields {
            assert_eq!(field, &Field::from_str(field.as_ref()).unwrap());
        }
    }
}
