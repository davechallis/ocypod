use std::fmt;
use std::str::FromStr;

use redis::{self, FromRedisValue, ToRedisArgs};

const ID_FIELD: &str = "id";
const QUEUE_FIELD: &str = "queue";
const STATUS_FIELD: &str = "status";
const TAGS_FIELD: &str = "tags";
const CREATED_AT_FIELD: &str = "created_at";
const STARTED_AT_FIELD: &str = "started_at";
const ENDED_AT_FIELD: &str = "ended_at";
const LAST_HEARTBEAT_FIELD: &str = "last_heartbeat";
const INPUT_FIELD: &str = "input";
const OUTPUT_FIELD: &str = "output";
const TIMEOUT_FIELD: &str = "timeout";
const HEARTBEAT_TIMEOUT_FIELD: &str = "heartbeat_timeout";
const EXPIRES_AFTER_FIELD: &str = "expires_after";
const RETRIES_FIELD: &str = "retries";
const RETRIES_ATTEMPTED_FIELD: &str = "retries_attempted";
const RETRY_DELAYS_FIELD: &str = "retry_delays";
const ENDED_FIELD: &str = "ended";


/// Represents a job field that's stored in a Redis hash.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all="snake_case")]
pub enum Field {
    Id,
    Queue,
    Status,
    Tags,
    CreatedAt,
    StartedAt,
    EndedAt,
    LastHeartbeat,
    Input,
    Output,
    Timeout,
    HeartbeatTimeout,
    ExpiresAfter,
    Retries,
    RetriesAttempted,
    RetryDelays,
    Ended,
}

impl Field {
    pub fn all_fields() -> &'static [Field] {
        static ALL_FIELDS: [Field; 17] = [
            Field::Id,
            Field::Queue,
            Field::Status,
            Field::Tags,
            Field::CreatedAt,
            Field::StartedAt,
            Field::EndedAt,
            Field::LastHeartbeat,
            Field::Input,
            Field::Output,
            Field::Timeout,
            Field::HeartbeatTimeout,
            Field::ExpiresAfter,
            Field::Retries,
            Field::RetriesAttempted,
            Field::RetryDelays,
            Field::Ended,
        ];

        &ALL_FIELDS
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl AsRef<str> for Field {
    fn as_ref(&self) -> &str {
        match self {
            Field::Id               => ID_FIELD,
            Field::Queue            => QUEUE_FIELD,
            Field::Status           => STATUS_FIELD,
            Field::Tags             => TAGS_FIELD,
            Field::CreatedAt        => CREATED_AT_FIELD,
            Field::StartedAt        => STARTED_AT_FIELD,
            Field::EndedAt          => ENDED_AT_FIELD,
            Field::LastHeartbeat    => LAST_HEARTBEAT_FIELD,
            Field::Input            => INPUT_FIELD,
            Field::Output           => OUTPUT_FIELD,
            Field::Timeout          => TIMEOUT_FIELD,
            Field::HeartbeatTimeout => HEARTBEAT_TIMEOUT_FIELD,
            Field::ExpiresAfter     => EXPIRES_AFTER_FIELD,
            Field::Retries          => RETRIES_FIELD,
            Field::RetriesAttempted => RETRIES_ATTEMPTED_FIELD,
            Field::RetryDelays      => RETRY_DELAYS_FIELD,
            Field::Ended            => ENDED_FIELD,
        }
    }
}

impl FromStr for Field {
    type Err = ();

    fn from_str(s: &str) -> Result<Field, ()> {
        match s {
            ID_FIELD                => Ok(Field::Id),
            QUEUE_FIELD             => Ok(Field::Queue),
            STATUS_FIELD            => Ok(Field::Status),
            TAGS_FIELD              => Ok(Field::Tags),
            CREATED_AT_FIELD        => Ok(Field::CreatedAt),
            STARTED_AT_FIELD        => Ok(Field::StartedAt),
            ENDED_AT_FIELD          => Ok(Field::EndedAt),
            LAST_HEARTBEAT_FIELD    => Ok(Field::LastHeartbeat),
            INPUT_FIELD             => Ok(Field::Input),
            OUTPUT_FIELD            => Ok(Field::Output),
            TIMEOUT_FIELD           => Ok(Field::Timeout),
            HEARTBEAT_TIMEOUT_FIELD => Ok(Field::HeartbeatTimeout),
            EXPIRES_AFTER_FIELD     => Ok(Field::ExpiresAfter),
            RETRIES_FIELD           => Ok(Field::Retries),
            RETRIES_ATTEMPTED_FIELD => Ok(Field::RetriesAttempted),
            RETRY_DELAYS_FIELD      => Ok(Field::RetryDelays),
            ENDED_FIELD             => Ok(Field::Ended),
            _ => Err(()),
        }
    }
}

impl ToRedisArgs for Field {
    fn write_redis_args(&self, out: &mut Vec<Vec<u8>>) {
        self.as_ref().write_redis_args(out)
    }
}

impl FromRedisValue for Field {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let s: String = String::from_redis_value(v)?;
        Field::from_str(&s).map_err(|_| (redis::ErrorKind::TypeError, "Invalid job field").into())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Ensure all fields correctly map to/from the same strings.
    #[test]
    fn field_to_from_str() {
        let all_fields = &[
            Field::Id,
            Field::Queue,
            Field::Status,
            Field::Tags,
            Field::CreatedAt,
            Field::StartedAt,
            Field::EndedAt,
            Field::LastHeartbeat,
            Field::Input,
            Field::Output,
            Field::Timeout,
            Field::HeartbeatTimeout,
            Field::ExpiresAfter,
            Field::Retries,
            Field::RetriesAttempted,
            Field::RetryDelays,
            Field::Ended,
        ];

        for field in all_fields {
            assert_eq!(field, &Field::from_str(field.as_ref()).unwrap());
        }
    }
}
