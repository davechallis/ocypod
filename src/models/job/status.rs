//! Defines structs used to represent the status of a job in Redis.

use std::fmt;
use std::str::FromStr;

use redis::{self, FromRedisValue, ToRedisArgs};
use serde_derive::*;

const QUEUED_STATUS: &str = "queued";
const RUNNING_STATUS: &str = "running";
const FAILED_STATUS: &str = "failed";
const COMPLETED_STATUS: &str = "completed";
const CANCELLED_STATUS: &str = "cancelled";
const TIMED_OUT_STATUS: &str = "timed_out";

/// Status of a job that exists in Redis.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all="snake_case")]
pub enum Status {
    /// Job is in a queue, waiting for a worker to start it.
    Queued,

    /// Job is currently running on a worker.
    Running,

    /// Worker notified server that the job failed.
    Failed,

    /// Worker notified server that the job completed successfully.
    Completed,

    /// Queued or running job was cancelled by client request.
    Cancelled,

    /// Job was marked as timed out by server, due to no completion or heartbeat by worker.
    TimedOut,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl AsRef<str> for Status {
    fn as_ref(&self) -> &str {
        match self {
            Status::Queued    => QUEUED_STATUS,
            Status::Running   => RUNNING_STATUS,
            Status::Failed    => FAILED_STATUS,
            Status::Completed => COMPLETED_STATUS,
            Status::Cancelled => CANCELLED_STATUS,
            Status::TimedOut  => TIMED_OUT_STATUS,
        }
    }
}

impl FromStr for Status {
    type Err = ();

    fn from_str(s: &str) -> Result<Status, ()> {
        match s {
            QUEUED_STATUS    => Ok(Status::Queued),
            RUNNING_STATUS   => Ok(Status::Running),
            FAILED_STATUS    => Ok(Status::Failed),
            COMPLETED_STATUS => Ok(Status::Completed),
            CANCELLED_STATUS => Ok(Status::Cancelled),
            TIMED_OUT_STATUS => Ok(Status::TimedOut),
            _ => Err(()),
        }
    }
}

impl ToRedisArgs for Status {
    fn write_redis_args(&self, out: &mut Vec<Vec<u8>>) {
        self.as_ref().write_redis_args(out)
    }
}

impl FromRedisValue for Status {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let s = String::from_redis_value(v)?;
        Status::from_str(&s).map_err(|_| (redis::ErrorKind::TypeError, "Invalid job status").into())
    }
}

impl<'a> ToRedisArgs for &'a Status {
    fn write_redis_args(&self, out: &mut Vec<Vec<u8>>) {
        self.as_ref().write_redis_args(out)
    }
}

#[cfg(test)]
mod test {
    use serde_json;
    use super::*;

    const ALL_STATUSES: [Status; 6] = [
        Status::Queued,
        Status::Running,
        Status::Failed,
        Status::Completed,
        Status::Cancelled,
        Status::TimedOut,
    ];

    /// Ensure all fields correctly map to/from the same strings.
    #[test]
    fn status_to_from_str() {
        for status in &ALL_STATUSES {
            assert_eq!(status, &Status::from_str(status.as_ref()).unwrap());
        }
    }

    #[test]
    fn serialisation() {
        assert_eq!(serde_json::to_string(&Status::Queued).unwrap(), "\"queued\"");
        assert_eq!(serde_json::to_string(&Status::Running).unwrap(), "\"running\"");
        assert_eq!(serde_json::to_string(&Status::Failed).unwrap(), "\"failed\"");
        assert_eq!(serde_json::to_string(&Status::Completed).unwrap(), "\"completed\"");
        assert_eq!(serde_json::to_string(&Status::Cancelled).unwrap(), "\"cancelled\"");
        assert_eq!(serde_json::to_string(&Status::TimedOut).unwrap(), "\"timed_out\"");
    }
}