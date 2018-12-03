//! Data structures used throughout the application.

pub mod job;
pub mod queue;
mod state;
mod duration;
mod datetime;
mod error;

pub use self::state::ApplicationState;
pub use self::duration::Duration;
pub use self::datetime::DateTime;
pub use self::error::{OcyError, OcyResult};

use std::collections::HashMap;

use redis::{self, RedisResult, FromRedisValue};

// TODO: add redis stats, e.g. memory used etc.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct ServerInfo {
    pub queues: HashMap<String, QueueInfo>,
    pub statistics: JobStats,
}

impl Default for ServerInfo {
    fn default() -> Self {
        ServerInfo { queues: HashMap::new(), statistics: JobStats::default() }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Serialize)]
pub struct QueueInfo {
    pub queued: u64,
    pub running: u64,
    pub failed: u64,
    pub completed: u64,
    pub cancelled: u64,
    pub timed_out: u64,
}

impl QueueInfo {
    pub fn incr_status_count(&mut self, status: &job::Status) {
        match status {
            job::Status::Queued    => self.queued += 1,
            job::Status::Running   => self.running += 1,
            job::Status::Failed    => self.failed += 1,
            job::Status::Completed => self.completed += 1,
            job::Status::Cancelled => self.cancelled += 1,
            job::Status::TimedOut  => self.timed_out += 1,
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Serialize)]
pub struct JobStats {
    pub total_jobs_created: u64,
    pub total_jobs_completed: u64,
    pub total_jobs_retried: u64,
    pub total_jobs_failed: u64,
    pub total_jobs_timed_out: u64,
    pub total_jobs_cancelled: u64,
}

impl FromRedisValue for JobStats {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        let (created, completed, retried, failed, timed_out, cancelled):
        (Option<u64>, Option<u64>, Option<u64>, Option<u64>, Option<u64>, Option<u64>) = redis::from_redis_value(v)?;

        Ok(JobStats {
            total_jobs_created: created.unwrap_or_default(),
            total_jobs_completed: completed.unwrap_or_default(),
            total_jobs_retried: retried.unwrap_or_default(),
            total_jobs_failed: failed.unwrap_or_default(),
            total_jobs_timed_out: timed_out.unwrap_or_default(),
            total_jobs_cancelled: cancelled.unwrap_or_default(),
        })
    }
}
