use serde_json;

use crate::models::{Duration, job::Status};

/// Request to create a new job.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct CreateRequest {
    /// Contains the job payload itself, clients will generally parse this to determine what work to do.
    pub input: Option<serde_json::Value>,

    /// Free text tags given to this job, can be used for searching later. Uses include giving an ID to a batch
    /// of related jobs, adding a user/owner field to a job, labelling the host/process that created it, etc.
    pub tags: Option<Vec<String>>,

    /// Total execution time of this job before it's marked as timed out. If not specified, then the queue's
    /// timeout will be used.
    ///
    /// Set to 0 for no timeout.
    pub timeout: Option<Duration>,

    /// Maximum time between heartbeats before a job is marked as timed out. If not specified, then the queue's
    /// heartbeat timeout will be used.
    ///
    /// Set to 0 for no heartbeat timeout.
    pub heartbeat_timeout: Option<Duration>,

    /// Amount of time after a job ends (whether through successful completion, failure, or timing out)
    /// before the job and its output are deleted from the task queuing system.
    ///
    /// If not specified, then the queue's expiry setting will be used.
    ///
    /// Set to 0 for no expiry.
    pub expires_after: Option<Duration>,

    // TODO: should -1 be used for unlimited retries?
    /// Number of times this job can be attempted to run. If not specified, then the queue's max tries setting
    /// will be used.
    ///
    /// Set to 0 to disable automatic retries.
    pub retries: Option<u64>,

    /// Defines how long the delay should be before each retry is attempted. If the number of retries exceeds the
    /// length of this list, then the final value will be used. If not specified, then the queue's retry delays
    /// setting
    ///
    /// E.g. with retries=5 and retry_delays=[10, 10, 20, 40], then the first two retries will be delayed by
    /// 10 seconds, the third by 20 seconds, and the fourth and fifth by 40 seconds.
    pub retry_delays: Option<Vec<Duration>>
}

/// Request to update an existing job with new data.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct UpdateRequest {
    /// The new status of the job.
    pub status: Option<Status>,

    /// The new output JSON to store as part of the job.
    pub output: Option<serde_json::Value>,
}