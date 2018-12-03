//! Contains definitions for Redis keys, prefixes, counters, etc. used throughout the application.

/// Redis key for list of all queues. This is used for fast lookups of all queue names without a scan.
pub const QUEUES_KEY: &str = "queues";

/// Redis key for limbo queue. This is a very short lived queue, used to keep jobs in the transition state between
/// `queued` and `running`. It's mostly a workaround for not being able to atomically pop a job from a queue and
/// update its metadata (stored in a separate hash) without the risk of losing some data.
pub const LIMBO_KEY: &str = "limbo";

/// Redis key for the running job list. Jobs are moved here from their original queue (via `limbo`) when they're
/// picked up by a worker. Jobs in this queue are checked for timeouts.
pub const RUNNING_KEY: &str = "running";

/// Redis key for the failed job list. Jobs that have either timed out, or failed by worker request are moved to this
/// queue. Jobs in this queue are monitored for retries.
pub const FAILED_KEY: &str = "failed";

/// Redis key for the ended job list. Jobs are moved here then they have either successfully completed,
/// or failed/timed out with no remaining retries to attempted. Jobs in this queue are monitored for expiry.
pub const ENDED_KEY: &str = "ended";

/// Redis key for the job ID counter. This is used as a counter to generate unique IDs for each job.
pub const JOB_ID_KEY: &str = "job_id";

/// Prefix used for queue settings keys in Redis. A user created queue with name "foo" have its configuration stored
/// under the key "queue:foo".
pub const QUEUE_PREFIX: &str = "queue:";

/// Prefix used for job keys in Redis. A job with the ID 123 would be stored under the key "job:123".
pub const JOB_PREFIX: &str = "job:";

/// Suffix used with queue keys get the Redis key for queued jobs. A user created queue with name "foo" would store
/// its queued jobs under the key "queue:foo:jobs";
pub const QUEUE_JOBS_SUFFIX: &str = ":jobs";

/// Prefix used for tag keys in Redis. These are used to index jobs by any tags they were given at creation time.
/// A tag created with name "foo" would be stored a "tag:foo".
pub const TAG_PREFIX: &str = "tag:";

pub const STAT_JOBS_CREATED_KEY: &str = "stats:jobs:num_created";
pub const STAT_JOBS_COMPLETED_KEY: &str = "stats:jobs:num_completed";
pub const STAT_JOBS_RETRIED_KEY: &str = "stats:jobs:num_retried";
pub const STAT_JOBS_FAILED_KEY: &str = "stats:jobs:num_failed";
pub const STAT_JOBS_TIMED_OUT_KEY: &str = "stats:jobs:num_timed_out";
pub const STAT_JOBS_CANCELLED_KEY: &str = "stats:jobs:cancelled";

pub static STATS_KEYS: [&str; 6] = [
    STAT_JOBS_CREATED_KEY,
    STAT_JOBS_COMPLETED_KEY,
    STAT_JOBS_RETRIED_KEY,
    STAT_JOBS_FAILED_KEY,
    STAT_JOBS_TIMED_OUT_KEY,
    STAT_JOBS_CANCELLED_KEY,
];

