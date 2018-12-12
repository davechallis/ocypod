//! Defines most of the core queue/job application logic.
//!
//! Main struct provided is `RedisManager`, through which all job queue operations are exposed.
//! These will typically have HTTP handlers mapped to them.
use std::collections::HashMap;
use std::default::Default;

use log::{debug, info, warn};
use redis::{self, Connection, Commands, PipelineCommands};
use serde_json;

use crate::models::{job, queue, JobStats, ServerInfo, DateTime, QueueInfo, OcyResult, OcyError};
use crate::redis_utils::{vec_from_redis_pipe, transaction};
use super::{queue::RedisQueue, job::RedisJob, tag::RedisTag, keys};

/// Manages queues and jobs within Redis. Contains main public functions that are called by HTTP services.
///
/// Internally, uses RedisJob and RedisQueue structs as convenient wrappers around interacting with jobs/queues.
pub struct RedisManager<'a> {
    conn: &'a Connection,
}

impl<'a> RedisManager<'a> {
    /// Get a new manager that uses given connection.
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    /// Create or update a queue in Redis with given name and settings.
    ///
    /// Returns true if a new queue was created, or false if an existing queue was updated.
    pub fn create_or_update_queue(&self, name: &str, settings: &queue::Settings) -> OcyResult<bool> {
        self.queue(name)?.create_or_update(settings)
    }

    /// Delete queue with given name from Redis.
    ///
    /// Returns true if a queue was deleted, and false if no queue with given name was found.
    pub fn delete_queue(&self, name: &str) -> OcyResult<bool> {
        self.queue(name)?.delete()
    }

    /// Delete a job with given ID from Redis.
    ///
    /// Returns true if a job was found and deleted, false if no job with given ID was found.
    pub fn delete_job(&self, job_id: u64) -> OcyResult<bool> {
        self.job(job_id).delete()
    }

    /// Get summary of server and queue data. Currently contains:
    /// * count of each job's status by queue
    /// * total number of jobs processed and their final status
    pub fn server_info(&self) -> OcyResult<ServerInfo> {
        let mut queues_info = HashMap::new();

        for queue_name in self.queue_names()? {
            let size = match self.queue(&queue_name)?.size() {
                Ok(size)                        => size,
                Err(OcyError::NoSuchQueue(_)) => continue,
                Err(err)                        => return Err(err),
            };
            queues_info.insert(queue_name, QueueInfo { queued: size, ..Default::default()} );
        }

        let mut pipeline = redis::pipe();
        let pipe = &mut pipeline;

        for queue_key in &[keys::FAILED_KEY, keys::ENDED_KEY, keys::RUNNING_KEY] {
            for job_id in self.conn.lrange::<_, Vec<u64>>(*queue_key, 0, -1)? {
                pipe.hget(self.job(job_id).key(), &[job::Field::Queue, job::Field::Status]);
            }
        }

        // option used to allow for jobs being deleted between calls
        for (queue_name, status) in vec_from_redis_pipe::<(Option<String>, Option<job::Status>)>(pipe, self.conn)? {
            let queue_name = match queue_name {
                Some(queue_name) => queue_name,
                None             => continue,
            };
            let status = match status {
                Some(status) => status,
                None         => continue,
            };
            let queue_info = queues_info.entry(queue_name).or_insert_with(QueueInfo::default);
            queue_info.incr_status_count(&status);
        }

        let job_stats: JobStats = self.conn.get(&keys::STATS_KEYS)?;
        Ok(ServerInfo { queues: queues_info, statistics: job_stats } )
    }

    /// Get one or more metadata fields from given job ID.
    ///
    /// If `None` is given as the `fields` argument, then get all fields.
    pub fn job_fields(&self, job_id: u64, fields: Option<&[job::Field]>) -> OcyResult<job::JobMeta> {
        self.job(job_id).fields(fields)
    }

    /// Update one or more job metadata fields.
    ///
    /// Only following fields can be updated in this way:
    ///
    /// * status - used to mark job as completed/failed/cancelled etc.
    /// * output - used to update user provided information related to this job
    pub fn update_job(&self, job_id: u64, update_req: &job::UpdateRequest) -> OcyResult<()> {
        self.job(job_id).update(update_req)
    }

    /// Update a job's `last_heartbeat` field with the current date/time.
    pub fn update_job_heartbeat(&self, job_id: u64) -> OcyResult<()> {
        self.job(job_id).update_heartbeat()
    }

    /// Get the `status` field of given job.
    pub fn job_status(&self, job_id: u64) -> OcyResult<job::Status> {
        self.job(job_id).status()
    }

    /// Update a job's `status` field to the given status, if an allowed state transition.
    ///
    /// Identical to calling `update_job` and with `Some(status)` provided.
    pub fn set_job_status(&self, job_id: u64, status: &job::Status) -> OcyResult<()> {
        self.job(job_id).set_status(status)
    }

    /// Get the `output` field of given job.
    pub fn job_output(&self, job_id: u64) -> OcyResult<serde_json::Value> {
        self.job(job_id).output()
    }

    /// Update a job's `output` field to the given output data.
    ///
    /// Identical to calling `update_job` and with `Some(output)` provided.
    pub fn set_job_output(&self, job_id: u64, value: &serde_json::Value) -> OcyResult<()> {
        self.job(job_id).set_output(value)
    }

    // TODO: add an endpoint to get fields too?
    /// Get a list of jobs IDs with given tag name.
    pub fn tagged_job_ids(&self, tag_name: &str) -> OcyResult<Vec<u64>> {
        self.tag(tag_name)?.tagged_job_ids()
    }

    /// Get list of all queue names.
    pub fn queue_names(&self) -> OcyResult<Vec<String>> {
        let mut names: Vec<String> = self.conn.smembers(keys::QUEUES_KEY)?;
        names.sort();
        Ok(names)
    }

    /// Get given queue's current settings.
    pub fn queue_settings(&self, queue_name: &str) -> OcyResult<queue::Settings> {
        self.queue(queue_name)?.ensure_exists()?.settings()
    }

    /// Get the number of queues jobs in given queue.
    pub fn queue_size(&self, queue_name: &str) -> OcyResult<u64> {
        self.queue(queue_name)?.ensure_exists()?.size()
    }

    /// Get total number of running jobs across all queues.
    pub fn running_queue_size(&self) -> OcyResult<u64> {
        Ok(self.conn.llen(keys::RUNNING_KEY)?)
    }

    /// Get total number of failed jobs across all queues.
    pub fn failed_queue_size(&self) -> OcyResult<u64> {
        Ok(self.conn.llen(keys::FAILED_KEY)?)
    }

    /// Get total number of ended jobs across all queues.
    pub fn ended_queue_size(&self) -> OcyResult<u64> {
        Ok(self.conn.llen(keys::ENDED_KEY)?)
    }

    /// Check all jobs in the failed queue for retries.
    ///
    /// Any which can be retried are re-queued on the queue they were created it.
    ///
    /// Any which have no automatic retries remaining are moved to the ended queue.
    pub fn check_job_retries(&self) -> OcyResult<Vec<u64>> {
        debug!("Checking for jobs to retry");
        let mut requeued: Vec<u64> = Vec::new();

        let mut pipeline = redis::pipe();
        let pipe = &mut pipeline;
        for job_id in self.conn.lrange::<_, Vec<u64>>(keys::FAILED_KEY, 0, -1)? {
            pipe.hget(self.job(job_id).key(), job::RetryMeta::fields());
        }

        for retry_meta in vec_from_redis_pipe::<job::RetryMeta>(pipe, self.conn)? {
            match retry_meta.retry_action() {
                job::RetryAction::Retry => {
                    let job = self.job(retry_meta.id());
                    if job.apply_retries()? {
                        requeued.push(job.id());
                    }
                },
                job::RetryAction::End => {
                    let job = self.job(retry_meta.id());
                    job.end_failed()?;
                },
                job::RetryAction::None => (),
            }
        }

        Ok(requeued)
    }

    /// Check all jobs in the running queue for timeouts.
    ///
    /// Any which timeout are moved to the failed queue, where they'll eventually either be retried, or moved to the
    /// ended queue.
    pub fn check_job_timeouts(&self) -> OcyResult<Vec<u64>> {
        debug!("Checking job timeouts");
        let mut timeouts: Vec<u64> = Vec::new();

        let mut pipeline = redis::pipe();
        let pipe = &mut pipeline;
        for job_id in self.conn.lrange::<_, Vec<u64>>(keys::RUNNING_KEY, 0, -1)? {
            pipe.hget(self.job(job_id).key(), job::TimeoutMeta::fields());
        }

        for timeout_meta in vec_from_redis_pipe::<job::TimeoutMeta>(pipe, self.conn)? {
            if timeout_meta.has_timed_out() {
                let job = self.job(timeout_meta.id());
                if job.apply_timeouts()? {
                    timeouts.push(job.id());
                }
            }
        }

        Ok(timeouts)
    }

    /// Check all jobs in the ended queue for expiry. Any expired jobs will be entirely removed from the queue system.
    pub fn check_job_expiry(&self) -> OcyResult<Vec<u64>> {
        debug!("Checking for expired jobs");
        let mut expired: Vec<u64> = Vec::new();

        let mut pipeline = redis::pipe();
        let pipe = &mut pipeline;
        for job_id in self.conn.lrange::<_, Vec<u64>>(keys::ENDED_KEY, 0, -1)? {
            pipe.hget(self.job(job_id).key(), job::ExpiryMeta::fields());
        }

        for expiry_meta in vec_from_redis_pipe::<job::ExpiryMeta>(pipe, self.conn)? {
            if expiry_meta.should_expire() {
                let job = self.job(expiry_meta.id());
                if job.apply_expiry()? {
                    expired.push(job.id());
                }
            }
        }

        Ok(expired)
    }

    // TODO: make available as endpoint? Or optional periodic check?
    /// Checks the integrity of Redis DB, e.g. checking for dangling indexes, jobs in invalid states, etc.
    ///
    /// Mostly intended for use during development, as it has a non-trivial runtime cost.
    pub fn check_db_integrity(&self) -> OcyResult<()> {
        for queue_name in self.queue_names()? {
            let queue = self.queue(&queue_name)?;
            if !(queue.exists()?) {
                warn!("Queue '{}' found in {}, but not as key", queue_name, keys::QUEUES_KEY);
            }
        }

        for queue_key in self.conn.scan_match::<_, String>("queue:*")? {
            if queue_key.ends_with(":jobs") {
                continue;
            }

            if !self.conn.sismember::<_, _, bool>(keys::QUEUES_KEY, &queue_key[6..])? {
                warn!("Queue '{}' found as key, but not in {}", &queue_key, keys::QUEUES_KEY);
            }
        }

        transaction(self.conn, &[keys::RUNNING_KEY], |pipe| {
            for job_id in self.conn.lrange::<_, Vec<u64>>(keys::RUNNING_KEY, 0, -1)? {
                pipe.hget(self.job(job_id).key(), &[job::Field::Id, job::Field::Status, job::Field::StartedAt]);
            }

            let info: Vec<(Option<u64>, Option<job::Status>, Option<DateTime>)> =
                vec_from_redis_pipe(pipe, self.conn)?;
            for (job_id, status, started_at) in info {
                let job_id = match job_id {
                    Some(job_id) => job_id,
                    None => {
                        warn!("Found job in {} queue, but did not find key", keys::RUNNING_KEY);
                        continue;
                    }
                };

                match status {
                    Some(job::Status::Running) => (),
                    Some(status) => warn!("Found status '{}' in {} queue", status, keys::RUNNING_KEY),
                    None => warn!("Found job {} in {} queue, but did not find key", job_id, keys::RUNNING_KEY),
                }

                if started_at.is_none() {
                    warn!("Found job {} in {} queue, but job has no started_at", job_id, keys::RUNNING_KEY);
                }
            }

            Ok(Some(()))
        })?;

        transaction(self.conn, &[keys::FAILED_KEY], |pipe| {
            for job_id in self.conn.lrange::<_, Vec<u64>>(keys::FAILED_KEY, 0, -1)? {
                pipe.hget(self.job(job_id).key(), &[job::Field::Id, job::Field::Status, job::Field::EndedAt]);
            }

            let info: Vec<(Option<u64>, Option<job::Status>, Option<DateTime>)> =
                vec_from_redis_pipe(pipe, self.conn)?;
            for (job_id, status, ended_at) in info {
                let job_id = match job_id {
                    Some(job_id) => job_id,
                    None => {
                        warn!("Found job in {} queue, but did not find key", keys::FAILED_KEY);
                        continue;
                    }
                };

                match status {
                    Some(job::Status::Failed) | Some(job::Status::TimedOut) => (),
                    Some(status) => warn!("Found status '{}' in {} queue", status, keys::FAILED_KEY),
                    None => warn!("Found job {} in {} queue, but did not find key", job_id, keys::FAILED_KEY),
                }

                if ended_at.is_none() {
                    warn!("Found job {} in {} queue, but job has no ended_at", job_id, keys::FAILED_KEY);
                }
            }

            Ok(Some(()))
        })?;

        transaction(self.conn, &[keys::ENDED_KEY], |pipe| {
            for job_id in self.conn.lrange::<_, Vec<u64>>(keys::ENDED_KEY, 0, -1)? {
                pipe.hget(self.job(job_id).key(), &[job::Field::Id, job::Field::Status, job::Field::EndedAt]);
            }

            let info: Vec<(Option<u64>, Option<job::Status>, Option<DateTime>)> =
                vec_from_redis_pipe(pipe, self.conn)?;
            for (job_id, status, ended_at) in info {
                let job_id = match job_id {
                    Some(job_id) => job_id,
                    None => {
                        warn!("Found job in {} queue, but did not find key", keys::ENDED_KEY);
                        continue;
                    }
                };

                match status {
                    Some(job::Status::Failed)
                    | Some(job::Status::TimedOut)
                    | Some(job::Status::Completed)
                    | Some(job::Status::Cancelled) => (),
                    Some(status) => warn!("Found status '{}' in {} queue", status, keys::ENDED_KEY),
                    None => warn!("Found job {} in {} queue, but did not find key", job_id, keys::ENDED_KEY),
                }

                if ended_at.is_none() {
                    warn!("Found job {} in {} queue, but job has no started_at", job_id, keys::ENDED_KEY);
                }
            }

            Ok(Some(()))
        })?;

        Ok(())
    }

    /// Check connection to Redis using ping command.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::unit_arg))]
    pub fn check_ping(&self) -> OcyResult<()> {
        Ok(redis::cmd("PING").query(self.conn)?)
    }

    /// Fetch the next job from given queue, if any.
    ///
    /// # Returns
    ///
    /// A `job::Payload` if a job is found, or `None` if the queue is empty.
    pub fn next_queued_job(&self, queue_name: &str) -> OcyResult<Option<job::Payload>> {
        debug!("Client requested job from queue={}", queue_name);
        // queue can be deleted between these two calls, but will just return no job, so harmless
        let queue = self.queue(queue_name)?.ensure_exists()?;
        let job = match self.conn.rpoplpush::<_, Option<u64>>(queue.jobs_key(), keys::LIMBO_KEY)? {
            Some(job_id) => self.job(job_id),
            None         => return Ok(None),
        };
        debug!("[{}{}] moved from {} -> {}", keys::JOB_PREFIX, job.id(), queue.jobs_key(), keys::LIMBO_KEY);

        // if Redis goes down before the following, job will be left in limbo, requeued at startup
        let job_payload: job::Payload = transaction(self.conn, &[&job.key], |pipe| {
            let input: Option<String> = self.conn.hget(&job.key, job::Field::Input)?;
            let payload = job::Payload::new(job.id(), input.map(|s| serde_json::from_str(&s).unwrap()));

            let result: Option<()> = pipe
                .hset(&job.key, job::Field::Status, job::Status::Running)
                .hset(&job.key, job::Field::StartedAt, DateTime::now())
                .lrem(keys::LIMBO_KEY, 1, job.id())
                .rpush(keys::RUNNING_KEY, job.id())
                .query(self.conn)?;
            Ok(result.map(|_| payload))
        })?;

        info!("[{}{}] started", keys::JOB_PREFIX, job_payload.id());
        Ok(Some(job_payload))
    }

    /// Create a new job on given queue.
    pub fn create_job(&self, queue_name: &str, job_req: &job::CreateRequest) -> OcyResult<u64> {
        // TODO: use transaction to ensure that queue isn't deleted partway through job creation
        let queue = self.queue(queue_name)?.ensure_exists()?;
        let queue_settings = queue.settings()?;
        let timeout = job_req.timeout.as_ref().unwrap_or(&queue_settings.timeout);
        let heartbeat_timeout = job_req.heartbeat_timeout.as_ref().unwrap_or(&queue_settings.heartbeat_timeout);
        let expires_after = job_req.expires_after.as_ref().unwrap_or(&queue_settings.expires_after);
        let retries = job_req.retries.unwrap_or(queue_settings.retries);
        let retry_delays = match job_req.retry_delays.clone() {
            Some(rd) => rd,
            None     => Vec::new(),
        };

        let job = self.job(self.conn.incr(keys::JOB_ID_KEY, 1)?);
        debug!("Creating job with job_id={} on queue={}", job.id(), &queue.name);

        let mut pipeline = redis::pipe();
        let pipe = pipeline
            .atomic()
            .hset(&job.key, job::Field::Id, job.id())
            .hset(&job.key, job::Field::Queue, &queue.name)
            .hset(&job.key, job::Field::Status, job::Status::Queued)
            .hset(&job.key, job::Field::CreatedAt, DateTime::now())
            .hset(&job.key, job::Field::Timeout, timeout)
            .hset(&job.key, job::Field::HeartbeatTimeout, heartbeat_timeout)
            .hset(&job.key, job::Field::ExpiresAfter, expires_after)
            .hset(&job.key, job::Field::Retries, retries)
            .hset(&job.key, job::Field::RetriesAttempted, 0)
            .incr(keys::STAT_JOBS_CREATED_KEY, 1)
            .lpush(queue.jobs_key(), job.id());

        if let Some(ref input) = job_req.input {
            pipe.hset(&job.key, job::Field::Input, input.to_string());
        }

        if let Some(ref tags) = job_req.tags {
            let tags_json: serde_json::Value = tags.as_slice().into();
            pipe.hset(&job.key, job::Field::Tags, tags_json.to_string());
            for tag in tags {
                let key = format!("{}{}", keys::TAG_PREFIX, tag);
                pipe.sadd(key, job.id());
            }
        }

        if !retry_delays.is_empty() {
            let retry_delays_json: serde_json::Value = retry_delays.as_slice().into();
            pipe.hset(&job.key, job::Field::RetryDelays, retry_delays_json.to_string());
        }

        pipe.query(self.conn)?;

        info!("[{}] [{}] created", &queue.key, &job.key);
        Ok(job.id())
    }

    /// Helper function for getting a `RedisQueue` struct that uses this manager's Redis connection.
    fn queue(&self, name: &str) -> OcyResult<RedisQueue> {
        RedisQueue::from_string(name, self.conn)
    }

    /// Helper function for getting a `RedisJob` struct that uses this manager's Redis connection.
    fn job(&self, job_id: u64) -> RedisJob {
        RedisJob::new(job_id, self.conn)
    }

    /// Helper function for getting a `RedisTag` struct that uses this manager's Redis connection.
    fn tag(&self, name: &str) -> OcyResult<RedisTag> {
        RedisTag::from_str(name, self.conn)
    }
}



