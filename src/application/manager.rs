//! Defines most of the core queue/job application logic.
//!
//! Main struct provided is `RedisManager`, through which all job queue operations are exposed.
//! These will typically have HTTP handlers mapped to them.
use std::collections::HashMap;
use std::default::Default;

use log::{debug, info, warn};
use redis::{aio::ConnectionLike, AsyncCommands};

use super::{job::RedisJob, keys, queue::RedisQueue, tag::RedisTag};
use crate::models::{job, queue, DateTime, JobStats, OcyError, OcyResult, QueueInfo, ServerInfo};
use crate::redis_utils::vec_from_redis_pipe;
use crate::transaction_async;

/// Manages queues and jobs within Redis. Contains main public functions that are called by HTTP services.
///
/// Internally, uses RedisJob and RedisQueue structs as convenient wrappers around interacting with jobs/queues.
pub struct RedisManager {}

impl RedisManager {
    /// Create or update a queue in Redis with given name and settings.
    ///
    /// Returns true if a new queue was created, or false if an existing queue was updated.
    pub async fn create_or_update_queue<C: ConnectionLike>(
        conn: &mut C,
        name: &str,
        settings: &queue::Settings,
    ) -> OcyResult<bool> {
        RedisQueue::from_string(name)?
            .create_or_update(conn, settings)
            .await
    }

    /// Delete queue with given name from Redis.
    ///
    /// Returns true if a queue was deleted, and false if no queue with given name was found.
    pub async fn delete_queue<C: ConnectionLike + Send>(
        conn: &mut C,
        name: &str,
    ) -> OcyResult<bool> {
        RedisQueue::from_string(name)?.delete(conn).await
    }

    /// Delete a job with given ID from Redis.
    ///
    /// Returns true if a job was found and deleted, false if no job with given ID was found.
    pub async fn delete_job<C: ConnectionLike + Send>(
        conn: &mut C,
        job_id: u64,
    ) -> OcyResult<bool> {
        RedisJob::new(job_id).delete(conn).await
    }

    /// Get summary of server and queue data. Currently contains:
    /// * count of each job's status by queue
    /// * total number of jobs processed and their final status
    pub async fn server_info<C: ConnectionLike + Send>(conn: &mut C) -> OcyResult<ServerInfo> {
        let mut queues_info = HashMap::new();

        for queue_name in Self::queue_names(conn).await? {
            let size = match RedisQueue::from_string(&queue_name)?.size(conn).await {
                Ok(size) => size,
                Err(OcyError::NoSuchQueue(_)) => continue,
                Err(err) => return Err(err),
            };
            queues_info.insert(
                queue_name,
                QueueInfo {
                    queued: size,
                    ..Default::default()
                },
            );
        }

        let mut pipeline = redis::pipe();
        let pipe = &mut pipeline;

        for queue_key in &[keys::FAILED_KEY, keys::ENDED_KEY, keys::RUNNING_KEY] {
            for job_id in conn.lrange::<_, Vec<u64>>(*queue_key, 0, -1).await? {
                pipe.hget(
                    RedisJob::new(job_id).key(),
                    &[job::Field::Queue, job::Field::Status],
                );
            }
        }

        // option used to allow for jobs being deleted between calls
        for (queue_name, status) in
            vec_from_redis_pipe::<C, (Option<String>, Option<job::Status>)>(conn, pipe).await?
        {
            let queue_name = match queue_name {
                Some(queue_name) => queue_name,
                None => continue,
            };
            let status = match status {
                Some(status) => status,
                None => continue,
            };
            let queue_info = queues_info
                .entry(queue_name)
                .or_insert_with(QueueInfo::default);
            queue_info.incr_status_count(&status);
        }

        let job_stats: JobStats = conn.get(&keys::STATS_KEYS).await?;
        Ok(ServerInfo {
            queues: queues_info,
            statistics: job_stats,
        })
    }

    /// Get one or more metadata fields from given job ID.
    ///
    /// If `None` is given as the `fields` argument, then get all fields.
    pub async fn job_fields<C: ConnectionLike>(
        conn: &mut C,
        job_id: u64,
        fields: Option<&[job::Field]>,
    ) -> OcyResult<job::JobMeta> {
        RedisJob::new(job_id).fields(conn, fields).await
    }

    /// Update one or more job metadata fields.
    ///
    /// Only following fields can be updated in this way:
    ///
    /// * status - used to mark job as completed/failed/cancelled etc.
    /// * output - used to update user provided information related to this job
    pub async fn update_job<C: ConnectionLike + Send>(
        conn: &mut C,
        job_id: u64,
        update_req: &job::UpdateRequest,
    ) -> OcyResult<()> {
        RedisJob::new(job_id).update(conn, update_req).await
    }

    /// Update a job's `last_heartbeat` field with the current date/time.
    pub async fn update_job_heartbeat<C: ConnectionLike + Send>(
        conn: &mut C,
        job_id: u64,
    ) -> OcyResult<()> {
        RedisJob::new(job_id).update_heartbeat(conn).await
    }

    /// Get the `status` field of given job.
    pub async fn job_status<C: ConnectionLike + Send>(
        conn: &mut C,
        job_id: u64,
    ) -> OcyResult<job::Status> {
        RedisJob::new(job_id).status(conn).await
    }

    /// Update a job's `status` field to the given status, if an allowed state transition.
    ///
    /// Identical to calling `update_job` and with `Some(status)` provided.
    pub async fn set_job_status<C: ConnectionLike + Send>(
        conn: &mut C,
        job_id: u64,
        status: &job::Status,
    ) -> OcyResult<()> {
        RedisJob::new(job_id).set_status(conn, status).await
    }

    /// Get the `output` field of given job.
    pub async fn job_output<C: ConnectionLike + Send>(
        conn: &mut C,
        job_id: u64,
    ) -> OcyResult<serde_json::Value> {
        RedisJob::new(job_id).output(conn).await
    }

    /// Update a job's `output` field to the given output data.
    ///
    /// Identical to calling `update_job` and with `Some(output)` provided.
    pub async fn set_job_output<C: ConnectionLike + Send>(
        conn: &mut C,
        job_id: u64,
        value: &serde_json::Value,
    ) -> OcyResult<()> {
        RedisJob::new(job_id).set_output(conn, value).await
    }

    // TODO: add an endpoint to get fields too?
    /// Get a list of jobs IDs with given tag name.
    pub async fn tagged_job_ids<C: ConnectionLike + Send>(
        conn: &mut C,
        tag_name: &str,
    ) -> OcyResult<Vec<u64>> {
        RedisTag::from_str(tag_name)?.tagged_job_ids(conn).await
    }

    /// Get list of all queue names.
    pub async fn queue_names<C: ConnectionLike + Send>(conn: &mut C) -> OcyResult<Vec<String>> {
        let mut names: Vec<String> = conn.smembers(keys::QUEUES_KEY).await?;
        names.sort();
        Ok(names)
    }

    /// Get given queue's current settings.
    pub async fn queue_settings<C: ConnectionLike + Send>(
        conn: &mut C,
        queue_name: &str,
    ) -> OcyResult<queue::Settings> {
        RedisQueue::from_string(queue_name)?
            .ensure_exists(conn)
            .await?
            .settings(conn)
            .await
    }

    /// Get the number of queues jobs in given queue.
    pub async fn queue_size<C: ConnectionLike + Send>(
        conn: &mut C,
        queue_name: &str,
    ) -> OcyResult<u64> {
        RedisQueue::from_string(queue_name)?
            .ensure_exists(conn)
            .await?
            .size(conn)
            .await
    }

    /// Get total number of running jobs across all queues.
    pub async fn running_queue_size<C: ConnectionLike + Send>(conn: &mut C) -> OcyResult<u64> {
        Ok(conn.llen(keys::RUNNING_KEY).await?)
    }

    /// Get total number of failed jobs across all queues.
    pub async fn failed_queue_size<C: ConnectionLike + Send>(conn: &mut C) -> OcyResult<u64> {
        Ok(conn.llen(keys::FAILED_KEY).await?)
    }

    /// Get total number of ended jobs across all queues.
    pub async fn ended_queue_size<C: ConnectionLike + Send>(conn: &mut C) -> OcyResult<u64> {
        Ok(conn.llen(keys::ENDED_KEY).await?)
    }

    pub async fn queue_job_ids<C: ConnectionLike + Send>(
        conn: &mut C,
        queue_name: &str,
    ) -> OcyResult<HashMap<job::Status, Vec<u64>>> {
        // TODO: check if this needs queue existence check
        RedisQueue::from_string(queue_name)?.job_ids(conn).await
    }

    /// Check all jobs in the failed queue for retries.
    ///
    /// Any which can be retried are re-queued on the queue they were created it.
    ///
    /// Any which have no automatic retries remaining are moved to the ended queue.
    pub async fn check_job_retries<C: ConnectionLike + Send>(conn: &mut C) -> OcyResult<Vec<u64>> {
        debug!("Checking for jobs to retry");
        let mut requeued: Vec<u64> = Vec::new();

        let mut pipeline = redis::pipe();
        let pipe = &mut pipeline;
        for job_id in conn.lrange::<_, Vec<u64>>(keys::FAILED_KEY, 0, -1).await? {
            pipe.hget(RedisJob::new(job_id).key(), job::RetryMeta::fields());
        }

        for retry_meta in vec_from_redis_pipe::<C, job::RetryMeta>(conn, pipe).await? {
            match retry_meta.retry_action() {
                job::RetryAction::Retry => {
                    let job = RedisJob::new(retry_meta.id());
                    if job.apply_retries(conn).await? {
                        requeued.push(job.id());
                    }
                }
                job::RetryAction::End => {
                    let job = RedisJob::new(retry_meta.id());
                    job.end_failed(conn).await?;
                }
                job::RetryAction::None => (),
            }
        }

        Ok(requeued)
    }

    /// Check all jobs in the running queue for timeouts.
    ///
    /// Any which timeout are moved to the failed queue, where they'll eventually either be retried, or moved to the
    /// ended queue.
    pub async fn check_job_timeouts<C: ConnectionLike + Send>(conn: &mut C) -> OcyResult<Vec<u64>> {
        debug!("Checking job timeouts");
        let mut timeouts: Vec<u64> = Vec::new();

        let mut pipeline = redis::pipe();
        let pipe = &mut pipeline;
        for job_id in conn.lrange::<_, Vec<u64>>(keys::RUNNING_KEY, 0, -1).await? {
            pipe.hget(RedisJob::new(job_id).key(), job::TimeoutMeta::fields());
        }

        for timeout_meta in vec_from_redis_pipe::<C, job::TimeoutMeta>(conn, pipe).await? {
            if timeout_meta.has_timed_out() {
                let job = RedisJob::new(timeout_meta.id());
                if job.apply_timeouts(conn).await? {
                    timeouts.push(job.id());
                }
            }
        }

        Ok(timeouts)
    }

    /// Check all jobs in the ended queue for expiry. Any expired jobs will be entirely removed from the queue system.
    pub async fn check_job_expiry<C: ConnectionLike + Send>(conn: &mut C) -> OcyResult<Vec<u64>> {
        debug!("Checking for expired jobs");
        let mut expired: Vec<u64> = Vec::new();

        let mut pipeline = redis::pipe();
        let pipe = &mut pipeline;
        for job_id in conn.lrange::<_, Vec<u64>>(keys::ENDED_KEY, 0, -1).await? {
            pipe.hget(RedisJob::new(job_id).key(), job::ExpiryMeta::fields());
        }

        for expiry_meta in vec_from_redis_pipe::<C, job::ExpiryMeta>(conn, pipe).await? {
            if expiry_meta.should_expire() {
                let job = RedisJob::new(expiry_meta.id());
                if job.apply_expiry(conn).await? {
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
    pub async fn check_db_integrity<C: ConnectionLike + Send>(conn: &mut C) -> OcyResult<()> {
        for queue_name in Self::queue_names(conn).await? {
            let queue = RedisQueue::from_string(&queue_name)?;
            if !(queue.exists(conn).await?) {
                warn!(
                    "Queue '{}' found in {}, but not as key",
                    queue_name,
                    keys::QUEUES_KEY
                );
            }
        }

        let mut iter: redis::AsyncIter<String> = conn.scan_match::<_, String>("queue:*").await?;
        let mut queues = Vec::new();
        while let Some(queue_key) = iter.next_item().await {
            if queue_key.ends_with(":jobs") {
                continue;
            }
            queues.push(queue_key);
        }

        for queue_key in queues {
            if !conn
                .sismember::<_, _, bool>(keys::QUEUES_KEY, &queue_key[6..])
                .await?
            {
                warn!(
                    "Queue '{}' found as key, but not in {}",
                    &queue_key,
                    keys::QUEUES_KEY
                );
            }
        }

        let _: () = transaction_async!(conn, &[keys::RUNNING_KEY], {
            let mut pipe = redis::pipe();
            let pipe_ref = pipe.atomic();

            for job_id in conn.lrange::<_, Vec<u64>>(keys::RUNNING_KEY, 0, -1).await? {
                pipe_ref.hget(
                    RedisJob::new(job_id).key(),
                    &[job::Field::Id, job::Field::Status, job::Field::StartedAt],
                );
            }

            let info: Vec<(Option<u64>, Option<job::Status>, Option<DateTime>)> =
                vec_from_redis_pipe(conn, pipe_ref).await?;
            for (job_id, status, started_at) in info {
                let job_id = match job_id {
                    Some(job_id) => job_id,
                    None => {
                        warn!(
                            "Found job in {} queue, but did not find key",
                            keys::RUNNING_KEY
                        );
                        continue;
                    }
                };

                match status {
                    Some(job::Status::Running) => (),
                    Some(status) => {
                        warn!("Found status '{}' in {} queue", status, keys::RUNNING_KEY)
                    }
                    None => warn!(
                        "Found job {} in {} queue, but did not find key",
                        job_id,
                        keys::RUNNING_KEY
                    ),
                }

                if started_at.is_none() {
                    warn!(
                        "Found job {} in {} queue, but job has no started_at",
                        job_id,
                        keys::RUNNING_KEY
                    );
                }
            }

            Some(())
        });

        let _: () = transaction_async!(conn, &[keys::FAILED_KEY], {
            let mut pipe = redis::pipe();
            let pipe_ref = pipe.atomic();

            for job_id in conn.lrange::<_, Vec<u64>>(keys::FAILED_KEY, 0, -1).await? {
                pipe_ref.hget(
                    RedisJob::new(job_id).key(),
                    &[job::Field::Id, job::Field::Status, job::Field::EndedAt],
                );
            }

            let info: Vec<(Option<u64>, Option<job::Status>, Option<DateTime>)> =
                vec_from_redis_pipe(conn, pipe_ref).await?;
            for (job_id, status, ended_at) in info {
                let job_id = match job_id {
                    Some(job_id) => job_id,
                    None => {
                        warn!(
                            "Found job in {} queue, but did not find key",
                            keys::FAILED_KEY
                        );
                        continue;
                    }
                };

                match status {
                    Some(job::Status::Failed) | Some(job::Status::TimedOut) => (),
                    Some(status) => {
                        warn!("Found status '{}' in {} queue", status, keys::FAILED_KEY)
                    }
                    None => warn!(
                        "Found job {} in {} queue, but did not find key",
                        job_id,
                        keys::FAILED_KEY
                    ),
                }

                if ended_at.is_none() {
                    warn!(
                        "Found job {} in {} queue, but job has no ended_at",
                        job_id,
                        keys::FAILED_KEY
                    );
                }
            }

            Some(())
        });

        let _: () = transaction_async!(conn, &[keys::ENDED_KEY], {
            let mut pipe = redis::pipe();
            let pipe_ref = pipe.atomic();

            for job_id in conn.lrange::<_, Vec<u64>>(keys::ENDED_KEY, 0, -1).await? {
                pipe_ref.hget(
                    RedisJob::new(job_id).key(),
                    &[job::Field::Id, job::Field::Status, job::Field::EndedAt],
                );
            }

            let info: Vec<(Option<u64>, Option<job::Status>, Option<DateTime>)> =
                vec_from_redis_pipe(conn, pipe_ref).await?;
            for (job_id, status, ended_at) in info {
                let job_id = match job_id {
                    Some(job_id) => job_id,
                    None => {
                        warn!(
                            "Found job in {} queue, but did not find key",
                            keys::ENDED_KEY
                        );
                        continue;
                    }
                };

                match status {
                    Some(job::Status::Failed)
                    | Some(job::Status::TimedOut)
                    | Some(job::Status::Completed)
                    | Some(job::Status::Cancelled) => (),
                    Some(status) => warn!("Found status '{}' in {} queue", status, keys::ENDED_KEY),
                    None => warn!(
                        "Found job {} in {} queue, but did not find key",
                        job_id,
                        keys::ENDED_KEY
                    ),
                }

                if ended_at.is_none() {
                    warn!(
                        "Found job {} in {} queue, but job has no started_at",
                        job_id,
                        keys::ENDED_KEY
                    );
                }
            }

            Some(())
        });

        Ok(())
    }

    /// Check connection to Redis using ping command.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::unit_arg))]
    pub async fn check_ping<C: ConnectionLike>(conn: &mut C) -> OcyResult<()> {
        Ok(redis::cmd("PING").query_async(conn).await?)
    }

    /// Fetch the next job from given queue, if any.
    ///
    /// # Returns
    ///
    /// A `job::Payload` if a job is found, or `None` if the queue is empty.
    pub async fn next_queued_job<C: ConnectionLike + Send>(
        conn: &mut C,
        queue_name: &str,
    ) -> OcyResult<Option<job::Payload>> {
        debug!("Client requested job from queue={}", queue_name);
        // queue can be deleted between these two calls, but will just return no job, so harmless
        let queue = RedisQueue::from_string(queue_name)?
            .ensure_exists(conn)
            .await?;
        let job = match conn
            .rpoplpush::<_, Option<u64>>(queue.jobs_key(), keys::LIMBO_KEY)
            .await?
        {
            Some(job_id) => RedisJob::new(job_id),
            None => return Ok(None),
        };
        debug!(
            "[{}{}] moved from {} -> {}",
            keys::JOB_PREFIX,
            job.id(),
            queue.jobs_key(),
            keys::LIMBO_KEY
        );

        // if Redis goes down before the following, job will be left in limbo, requeued at startup
        let job_payload: job::Payload = transaction_async!(conn, &[&job.key], {
            let input: Option<String> = conn.hget(&job.key, job::Field::Input).await?;
            let payload =
                job::Payload::new(job.id(), input.map(|s| serde_json::from_str(&s).unwrap()));

            let result: Option<()> = redis::pipe()
                .atomic()
                .hset(&job.key, job::Field::Status, job::Status::Running)
                .hset(&job.key, job::Field::StartedAt, DateTime::now())
                .lrem(keys::LIMBO_KEY, 1, job.id())
                .rpush(keys::RUNNING_KEY, job.id())
                .query_async(conn)
                .await?;
            result.map(|_| payload)
        });

        info!("[{}{}] started", keys::JOB_PREFIX, job_payload.id());
        Ok(Some(job_payload))
    }

    /// Create a new job on given queue.
    pub async fn create_job<C: ConnectionLike + Send>(
        conn: &mut C,
        queue_name: &str,
        job_req: &job::CreateRequest,
    ) -> OcyResult<u64> {
        // TODO: use transaction to ensure that queue isn't deleted partway through job creation
        let queue = RedisQueue::from_string(queue_name)?
            .ensure_exists(conn)
            .await?;
        let queue_settings = queue.settings(conn).await?;
        let timeout = job_req.timeout.as_ref().unwrap_or(&queue_settings.timeout);
        let heartbeat_timeout = job_req
            .heartbeat_timeout
            .as_ref()
            .unwrap_or(&queue_settings.heartbeat_timeout);
        let expires_after = job_req
            .expires_after
            .as_ref()
            .unwrap_or(&queue_settings.expires_after);
        let retries = job_req.retries.unwrap_or(queue_settings.retries);
        let retry_delays = match job_req.retry_delays.clone() {
            Some(rd) => rd,
            None => Vec::new(),
        };

        let job = RedisJob::new(conn.incr(keys::JOB_ID_KEY, 1).await?);
        debug!(
            "Creating job with job_id={} on queue={}",
            job.id(),
            &queue.name
        );

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
            pipe.hset(
                &job.key,
                job::Field::RetryDelays,
                retry_delays_json.to_string(),
            );
        }

        pipe.query_async(conn).await?;

        info!("[{}] [{}] created", &queue.key, &job.key);
        Ok(job.id())
    }
}
