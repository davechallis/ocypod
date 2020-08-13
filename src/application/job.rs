//! Defines most application logic that's based around jobs.

use log::{debug, info};
use redis::{aio::ConnectionLike, AsyncCommands, Pipeline, RedisWrite, ToRedisArgs};

use super::{keys, RedisQueue, RedisTag};
use crate::models::{job, DateTime, OcyError, OcyResult};
use crate::transaction_async;

/// Convenient wrapper struct for combing a job ID plus a connection.
#[derive(Debug)]
pub struct RedisJob {
    /// ID of the job.
    pub id: u64,

    /// Redis key used to store the job under. Can be derived from the ID, but stored separately
    /// avoid having to regenerate it each time it's needed.
    pub key: String,
}

impl ToRedisArgs for &RedisJob {
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        self.key.to_redis_args()
    }

    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        self.key.write_redis_args(out)
    }
}

impl RedisJob {
    /// Create a new RedisJob with given ID and given connection. This ID may or may not exist.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            key: Self::build_key(id),
        }
    }

    /// Get a populated `JobMeta` from given connection by getting data for a given hash key and fields.
    pub async fn metadata<C>(&self, conn: &mut C, fields: &[job::Field]) -> OcyResult<job::JobMeta>
    where
        C: ConnectionLike,
    {
        let (fields, hidden_fields): (Vec<job::Field>, Vec<job::Field>) = {
            let mut fields: Vec<job::Field> = fields.to_vec();
            let mut hidden_fields = Vec::new();

            // TODO: better handling of internal vs. visible fields here, this is all pretty messy
            // ended is a derived field from retries and status, so ensure they're present
            if fields.contains(&job::Field::Ended) {
                // if caller didn't request retries, ensure it's a hidden field
                if !fields.contains(&job::Field::Retries) {
                    hidden_fields.push(job::Field::Retries);
                    fields.push(job::Field::Retries);
                }

                // if caller didn't request status, ensure it's a hidden field
                if !fields.contains(&job::Field::Status) {
                    hidden_fields.push(job::Field::Status);
                    fields.push(job::Field::Status);
                }
            }

            (fields, hidden_fields)
        };
        let (id, value): (Option<u64>, redis::Value) = redis::pipe()
            .atomic()
            .hget(&self.key, job::Field::Id)
            .hget(&self.key, fields.as_slice())
            .query_async(conn)
            .await?;

        if id.is_none() {
            return Err(OcyError::NoSuchJob(self.id));
        }

        Ok(job::JobMeta::from_redis_value(
            &fields,
            &value,
            &hidden_fields,
        )?)
    }

    /// Get subset of metadata needed to check whether this job has timed out or not.
    pub async fn timeout_metadata<C>(&self, conn: &mut C) -> OcyResult<job::TimeoutMeta>
    where
        C: ConnectionLike + Send,
    {
        Ok(job::TimeoutMeta::from_conn(conn, self).await?)
    }

    /// Get this job's ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get this job's Redis key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Create a Redis key for a job from a job ID.
    pub fn build_key(id: u64) -> String {
        format!("{}{}", keys::JOB_PREFIX, id)
    }

    /// Update this job's status and/or output from a given request.
    pub async fn update<C>(&self, conn: &mut C, update_req: &job::UpdateRequest) -> OcyResult<()>
    where
        C: ConnectionLike + Send,
    {
        debug!("[{}] update request: {:?}", &self.key, update_req);
        let _: () = transaction_async!(conn, &[&self.key], {
            let mut pipe = redis::pipe();
            let pipe_ref = pipe.atomic();
            if let Some(ref output) = update_req.output {
                self.set_output_in_pipe(conn, pipe_ref, output).await?;
            }

            if let Some(ref status) = update_req.status {
                self.set_status_in_pipe(conn, pipe_ref, status).await?;
                info!("[{}] {}", &self.key, status);
            }

            pipe.query_async(conn).await?
        });
        Ok(())
    }

    /// Add commands to a pipeline to mark this job as completed.
    ///
    /// Note: caller is responsible for ensuring job exists and status change is valid before this is called.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_lifetimes))]
    pub fn complete<'b>(&self, pipe: &'b mut Pipeline) -> &'b mut Pipeline {
        pipe.hset(&self.key, job::Field::Status, job::Status::Completed)
            .hset(&self.key, job::Field::EndedAt, DateTime::now())
            .lrem(keys::RUNNING_KEY, 1, self.id)
            .lpush(keys::ENDED_KEY, self.id)
            .incr(keys::STAT_JOBS_COMPLETED_KEY, 1)
    }

    /// Add commands to pipeline to re-queue this job so that it can be retried later.
    ///
    /// If `incr_retries` is true, then increment the count of retry attempts for this job. This will
    /// typically be done for automatic retries, but not for manually requested retries.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_lifetimes))]
    pub async fn requeue<'b, C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        pipe: &'b mut Pipeline,
        incr_retries: bool,
    ) -> OcyResult<&'b mut Pipeline> {
        let queue = self.queue(conn).await?.ensure_exists(conn).await?; // ensure both job and queue exist

        pipe.hdel(
            &self.key,
            &[
                job::Field::StartedAt,
                job::Field::EndedAt,
                job::Field::LastHeartbeat,
                job::Field::Output,
            ],
        )
        .hset(&self.key, job::Field::Status, job::Status::Queued)
        .lrem(keys::FAILED_KEY, 1, self.id)
        .lrem(keys::ENDED_KEY, 1, self.id)
        .lpush(&queue.jobs_key, self.id)
        .incr(keys::STAT_JOBS_RETRIED_KEY, 1);

        if incr_retries {
            pipe.hincr(&self.key, job::Field::RetriesAttempted, 1);
        }

        Ok(pipe)
    }

    /// Add commands to pipeline to re-queue this job so that it can be retried later.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_lifetimes))]
    pub async fn cancel<'b, C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        pipe: &'b mut Pipeline,
    ) -> OcyResult<&'b mut Pipeline> {
        let queue = self.queue(conn).await?; // only present if job exists

        Ok(pipe
            .hset(&self.key, job::Field::Status, job::Status::Cancelled)
            .hset(&self.key, job::Field::EndedAt, DateTime::now())
            .lrem(keys::RUNNING_KEY, 1, self.id) // remove from running queue if present
            .lrem(keys::FAILED_KEY, 1, self.id) // remove from failed queue if present
            .lrem(&queue.jobs_key, 1, self.id) // remove from original queue if present
            .rpush(keys::ENDED_KEY, self.id) // add to ended queue
            .incr(keys::STAT_JOBS_CANCELLED_KEY, 1))
    }

    /// Add commands to pipeline to mark this job as failed or timed out.
    ///
    /// Note: caller is responsible for ensuring job exists and status change is valid before this is called.
    pub fn fail<'b>(&self, pipe: &'b mut Pipeline, status: &job::Status) -> &'b mut Pipeline {
        assert!(status == &job::Status::TimedOut || status == &job::Status::Failed);
        let stats_key = match status {
            job::Status::TimedOut => keys::STAT_JOBS_TIMED_OUT_KEY,
            job::Status::Failed => keys::STAT_JOBS_FAILED_KEY,
            _ => panic!("fail() was called with invalid status of: {}", status),
        };

        pipe.hset(&self.key, job::Field::Status, status)
            .hset(&self.key, job::Field::EndedAt, DateTime::now())
            .lrem(keys::RUNNING_KEY, 1, self.id)
            .rpush(keys::FAILED_KEY, self.id)
            .incr(stats_key, 1)
    }

    /// Move this job to the ended queue if it's failed or timed out.
    pub async fn end_failed<C: ConnectionLike + Send>(&self, conn: &mut C) -> OcyResult<bool> {
        let result: bool = transaction_async!(conn, &[&self.key], {
            let mut pipe = redis::pipe();
            let pipe_ref = pipe.atomic();
            match self.status(conn).await {
                Ok(job::Status::Failed) | Ok(job::Status::TimedOut) => {
                    let result: Option<()> = pipe_ref
                        .lrem(keys::FAILED_KEY, 1, self.id)
                        .rpush(keys::ENDED_KEY, self.id)
                        .query_async(conn)
                        .await?;
                    result.map(|_| true)
                }
                Ok(_) => Some(false), // jobs status already changed
                Err(OcyError::NoSuchJob(_)) => Some(false), // job already deleted
                Err(err) => return Err(err),
            }
        });
        Ok(result)
    }

    /// Get some or all fields of this job's metadata.
    ///
    /// If `None` is given, then all fields are fetched.
    pub async fn fields<C: ConnectionLike>(
        &self,
        conn: &mut C,
        fields: Option<&[job::Field]>,
    ) -> OcyResult<job::JobMeta> {
        debug!("Fetching job info for job_id={}", self.id);
        let fields = fields.unwrap_or_else(|| job::Field::all_fields());
        self.metadata(conn, fields).await
    }

    /// Get queue this job was created in. This queue may or may not exist.
    pub async fn queue<C: ConnectionLike + Send>(&self, conn: &mut C) -> OcyResult<RedisQueue> {
        match conn
            .hget::<_, _, Option<String>>(&self.key, job::Field::Queue)
            .await?
        {
            Some(queue) => Ok(RedisQueue::from_string(queue)?),
            None => Err(OcyError::NoSuchJob(self.id)),
        }
    }

    /// Get this job's output field.
    pub async fn output<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
    ) -> OcyResult<serde_json::Value> {
        debug!("Getting job output for job_id={}", self.id);

        // if no output set, return null
        let (exists, output): (bool, Option<String>) = redis::pipe()
            .atomic()
            .exists(&self.key)
            .hget(&self.key, job::Field::Output)
            .query_async(conn)
            .await?;

        if exists {
            match output {
                // JSON parse error should never happen unless someone manually writes data to Redis outside of Ocypod.
                Some(ref s) => Ok(serde_json::from_str(s)?),
                None => Ok(serde_json::Value::Null),
            }
        } else {
            Err(OcyError::NoSuchJob(self.id))
        }
    }

    /// Update this job's output field in a transaction.
    pub async fn set_output<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        value: &serde_json::Value,
    ) -> OcyResult<()> {
        let _: () = transaction_async!(conn, &[&self.key], {
            let mut pipe = redis::pipe();
            let pipe_ref = self.set_output_in_pipe(conn, pipe.atomic(), &value).await?;
            pipe_ref.query_async(conn).await?
        });
        Ok(())
    }

    /// Add commands to update this job's output to a pipeline.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_lifetimes))]
    pub async fn set_output_in_pipe<'b, C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        pipe: &'b mut Pipeline,
        value: &serde_json::Value,
    ) -> OcyResult<&'b mut Pipeline> {
        match self.status(conn).await? {
            job::Status::Running => Ok(pipe.hset(&self.key, job::Field::Output, value.to_string())),
            _ => Err(OcyError::conflict("Can only set output for running jobs")),
        }
    }

    /// Get this job's status field.
    pub async fn status<C: ConnectionLike + Send>(&self, conn: &mut C) -> OcyResult<job::Status> {
        debug!("Fetching job status for job_id={}", self.id);
        conn.hget::<_, _, Option<job::Status>>(&self.key, job::Field::Status)
            .await?
            .ok_or_else(|| OcyError::NoSuchJob(self.id))
    }

    /// Update this job's status field in a transaction.
    pub async fn set_status<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        status: &job::Status,
    ) -> OcyResult<()> {
        // retrying jobs relies on original queue still existing
        let watch_keys = match status {
            job::Status::Queued => vec![
                self.key.to_owned(),
                self.queue(conn).await?.jobs_key,
            ],
            _ => vec![self.key.to_owned()],
        };

        let _: () = transaction_async!(conn, &watch_keys[..], {
            self.set_status_in_pipe(conn, redis::pipe().atomic(), status)
                .await?
                .query_async(conn)
                .await?
        });
        info!("[{}] {}", &self.key, status);
        Ok(())
    }

    /// Add commands to update this job's status to a pipeline.
    ///
    /// Checks the validity of status transitions.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_lifetimes))]
    pub async fn set_status_in_pipe<'b, C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        pipe: &'b mut Pipeline,
        status: &job::Status,
    ) -> OcyResult<&'b mut Pipeline> {
        let current_status = self.status(conn).await?; // only present if job exists
        debug!(
            "Status update for job_id={}, {} -> {}",
            self.id, &current_status, &status
        );

        // ensure status transitions are valid
        Ok(match (current_status, status) {
            (job::Status::Running, job::Status::Completed) => self.complete(pipe),
            (job::Status::Running, cause @ job::Status::Failed) => self.fail(pipe, cause),
            (job::Status::Running, cause @ job::Status::TimedOut) => self.fail(pipe, cause),
            (job::Status::Running, job::Status::Cancelled) => self.cancel(conn, pipe).await?,
            (job::Status::Failed, job::Status::Cancelled) => self.cancel(conn, pipe).await?,
            (job::Status::Queued, job::Status::Cancelled) => self.cancel(conn, pipe).await?,
            (job::Status::Cancelled, job::Status::Queued) => {
                self.requeue(conn, pipe, false).await?
            }
            (job::Status::Failed, job::Status::Queued) => self.requeue(conn, pipe, false).await?,
            (job::Status::TimedOut, job::Status::Queued) => self.requeue(conn, pipe, false).await?,
            (from, to) => {
                return Err(OcyError::conflict(format!("Cannot change status from {} to {}", from, to)))
            }
        })
    }

    /// Updates a job's last heartbeat date/time.
    pub async fn update_heartbeat<C: ConnectionLike + Send>(&self, conn: &mut C) -> OcyResult<()> {
        let _: () = transaction_async!(conn, &[&self.key], {
            // only existing/running jobs can have heartbeat updated
            if self.status(conn).await? != job::Status::Running {
                return Err(OcyError::conflict(format!("Cannot heartbeat job {}, job is not running", self.id)));
            }

            redis::pipe()
                .atomic()
                .hset(&self.key, job::Field::LastHeartbeat, DateTime::now())
                .ignore()
                .query_async(conn)
                .await?
        });
        debug!("[{}] updated heartbeat", &self.key);
        Ok(())
    }

    /// Expires a job in a transaction.
    ///
    /// Callers should typically check for job expiry outside of a transaction (and probably in a pipeline),
    /// then only call this on jobs to expire (since transaction here is much more expensive).
    pub async fn apply_expiry<C: ConnectionLike + Send>(&self, conn: &mut C) -> OcyResult<bool> {
        let expired: bool = transaction_async!(conn, &[&self.key], {
            if job::ExpiryMeta::from_conn(conn, &self.key)
                .await?
                .should_expire()
            {
                let result: Option<()> = self
                    .delete_in_pipe(conn, redis::pipe().atomic())
                    .await?
                    .query_async(conn)
                    .await?;
                result.map(|_| true)
            } else {
                Some(false)
            }
        });

        if expired {
            info!("[{}] expired, removed from DB", &self.key);
        }
        Ok(expired)
    }

    /// Times out a job in a transaction.
    ///
    /// Callers should typically check for job timeout outside of a transaction (and probably in a pipeline),
    /// then only call this on jobs to timeout (since transaction here is much more expensive).
    pub async fn apply_timeouts<C: ConnectionLike + Send>(&self, conn: &mut C) -> OcyResult<bool> {
        let timed_out: bool = transaction_async!(conn, &[&self.key], {
            // timeouts will typically be checked outside of a transaction for performance, then checked again
            // on timeout to confirm that this job should still timeout within a transaction
            if job::TimeoutMeta::from_conn(conn, &self.key)
                .await?
                .has_timed_out()
            {
                let result: Option<()> = self
                    .fail(redis::pipe().atomic(), &job::Status::TimedOut)
                    .query_async(conn)
                    .await?;
                result.map(|_| true)
            } else {
                Some(false)
            }
        });

        if timed_out {
            info!("[{}] timed out", self.key);
        }
        Ok(timed_out)
    }

    /// Retries out a job in a transaction.
    ///
    /// Callers should typically check for job retries outside of a transaction (and probably in a pipeline),
    /// then only call this on jobs to retry (since transaction here is much more expensive).
    pub async fn apply_retries<C: ConnectionLike + Send>(&self, conn: &mut C) -> OcyResult<bool> {
        let queue = self.queue(conn).await?;
        let result: bool = transaction_async!(conn, &[&self.key, &queue.key], {
            // retries will typically be checked outside of a transaction for performance, then checked again
            // on retry to confirm that this job should still be retried within a transaction
            match job::RetryMeta::from_conn(conn, &self.key)
                .await?
                .retry_action()
            {
                job::RetryAction::Retry => {
                    // cannot requeue if original queue no longer exists
                    if !queue.exists(conn).await? {
                        // TODO: leave job in failed? Or move to ended?
                        return Ok(false);
                    }

                    let result: Option<()> = self
                        .requeue(conn, redis::pipe().atomic(), true)
                        .await?
                        .query_async(conn)
                        .await?;
                    result.map(|_| true)
                }
                _ => Some(false),
            }
        });
        Ok(result)
    }

    /// Deletes this job from the server in a transaction.
    ///
    /// # Returns
    ///
    /// Returns `true` if this job was deleted, `false` if the job wasn't found.
    pub async fn delete<C: ConnectionLike + Send>(&self, conn: &mut C) -> OcyResult<bool> {
        let result: bool = transaction_async!(conn, &[&self.key], {
            // delete from queue here if job still exists, doesn't matter if queue doesn't exist
            let mut pipe = redis::pipe();
            let pipe_ref = pipe.atomic();
            match self.delete_in_pipe(conn, pipe_ref).await {
                Ok(p) => {
                    let (num_deleted,): (Option<u8>,) = p.query_async(conn).await?;
                    num_deleted.map(|n| n > 0)
                }
                Err(OcyError::NoSuchJob(_)) => Some(false), // job already deleted
                Err(err) => return Err(err),
            }
        });
        Ok(result)
    }

    /// Add commands to delete this job status to a pipeline.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_lifetimes))]
    pub async fn delete_in_pipe<'b, C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        pipe: &'b mut Pipeline,
    ) -> OcyResult<&'b mut Pipeline> {
        let (queue, tags): (Option<String>, Option<String>) = conn
            .hget(&self.key, &[job::Field::Queue, job::Field::Tags])
            .await?;

        if let Some(queue) = queue {
            pipe.lrem(RedisQueue::build_jobs_key(&queue), 1, self.id)
                .ignore();
        } else {
            // queue is mandatory field, if missing then means job has been deleted
            return Err(OcyError::NoSuchJob(self.id));
        }

        // delete job itself, and remove it from all global queues it might be in
        pipe.del(&self.key) // always delete job itself
            .lrem(keys::FAILED_KEY, 1, self.id)
            .ignore()
            .lrem(keys::RUNNING_KEY, 1, self.id)
            .ignore()
            .lrem(keys::ENDED_KEY, 1, self.id)
            .ignore();

        if let Some(tags) = tags {
            for tag in serde_json::from_str::<Vec<&str>>(&tags).unwrap() {
                pipe.srem(RedisTag::build_key(tag), self.id).ignore(); // delete all tags
            }
        }

        Ok(pipe)
    }
}