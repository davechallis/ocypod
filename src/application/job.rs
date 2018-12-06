//! Defines most application logic that's based around jobs.

use redis::{Commands, Pipeline, ToRedisArgs, PipelineCommands};

use crate::models::{DateTime, OcyError, OcyResult, job};
use crate::redis_utils::transaction;
use super::{RedisQueue, RedisTag, keys};

/// Convenient wrapper struct for combing a job ID plus a connection.
pub struct RedisJob<'a> {
    pub id: u64,
    conn: &'a redis::Connection,
    pub key: String,
}

impl<'a> ToRedisArgs for &'a RedisJob<'a> {
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        self.key.to_redis_args()
    }

    fn write_redis_args(&self, out: &mut Vec<Vec<u8>>) {
        self.key.write_redis_args(out)
    }
}


impl<'a> RedisJob<'a> {
    /// Create a new RedisJob with given ID and given connection. This ID may or may not exist.
    pub fn new(id: u64, conn: &'a redis::Connection) -> Self {
        Self { id, conn, key: Self::build_key(id) }
    }

    /// Get a populated `JobMeta` from given connection by getting data for a given hash key and fields.
    pub fn metadata(&self, fields: &[job::Field]) -> OcyResult<job::JobMeta> {
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
            .hget(self, job::Field::Id)
            .hget(self, fields.as_slice())
            .query(self.conn)?;

        if id.is_none() {
            return Err(OcyError::NoSuchJob(self.id));
        }

        Ok(job::JobMeta::from_redis_value(&fields, &value, &hidden_fields)?)
    }

    /// Get subset of metadata needed to check whether this job has timed out or not.
    pub fn timeout_metadata(&self) -> OcyResult<job::TimeoutMeta> {
        Ok(job::TimeoutMeta::from_conn(self.conn, self)?)
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
    pub fn update(&self, update_req: &job::UpdateRequest) -> OcyResult<()> {
        debug!("[{}] update request: {:?}", &self.key, update_req);
        transaction(self.conn, &[&self.key], |pipe| {
            if let Some(ref output) = update_req.output {
                self.set_output_in_pipe(pipe, output)?;
            }

            if let Some(ref status) = update_req.status {
                self.set_status_in_pipe(pipe, status)?;
                info!("[{}] {}", &self.key, status);
            }

            Ok(pipe.query(self.conn)?)
        })?;
        Ok(())
    }

    /// Add commands to a pipeline to mark this job as completed.
    ///
    /// Note: caller is responsible for ensuring job exists and status change is valid before this is called.
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
    pub fn requeue<'b>(&self, pipe: &'b mut Pipeline, incr_retries: bool) -> OcyResult<&'b mut Pipeline> {
        let queue = self.queue()?.ensure_exists()?; // ensure both job and queue exist

        pipe.hdel(&self.key, &[job::Field::StartedAt,
            job::Field::EndedAt,
            job::Field::LastHeartbeat,
            job::Field::Output])
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
    pub fn cancel<'b>(&self, pipe: &'b mut Pipeline) -> OcyResult<&'b mut Pipeline> {
        let queue = self.queue()?; // only present if job exists

        Ok(pipe.hset(&self.key, job::Field::Status, job::Status::Cancelled)
            .hset(&self.key, job::Field::EndedAt, DateTime::now())
            .lrem(keys::RUNNING_KEY, 1, self.id)       // remove from running queue if present
            .lrem(keys::FAILED_KEY, 1, self.id)        // remove from failed queue if present
            .lrem(&queue.jobs_key, 1, self.id)  // remove from original queue if present
            .rpush(keys::ENDED_KEY, self.id)           // add to ended queue
            .incr(keys::STAT_JOBS_CANCELLED_KEY, 1))
    }

    /// Add commands to pipeline to mark this job as failed or timed out.
    ///
    /// Note: caller is responsible for ensuring job exists and status change is valid before this is called.
    pub fn fail<'b>(&self, pipe: &'b mut Pipeline, status: &job::Status) -> &'b mut Pipeline {
        assert!(status == &job::Status::TimedOut || status == &job::Status::Failed);
        let stats_key = match status {
            job::Status::TimedOut => keys::STAT_JOBS_TIMED_OUT_KEY,
            job::Status::Failed   => keys::STAT_JOBS_FAILED_KEY,
            _                     => panic!("fail() was called with invalid status of: {}", status),
        };

        pipe.hset(&self.key, job::Field::Status, status)
            .hset(&self.key, job::Field::EndedAt, DateTime::now())
            .lrem(keys::RUNNING_KEY, 1, self.id)
            .rpush(keys::FAILED_KEY, self.id)
            .incr(stats_key, 1)
    }

    /// Move this job to the ended queue if it's failed or timed out.
    pub fn end_failed(&self) -> OcyResult<bool> {
        Ok(transaction(self.conn, &[&self.key], |pipe| {
            match self.status() {
                Ok(job::Status::Failed) | Ok(job::Status::TimedOut) => {
                    let result: Option<()> = pipe
                        .lrem(keys::FAILED_KEY, 1, self.id)
                        .rpush(keys::ENDED_KEY, self.id)
                        .query(self.conn)?;
                    Ok(result.map(|_| true))
                },
                Ok(_)                         => Ok(Some(false)), // jobs status already changed
                Err(OcyError::NoSuchJob(_)) => Ok(Some(false)), // job already deleted
                Err(err)                      => Err(err),
            }
        })?)
    }

    /// Get some or all fields of this job's metadata.
    ///
    /// If `None` is given, then all fields are fetched.
    pub fn fields(&self, fields: Option<&[job::Field]>) -> OcyResult<job::JobMeta> {
        debug!("Fetching job info for job_id={}", self.id);
        let fields = fields.unwrap_or_else(|| job::Field::all_fields());
        self.metadata(fields)
    }

    /// Get queue this job was created in. This queue may or may not exist.
    pub fn queue(&self) -> OcyResult<RedisQueue> {
        match self.conn.hget::<_, _, Option<String>>(&self.key, job::Field::Queue)? {
            Some(queue) => Ok(RedisQueue::from_string(queue, self.conn)?),
            None        => Err(OcyError::NoSuchJob(self.id)),
        }
    }

    /// Get this job's output field.
    pub fn output(&self) -> OcyResult<serde_json::Value> {
        debug!("Getting job output for job_id={}", self.id);

        // if no output set, return null
        let (exists, output): (bool, Option<String>) = redis::pipe()
            .atomic()
            .exists(&self.key)
            .hget(&self.key, job::Field::Output)
            .query(self.conn)?;

        if exists {
            match output {
                Some(ref s) => Ok(serde_json::from_str(s).unwrap()),
                None => Ok(serde_json::Value::Null),
            }
        } else {
            Err(OcyError::NoSuchJob(self.id))
        }
    }

    /// Update this job's output field in a transaction.
    pub fn set_output(&self, value: &serde_json::Value) -> OcyResult<()> {
        transaction(self.conn, &[&self.key], |pipe| {
            Ok(self.set_output_in_pipe(pipe, value)?.query(self.conn)?)
        })?;
        Ok(())
    }

    /// Add commands to update this job's output to a pipeline.
    pub fn set_output_in_pipe<'b>(
        &self,
        pipe: &'b mut Pipeline,
        value: &serde_json::Value
    ) -> OcyResult<&'b mut Pipeline> {
        match self.status()? {
            job::Status::Running => Ok(pipe.hset(&self.key, job::Field::Output, value.to_string())),
            _                    => Err(OcyError::BadRequest("Can only set output for running jobs".to_string())),
        }
    }

    /// Get this job's status field.
    pub fn status(&self) -> OcyResult<job::Status> {
        debug!("Fetching job status for job_id={}", self.id);
        self.conn.hget::<_, _, Option<job::Status>>(&self.key, job::Field::Status)?
            .ok_or_else(|| OcyError::NoSuchJob(self.id))
    }

    /// Update this job's status field in a transaction.
    pub fn set_status(&self, status: &job::Status) -> OcyResult<()> {
        // retrying jobs relies on original queue still existing
        let watch_keys = match status {
            job::Status::Queued => vec![self.key.to_owned(), self.queue()?.jobs_key.to_owned()],
            _                   => vec![self.key.to_owned()],
        };

        transaction(self.conn, &watch_keys, |pipe| {
            Ok(self.set_status_in_pipe(pipe, status)?.query(self.conn)?)
        })?;
        info!("[{}] {}", &self.key, status);
        Ok(())
    }

    /// Add commands to update this job's status to a pipeline.
    ///
    /// Checks the validity of status transitions.
    pub fn set_status_in_pipe<'b>(
        &self,
        pipe: &'b mut Pipeline,
        status: &job::Status
    ) -> OcyResult<&'b mut Pipeline> {
        let current_status = self.status()?; // only present if job exists
        debug!("Status update for job_id={}, {} -> {}", self.id, &current_status, &status);

        // ensure status transitions are valid
        Ok(match (current_status, status) {
            (job::Status::Running, job::Status::Completed)        => self.complete(pipe),
            (job::Status::Running, cause @ job::Status::Failed)   => self.fail(pipe, cause),
            (job::Status::Running, cause @ job::Status::TimedOut) => self.fail(pipe, cause),
            (job::Status::Running, job::Status::Cancelled)        => self.cancel(pipe)?,
            (job::Status::Failed, job::Status::Cancelled)         => self.cancel(pipe)?,
            (job::Status::Queued, job::Status::Cancelled)         => self.cancel(pipe)?,
            (job::Status::Cancelled, job::Status::Queued)         => self.requeue(pipe, false)?,
            (job::Status::Failed, job::Status::Queued)            => self.requeue(pipe, false)?,
            (job::Status::TimedOut, job::Status::Queued)          => self.requeue(pipe, false)?,
            (from, to) => return Err(OcyError::BadRequest(format!("Cannot change status from {} to {}", from, to))),
        })
    }

    /// Updates a job's last heartbeat date/time.
    pub fn update_heartbeat(&self) -> OcyResult<()> {
        transaction(self.conn, &[&self.key], |pipe| {
            // only existing/running jobs can have heartbeat updated
            if self.status()? != job::Status::Running {
                return Err(OcyError::BadRequest(format!("Cannot heartbeat job {}, job is not running", self.id)));
            }

            Ok(pipe
                .hset(&self.key, job::Field::LastHeartbeat, DateTime::now())
                .ignore()
                .query(self.conn)?)
        })?;
        debug!("[{}] updated heartbeat", &self.key);
        Ok(())
    }

    /// Expires a job in a transaction.
    ///
    /// Callers should typically check for job expiry outside of a transaction (and probably in a pipeline),
    /// then only call this on jobs to expire (since transaction here is much more expensive).
    pub fn apply_expiry(&self) -> OcyResult<bool> {
        let expired: bool = transaction(self.conn, &[&self.key], |pipe| {
            if job::ExpiryMeta::from_conn(self.conn, &self.key)?.should_expire() {
                let result: Option<()> = self.delete_in_pipe(pipe)?.query(self.conn)?;
                Ok(result.map(|_| true))
            } else {
                Ok(Some(false))
            }
        })?;

        if expired {
            info!("[{}] expired, removed from DB", &self.key);
        }
        Ok(expired)
    }

    /// Times out a job in a transaction.
    ///
    /// Callers should typically check for job timeout outside of a transaction (and probably in a pipeline),
    /// then only call this on jobs to timeout (since transaction here is much more expensive).
    pub fn apply_timeouts(&self) -> OcyResult<bool> {
        let timed_out: bool = transaction(self.conn, &[&self.key], |pipe| {
            // timeouts will typically be checked outside of a transaction for performance, then checked again
            // on timeout to confirm that this job should still timeout within a transaction
            if job::TimeoutMeta::from_conn(self.conn, &self.key)?.has_timed_out() {
                let result: Option<()> = self.fail(pipe, &job::Status::TimedOut).query(self.conn)?;
                Ok(result.map(|_| true))
            } else {
                Ok(Some(false))
            }
        })?;

        if timed_out {
            info!("[{}] timed out", self.key);
        }
        Ok(timed_out)
    }

    /// Retries out a job in a transaction.
    ///
    /// Callers should typically check for job retries outside of a transaction (and probably in a pipeline),
    /// then only call this on jobs to retry (since transaction here is much more expensive).
    pub fn apply_retries(&self) -> OcyResult<bool> {
        let queue = self.queue()?;
        Ok(transaction(self.conn, &[&self.key, &queue.key], |pipe| {
            // retries will typically be checked outside of a transaction for performance, then checked again
            // on retry to confirm that this job should still be retried within a transaction
            match job::RetryMeta::from_conn(self.conn, &self.key)?.retry_action() {
                job::RetryAction::Retry => {
                    // cannot requeue if original queue no longer exists
                    if !queue.exists()? {
                        // TODO: leave job in failed? Or move to ended?
                        return Ok(Some(false));
                    }

                    let result: Option<()> = self.requeue(pipe, true)?.query(self.conn)?;
                    Ok(result.map(|_| true))
                },
                _ => Ok(Some(false)),
            }
        })?)
    }

    /// Deletes this job from the server in a transaction.
    ///
    /// # Returns
    ///
    /// Returns `true` if this job was deleted, `false` if the job wasn't found.
    pub fn delete(&self) -> OcyResult<bool> {
        Ok(transaction(self.conn, &[&self.key], |pipe| {
            // delete from queue here if job still exists, doesn't matter if queue doesn't exist
            match self.delete_in_pipe(pipe) {
                Ok(pipe) => {
                    let (num_deleted,): (Option<u8>,) = pipe.query(self.conn)?;
                    Ok(num_deleted.map(|n| n > 0))
                },
                Err(OcyError::NoSuchJob(_)) => Ok(Some(false)), // job already deleted
                Err(err)                      => Err(err),
            }
        })?)
    }

    /// Add commands to delete this job status to a pipeline.
    pub fn delete_in_pipe<'b>(&self, pipe: &'b mut Pipeline) -> OcyResult<&'b mut Pipeline> {
        let (queue, tags): (Option<String>, Option<String>) =
            self.conn.hget(&self.key, &[job::Field::Queue, job::Field::Tags])?;

        if let Some(queue) = queue {
            pipe.lrem(RedisQueue::build_jobs_key(&queue), 1, self.id).ignore();
        } else {
            // queue is mandatory field, if missing then means job has been deleted
            return Err(OcyError::NoSuchJob(self.id));
        }

        // delete job itself, and remove it from all global queues it might be in
        pipe.del(&self.key) // always delete job itself
            .lrem(keys::FAILED_KEY, 1, self.id).ignore()
            .lrem(keys::RUNNING_KEY, 1, self.id).ignore()
            .lrem(keys::ENDED_KEY, 1, self.id).ignore();

        if let Some(tags) = tags {
            for tag in serde_json::from_str::<Vec<&str>>(&tags).unwrap() {
                pipe.srem(RedisTag::build_key(tag), self.id).ignore(); // delete all tags
            }
        }

        Ok(pipe)
    }
}
