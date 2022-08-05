//! Defines convenience interface to queue in Redis.

use std::collections::HashMap;

use log::{debug, info};
use redis::{aio::ConnectionLike, AsyncCommands, RedisResult};

use super::{RedisJob, RedisManager};
use crate::application::manager::QUEUE_JOBS_SUFFIX;
use crate::models::{job, queue, OcyError, OcyResult};
use crate::redis_utils::vec_from_redis_pipe;
use crate::transaction_async;

/// Interface to a queue in Redis. This consists of a list containing queued jobs, and a hash containing queue settings.
///
/// Primarily used by RedisManager as a wrapper around some queue information.
#[derive(Debug)]
pub struct RedisQueue<'a> {
    redis_manager: &'a RedisManager,

    /// Name of the queue.
    pub name: String,

    /// Redis key of the queue.
    pub key: String,

    /// Redis key used to store this queue's jobs under.
    pub jobs_key: String,
}

impl<'a> RedisQueue<'a> {
    /// Get a new RedisQueue struct, ensuring its name is valid.
    pub fn new<S: Into<String>>(redis_manager: &'a RedisManager, name: S) -> OcyResult<Self> {
        let name = name.into();
        if Self::is_valid_name(&name) {
            let key = Self::build_key(&redis_manager.queue_prefix, &name);
            let jobs_key = Self::build_jobs_key(&redis_manager.queue_prefix, &name);
            Ok(Self {
                redis_manager,
                name,
                key,
                jobs_key,
            })
        } else {
            Err(OcyError::bad_request( "Invalid queue name, valid characters: a-zA-Z0-9_.-"))
        }
    }

    /// Get key for this queue's jobs list in Redis.
    ///
    /// This contains queued job IDs.
    pub fn jobs_key(&self) -> &str {
        &self.jobs_key
    }

    // TODO: this list could probably be expanded a bit
    /// Validate queue name, allowed chars for names are: [a-zA-Z0-9_.-].
    pub fn is_valid_name(name: &str) -> bool {
        !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
    }

    /// Create a new queue with given settings, or update settings for an existing queue.
    ///
    /// Returns true if a new queue was created, or false if an existing queue was updated.
    pub async fn create_or_update<C: ConnectionLike>(
        &self,
        conn: &mut C,
        settings: &queue::Settings,
    ) -> OcyResult<bool> {
        debug!("[{}] writing settings: {:?}", &self.key, settings);

        let mut pipeline = redis::pipe();

        let pipe = pipeline
            .atomic()
            .hset(&self.key, queue::Field::Timeout, &settings.timeout)
            .hset(
                &self.key,
                queue::Field::HeartbeatTimeout,
                &settings.heartbeat_timeout,
            )
            .ignore()
            .hset(
                &self.key,
                queue::Field::ExpiresAfter,
                &settings.expires_after,
            )
            .ignore()
            .hset(&self.key, queue::Field::Retries, settings.retries)
            .ignore()
            .sadd(&self.redis_manager.queues_key, &self.name)
            .ignore();

        if settings.retry_delays.is_empty() {
            pipe.hdel(&self.key, queue::Field::RetryDelays).ignore();
        } else {
            let retry_delays_json: serde_json::Value = settings.retry_delays.as_slice().into();
            pipe.hset(
                &self.key,
                queue::Field::RetryDelays,
                retry_delays_json.to_string(),
            )
            .ignore();
        }

        let (is_new,): (bool,) = pipe.query_async(conn).await?;

        // all fields are mandatory, so if 1st is updated, this is a new entry
        info!(
            "[{}] {}",
            &self.key,
            if is_new { "created" } else { "updated" }
        );
        Ok(is_new)
    }

    /// Delete an existing queue, if it exists.
    ///
    /// Deletes any jobs currently in the queue, but won't affected any running/completed jobs.
    ///
    /// Returns true if a queue was deleted, false otherwise.
    pub async fn delete<C: ConnectionLike + Send>(&self, conn: &mut C) -> OcyResult<bool> {
        debug!("Deleting queue '{}'", self.name);
        let (queue_deleted, num_jobs_deleted): (bool, usize) =
            transaction_async!(conn, &[&self.jobs_key], {
                // if queue has already been deleted, nothing to do
                if !self.exists(conn).await? {
                    Some((false, 0))
                } else {
                    let mut keys_to_del: Vec<String> =
                        vec![self.key.to_owned(), self.jobs_key.to_owned()];

                    // fetch all tags for all jobs to delete in separate non-atomic/transactional pipeline
                    let mut tag_pipeline = redis::pipe();
                    let tag_pipe = &mut tag_pipeline;

                    let job_ids: Vec<u64> = conn.lrange(&self.jobs_key, 0, -1).await?;
                    for job_id in &job_ids {
                        let job_key = RedisJob::new(self.redis_manager, *job_id).key().to_owned();
                        tag_pipe.hget(&job_key, &[job::Field::Id, job::Field::Tags]);
                        keys_to_del.push(job_key);
                    }

                    let mut pipe = redis::pipe();
                    let pipe_ref = pipe.atomic();

                    let tagged_jobs: Vec<(Option<u64>, Option<String>)> =
                        vec_from_redis_pipe(conn, tag_pipe).await?;
                    for (job_id, tags) in tagged_jobs {
                        if let (Some(job_id), Some(tags)) = (job_id, tags) {
                            for tag in serde_json::from_str::<Vec<&str>>(&tags).unwrap() {
                                pipe_ref.srem(self.redis_manager.build_tag_key(tag)?, job_id);
                            }
                        }
                    }

                    let result: Option<()> = pipe_ref
                        .del(keys_to_del)
                        .srem(&self.redis_manager.queues_key, &self.name)
                        .query_async(conn)
                        .await?;
                    result.map(|_| (true, job_ids.len()))
                }
            });

        if queue_deleted {
            info!(
                "[{}] deleted ({} queued jobs deleted)",
                &self.key, num_jobs_deleted
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn job_ids<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
    ) -> OcyResult<HashMap<job::Status, Vec<u64>>> {
        let mut pipeline = redis::pipe();
        let pipe = &mut pipeline;
        let mut job_ids = HashMap::new();
        for status in &job::ALL_STATUSES {
            job_ids.insert(status.clone(), Vec::new());
        }

        for queue_key in &[
            &self.jobs_key,
            &self.redis_manager.failed_key,
            &self.redis_manager.ended_key,
            &self.redis_manager.running_key,
        ] {
            for job_id in conn.lrange::<_, Vec<u64>>(*queue_key, 0, -1).await? {
                pipe.hget(
                    RedisJob::new(self.redis_manager, job_id).key(),
                    &[job::Field::Id, job::Field::Queue, job::Field::Status],
                );
            }
        }

        for (job_id, queue_name, status) in
            vec_from_redis_pipe::<C, (Option<u64>, Option<String>, Option<job::Status>)>(conn, pipe)
                .await?
        {
            if queue_name.as_ref() != Some(&self.name) {
                continue;
            }

            let status = match status {
                Some(status) => status,
                None => continue,
            };

            let job_id = match job_id {
                Some(job_id) => job_id,
                None => continue,
            };

            job_ids.get_mut(&status).unwrap().push(job_id);
        }

        Ok(job_ids)
    }

    /// Get number of jobs currently queued.
    pub async fn size<C: ConnectionLike>(&self, conn: &mut C) -> OcyResult<u64> {
        let (exists, size): (bool, u64) = transaction_async!(conn, &[&self.key, &self.jobs_key], {
            redis::pipe()
                .atomic()
                .exists(&self.key) // check queue settings exist
                .llen(&self.jobs_key) // check length of queued jobs
                .query_async(conn)
                .await?
        });

        if exists {
            Ok(size)
        } else {
            Err(OcyError::NoSuchQueue(self.name.to_owned()))
        }
    }

    /// Get this queue's settings.
    pub async fn settings<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
    ) -> OcyResult<queue::Settings> {
        Ok(conn
            .hget(
                &self.key,
                &[
                    queue::Field::Timeout,
                    queue::Field::HeartbeatTimeout,
                    queue::Field::ExpiresAfter,
                    queue::Field::Retries,
                    queue::Field::RetryDelays,
                ],
            )
            .await?)
    }

    /// Check whether this queue exists in Redis or not.
    pub async fn exists<C: ConnectionLike + Send>(&self, conn: &mut C) -> RedisResult<bool> {
        conn.exists(&self.key).await
    }

    /// Return either this queue or an error if it doesn't exist.
    ///
    /// Convenience function for chaining function calls on queues.
    pub async fn ensure_exists<C: ConnectionLike + Send>(self, conn: &mut C) -> OcyResult<RedisQueue<'a>> {
        if self.exists(conn).await? {
            Ok(self)
        } else {
            Err(OcyError::NoSuchQueue(self.name))
        }
    }

    /// Generate a Redis key to use for this queue's settings.
    pub fn build_key(queue_prefix: &str, name: &str) -> String {
        format!("{}{}", queue_prefix, name)
    }

    /// Generate a Redis key to use for this queue's job IDs.
    pub fn build_jobs_key(queue_prefix: &str, name: &str) -> String {
        format!("{}{}{}", queue_prefix, name, QUEUE_JOBS_SUFFIX)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn queue_name_validation() {
        assert!(RedisQueue::is_valid_name("name"));
        assert!(RedisQueue::is_valid_name("1"));
        assert!(RedisQueue::is_valid_name("a"));
        assert!(RedisQueue::is_valid_name("abc-123-ABC"));
        assert!(RedisQueue::is_valid_name("123_456"));
        assert!(RedisQueue::is_valid_name("name.1.low"));
        assert!(RedisQueue::is_valid_name("_"));
        assert!(RedisQueue::is_valid_name("."));
        assert!(RedisQueue::is_valid_name("-"));

        assert!(!RedisQueue::is_valid_name(""));
        assert!(!RedisQueue::is_valid_name("   "));
        assert!(!RedisQueue::is_valid_name(":"));
        assert!(!RedisQueue::is_valid_name("name "));
        assert!(!RedisQueue::is_valid_name(" name"));
        assert!(!RedisQueue::is_valid_name("name/name"));
        assert!(!RedisQueue::is_valid_name("x'y"));
        assert!(!RedisQueue::is_valid_name("⨀⨁⨂"));
        assert!(!RedisQueue::is_valid_name("nâme"));
    }
}
