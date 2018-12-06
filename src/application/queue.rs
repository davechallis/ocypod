//! Defines convenience interface to queue in Redis.

use redis::{Commands, PipelineCommands, RedisResult};

use crate::models::{OcyError, OcyResult, queue, job};
use crate::redis_utils::{transaction, vec_from_redis_pipe};
use super::{keys, RedisJob, RedisTag};

/// Interface to a queue in Redis. This consists of a list containing queued jobs, and a hash containing queue settings.
///
/// Primarily used by RedisManager as a wrapper around some queue information.
pub struct RedisQueue<'a> {
    pub name: String,
    conn: &'a redis::Connection,
    pub key: String,
    pub jobs_key: String,
}

impl<'a> RedisQueue<'a> {
    /// Get a new RedisQueue struct, ensuring its name is valid.
    pub fn from_string<S: Into<String>>(name: S, conn: &'a redis::Connection) -> OcyResult<Self> {
        let name = name.into();
        if Self::is_valid_name(&name) {
            let key = Self::build_key(&name);
            let jobs_key = Self::build_jobs_key(&name);
            Ok(Self { name, conn, key, jobs_key })
        } else {
            Err(OcyError::BadRequest("Invalid queue name, valid characters: a-zA-Z0-9_.-".to_owned()))
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
        !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
    }

    /// Create a new queue with given settings, or update settings for an existing queue.
    ///
    /// Returns true if a new queue was created, or false if an existing queue was updated.
    pub fn create_or_update(&self, settings: &queue::Settings) -> OcyResult<bool> {
        debug!("[{}] writing settings: {:?}", &self.key, settings);

        let mut pipeline = redis::pipe();

        let pipe = pipeline.atomic()
            .hset(&self.key, queue::Field::Timeout, &settings.timeout)
            .hset(&self.key, queue::Field::HeartbeatTimeout, &settings.heartbeat_timeout).ignore()
            .hset(&self.key, queue::Field::ExpiresAfter, &settings.expires_after).ignore()
            .hset(&self.key, queue::Field::Retries, settings.retries).ignore()
            .sadd(keys::QUEUES_KEY, &self.name).ignore();

        if settings.retry_delays.is_empty() {
            pipe.hdel(&self.key, queue::Field::RetryDelays).ignore();
        } else {
            let retry_delays_json: serde_json::Value = settings.retry_delays.as_slice().into();
            pipe.hset(&self.key, queue::Field::RetryDelays, retry_delays_json.to_string()).ignore();
        }

        let (is_new,): (bool,) = pipe.query(self.conn)?;

        // all fields are mandatory, so if 1st is updated, this is a new entry
        info!("[{}] {}", &self.key, if is_new { "created" } else { "updated" });
        Ok(is_new)
    }

    /// Delete an existing queue, if it exists.
    ///
    /// Deletes any jobs currently in the queue, but won't affected any running/completed jobs.
    ///
    /// Returns true if a queue was deleted, false otherwise.
    pub fn delete(&self) -> OcyResult<bool> {
        debug!("Deleting queue '{}'", self.name);
        let (queue_deleted, num_jobs_deleted): (bool, usize) = transaction(self.conn, &[&self.jobs_key], |pipe| {
            // if queue has already been deleted, nothing to do
            if !self.exists()? {
                return Ok(Some((false, 0)));
            }

            let mut keys_to_del: Vec<String> = vec![self.key.to_owned(), self.jobs_key.to_owned()];

            // fetch all tags for all jobs to delete in separate non-atomic/transactional pipeline
            let mut tag_pipeline = redis::pipe();
            let tag_pipe = &mut tag_pipeline;

            let job_ids: Vec<u64> = self.conn.lrange(&self.jobs_key, 0, -1)?;
            for job_id in &job_ids {
                let job_key = RedisJob::build_key(*job_id);
                tag_pipe.hget(&job_key, &[job::Field::Id, job::Field::Tags]);
                keys_to_del.push(job_key);
            }

            let tagged_jobs: Vec<(Option<u64>, Option<String>)> = vec_from_redis_pipe(tag_pipe, self.conn)?;
            for (job_id, tags) in tagged_jobs {
                if let (Some(job_id), Some(tags)) = (job_id, tags) {
                    for tag in serde_json::from_str::<Vec<&str>>(&tags).unwrap() {
                        pipe.srem(RedisTag::build_key(tag), job_id);
                    }
                }
            }

            let result: Option<()> = pipe
                .del(keys_to_del)
                .srem(keys::QUEUES_KEY, &self.name)
                .query(self.conn)?;
            Ok(result.map(|_| (true, job_ids.len())))
        })?;

        if queue_deleted {
            info!("[{}] deleted ({} queued jobs deleted)", &self.key, num_jobs_deleted);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get number of jobs currently queued.
    pub fn size(&self) -> OcyResult<u64> {
        let conn = self.conn;
        let (exists, size): (bool, u64) =
            redis::transaction(conn, &[&self.key, &self.jobs_key], |pipe| {
                pipe.exists(&self.key)    // check queue settings exist
                    .llen(&self.jobs_key) // check length of queued jobs
                    .query(conn)
            })?;

        if exists {
            Ok(size)
        } else {
            Err(OcyError::NoSuchQueue(self.name.to_owned()))
        }
    }

    /// Get this queue's settings.
    pub fn settings(&self) -> OcyResult<queue::Settings> {
        Ok(self.conn.hget(&self.key, &[queue::Field::Timeout,
            queue::Field::HeartbeatTimeout,
            queue::Field::ExpiresAfter,
            queue::Field::Retries,
            queue::Field::RetryDelays])?)
    }

    /// Check whether this queue exists in Redis or not.
    pub fn exists(&self) -> RedisResult<bool> {
        self.conn.exists(&self.key)
    }

    /// Return either this queue or an error if it doesn't exist.
    ///
    /// Convenience function for chaining function calls on queues.
    pub fn ensure_exists(self) -> OcyResult<Self> {
        if self.exists()? {
            Ok(self)
        } else {
            Err(OcyError::NoSuchQueue(self.name))
        }
    }

    /// Generate a Redis key to use for this queue's settings.
    pub fn build_key(name: &str) -> String {
        format!("{}{}", keys::QUEUE_PREFIX, name)
    }

    /// Generate a Redis key to use for this queue's job IDs.
    pub fn build_jobs_key(name: &str) -> String {
        format!("{}{}{}", keys::QUEUE_PREFIX, name, keys::QUEUE_JOBS_SUFFIX)
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
