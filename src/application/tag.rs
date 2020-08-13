//! Defines convenience interface to a tag in Redis.

use redis::{aio::ConnectionLike, AsyncCommands};

use crate::models::{OcyError, OcyResult};

/// Represents a tag that can be attached to jobs in Redis.
///
/// Mostly used as convenient way of operating on a tag with a key.
pub struct RedisTag {
    key: String,
}

impl RedisTag {
    /// Create new RedisTag struct with given name. Will return error unless name is valid.
    pub fn from_str(tag: &str) -> OcyResult<Self> {
        if Self::is_valid_tag(tag) {
            Ok(Self {
                key: Self::build_key(tag),
            })
        } else {
            Err(OcyError::bad_request("Invalid tag name, valid characters: a-zA-Z0-9_.-"))
        }
    }

    /// Get this tag's Redis key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get Redis key to add tagged jobs under.
    pub fn build_key(tag: &str) -> String {
        format!("tag:{}", tag)
    }

    /// Get list of job IDs with this tag.
    pub async fn tagged_job_ids<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
    ) -> OcyResult<Vec<u64>> {
        let mut job_ids: Vec<u64> = conn.smembers::<_, Vec<u64>>(self.key()).await?;
        job_ids.sort();
        Ok(job_ids)
    }

    // TODO: extend range of valid chars?
    /// Check whether a given string representation of a tag is valid.
    pub fn is_valid_tag(tag: &str) -> bool {
        !tag.is_empty()
            && tag
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
    }
}

// TODO: character validity tests
