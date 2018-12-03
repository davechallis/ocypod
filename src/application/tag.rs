//! Defines convenience interface to a tag in Redis.

use redis::{Connection, Commands};

use crate::models::{OcyError, OcyResult};

/// Represents a tag that can be attached to jobs in Redis.
///
/// Mostly used as convenient way of operating on a tag with a key.
pub struct RedisTag<'a> {
    key: String,
    conn: &'a Connection,
}

impl<'a> RedisTag<'a> {
    /// Create new RedisTag struct with given name. Will return error unless name is valid.
    pub fn new(tag: &str, conn: &'a Connection) -> OcyResult<Self> {
        if Self::is_valid_tag(tag) {
            Ok(Self { key: Self::build_key(tag), conn })
        } else {
            Err(OcyError::BadRequest("Invalid tag name, valid characters: a-zA-Z0-9_.-".to_owned()))
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
    pub fn tagged_job_ids(&self) -> OcyResult<Vec<u64>> {
        let mut job_ids: Vec<u64> = self.conn.smembers::<_, Vec<u64>>(self.key())?;
        job_ids.sort();
        Ok(job_ids)
    }

    // TODO: extend range of valid chars?
    /// Check whether a given string representation of a tag is valid.
    pub fn is_valid_tag(tag: &str) -> bool {
        !tag.is_empty() && tag.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
    }
}

// TODO: character validity tests