mod field;
mod payload;
mod request;
mod status;

pub use self::field::Field;
pub use self::payload::Payload;
pub use self::request::{CreateRequest, UpdateRequest};
pub use self::status::Status;

use serde_json;
use serde::ser::{Serialize, Serializer, SerializeMap};
use redis::{self, Commands, FromRedisValue};
use std::collections::{HashMap, HashSet};
use crate::models::{OcyResult, DateTime, Duration};

/// Generic data structure for containing a subset of job metadata.
///
/// Used as a convenient way of dealing with getting/mapping Redis data that might be missing.
#[derive(Debug, PartialEq)]
pub struct JobMeta {
    map: HashMap<Field, redis::Value>,
    fields: Vec<Field>, // TODO: change above to use ref to field here?
    hidden_fields: HashSet<Field>,
}

impl Serialize for JobMeta {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.fields.len()))?;
        for field in self.fields.iter().filter(|f| !self.hidden_fields.contains(f)) {
            match field {
                Field::Id               => map.serialize_entry(field, &self.id())?,
                Field::Queue            => map.serialize_entry(field, &self.queue())?,
                Field::Status           => map.serialize_entry(field, &self.status())?,
                Field::Tags             => map.serialize_entry(field, &self.tags())?,
                Field::CreatedAt        => map.serialize_entry(field, &self.created_at())?,
                Field::StartedAt        => map.serialize_entry(field, &self.started_at())?,
                Field::EndedAt          => map.serialize_entry(field, &self.ended_at())?,
                Field::LastHeartbeat    => map.serialize_entry(field, &self.last_heartbeat())?,
                Field::Input            => map.serialize_entry(field, &self.input())?,
                Field::Output           => map.serialize_entry(field, &self.output())?,
                Field::Timeout          => map.serialize_entry(field, &self.timeout())?,
                Field::HeartbeatTimeout => map.serialize_entry(field, &self.heartbeat_timeout())?,
                Field::ExpiresAfter     => map.serialize_entry(field, &self.expires_after())?,
                Field::Retries          => map.serialize_entry(field, &self.retries())?,
                Field::RetriesAttempted => map.serialize_entry(field, &self.retries_attempted())?,
                Field::RetryDelays      => map.serialize_entry(field, &self.retry_delays())?,
                Field::Ended            => map.serialize_entry(field, &self.ended())?,
            }
        }

        map.end()
    }
}

impl JobMeta {
    /// Construct an instance of `JobMeta` from given fields and a raw `redis::Value`.
    pub fn from_redis_value(
        fields: &[Field],
        v: &redis::Value,
        hidden_fields: &[Field]
    ) -> redis::RedisResult<Self> {
        let hidden_fields: HashSet<Field> = if hidden_fields.is_empty() {
            HashSet::new()
        } else {
            hidden_fields.iter().cloned().collect()
        };

        match v {
            redis::Value::Bulk(items) => {
                let mut map = HashMap::with_capacity(fields.len());
                for (field, item) in fields.into_iter().zip(items) {
                    match item {
                        redis::Value::Nil => (),
                        data              => { map.insert(field.clone(), data.to_owned()); },
                    }
                }

                Ok(Self { map, fields: fields.to_vec(), hidden_fields })
            },
            item @ redis::Value::Data(_) => {
                let mut map = HashMap::with_capacity(1);
                map.insert(fields[0].clone(), item.to_owned());
                Ok(Self { map, fields: fields.to_vec(), hidden_fields })
            },
            redis::Value::Nil => Ok(Self { map: HashMap::new(), fields: fields.to_vec(), hidden_fields }),
            _ => panic!("Unhandled response type for JobMeta query: {:?}", v),
        }
    }

    /// Check whether this job was found in Redis.
    ///
    /// Considered to exist if any mandatory field was mapped to a value from a query. Callers
    /// must ensure that at least one mandatory field is present in every query, as Redis has no
    /// way to differentiate between querying missing hash field for an existing key, and a hash
    /// field for a missing key.
    fn exists(&self) -> bool {
        !self.map.is_empty()
    }

    /// Get a mandatory field value from this struct's map. Caller must ensure that field is present.
    fn get_mandatory_field<T: redis::FromRedisValue>(&self, field: &Field) -> T {
        redis::from_redis_value(&self.map.get(field).unwrap_or_else(|| panic!("failed to get: {}", field))).unwrap()
    }

    /// Get an optional field value from this struct's map.
    fn get_optional_field<T: redis::FromRedisValue>(&self, field: &Field) -> Option<T> {
        self.map.get(field).map(|v| redis::from_redis_value(v).unwrap())
    }

    pub fn id(&self) -> u64 {
        self.get_mandatory_field(&Field::Id)
    }

    pub fn queue(&self) -> String {
        self.get_mandatory_field(&Field::Queue)
    }

    pub fn status(&self) -> Status {
        self.get_mandatory_field(&Field::Status)
    }

    pub fn tags(&self) -> Option<Vec<String>> {
        self.get_optional_field::<String>(&Field::Tags).map(|s| serde_json::from_str(&s).unwrap())
    }

    pub fn created_at(&self) -> DateTime {
        self.get_mandatory_field(&Field::CreatedAt)
    }

    pub fn started_at(&self) -> Option<DateTime> {
        self.get_optional_field(&Field::StartedAt)
    }

    pub fn ended_at(&self) -> Option<DateTime> {
        self.get_optional_field(&Field::EndedAt)
    }

    pub fn last_heartbeat(&self) -> Option<DateTime> {
        self.get_optional_field(&Field::LastHeartbeat)
    }

    pub fn input(&self) -> Option<serde_json::Value> {
        self.get_optional_field::<String>(&Field::Input).map(|s| serde_json::from_str(&s).unwrap())
    }

    pub fn output(&self) -> Option<serde_json::Value> {
        self.get_optional_field::<String>(&Field::Output).map(|s| serde_json::from_str(&s).unwrap())
    }

    pub fn timeout(&self) -> Duration {
        self.get_mandatory_field(&Field::Timeout)
    }

    pub fn heartbeat_timeout(&self) -> Duration {
        self.get_mandatory_field(&Field::HeartbeatTimeout)
    }

    pub fn expires_after(&self) -> Duration {
        self.get_mandatory_field(&Field::ExpiresAfter)
    }

    pub fn retries(&self) -> u64 {
        self.get_mandatory_field(&Field::Retries)
    }

    pub fn retries_attempted(&self) -> u64 {
        self.get_mandatory_field(&Field::RetriesAttempted)
    }

    pub fn retry_delays(&self) -> Option<Vec<Duration>> {
        self.get_optional_field::<String>(&Field::RetryDelays).map(|s| serde_json::from_str(&s).unwrap())
    }

    pub fn ended(&self) -> bool {
        match self.status() {
            Status::Running | Status::Queued      => false,
            Status::TimedOut | Status::Failed     => self.retries() == 0,
            Status::Completed | Status::Cancelled => true,
        }
    }
}

/// Subset of job data used for determining whether a job should be timed out.
pub struct TimeoutMeta(JobMeta);

impl FromRedisValue for TimeoutMeta {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        Ok(TimeoutMeta(JobMeta::from_redis_value(TimeoutMeta::fields(), v, &[])?))
    }
}

impl TimeoutMeta {
    pub fn from_conn<K: redis::ToRedisArgs>(conn: &redis::Connection, key: K) -> redis::RedisResult<Self> {
        let fields = TimeoutMeta::fields();
        Ok(TimeoutMeta(JobMeta::from_redis_value(fields, &conn.hget(key, fields)?, &[])?))
    }

    pub fn id(&self) -> u64 {
        self.0.id()
    }

    pub fn has_timed_out(&self) -> bool {
        // no timeout metadata means that job has been deleted
        if !self.0.exists() {
            return false;
        }

        // only running jobs can timeout
        if self.0.status() != Status::Running {
            return false;
        }

        let timeout_seconds = self.0.timeout().as_secs();
        let heartbeat_timeout_seconds = self.0.heartbeat_timeout().as_secs();
        let last_heartbeat = self.0.last_heartbeat();
        let started_at = self.0.started_at().unwrap();

        if heartbeat_timeout_seconds > 0 {
            let hb = last_heartbeat.as_ref().unwrap_or(&started_at);
            let duration_seconds = DateTime::now().seconds_since(hb);
            assert!(duration_seconds >= 0); // TODO: error handling here?
            if duration_seconds as u64 > heartbeat_timeout_seconds {
                return true;
            }
        }

        if timeout_seconds > 0 {
            let duration_seconds = DateTime::now().seconds_since(&started_at);
            assert!(duration_seconds >= 0);
            if duration_seconds as u64 > timeout_seconds {
                return true;
            }
        }

        false
    }

    pub fn fields() -> &'static [Field] {
        static FIELDS: [Field; 6] = [
            Field::Id,
            Field::Status,
            Field::Timeout,
            Field::HeartbeatTimeout,
            Field::LastHeartbeat,
            Field::StartedAt
        ];
        &FIELDS
    }
}


pub struct ExpiryMeta(JobMeta);

impl FromRedisValue for ExpiryMeta {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        Ok(ExpiryMeta(JobMeta::from_redis_value(ExpiryMeta::fields(), v, &[])?))
    }
}

impl ExpiryMeta {
    pub fn from_conn<K: redis::ToRedisArgs>(conn: &redis::Connection, key: K) -> OcyResult<Self> {
        let fields = ExpiryMeta::fields();
        Ok(ExpiryMeta(JobMeta::from_redis_value(fields, &conn.hget(key, fields)?, &[])?))
    }

    pub fn id(&self) -> u64 {
        self.0.id()
    }

    pub fn fields() -> &'static [Field] {
        static FIELDS: [Field; 3] = [
            Field::Id,
            Field::EndedAt,
            Field::ExpiresAfter,
        ];
        &FIELDS
    }

    pub fn should_expire(&self) -> bool {
        // no retry metadata means that job has been deleted
        if !self.0.exists() {
            return false;
        }

        let expires_after_seconds = self.0.expires_after().as_secs();
        if expires_after_seconds == 0 {
            return false;
        }

        if let Some(end_dt) = self.0.ended_at() {
            let now = DateTime::now();
            let duration_seconds = now.seconds_since(&end_dt);
            assert!(duration_seconds >= 0); // TODO: error handling here?
            if duration_seconds as u64 > expires_after_seconds {
                return true;
            }
        }

        false
    }
}


pub enum RetryAction {
    /// Move job back to its original queue to be retried later.
    Retry,

    /// Move job to ended queue, no further automatic retries are possible.
    End,

    /// Do nothing, as job has either already been moved/deleted, or has a retry delay.
    None,
}


pub struct RetryMeta(JobMeta);

impl FromRedisValue for RetryMeta {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        Ok(RetryMeta(JobMeta::from_redis_value(RetryMeta::fields(), v, &[])?))
    }
}

impl RetryMeta {
    pub fn from_conn<K: redis::ToRedisArgs>(conn: &redis::Connection, key: K) -> OcyResult<Self> {
        let fields = RetryMeta::fields();
        Ok(RetryMeta(JobMeta::from_redis_value(fields, &conn.hget(key, fields)?, &[])?))
    }

    pub fn id(&self) -> u64 {
        self.0.id()
    }

    pub fn fields() -> &'static [Field] {
        static FIELDS: [Field; 5] = [
            Field::Id,
            Field::EndedAt,
            Field::Retries,
            Field::RetriesAttempted,
            Field::RetryDelays,
        ];
        &FIELDS
    }

    pub fn retry_action(&self) -> RetryAction {
        // no retry metadata means that job has been deleted
        if !self.0.exists() {
            return RetryAction::None;
        }

        // 0 means that retries are disabled for this job
        let retries = self.0.retries();
        if retries == 0 {
            return RetryAction::End;
        }

        // retry limit reached
        let retries_attempted = self.0.retries_attempted();
        if retries_attempted >= retries {
            return RetryAction::End;
        }

        // check whether enough time has passed between retry delays
        if let Some(retry_delays) = self.0.retry_delays() {
            if !retry_delays.is_empty() {
                let retry_attempt = (retries_attempted + 1) as usize;
                let delay_secs = if retry_attempt > retry_delays.len() {
                    retry_delays.last().unwrap().as_secs()
                } else {
                    retry_delays[retry_attempt - 1].as_secs()
                };

                if (DateTime::now().seconds_since(&self.0.ended_at().unwrap()) as u64) < delay_secs {
                    return RetryAction::End;
                }
            }
        }

        RetryAction::Retry
    }
}
