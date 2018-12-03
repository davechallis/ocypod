//! Defines custom `Duration` type.

use std::{fmt, time};

use serde::de::{Deserialize, Deserializer, Error};
use serde::ser::{Serialize, Serializer};
use serde_json;
use redis::{self, FromRedisValue, RedisResult, ToRedisArgs};
use humantime;

/// Duration to second resolution, thin wrapper around `time::Duration` allowing for custom
/// (de)serialisation.
///
/// Serialised to/from JSON as a human readable time (e.g. "1m", "1day", "1h 22m 58s").
/// Serialised to/from Redis as u64 seconds.
#[derive(Clone, Debug, PartialEq)]
pub struct Duration(pub time::Duration);

impl Duration {
    /// Create a new Duration from given number of seconds.
    pub fn from_secs(seconds: u64) -> Self {
        Duration(time::Duration::new(seconds, 0))
    }

    /// Get this duration as number of seconds.
    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }

    pub fn is_zero(&self) -> bool {
        self.0.as_secs() == 0 && self.0.subsec_nanos() == 0
    }
}

impl FromRedisValue for Duration {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        let seconds: u64 = redis::from_redis_value(v)?;
        Ok(Self::from_secs(seconds))
    }
}

impl ToRedisArgs for Duration {
    fn write_redis_args(&self, out: &mut Vec<Vec<u8>>) {
        self.0.as_secs().write_redis_args(out)
    }
}

impl<'a> ToRedisArgs for &'a Duration {
    fn write_redis_args(&self, out: &mut Vec<Vec<u8>>) {
        self.0.as_secs().write_redis_args(out)
    }
}

impl Serialize for Duration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&humantime::format_duration(self.0).to_string())
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let s: &str = Deserialize::deserialize(deserializer)?;
        humantime::parse_duration(s).map(Duration).map_err(D::Error::custom)
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", humantime::format_duration(self.0))
    }
}

impl From<Duration> for serde_json::Value {
    fn from(duration: Duration) -> Self {
        serde_json::Value::String(duration.to_string())
    }
}

#[cfg(test)]
mod test {
    use serde_json;
    use super::*;

    #[test]
    fn is_zero() {
        let dur = Duration::from_secs(0);
        assert!(dur.is_zero());

        let dur = Duration::from_secs(1);
        assert!(!dur.is_zero());
    }

    #[test]
    fn json_serialisation() {
        let dur = Duration::from_secs(0);
        assert_eq!(serde_json::to_string(&dur).unwrap(), "\"0s\"");

        let dur = Duration::from_secs(135);
        assert_eq!(serde_json::to_string(&dur).unwrap(), "\"2m 15s\"");

        let dur = Duration::from_secs(1246656);
        assert_eq!(serde_json::to_string(&dur).unwrap(), "\"14days 10h 17m 36s\"");
    }

    #[test]
    fn json_deserialisation() {
        let dur: Duration = serde_json::from_str("\"0s\"").unwrap();
        assert_eq!(dur, Duration::from_secs(0));

        let dur: Duration = serde_json::from_str("\"59s\"").unwrap();
        assert_eq!(dur, Duration::from_secs(59));

        let dur: Duration = serde_json::from_str("\"3h27m\"").unwrap();
        assert_eq!(dur, Duration::from_secs(12420));
    }

    #[test]
    fn roundtrip() {
        let dur = Duration::from_secs(1234567890);
        let ser = serde_json::to_string(&dur).unwrap();
        let deser: Duration = serde_json::from_str(&ser).unwrap();
        assert_eq!(dur, deser);
    }
}