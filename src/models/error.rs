//! Defines basic error and result types used throughout the application.

use std::{error::Error, fmt};

use redis::RedisError;

/// Result type used throughout the application.
pub type OcyResult<T> = Result<T, OcyError>;

/// Error type used throughout the application.
#[derive(Debug, PartialEq)]
pub enum OcyError {
    /// Error occurred during interaction with Redis.
    Redis(RedisError),

    /// Error occurred while trying to get a pooled DB connection to Redis.
    RedisConnection(String),

    /// Operation attempted on queue that does not exist.
    NoSuchQueue(String),

    /// Operation attempted on job that does not exist.
    NoSuchJob(u64),

    /// Could not complete request with given parameters.
    BadRequest(String),

    /// Request was not valid due to current state of some resource(s).
    Conflict(String),

    /// Internal application error, e.g. actor mailbox full.
    Internal(String),

    /// Parsing of some data structure failed. Typically used when parsing JSON.
    ParseError(String),
}

impl OcyError {
    /// Construct a new OcyError::Conflict with given message.
    pub fn conflict<S: Into<String>>(msg: S) -> Self {
        OcyError::Conflict(msg.into())
    }

    /// Construct a new OcyError::BadRequest with given message.
    pub fn bad_request<S: Into<String>>(msg: S) -> Self {
        OcyError::BadRequest(msg.into())
    }
}

impl From<RedisError> for OcyError {
    fn from(err: RedisError) -> Self {
        OcyError::Redis(err)
    }
}

impl From<serde_json::Error> for OcyError {
    fn from(err: serde_json::Error) -> Self {
        OcyError::ParseError(err.to_string())
    }
}

impl From<OcyError> for actix_web::body::Body {
    fn from(o: OcyError) -> actix_web::body::Body {
        o.to_string().into_bytes().into()
    }
}

impl fmt::Display for OcyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OcyError::Redis(err) => err.fmt(f),
            OcyError::RedisConnection(msg) => write!(f, "Failed to connect to Redis: {}", msg),
            OcyError::NoSuchQueue(queue) => write!(f, "Queue '{}' does not exist", queue),
            OcyError::NoSuchJob(job_id) => write!(f, "Job with ID {} does not exist", job_id),
            OcyError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            OcyError::BadRequest(msg) | OcyError::Conflict(msg) | OcyError::Internal(msg) => {
                write!(f, "{}", msg)
            }
        }
    }
}

impl Error for OcyError {
    fn cause(&self) -> Option<&dyn Error> {
        match self {
            OcyError::Redis(err) => err.source(),
            _ => None,
        }
    }
}
