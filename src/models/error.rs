//! Defines basic error and result types used throughout the application.

use std::{error::Error, fmt};

use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use deadpool_redis::PoolError;
use redis::RedisError;

/// Result type used throughout the application.
pub type OcyResult<T> = Result<T, OcyError>;

/// Error type used throughout the application.
#[derive(Debug)]
pub enum OcyError {
    /// Error occurred during interaction with Redis.
    Redis(RedisError),

    /// Error occurred while trying to get a pooled DB connection to Redis.
    RedisConnection(PoolError),

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

impl PartialEq for OcyError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Redis(l0), Self::Redis(r0)) => l0 == r0,
            (Self::RedisConnection(l0), Self::RedisConnection(r0)) => l0.to_string() == r0.to_string(),
            (Self::NoSuchQueue(l0), Self::NoSuchQueue(r0)) => l0 == r0,
            (Self::NoSuchJob(l0), Self::NoSuchJob(r0)) => l0 == r0,
            (Self::BadRequest(l0), Self::BadRequest(r0)) => l0 == r0,
            (Self::Conflict(l0), Self::Conflict(r0)) => l0 == r0,
            (Self::Internal(l0), Self::Internal(r0)) => l0 == r0,
            (Self::ParseError(l0), Self::ParseError(r0)) => l0 == r0,
            _ => false,
        }
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

impl From<deadpool_redis::PoolError> for OcyError {
    fn from(err: deadpool_redis::PoolError) -> Self {
        OcyError::RedisConnection(err)
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

impl ResponseError for OcyError {
    fn error_response(&self) -> HttpResponse {
        actix_web::HttpResponseBuilder::new(self.status_code())
            .insert_header((actix_web::http::header::CONTENT_TYPE, "text/html; charset=utf-8"))
            .body(self.to_string())
    }

    fn status_code(&self) -> StatusCode {
        match self {
            // TODO: check that this is the right response for ParseError
            OcyError::Redis(_) | OcyError::Internal(_) | OcyError::ParseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            OcyError::RedisConnection(_) => StatusCode::SERVICE_UNAVAILABLE,
            OcyError::NoSuchQueue(_) | OcyError::NoSuchJob(_) => StatusCode::NOT_FOUND,
            OcyError::BadRequest(_) => StatusCode::BAD_REQUEST,
            OcyError::Conflict(_) => StatusCode::CONFLICT,
        }
    }
}