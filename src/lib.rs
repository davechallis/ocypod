//! Job queue system designed for long running tasks.

#[deny(
    missing_docs,
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features,
    unused_import_braces,
    unused_qualifications
)]

pub mod application;
pub mod config;
pub mod handlers;
pub mod models;
pub mod redis_utils;
