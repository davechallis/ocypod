//! Defines actor for running periodic Redis tasks.
use crate::application::RedisManager;
use std::time::Duration;

use log::{error, info};

use crate::config::ServerConfig;

/// Start all background tasks that perform monitoring/cleanup.
pub fn start_monitors(conn: redis::aio::ConnectionManager, config: &ServerConfig) {
    start_timeout_monitor(conn.clone(), config.timeout_check_interval.0);
    start_retry_monitor(conn.clone(), config.retry_check_interval.0);
    start_expiry_monitor(conn, config.expiry_check_interval.0);
}

/// Start periodic background task that checks jobs for timeouts.
fn start_timeout_monitor(conn: redis::aio::ConnectionManager, check_interval: Duration) {
    info!(
        "Checking job timeouts every {}",
        humantime::format_duration(check_interval)
    );
    actix_rt::spawn(async move {
        let mut interval = actix_rt::time::interval(check_interval);
        let mut conn = conn;
        loop {
            interval.tick().await;
            if let Err(err) = RedisManager::check_job_timeouts(&mut conn).await {
                error!("Job timeout monitoring failed: {}", err);
            }
        }
    })
}

/// Start periodic background task that checks for jobs that need retrying.
fn start_retry_monitor(conn: redis::aio::ConnectionManager, check_interval: Duration) {
    info!(
        "Checking job retries every {}",
        humantime::format_duration(check_interval)
    );
    actix_rt::spawn(async move {
        let mut interval = actix_rt::time::interval(check_interval);
        let mut conn = conn;
        loop {
            interval.tick().await;
            if let Err(err) = RedisManager::check_job_retries(&mut conn).await {
                error!("Job retry monitoring failed: {}", err);
            }
        }
    })
}

/// Start periodic background that checks for expired jobs and cleans them up.
fn start_expiry_monitor(conn: redis::aio::ConnectionManager, check_interval: Duration) {
    info!(
        "Checking job expiry every {}",
        humantime::format_duration(check_interval)
    );
    actix_rt::spawn(async move {
        let mut interval = actix_rt::time::interval(check_interval);
        let mut conn = conn;
        loop {
            interval.tick().await;
            if let Err(err) = RedisManager::check_job_expiry(&mut conn).await {
                error!("Job expiry monitoring failed: {}", err);
            }
        }
    })
}
