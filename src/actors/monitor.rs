//! Defines actor for running periodic Redis tasks.
use std::time::Duration;

use log::{info, error};
use actix::prelude::{Actor, Addr, Arbiter, AsyncContext, Context};
use futures::Future;

use crate::config::ServerConfig;
use super::application::{ApplicationActor, CheckJobTimeouts, CheckJobExpiry, CheckJobRetries};

/// Actor that performs periodic operations in Redis.
///
/// This includes:
///
/// * checking job timeouts
/// * checking job retries
/// * deleting expired jobs
pub struct MonitorActor {
    redis_addr: Addr<ApplicationActor>,
    timeout_check_interval_secs: u64,
    expiry_check_interval_secs: u64,
    retry_check_interval_secs: u64,
}

impl Actor for MonitorActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // TODO: add to actor state to allow for pausing, changing interval, etc.?

        // checks to see if any running jobs have timed out
        info!("Checking job timeouts every {}s", self.timeout_check_interval_secs);
        ctx.run_interval(Duration::from_secs(self.timeout_check_interval_secs), |monitor, _ctx| {
            let res = monitor.redis_addr.send(CheckJobTimeouts);

            Arbiter::spawn(
                res
                    .map(|res| {
                        if let Err(err) = res {
                            error!("Job timeout monitoring failed: {}", err);
                        };
                    })
                    .map_err(|e| error!("Sending timeout check message failed: {}", e))
            );
        });

        // checks to see if any ended jobs need removing from Redis
        info!("Checking job expiry every {}s", self.expiry_check_interval_secs);
        ctx.run_interval(Duration::from_secs(self.expiry_check_interval_secs), |monitor, _ctx| {
            let res = monitor.redis_addr.send(CheckJobExpiry);

            Arbiter::spawn(
                res
                    .map(|res| {
                        if let Err(err) = res {
                            error!("Job expiry monitoring failed: {}", err);
                        };
                    })
                    .map_err(|e| error!("Sending expiry check message failed: {}", e))
            );
        });

        // checks to see if any failed jobs need re-queuing
        info!("Checking job retries every {}s", self.retry_check_interval_secs);
        ctx.run_interval(Duration::from_secs(self.retry_check_interval_secs), |monitor, _ctx| {
            let res = monitor.redis_addr.send(CheckJobRetries);

            Arbiter::spawn(
                res
                    .map(|res| {
                        if let Err(err) = res {
                            error!("Job retry monitoring failed: {}", err);
                        };
                    })
                    .map_err(|e| error!("Sending retry check message failed: {}", e))
            );
        });

//        // use during development only, checks for any inconsistency in Redis data structures
//        info!("Checking DB integrity every {}s", 5);
//        ctx.run_interval(Duration::from_secs(5), |monitor, _ctx| {
//            let res = monitor.redis_addr.send(CheckDbIntegrity);
//
//            Arbiter::spawn(
//                res
//                    .map(|res| {
//                        if let Err(err) = res {
//                            error!("DB integrity checking failed: {}", err);
//                        };
//                    })
//                    .map_err(|e| error!("Sending integrity check message failed: {}", e))
//            );
//        });
    }
}

impl MonitorActor {
    pub fn new(redis_addr: Addr<ApplicationActor>, config: &ServerConfig) -> Self {
        MonitorActor {
            redis_addr,
            timeout_check_interval_secs: config.timeout_check_interval.as_secs(),
            expiry_check_interval_secs: config.expiry_check_interval.as_secs(),
            retry_check_interval_secs: config.retry_check_interval.as_secs(),
        }
    }
}