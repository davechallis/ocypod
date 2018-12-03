//! Defines shared application state available to each HTTP worker thread.

use actix::prelude::*;
use crate::actors::application::ApplicationActor;
use crate::config::Config;

/// Shared state for an HTTP worker thread.
pub struct ApplicationState {
    /// Address of the `RedisActor` mailbox, used to send queue/job actions to.
    pub redis_addr: Addr<ApplicationActor>,
    pub config: Config,
}

impl ApplicationState {
    pub fn new(redis_addr: Addr<ApplicationActor>, config: Config) -> Self {
        ApplicationState { redis_addr, config }
    }
}