//! Defines server state, typically passed to HTTP handlers by Actix web as required.

use deadpool_redis::Pool;

use crate::application::RedisManager;

pub struct ApplicationState {
    pub pool: Pool,
    pub config: crate::config::Config,
    pub redis_manager: RedisManager,
}
