//! Defines server state, typically passed to HTTP handlers by Actix web as required.

pub struct ApplicationState {
    pub redis_conn_pool: deadpool_redis::Pool,
    pub config: crate::config::Config,
}
