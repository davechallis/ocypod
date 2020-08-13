//! Defines server state, typically passed to HTTP handlers by Actix web as required.

pub struct ApplicationState {
    pub redis_conn_manager: redis::aio::ConnectionManager,
    pub config: crate::config::Config,
}
