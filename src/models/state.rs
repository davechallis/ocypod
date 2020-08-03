pub struct ApplicationState {
    pub redis_conn_manager: redis::aio::ConnectionManager,
    pub config: crate::config::Config,
}
