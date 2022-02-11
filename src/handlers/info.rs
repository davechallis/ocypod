//! Handlers for getting general information about the Ocypod server as a whole.

use actix_web::ResponseError;
use actix_web::{web, HttpResponse, Responder};
use log::error;

use crate::models::ApplicationState;
use crate::models::OcyError;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Handles `GET /info` requests.
///
/// # Returns
///
/// * 200 - JSON containing summary of server information
pub async fn index(data: web::Data<ApplicationState>) -> impl Responder {
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.server_info(&mut conn).await {
        Ok(info) => HttpResponse::Ok().json(info),
        Err(err) => {
            error!("Failed to fetch summary data: {}", err);
            err.error_response()
        },
    }
}

/// Handles `GET /info/version` requests. This returns the version number of this server.
pub async fn version() -> impl Responder {
    HttpResponse::Ok().json(VERSION)
}
