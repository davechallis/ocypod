//! Handlers for getting general information about the Ocypod server as a whole.

use actix_web::{web, HttpResponse, Responder};
use log::error;

use crate::application::RedisManager;
use crate::models::ApplicationState;
use crate::models::OcyError;
//use crate::models::{ApplicationState, OcyError};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Handles `GET /info` requests.
///
/// # Returns
///
/// * 200 - JSON containing summary of server information
pub async fn index(data: web::Data<ApplicationState>) -> impl Responder {
    let mut conn = data.redis_conn_manager.clone();

    match RedisManager::server_info(&mut conn).await {
        Ok(info) => HttpResponse::Ok().json(info),
        Err(OcyError::RedisConnection(err)) => {
            error!("Failed to fetch summary data: {}", err);
            HttpResponse::ServiceUnavailable().body(err)
        }
        Err(err) => {
            error!("Failed to fetch summary data: {}", err);
            HttpResponse::InternalServerError().body(err.to_string())
        }
    }
}

/// Handles `GET /info/version` requests. This returns the version number of this server.
pub async fn version() -> impl Responder {
    HttpResponse::Ok().json(VERSION)
}
