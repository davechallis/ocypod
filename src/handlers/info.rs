//! Handlers for getting general information about the Ocypod server as a whole.

use futures::Future;
use actix_web::{self, AsyncResponder, HttpRequest, HttpResponse};

use crate::actors::application;
use crate::models::{ApplicationState, OcyError};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Handles `GET /info` requests.
///
/// # Returns
///
/// * 200 - JSON containing summary of server information
pub fn index(req: &HttpRequest<ApplicationState>) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    req.state().redis_addr.send(application::GetInfo)
        .from_err()
        .and_then(|res| {
            match res {
                Ok(summary) => Ok(HttpResponse::Ok().json(summary)),
                Err(OcyError::RedisConnection(err)) => {
                    error!("Failed to fetch summary data: {}", err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)    => {
                    error!("Failed to fetch summary data: {}", err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
        .responder()
}

// TODO: should this return JSON instead of text/plain?
/// Handles `GET /info/version` requests.
pub fn version(_: &HttpRequest<ApplicationState>) -> &'static str {
    VERSION
}
