//! HTTP handlers for the `/queue` endpoints.

// TODO: examples of responses

use std::time::Instant;

use log::error;
use futures::{future, Future};
use actix_web::{self, Path, State, AsyncResponder, HttpRequest, HttpResponse, Json};

use crate::actors::application;
use crate::models::{ApplicationState, job, queue, OcyError};

/// Handle `GET /queue` requests to get a JSON list of all existing queues.
///
/// # Returns
///
/// * 200 - JSON response containing list of queue names.
#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn index(req: &HttpRequest<ApplicationState>) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    req.state().redis_addr.send(application::GetQueueNames)
        .from_err()
        .and_then(|res| {
            match res {
                Ok(queue_names) => Ok(HttpResponse::Ok().json(queue_names)),
                Err(OcyError::RedisConnection(err)) => {
                    error!("Failed to fetch queue names: {}", err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)        => {
                    error!("Failed to fetch queue names: {}", err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
        .responder()
}

/// Handles `PUT /queue/{queue_name}` requests.
#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn create_or_update(
    (path, json): (Path<String>, Json<queue::Settings>),
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let queue_name = path.into_inner();
    let queue_settings = json.into_inner();
    state.redis_addr.send(application::CreateOrUpdateQueue(queue_name.to_owned(), queue_settings))
        .from_err()
        .and_then(move |res| {
            match res {
                Ok(true)  => Ok(HttpResponse::Created()
                    .header("Location", format!("/queue/{}", queue_name))
                    .finish()),
                Ok(false) => Ok(HttpResponse::NoContent()
                    .reason("Queue setting updated")
                    .header("Location", format!("/queue/{}", queue_name))
                    .finish()),
                Err(OcyError::BadRequest(msg)) => Ok(HttpResponse::BadRequest().body(msg)),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[queue:{}] failed to create/update queue: {}", &queue_name, err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)  => {
                    error!("[queue:{}] failed to create/update queue: {}", &queue_name, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
        .responder()
}

#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn delete(
    path: Path<String>,
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let queue_name = path.into_inner();
    state.redis_addr.send(application::DeleteQueue(queue_name.clone()))
        .from_err()
        .and_then(move |res| {
            match res {
                Ok(true)  => Ok(HttpResponse::NoContent().reason("Queue deleted").finish()),
                Ok(false) => Ok(HttpResponse::NotFound().reason("Queue not found").finish()),
                Err(OcyError::BadRequest(msg)) => Ok(HttpResponse::BadRequest().body(msg)),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[queue:{}] failed to delete queue: {}", &queue_name, err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)  => {
                    error!("[queue:{}] failed to delete queue: {}", &queue_name, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
        .responder()
}

#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn settings(
    path: Path<String>,
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let queue_name = path.into_inner();
    state.redis_addr.send(application::GetQueueSettings(queue_name.clone()))
        .from_err()
        .and_then(move |res| {
            match res {
                Ok(summary) => Ok(HttpResponse::Ok().json(summary)),
                Err(OcyError::NoSuchQueue(_)) => Ok(HttpResponse::NotFound().into()),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[queue:{}] failed to fetch queue summary: {}", &queue_name, err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)    => {
                    error!("[queue:{}] failed to fetch queue summary: {}", &queue_name, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
        .responder()
}

#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn size(
    path: Path<String>,
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let queue_name = path.into_inner();
    state.redis_addr.send(application::GetQueueSize(queue_name.clone()))
        .from_err()
        .and_then(move |res| {
            match res {
                Ok(size) => Ok(HttpResponse::Ok().json(size)),
                Err(OcyError::NoSuchQueue(_)) => Ok(HttpResponse::NotFound().into()),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[queue:{}] failed to fetch queue size: {}", &queue_name, err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)    => {
                    error!("[queue:{}] failed to fetch queue size: {}", &queue_name, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
        .responder()
}

#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn create_job(
    (path, json): (Path<String>, Json<job::CreateRequest>),
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let queue_name = path.into_inner();
    let job_req = json.into_inner();
    state.redis_addr.send(application::CreateJob(queue_name.clone(), job_req))
        .from_err()
        .and_then(move |res| {
            match res {
                Ok(job_id) => Ok(HttpResponse::Created()
                    .header("Location", format!("/job/{}", job_id))
                    .json(job_id)),
                Err(OcyError::NoSuchQueue(_))  => Ok(HttpResponse::NotFound().reason("Queue Not Found").finish()),
                Err(OcyError::BadRequest(msg)) => Ok(HttpResponse::BadRequest().body(msg)),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[queue:{}] failed to create new job: {}", &queue_name, err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)                       => {
                    error!("[queue:{}] failed to create new job: {}", &queue_name, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                }
            }
        })
        .responder()
}

#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn next_job(
    path: Path<String>,
    state: State<ApplicationState>
) -> Box<Future<Item = HttpResponse, Error = actix_web::Error>> {
    let queue_name = path.into_inner();
    Box::new(state.redis_addr.send(application::NextJob(queue_name.clone()))
        .from_err()
        .and_then(move |res| {
            match res {
                Ok(Some(job)) => future::Either::A(future::ok(HttpResponse::Ok().json(job))),
                Ok(None) => match &state.config.server.next_job_delay {
                    Some(delay) if !delay.is_zero() => {
                        let when = Instant::now() + delay.0;
                        let resp = tokio_timer::Delay::new(when)
                            .from_err()
                            .and_then(|_| future::ok(HttpResponse::NoContent().into()));
                        future::Either::B(resp)
                    },
                    _ => future::Either::A(future::ok(HttpResponse::NoContent().into())),
                },
                Err(OcyError::NoSuchQueue(_)) => future::Either::A(future::ok(HttpResponse::NotFound().into())),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[queue:{}] failed to fetch next job: {}", &queue_name, err);
                    future::Either::A(future::ok(HttpResponse::ServiceUnavailable().body(err.to_string())))
                },
                Err(err)                      => {
                    error!("[queue:{}] failed to fetch next job: {}", &queue_name, err);
                    future::Either::A(future::ok(HttpResponse::InternalServerError().body(err.to_string())))
                }
            }
        }))
}
