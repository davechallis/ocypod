//! HTTP handlers for `/job/*` endpoints.

use std::str::FromStr;
use futures::{future, Future};
use log::error;
use serde_derive::*;

use actix_web::{self, HttpResponse};
use actix_web::web::{Json, Path, Query, Data};

use crate::actors::application;
use crate::models::{ApplicationState, job, OcyError};
use serde_json;

#[derive(Deserialize)]
pub struct JobFields {
    fields: Option<String>,
}

/// Handles `GET /job/{job_id}` requests.
///
/// # Returns
///
/// * 200 - JSON response containing all data about a job
/// * 400 - bad request error if any requested fields were not recognised
/// * 404 - not found error if no job with given `job_id` is found
/// * 500 - unexpected internal error
/// * 503 - Redis connection unavailable
#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn index(
    path: Path<u64>,
    query: Query<JobFields>,
    data: Data<ApplicationState>,
) -> Box<Future<Item=HttpResponse, Error=()>> {
    let job_id = path.into_inner();
    let fields = match query.into_inner().fields {
        Some(raw_fields) => {
            let mut fields = Vec::new();
            for raw_field in raw_fields.split(',') {
                match job::Field::from_str(raw_field) {
                    Ok(field) => fields.push(field),
                    Err(_)    => return Box::new(future::ok(HttpResponse::BadRequest().body(format!("Unrecognised field: {}", raw_field)))),
                }
            }
            Some(fields)
        },
        None => None,
    };

    Box::new(data.redis_addr.send(application::GetJobFields(job_id, fields))
        .then(move |res| {
            let msg = match res {
                Ok(msg) => msg,
                Err(err) => Err(OcyError::Internal(err.to_string())),
            };
            match msg {
                Ok(job)                     => Ok(HttpResponse::Ok().json(job)),
                Err(OcyError::NoSuchJob(_)) => Ok(HttpResponse::NotFound().into()),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[job:{}] failed to fetch metadata fields: {}", job_id, err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)                    => {
                    error!("[job:{}] failed to fetch metadata fields: {}", job_id, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        }))
}

/// Handles `GET /job/{job_id}/status` requests.
///
/// # Returns
///
/// * 200 - JSON response containing status string
/// * 404 - not found error if no job with given `job_id` is found
/// * 500 - unexpected internal error
/// * 503 - Redis connection unavailable
#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn status(
    path: Path<u64>,
    data: Data<ApplicationState>
) -> impl Future<Item=HttpResponse, Error=actix_web::Error> {
    let job_id = path.into_inner();
    data.redis_addr.send(application::GetJobStatus(job_id))
        .then(move |res| {
            match res {
                Ok(msg) => {
                    match msg {
                        Ok(status)                  => Ok(HttpResponse::Ok().json(status)),
                        Err(OcyError::NoSuchJob(_)) => Ok(HttpResponse::NotFound().into()),
                        Err(OcyError::RedisConnection(err)) => {
                            error!("[job:{}] failed to fetch status: {}", job_id, err);
                            Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                        },
                        Err(err)                    => {
                            error!("[job:{}] failed to fetch status: {}", job_id, err);
                            Ok(HttpResponse::InternalServerError().body(err.to_string()))
                        },
                    }
                },
                Err(err) => {
                    error!("[job:{}] failed to fetch status: {}", job_id, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
}

/// Handles `PATCH /job/{job_id}` requests. This endpoint allows a job's status and/or output to
/// be updated via a JSON request.
///
/// # Returns
///
/// * 204 - update successfully performed
/// * 400 - bad request, could not perform update with given JSON request
/// * 404 - not found error if no job with given `job_id` is found
/// * 409 - conflict, job not in state where updates allowed
/// * 500 - unexpected internal error
/// * 503 - Redis connection unavailable
#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn update(
    path: Path<u64>,
    json: Json<job::UpdateRequest>,
    data: Data<ApplicationState>,
) -> impl Future<Item=HttpResponse, Error=actix_web::Error> {
    let job_id = path.into_inner();
    let update_req = json.into_inner();
    data.redis_addr.send(application::UpdateJob(job_id, update_req))
        .then(move |res| {
            match res {
                Ok(msg) => {
                    match msg {
                        Ok(_) => Ok(HttpResponse::NoContent().into()),
                        Err(OcyError::BadRequest(msg)) => Ok(HttpResponse::BadRequest().body(msg)),
                        Err(OcyError::Conflict(msg)) => Ok(HttpResponse::Conflict().body(msg)),
                        Err(OcyError::NoSuchJob(_)) => Ok(HttpResponse::NotFound().into()),
                        Err(OcyError::RedisConnection(err)) => {
                            error!("[job:{}] failed to update metadata: {}", job_id, err);
                            Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                        },
                        Err(err) => {
                            error!("[job:{}] failed to update metadata: {}", job_id, err);
                            Ok(HttpResponse::InternalServerError().body(err.to_string()))
                        },
                    }
                },
                Err(err) => {
                    error!("[job:{}] failed to update metadata: {}", job_id, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
}

/// Handles `PUT /job/{job_id}/heartbeat` requests. This endpoint updates the last heartbeat time
/// for a job (heartbeat is used to detect jobs that have timed out).
///
/// # Returns
///
/// * 204 - update successfully performed
/// * 404 - not found error if no job with given `job_id` is found
/// * 409 - unable to update heartbeat, job not in `running` state
/// * 500 - unexpected internal error
/// * 503 - Redis connection unavailable
#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn heartbeat(
    path: Path<u64>,
    data: Data<ApplicationState>
) -> impl Future<Item=HttpResponse, Error=actix_web::Error> {
    let job_id = path.into_inner();
    data.redis_addr.send(application::Heartbeat(job_id))
        .map_err(|err| OcyError::Internal(err.to_string()))
        .then(move |res| {
            match res {
                Ok(_)                          => Ok(HttpResponse::NoContent().reason("Heartbeat updated").finish()),
                Err(OcyError::NoSuchJob(_))    => Ok(HttpResponse::NotFound().into()),
                Err(OcyError::Conflict(msg))   => Ok(HttpResponse::Conflict().body(msg)),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[job:{}] failed to update heartbeat: {}", job_id, err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)                       => {
                    error!("[job:{}] failed to update heartbeat: {}", job_id, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
}

/// Handles `DELETE /job/{job_id}` requests. This endpoint deletes a job from the DB regardless of the
/// state of execution it's in.
///
/// Generally intended to be called after a job has completed/failed, and clients have retrieved any
/// data they need from it.
///
/// Running/queued jobs can be more gracefully removed by updating the job's status to `cancelled`.
///
/// # Returns
///
/// * 204 - update successfully performed
/// * 404 - not found error if no job with given `job_id` is found
/// * 500 - unexpected internal error
/// * 503 - Redis connection unavailable
#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn delete(
    path: Path<u64>,
    data: Data<ApplicationState>
) -> impl Future<Item=HttpResponse, Error=()> {
    let job_id = path.into_inner();
    data.redis_addr.send(application::DeleteJob(job_id))
        .then(move |res| {
            let msg = match res {
                Ok(msg) => msg,
                Err(err) => Err(OcyError::Internal(err.to_string())),
            };
            match msg {
                Ok(true)  => Ok(HttpResponse::NoContent().reason("Job deleted").finish()),
                Ok(false) => Ok(HttpResponse::NotFound().into()),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[job:{}] failed to delete: {}", job_id, err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)  => {
                    error!("[job:{}] failed to delete: {}", job_id, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
}

/// Handles `GET /job/{job_id}/output` requests. Gets the current output for a given job.
///
/// # Returns
///
/// * 200 - JSON response containing job output, if any
/// * 404 - not found error if no job with given `job_id` is found
/// * 500 - unexpected internal error
/// * 503 - Redis connection unavailable
#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn output(
    path: Path<u64>,
    data: Data<ApplicationState>
) -> impl Future<Item=HttpResponse, Error=()> {
    let job_id = path.into_inner();
    data.redis_addr.send(application::GetJobOutput(job_id))
        .then(move |res| {
            let msg = match res {
                Ok(msg) => msg,
                Err(err) => Err(OcyError::Internal(err.to_string())),
            };
            match msg {
                Ok(v)                       => Ok(HttpResponse::Ok().json(v)),
                Err(OcyError::NoSuchJob(_)) => Ok(HttpResponse::NotFound().reason("Job Not Found").finish()),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[job:{}] failed to fetch output: {}", job_id, err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)                    => {
                    error!("[job:{}] failed to fetch output: {}", job_id, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
}

/// Handles `PUT /job/{job_id}/output` requests. Replaces the job's output with given JSON.
///
/// # Returns
///
/// * 204 - if output was successfully updated
/// * 404 - not found error if no job with given `job_id` is found
/// * 409 - job not in "running" state, so output cannot be updated
/// * 500 - unexpected internal error
/// * 503 - Redis connection unavailable
#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn set_output(
    path: Path<u64>,
    json: Json<serde_json::Value>,
    data: Data<ApplicationState>,
) -> impl Future<Item=HttpResponse, Error=()> {
    let job_id = path.into_inner();
    let value = json.into_inner();
    data.redis_addr.send(application::SetJobOutput(job_id, value))
        .then(move |res| {
            let msg = match res {
                Ok(msg) => msg,
                Err(err) => Err(OcyError::Internal(err.to_string())),
            };
            match msg {
                Ok(_)                          => Ok(HttpResponse::NoContent().into()),
                Err(OcyError::NoSuchJob(_))    => Ok(HttpResponse::NotFound().reason("Job Not Found").finish()),
                Err(OcyError::BadRequest(msg)) => Ok(HttpResponse::BadRequest().body(msg)),
                Err(OcyError::Conflict(msg))   => Ok(HttpResponse::Conflict().body(msg)),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[job:{}] failed set output: {}", job_id, err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)                       => {
                    error!("[job:{}] failed set output: {}", job_id, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
}
