//! HTTP handlers for `/job/*` endpoints.

// TODO: docs are very out of date, ensure they match return values

use std::str::FromStr;
use futures::{future, Future};

use actix_web::{self, Path, State, Json, Query, AsyncResponder, HttpResponse};

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
pub fn index(
    (path, query): (Path<u64>, Query<JobFields>),
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
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

    state.redis_addr.send(application::GetJobFields(job_id, fields))
        .from_err()
        .and_then(move |res| {
            match res {
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
        })
        .responder()
}

/// Handles `GET /job/{job_id}/status` requests.
///
/// # Returns
///
/// * 200 - JSON response containing status string
/// * 404 - not found error if no job with given `job_id` is found
pub fn status(
    path: Path<u64>,
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let job_id = path.into_inner();
    state.redis_addr.send(application::GetJobStatus(job_id))
        .from_err()
        .and_then(move |res| {
            match res {
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
        })
        .responder()
}

/// Handles `PATCH /job/{job_id}` requests. This endpoint allows a job's status and/or output to
/// be updated via a JSON request.
///
/// # Returns
///
/// * 204 - update successfully performed
/// * 400 - bad request, could not perform update with given JSON request
/// * 404 - not found error if no job with given `job_id` is found
pub fn update(
    (path, json): (Path<u64>, Json<job::UpdateRequest>),
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let job_id = path.into_inner();
    let update_req = json.into_inner();
    state.redis_addr.send(application::UpdateJob(job_id, update_req))
        .from_err()
        .and_then(move |res| {
            match res {
                Ok(_)                          => Ok(HttpResponse::NoContent().into()),
                Err(OcyError::BadRequest(msg)) => Ok(HttpResponse::BadRequest().body(msg)),
                Err(OcyError::NoSuchJob(_))    => Ok(HttpResponse::NotFound().into()),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[job:{}] failed to update metadata: {}", job_id, err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err)                       => {
                    error!("[job:{}] failed to update metadata: {}", job_id, err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
        .responder()
}

/// Handles `PUT /job/{job_id}/heartbeat` requests. This endpoint updates the last heartbeat time
/// for a job (heartbeat is used to detect jobs that have timed out).
///
/// # Returns
///
/// * 204 - update successfully performed
/// * 404 - not found error if no job with given `job_id` is found
/// * 409 - unable to update heartbeat, job not in `running` state
pub fn heartbeat(
    path: Path<u64>,
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let job_id = path.into_inner();
    state.redis_addr.send(application::Heartbeat(job_id))
        .from_err()
        .and_then(move |res| {
            match res {
                Ok(_)                          => Ok(HttpResponse::NoContent().reason("Heartbeat updated").finish()),
                Err(OcyError::NoSuchJob(_))    => Ok(HttpResponse::NotFound().into()),
                Err(OcyError::BadRequest(msg)) => Ok(HttpResponse::Conflict().body(msg)),
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
        .responder()
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
/// * 400 - bad request, could not perform update with given JSON request
/// * 404 - not found error if no job with given `job_id` is found
pub fn delete(
    path: Path<u64>,
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let job_id = path.into_inner();
    state.redis_addr.send(application::DeleteJob(job_id))
        .from_err()
        .and_then(move |res| {
            match res {
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
        .responder()
}

/// Handles `GET /job/{job_id}/output` requests. Gets the current output for a given job.
///
/// # Returns
///
/// * 200 - JSON response containing job output, if any
/// * 404 - not found error if no job with given `job_id` is found
pub fn output(
    path: Path<u64>,
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let job_id = path.into_inner();
    state.redis_addr.send(application::GetJobOutput(job_id))
        .from_err()
        .and_then(move |res| {
            match res {
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
        .responder()
}

/// Handles `PUT /job/{job_id}/output` requests. Replaces the job's output with given JSON.
///
/// # Returns
///
/// * 204 - if output was successfully updated
/// * 404 - not found error if no job with given `job_id` is found
pub fn set_output(
    (path, json): (Path<u64>, Json<serde_json::Value>),
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let job_id = path.into_inner();
    let value = json.into_inner();
    state.redis_addr.send(application::SetJobOutput(job_id, value))
        .from_err()
        .and_then(move |res| {
            match res {
                Ok(_)                          => Ok(HttpResponse::NoContent().into()),
                Err(OcyError::NoSuchJob(_))    => Ok(HttpResponse::NotFound().reason("Job Not Found").finish()),
                Err(OcyError::BadRequest(msg)) => Ok(HttpResponse::Conflict().body(msg)),
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
        .responder()
}
