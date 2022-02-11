//! HTTP handlers for `/job/*` endpoints.

use std::str::FromStr;

use log::error;
use serde::Deserialize;
use actix_web::{web, HttpResponse, Responder, ResponseError};

use crate::models::{job, ApplicationState, OcyError};

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
pub async fn index(
    path: web::Path<u64>,
    query: web::Query<JobFields>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let job_id = path.into_inner();
    let fields = match query.into_inner().fields {
        Some(raw_fields) => {
            let mut fields = Vec::new();
            for raw_field in raw_fields.split(',') {
                match job::Field::from_str(raw_field) {
                    Ok(field) => fields.push(field),
                    Err(_) => {
                        return HttpResponse::BadRequest()
                            .body(format!("Unrecognised field: {}", raw_field))
                    }
                }
            }
            Some(fields)
        }
        None => None,
    };

    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.job_fields(&mut conn, job_id, fields.as_deref()).await {
        Ok(job) => HttpResponse::Ok().json(job),
        Err(err @ OcyError::NoSuchJob(_)) => err.error_response(),
        Err(err) => {
            error!("[job:{}] failed to fetch metadata fields: {}", job_id, err);
            err.error_response()
        },
    }
}

/// Handles `GET /job/{job_id}/status` requests.
///
/// # Returns
///
/// * 200 - JSON response containing status string
/// * 404 - not found error if no job with given `job_id` is found
/// * 500 - unexpected internal error
/// * 503 - Redis connection unavailable
pub async fn status(path: web::Path<u64>, data: web::Data<ApplicationState>) -> impl Responder {
    let job_id = path.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.job_status(&mut conn, job_id).await {
        Ok(status) => HttpResponse::Ok().json(status),
        Err(err @ OcyError::NoSuchJob(_)) => err.error_response(),
        Err(err) => {
            error!("[job:{}] failed to fetch status: {}", job_id, err);
            err.error_response()
        },
    }
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
pub async fn update(
    path: web::Path<u64>,
    json: web::Json<job::UpdateRequest>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let job_id = path.into_inner();
    let update_req = json.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.update_job(&mut conn, job_id, &update_req).await {
        Ok(_) => HttpResponse::NoContent().into(),
        Err(err @ OcyError::BadRequest(_) | err @ OcyError::Conflict(_) | err @ OcyError::NoSuchJob(_)) => err.error_response(),
        Err(err) => {
            error!("[job:{}] failed to update metadata: {}", job_id, err);
            err.error_response()
        },
    }
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
pub async fn heartbeat(path: web::Path<u64>, data: web::Data<ApplicationState>) -> impl Responder {
    let job_id = path.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.update_job_heartbeat(&mut conn, job_id).await {
        Ok(_) => HttpResponse::NoContent()
            .reason("Heartbeat updated")
            .finish(),
        Err(err @ OcyError::NoSuchJob(_)) | Err(err @ OcyError::Conflict(_)) => err.error_response(),
        Err(err) => {
            error!("[job:{}] failed to update heartbeat: {}", job_id, err);
            err.error_response()
        },
    }
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
pub async fn delete(path: web::Path<u64>, data: web::Data<ApplicationState>) -> impl Responder {
    let job_id = path.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.delete_job(&mut conn, job_id).await {
        Ok(true) => HttpResponse::NoContent().reason("Job deleted").finish(),
        Ok(false) => HttpResponse::NotFound().into(),
        Err(err) => {
            error!("[job:{}] failed to delete: {}", job_id, err);
            err.error_response()
        },
    }
}

/// Handles `GET /job/{job_id}/output` requests. Gets the current output for a given job.
///
/// # Returns
///
/// * 200 - JSON response containing job output, if any
/// * 404 - not found error if no job with given `job_id` is found
/// * 500 - unexpected internal error
/// * 503 - Redis connection unavailable
pub async fn output(path: web::Path<u64>, data: web::Data<ApplicationState>) -> impl Responder {
    let job_id = path.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.job_output(&mut conn, job_id).await {
        Ok(v) => HttpResponse::Ok().json(v),
        Err(err @ OcyError::NoSuchJob(_)) => err.error_response(),
        Err(err) => {
            error!("[job:{}] failed to fetch output: {}", job_id, err);
            err.error_response()
        }
    }
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
pub async fn set_output(
    path: web::Path<u64>,
    json: web::Json<serde_json::Value>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let job_id = path.into_inner();
    let value = json.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.set_job_output(&mut conn, job_id, &value).await {
        Ok(_) => HttpResponse::NoContent().into(),
        Err(err @ OcyError::NoSuchJob(_) | err @ OcyError::BadRequest(_) | err @ OcyError::Conflict(_)) => err.error_response(),
        Err(err) => {
            error!("[job:{}] failed set output: {}", job_id, err);
            err.error_response()
        },
    }
}
