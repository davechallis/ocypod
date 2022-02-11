//! HTTP handlers for the `/queue` endpoints.

use actix_web::{web, HttpResponse, Responder, ResponseError};
use log::error;

use crate::models::{job, queue, ApplicationState, OcyError};

/// Handle `GET /queue` requests to get a JSON list of all existing queues.
///
/// # Returns
///
/// * 200 - JSON response containing list of queue names.
pub async fn index(data: web::Data<ApplicationState>) -> impl Responder {
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.queue_names(&mut conn).await {
        Ok(queue_names) => HttpResponse::Ok().json(queue_names),
        Err(err) => {
            error!("Failed to fetch queue names: {}", err);
            err.error_response()
        },
    }
}

/// Handles `PUT /queue/{queue_name}` requests.
pub async fn create_or_update(
    path: web::Path<String>,
    json: web::Json<queue::Settings>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let queue_name = path.into_inner();
    let queue_settings = json.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.create_or_update_queue(&mut conn, &queue_name, &queue_settings).await {
        Ok(true) => HttpResponse::Created()
            .append_header(("Location", format!("/queue/{}", queue_name)))
            .finish(),
        Ok(false) => HttpResponse::NoContent()
            .reason("Queue setting updated")
            .append_header(("Location", format!("/queue/{}", queue_name)))
            .finish(),
        Err(err @ OcyError::BadRequest(_)) => err.error_response(),
        Err(err) => {
            error!("[queue:{}] failed to create/update queue: {}", &queue_name, err);
            err.error_response()
        },
    }
}

pub async fn delete(path: web::Path<String>, data: web::Data<ApplicationState>) -> impl Responder {
    let queue_name = path.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.delete_queue(&mut conn, &queue_name).await {
        Ok(true) => HttpResponse::NoContent().reason("Queue deleted").finish(),
        Ok(false) => HttpResponse::NotFound().reason("Queue not found").finish(),
        Err(err @ OcyError::BadRequest(_)) => err.error_response(),
        Err(err) => {
            error!("[queue:{}] failed to delete queue: {}", &queue_name, err);
            err.error_response()
        },
    }
}

pub async fn settings(
    path: web::Path<String>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let queue_name = path.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.queue_settings(&mut conn, &queue_name).await {
        Ok(summary) => HttpResponse::Ok().json(summary),
        Err(err @ OcyError::NoSuchQueue(_)) => err.error_response(),
        Err(err) => {
            error!(
                "[queue:{}] failed to fetch queue summary: {}",
                &queue_name, err
            );
            err.error_response()
        },
    }
}

pub async fn size(path: web::Path<String>, data: web::Data<ApplicationState>) -> impl Responder {
    let queue_name = path.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.queue_size(&mut conn, &queue_name).await {
        Ok(size) => HttpResponse::Ok().json(size),
        Err(err @ OcyError::NoSuchQueue(_)) => err.error_response(),
        Err(err) => {
            error!(
                "[queue:{}] failed to fetch queue size: {}",
                &queue_name, err
            );
            err.error_response()
        }
    }
}

pub async fn job_ids(path: web::Path<String>, data: web::Data<ApplicationState>) -> impl Responder {
    let queue_name = path.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::RedisConnection(err).error_response(),
    };

    match data.redis_manager.queue_job_ids(&mut conn, &queue_name).await {
        Ok(size) => HttpResponse::Ok().json(size),
        Err(err @ OcyError::NoSuchQueue(_)) => err.error_response(),
        Err(err) => {
            error!(
                "[queue:{}] failed to fetch queue size: {}",
                &queue_name, err
            );
            err.error_response()
        }
    }
}

pub async fn create_job(
    path: web::Path<String>,
    json: web::Json<job::CreateRequest>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let queue_name = path.into_inner();
    let job_req = json.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::from(err).error_response(),
    };

    match data.redis_manager.create_job(&mut conn, &queue_name, &job_req).await {
        Ok(job_id) => HttpResponse::Created()
            .append_header(("Location", format!("/job/{}", job_id)))
            .json(job_id),
        Err(err @ OcyError::NoSuchQueue(_) | err @ OcyError::BadRequest(_) ) => err.error_response(),
        Err(err) => {
            error!("[queue:{}] failed to create new job: {}", &queue_name, err);
            err.error_response()
        }
    }
}

pub async fn next_job(
    path: web::Path<String>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let queue_name = path.into_inner();
    let mut conn = match data.pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::from(err).error_response(),
    };

    match data.redis_manager.next_queued_job(&mut conn, &queue_name).await {
        Ok(Some(job)) => HttpResponse::Ok().json(job),
        Ok(None) => match &data.config.server.next_job_delay {
            Some(delay) if !delay.is_zero() => {
                tokio::time::sleep(delay.0).await;
                HttpResponse::NoContent().into()
            }
            _ => HttpResponse::NoContent().into(),
        },
        Err(err) => {
            error!("[queue:{}] failed to fetch next job: {}", &queue_name, err);
            err.error_response()
        }
    }
}
