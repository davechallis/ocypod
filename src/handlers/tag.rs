use log::error;

use actix_web::{web, HttpResponse, Responder};

use crate::application::RedisManager;
use crate::models::{ApplicationState, OcyError};

pub async fn tagged_jobs(
    path: web::Path<String>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let tag = path.into_inner();
    let mut conn = data.redis_conn_manager.clone();

    match RedisManager::tagged_job_ids(&mut conn, &tag).await {
        Ok(tags) => HttpResponse::Ok().json(tags),
        Err(OcyError::RedisConnection(err)) => {
            error!("Failed to read tag data: {}", err);
            HttpResponse::ServiceUnavailable().body(err)
        }
        Err(err) => {
            error!("Failed to read tag data: {}", err);
            HttpResponse::InternalServerError().body(err)
        }
    }
}
