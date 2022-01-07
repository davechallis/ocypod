use log::error;

use actix_web::{web, HttpResponse, Responder, ResponseError};

use crate::application::RedisManager;
use crate::models::{ApplicationState, OcyError};

pub async fn tagged_jobs(
    path: web::Path<String>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let tag = path.into_inner();
    let mut conn = match data.redis_conn_pool.get().await {
        Ok(conn) => conn,
        Err(err) => return OcyError::from(err).error_response(),
    };

    match RedisManager::tagged_job_ids(&mut conn, &tag).await {
        Ok(tags) => HttpResponse::Ok().json(tags),
        Err(err) => {
            error!("Failed to read tag data: {}", err);
            err.error_response()
        }
    }
}
