use futures::Future;

use actix_web::{self, Path, State, AsyncResponder, HttpResponse};

use crate::actors::application;
use crate::models::{ApplicationState, OcyError};

// TODO: re-add in 1.31 #[allow(clippy::needless_pass_by_value)]
pub fn tagged_jobs(
    path: Path<String>,
    state: State<ApplicationState>
) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    let tag = path.into_inner();
    state.redis_addr.send(application::GetTaggedJobs(tag))
        .from_err()
        .and_then(|res| {
            match res {
                Ok(tags) => Ok(HttpResponse::Ok().json(tags)),
                Err(OcyError::RedisConnection(err)) => {
                    error!("Failed to read tag data: {}", err);
                    Ok(HttpResponse::ServiceUnavailable().body(err.to_string()))
                },
                Err(err) => {
                    error!("Failed to read tag data: {}", err);
                    Ok(HttpResponse::InternalServerError().body(err.to_string()))
                },
            }
        })
        .responder()
}
