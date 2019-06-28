use futures::Future;
use log::error;

use actix_web::{self, HttpResponse};
use actix_web::web::{Path, Data};

use crate::actors::application;
use crate::models::{ApplicationState, OcyResult, OcyError};

#[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
pub fn tagged_jobs(
    path: Path<String>,
    data: Data<ApplicationState>,
) -> impl Future<Item=HttpResponse, Error=()> {
    let tag = path.into_inner();
    data.redis_addr.send(application::GetTaggedJobs(tag))
        .then(|res: Result<OcyResult<Vec<u64>>, actix::MailboxError>| {
            let msg = match res {
                Ok(msg) => msg,
                Err(err) => Err(OcyError::Internal(err.to_string())),
            };
            match msg {
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
}
