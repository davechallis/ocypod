//! HTTP handlers for the `/health` endpoint.

use futures::Future;
use actix_web::{self, AsyncResponder, HttpRequest, HttpResponse};

use crate::actors::application;
use crate::models::ApplicationState;

#[derive(Serialize)]
#[serde(rename_all="lowercase")]
enum HealthStatus {
    HEALTHY,
    UNHEALTHY,
}

#[derive(Serialize)]
struct Health {
    status: HealthStatus,

    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl Health {
    fn new_healthy() -> Self {
        Health { status: HealthStatus::HEALTHY, error: None }
    }

    fn new_from_error<S: Into<String>>(err: S) -> Self {
        Health { status: HealthStatus::UNHEALTHY, error: Some(err.into()) }
    }
}

/// Handle `GET /health` requests to get a JSON list of all existing queues.
pub fn index(req: &HttpRequest<ApplicationState>) -> Box<Future<Item=HttpResponse, Error=actix_web::Error>> {
    req.state().redis_addr.send(application::CheckHealth)
        .from_err()
        .and_then(|res| {
            match res {
                Ok(_)    => Ok(HttpResponse::Ok().json(Health::new_healthy())),
                Err(err) => {
                    error!("Health check failed: {}", err);
                    Ok(HttpResponse::Ok().json(Health::new_from_error(err.to_string())))
                },
            }
        })
        .responder()
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json;

    #[test]
    fn serialisation() {
        let h = Health::new_healthy();
        assert_eq!(serde_json::to_string(&h).unwrap(), "{\"status\":\"healthy\"}");

        let h = Health::new_from_error("message");
        assert_eq!(serde_json::to_string(&h).unwrap(), "{\"status\":\"unhealthy\",\"error\":\"message\"}");
    }
}
