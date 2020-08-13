//! Defines handlers for health check HTTP endpoints.

use actix_web::{web, HttpResponse, Responder};
use serde::Serialize;

use crate::models::ApplicationState;

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum HealthStatus {
    Healthy,
    Unhealthy,
}

#[derive(Serialize)]
struct Health {
    status: HealthStatus,

    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl Health {
    fn new_healthy() -> Self {
        Health {
            status: HealthStatus::Healthy,
            error: None,
        }
    }

    fn new_from_error<S: Into<String>>(err: S) -> Self {
        Health {
            status: HealthStatus::Unhealthy,
            error: Some(err.into()),
        }
    }
}

pub async fn index(data: web::Data<ApplicationState>) -> impl Responder {
    let mut conn = data.redis_conn_manager.clone();

    let reply: String = match redis::cmd("PING").query_async(&mut conn).await {
        Ok(s) => s,
        Err(err) => return HttpResponse::Ok().json(Health::new_from_error(err.to_string())),
    };

    match reply.as_ref() {
        "PONG" => HttpResponse::Ok().json(Health::new_healthy()),
        other => HttpResponse::Ok().json(Health::new_from_error(format!(
            "unexpected PING response from Redis: {}",
            other
        ))),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json;

    #[test]
    fn serialisation() {
        let h = Health::new_healthy();
        assert_eq!(
            serde_json::to_string(&h).unwrap(),
            "{\"status\":\"healthy\"}"
        );

        let h = Health::new_from_error("message");
        assert_eq!(
            serde_json::to_string(&h).unwrap(),
            "{\"status\":\"unhealthy\",\"error\":\"message\"}"
        );
    }
}
