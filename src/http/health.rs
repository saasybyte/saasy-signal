use std::sync::Arc;

use actix_web::{get, web, HttpResponse, Responder};
use tokio::sync::RwLock;

use crate::background::HealthStatus;

#[get("/health/live")]
pub async fn liveness() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "alive"
    }))
}

#[get("/health/ready")]
pub async fn readiness(
    status: web::Data<Arc<RwLock<HealthStatus>>>,
) -> impl Responder {
    let status = status.read().await;
    
    if status.is_ready() {
        HttpResponse::Ok().json(serde_json::json!({
            "status": "ready",
            "dependencies": {
                "sfu": status.sfu
            }
        }))
    } else {
        HttpResponse::ServiceUnavailable().json(serde_json::json!({
            "status": "not_ready",
            "dependencies": {
                "sfu": status.sfu
            }
        }))
    }
}
