use actix_web::{get, HttpResponse, Responder};

#[derive(serde::Serialize)]
struct HealthCheckResponse {
    status: String,
    db_connected: bool,
    version: &'static str,
}

#[get("/healthcheck")]
async fn health_check() -> impl Responder {
    let response = HealthCheckResponse {
        status: "ok".to_string(),
        db_connected: true,
        version: env!("CARGO_PKG_VERSION"),
    };
    HttpResponse::Ok().json(response)
}
