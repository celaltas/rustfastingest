use crate::api::helpers::spawn_app;
use reqwest::Client;

#[actix_rt::test]
async fn test_health_check() {
    let app = spawn_app().await.expect("test app initialization failed!");
    let client = Client::new();
    let response = client
        .get(&format!("{}/healthcheck", &app.address))
        .send()
        .await
        .expect("Failed to execute request.");

    assert!(response.status().is_success());
}
