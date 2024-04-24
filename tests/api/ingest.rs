use crate::helpers::spawn_app;
use reqwest::Client;
use rustfastingest::routes::ingest::IngestionRequest;

#[actix_rt::test]
async fn test_ingest() {
    let app = spawn_app().await.expect("test app initialization failed!");
    let client = Client::new();
    let test_files = create_test_files(10);
    let ingestion_id = "test_ingestion_id".to_string();
    let payload = IngestionRequest::new(test_files, ingestion_id);
    let start = std::time::Instant::now();

    let response = client
        .post(&format!("{}/ingest", &app.address))
        .json(&payload)
        .send()
        .await
        .expect("Failed to execute request.");

    let duration = start.elapsed();
    println!("Ingest request took: {:?}", duration);
    println!("{:?}", response);

    assert!(response.status().is_success());
}

fn create_test_files(count: usize) -> Vec<String> {
    let mut test_files = Vec::new();
    for i in 0..count {
        test_files.push(format!("test_files/test_file_{}.txt", i));
    }
    test_files
}
