use std::{sync::Arc, thread::sleep, time::Duration};

use crate::{
    application::AppState,
    s3::download::{create_bucket_ops, read_graph_from_s3},
};
use actix_web::{
    post,
    web::{self, Data},
    Error, HttpResponse,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use tokio::{sync::Semaphore, task};
use tracing::{error, info};

#[derive(Debug, Deserialize, Serialize)]
pub struct IngestionRequest {
    files: Vec<String>,
    ingestion_id: String,
}

impl IngestionRequest {
    pub fn new(files: Vec<String>, ingestion_id: String) -> Self {
        Self {
            files,
            ingestion_id,
        }
    }
}

// #[post("/ingest")]
// async fn ingest(
//     payload: web::Json<IngestionRequest>,
//     state: Data<AppState>,
// ) -> Result<HttpResponse, Error> {
//     let mut handles = vec![];
//     let mut semaphore = Arc::new(Semaphore::new(4));
//     for file in payload.files.iter() {
//         let file = file.clone();
//         let state = state.clone();
//         let semaphore = semaphore.clone();
//         let handle = task::spawn_blocking(move || {
//             let res = block_on(process_file(file, state));
//             res
//         });
//         handles.push(handle);
//     }

//     for handle in handles {
//         let res = handle.await.expect("");
//         match res {
//             Ok(_) => info!("The record save successfully"),
//             Err(_) => error!("Error occured when record saved"),
//         }
//     }

//     Ok(HttpResponse::Ok().json(r#"{ "status": "OK"}"#))
// }

// async fn process_file(file: String, state: Data<AppState>) -> eyre::Result<()> {
//     println!("Ingesting file {}", file);
//     info!("Ingesting file {}", file);
//     sleep(Duration::from_secs(5));
//     println!("file processed\n--------------\n");
//     Ok(())
// }

#[post("/ingest")]
async fn ingest(
    payload: web::Json<IngestionRequest>,
    state: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let semaphore = Arc::new(Semaphore::new(4));
    let mut handles = vec![];
    for file in payload.files.iter() {
        let ingestion_id = payload.ingestion_id.clone();
        let file = file.clone();
        let state = state.clone();
        let semaphore = semaphore.clone();
        let handle = task::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let res = process_file(ingestion_id, file, state).await;
            drop(_permit);
            res
        });
        handles.push(handle);
    }

    for handle in handles {
        let res = handle.await.expect("");
        match res {
            Ok(_) => info!("The record save successfully"),
            Err(_) => error!("Error occured when record saved"),
        }
    }

    Ok(HttpResponse::Ok().json(r#"{ "status": "OK"}"#))
}

async fn process_file(
    ingestion_id: String,
    file: String,
    state: Data<AppState>,
) -> eyre::Result<()> {
    info!(
        "Processing File {} for provider {}. Reading file...",
        file, ingestion_id
    );
    // let bucket_ops = create_bucket_ops("test_region", "test_bucket")?;
    // let contents = read_graph_from_s3(bucket_ops, &file).await?;

    Ok(())
}
