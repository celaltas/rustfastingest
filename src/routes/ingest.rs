use crate::application::AppState;
use crate::db::model::process_nodes;
use crate::domain::relation::process_relations;
use crate::domain::s3::download::read_from_local_file;
use actix_web::{
    post,
    web::{self, Data},
    Error, HttpResponse,
};
use serde::{Deserialize, Serialize};
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

#[post("/ingest")]
async fn ingest(
    payload: web::Json<IngestionRequest>,
    state: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let mut handles = vec![];
    for file in payload.files.iter() {
        let ingestion_id = payload.ingestion_id.clone();
        let file = file.clone();
        let state = state.clone();
        let handle = tokio::spawn(async move { process_file(ingestion_id, file, state).await });
        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok(res) => match res {
                Ok(_) => info!("The record save successfully"),
                Err(err) => error!("Error occured when record saved: {}", err),
            },
            Err(err) => error!("Error joining task: {}", err),
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
        "Processing file {} with ingestion id {}",
        file, ingestion_id
    );
    // let bucket_ops = create_bucket_ops("test_region", "test_bucket")?;
    // let contents = read_graph_from_s3(bucket_ops, &file).await?;
    let contents = read_from_local_file(&file).await?;
    let relations = process_relations(&ingestion_id, contents.relations);
    let nodes = process_nodes(&ingestion_id, contents.nodes, relations).await?;
    let _result = state.db.insert_nodes(nodes).await?;
    Ok(())
}
