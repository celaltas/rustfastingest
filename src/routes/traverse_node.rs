use actix_web::{get, web, Result};

#[get("/traversal/{id}")]
async fn traverse_node_by_id(path: web::Path<String>) -> Result<String> {
    let node_id = path.into_inner();
    Ok(format!("traversal node id {}!", node_id))
}
