use crate::{application::AppState, domain::node::Node};
use actix_web::{
    error::ErrorInternalServerError,
    get,
    web::{self, Data},
    Error, HttpResponse, Result,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Clone, Deserialize, Default)]
struct NodeQuery {
    tags: Option<bool>,
    relations: Option<bool>,
}

#[get("/nodes/{id}")]
async fn get_node_by_id(
    path: web::Path<String>,
    query_data: web::Query<NodeQuery>,
    state: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let node_id = path.into_inner();
    let uuid = match Uuid::parse_str(&node_id) {
        Ok(uuid) => uuid,
        Err(err) => {
            tracing::error!("Error parsing id: {:?}", err);
            return Err(ErrorInternalServerError(err));
        }
    };
    let query_data = query_data.into_inner();
    let result = get_node_by_id_internal(uuid, query_data, state).await;
    match result {
        Ok(node) => {
            if let Some(node) = node {
                Ok(HttpResponse::Ok().json(node))
            } else {
                Ok(HttpResponse::NoContent().finish())
            }
        }
        Err(err) => {
            tracing::error!("Error fetching node: {:?}", err);
            Err(ErrorInternalServerError(err))
        }
    }
}

async fn get_node_by_id_internal(
    uuid: Uuid,
    query: NodeQuery,
    state: Data<AppState>,
) -> eyre::Result<Option<Node>> {
    let tags = query.tags.unwrap_or(true);
    let relations = query.relations.unwrap_or(true);
    let result = state.db.get_node(uuid, tags, relations).await?;
    match result {
        Some(node) => Ok(Some(Node::from(node))),
        None => Ok(None),
    }
}
