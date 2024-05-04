use crate::{application::AppState, domain::node::TraversalNode};
use actix_web::{
    error::ErrorInternalServerError,
    get,
    web::{self, Data},
    Error, HttpResponse,
};
use eyre::{eyre, Result};
use futures::{future::BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Clone, Deserialize, Default)]
pub struct TraversalNodeQuery {
    pub direction: String,
    pub relation_type: Option<String>,
    pub max_depth: usize,
}

impl TraversalNodeQuery {
    pub fn validate(&self) -> Result<()> {
        if self.direction.is_empty() {
            return Err(eyre!(
                "Direction cannot be empty. Please use 'in' or 'out'."
            ));
        }

        match self.direction.to_lowercase().as_str() {
            "in" | "out" => {}
            _ => return Err(eyre!("Direction can only be 'in' or 'out'.")),
        }

        if let Some(relation_type) = &self.relation_type {
            match relation_type.to_lowercase().as_str() {
                "parent" | "child" => {}
                _ => return Err(eyre!("Relation type can only be 'parent' or 'child'.")),
            }
        }

        Ok(())
    }

    pub fn convert_to_query_parameter(&self) -> String {
        let relation_type = self.relation_type.as_deref().unwrap_or("");
        format!(
            "direction={}&relation_type={}&max_depth={}",
            self.direction, relation_type, self.max_depth
        )
    }
}

#[get("/traversal/{id}")]
async fn traverse_node_by_id(
    path: web::Path<String>,
    query: web::Query<TraversalNodeQuery>,
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
    let query = query.into_inner();
    if let Err(err) = query.validate() {
        return Err(ErrorInternalServerError(err));
    }

    let result = traverse_node_by_id_internal(uuid, query, state, 0).await;
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

fn traverse_node_by_id_internal<'a>(
    node_id: Uuid,
    query: TraversalNodeQuery,
    state: Data<AppState>,
    depth: usize,
) -> BoxFuture<'a, Result<Option<TraversalNode>>> {

    println!("depth: {}", depth);
    println!("query: {:?}", query);
    println!("node_id: {:?}", node_id);
    println!("***********************************");



    async move {
        let direction = query.direction.clone();
        let relation_type = query.relation_type.clone();
        let result = state
            .db
            .get_node_traversal(node_id, direction, relation_type)
            .await?;
        match result {
            Some(relation) => {
                let mut node = TraversalNode::from(relation, query.max_depth);
                if depth < query.max_depth && node.relation_ids.len() > 0 {
                    let mut handles = vec![];
                    for relation_id in node.relation_ids.iter() {
                        let query = query.clone();
                        let state = state.clone();
                        let depth = depth + 1;
                        let node_id = Uuid::parse_str(&relation_id).unwrap();
                        let handle = tokio::spawn(async move {
                            traverse_node_by_id_internal(node_id, query, state, depth).await
                        });
                        handles.push(handle);
                    }
                    for handle in handles {
                        let result = handle.await??;
                        if let Some(result) = result {
                            node.relations.push(result);
                        }
                    }
                }
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }
    .boxed()
}

#[cfg(test)]
mod tests {

    use crate::routes::traverse_node::TraversalNodeQuery;

    #[test]
    fn test_validate_direction_empty() {
        let query = TraversalNodeQuery {
            direction: String::new(),
            relation_type: None,
            max_depth: 4,
        };
        let result = query.validate();
        assert!(result.is_err());
        let result = result.unwrap_err();

        assert_eq!(
            result.to_string(),
            "Direction cannot be empty. Please use 'in' or 'out'."
        );
    }

    #[test]
    fn test_validate_direction_invalid() {
        let query = TraversalNodeQuery {
            direction: "invalid".to_string(),
            relation_type: None,
            max_depth: 4,
        };
        assert_eq!(
            query.validate().unwrap_err().to_string(),
            "Direction can only be 'in' or 'out'."
        );
    }

    #[test]
    fn test_validate_direction_valid() {
        let query = TraversalNodeQuery {
            direction: "in".to_string(),
            relation_type: None,
            max_depth: 4,
        };
        assert!(query.validate().is_ok());
    }

    #[test]
    fn test_validate_relation_type_invalid() {
        let query = TraversalNodeQuery {
            direction: "out".to_string(),
            relation_type: Some("invalid".to_string()),
            max_depth: 4,
        };
        assert_eq!(
            query.validate().unwrap_err().to_string(),
            "Relation type can only be 'parent' or 'child'."
        );
    }

    #[test]
    fn test_validate_relation_type_valid() {
        let query = TraversalNodeQuery {
            direction: "out".to_string(),
            relation_type: Some("parent".to_string()),
            max_depth: 4,
        };
        assert!(query.validate().is_ok());
    }
}
