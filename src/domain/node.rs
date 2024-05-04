use super::relation::Relation;
use crate::db::model::RelationModel;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub uuid: Uuid,
    pub ingestion_id: String,
    pub name: String,
    pub path: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub tags: Vec<(String, String)>,
    pub relations: Vec<Relation>,
}

impl Node {
    pub fn new(
        uuid: Uuid,
        ingestion_id: String,
        name: String,
        path: String,
        node_type: String,
    ) -> Self {
        Node {
            uuid,
            ingestion_id,
            name,
            path,
            node_type,
            tags: Vec::new(),
            relations: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalNode {
    pub uuid: Uuid,
    pub depth: usize,
    pub name: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub relations: Vec<TraversalNode>,
    pub relation_ids: Vec<String>,
}

impl TraversalNode {
    fn new(uuid: Uuid, depth: usize, name: String, node_type: String) -> Self {
        Self {
            uuid,
            depth,
            name,
            node_type,
            relations: vec![],
            relation_ids: vec![],
        }
    }

    pub fn from(relation: RelationModel, depth: usize) -> TraversalNode {
        let mut node = TraversalNode::new(
            relation.uuid.clone(),
            depth,
            relation.name.clone(),
            relation.node_type.clone(),
        );
        node.relation_ids.push(relation.relates_to.clone().unwrap());
        node
    }
}
