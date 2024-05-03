use crate::db::model::RelationModel;
use super::relation::Relation;
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

    pub fn from(relations: Vec<RelationModel>, depth: usize) -> TraversalNode {
        let n = &relations[0];
        let mut node =
            TraversalNode::new(n.uuid.clone(), depth, n.name.clone(), n.node_type.clone());
        let mut ids = vec![];
        for r in &relations[1..] {
            ids.push(r.relates_to.clone().unwrap());
        }
        node.relation_ids = ids;
        node
    }
}
