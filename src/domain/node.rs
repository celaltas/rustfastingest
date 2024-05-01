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
