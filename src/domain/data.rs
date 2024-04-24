use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

const NAMESPACE_UUID: Uuid = Uuid::from_bytes([
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
]);

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct GraphData {
    pub nodes: Vec<Node>,
    pub relations: Vec<Relation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub uuid: Uuid,
    pub ingestion_id: String,
    pub name: String,
    pub url: String,
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
        url: String,
        node_type: String,
    ) -> Self {
        Node {
            uuid,
            ingestion_id,
            name,
            url,
            node_type,
            tags: Vec::new(),
            relations: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    #[serde(rename = "type")]
    pub rel_type: String,
    pub outbound: bool,
    pub target_name: String,
    pub relates_to: String,
}

impl Relation {
    pub fn new(rel_type: String, outbound: bool, target_name: String, relates_to: String) -> Self {
        Relation {
            rel_type,
            outbound,
            target_name,
            relates_to,
        }
    }
}
