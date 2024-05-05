use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Tag {
    #[serde(rename = "type")]
    pub type_field: String,
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexNode {
    pub uuid: Uuid,
    pub name: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub tags: Vec<Tag>,
}
