
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

const NAMESPACE_UUID: Uuid = Uuid::from_bytes([
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
]);

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphStructure {
    pub nodes: Vec<Node>,
    pub relations: Vec<Relation>,
}



#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    pub name: String,
    #[serde(rename = "type")]
    pub kind:String,
    pub children: Vec<Node>,
    pub tags: Option<Vec<Tag>>,
    pub total_children:Option<i64>,
}


#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tag {
    #[serde(rename = "type")]
    pub kind: String,
    pub value: String,
}


#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Relation {
    #[serde(rename = "type")]
    pub kind: String,
    pub source: Vec<String>,
    pub target: Vec<String>,
    pub tags: Option<Vec<Tag>>,
}


pub fn get_id_from_url(ingestion_id: String, url: String) -> Uuid {
    let unique_id = ingestion_id + "/" + url.as_str();
    let uuid = Uuid::new_v5(&NAMESPACE_UUID, unique_id.as_bytes());
    uuid
}
