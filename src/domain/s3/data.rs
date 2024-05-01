use serde::Deserialize;
use serde::Serialize;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphData {
    pub nodes: Vec<RawNode>,
    pub relations: Vec<RawRelation>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawNode {
    pub name: String,
    #[serde(rename = "type")]
    pub type_field: String,
    pub children: Vec<RawNode>,
    pub tags: Option<Vec<RawTag>>,
    pub total_children: Option<i64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawTag {
    #[serde(rename = "type")]
    pub type_field: String,
    pub value: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawRelation {
    #[serde(rename = "type")]
    pub type_field: String,
    pub source: Vec<String>,
    pub target: Vec<String>,
    pub tags: Option<Vec<RawTag>>,
}
