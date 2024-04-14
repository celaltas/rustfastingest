
use serde::Deserialize;
use serde::Serialize;


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
