use scylla::FromRow;
use uuid::Uuid;

#[derive(Default, Debug, Clone, FromRow)]
pub struct NodeModel {
    pub uuid: Uuid,
    pub direction: Option<String>,
    pub relation: Option<String>,
    pub relates_to: Option<String>,
    pub name: String,
    pub ingestion_id: String,
    pub url: String,
    pub node_type: String,
    pub tags: Option<Vec<(String, String)>>,
}

#[derive(Default, Debug, Clone, FromRow)]
pub struct RelationModel {
    pub uuid: Uuid,
    pub direction: Option<String>,
    pub relation: Option<String>,
    pub relates_to: Option<String>,
    pub name: String,
    pub node_type: String,
}
