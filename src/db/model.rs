use std::error::Error;

use scylla::FromRow;
use uuid::Uuid;

use crate::domain::data::{get_id_from_url, Relation, Tag};

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
pub struct NodeModelSimple {
    pub uuid: Uuid,
    pub name: String,
    pub node_type: String,
    pub url: String,
    pub ingestion_id: String,
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

impl NodeModel {
    pub fn root(
        ingestion_id: String,
        name: String,
        url: String,
        node_type: String,
        source_tags: Vec<Tag>,
    ) -> Self {
        let id = get_id_from_url(ingestion_id.clone(), url.clone());
        let tags: Vec<(String, String)> = source_tags
            .iter()
            .map(|a| (a.kind.clone(), a.value.clone()))
            .collect();

        Self {
            uuid: id,
            direction: None,
            relation: None,
            relates_to: None,
            name: name,
            ingestion_id: ingestion_id,
            url: url,
            node_type: node_type,
            tags: Some(tags),
        }
    }
}

impl TryFrom<NodeModelSimple> for NodeModel {
    type Error = Box<dyn Error>;

    fn try_from(node: NodeModelSimple) -> Result<Self, Self::Error> {
        Ok(NodeModel {
            uuid: node.uuid,
            direction: None,
            relation: None,
            relates_to: None,
            name: node.name.to_owned(),
            ingestion_id: node.ingestion_id.to_owned(),
            url: node.url.to_owned(),
            node_type: node.node_type,
            tags: None,
        })
    }
}
