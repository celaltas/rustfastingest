use crate::domain::{
    relation::Relation,
    s3::data::{RawNode, RawTag},
};
use scylla::FromRow;
use std::collections::HashMap;
use uuid::Uuid;

const NAMESPACE_UUID: Uuid = Uuid::from_bytes([
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
]);

pub fn path_from_name(path: &str, name: &str) -> String {
    format!("{}/{}", path, name)
}

pub fn path_to_uuid(ingestion_id: &str, path: &str) -> Uuid {
    let unique_id = format!("{}/{}", ingestion_id, path);
    Uuid::new_v5(&NAMESPACE_UUID, unique_id.as_bytes())
}

pub fn extract_tag_pairs(tags: Vec<RawTag>) -> Vec<(String, String)> {
    tags.into_iter()
        .map(|RawTag { type_field, value }| (type_field, value))
        .collect()
}

#[derive(Debug)]
pub enum Direction {
    In,
    Out,
}

impl Direction {
    fn as_string(&self) -> String {
        match self {
            Direction::In => "In".to_string(),
            Direction::Out => "Out".to_string(),
        }
    }
}

#[derive(Debug)]
enum Role {
    Parent,
    Child,
}

impl Role {
    fn as_string(&self) -> String {
        match self {
            Role::Parent => "Parent".to_string(),
            Role::Child => "Child".to_string(),
        }
    }
}

#[derive(Default, Debug, Clone, FromRow)]
pub struct NodeModel {
    pub uuid: Uuid,
    pub direction: Option<String>,
    pub relation: Option<String>,
    pub relates_to: Option<String>,
    pub name: String,
    pub ingestion_id: String,
    pub path: String,
    pub node_type: String,
    pub tags: Option<Vec<(String, String)>>,
}

impl NodeModel {
    fn root(
        ingestion_id: String,
        path: String,
        name: String,
        node_type: String,
        raw_tags: Vec<RawTag>,
    ) -> Self {
        let id = path_to_uuid(&ingestion_id, &path);
        let tags: Vec<(String, String)> = extract_tag_pairs(raw_tags);
        Self {
            uuid: id,
            direction: None,
            relation: None,
            relates_to: None,
            name: name,
            ingestion_id: ingestion_id,
            path: path,
            node_type: node_type,
            tags: Some(tags),
        }
    }

    fn relation(
        uuid: Uuid,
        ingestion_id: String,
        direction: String,
        relation: String,
        relates_to: String,
        relates_to_name: String,
    ) -> Self {
        Self {
            uuid,
            direction: Some(direction),
            relation: Some(relation),
            relates_to: Some(relates_to),
            name: relates_to_name,
            ingestion_id,
            path: "".to_owned(),
            node_type: "".to_owned(),
            tags: None,
        }
    }

    fn from_relation(uuid: Uuid, ingestion_id: String, relation: &Relation) -> Self {
        let direction = if relation.outbound {
            Direction::Out
        } else {
            Direction::In
        };

        Self {
            uuid,
            direction: Some(direction.as_string()),
            relation: Some(relation.rel_type.to_owned()),
            relates_to: Some(relation.relates_to.to_owned()),
            name: relation.target_name.to_owned(),
            ingestion_id,
            path: "".to_string(),
            node_type: "".to_string(),
            tags: None,
        }
    }
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

pub async fn process_nodes(
    ingestion_id: &str,
    raw_nodes: Vec<RawNode>,
    relations: HashMap<String, Vec<Relation>>,
) -> eyre::Result<Vec<NodeModel>> {
    let mut db_nodes: Vec<NodeModel> = Vec::new();
    let parent = None;
    let path: String = String::new();
    flatten_nodes(
        ingestion_id,
        &raw_nodes,
        &path,
        &parent,
        &mut db_nodes,
        &relations,
    );
    Ok(db_nodes)
}

fn flatten_nodes(
    ingestion_id: &str,
    raw_nodes: &Vec<RawNode>,
    path: &String,
    parent: &Option<(Uuid, String)>,
    nodes: &mut Vec<NodeModel>,
    relations: &HashMap<String, Vec<Relation>>,
) -> eyre::Result<()> {
    for raw_node in raw_nodes {
        let empty = Vec::new();
        let tags = raw_node.tags.as_ref().get_or_insert(&empty).clone();
        let path = path_from_name(path, &raw_node.name);
        let root = NodeModel::root(
            ingestion_id.to_owned(),
            path.clone(),
            raw_node.name.clone(),
            raw_node.type_field.clone(),
            tags.to_vec(),
        );

        let id = root.uuid;
        let name = root.name.clone();
        nodes.push(root);

        if let Some(parent) = parent {
            let (parent_id, parent_name) = parent;
            let relation = NodeModel::relation(
                id,
                ingestion_id.to_owned(),
                Direction::In.as_string(),
                Role::Parent.as_string(),
                parent_id.to_string(),
                parent_name.to_owned(),
            );
            nodes.push(relation);
        }

        let empty_rel = &mut Vec::new();
        for r in relations.get(&path).get_or_insert(empty_rel).iter() {
            nodes.push(NodeModel::from_relation(id, ingestion_id.to_owned(), r));
        }

        for c in &raw_node.children {
            let child_path = path_from_name(&path, &c.name);
            let child_id = path_to_uuid(&ingestion_id, &child_path);
            let rel = NodeModel::relation(
                id,
                ingestion_id.to_owned(),
                Direction::Out.as_string(),
                Role::Child.as_string(),
                child_id.to_string(),
                c.name.clone(),
            );
            nodes.push(rel);
        }

        if !raw_node.children.is_empty() {
            let parent = Some((id, name));
            let _ = flatten_nodes(
                ingestion_id,
                &raw_node.children,
                &path,
                &parent,
                nodes,
                relations,
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{extract_tag_pairs, flatten_nodes};
    use crate::domain::s3::data::{RawNode, RawTag};
    use std::collections::HashMap;

    #[test]
    fn test_convert_tag() {
        let tags = vec![
            RawTag {
                type_field: "type_1".to_string(),
                value: "value_1".to_string(),
            },
            RawTag {
                type_field: "type_2".to_string(),
                value: "value2".to_string(),
            },
        ];
        let tags1 = tags.clone();
        let result = extract_tag_pairs(tags1);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_flatten_nodes() {
        let ingestion_id = "ingestion_id";
        let raw_node_1 = RawNode {
            name: "node1".to_string(),
            type_field: "type1".to_string(),
            children: vec![],
            tags: None,
            total_children: Some(0),
        };
        let raw_node_2 = RawNode {
            name: "node2".to_string(),
            type_field: "type_2".to_string(),
            children: vec![raw_node_1],
            tags: None,
            total_children: Some(1),
        };
        let raw_node_3 = RawNode {
            name: "node3".to_string(),
            type_field: "type_3".to_string(),
            children: vec![raw_node_2],
            tags: None,
            total_children: Some(1),
        };

        let raw_nodes = vec![raw_node_3];

        let path = "".to_string();
        let parent = None;
        let mut nodes = vec![];
        let relations = HashMap::new();
        let result = flatten_nodes(
            ingestion_id,
            &raw_nodes,
            &path,
            &parent,
            &mut nodes,
            &relations,
        );
        println!("nodes={:#?}", nodes);
        assert!(result.is_ok());
    }
}
