use crate::domain::s3::data::RawRelation;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

const NAMESPACE_UUID: Uuid = Uuid::from_bytes([
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
]);

pub fn url_to_uuid(ingestion_id: String, url: String) -> Uuid {
    let unique_id = format!("{}/{}", ingestion_id, url);
    Uuid::new_v5(&NAMESPACE_UUID, unique_id.as_bytes())
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
    pub fn new(ingestion_id: String, rel_type: String, url: String, outbound: bool) -> Self {
        let name = url
            .split("/")
            .last()
            .map_or("default".to_string(), |s| s.to_string());
        let relates_to = url_to_uuid(ingestion_id, url);
        Self {
            rel_type: rel_type,
            outbound: outbound,
            target_name: name,
            relates_to: relates_to.to_string(),
        }
    }
}

pub fn process_relations(
    ingestion_id: &str,
    relations: Vec<RawRelation>,
) -> HashMap<String, Vec<Relation>> {
    let mut rels: HashMap<String, Vec<Relation>> = HashMap::new();

    for r in relations {
        let source = join_paths(&r.source);
        let target = join_paths(&r.target);

        let relation = Relation::new(
            ingestion_id.to_owned(),
            r.type_field.clone(),
            target.clone(),
            true,
        );
        rels.entry(source.clone()).or_insert(vec![]).push(relation);

        let relation = Relation::new(
            ingestion_id.to_owned(),
            r.type_field.clone(),
            source.clone(),
            false,
        );
        rels.entry(target.clone()).or_insert(vec![]).push(relation);
    }

    rels
}

fn join_paths(paths: &[String]) -> String {
    paths
        .iter()
        .map(|p| p.as_str())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use crate::domain::{
        relation::{join_paths, process_relations},
        s3::data::{RawRelation, RawTag},
    };

    #[test]
    fn test_process_relations() {
        let type_field_1 = "type_field_1".to_string();
        let type_field_2 = "type_field_2".to_string();

        let source_a = vec!["sourceA1".to_string()];
        let target_a = vec!["targetA1".to_string()];

        let source_b = vec!["sourceB1".to_string(), "sourceB2".to_string()];
        let target_b = vec!["targetB1".to_string(), "targetB2".to_string()];

        let tags1 = Some(vec![RawTag {
            type_field: "tag_type_x".to_string(),
            value: "value".to_string(),
        }]);
        let tags2 = None;

        let relations = vec![
            RawRelation {
                type_field: type_field_1,
                source: source_a,
                target: target_a,
                tags: tags1,
            },
            RawRelation {
                type_field: type_field_2,
                source: source_b,
                target: target_b,
                tags: tags2,
            },
        ];
        let result = process_relations("test", relations);
        println!("{:?}", result);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_join_paths() {
        let path = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let result = join_paths(&path);
        assert_eq!(result, "a/b/c");

        let single_path = vec!["a".to_string()];
        let result = join_paths(&single_path);
        assert_eq!(result, "a");
    }
}
