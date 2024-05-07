use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SearchQueryParams {
    pub query: String,
    pub tag: Option<Tag>,
}

impl SearchQueryParams {
    pub fn to_es_query(&self) -> Value {
        let mut query = json!({
            "query": {
                "bool": {
                    "should": Vec::<Value>::new()
                }
            }
        });

        if let Some(tag) = &self.tag {
            query["query"]["bool"]["should"]
                .as_array_mut()
                .unwrap()
                .push(json!({
                    "match": {
                        "tags.type": tag.type_field
                    }
                }));
            query["query"]["bool"]["should"]
                .as_array_mut()
                .unwrap()
                .push(json!({
                    "wildcard": {
                        "tags.value": tag.value
                    }
                }));
        }

        query["query"]["bool"]["should"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "multi_match": {
                    "query": self.query.clone(),
                    "fields": ["*"],
                }
            }));

        query
    }
}

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
