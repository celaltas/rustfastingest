use rustfastingest::{
    config::config::ElasticSearchConfig,
    elastic::model::{IndexNode, Tag},
};
use uuid::Uuid;

pub fn get_test_configuration() -> ElasticSearchConfig {
    let config = ElasticSearchConfig {
        url: "http://localhost:9200".to_string(),
        enabled: true,
        batch_size: 10,
        num_shards: 4,
        index: "graph".to_string(),
        user: Some("elastic".to_string()),
        password: Some("password".to_string()),
        concurrency_limit: 10,
        refresh_interval: "20s".to_string(),
        source_enabled: true,
    };
    config
}

pub fn create_test_nodes(count: usize) -> Vec<IndexNode> {
    let mut nodes = Vec::new();
    for i in 0..count {
        nodes.push(IndexNode {
            uuid: Uuid::new_v4(),
            name: format!("node_{}", i),
            node_type: format!("node_type_{}", i),
            tags: [Tag {
                type_field: format!("type_{}", i),
                value: format!("value_{}", i),
            }]
            .to_vec(),
        });
    }
    nodes
}
