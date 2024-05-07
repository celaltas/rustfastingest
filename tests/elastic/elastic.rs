use rustfastingest::elastic::{
    elastic::ElasticService,
    model::{IndexNode, SearchQueryParams, Tag},
};
use uuid::Uuid;

use crate::elastic::helpers::{create_test_nodes, get_test_configuration};

#[tokio::test]
async fn test_search() {
    let config = get_test_configuration();
    let elastic_service = ElasticService::initialize(&config)
        .await
        .expect("initialize elastic service failed");

    let neo = IndexNode {
        uuid: Uuid::new_v4(),
        name: "Neo".to_string(),
        node_type: "The One".to_string(),
        tags: vec![
            Tag {
                type_field: "Skill".to_string(),
                value: "Martial Arts".to_string(),
            },
            Tag {
                type_field: "Vehicle".to_string(),
                value: "Nebuchadnezzar".to_string(),
            },
            Tag {
                type_field: "Goal".to_string(),
                value: "Free Humanity".to_string(),
            },
        ],
    };

    let agent_smith = IndexNode {
        uuid: Uuid::new_v4(),
        name: "Agent Smith".to_string(),
        node_type: "Program".to_string(),
        tags: vec![
            Tag {
                type_field: "Purpose".to_string(),
                value: "Eliminate The One".to_string(),
            },
            Tag {
                type_field: "Ability".to_string(),
                value: "Self-Replication".to_string(),
            },
            Tag {
                type_field: "Weakness".to_string(),
                value: "Vulnerability to EMP".to_string(),
            },
        ],
    };

    let test_nodes = [neo, agent_smith].to_vec();
    let index_name = "graph";
    let result = elastic_service.index_nodes(test_nodes, index_name).await;
    assert!(result.is_ok());

    let search_query_params = SearchQueryParams {
        query: "Neo".to_string(),
        tag: None,
    };
    let result = elastic_service
        .search(search_query_params, index_name)
        .await;
    println!("{:?}", result);
    assert!(result.is_ok());
    let _res = elastic_service
        .delete_all_record(index_name)
        .await
        .expect("deleting all records failed!");
}

#[tokio::test]
async fn test_batch_indexing() {
    let config = get_test_configuration();
    let elastic_service = ElasticService::initialize(&config)
        .await
        .expect("initialize elastic service failed");
    let test_nodes = create_test_nodes(20);
    let index_name = "graph";
    let result = elastic_service.index_nodes(test_nodes, index_name).await;
    assert!(result.is_ok());
    let _res = elastic_service
        .delete_all_record(index_name)
        .await
        .expect("deleting all records failed!");
}

#[tokio::test]
async fn test_elastic_initialize() {
    let config = get_test_configuration();
    let elastic_service = ElasticService::initialize(&config).await;
    assert!(elastic_service.is_ok());
}
