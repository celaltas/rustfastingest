use crate::{
    api::helpers::{create_traversal_query, spawn_app},
    db::helpers::{create_test_nodes, get_random_node},
};
use reqwest::Client;
use rustfastingest::{domain::node::TraversalNode, routes::traverse_node::TraversalNodeQuery};

#[actix_rt::test]
async fn test_get_traversal_node() {
    let app = spawn_app().await.expect("test app initialization failed!");
    let client = Client::new();
    let query = create_traversal_query();
    let nodes = create_test_nodes(10);
    let node = get_random_node(&nodes).unwrap();
    println!("traverse node = {:#?}", node);
    let node_id = node.uuid.to_owned();
    let _ = app
        .db
        .insert_nodes(nodes)
        .await
        .expect("insert nodes failed");

    let initial_response = client
        .get(&format!("{}/traversal/{}?{}", &app.address, node_id, query))
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(initial_response.status(), reqwest::StatusCode::OK);

    let response = initial_response
        .json::<TraversalNode>()
        .await
        .expect("failed to get payload");
    println!("{:#?}", response);
    assert_eq!(response.uuid, node_id);
}

#[actix_rt::test]
async fn test_traversal_node_handler_nonexist_node() {
    let app = spawn_app().await.expect("test app initialization failed!");
    let client = Client::new();
    let node_id = "550e8400-e29b-41d4-a716-446655440000";
    let query = create_traversal_query();
    let response = client
        .get(&format!("{}/traversal/{}?{}", &app.address, node_id, query))
        .send()
        .await
        .expect("Failed to execute request.");
    assert_eq!(response.status().as_u16(), reqwest::StatusCode::NO_CONTENT);
}

#[actix_rt::test]
async fn test_traversal_node_handler_query_parameters() {
    let app = spawn_app().await.expect("test app initialization failed!");
    let client = Client::new();
    let node_id = "1";
    let test_cases = create_query_parameter_test_cases();
    for (name, query, expected_code) in test_cases {
        let response = client
            .get(&format!(
                "{}/traversal/{}?{}",
                &app.address,
                node_id,
                query.convert_to_query_parameter()
            ))
            .send()
            .await
            .expect("Failed to execute request.");
        assert_eq!(
            response.status().as_u16(),
            expected_code,
            "Test name: {}",
            name
        )
    }
}

#[actix_rt::test]
async fn test_traversal_node_handler_exist() {
    let app = spawn_app().await.expect("test app initialization failed!");
    let client = Client::new();
    let node_id = "1";
    let response = client
        .get(&format!("{}/traversal/{}", &app.address, node_id))
        .send()
        .await
        .expect("Failed to execute request.");
    assert_ne!(response.status().as_u16(), 404);
}

fn create_query_parameter_test_cases() -> Vec<(String, TraversalNodeQuery, u16)> {
    let test_cases = vec![
        (
            "Empty direction string".to_string(),
            TraversalNodeQuery {
                direction: String::new(),
                relation_type: None,
                max_depth: 0,
            },
            500,
        ),
        (
            "Invalid direction value".to_string(),
            TraversalNodeQuery {
                direction: "invalid".to_string(),
                relation_type: None,
                max_depth: 0,
            },
            500,
        ),
        (
            "Valid direction value".to_string(),
            TraversalNodeQuery {
                direction: "in".to_string(),
                relation_type: None,
                max_depth: 0,
            },
            500,
        ),
        (
            "Invalid relation type value".to_string(),
            TraversalNodeQuery {
                direction: "out".to_string(),
                relation_type: Some("invalid".to_string()),
                max_depth: 0,
            },
            500,
        ),
        (
            "Valid relation type value".to_string(),
            TraversalNodeQuery {
                direction: "out".to_string(),
                relation_type: Some("parent".to_string()),
                max_depth: 0,
            },
            200,
        ),
    ];
    test_cases
}
