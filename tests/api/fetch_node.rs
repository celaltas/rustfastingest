use crate::{
    api::helpers::spawn_app,
    db::helpers::{create_test_nodes, get_random_node},
};
use reqwest::Client;
use rustfastingest::domain::node::Node;

#[actix_rt::test]
async fn test_get_node_by_id() {
    let app = spawn_app().await.expect("test app initialization failed!");
    let client = Client::new();
    let query = "tags=true&relations=true";
    let mut nodes = create_test_nodes(10);
    let node = get_random_node(&mut nodes).unwrap();
    node.relation = None;
    node.direction = None;
    let node_id = node.uuid.to_owned();
    println!("record: {} with id={}", node.name, node_id);
    let _ = app
        .db
        .insert_nodes(nodes)
        .await
        .expect("insert nodes failed");

    let initial_response = client
        .get(&format!("{}/nodes/{}?{}", &app.address, node_id, query))
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(initial_response.status(), reqwest::StatusCode::OK);

    let response = initial_response
        .json::<Node>()
        .await
        .expect("failed to get payload");

    assert_eq!(response.uuid, node_id);
}

#[actix_rt::test]
async fn test_get_node_by_id_non_exist() {
    let app = spawn_app().await.expect("test app initialization failed!");
    let client = Client::new();
    let node_id = "550e8400-e29b-41d4-a716-446655440000";
    let query = "tags=true&relations=true";

    let response = client
        .get(&format!("{}/nodes/{}?{}", &app.address, node_id, query))
        .send()
        .await
        .expect("Failed to execute request.");
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
}

#[actix_rt::test]
async fn test_get_node_by_id_query_parameters() {
    let app = spawn_app().await.expect("test app initialization failed!");
    let client = Client::new();
    let node_id = "550e8400-e29b-41d4-a716-446655440000";

    let test_cases = vec![
        (
            "tags=true&relations=true",
            "Request with both tags=true and relations=true",
        ),
        ("tags=true", "Request with only tags=true"),
        ("relations=true", "Request with only relations=true"),
        (
            "tags=false&relations=false",
            "Request with both tags=false and relations=false",
        ),
    ];

    for (query_params, description) in test_cases {
        let response = client
            .get(&format!(
                "{}/nodes/{}?{}",
                &app.address, node_id, query_params
            ))
            .send()
            .await
            .expect("Failed to execute request.");
        assert!(response.status().is_success(), "{}", description);
    }
}

#[actix_rt::test]
async fn test_get_node_by_id_handler_exist() {
    let app = spawn_app().await.expect("test app initialization failed!");
    let client = Client::new();
    let node_id = 123;
    let response = client
        .get(&format!("{}/nodes/{}", &app.address, node_id))
        .send()
        .await
        .expect("Failed to execute request.");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
}
