use crate::helpers::{
    cleanup_database, create_node_vector, create_sample_node, create_test_database_config,
};
use rustfastingest::db::syclla::ScyllaService;

#[tokio::test]
async fn test_get_node_by_id() {
    let config = create_test_database_config();
    let service = ScyllaService::init(&config)
        .await
        .expect("Initialization database failed:");
    let id_not_exist = "test_id";
    let result = service.get_node_by_id(id_not_exist).await;
    assert!(result.is_err());

    let node = create_sample_node();
    let binding = node.uuid.to_string();
    let exist_id: &str = binding.as_ref();

    service
        .insert_node(node)
        .await
        .expect("Insert node failed:");
    let result = service.get_node_by_id(exist_id).await;
    assert!(result.is_ok());
    let node = result.unwrap();
    assert_eq!(node.name, "Example Node".to_string());
}

#[tokio::test]
async fn test_insert_nodes() {
    let nodes = create_node_vector(10);
    let config = create_test_database_config();
    let service = ScyllaService::init(&config)
        .await
        .expect("Initialization database failed.");
    let result = ScyllaService::insert_nodes(service, nodes).await;
    assert!(result.is_ok());
    let session = ScyllaService::new_session("127.0.0.1:9042".to_string())
        .await
        .expect("failed connection to scylladb");
    let res = session
        .query("SELECT * FROM graph.nodes", &[])
        .await
        .expect("Failed to query database");
    if let Some(rows) = res.rows {
        assert_eq!(10, rows.len());
    }
    cleanup_database(&session).await.expect("error occured.");
}

#[tokio::test]
async fn test_insert_node() {
    let node = create_sample_node();
    let config = create_test_database_config();
    let service = ScyllaService::init(&config)
        .await
        .expect("Initialization database failed:");
    service
        .insert_node(node)
        .await
        .expect("Insert node failed:");

    let res = service
        .client
        .query("SELECT name FROM graph.nodes LIMIT 1", &[])
        .await
        .expect("Failed to query database");

    if let Some(rows) = res.rows {
        for row in rows {
            let (name,): (String,) = row
                .into_typed::<(String,)>()
                .expect("parsing error query result");
            assert_eq!(name, "Example Node".to_string());
        }
    }
    cleanup_database(&service.client)
        .await
        .expect("error occured.");
}
