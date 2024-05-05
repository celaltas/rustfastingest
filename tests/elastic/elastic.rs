use rustfastingest::elastic::elastic::ElasticService;

use crate::elastic::helpers::{create_test_nodes, get_test_configuration};

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
}

#[tokio::test]
async fn test_elastic_initialize() {
    let config = get_test_configuration();
    let elastic_service = ElasticService::initialize(&config).await;
    assert!(elastic_service.is_ok());
}
