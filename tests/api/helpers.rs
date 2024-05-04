use rustfastingest::{
    application::Application, config::config::GeneralConfig, db::syclla::ScyllaService,
    routes::traverse_node::TraversalNodeQuery,
};

pub struct TestApp {
    pub address: String,
    pub db: ScyllaService,
}

pub async fn spawn_app() -> eyre::Result<TestApp> {
    let mut configuration = GeneralConfig::from_env()?;
    configuration.app.port = 0;
    let application = Application::build(configuration.clone())
        .await
        .expect("Failed to build application");
    let address = format!("http://localhost:{}", application.port());
    let _ = tokio::spawn(application.run());

    let service = ScyllaService::init(&configuration.db)
        .await
        .expect("Initialization database failed:");

    Ok(TestApp {
        address,
        db: service,
    })
}

pub fn create_traversal_query() -> String {
    let traversal_node_query = TraversalNodeQuery {
        direction: "In".to_string(),
        relation_type: Some("Child".to_string()),
        max_depth: 2,
    };
    let query = traversal_node_query.convert_to_query_parameter();
    query
}
