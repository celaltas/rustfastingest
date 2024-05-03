use rustfastingest::{
    application::Application, config::config::GeneralConfig, db::syclla::ScyllaService,
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
