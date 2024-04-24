use rustfastingest::{application::Application, config::config::GeneralConfig};

pub struct TestApp {
    pub address: String,
}

pub async fn spawn_app() -> eyre::Result<TestApp> {
    let mut configuration = GeneralConfig::from_env()?;
    configuration.app.port = 0;
    let application = Application::build(configuration)
        .await
        .expect("Failed to build application");
    let address = format!("http://localhost:{}", application.port());
    let _ = tokio::spawn(application.run());
    Ok(TestApp { address })
}
