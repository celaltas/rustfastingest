use eyre::Result;
use rustfastingest::{
    application::Application,
    config::config::GeneralConfig,
    telemetry::{get_subscriber, init_subscriber},
};

#[actix_web::main]
async fn main() -> Result<()> {
    let subscriber = get_subscriber("app".into(), "info".into());
    init_subscriber(subscriber);
    let configuration = GeneralConfig::from_env().expect("Failed to read configuration.");
    let application = Application::build(configuration)
        .await
        .expect("Failed to build application");
    application.run().await?;
    Ok(())
}
