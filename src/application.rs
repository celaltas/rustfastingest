use std::net::TcpListener;

use crate::{
    config::config::GeneralConfig,
    db::syclla::ScyllaService,
    routes::{health_check::health_check, ingest::ingest},
};
use actix_web::{dev::Server, web, App, HttpServer};
use eyre::Result;
use tokio::sync::Semaphore;
use tracing::info;
use tracing_actix_web::TracingLogger;

pub struct Application {
    server: Server,
    port: u16,
}

#[derive(Debug)]
pub struct AppState {
    pub db: ScyllaService,
    pub semaphore: Semaphore,
}

impl AppState {
    pub fn new(db: ScyllaService, semaphore: Semaphore) -> Self {
        Self { db, semaphore }
    }
}

impl Application {
    pub async fn build(config: GeneralConfig) -> Result<Application> {
        let address = format!("{}:{}", config.app.host, config.app.port);
        info!("Listening on {}", address);
        let listener = TcpListener::bind(address)?;
        let port = listener.local_addr()?.port();
        let db = ScyllaService::init(&config.db).await?;
        let semaphore = Semaphore::new(config.app.parallel_files);
        let app_state = AppState::new(db, semaphore);
        let server = Self::create_server(listener, app_state)?;
        Ok(Application {
            server: server,
            port: port,
        })
    }

    pub async fn run(self) -> Result<()> {
        let _ = self.server.await;
        Ok(())
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    fn create_server(listener: TcpListener, state: AppState) -> Result<Server> {
        let state = web::Data::new(state);
        let server = HttpServer::new(move || {
            App::new()
                .wrap(TracingLogger::default())
                .service(health_check)
                .service(ingest)
                .app_data(state.clone())
        })
        .listen(listener)?
        .run();
        Ok(server)
    }
}
