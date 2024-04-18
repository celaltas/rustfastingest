use std::net::TcpListener;

use crate::{config::config::GeneralConfig, db::syclla::ScyllaService};
use actix_web::{dev::Server, get, web, App, HttpResponse, HttpServer, Responder};
use eyre::Result;
use tracing::info;
use tracing_actix_web::TracingLogger;

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

pub struct Application {
    server: Server,
}

impl Application {
    pub async fn build(config: GeneralConfig) -> Result<Application> {
        let address = format!("{}:{}", config.app.host, config.app.port);
        info!("Listening on {}", address);
        let listener = TcpListener::bind(address)?;
        let db = ScyllaService::init(&config.db).await?;
        let server = Self::create_server(listener, db)?;
        Ok(Application { server: server })
    }

    pub async fn run(self) -> Result<()> {
        let _ = self.server.await;
        Ok(())
    }

    fn create_server(listener: TcpListener, db: ScyllaService) -> Result<Server> {
        let db = web::Data::new(db);
        let server = HttpServer::new(move || {
            App::new()
                .wrap(TracingLogger::default())
                .service(hello)
                .app_data(db.clone())
        })
        .listen(listener)?
        .run();
        Ok(server)
    }
}
