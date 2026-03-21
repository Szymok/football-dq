use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    // Uruchom serwer API (domyślnie port 8080)
    api::server::start_server(8080).await
}
