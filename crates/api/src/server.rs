use axum::{
    routing::{get, post},
    Router,
};
use std::net::SocketAddr;

use crate::controllers::extract::{get_status, trigger_sync};

pub async fn start_server(port: u16) -> anyhow::Result<()> {
    // Rejestracja endpointów i routerów Axios / REST API Admin Panel
    let app = Router::new()
        // Zwraca dla tabelki Reactowej status synchronizacji aktualnych lig 
        .route("/api/sync/status", get(get_status))
        // Odbiera zapytanie odciągnięcia danych manualnie ze źródeł (Przycisk "Sync All" albo customowe)
        .route("/api/sync/trigger", post(trigger_sync));

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    tracing::info!("Magistrala Axum / Panel Admina pomyślnie uruchomiona na porcie: {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
