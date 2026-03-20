use axum::{routing::get, Router, extract::State};
use domain::db::DbStore;
use std::net::SocketAddr;
use std::sync::Arc;

struct AppState {
    db: Arc<DbStore>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    tracing::info!("Initializing SQLite database connection...");
    // Connect to SQLite database used in python PoC
    let db = DbStore::new("sqlite://football_dq.db").await?;
    db.init_schema().await?;
    
    let state = Arc::new(AppState {
        db: Arc::new(db),
    });

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/dq/reports", get(dq_reports))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("Server listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
    
    Ok(())
}

async fn health_check() -> &'static str {
    "OK"
}

// Endpoint serwujący wyliczone metryki dla frontendu w React
async fn dq_reports(State(_state): State<Arc<AppState>>) -> axum::Json<Vec<domain::models::DataQualityMetric>> {
    // TODO: Fetch existing metrics from DB
    axum::Json(vec![])
}
