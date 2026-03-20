use axum::{
    Json,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct SourceStatus {
    pub source: String,
    pub status: String,
}

/// Endpoint GET dla odczytu stanu pobranych lig na stronie front-endu
pub async fn get_status() -> impl IntoResponse {
    let mock_status = vec![
        SourceStatus { source: "FBref".to_string(), status: "✅ Cache Pobrany".to_string() },
        SourceStatus { source: "Understat".to_string(), status: "⏳ Oczekuje".to_string() },
        SourceStatus { source: "ESPN".to_string(), status: "✅ Cache Pobrany".to_string() },
        SourceStatus { source: "WhoScored".to_string(), status: "❌ Błąd Selenium".to_string() },
    ];
    Json(mock_status)
}

#[derive(Deserialize)]
pub struct SyncRequest {
    pub source: String,
    pub league: String,
    pub season: String,
    pub force_cache: bool,
}

#[derive(Serialize)]
pub struct SyncResponse {
    pub message: String,
}

/// Endpoint POST dla pociągnięcia za symulowany guzik Admin Panelu (wyzwala logike Rusta CLI)
pub async fn trigger_sync(Json(payload): Json<SyncRequest>) -> impl IntoResponse {
    tracing::info!(
        "Otrzymano żądanie POST z Frontendu (Admin Panel): Pobierz {} [LIGA: {} | SEZON: {}]",
        payload.source, payload.league, payload.season
    );
    
    // To wywoła prawdziwy ekstraktor Rusta przekazany przez Web Interface
    let msg = format!(
        "Zlecono zadanie background synchronizacji asynchronicznej dla agregatora: {}",
        payload.source
    );
    
    Json(SyncResponse { message: msg })
}
