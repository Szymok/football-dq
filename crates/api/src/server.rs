use axum::{
    routing::{get, post},
    Router,
    extract::State,
    Json,
};
use serde::Serialize;
use sqlx::SqlitePool;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{CorsLayer, Any};

use crate::controllers::extract::{get_status, trigger_sync};

pub struct ApiState {
    pub pool: SqlitePool,
}

pub async fn start_server(port: u16) -> anyhow::Result<()> {
    // Inicjalizacja SQLite — ten sam plik co CLI
    let pool = domain::db::init_db("sqlite:football-dq.db").await?;

    let state = Arc::new(ApiState { pool });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/api/sync/status", get(get_status))
        .route("/api/sync/trigger", post(trigger_sync))
        .route("/api/linked-matches", get(get_linked_matches))
        .route("/api/matches/:id", axum::routing::get(get_match_stats))
        .route("/api/dq/summary", get(get_dq_summary))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    tracing::info!("Magistrala Axum / Panel Admina pomyślnie uruchomiona na porcie: {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// ─── Linked Matches ──────────────────────────────────────────────────────────

#[derive(Debug, Serialize, sqlx::FromRow)]
struct LinkedMatchRow {
    id: i64,
    date: String,
    home_team_canonical: String,
    away_team_canonical: String,
    home_goals: Option<i32>,
    away_goals: Option<i32>,
    sources_json: String,
    source_count: i32,
    score_agreement: bool,
    xg_discrepancy: Option<f64>,
}

async fn get_linked_matches(
    State(state): State<Arc<ApiState>>,
) -> Json<Vec<LinkedMatchRow>> {
    let rows = sqlx::query_as::<_, LinkedMatchRow>(
        "SELECT id, date, home_team_canonical, away_team_canonical, home_goals, away_goals, sources_json, source_count, score_agreement, xg_discrepancy FROM linked_matches ORDER BY date DESC LIMIT 500"
    )
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    Json(rows)
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct MatchSourceStatRow {
    id: i64,
    linked_match_id: i64,
    source: String,
    home_goals: Option<i32>, away_goals: Option<i32>,
    ht_home_goals: Option<i32>, ht_away_goals: Option<i32>,
    home_xg: Option<f64>, away_xg: Option<f64>,
    home_npxg: Option<f64>, away_npxg: Option<f64>,
    home_shots: Option<i32>, away_shots: Option<i32>,
    home_shots_target: Option<i32>, away_shots_target: Option<i32>,
    home_corners: Option<i32>, away_corners: Option<i32>,
    home_fouls: Option<i32>, away_fouls: Option<i32>,
    home_yellow: Option<i32>, away_yellow: Option<i32>,
    home_red: Option<i32>, away_red: Option<i32>,
    home_ppda: Option<f64>, away_ppda: Option<f64>,
    home_deep: Option<i32>, away_deep: Option<i32>,
    referee: Option<String>,
}

async fn get_match_stats(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Json<Vec<MatchSourceStatRow>> {
    let rows = sqlx::query_as::<_, MatchSourceStatRow>(
        "SELECT * FROM match_source_stats WHERE linked_match_id = ?"
    )
    .bind(id)
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    Json(rows)
}

// ─── DQ Summary ──────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct DqSummary {
    total_matches: i64,
    multi_source_matches: i64,
    score_agreement_pct: f64,
    avg_xg_discrepancy: Option<f64>,
    sources: Vec<String>,
}

async fn get_dq_summary(
    State(state): State<Arc<ApiState>>,
) -> Json<DqSummary> {
    let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM linked_matches")
        .fetch_one(&state.pool)
        .await
        .unwrap_or((0,));

    let agreed: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM linked_matches WHERE score_agreement = 1")
        .fetch_one(&state.pool)
        .await
        .unwrap_or((0,));

    let avg_xg: (Option<f64>,) = sqlx::query_as("SELECT AVG(xg_discrepancy) FROM linked_matches WHERE xg_discrepancy IS NOT NULL")
        .fetch_one(&state.pool)
        .await
        .unwrap_or((None,));

    // Policz mecze z >1 źródłem (sources_json zawiera więcej niż 1 element)
    let multi: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM linked_matches WHERE sources_json LIKE '%},{%'"
    )
    .fetch_one(&state.pool)
    .await
    .unwrap_or((0,));

    let agreement_pct = if total.0 > 0 {
        (agreed.0 as f64 / total.0 as f64) * 100.0
    } else {
        0.0
    };

    Json(DqSummary {
        total_matches: total.0,
        multi_source_matches: multi.0,
        score_agreement_pct: agreement_pct,
        avg_xg_discrepancy: avg_xg.0,
        sources: vec!["espn".into(), "sofascore".into(), "understat".into(), "matchhistory".into()],
    })
}
