use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::path::Path;

pub async fn init_db(db_url: &str) -> anyhow::Result<SqlitePool> {
    // Auto-tworzenie pliku dla SQlite MVP pod komendę `cargo run`
    let path_str = db_url.trim_start_matches("sqlite:");
    if !Path::new(path_str).exists() {
        std::fs::File::create(path_str)?;
    }

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(db_url)
        .await?;

    // Tabela: Surowe zrzuty Cache dla Ekstraktorów HTTP & Selenium
    // Zapobiega ponownemu odpytywaniu WhoScored lub FBref
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS raw_extractions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            league TEXT NOT NULL,
            season TEXT NOT NULL,
            url TEXT NOT NULL,
            payload_html_json TEXT,
            fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );"
    ).execute(&pool).await?;

    // Tabela: Data Quality Engine (Core)
    // Agregująca porównania logiki `MatchLinker` dla Frontendu Rusta!
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS matches_dq (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id_fbref TEXT,
            match_id_understat TEXT,
            home_team TEXT,
            away_team TEXT,
            date TEXT,
            home_xg_fbref REAL,
            home_xg_understat REAL,
            away_xg_fbref REAL,
            away_xg_understat REAL,
            flagged_discrepancy BOOLEAN
        );"
    ).execute(&pool).await?;

    tracing::info!("✅ SQLite połączony i tabele schematów (raw_extractions, matches_dq) utworzone.");

    Ok(pool)
}
