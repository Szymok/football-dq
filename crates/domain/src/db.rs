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

    // Tabela: Linked Matches — mecze powiązane między wieloma źródłami
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS linked_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            home_team_canonical TEXT NOT NULL,
            away_team_canonical TEXT NOT NULL,
            home_goals INTEGER,
            away_goals INTEGER,
            sources_json TEXT NOT NULL,
            source_count INTEGER NOT NULL DEFAULT 1,
            score_agreement BOOLEAN NOT NULL DEFAULT 1,
            xg_discrepancy REAL,
            linked_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );"
    ).execute(&pool).await?;

    // Tabela: Statystyki z poszczególnych źródeł per mecz
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS match_source_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            linked_match_id INTEGER NOT NULL REFERENCES linked_matches(id),
            source TEXT NOT NULL,
            home_goals INTEGER, away_goals INTEGER,
            ht_home_goals INTEGER, ht_away_goals INTEGER,
            home_xg REAL, away_xg REAL,
            home_npxg REAL, away_npxg REAL,
            home_shots INTEGER, away_shots INTEGER,
            home_shots_target INTEGER, away_shots_target INTEGER,
            home_corners INTEGER, away_corners INTEGER,
            home_fouls INTEGER, away_fouls INTEGER,
            home_yellow INTEGER, away_yellow INTEGER,
            home_red INTEGER, away_red INTEGER,
            home_ppda REAL, away_ppda REAL,
            home_deep INTEGER, away_deep INTEGER,
            referee TEXT
        );"
    ).execute(&pool).await?;

    tracing::info!("✅ SQLite połączony i tabele schematów (raw_extractions, matches_dq, linked_matches, match_source_stats) utworzone.");

    Ok(pool)
}
