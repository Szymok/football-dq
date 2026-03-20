use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use anyhow::{Result, Context};
use crate::models::Match;

pub struct DbStore {
    pool: SqlitePool,
}

impl DbStore {
    pub async fn new(db_url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(db_url)
            .await
            .context("Failed to connect to SQLite database")?;
        
        Ok(Self { pool })
    }

    pub async fn init_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS matches (
                id TEXT PRIMARY KEY,
                date TEXT NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                home_score INTEGER NOT NULL,
                away_score INTEGER NOT NULL,
                source TEXT NOT NULL
            );
            "#
        )
        .execute(&self.pool)
        .await
        .context("Failed to initialize database schema")?;
        Ok(())
    }

    pub async fn insert_match(&self, m: &Match) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO matches (id, date, home_team, away_team, home_score, away_score, source)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(id) DO UPDATE SET
                home_score=excluded.home_score,
                away_score=excluded.away_score
            "#
        )
        .bind(&m.id)
        .bind(&m.date)
        .bind(&m.home_team)
        .bind(&m.away_team)
        .bind(m.home_score)
        .bind(m.away_score)
        .bind(&m.source)
        .execute(&self.pool)
        .await
        .context("Failed to insert match")?;
        
        Ok(())
    }
}
