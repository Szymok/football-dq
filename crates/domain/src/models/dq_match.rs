use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct MatchDQ {
    pub id: i64,
    pub match_id_fbref: Option<String>,
    pub match_id_understat: Option<String>,
    pub home_team: String,
    pub away_team: String,
    pub date: String,
    pub home_xg_fbref: f64,
    pub home_xg_understat: f64,
    pub away_xg_fbref: f64,
    pub away_xg_understat: f64,
    pub flagged_discrepancy: bool,
}
