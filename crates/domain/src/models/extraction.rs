use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct RawExtraction {
    pub id: i64,
    pub source: String,
    pub league: String,
    pub season: String,
    pub url: String,
    pub payload_html_json: Option<String>,
    pub fetched_at: String, // String na Datetime w SQLite
}
