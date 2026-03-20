use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Match {
    pub id: String,
    pub date: String,
    pub home_team: String,
    pub away_team: String,
    pub home_score: u8,
    pub away_score: u8,
    pub source: String, // e.g., "FBref", "Official"
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataQualityMetric {
    pub id: String,
    pub dimension: String, // e.g. Completeness, Consistency
    pub score: f64,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClubEloRow {
    #[serde(rename = "Rank")]
    pub rank: Option<u32>,
    #[serde(rename = "Club")]
    pub club: String,
    #[serde(rename = "Country")]
    pub country: String,
    #[serde(rename = "Level")]
    pub level: Option<u32>,
    #[serde(rename = "Elo")]
    pub elo: f64,
    #[serde(rename = "From")]
    pub from_date: String,
    #[serde(rename = "To")]
    pub to_date: String,
}
