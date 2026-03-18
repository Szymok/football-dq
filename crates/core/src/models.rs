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
