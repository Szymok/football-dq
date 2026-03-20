use serde::{Deserialize, Serialize};

/// Mecz powiązany między wieloma źródłami
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LinkedMatch {
    pub id: Option<i64>,
    pub date: String,
    pub home_team_canonical: String,
    pub away_team_canonical: String,
    pub home_goals: Option<u8>,
    pub away_goals: Option<u8>,
    /// JSON z danymi per źródło: {"espn": {"home_goals": 2, ...}, "understat": {...}}
    pub sources_json: String,
    pub score_agreement: bool,
    pub xg_discrepancy: Option<f64>,
}

/// Dane z jednego źródła dla danego meczu
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SourceMatchData {
    pub source: String,
    pub home_team: String,
    pub away_team: String,
    pub home_goals: Option<u8>,
    pub away_goals: Option<u8>,
    pub home_xg: Option<f64>,
    pub away_xg: Option<f64>,
}
