use crate::Extractor;
use anyhow::{Result, Context};
use domain::models::Match;
use reqwest::Client;

pub struct OfficialApiExtractor {
    client: Client,
    base_url: String,
}

impl OfficialApiExtractor {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.to_string(),
        }
    }
}

impl Extractor for OfficialApiExtractor {
    fn source_name(&self) -> &str {
        "Official"
    }

    async fn fetch_matches(&self, league: &str, season: &str) -> Result<Vec<Match>> {
        println!("OfficialApiExtractor: Pobieram z API zdefiniowanego pod {} dla ligi: {} ({})", self.base_url, league, season);
        
        Ok(vec![
            Match {
                id: format!("official_{}_{}_1", league, season),
                date: "2024-01-01T15:00:00Z".to_string(),
                home_team: "Arsenal FC".to_string(), // Inna konwencja nazewnictwa
                away_team: "Chelsea FC".to_string(),
                home_score: 2,
                away_score: 1,
                source: self.source_name().to_string(),
            }
        ])
    }
}
