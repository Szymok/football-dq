use crate::Extractor;
use anyhow::{Result, Context};
use domain::models::Match;
use reqwest::Client;

pub struct FbrefExtractor {
    client: Client,
}

impl FbrefExtractor {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }
}

impl Extractor for FbrefExtractor {
    fn source_name(&self) -> &str {
        "FBref"
    }

    async fn fetch_matches(&self, league: &str, season: &str) -> Result<Vec<Match>> {
        // TODO: Impl HTML scraping with `scraper` crate or API call. Here we return a mock.
        println!("FBrefExtractor: Pobieram mecze z {} dla ligi: {}, sezon: {}", self.source_name(), league, season);
        
        Ok(vec![
            Match {
                id: format!("fbref_{}_{}_1", league, season),
                date: "2024-01-01T15:00:00Z".to_string(),
                home_team: "Arsenal".to_string(),
                away_team: "Chelsea".to_string(),
                home_score: 2,
                away_score: 1,
                source: self.source_name().to_string(),
            }
        ])
    }
}
