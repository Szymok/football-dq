use anyhow::{Context, Result};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::fs;
use std::collections::HashMap;
use csv::ReaderBuilder;

/// Konfig dla ekstraktora MatchHistory API: football-data.co.uk
pub struct MatchHistoryConfig {
    pub leagues: Vec<String>,
    pub seasons: Vec<String>,
    pub no_cache: bool,
    pub no_store: bool,
    pub data_dir: PathBuf,
}

impl Default for MatchHistoryConfig {
    fn default() -> Self {
        Self {
            leagues: Vec::new(),
            seasons: Vec::new(),
            no_cache: false,
            no_store: false,
            data_dir: PathBuf::from("data/MatchHistory"),
        }
    }
}

pub struct MatchHistoryExtractor {
    client: Client,
    base_url: String,
    pub config: MatchHistoryConfig,
}

impl MatchHistoryExtractor {
    pub fn new(config: Option<MatchHistoryConfig>) -> Self {
        Self {
            client: Client::new(),
            // historyczne ścieżki football-data.co.uk pod prefixem bazowym
            base_url: "https://www.football-data.co.uk/mmz4281".to_string(),
            config: config.unwrap_or_default(),
        }
    }

    /// Pobieranie statystyk meczowych z podanych `leagues` i `seasons` używające Cache
    pub async fn read_games(&self) -> Result<Vec<HashMap<String, String>>> {
        let mut results = Vec::new();
        
        for season in &self.config.seasons {
            for league in &self.config.leagues {
                // Składnia football-data np. 2122/E0.csv dla Premier League 2021-22
                let url = format!("{}/{}/{}.csv", self.base_url, season, league);
                let cache_file = self.config.data_dir.join(format!("{}_{}.csv", season, league));
                
                let csv_data = self.fetch_csv_with_cache(&url, &cache_file).await.unwrap_or_default();
                results.extend(csv_data);
            }
        }
        
        Ok(results)
    }

    /// Implementacja pobierania pliku CSV na wzór socerdata z plikami w ~/soccerdata/data/MatchHistory
    async fn fetch_csv_with_cache(&self, url: &str, cache_path: &Path) -> Result<Vec<HashMap<String, String>>> {
        let csv_text = if !self.config.no_cache && cache_path.exists() {
            tracing::info!("Wczytywanie MatchHistory z Cache: {:?}", cache_path);
            fs::read_to_string(cache_path).context("Błąd odczytu Cache MatchHistory")?
        } else {
            tracing::info!("Pobieranie CSV z: {}", url);
            let response = self.client.get(url).send().await.context("HTTP error z football-data.co.uk")?;
            let text = response.text().await.context("Pusta zawartość CSV z API")?;
            
            if !self.config.no_store {
                if let Some(parent) = cache_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(cache_path, &text)?;
                tracing::info!("Zapisano CSV do Cache: {:?}", cache_path);
            }
            text
        };
        
        // Z uwagi na nieznane kolumny dla każdego roku ładujemy całość dynamicznie do map
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(csv_text.as_bytes());
            
        let mut rows = Vec::new();
        
        for result in rdr.deserialize() {
            let record: HashMap<String, String> = result.unwrap_or_default();
            if !record.is_empty() {
                rows.push(record);
            }
        }
        
        Ok(rows)
    }
}
