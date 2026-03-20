use anyhow::{Context, Result};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::fs;
use serde_json::Value;

/// Konfiguracja mechanizmów dla ESPN wzorowana na bibliotece soccerdata
pub struct EspnConfig {
    pub leagues: Vec<String>,
    pub seasons: Vec<String>,
    pub no_cache: bool,
    pub no_store: bool,
    pub data_dir: PathBuf,
}

impl Default for EspnConfig {
    fn default() -> Self {
        Self {
            leagues: Vec::new(),
            seasons: Vec::new(),
            no_cache: false,
            no_store: false,
            data_dir: PathBuf::from("data/ESPN"),
        }
    }
}

pub struct EspnExtractor {
    client: Client,
    base_url: String,
    pub config: EspnConfig,
}

impl EspnExtractor {
    pub fn new(config: Option<EspnConfig>) -> Self {
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            // Bazowy URL dla ESPN API m.in. z informacjami o meczach piłki nożnej
            base_url: "http://site.api.espn.com/apis/site/v2/sports/soccer".to_string(),
            config: config.unwrap_or_default(),
        }
    }

    /// Pobieranie statystyk ligowych dla podanych `leagues` z parametrem wymuszenia cache
    pub async fn read_schedule(&self, force_cache: bool) -> Result<Vec<Value>> {
        let mut results = Vec::new();
        
        let no_cache_override = if force_cache { true } else { self.config.no_cache };
        
        for league in &self.config.leagues {
            let mapped_league = if league == "EPL" { "eng.1" } else { league.as_str() };
            let url = format!("{}/{}/scoreboard", self.base_url, mapped_league);
            let cache_file = self.config.data_dir.join(format!("schedule_{}.json", league));
            
            let json_value = self.fetch_json_with_cache(&url, &cache_file, no_cache_override).await?;
            results.push(json_value);
        }
        
        Ok(results)
    }

    /// Pobieranie arkusza wydarzeń na żądanie na podstawie ID
    pub async fn read_matchsheet(&self, match_id: &str) -> Result<Value> {
        let url = format!("{}/all/summary?event={}", self.base_url, match_id);
        let cache_file = self.config.data_dir.join(format!("matchsheet_{}.json", match_id));
        
        self.fetch_json_with_cache(&url, &cache_file, self.config.no_cache).await
    }

    /// Pobieranie listy rezerwowych i podstawowej XI 
    pub async fn read_lineup(&self, match_id: &str) -> Result<Value> {
        // Line-upy meczowe ESPN wędrują zazwyczaj w payloadzie endpointa "summary"
        // Odwołujemy się po zapisane wartości:
        self.read_matchsheet(match_id).await
    }

    /// Bezpieczny loader plików do struktury serde_json z cache'owaniem
    async fn fetch_json_with_cache(&self, url: &str, cache_path: &Path, no_cache: bool) -> Result<Value> {
        let json_text = if !no_cache && cache_path.exists() {
            tracing::info!("Wczytywanie z lokalnego Cache ESPN: {:?}", cache_path);
            fs::read_to_string(cache_path).context("Błąd czytania Cache ESPN")?
        } else {
            tracing::info!("Pobieranie JSON z API ESPN: {}", url);
            let response = self.client.get(url).send().await.context("HTTP error z API ESPN")?;
            let text = response.text().await.context("Puste API ESPN")?;
            
            if !self.config.no_store {
                if let Some(parent) = cache_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(cache_path, &text)?;
                tracing::info!("Zapisano JSON do Cache: {:?}", cache_path);
            }
            text
        };
        
        let val: Value = serde_json::from_str(&json_text).context("Błąd parsowania JSON'a z ESPN")?;
        Ok(val)
    }
}
