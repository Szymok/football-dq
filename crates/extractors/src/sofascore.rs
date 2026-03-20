use anyhow::{Context, Result};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::fs;
use serde_json::Value;

/// Konfig dla ekstraktora Sofascore
pub struct SofascoreConfig {
    pub leagues: Vec<String>,
    pub seasons: Vec<String>,
    pub no_cache: bool,
    pub no_store: bool,
    pub data_dir: PathBuf,
}

impl Default for SofascoreConfig {
    fn default() -> Self {
        Self {
            leagues: Vec::new(),
            seasons: Vec::new(),
            no_cache: false,
            no_store: false,
            data_dir: PathBuf::from("data/Sofascore"),
        }
    }
}

pub struct SofascoreExtractor {
    client: Client,
    base_url: String,
    pub config: SofascoreConfig,
}

impl SofascoreExtractor {
    pub fn new(config: Option<SofascoreConfig>) -> Self {
        Self {
            client: Client::new(),
            base_url: "https://api.sofascore.com/api/v1".to_string(), // Główne API
            config: config.unwrap_or_default(),
        }
    }

    /// Pobieranie definicji i ID interesujących nas turniejów i lig
    pub async fn read_leagues(&self) -> Result<Value> {
        let url = format!("{}/config/unique-tournaments/EN", self.base_url);
        let cache_file = self.config.data_dir.join("leagues.json");
        self.fetch_json_with_cache(&url, &cache_file, self.config.no_cache).await
    }

    /// Wysyłanie zapytań o dostępne sezony do poszczególnych lig z konfigu
    pub async fn read_seasons(&self) -> Result<Vec<Value>> {
        let mut results = Vec::new();
        for league in &self.config.leagues {
            let url = format!("{}/unique-tournament/{}/seasons", self.base_url, league);
            let cache_file = self.config.data_dir.join(format!("seasons_{}.json", league));
            
            let data = self.fetch_json_with_cache(&url, &cache_file, self.config.no_cache).await?;
            results.push(data);
        }
        Ok(results)
    }

    /// Odczyt tabel poszczególnych lig (na dany sezon)
    pub async fn read_league_table(&self, force_cache: bool) -> Result<Vec<Value>> {
        let mut results = Vec::new();
        let no_cache_override = if force_cache { false } else { self.config.no_cache };
        
        for season in &self.config.seasons {
            for league in &self.config.leagues {
                let url = format!("{}/unique-tournament/{}/season/{}/standings/total", self.base_url, league, season);
                let cache_file = self.config.data_dir.join(format!("table_{}_{}.json", season, league));
                
                let data = self.fetch_json_with_cache(&url, &cache_file, no_cache_override).await?;
                results.push(data);
            }
        }
        Ok(results)
    }

    /// Odczyt terminarzy / wydarzeń (składów, statystyk) na ligę w sezonie
    pub async fn read_schedule(&self, force_cache: bool) -> Result<Vec<Value>> {
        let mut results = Vec::new();
        let no_cache_override = if force_cache { false } else { self.config.no_cache };
        
        for season in &self.config.seasons {
            for league in &self.config.leagues {
                let url = format!("{}/unique-tournament/{}/season/{}/events", self.base_url, league, season);
                let cache_file = self.config.data_dir.join(format!("schedule_{}_{}.json", season, league));
                
                let data = self.fetch_json_with_cache(&url, &cache_file, no_cache_override).await?;
                results.push(data);
            }
        }
        Ok(results)
    }

    // Bezpieczny fetcher JSON dla API Sofascore uwzględniający politykę cache'owania z dyskiem
    async fn fetch_json_with_cache(&self, url: &str, cache_path: &Path, no_cache: bool) -> Result<Value> {
        let json_text = if !no_cache && cache_path.exists() {
            tracing::info!("Wczytywanie Sofascore z lokalnego Cache: {:?}", cache_path);
            fs::read_to_string(cache_path).context("Błąd czytania Cache Sofascore")?
        } else {
            tracing::info!("Pobieranie HTTP z Sofascore: {}", url);
            let response = self.client.get(url)
                // Sofascore wymaga często ustawienia User-Agent, aby odbijać ataki skryptów
                .header("User-Agent", "Mozilla/5.0")
                .send()
                .await.context("HTTP error z API Sofascore")?;
                
            let text = response.text().await.context("Brak treści JSON w API Sofascore")?;
            
            if !self.config.no_store {
                if let Some(parent) = cache_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(cache_path, &text)?;
                tracing::info!("Zapisano JSON Sofascore do Cache: {:?}", cache_path);
            }
            text
        };
        
        let val: Value = serde_json::from_str(&json_text).context("Błąd parsowania JSON'a z Sofascore")?;
        Ok(val)
    }
}
