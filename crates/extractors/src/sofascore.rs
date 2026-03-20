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
        use reqwest::header;
        let mut headers = header::HeaderMap::new();
        headers.insert(header::USER_AGENT, header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"));
        headers.insert(header::ACCEPT, header::HeaderValue::from_static("application/json, text/plain, */*"));
        headers.insert(header::REFERER, header::HeaderValue::from_static("https://www.sofascore.com/"));
        headers.insert("Origin", header::HeaderValue::from_static("https://www.sofascore.com"));
        headers.insert("Cache-Control", header::HeaderValue::from_static("no-cache"));

        let client = Client::builder()
            .default_headers(headers)
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
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
        let no_cache_override = if force_cache { true } else { self.config.no_cache };
        
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
        let no_cache_override = if force_cache { true } else { self.config.no_cache };
        
        for season in &self.config.seasons {
            for league in &self.config.leagues {
                let mapped_league = if league == "EPL" { "17" } else { league.as_str() };
                let mapped_season = if season == "2526" { "61627" } else if season == "2425" { "52186" } else { season.as_str() };
                let url = format!("{}/unique-tournament/{}/season/{}/events/last/0", self.base_url, mapped_league, mapped_season);
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
            tracing::info!("Uruchamianie robota Headless Chrome dla API Sofascore: {}", url);
            let url_clone = url.to_string();
            let text = tokio::task::spawn_blocking(move || -> Result<String> {
                let options = headless_chrome::LaunchOptions {
                    headless: false,
                    args: vec![
                        std::ffi::OsStr::new("--disable-blink-features=AutomationControlled"),
                    ],
                    ..Default::default()
                };
                let browser = headless_chrome::Browser::new(options).map_err(|e| anyhow::anyhow!("Browser err: {:?}", e))?;
                let tab = browser.new_tab().map_err(|e| anyhow::anyhow!("Tab err: {:?}", e))?;
                tab.navigate_to(&url_clone).map_err(|e| anyhow::anyhow!("Nav err: {:?}", e))?;
                
                std::thread::sleep(std::time::Duration::from_secs(4));
                
                let result = tab.evaluate("document.body.innerText", false)
                    .map_err(|e| anyhow::anyhow!("JS err: {:?}", e))?;
                
                let json_str = result.value.and_then(|v| v.as_str().map(|s| s.to_string()))
                    .unwrap_or_else(|| "{}".to_string());
                
                Ok(json_str)
            }).await??;
            
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
