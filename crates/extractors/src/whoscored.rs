use anyhow::{Context, Result};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::fs;

/// Konfig dla ekstraktora WhoScored, który replikuje flagi sterujące przeglądarkami z soccerdata
pub struct WhoscoredConfig {
    pub leagues: Vec<String>,
    pub seasons: Vec<String>,
    pub no_cache: bool,
    pub no_store: bool,
    pub data_dir: PathBuf,
    // Flagi specyficzne dla WhoScored chroniącego się potężnie przed ruchem botów (wymaga Selenium / Headless Chrome)
    pub path_to_browser: Option<PathBuf>,
    pub headless: bool,
}

impl Default for WhoscoredConfig {
    fn default() -> Self {
        Self {
            leagues: Vec::new(),
            seasons: Vec::new(),
            no_cache: false,
            no_store: false,
            data_dir: PathBuf::from("data/WhoScored"),
            path_to_browser: None,
            headless: true,
        }
    }
}

pub struct WhoscoredExtractor {
    client: Client,
    base_url: String,
    pub config: WhoscoredConfig,
}

impl WhoscoredExtractor {
    pub fn new(config: Option<WhoscoredConfig>) -> Self {
        Self {
            client: Client::new(),
            base_url: "https://www.whoscored.com".to_string(),
            config: config.unwrap_or_default(),
        }
    }

    /// Pobieranie kalendarza meczowego
    pub async fn read_schedule(&self, force_cache: bool) -> Result<Vec<String>> {
        let mut results = Vec::new();
        let no_cache_override = if force_cache { false } else { self.config.no_cache };
        
        for season in &self.config.seasons {
            for league in &self.config.leagues {
                // Generowanie przybliżonych ścieżek WhoScored (zabezpieczenia URLi wymagają precyzyjnych map, upraszczamy na MVP)
                let url = format!("{}/Regions/252/Tournaments/{}/Seasons/{}", self.base_url, league, season);
                let cache_file = self.config.data_dir.join(format!("schedule_{}_{}.html", season, league));
                
                let data = self.fetch_with_cache_override(&url, &cache_file, no_cache_override).await.unwrap_or_default();
                results.push(data);
            }
        }
        Ok(results)
    }

    /// Pobieranie kontuzji i wykluczeń przedmeczowych 
    pub async fn read_missing_players(&self, match_id: Option<&str>, force_cache: bool) -> Result<String> {
        let id_str = match_id.unwrap_or("all");
        let url = format!("{}/Matches/{}/Preview", self.base_url, id_str);
        let cache_file = self.config.data_dir.join(format!("missing_players_{}.html", id_str));
        
        let no_cache_override = if force_cache { false } else { self.config.no_cache };
        self.fetch_with_cache_override(&url, &cache_file, no_cache_override).await
    }

    /// Analiza szczegółowych Eventów zebranych dla meczu (strzały, heatmaps, passing networks z WhoScored)
    pub async fn read_events(
        &self, 
        match_id: Option<&str>, 
        force_cache: bool, 
        _live: bool, 
        output_fmt: &str, 
        _retry_missing: bool,
        _on_error: &str
    ) -> Result<String> {
        let id_str = match_id.unwrap_or("all");
        let url = format!("{}/Matches/{}/Live", self.base_url, id_str);
        
        let mut cache_file = self.config.data_dir.join(format!("events_{}.html", id_str));
        
        // Zgodność z opcjami formatowania WhoScored dla docstringa
        if output_fmt == "raw" {
            cache_file = self.config.data_dir.join(format!("events_{}.json", id_str));
        } else if output_fmt == "spadl" {
            cache_file = self.config.data_dir.join(format!("events_spadl_{}.json", id_str));
        }

        let no_cache_override = if force_cache { false } else { self.config.no_cache };
        self.fetch_with_cache_override(&url, &cache_file, no_cache_override).await
    }

    /// Moduł pobierania (docelowo ten blok będzie wywoływał Headless Chrome API w Rust, jeżeli jest aktywowane przez config.headless)
    async fn fetch_with_cache_override(&self, url: &str, cache_path: &Path, no_cache: bool) -> Result<String> {
        if !no_cache && cache_path.exists() {
            tracing::info!("Wczytywanie WhoScored z dysku: {:?}", cache_path);
            fs::read_to_string(cache_path).context("Błąd czytania Cache WhoScored z dysku")
        } else {
            tracing::info!("Ostrzeżenie: System WhoScored wymaga potężnych silników Headless z powodu ochrony Incapsula.");
            tracing::info!("Wykonuję próbny Request HTTP na: {}", url);
            let response = self.client.get(url)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                .send()
                .await.context("HTTP z sieci: Błąd dla domeny WhoScored (prawdopodobny ban IP)")?;
                
            let text = response.text().await.context("Pusta zawartość WhoScored API")?;
            
            if !self.config.no_store {
                if let Some(parent) = cache_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&cache_path, &text)?;
                tracing::info!("Zapisano kopię danych ujęcia z WhoScored: {:?}", cache_path);
            }
            Ok(text)
        }
    }
}
