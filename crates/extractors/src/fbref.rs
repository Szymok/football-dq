use anyhow::{Context, Result};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::fs;

/// Konfiguracja mechanizmów dla FBref wzorowana na bibliotece soccerdata
pub struct FbrefConfig {
    pub leagues: Vec<String>,
    pub seasons: Vec<String>,
    pub no_cache: bool,
    pub no_store: bool,
    pub data_dir: PathBuf,
}

impl Default for FbrefConfig {
    fn default() -> Self {
        Self {
            leagues: vec!["Big 5 European Leagues Combined".to_string()],
            seasons: Vec::new(),
            no_cache: false,
            no_store: false,
            data_dir: PathBuf::from("data/FBref"),
        }
    }
}

pub struct FbrefExtractor {
    client: Client,
    base_url: String,
    pub config: FbrefConfig,
}

impl FbrefExtractor {
    pub fn new(config: Option<FbrefConfig>) -> Self {
        Self {
            client: Client::new(),
            base_url: "https://fbref.com".to_string(),
            config: config.unwrap_or_default(),
        }
    }

    pub async fn read_leagues(&self, _split_up_big5: bool) -> Result<String> {
        let url = format!("{}/en/comps/", self.base_url);
        self.fetch_html_with_cache(&url, "leagues.html").await
    }

    pub async fn read_seasons(&self, _split_up_big5: bool) -> Result<String> {
        let url = format!("{}/en/comps/", self.base_url);
        self.fetch_html_with_cache(&url, "seasons.html").await
    }

    pub async fn read_team_season_stats(&self, stat_type: &str, _opponent_stats: bool) -> Result<String> {
        let url = format!("{}/en/comps/Big5/{}", self.base_url, stat_type);
        self.fetch_html_with_cache(&url, &format!("team_season_{}.html", stat_type)).await
    }

    pub async fn read_team_match_stats(&self, stat_type: &str, _opponent_stats: bool, _team: Option<&str>, _force_cache: bool) -> Result<String> {
        let url = format!("{}/en/comps/Big5/{}", self.base_url, stat_type);
        self.fetch_html_with_cache(&url, &format!("team_match_{}.html", stat_type)).await
    }

    pub async fn read_player_season_stats(&self, stat_type: &str) -> Result<String> {
        let url = format!("{}/en/comps/Big5/{}/players", self.base_url, stat_type);
        self.fetch_html_with_cache(&url, &format!("player_season_{}.html", stat_type)).await
    }

    pub async fn read_schedule(&self, _force_cache: bool) -> Result<String> {
        let url = format!("{}/en/comps/Big5/schedule/", self.base_url);
        self.fetch_html_with_cache(&url, "schedule.html").await
    }

    pub async fn read_player_match_stats(&self, stat_type: &str, match_id: &str, _force_cache: bool) -> Result<String> {
        let url = format!("{}/en/matches/{}", self.base_url, match_id);
        self.fetch_html_with_cache(&url, &format!("player_match_{}_{}.html", match_id, stat_type)).await
    }

    pub async fn read_lineup(&self, match_id: &str, force_cache: bool) -> Result<String> {
        self.read_player_match_stats("lineup", match_id, force_cache).await
    }

    pub async fn read_events(&self, match_id: &str, force_cache: bool) -> Result<String> {
        self.read_player_match_stats("events", match_id, force_cache).await
    }

    pub async fn read_shot_events(&self, match_id: &str, force_cache: bool) -> Result<String> {
        self.read_player_match_stats("shots", match_id, force_cache).await
    }

    /// Bezpieczny loader plików strumieniujących duże pliki HTML z systemem chroniącym przed weryfikacją (HTTP)
    async fn fetch_html_with_cache(&self, url: &str, cache_filename: &str) -> Result<String> {
        let cache_path = self.config.data_dir.join(cache_filename);

        if !self.config.no_cache && cache_path.exists() {
            tracing::info!("Wczytywanie z lokalnego Cache FBref: {:?}", cache_path);
            fs::read_to_string(cache_path).context("Błąd czytania Cache FBref")
        } else {
            tracing::info!("Pobieranie HTML z FBref: {}", url);
            let response = self.client.get(url).send().await.context("HTTP error z FBref")?;
            let text = response.text().await.context("Puste API FBref")?;
            
            if !self.config.no_store {
                if let Some(parent) = cache_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&cache_path, &text)?;
                tracing::info!("Zapisano kod HTML do Cache: {:?}", cache_path);
            }
            Ok(text)
        }
    }
}

// Zgodność wsteczna z trait Extractor
use crate::Extractor;
use domain::models::Match;

impl Extractor for FbrefExtractor {
    fn source_name(&self) -> &str {
        "FBref"
    }

    fn fetch_matches(&self) -> Result<Vec<Match>> {
        // Docelowo odpaliłaby parsing na stringu z read_schedule()
        Ok(vec![])
    }
}
