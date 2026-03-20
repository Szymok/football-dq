use anyhow::{Context, Result};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::fs;

/// Konfig dla ekstraktora SoFIFA oparty w 100% o specyfikacje soccerdata
pub struct SofifaConfig {
    pub leagues: Vec<String>,
    pub versions: String, // FIFA versions - domyślnie 'latest'
    pub no_cache: bool,
    pub no_store: bool,
    pub data_dir: PathBuf,
}

impl Default for SofifaConfig {
    fn default() -> Self {
        Self {
            leagues: Vec::new(),
            versions: "latest".to_string(),
            no_cache: false,
            no_store: false,
            data_dir: PathBuf::from("data/SoFIFA"),
        }
    }
}

pub struct SofifaExtractor {
    client: Client,
    base_url: String,
    pub config: SofifaConfig,
}

impl SofifaExtractor {
    pub fn new(config: Option<SofifaConfig>) -> Self {
        Self {
            client: Client::new(),
            base_url: "https://sofifa.com".to_string(),
            config: config.unwrap_or_default(),
        }
    }

    /// Pobieranie definicji wybranych lig
    pub async fn read_leagues(&self) -> Result<String> {
        let url = format!("{}/leagues", self.base_url);
        self.fetch_html_with_cache(&url, "leagues.html").await
    }

    /// Informacje o wersjach silnika / gry FIFA
    pub async fn read_versions(&self, _max_age: u32) -> Result<String> {
        let url = format!("{}/", self.base_url);
        self.fetch_html_with_cache(&url, "versions.html").await
    }

    /// Lista połączonych drużyn na startym API
    pub async fn read_teams(&self) -> Result<String> {
        let url = format!("{}/teams", self.base_url);
        self.fetch_html_with_cache(&url, "teams.html").await
    }

    /// Filtrowany arkusz wszystkich piłkarzy (per wszystkie ligi bądz per 1 drużyna)
    pub async fn read_players(&self, team: Option<&str>) -> Result<String> {
        let url = match team {
            Some(t) => format!("{}/team/{}", self.base_url, t), 
            None => format!("{}/players", self.base_url),
        };
        let cache_name = team.unwrap_or("all");
        let cache_file = format!("players_{}.html", cache_name);
        
        self.fetch_html_with_cache(&url, &cache_file).await
    }

    /// Retrieve ratings for all teams in the selected leagues
    pub async fn read_team_ratings(&self) -> Result<String> {
        let url = format!("{}/teams?type=all", self.base_url);
        self.fetch_html_with_cache(&url, "team_ratings.html").await
    }

    /// Odczytywanie punktów / overall ratings dla wybranego zawodnika i / lub drużyny
    pub async fn read_player_ratings(&self, team: Option<&str>, player: Option<&str>) -> Result<String> {
        let url = match (team, player) {
            (_, Some(p)) => format!("{}/player/{}", self.base_url, p),
            (Some(t), _) => format!("{}/team/{}", self.base_url, t),
            _ => format!("{}/players?type=all", self.base_url),
        };
        
        let t_name = team.unwrap_or("all");
        let p_name = player.unwrap_or("all");
        let cache_file = format!("player_ratings_{}_{}.html", t_name, p_name);
        
        self.fetch_html_with_cache(&url, &cache_file).await
    }

    /// Scrapper HTML osłonięty flagowaniem proxy oraz cache lokalnym
    async fn fetch_html_with_cache(&self, url: &str, cache_filename: &str) -> Result<String> {
        let cache_path = self.config.data_dir.join(cache_filename);

        if !self.config.no_cache && cache_path.exists() {
            tracing::info!("Wczytywanie lokalnego Cache SoFIFA: {:?}", cache_path);
            fs::read_to_string(cache_path).context("Błąd wczytywania Cache dla portalu SoFIFA z dysku")
        } else {
            tracing::info!("Pobieranie w locie HTML z SoFIFA: {}", url);
            let response = self.client.get(url)
                // Wzorowanie zabezpieczenia - oszustwo headerów, by strona nie odrzucała nas na warstwie Cloudflare
                .header("User-Agent", "Mozilla/5.0")
                .send()
                .await.context("HTTP z sieci: Błąd dla domeny SoFIFA API")?;
                
            let text = response.text().await.context("Pusta zawartość API SoFIFA")?;
            
            if !self.config.no_store {
                if let Some(parent) = cache_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&cache_path, &text)?;
                tracing::info!("Zapisano do dysku potężny HTML SoFIFA pod ścieżkę: {:?}", cache_path);
            }
            Ok(text)
        }
    }
}
