use anyhow::{Context, Result};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::fs;

/// Konfig dla ekstraktora Understat w oparciu o parametry z `soccerdata`
pub struct UnderstatConfig {
    pub leagues: Vec<String>,
    pub seasons: Vec<String>,
    pub no_cache: bool,
    pub no_store: bool,
    pub data_dir: PathBuf,
}

impl Default for UnderstatConfig {
    fn default() -> Self {
        Self {
            leagues: Vec::new(),
            seasons: Vec::new(),
            no_cache: false,
            no_store: false,
            data_dir: PathBuf::from("data/Understat"),
        }
    }
}

pub struct UnderstatExtractor {
    client: Client,
    base_url: String,
    pub config: UnderstatConfig,
}

impl UnderstatExtractor {
    pub fn new(config: Option<UnderstatConfig>) -> Self {
        Self {
            client: Client::new(),
            base_url: "https://understat.com".to_string(),
            config: config.unwrap_or_default(),
        }
    }

    /// Pobieranie HTMLa ze stron głównych zawierających ligi
    pub async fn read_leagues(&self) -> Result<String> {
        let url = format!("{}/", self.base_url);
        self.fetch_source_with_cache(&url, "leagues.html").await
    }

    /// Pobieranie zbiorów z sezonami
    pub async fn read_seasons(&self) -> Result<String> {
        let url = format!("{}/", self.base_url);
        self.fetch_source_with_cache(&url, "seasons.html").await
    }

    /// Terminarz gier wg sezonu i ligi — Understat teraz serwuje dane wyłącznie przez XHR API
    pub async fn read_schedule(&self, force_cache: bool, _extract_scripts: bool) -> Result<Vec<serde_json::Value>> {
        let mut results = Vec::new();
        
        let no_cache_override = if force_cache { true } else { self.config.no_cache };
        
        for league in &self.config.leagues {
            for season in &self.config.seasons {
                let season_year = if season.len() == 4 && season.chars().all(char::is_numeric) {
                    format!("20{}", &season[0..2])
                } else {
                    season.clone()
                };

                tracing::info!("Rozpoczynam ekstrakcję Understat XHR API - Liga: {}, Rok: {}", league, season_year);
                
                let cache_file = self.config.data_dir.join(format!("schedule_{}_{}.json", season, league));
                
                let json_data = if !no_cache_override && cache_file.exists() {
                    tracing::info!("Understat wczytywany bezpiecznie z Cache: {:?}", cache_file);
                    let text = fs::read_to_string(&cache_file).context("Błąd I/O Cache Understat")?;
                    serde_json::from_str(&text).context("Błąd parsowania JSON'a wejściowego ze schowka")?
                } else {
                    // Nowy endpoint XHR API Understata (zmiana z 2025/2026)
                    let url = format!("{}/getLeagueData/{}/{}", self.base_url, league, season_year);
                    tracing::info!("Wywoływanie XHR API Understat: {}", url);
                    
                    let response = self.client.get(&url)
                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                        .header("X-Requested-With", "XMLHttpRequest")
                        .header("Referer", &format!("{}/league/{}/{}", self.base_url, league, season_year))
                        .send()
                        .await.context("Błąd HTTP dla XHR API Understat")?;
                    
                    let text = response.text().await.context("Puste dane z XHR API Understat")?;
                    
                    let json_val: serde_json::Value = serde_json::from_str(&text)
                        .context("Błąd parsowania JSON z nowego API Understat")?;

                    if !self.config.no_store {
                        if let Some(parent) = cache_file.parent() {
                            fs::create_dir_all(parent)?;
                        }
                        fs::write(&cache_file, serde_json::to_string_pretty(&json_val)?)?;
                        tracing::info!("Powodzenie! Zapisano JSON z XHR API Understat: {:?}", cache_file);
                    }
                    json_val
                };
                results.push(json_data);
            }
        }
        Ok(results)
    }

    /// Analiza XG z meczów całej ligi - skrypt z landing page understat
    pub async fn read_team_match_stats(&self, force_cache: bool) -> Result<Vec<String>> {
        let mut results = Vec::new();
        let no_cache_override = if force_cache { true } else { self.config.no_cache };
        
        for season in &self.config.seasons {
            for league in &self.config.leagues {
                let mapped_league = if league == "EPL" { "EPL" } else { league.as_str() };
                let mapped_season = if season == "2526" { "2025" } else if season == "2425" { "2024" } else { season.as_str() };
                let url = format!("{}/league/{}/{}", self.base_url, mapped_league, mapped_season);
                let cache_file = self.config.data_dir.join(format!("schedule_{}_{}.json", season, league));
                
                let source = self.fetch_source_with_cache_override(&url, &cache_file, no_cache_override).await?;
                results.push(source);
            }
        }
        Ok(results)
    }

    /// Statystyki xG oraz asyst i szans wygenerowanych dla pojedynczych zawodników per sezon
    pub async fn read_player_season_stats(&self, force_cache: bool) -> Result<Vec<String>> {
        let mut results = Vec::new();
        let no_cache_override = if force_cache { true } else { self.config.no_cache };
        
        for season in &self.config.seasons {
            for league in &self.config.leagues {
                let url = format!("{}/league/{}/{}", self.base_url, league, season);
                let cache_file = self.config.data_dir.join(format!("player_season_stats_{}_{}.html", season, league));
                
                let source = self.fetch_source_with_cache_override(&url, &cache_file, no_cache_override).await?;
                results.push(source);
            }
        }
        Ok(results)
    }

    /// Odczyt szczegółów rozegranego meczu wraz ze zmiennymi xG z timeline
    pub async fn read_player_match_stats(&self, match_id: &str) -> Result<String> {
        let url = format!("{}/match/{}", self.base_url, match_id);
        let cache_file = self.config.data_dir.join(format!("player_match_stats_{}.html", match_id));
        self.fetch_source_with_cache_override(&url, &cache_file, self.config.no_cache).await
    }

    /// Pobieranie bazy danych każdego poszczególnego strzału w meczu z koordynatami [X,Y] i xG
    pub async fn read_shot_events(&self, match_id: &str) -> Result<String> {
        let url = format!("{}/match/{}", self.base_url, match_id);
        let cache_file = self.config.data_dir.join(format!("shot_events_{}.html", match_id));
        self.fetch_source_with_cache_override(&url, &cache_file, self.config.no_cache).await
    }

    async fn fetch_source_with_cache(&self, url: &str, cache_filename: &str) -> Result<String> {
        let cache_path = self.config.data_dir.join(cache_filename);
        self.fetch_source_with_cache_override(url, &cache_path, self.config.no_cache).await
    }

    /// Bezpieczny klient HTML imitujący sesję dla ominięcia blokad w Understacie (w tym obejście dla wymuszania zrzutu JSON script tag)
    async fn fetch_source_with_cache_override(&self, url: &str, cache_path: &Path, no_cache: bool) -> Result<String> {
        if !no_cache && cache_path.exists() {
            tracing::info!("Wczytywanie statystyk Understat z lokalnego pliku: {:?}", cache_path);
            fs::read_to_string(cache_path).context("Błąd czytania Cache Understat z dysku")
        } else {
            tracing::info!("Rozpoczynanie transmisji HTML z Understat: {}", url);
            let response = self.client.get(url)
                .header("User-Agent", "Mozilla/5.0")
                .send()
                .await.context("Internet HTTP: Błąd połączenia dla domeny Understat")?;
                
            let text = response.text().await.context("Serwer zwrócił puste dane HTML")?;
            
            if !self.config.no_store {
                if let Some(parent) = cache_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(cache_path, &text)?;
                tracing::info!("Powodzenie! Zapisano źródło HTML z parametrami xG: {:?}", cache_path);
            }
            Ok(text)
        }
    }
}
