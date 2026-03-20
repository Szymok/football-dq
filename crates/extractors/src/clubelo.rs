use anyhow::{Context, Result};
use reqwest::Client;
use domain::models::ClubEloRow;
use csv::ReaderBuilder;
use std::path::{Path, PathBuf};
use std::fs;

/// Konfiguracja mechanizmów zachowania ekstraktora, odwzorowująca bibliotekę soccerdata
pub struct ClubEloConfig {
    pub no_cache: bool,
    pub no_store: bool,
    pub data_dir: PathBuf,
}

impl Default for ClubEloConfig {
    fn default() -> Self {
        Self {
            no_cache: false,
            no_store: false,
            data_dir: PathBuf::from("data/ClubElo"),
        }
    }
}

pub struct ClubEloExtractor {
    client: Client,
    base_url: String,
    pub config: ClubEloConfig,
}

impl ClubEloExtractor {
    pub fn new(config: Option<ClubEloConfig>) -> Self {
        Self {
            client: Client::new(),
            base_url: "http://api.clubelo.com".to_string(),
            config: config.unwrap_or_default(),
        }
    }

    /// Pobieranie statystyk ClubElo dla wszystkich drużyn we wskazanym dniu (format YYYY-MM-DD)
    pub async fn read_by_date(&self, date: &str) -> Result<Vec<ClubEloRow>> {
        let url = format!("{}/{}", self.base_url, date);
        let cache_file = self.config.data_dir.join(format!("{}.csv", date));
        
        self.fetch_with_cache(&url, &cache_file).await
    }

    /// Pobieranie pełnej historii rankingowej dla wybranego klubu
    pub async fn read_team_history(&self, team: &str) -> Result<Vec<ClubEloRow>> {
        let formatted_team = team.replace(" ", "");
        let url = format!("{}/{}", self.base_url, formatted_team);
        let cache_file = self.config.data_dir.join(format!("{}.csv", formatted_team));
        
        self.fetch_with_cache(&url, &cache_file).await
    }

    /// Bezpieczny transfer sieciowy uwzględniający zapisywanie CSV na dysk w celu uniknięcia rate-limitów
    async fn fetch_with_cache(&self, url: &str, cache_path: &Path) -> Result<Vec<ClubEloRow>> {
        let csv_text = if !self.config.no_cache && cache_path.exists() {
            tracing::info!("Wczytywanie z lokalnego Cache: {:?}", cache_path);
            fs::read_to_string(cache_path).context("Błąd wczytywania z dysku")?
        } else {
            tracing::info!("Pobieranie z API ClubElo: {}", url);
            let response = self.client.get(url).send().await.context("Błąd połączenia HTTP z ClubElo API")?;
            let text = response.text().await.context("Nie udało się odczytać odpowiedzi z API")?;
            
            if !self.config.no_store {
                if let Some(parent) = cache_path.parent() {
                    fs::create_dir_all(parent).context("Nie mogę utworzyć katalogu cache")?;
                }
                fs::write(cache_path, &text).context("Nie mogę zapisać do cache")?;
                tracing::info!("Zapisano historię do Cache: {:?}", cache_path);
            }
            
            text
        };
        
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(csv_text.as_bytes());
            
        let mut rows = Vec::new();
        
        for result in rdr.deserialize() {
            let record: ClubEloRow = result.context("Błąd podczas parsowania wiersza CSV z ClubElo")?;
            rows.push(record);
        }
        
        Ok(rows)
    }
}
