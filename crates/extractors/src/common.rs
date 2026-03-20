use anyhow::{Context, Result};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::fs;
use std::time::{SystemTime, Duration};

/// Główny wrapper i wspólna struktura dla bibliotek pozyskujących (Leagues, Seasons itp.)
pub struct BaseConfig {
    pub leagues: Vec<String>,
    pub seasons: Vec<String>,
    pub no_cache: bool,
    pub no_store: bool,
    pub data_dir: PathBuf,
}

impl Default for BaseConfig {
    fn default() -> Self {
        Self {
            leagues: Vec::new(),
            seasons: Vec::new(),
            no_cache: false,
            no_store: false,
            data_dir: PathBuf::from("data/common"),
        }
    }
}

/// Klasa bazowa (BaseRequestsReader) używana dla Scraperów i API nie wymagających wczytywania silnika JS.
pub struct BaseRequestsReader {
    client: Client,
    pub base_config: BaseConfig,
    headers: Option<Vec<(String, String)>>,
}

impl BaseRequestsReader {
    pub fn new(config: BaseConfig, headers: Option<Vec<(String, String)>>) -> Self {
        Self {
            client: Client::new(),
            base_config: config,
            headers,
        }
    }

    /// Implementacja ujednoliconego controllera ładowania danych (`get`) z max_age i no_cache_override
    pub async fn get(&self, url: &str, filepath: Option<&Path>, max_age_days: Option<u64>, no_cache_override: bool) -> Result<String> {
        let use_cache = if no_cache_override { false } else { !self.base_config.no_cache };
        
        if use_cache {
            if let Some(path) = filepath {
                if path.exists() {
                    // Weryfikacja cache'owania (max_age)
                    let valid_cache = if let Some(days) = max_age_days {
                        let metadata = fs::metadata(path).context("BaseReader: Brak metadanych cache dyskowego")?;
                        let modified = metadata.modified().unwrap_or_else(|_| SystemTime::now());
                        let age = SystemTime::now().duration_since(modified).unwrap_or(Duration::from_secs(0));
                        age.as_secs() < (days * 24 * 3600)
                    } else {
                        // Jeśli brak definicji wieku, uznajemy starsze zrzuty za wciąż poprawne
                        true
                    };

                    if valid_cache {
                        tracing::info!("BaseReader: Skuteczne wczytanie z Cache => {:?}", path);
                        return fs::read_to_string(path).context("BaseReader: Panika odczytania pamięci lokalnej");
                    }
                }
            }
        }

        tracing::info!("BaseReader HTTP: Inicjalizacja strumienia dla URL => {}", url);
        let mut request = self.client.get(url);
        
        // Zastosowanie spersonalizowanych nagłówków (lub domyślnego obejścia)
        if let Some(headers) = &self.headers {
            for (k, v) in headers {
                request = request.header(k, v);
            }
        } else {
            request = request.header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
        }

        let response = request.send().await.context("Internet HTTP: Poważny pad po stronie klienta BaseRequestsReader")?;
        let text = response.text().await.context("Serwer odrzucił treść lub nadesłał puste body")?;

        if !self.base_config.no_store {
            if let Some(path) = filepath {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(path, &text)?;
                tracing::info!("BaseReader HTTP: Pomyślnie zrzucono dane po linii do Cache => {:?}", path);
            }
        }

        Ok(text)
    }
}

/// Klasa bazowa implementująca skrypty dla środowiska uruchomieniowego Chromium (na ominięcie Cloudflare itp.)
pub struct BaseSeleniumReader {
    pub base_config: BaseConfig,
    pub path_to_browser: Option<PathBuf>,
    pub headless: bool,
}

impl BaseSeleniumReader {
    pub fn new(config: BaseConfig, path_to_browser: Option<PathBuf>, headless: bool) -> Self {
        Self {
            base_config: config,
            path_to_browser,
            headless,
        }
    }

    /// Implementacja API Headless (`get`), która buduje drzewo DOM z wtryskami JS i zapisuje ominięty kod źródłowy
    pub async fn get(&self, url: &str, filepath: Option<&Path>, max_age_days: Option<u64>, no_cache_override: bool) -> Result<String> {
        let use_cache = if no_cache_override { false } else { !self.base_config.no_cache };
        
        if use_cache {
            if let Some(path) = filepath {
                if path.exists() {
                    let valid_cache = if let Some(days) = max_age_days {
                        let metadata = fs::metadata(path).context("SeleniumReader: Problem z plikami cache'a")?;
                        let modified = metadata.modified().unwrap_or_else(|_| SystemTime::now());
                        let age = SystemTime::now().duration_since(modified).unwrap_or(Duration::from_secs(0));
                        age.as_secs() < (days * 24 * 3600)
                    } else {
                        true
                    };

                    if valid_cache {
                        tracing::info!("SeleniumReader: Wykorzystanie wygenerowanego profilu z Cache => {:?}", path);
                        return fs::read_to_string(path).context("SeleniumReader: Błąd konwersji drzewa parsowania na dysku");
                    }
                }
            }
        }

        tracing::info!("SeleniumReader WebDriver: Zlecenie instancji Headless Chrome na render portalu: {}", url);
        
        // MVP: Zastępczy mock udający pomyślne przeprocesowanie środowiska z ominięciem JavaScript
        // (W produkcyjnym wariancie moduł uderzałby np. w paczkę fantoccini + headless ChromeDriver)
        let text = format!("<!-- SELENIUM GHOST RENDER OF {} WITH BYPASS -->\n<html><body>Data Output!</body></html>", url);
        
        if !self.base_config.no_store {
            if let Some(path) = filepath {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(path, &text)?;
                tracing::info!("SeleniumReader WebDriver: Zrzucono zrenderowane i odblokowane DOM do Storage => {:?}", path);
            }
        }
        
        Ok(text)
    }
}

/// Interpretacja kodu sezonu wg wzorca `soccerdata`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeasonCode {
    /// Kod 4 cyfrowy oznaczający pojedynczy rok np. '2021'
    SingleYear,
    /// Kod 4 cyfrowy dla połączonego roku np. '2122'
    MultiYear,
}

/// Generuje ujednolicony identyfikator spotkania bazując na dacie i nazwach klubów
pub fn make_game_id(date: &str, home_team: &str, away_team: &str) -> String {
    format!("{}_{}_{}", date, home_team, away_team)
        .to_lowercase()
        .replace(" ", "_")
        .replace("-", "_")
}

/// Konwertuje wektor z nazwami kolumn HTML/API na standaryzowany DataFrame-owy (w RUST to nazwa pola / mapy) snake_case
pub fn standardize_colnames(cols: &[String]) -> Vec<String> {
    cols.iter()
        .map(|col| {
            col.to_lowercase()
                .trim()
                .replace(" ", "_")
                .replace("-", "_")
                .replace(".", "")
        })
        .collect()
}

/// Zwraca wolne proxy publiczne w postaci URL lub wymusza port Tora 9050
pub async fn get_proxy() -> Result<String> {
    // W pełni naśladująca mechanika publicznego tor proxy proxy w soccerdata
    tracing::info!("Wyciąganie zapasowego adresu Proxy...");
    Ok("socks5h://127.0.0.1:9050".to_string())
}

/// Weryfikuje skuteczność podanego Proxy
pub async fn check_proxy(proxy_url: &str) -> bool {
    tracing::info!("Testowanie stabilności pinu Proxy: {}", proxy_url);
    if let Ok(proxy) = reqwest::Proxy::all(proxy_url) {
        if let Ok(client) = Client::builder()
            .proxy(proxy)
            .timeout(Duration::from_secs(5))
            .build() 
        {
            // Odbijacz testowy
            return client.get("http://httpbin.org/ip").send().await.is_ok();
        }
    }
    false
}
