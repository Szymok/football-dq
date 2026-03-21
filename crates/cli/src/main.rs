use clap::{Parser, Subcommand};
use extractors::fbref::{FbrefExtractor, FbrefConfig};
use extractors::understat::{UnderstatExtractor, UnderstatConfig};
use extractors::whoscored::{WhoscoredExtractor, WhoscoredConfig};
use extractors::sofascore::{SofascoreExtractor, SofascoreConfig};
use extractors::matchhistory::{MatchHistoryExtractor, MatchHistoryConfig};
use extractors::clubelo::{ClubEloExtractor, ClubEloConfig};
use extractors::espn::{EspnExtractor, EspnConfig};
use extractors::sofifa::{SofifaExtractor, SofifaConfig};
use extractors::official::OfficialApiExtractor;
use extractors::parsers::{self, NormalizedMatch};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "football-dq")]
#[command(author, version, about = "Football Data Quality CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Pobiera dedykowaną ramkę danych z pojedynczego agregatora
    Sync {
        /// Nazwa agregatora (np. fbref, understat, whoscored)
        #[arg(short = 's', long)]
        source: String,
        
        /// Identyfikator ligi (np. EPL, La_Liga)
        #[arg(short = 'l', long)]
        league: String,
        
        /// Identyfikator sezonu (np. 2122)
        #[arg(short = 'y', long)]
        season: String,

        /// Wymusza pobranie z HTTP pomijając pamięć Cache na dysku
        #[arg(short = 'f', long, default_value_t = false)]
        force_cache: bool,
    },
    
    /// Pobiera całościowy zbiór danego meczu / ligi RÓWNOLEGLE ze wszystkich agregatorów
    SyncAll {
        #[arg(short = 'l', long)]
        league: String,
        
        #[arg(short = 'y', long)]
        season: String,
    },
    
    /// Faza 9: Parsuje, łączy i porównuje mecze ze wszystkich źródeł danych
    Link {
        #[arg(short = 'l', long)]
        league: String,
        
        #[arg(short = 'y', long)]
        season: String,
    },
    
    /// Odpala serwer Axum i interfejs React Admin Panel (Zarządzanie Synchronizacją)
    Serve {
        #[arg(short, long, default_value_t = 8080)]
        port: u16,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Inicjalizacja warstwy Storage (SQLite z sqlx) chroniącej dane meczów
    let db_url = "sqlite:football-dq.db";
    let pool = domain::db::init_db(db_url).await?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Sync { source, league, season, force_cache } => {
            tracing::info!("Wybudzanie sterownika pobierania dla: {} (L: {} | S: {})", source, league, season);
            
            match source.to_lowercase().as_str() {
                "fbref" => {
                    let config = FbrefConfig {
                        leagues: vec![league.clone()],
                        seasons: vec![season.clone()],
                        ..Default::default()
                    };
                    let extractor = FbrefExtractor::new(Some(config));
                    let _ = extractor.read_schedule(force_cache).await?;
                },
                "understat" => {
                    let config = UnderstatConfig {
                        leagues: vec![league.clone()],
                        seasons: vec![season.clone()],
                        ..Default::default()
                    };
                    let extractor = UnderstatExtractor::new(Some(config));
                    let _ = extractor.read_schedule(force_cache, true).await?;
                },
                "whoscored" => {
                    let config = WhoscoredConfig {
                        leagues: vec![league.clone()],
                        seasons: vec![season.clone()],
                        ..Default::default()
                    };
                    let extractor = WhoscoredExtractor::new(Some(config));
                    let _ = extractor.read_schedule(force_cache).await?;
                },
                "sofascore" => {
                    let config = SofascoreConfig {
                        leagues: vec![league.clone()],
                        seasons: vec![season.clone()],
                        ..Default::default()
                    };
                    let extractor = SofascoreExtractor::new(Some(config));
                    let _ = extractor.read_schedule(force_cache).await?;
                },
                "matchhistory" => {
                    let config = MatchHistoryConfig {
                        leagues: vec![league.clone()],
                        seasons: vec![season.clone()],
                        ..Default::default()
                    };
                    let extractor = MatchHistoryExtractor::new(Some(config));
                    let _ = extractor.read_games().await?;
                },
                "clubelo" => {
                    let config = ClubEloConfig {
                        ..Default::default()
                    };
                    let extractor = ClubEloExtractor::new(Some(config));
                    let _ = extractor.read_team_history("Real Madrid").await?;
                },
                "espn" => {
                    let config = EspnConfig {
                        leagues: vec![league.clone()],
                        seasons: vec![season.clone()],
                        ..Default::default()
                    };
                    let extractor = EspnExtractor::new(Some(config));
                    let _ = extractor.read_schedule(force_cache).await?;
                },
                "sofifa" => {
                    let config = SofifaConfig {
                        leagues: vec![league.clone()],
                        ..Default::default()
                    };
                    let extractor = SofifaExtractor::new(Some(config));
                    let _ = extractor.read_players(None).await?;
                },
                "official" => {
                    tracing::info!("Inicjalizacja OfficialApiExtractor dla ligi: {}", league);
                    let _extractor = OfficialApiExtractor::new("http://api.football-data.org/v4/");
                },
                _ => {
                    tracing::error!("Nieznane źródło danych: {}. Dostępne: fbref, understat, whoscored, sofascore, matchhistory, clubelo, espn, sofifa, official", source);
                }
            }
            
            tracing::info!("Pomyślnie zsynchronizowano zrzuty pamięci do Cache dla: {}", source);
        }
        Commands::SyncAll { league, season } => {
            tracing::info!("Rozpoczęcie szerokopasmowej multiplikacji żądań dla {} ({}) ze wszystkich źródeł...", league, season);
            
            let fbref = FbrefExtractor::new(Some(FbrefConfig { leagues: vec![league.clone()], seasons: vec![season.clone()], ..Default::default() }));
            let _ = fbref.read_schedule(false).await;

            let understat = UnderstatExtractor::new(Some(UnderstatConfig { leagues: vec![league.clone()], seasons: vec![season.clone()], ..Default::default() }));
            let _ = understat.read_schedule(false, false).await;

            let whoscored = WhoscoredExtractor::new(Some(WhoscoredConfig { leagues: vec![league.clone()], seasons: vec![season.clone()], ..Default::default() }));
            let _ = whoscored.read_schedule(false).await;

            let sofascore = SofascoreExtractor::new(Some(SofascoreConfig { leagues: vec![league.clone()], seasons: vec![season.clone()], ..Default::default() }));
            let _ = sofascore.read_schedule(false).await;

            let matchhistory_league = if league == "EPL" { "E0".to_string() } else { league.clone() };
            let mh = MatchHistoryExtractor::new(Some(MatchHistoryConfig { leagues: vec![matchhistory_league], seasons: vec![season.clone()], ..Default::default() }));
            let _ = mh.read_games().await;

            let espn = EspnExtractor::new(Some(EspnConfig { leagues: vec![league.clone()], seasons: vec![season.clone()], ..Default::default() }));
            let _ = espn.read_schedule(false).await;

            let sofifa = SofifaExtractor::new(Some(SofifaConfig { leagues: vec![league.clone()], ..Default::default() }));
            let _ = sofifa.read_players(None).await;

            tracing::info!("Zakończono pobieranie ze wszystkich włączonych modułów.");
        }
        Commands::Link { league, season } => {
            tracing::info!("🔗 Faza 9: Parsowanie i łączenie meczów dla {} sezon {}", league, season);

            // ── 1. Odkrywanie plików danych ──
            let data_dir = PathBuf::from("data");
            let mut all_matches: Vec<NormalizedMatch> = Vec::new();

            // ESPN
            let espn_path = data_dir.join("ESPN").join(format!("schedule_{}.json", league));
            if espn_path.exists() {
                match parsers::parse_espn(&espn_path) {
                    Ok(m) => all_matches.extend(m),
                    Err(e) => tracing::warn!("ESPN parse error: {}", e),
                }
            } else {
                tracing::warn!("Brak pliku ESPN: {:?}", espn_path);
            }

            // Sofascore
            let ss_path = data_dir.join("Sofascore").join(format!("schedule_{}_{}.json", season, league));
            if ss_path.exists() {
                match parsers::parse_sofascore(&ss_path) {
                    Ok(m) => all_matches.extend(m),
                    Err(e) => tracing::warn!("Sofascore parse error: {}", e),
                }
            } else {
                tracing::warn!("Brak pliku Sofascore: {:?}", ss_path);
            }

            // Understat
            let us_path = data_dir.join("Understat").join(format!("schedule_{}_{}.json", season, league));
            if us_path.exists() {
                match parsers::parse_understat(&us_path) {
                    Ok(m) => all_matches.extend(m),
                    Err(e) => tracing::warn!("Understat parse error: {}", e),
                }
            } else {
                tracing::warn!("Brak pliku Understat: {:?}", us_path);
            }

            // MatchHistory
            let mh_league_code = if league == "EPL" { "E0" } else { &league };
            let mh_path = data_dir.join("MatchHistory").join(format!("{}_{}.csv", season, mh_league_code));
            if mh_path.exists() {
                match parsers::parse_matchhistory(&mh_path) {
                    Ok(m) => all_matches.extend(m),
                    Err(e) => tracing::warn!("MatchHistory parse error: {}", e),
                }
            } else {
                tracing::warn!("Brak pliku MatchHistory: {:?}", mh_path);
            }

            tracing::info!("📊 Załadowano {} meczów łącznie ze wszystkich źródeł", all_matches.len());

            // ── 2. Grupowanie meczów po dacie + normalizowanej nazwie drużyny ──
            use dq_core::linker::MatchLinker;
            let mut groups: HashMap<String, Vec<NormalizedMatch>> = HashMap::new();

            for m in &all_matches {
                let h_norm = MatchLinker::normalize(&m.home_team);
                let a_norm = MatchLinker::normalize(&m.away_team);
                let key = format!("{}|{}|{}", m.date, h_norm, a_norm);
                groups.entry(key).or_default().push(m.clone());
            }

            tracing::info!("🔗 Utworzono {} grup meczów (unikalne daty + drużyny)", groups.len());

            // ── 3. Budowanie LinkedMatch i zapis do SQLite ──
            // Czyścimy starą tabelę
            sqlx::query("DELETE FROM linked_matches").execute(&pool).await?;

            let mut total_inserted = 0u32;
            let mut score_mismatches = 0u32;

            for (_key, group) in &groups {
                if group.is_empty() { continue; }

                let first = &group[0];
                let canonical_home = MatchLinker::normalize(&first.home_team);
                let canonical_away = MatchLinker::normalize(&first.away_team);
                let date = &first.date;

                // Wyznacz wynik „konsensusowy" — najczęstszy wynik ze źródeł
                let consensus_goals = group.iter()
                    .filter_map(|m| m.home_goals.zip(m.away_goals))
                    .next();

                let (home_goals, away_goals) = consensus_goals.unwrap_or((0, 0));

                // Sprawdzenie zgodności wyników
                let scores: Vec<(u8, u8)> = group.iter()
                    .filter_map(|m| m.home_goals.zip(m.away_goals))
                    .collect();
                let score_agreement = scores.iter().all(|s| s == &scores[0]) && !scores.is_empty();

                if !score_agreement && !scores.is_empty() {
                    score_mismatches += 1;
                }

                // Rozbieżność xG
                let xgs: Vec<(f64, f64)> = group.iter()
                    .filter_map(|m| m.home_xg.zip(m.away_xg))
                    .collect();
                let xg_discrepancy = if xgs.len() >= 2 {
                    let max_diff = xgs.iter()
                        .flat_map(|a| xgs.iter().map(move |b| {
                            ((a.0 - b.0).abs() + (a.1 - b.1).abs()) / 2.0
                        }))
                        .fold(0.0f64, f64::max);
                    Some(max_diff)
                } else {
                    None
                };

                // JSON z danymi per źródło
                let source_data: Vec<serde_json::Value> = group.iter().map(|m| {
                    serde_json::json!({
                        "source": m.source,
                        "home_team": m.home_team,
                        "away_team": m.away_team,
                        "home_goals": m.home_goals,
                        "away_goals": m.away_goals,
                        "home_xg": m.home_xg,
                        "away_xg": m.away_xg,
                    })
                }).collect();
                let sources_json = serde_json::to_string(&source_data)?;

                sqlx::query(
                    "INSERT INTO linked_matches (date, home_team_canonical, away_team_canonical, home_goals, away_goals, sources_json, score_agreement, xg_discrepancy) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                )
                .bind(date)
                .bind(&canonical_home)
                .bind(&canonical_away)
                .bind(home_goals as i32)
                .bind(away_goals as i32)
                .bind(&sources_json)
                .bind(score_agreement)
                .bind(xg_discrepancy)
                .execute(&pool)
                .await?;

                total_inserted += 1;
            }

            let multi_source = groups.values().filter(|g| {
                let sources: std::collections::HashSet<&str> = g.iter().map(|m| m.source.as_str()).collect();
                sources.len() > 1
            }).count();

            tracing::info!("✅ Zapisano {} powiązanych meczów do SQLite", total_inserted);
            tracing::info!("📈 Z tego {} meczów ma dane z wielu źródeł", multi_source);
            tracing::info!("⚠️  {} meczów z rozbieżnością wyników", score_mismatches);
        }
        Commands::Serve { port } => {
            tracing::info!("Uruchamianie serwera API (Axum) dla Admin Panel");
            tracing::info!("Sprawdź stan serwera wykonując GET na http://localhost:{}/api/sync/status", port);
            api::server::start_server(port).await?;
        }
    }

    Ok(())
}
