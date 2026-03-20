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
    
    /// Faza 3: Uruchamia logikę Fuzzy Matchingu pomiędzy drużynami zapisanch źródeł
    Reconcile,
    
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
    let _pool = domain::db::init_db(db_url).await?;

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
        Commands::Reconcile => {
            tracing::info!("Analizowanie drzewa Data Quality. Uruchamianie algorytmu MatchLinker...");
        }
        Commands::Serve { port } => {
            tracing::info!("Uruchamianie serwera API (Axum) dla Admin Panel");
            tracing::info!("Sprawdź stan serwera wykonując GET na http://localhost:{}/api/sync/status", port);
            api::server::start_server(port).await?;
        }
    }

    Ok(())
}
