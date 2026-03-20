use clap::{Parser, Subcommand};
use extractors::fbref::{FbrefExtractor, FbrefConfig};
use extractors::understat::{UnderstatExtractor, UnderstatConfig};
use extractors::whoscored::{WhoscoredExtractor, WhoscoredConfig};
use extractors::sofascore::{SofascoreExtractor, SofascoreConfig};
use extractors::matchhistory::{MatchHistoryExtractor, MatchHistoryConfig};
// reszta ekstraktorów np. ClubElo

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
        #[arg(short, long)]
        source: String,
        
        /// Identyfikator ligi (np. EPL, La_Liga)
        #[arg(short, long)]
        league: String,
        
        /// Identyfikator sezonu (np. 2122)
        #[arg(short, long)]
        season: String,

        /// Wymusza pobranie z HTTP pomijając pamięć Cache na dysku
        #[arg(short = 'f', long, default_value_t = false)]
        force_cache: bool,
    },
    
    /// Pobiera całościowy zbiór danego meczu / ligi RÓWNOLEGLE ze wszystkich agregatorów
    SyncAll {
        #[arg(short, long)]
        league: String,
        
        #[arg(short, long)]
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
                    let _ = extractor.read_schedule(false, force_cache).await?;
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
                    let _ = extractor.read_schedule(force_cache).await?;
                },
                _ => {
                    tracing::error!("Nieznane źródło danych: {}. Dostępne: fbref, understat, whoscored, sofascore, matchhistory", source);
                }
            }
            
            tracing::info!("Pomyślnie zsynchronizowano zrzuty pamięci do Cache dla: {}", source);
        }
        Commands::SyncAll { league, season } => {
            tracing::info!("Rozpoczęcie szerokopasmowej multiplikacji żądań dla {} ({}) ze wszystkich źródeł...", league, season);
            tracing::info!("Zakończono...");
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
