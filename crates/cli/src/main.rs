use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "football-dq")]
#[command(about = "Football Data Quality CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Ingest data from a specific source
    Ingest {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        league: String,
        #[arg(short, long)]
        season: String,
    },
    /// Reconcile matched data between aggregators and official sources
    Reconcile,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Ingest { source, league, season } => {
            tracing::info!("Ingesting data from {} for {} ({})", source, league, season);
            // Example usage:
            // let extractor = extractors::fbref::FbrefExtractor;
            // extractor.fetch_matches(&league, &season).await?;
        }
        Commands::Reconcile => {
            tracing::info!("Starting reconciliation process...");
        }
    }

    Ok(())
}
