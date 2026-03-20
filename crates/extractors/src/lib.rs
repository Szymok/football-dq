pub mod fbref;
pub mod official;
pub mod clubelo;
pub mod normalize;
pub mod espn;
pub mod matchhistory;
pub mod sofascore;
pub mod sofifa;
pub mod understat;


use anyhow::Result;
use domain::models::Match;

pub trait Extractor {
    fn source_name(&self) -> &str;
    fn fetch_matches(&self, league: &str, season: &str) -> impl std::future::Future<Output = Result<Vec<Match>>> + Send;
}
