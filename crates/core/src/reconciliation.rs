use crate::models::{DataQualityMetric, Match};

/// Moduł odpowiedzialny za łączenie rekordów pochodzących z różnych źródeł
pub struct MatchLinker;

impl MatchLinker {
    pub fn new() -> Self {
        Self
    }
    
    /// Symulacja Fuzzy Matching dla drużyn oparta o skrócone matchowanie dat i częściowych nazw
    pub fn link<'a>(&self, official: &'a [Match], aggregators: &'a [Match]) -> Vec<(&'a Match, &'a Match)> {
        let mut linked = Vec::new();
        // W przyszłości: zaawansowany dystans Levenshteina, mapy synonimów
        for off_match in official {
            for agg_match in aggregators {
                if off_match.date == agg_match.date {
                    // Prosty przypadek - np. "Arsenal FC" z official API zawiera słowo "Arsenal" ze scrapera
                    if off_match.home_team.contains(&agg_match.home_team) || agg_match.home_team.contains(&off_match.home_team) {
                        linked.push((off_match, agg_match));
                    }
                }
            }
        }
        linked
    }
}

/// Moduł ewaluujący jakość połączonych par za pomocą reguł DQ
pub struct MatchReconciler;

impl MatchReconciler {
    pub fn new() -> Self {
        Self
    }

    /// Oblicza metryki Data Quality zestawiając oficjalne źródło prawdy z agregatorem
    pub fn reconcile(&self, off_match: &Match, agg_match: &Match) -> Vec<DataQualityMetric> {
        let mut metrics = Vec::new();
        
        // Sprawdzenie "Consistency" - czy oficjalny wynik bramkowy zgadza się ze scrapowanym wynikiem?
        if off_match.home_score != agg_match.home_score || off_match.away_score != agg_match.away_score {
            metrics.push(DataQualityMetric {
                id: format!("dq_score_{}_{}", off_match.id, agg_match.id),
                dimension: "Consistency".to_string(),
                score: 0.0,
                description: format!("Score mismatch: {} ({} - {}) vs {} ({} - {})", 
                    off_match.source, off_match.home_score, off_match.away_score,
                    agg_match.source, agg_match.home_score, agg_match.away_score
                ),
            });
        } else {
             metrics.push(DataQualityMetric {
                id: format!("dq_score_{}_{}", off_match.id, agg_match.id),
                dimension: "Consistency".to_string(),
                score: 1.0,
                description: "Scores match perfectly between official and aggregated source".to_string(),
            });
        }
        
        metrics
    }
}
