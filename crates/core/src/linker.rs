use strsim::jaro_winkler;

pub struct MatchLinker {
    threshold: f64,
}

impl Default for MatchLinker {
    fn default() -> Self {
        Self { threshold: 0.89 }
    }
}

impl MatchLinker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_threshold(threshold: f64) -> Self {
        Self { threshold }
    }

    /// Sprawdza czy dwie nazwy oznaczają ten sam klub, uwzględniając normalizację i Jaro-Winkler
    pub fn is_match(&self, name_a: &str, name_b: &str) -> bool {
        let score = self.get_score(name_a, name_b);
        score >= self.threshold
    }

    /// Zwraca współczynnik podobieństwa Stringów (0.0 - 1.0)
    pub fn get_score(&self, name_a: &str, name_b: &str) -> f64 {
        let n_a = Self::normalize(name_a);
        let n_b = Self::normalize(name_b);

        if n_a == n_b {
            1.0
        } else if n_a.contains("manchester") && n_b.contains("manchester") {
            0.0 // Manchester City != Manchester United
        } else {
            jaro_winkler(&n_a, &n_b)
        }
    }

    /// Kanoniczna normalizacja nazwy drużyny — mapuje aliasy ze wszystkich źródeł
    /// na jedną wspólną formę, aby łączyć mecze z ESPN/Understat/MatchHistory/Sofascore
    pub fn normalize(name: &str) -> String {
        let mut s = name.to_lowercase().trim().to_string();

        // Faza 1: Usuń typowe sufiksy
        for suffix in &[" fc", " cf", " afc", " f.c.", " sc"] {
            s = s.replace(suffix, "");
        }
        s = s.trim().to_string();

        // Faza 2: Aliasy EPL — mapowanie na kanoniczny klucz
        let canonical = match s.as_str() {
            // Manchester United warianty
            "man united" | "man utd" | "manchester utd" | "manchester united" | "mufc" => "manchester united",
            // Manchester City warianty
            "man city" | "manchester city" | "mcfc" => "manchester city",
            // Wolverhampton Wanderers
            "wolves" | "wolverhampton wanderers" | "wolverhampton" => "wolverhampton",
            // Newcastle United
            "newcastle" | "newcastle united" | "newcastle utd" | "nufc" => "newcastle united",
            // Nottingham Forest
            "nott'm forest" | "nottingham forest" | "nott forest" | "nottm forest" => "nottingham forest",
            // Bournemouth
            "bournemouth" | "afc bournemouth" => "bournemouth",
            // Tottenham
            "tottenham" | "tottenham hotspur" | "spurs" => "tottenham",
            // West Ham
            "west ham" | "west ham united" | "west ham utd" => "west ham",
            // Brighton
            "brighton" | "brighton and hove albion" | "brighton & hove albion" => "brighton",
            // Crystal Palace
            "crystal palace" | "c palace" => "crystal palace",
            // Leeds
            "leeds" | "leeds united" | "leeds utd" => "leeds",
            // Ipswich
            "ipswich" | "ipswich town" => "ipswich",
            // Leicester
            "leicester" | "leicester city" => "leicester",
            // Southampton
            "southampton" | "soton" => "southampton",
            // Sheffield United
            "sheffield united" | "sheffield utd" | "sheff utd" | "sheffield" => "sheffield united",
            // Fulham
            "fulham" => "fulham",
            // Brentford
            "brentford" => "brentford",
            // Burnley
            "burnley" => "burnley",
            // Luton
            "luton" | "luton town" => "luton",
            // Sunderland
            "sunderland" => "sunderland",
            _ => &s,
        };

        canonical.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_aliases() {
        // EPL aliases — different sources produce the same canonical name
        assert_eq!(MatchLinker::normalize("Arsenal FC"), "arsenal");
        assert_eq!(MatchLinker::normalize("Man Utd"), "manchester united");
        assert_eq!(MatchLinker::normalize("Manchester United"), "manchester united");
        assert_eq!(MatchLinker::normalize("Man City"), "manchester city");
        assert_eq!(MatchLinker::normalize("Manchester City"), "manchester city");
        assert_eq!(MatchLinker::normalize("Wolves"), "wolverhampton");
        assert_eq!(MatchLinker::normalize("Wolverhampton Wanderers"), "wolverhampton");
        assert_eq!(MatchLinker::normalize("Newcastle"), "newcastle united");
        assert_eq!(MatchLinker::normalize("Newcastle United"), "newcastle united");
        assert_eq!(MatchLinker::normalize("Nott'm Forest"), "nottingham forest");
        assert_eq!(MatchLinker::normalize("Nottingham Forest"), "nottingham forest");
        assert_eq!(MatchLinker::normalize("AFC Bournemouth"), "bournemouth");
        assert_eq!(MatchLinker::normalize("Bournemouth"), "bournemouth");
        assert_eq!(MatchLinker::normalize("Tottenham Hotspur"), "tottenham");
        assert_eq!(MatchLinker::normalize("Tottenham"), "tottenham");
    }

    #[test]
    fn test_different_teams_stay_different() {
        let linker = MatchLinker::default();
        assert!(!linker.is_match("Arsenal", "Chelsea"));
        assert!(!linker.is_match("Manchester City", "Manchester United"));
    }
}
