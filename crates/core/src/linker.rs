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
    pub fn new(threshold: f64) -> Self {
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
        } else {
            jaro_winkler(&n_a, &n_b)
        }
    }

    /// Wycina często spotykane "śmieci" oszukujące algorytmy podobieństwa np "FC", "Utd"
    fn normalize(name: &str) -> String {
        name.to_lowercase()
            .replace(" fc", "")
            .replace(" cf", "")
            .replace(" afc", "")
            .replace(" f.c.", "")
            .replace(" utd", " united")
            .replace("manchester united", "man_united")
            .replace("man united", "man_united")
            .replace("manchester city", "man_city")
            .replace("man city", "man_city")
            .replace("athletic ", "")
            .replace(" athletic", "")
            .replace("sporting ", "")
            .replace(" sporting", "")
            .trim()
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize() {
        assert_eq!(MatchLinker::normalize("Arsenal FC"), "arsenal");
        assert_eq!(MatchLinker::normalize("Man Utd"), "man_united");
        assert_eq!(MatchLinker::normalize("Athletic Bilbao"), "bilbao");
    }

    #[test]
    fn test_jaro_winkler_match() {
        let linker = MatchLinker::default();
        
        // Exact after normalize
        assert!(linker.is_match("Arsenal", "Arsenal FC"));
        
        // High similarity
        assert!(linker.is_match("Manchester United", "Manchester Utd"));
        assert!(linker.is_match("Wolverhampton Wanderers", "Wolverhampton"));
        
        // Low similarity
        assert!(!linker.is_match("Arsenal", "Chelsea"));
        assert!(!linker.is_match("Manchester City", "Manchester United"));
    }
}
