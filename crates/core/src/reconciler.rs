use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct StatDiscrepancy {
    pub stat_name: String,
    pub source_a_val: f64,
    pub source_b_val: f64,
    pub abs_diff: f64,
}

pub struct MatchReconciler {
    pub tolerance: f64,
}

impl Default for MatchReconciler {
    fn default() -> Self {
        Self { tolerance: 0.05 } // 5% różnicy w statystykach typu xG
    }
}

impl MatchReconciler {
    pub fn new(tolerance: f64) -> Self {
        Self { tolerance }
    }

    pub fn compare_xg(&self, xg_a: f64, xg_b: f64) -> Option<StatDiscrepancy> {
        let diff = (xg_a - xg_b).abs();
        if diff > self.tolerance {
            Some(StatDiscrepancy {
                stat_name: "xG".to_string(),
                source_a_val: xg_a,
                source_b_val: xg_b,
                abs_diff: diff,
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_xg() {
        let rec = MatchReconciler::default();
        
        let diff1 = rec.compare_xg(1.23, 1.25);
        assert!(diff1.is_none());

        let diff2 = rec.compare_xg(1.23, 1.50);
        assert!(diff2.is_some());
        assert_eq!(diff2.unwrap().stat_name, "xG");
    }
}
