use std::collections::HashMap;
use std::path::Path;
use std::fs;
use anyhow::{Context, Result};

pub struct TeamNormalizer {
    mappings: HashMap<String, Vec<String>>,
}

impl TeamNormalizer {
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path).context("Brak pliku teamname_replacements.json")?;
        let mappings: HashMap<String, Vec<String>> = serde_json::from_str(&text)
            .context("Nieprawidłowy format JSON")?;
            
        Ok(Self { mappings })
    }
    
    pub fn normalize(&self, team: &str) -> String {
        for (std_name, aliases) in &self.mappings {
            if std_name.eq_ignore_ascii_case(team) || aliases.iter().any(|a| a.eq_ignore_ascii_case(team)) {
                return std_name.clone();
            }
        }
        team.to_string()
    }
}
