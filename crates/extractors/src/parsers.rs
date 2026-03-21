use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;

/// Zunifikowany model meczu — wspólny format dla wszystkich źródeł
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedMatch {
    pub source: String,
    pub date: String,           // YYYY-MM-DD
    pub home_team: String,
    pub away_team: String,
    pub home_goals: Option<u8>,
    pub away_goals: Option<u8>,
    pub home_xg: Option<f64>,
    pub away_xg: Option<f64>,
    pub ht_home_goals: Option<u8>,
    pub ht_away_goals: Option<u8>,
    pub home_shots: Option<u8>,
    pub away_shots: Option<u8>,
    pub home_shots_on_target: Option<u8>,
    pub away_shots_on_target: Option<u8>,
    pub home_corners: Option<u8>,
    pub away_corners: Option<u8>,
    pub home_fouls: Option<u8>,
    pub away_fouls: Option<u8>,
    pub home_yellow: Option<u8>,
    pub away_yellow: Option<u8>,
    pub home_red: Option<u8>,
    pub away_red: Option<u8>,
    pub home_npxg: Option<f64>,
    pub away_npxg: Option<f64>,
    pub home_ppda: Option<f64>,
    pub away_ppda: Option<f64>,
    pub home_deep: Option<u8>,
    pub away_deep: Option<u8>,
    pub referee: Option<String>,
}

// ─── ESPN ────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct EspnRoot {
    events: Option<Vec<EspnEvent>>,
    leagues: Option<Vec<EspnLeague>>,
}

#[derive(Deserialize)]
struct EspnLeague {
    events: Option<Vec<EspnEvent>>,
}

#[derive(Deserialize)]
struct EspnEvent {
    date: Option<String>,
    competitions: Option<Vec<EspnCompetition>>,
}

#[derive(Deserialize)]
struct EspnCompetition {
    competitors: Option<Vec<EspnCompetitor>>,
    date: Option<String>,
}

#[derive(Deserialize)]
struct EspnCompetitor {
    #[serde(rename = "homeAway")]
    home_away: Option<String>,
    score: Option<String>,
    team: Option<EspnTeam>,
}

#[derive(Deserialize)]
struct EspnTeam {
    #[serde(rename = "displayName")]
    display_name: Option<String>,
}

pub fn parse_espn(path: &Path) -> Result<Vec<NormalizedMatch>> {
    let text = std::fs::read_to_string(path).context("Nie można odczytać ESPN JSON")?;
    let root: EspnRoot = serde_json::from_str(&text).context("Błąd parsowania ESPN JSON")?;

    let events = root.events.or_else(|| {
        root.leagues.and_then(|leagues| {
            leagues.into_iter().next().and_then(|l| l.events)
        })
    }).unwrap_or_default();

    let mut matches = Vec::new();

    for event in &events {
        let comps = event.competitions.as_deref().unwrap_or_default();
        for comp in comps {
            let competitors = comp.competitors.as_deref().unwrap_or_default();
            let mut home_team = String::new();
            let mut away_team = String::new();
            let mut home_score: Option<u8> = None;
            let mut away_score: Option<u8> = None;

            for c in competitors {
                let team_name = c.team.as_ref()
                    .and_then(|t| t.display_name.clone())
                    .unwrap_or_default();
                let score = c.score.as_ref().and_then(|s| s.parse::<u8>().ok());

                match c.home_away.as_deref() {
                    Some("home") => { home_team = team_name; home_score = score; }
                    Some("away") => { away_team = team_name; away_score = score; }
                    _ => {}
                }
            }

            let raw_date = comp.date.as_ref()
                .or(event.date.as_ref())
                .cloned()
                .unwrap_or_default();
            let date = extract_date(&raw_date);

            if !home_team.is_empty() && !away_team.is_empty() {
                matches.push(NormalizedMatch {
                    source: "espn".into(),
                    date,
                    home_team,
                    away_team,
                    home_goals: home_score,
                    away_goals: away_score,
                    home_xg: None,
                    away_xg: None,
                    ht_home_goals: None,
                    ht_away_goals: None,
                    home_shots: None,
                    away_shots: None,
                    home_shots_on_target: None,
                    away_shots_on_target: None,
                    home_corners: None,
                    away_corners: None,
                    home_fouls: None,
                    away_fouls: None,
                    home_yellow: None,
                    away_yellow: None,
                    home_red: None,
                    away_red: None,
                    home_npxg: None,
                    away_npxg: None,
                    home_ppda: None,
                    away_ppda: None,
                    home_deep: None,
                    away_deep: None,
                    referee: None,
                });
            }
        }
    }

    tracing::info!("ESPN: sparsowano {} meczów", matches.len());
    Ok(matches)
}

// ─── Sofascore ───────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct SofascoreRoot {
    events: Option<Vec<SofascoreEvent>>,
}

#[derive(Deserialize)]
struct SofascoreEvent {
    #[serde(rename = "homeTeam")]
    home_team: Option<SofascoreTeam>,
    #[serde(rename = "awayTeam")]
    away_team: Option<SofascoreTeam>,
    #[serde(rename = "homeScore")]
    home_score: Option<SofascoreScore>,
    #[serde(rename = "awayScore")]
    away_score: Option<SofascoreScore>,
    #[serde(rename = "startTimestamp")]
    start_timestamp: Option<i64>,
}

#[derive(Deserialize)]
struct SofascoreTeam {
    name: Option<String>,
}

#[derive(Deserialize)]
struct SofascoreScore {
    current: Option<u8>,
}

pub fn parse_sofascore(path: &Path) -> Result<Vec<NormalizedMatch>> {
    let text = std::fs::read_to_string(path).context("Nie można odczytać Sofascore JSON")?;
    let root: SofascoreRoot = serde_json::from_str(&text).context("Błąd parsowania Sofascore JSON")?;

    let events = root.events.unwrap_or_default();
    let mut matches = Vec::new();

    for ev in &events {
        let home = ev.home_team.as_ref().and_then(|t| t.name.clone()).unwrap_or_default();
        let away = ev.away_team.as_ref().and_then(|t| t.name.clone()).unwrap_or_default();
        let h_goals = ev.home_score.as_ref().and_then(|s| s.current);
        let a_goals = ev.away_score.as_ref().and_then(|s| s.current);

        let date = ev.start_timestamp
            .map(|ts| {
                let dt = chrono::DateTime::from_timestamp(ts, 0)
                    .unwrap_or_default();
                dt.format("%Y-%m-%d").to_string()
            })
            .unwrap_or_default();

        if !home.is_empty() && !away.is_empty() {
            matches.push(NormalizedMatch {
                source: "sofascore".into(),
                date,
                home_team: home,
                away_team: away,
                home_goals: h_goals,
                away_goals: a_goals,
                home_xg: None,
                away_xg: None,
                ht_home_goals: None,
                ht_away_goals: None,
                home_shots: None,
                away_shots: None,
                home_shots_on_target: None,
                away_shots_on_target: None,
                home_corners: None,
                away_corners: None,
                home_fouls: None,
                away_fouls: None,
                home_yellow: None,
                away_yellow: None,
                home_red: None,
                away_red: None,
                home_npxg: None,
                away_npxg: None,
                home_ppda: None,
                away_ppda: None,
                home_deep: None,
                away_deep: None,
                referee: None,
            });
        }
    }

    tracing::info!("Sofascore: sparsowano {} meczów", matches.len());
    Ok(matches)
}

// ─── Understat ───────────────────────────────────────────────────────────────

pub fn parse_understat(path: &Path) -> Result<Vec<NormalizedMatch>> {
    let text = std::fs::read_to_string(path).context("Nie można odczytać Understat JSON")?;
    let root: serde_json::Value = serde_json::from_str(&text).context("Błąd parsowania Understat JSON")?;

    let teams = root.get("teams")
        .and_then(|t| t.as_object())
        .context("Brak klucza 'teams' w JSON Understat")?;

    // Krok 1: Zbierz dane z każdego team → ich mecze z datą i scored/missed/h_a
    struct TeamEntry {
        title: String,
        date: String,
        h_a: String,     // "h" lub "a"
        scored: Option<u8>,
        missed: Option<u8>,
        xg: Option<f64>,
        xga: Option<f64>,
        npxg: Option<f64>,
        npxga: Option<f64>,
        ppda: Option<f64>,
        deep: Option<u8>,
    }

    let mut all_entries: Vec<TeamEntry> = Vec::new();

    for (_team_id, team_data) in teams {
        let team_title = team_data.get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let history = team_data.get("history")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for h in &history {
            let date = h.get("date").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let date_short = if date.len() >= 10 { date[..10].to_string() } else { date.clone() };
            let h_a = h.get("h_a").and_then(|v| v.as_str()).unwrap_or("").to_string();

            let scored = h.get("scored").and_then(|v| {
                v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            }).map(|v| v as u8);
            let missed = h.get("missed").and_then(|v| {
                v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            }).map(|v| v as u8);
            let xg = h.get("xG").and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
            let xga = h.get("xGA").and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
            let npxg = h.get("npxG").and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
            let npxga = h.get("npxGA").and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
            let ppda = h.get("ppda").and_then(|v| v.get("att")).and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
            let deep = h.get("deep").and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))).map(|v| v as u8);

            all_entries.push(TeamEntry {
                title: team_title.clone(),
                date: date_short,
                h_a,
                scored,
                missed,
                xg,
                xga,
                npxg,
                npxga,
                ppda,
                deep,
            });
        }
    }

    // Krok 2: Buduj mapę (date, "a") → Vec<TeamEntry> dla szybkiego wyszukiwania przeciwników
    use std::collections::HashMap;
    let mut away_by_date: HashMap<String, Vec<&TeamEntry>> = HashMap::new();
    for entry in &all_entries {
        if entry.h_a == "a" {
            away_by_date.entry(entry.date.clone()).or_default().push(entry);
        }
    }

    // Krok 3: Dla każdego home entry — znajdź drużynę away na tę samą datę (scores matching)
    let mut matches = Vec::new();
    let mut seen = HashSet::new();

    for entry in &all_entries {
        if entry.h_a != "h" {
            continue;
        }

        let key = format!("{}_{}", entry.date, entry.title);
        if seen.contains(&key) {
            continue;
        }

        // Znajdź przeciwnika: away entry z tą samą datą, scored==missed home, missed==scored home
        let mut opponent_title = String::new();
        let mut away_xg: Option<f64> = None;
        let mut away_xga: Option<f64> = None;
        let mut away_npxg: Option<f64> = None;
        let mut away_npxga: Option<f64> = None;
        let mut away_ppda: Option<f64> = None;
        let mut away_deep: Option<u8> = None;

        if let Some(away_entries) = away_by_date.get(&entry.date) {
            for away in away_entries {
                // Wynik musi się zgadzać: home scored = away missed, home missed = away scored
                let scores_match = entry.scored == away.missed && entry.missed == away.scored;
                if scores_match && away.title != entry.title {
                    opponent_title = away.title.clone();
                    away_xg = away.xg;
                    away_xga = away.xga;
                    away_npxg = away.npxg;
                    away_npxga = away.npxga;
                    away_ppda = away.ppda;
                    away_deep = away.deep;
                    break;
                }
            }
        }

        if opponent_title.is_empty() {
            // Fallback: jeśli nie znalazł po score — bierz jedyną away entry na tę datę
            if let Some(away_entries) = away_by_date.get(&entry.date) {
                let non_self: Vec<&&TeamEntry> = away_entries.iter()
                    .filter(|a| a.title != entry.title)
                    .collect();
                if non_self.len() == 1 {
                    opponent_title = non_self[0].title.clone();
                    away_xg = non_self[0].xg;
                    away_xga = non_self[0].xga;
                    away_npxg = non_self[0].npxg;
                    away_npxga = non_self[0].npxga;
                    away_ppda = non_self[0].ppda;
                    away_deep = non_self[0].deep;
                }
            }
        }

        if opponent_title.is_empty() {
            tracing::warn!("Understat: nie znaleziono przeciwnika dla {} dnia {}", entry.title, entry.date);
            continue;
        }

        seen.insert(key);

        matches.push(NormalizedMatch {
            source: "understat".into(),
            date: entry.date.clone(),
            home_team: entry.title.clone(),
            away_team: opponent_title,
            home_goals: entry.scored,
            away_goals: entry.missed,
            home_xg: entry.xg,
            away_xg,
            ht_home_goals: None,
            ht_away_goals: None,
            home_shots: None,
            away_shots: None,
            home_shots_on_target: None,
            away_shots_on_target: None,
            home_corners: None,
            away_corners: None,
            home_fouls: None,
            away_fouls: None,
            home_yellow: None,
            away_yellow: None,
            home_red: None,
            away_red: None,
            home_npxg: entry.npxg,
            away_npxg: away_npxg,
            home_ppda: entry.ppda,
            away_ppda: away_ppda,
            home_deep: entry.deep,
            away_deep: away_deep,
            referee: None,
        });
    }

    tracing::info!("Understat: sparsowano {} meczów (cross-ref po dacie)", matches.len());
    Ok(matches)
}

// ─── MatchHistory (CSV) ─────────────────────────────────────────────────────

pub fn parse_matchhistory(path: &Path) -> Result<Vec<NormalizedMatch>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .context("Nie można odczytać CSV MatchHistory")?;

    let headers = reader.headers()?.clone();
    let mut matches = Vec::new();

    let col = |name: &str| -> Option<usize> {
        headers.iter().position(|h| h == name)
    };

    let i_home = col("HomeTeam");
    let i_away = col("AwayTeam");
    let i_date = col("Date");
    let i_fthg = col("FTHG");
    let i_ftag = col("FTAG");
    let i_hthg = col("HTHG");
    let i_htag = col("HTAG");
    let i_hs = col("HS");
    let i_as = col("AS");
    let i_hst = col("HST");
    let i_ast = col("AST");
    let i_hc = col("HC");
    let i_ac = col("AC");
    let i_hf = col("HF");
    let i_af = col("AF");
    let i_hy = col("HY");
    let i_ay = col("AY");
    let i_hr = col("HR");
    let i_ar = col("AR");
    let i_referee = col("Referee");

    for result in reader.records() {
        let record = result.context("Błąd wiersza CSV")?;

        let home = i_home.and_then(|i| record.get(i)).unwrap_or("").to_string();
        let away = i_away.and_then(|i| record.get(i)).unwrap_or("").to_string();
        let raw_date = i_date.and_then(|i| record.get(i)).unwrap_or("").to_string();

        let date = if raw_date.contains('/') {
            let parts: Vec<&str> = raw_date.split('/').collect();
            if parts.len() == 3 {
                format!("{}-{}-{}", parts[2], parts[1], parts[0])
            } else {
                raw_date
            }
        } else {
            raw_date
        };

        let parse_u8 = |col_idx: Option<usize>| -> Option<u8> {
            col_idx.and_then(|i| record.get(i)).and_then(|s| s.parse::<u8>().ok())
        };

        let fthg = parse_u8(i_fthg);
        let ftag = parse_u8(i_ftag);
        let hthg = parse_u8(i_hthg);
        let htag = parse_u8(i_htag);
        let hs = parse_u8(i_hs);
        let a_s = parse_u8(i_as);
        let hst = parse_u8(i_hst);
        let ast = parse_u8(i_ast);
        let hc = parse_u8(i_hc);
        let ac = parse_u8(i_ac);
        let hf = parse_u8(i_hf);
        let af = parse_u8(i_af);
        let hy = parse_u8(i_hy);
        let ay = parse_u8(i_ay);
        let hr = parse_u8(i_hr);
        let ar = parse_u8(i_ar);
        let referee = i_referee.and_then(|i| record.get(i)).filter(|s| !s.is_empty()).map(|s| s.to_string());

        if !home.is_empty() && !away.is_empty() {
            matches.push(NormalizedMatch {
                source: "matchhistory".into(),
                date,
                home_team: home,
                away_team: away,
                home_goals: fthg,
                away_goals: ftag,
                home_xg: None,
                away_xg: None,
                ht_home_goals: hthg,
                ht_away_goals: htag,
                home_shots: hs,
                away_shots: a_s,
                home_shots_on_target: hst,
                away_shots_on_target: ast,
                home_corners: hc,
                away_corners: ac,
                home_fouls: hf,
                away_fouls: af,
                home_yellow: hy,
                away_yellow: ay,
                home_red: hr,
                away_red: ar,
                home_npxg: None,
                away_npxg: None,
                home_ppda: None,
                away_ppda: None,
                home_deep: None,
                away_deep: None,
                referee,
            });
        }
    }

    tracing::info!("MatchHistory: sparsowano {} meczów z CSV", matches.len());
    Ok(matches)
}

// ─── Utils ───────────────────────────────────────────────────────────────────

/// Wyciąga datę YYYY-MM-DD z ISO 8601 lub innego formatu
fn extract_date(raw: &str) -> String {
    if raw.len() >= 10 {
        raw[..10].to_string()
    } else {
        raw.to_string()
    }
}
