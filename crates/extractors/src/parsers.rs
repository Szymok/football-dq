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

    let mut seen = HashSet::new();
    let mut matches = Vec::new();

    for (_team_id, team_data) in teams {
        let team_title = team_data.get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let history = team_data.get("history")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for h in &history {
            let date = h.get("date").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let date_short = if date.len() >= 10 { date[..10].to_string() } else { date.clone() };

            let is_home = h.get("h_a").and_then(|v| v.as_str()) == Some("h");
            let opponent = h.get("oppTitle").and_then(|v| v.as_str()).unwrap_or("");

            let (home, away) = if is_home {
                (team_title.to_string(), opponent.to_string())
            } else {
                (opponent.to_string(), team_title.to_string())
            };

            // Deduplikacja — każdy mecz występuje 2x (raz per drużyna)
            let key = format!("{}_{}_{}", date_short, &home, &away);
            if seen.contains(&key) {
                continue;
            }
            seen.insert(key);

            let xg = h.get("xG").and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
            let xga = h.get("xGA").and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
            let scored = h.get("scored").and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))).map(|v| v as u8);
            let missed = h.get("missed").and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))).map(|v| v as u8);

            let (home_goals, away_goals, home_xg, away_xg) = if is_home {
                (scored, missed, xg, xga)
            } else {
                (missed, scored, xga, xg)
            };

            matches.push(NormalizedMatch {
                source: "understat".into(),
                date: date_short,
                home_team: home,
                away_team: away,
                home_goals,
                away_goals,
                home_xg,
                away_xg,
            });
        }
    }

    tracing::info!("Understat: sparsowano {} meczów (po deduplikacji)", matches.len());
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

    for result in reader.records() {
        let record = result.context("Błąd wiersza CSV")?;

        let home = i_home.and_then(|i| record.get(i)).unwrap_or("").to_string();
        let away = i_away.and_then(|i| record.get(i)).unwrap_or("").to_string();
        let raw_date = i_date.and_then(|i| record.get(i)).unwrap_or("").to_string();

        // Format daty "DD/MM/YYYY" → "YYYY-MM-DD"
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

        let fthg = i_fthg.and_then(|i| record.get(i)).and_then(|s| s.parse::<u8>().ok());
        let ftag = i_ftag.and_then(|i| record.get(i)).and_then(|s| s.parse::<u8>().ok());

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
