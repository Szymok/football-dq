const API_BASE_URL = 'http://127.0.0.1:8080';

export async function fetchScorecard() {
  try {
    const res = await fetch(`${API_BASE_URL}/api/dq/summary`);
    if (!res.ok) throw new Error('API Error');
    return await res.json();
  } catch (err) {
    console.error("Error fetching summary:", err);
    return null;
  }
}

export type LinkedMatch = {
  id: number;
  date: string;
  home_team_canonical: string;
  away_team_canonical: string;
  home_goals: number | null;
  away_goals: number | null;
  sources_json: string;
  source_count: number;
  score_agreement: boolean;
  xg_discrepancy: number | null;
};

export type MatchSourceStat = {
  id: number;
  linked_match_id: number;
  source: string;
  home_goals: number | null; away_goals: number | null;
  ht_home_goals: number | null; ht_away_goals: number | null;
  home_xg: number | null; away_xg: number | null;
  home_npxg: number | null; away_npxg: number | null;
  home_shots: number | null; away_shots: number | null;
  home_shots_target: number | null; away_shots_target: number | null;
  home_corners: number | null; away_corners: number | null;
  home_fouls: number | null; away_fouls: number | null;
  home_yellow: number | null; away_yellow: number | null;
  home_red: number | null; away_red: number | null;
  home_ppda: number | null; away_ppda: number | null;
  home_deep: number | null; away_deep: number | null;
  referee: string | null;
};

export type DqSummary = {
  total_matches: number;
  multi_source_matches: number;
  score_agreement_pct: number;
  avg_xg_discrepancy: number | null;
  sources: string[];
};

export async function fetchLinkedMatches(): Promise<LinkedMatch[]> {
  try {
    const res = await fetch(`${API_BASE_URL}/api/linked-matches`);
    if (!res.ok) throw new Error('API Error');
    return await res.json();
  } catch (err) {
    console.error("Error fetching linked matches:", err);
    return [];
  }
}

export async function fetchMatchStats(id: number): Promise<MatchSourceStat[]> {
  try {
    const res = await fetch(`${API_BASE_URL}/api/matches/${id}`);
    if (!res.ok) throw new Error('API Error');
    return await res.json();
  } catch (err) {
    console.error(`Error fetching match stats for ${id}:`, err);
    return [];
  }
}

export async function fetchDqSummary(): Promise<DqSummary | null> {
  try {
    const res = await fetch(`${API_BASE_URL}/api/dq/summary`);
    if (!res.ok) throw new Error('API Error');
    return await res.json();
  } catch (err) {
    console.error("Error fetching DQ summary:", err);
    return null;
  }
}
