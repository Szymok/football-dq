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
  score_agreement: boolean;
  xg_discrepancy: number | null;
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
