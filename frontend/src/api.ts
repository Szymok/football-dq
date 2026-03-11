const API_BASE_URL = 'http://127.0.0.1:8000/api/v1';

export async function fetchScorecard() {
  try {
    const res = await fetch(`${API_BASE_URL}/quality/scorecard`);
    if (!res.ok) throw new Error('API Error');
    return await res.json();
  } catch (err) {
    console.error("Error fetching run:", err);
    return null;
  }
}
