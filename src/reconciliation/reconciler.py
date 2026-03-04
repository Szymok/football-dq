"""
Reconciliation Engine – porównanie danych między źródłami.

Moduły:
  1. Match Reconciler – porównanie wyników meczów z różnych źródeł
  2. Stats Reconciler – porównanie statystyk zawodników między źródłami

Wyniki reconciliation zapisywane do tabel w SQLite
i wykorzystywane przez DQ checks + dashboard.
"""

import logging
from datetime import datetime
from difflib import SequenceMatcher

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.storage.models import Match, PlayerMatchStats, TeamEloRating, DQCheckResult
from src.config.settings import DQ_THRESHOLDS

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _fuzzy_match(name_a: str, name_b: str, threshold: float = 0.82) -> bool:
    return SequenceMatcher(None, name_a.lower(), name_b.lower()).ratio() >= threshold


def _normalize_team(name: str) -> str:
    """Upraszcza nazwę drużyny do porównania."""
    return (name.lower()
            .replace("fc ", "").replace(" fc", "")
            .replace("afc ", "").replace(" afc", "")
            .replace("'", "").replace("'", "")
            .strip())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MATCH RECONCILER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MatchReconciler:
    """Porównuje wyniki meczów między źródłami."""

    def __init__(self, session: Session):
        self.session = session

    def get_source_summary(self) -> pd.DataFrame:
        """Zwraca podsumowanie per źródło: ile meczów, zakres dat, unikalne drużyny."""
        rows = self.session.query(
            Match.source,
            func.count(Match.id).label("match_count"),
            func.min(Match.date).label("earliest"),
            func.max(Match.date).label("latest"),
            func.count(func.distinct(Match.home_team)).label("home_teams"),
        ).group_by(Match.source).all()

        data = []
        for r in rows:
            data.append({
                "source": r.source,
                "match_count": r.match_count,
                "earliest": str(r.earliest)[:10] if r.earliest else "—",
                "latest": str(r.latest)[:10] if r.latest else "—",
                "teams": r.home_teams,
            })
        return pd.DataFrame(data)

    def find_score_discrepancies(self) -> pd.DataFrame:
        """Szuka meczów z różnymi wynikami między źródłami."""
        matches = self.session.query(Match).filter(
            Match.home_score.isnot(None),
            Match.away_score.isnot(None),
        ).all()

        # Grupuj po (home_norm, away_norm, date)
        grouped = {}
        for m in matches:
            home_n = _normalize_team(m.home_team)
            away_n = _normalize_team(m.away_team)
            date_str = str(m.date)[:10] if m.date else "unknown"
            key = (home_n, away_n, date_str)

            if key not in grouped:
                grouped[key] = []
            grouped[key].append(m)

        discrepancies = []
        for key, group in grouped.items():
            if len(group) < 2:
                continue
            scores = set((m.home_score, m.away_score) for m in group)
            if len(scores) > 1:
                sources = ", ".join(f"{m.source}({m.home_score}-{m.away_score})" for m in group)
                discrepancies.append({
                    "home": group[0].home_team,
                    "away": group[0].away_team,
                    "date": key[2],
                    "discrepancy": sources,
                    "sources_count": len(group),
                })

        return pd.DataFrame(discrepancies) if discrepancies else pd.DataFrame()

    def get_coverage_matrix(self) -> pd.DataFrame:
        """Macierz pokrycia: drużyna × źródło → ile meczów."""
        rows = self.session.query(
            Match.home_team,
            Match.source,
            func.count(Match.id).label("cnt"),
        ).group_by(Match.home_team, Match.source).all()

        data = []
        for r in rows:
            data.append({"team": r.home_team, "source": r.source, "matches": r.cnt})
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        return df.pivot_table(index="team", columns="source", values="matches", fill_value=0).reset_index()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STATS RECONCILER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class StatsReconciler:
    """Porównuje statystyki zawodników między źródłami."""

    def __init__(self, session: Session):
        self.session = session

    def compare_player_stats(self) -> pd.DataFrame:
        """Porównanie xG, goals, assists per zawodnik między FBref i Understat."""
        stats = self.session.query(
            PlayerMatchStats.player_name,
            PlayerMatchStats.team,
            PlayerMatchStats.source,
            PlayerMatchStats.goals,
            PlayerMatchStats.assists,
            PlayerMatchStats.xg,
            PlayerMatchStats.xg_assist,
            PlayerMatchStats.minutes,
            PlayerMatchStats.shots,
        ).all()

        if not stats:
            return pd.DataFrame()

        df = pd.DataFrame([{
            "player": s.player_name,
            "team": s.team,
            "source": s.source,
            "goals": s.goals,
            "assists": s.assists,
            "xg": s.xg,
            "xg_assist": s.xg_assist,
            "minutes": s.minutes,
            "shots": s.shots,
        } for s in stats])

        return df

    def get_cross_source_comparison(self) -> pd.DataFrame:
        """Side-by-side porównanie statystyk per zawodnik – FBref vs Understat."""
        df = self.compare_player_stats()
        if df.empty:
            return df

        sources = df["source"].unique()
        if len(sources) < 2:
            return pd.DataFrame()

        # Pivot: zawodnik × (source → metrics)
        fbref = df[df["source"] == "fbref"].set_index("player")
        understat = df[df["source"] == "understat"].set_index("player")

        # Fuzzy match players
        result = []
        matched_understat = set()

        for fb_name in fbref.index:
            best_match = None
            best_score = 0
            for un_name in understat.index:
                if un_name in matched_understat:
                    continue
                score = SequenceMatcher(None, fb_name.lower(), un_name.lower()).ratio()
                if score > best_score:
                    best_score = score
                    best_match = un_name

            if best_match and best_score >= 0.80:
                matched_understat.add(best_match)
                fb = fbref.loc[fb_name]
                un = understat.loc[best_match]
                result.append({
                    "player": fb_name,
                    "team": fb.get("team", ""),
                    "fbref_goals": fb.get("goals"),
                    "understat_goals": un.get("goals"),
                    "goals_delta": _safe_delta(fb.get("goals"), un.get("goals")),
                    "fbref_xg": fb.get("xg"),
                    "understat_xg": un.get("xg"),
                    "xg_delta": _safe_delta_float(fb.get("xg"), un.get("xg")),
                    "fbref_assists": fb.get("assists"),
                    "understat_assists": un.get("assists"),
                    "assists_delta": _safe_delta(fb.get("assists"), un.get("assists")),
                    "fbref_minutes": fb.get("minutes"),
                    "understat_minutes": un.get("minutes"),
                    "match_score": round(best_score, 2),
                })

        return pd.DataFrame(result) if result else pd.DataFrame()

    def get_source_stats_summary(self) -> pd.DataFrame:
        """Agregowane statystyki per źródło."""
        df = self.compare_player_stats()
        if df.empty:
            return df

        summary = df.groupby("source").agg({
            "player": "nunique",
            "goals": ["sum", "mean"],
            "xg": ["sum", "mean"],
            "assists": ["sum", "mean"],
            "minutes": ["sum", "mean"],
        }).reset_index()

        summary.columns = [
            "source", "players", "total_goals", "avg_goals",
            "total_xg", "avg_xg", "total_assists", "avg_assists",
            "total_minutes", "avg_minutes",
        ]
        return summary


def _safe_delta(a, b) -> int | None:
    try:
        if a is None or b is None:
            return None
        return int(a) - int(b)
    except (ValueError, TypeError):
        return None


def _safe_delta_float(a, b) -> float | None:
    try:
        if a is None or b is None:
            return None
        return round(float(a) - float(b), 2)
    except (ValueError, TypeError):
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DQ CHECKS (nowe sprawdzenia cross-source)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CrossSourceDQChecks:
    """Dodatkowe DQ checks oparte na reconciliation."""

    def __init__(self, session: Session):
        self.session = session
        self.results: list[DQCheckResult] = []
        self.run_time = datetime.utcnow()

    def _record(self, check_name, dimension, passed, value=None, threshold=None, details=None):
        result = DQCheckResult(
            check_name=check_name, dimension=dimension, passed=passed,
            value=value, threshold=threshold, details=details,
            run_at=self.run_time,
        )
        self.results.append(result)
        logger.info(f"  {'PASS' if passed else 'FAIL'} [{dimension}] {check_name}: {details}")

    def check_match_count_balance(self):
        """Sprawdza czy źródła mają zbliżoną liczbę meczów."""
        counts = self.session.query(
            Match.source, func.count(Match.id).label("cnt")
        ).group_by(Match.source).all()

        if len(counts) < 2:
            self._record("match_count_balance", "consistency", True,
                         details="Mniej niż 2 źródła z meczami")
            return

        vals = [c.cnt for c in counts]
        max_val = max(vals)
        min_val = min(vals)
        ratio = round(min_val / max_val * 100, 1) if max_val > 0 else 0

        self._record(
            "match_count_balance", "consistency",
            passed=ratio >= 70,
            value=ratio, threshold=70.0,
            details=f"Min/max ratio: {min_val}/{max_val} = {ratio}% | {', '.join(f'{c.source}={c.cnt}' for c in counts)}",
        )

    def check_team_name_overlap(self):
        """Sprawdza % pokrycia nazw drużyn między źródłami."""
        sources = [r[0] for r in self.session.query(Match.source.distinct()).all()]
        if len(sources) < 2:
            return

        teams_per_source = {}
        for src in sources:
            teams = set(_normalize_team(r[0]) for r in self.session.query(
                Match.home_team.distinct()
            ).filter(Match.source == src).all())
            teams_per_source[src] = teams

        # Porównanie parami
        all_teams = set()
        for t in teams_per_source.values():
            all_teams |= t

        # % drużyn wspólnych dla >= 2 źródeł
        common = 0
        for team in all_teams:
            in_sources = sum(1 for s in sources if team in teams_per_source.get(s, set()))
            if in_sources >= 2:
                common += 1

        overlap_pct = round(common / len(all_teams) * 100, 1) if all_teams else 0

        self._record(
            "team_name_overlap", "consistency",
            passed=overlap_pct >= 50,
            value=overlap_pct, threshold=50.0,
            details=f"{common}/{len(all_teams)} teams in ≥2 sources ({overlap_pct}%)",
        )

    def check_elo_range(self):
        """Sprawdza czy Elo ratings są w sensownym zakresie."""
        total = self.session.query(func.count(TeamEloRating.id)).scalar()
        if total == 0:
            return

        out = self.session.query(func.count(TeamEloRating.id)).filter(
            (TeamEloRating.elo < 800) | (TeamEloRating.elo > 2200)
        ).scalar()

        self._record(
            "elo_range_check", "validity",
            passed=out == 0,
            value=float(out), threshold=0.0,
            details=f"{out}/{total} Elo ratings outside [800, 2200]",
        )

    def run_all(self) -> list[DQCheckResult]:
        """Uruchamia wszystkie cross-source checks."""
        logger.info("── Cross-source DQ checks ──")
        self.check_match_count_balance()
        self.check_team_name_overlap()
        self.check_elo_range()

        for r in self.results:
            self.session.add(r)
        self.session.commit()

        logger.info(f"Zapisano {len(self.results)} cross-source DQ results.")
        return self.results
