"""
Reconciliation Engine – cross-source match linking and data comparison.

Components:
  1. MatchLinker       – links individual matches across sources by date
  2. MatchReconciler   – compares linked matches for score/team discrepancies
  3. StatsReconciler   – compares player stats between sources
  4. CrossSourceDQChecks – DQ checks based on reconciliation data

Key challenge:
  Understat uses numeric team IDs (71, 80, 92...)
  ESPN uses full names (Arsenal, Chelsea...)
  → We link by (date) since both sources have same 380 matches
  → Then we build a team name mapping from linked matches
"""

import logging
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from collections import defaultdict

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.storage.models import Match, PlayerMatchStats, TeamEloRating, DQCheckResult
from src.config.settings import DQ_THRESHOLDS

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _normalize_team(name: str) -> str:
    return (name.lower()
            .replace("fc ", "").replace(" fc", "")
            .replace("afc ", "").replace(" afc", "")
            .replace("'", "").replace("\u2019", "")
            .strip())


def _date_key(dt) -> str:
    """Normalizuje datę do YYYY-MM-DD."""
    if dt is None:
        return "unknown"
    return str(dt)[:10]


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
# MATCH LINKER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MatchLinker:
    """
    Links individual matches across sources.
    
    Strategy: group matches by exact date+time, then match by position
    in each group (same kickoff = same match).
    """

    def __init__(self, session: Session):
        self.session = session

    def link_matches(self) -> pd.DataFrame:
        """
        Łączy mecze z różnych źródeł po dacie.
        
        Returns DataFrame with columns:
          date, source_A, home_A, away_A, score_A,
                source_B, home_B, away_B, score_B,
          status (LINKED, ONLY_A, ONLY_B)
        """
        all_matches = self.session.query(Match).order_by(Match.date).all()

        # Group by exact datetime string
        by_datetime = defaultdict(lambda: defaultdict(list))
        for m in all_matches:
            dt_key = str(m.date) if m.date else "unknown"
            by_datetime[dt_key][m.source].append(m)

        links = []
        sources = sorted(set(m.source for m in all_matches))

        for dt_key, source_groups in by_datetime.items():
            if len(source_groups) < 2:
                # Only one source for this datetime
                for src, matches in source_groups.items():
                    for m in matches:
                        links.append({
                            "date": _date_key(m.date),
                            "datetime": dt_key,
                            "source_a": src,
                            "home_a": m.home_team,
                            "away_a": m.away_team,
                            "score_a": f"{m.home_score}-{m.away_score}" if m.home_score is not None else "—",
                            "source_b": "—",
                            "home_b": "—",
                            "away_b": "—",
                            "score_b": "—",
                            "status": "UNMATCHED",
                        })
                continue

            # Multiple sources → link by position (same kickoff time)
            source_list = list(source_groups.keys())
            primary = source_list[0]
            
            for other in source_list[1:]:
                primary_matches = source_groups[primary]
                other_matches = source_groups[other]

                # Match by index (same position = same match for same kickoff)
                max_len = max(len(primary_matches), len(other_matches))
                for i in range(max_len):
                    pm = primary_matches[i] if i < len(primary_matches) else None
                    om = other_matches[i] if i < len(other_matches) else None

                    if pm and om:
                        # Check score match
                        score_a = f"{pm.home_score}-{pm.away_score}" if pm.home_score is not None else "—"
                        score_b = f"{om.home_score}-{om.away_score}" if om.home_score is not None else "—"
                        
                        if score_a == "—" or score_b == "—":
                            match_status = "LINKED_NO_SCORES"
                        elif score_a == score_b:
                            match_status = "MATCH"
                        else:
                            match_status = "DISCREPANCY"

                        links.append({
                            "date": _date_key(pm.date),
                            "datetime": dt_key,
                            "source_a": primary,
                            "home_a": pm.home_team,
                            "away_a": pm.away_team,
                            "score_a": score_a,
                            "source_b": other,
                            "home_b": om.home_team,
                            "away_b": om.away_team,
                            "score_b": score_b,
                            "status": match_status,
                        })
                    elif pm:
                        links.append({
                            "date": _date_key(pm.date),
                            "datetime": dt_key,
                            "source_a": primary,
                            "home_a": pm.home_team,
                            "away_a": pm.away_team,
                            "score_a": f"{pm.home_score}-{pm.away_score}" if pm.home_score is not None else "—",
                            "source_b": other,
                            "home_b": "—",
                            "away_b": "—",
                            "score_b": "—",
                            "status": "UNMATCHED",
                        })

        return pd.DataFrame(links) if links else pd.DataFrame()

    def build_team_name_map(self) -> pd.DataFrame:
        """
        Buduje mapę nazw drużyn między źródłami.
        
        Jeśli mecz na tym samym datetime jest linkowany (pozycyjnie),
        to home_A odpowiada home_B, away_A odpowiada away_B.
        """
        linked = self.link_matches()
        if linked.empty:
            return pd.DataFrame()

        linked_only = linked[linked["status"].isin(["LINKED_NO_SCORES", "MATCH", "DISCREPANCY"])]

        mapping = []
        seen = set()
        for _, row in linked_only.iterrows():
            # Home team mapping
            key_h = (row["source_a"], row["home_a"], row["source_b"], row["home_b"])
            if key_h not in seen:
                seen.add(key_h)
                mapping.append({
                    "source_a": row["source_a"],
                    "team_a": row["home_a"],
                    "source_b": row["source_b"],
                    "team_b": row["home_b"],
                    "role": "home",
                })
            # Away team mapping
            key_a = (row["source_a"], row["away_a"], row["source_b"], row["away_b"])
            if key_a not in seen:
                seen.add(key_a)
                mapping.append({
                    "source_a": row["source_a"],
                    "team_a": row["away_a"],
                    "source_b": row["source_b"],
                    "team_b": row["away_b"],
                    "role": "away",
                })

        if not mapping:
            return pd.DataFrame()

        df = pd.DataFrame(mapping)
        # Aggregate: find most common mapping per team
        return df.groupby(["source_a", "team_a", "source_b", "team_b"]).size().reset_index(name="occurrences").sort_values("occurrences", ascending=False)

    def get_reconciliation_summary(self) -> dict:
        """Podsumowanie reconciliation."""
        linked = self.link_matches()
        if linked.empty:
            return {"total": 0, "linked": 0, "unmatched": 0, "discrepancies": 0,
                    "match_rate": 0, "linked_no_scores": 0}

        total = len(linked)
        statuses = linked["status"].value_counts().to_dict()

        return {
            "total": total,
            "linked": statuses.get("MATCH", 0) + statuses.get("LINKED_NO_SCORES", 0) + statuses.get("DISCREPANCY", 0),
            "match": statuses.get("MATCH", 0),
            "linked_no_scores": statuses.get("LINKED_NO_SCORES", 0),
            "discrepancies": statuses.get("DISCREPANCY", 0),
            "unmatched": statuses.get("UNMATCHED", 0),
            "match_rate": round(
                (statuses.get("MATCH", 0) + statuses.get("LINKED_NO_SCORES", 0) + statuses.get("DISCREPANCY", 0))
                / total * 100, 1
            ) if total > 0 else 0,
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MATCH RECONCILER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MatchReconciler:
    """Porównuje wyniki meczów między źródłami."""

    def __init__(self, session: Session):
        self.session = session

    def get_source_summary(self) -> pd.DataFrame:
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

    def get_coverage_matrix(self) -> pd.DataFrame:
        rows = self.session.query(
            Match.home_team, Match.source, func.count(Match.id).label("cnt"),
        ).group_by(Match.home_team, Match.source).all()

        data = [{"team": r.home_team, "source": r.source, "matches": r.cnt} for r in rows]
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
        stats = self.session.query(
            PlayerMatchStats.player_name, PlayerMatchStats.team,
            PlayerMatchStats.source, PlayerMatchStats.goals,
            PlayerMatchStats.assists, PlayerMatchStats.xg,
            PlayerMatchStats.xg_assist, PlayerMatchStats.minutes,
            PlayerMatchStats.shots,
        ).all()

        if not stats:
            return pd.DataFrame()

        return pd.DataFrame([{
            "player": s.player_name, "team": s.team, "source": s.source,
            "goals": s.goals, "assists": s.assists, "xg": s.xg,
            "xg_assist": s.xg_assist, "minutes": s.minutes, "shots": s.shots,
        } for s in stats])

    def get_cross_source_comparison(self) -> pd.DataFrame:
        df = self.compare_player_stats()
        if df.empty:
            return df

        sources = df["source"].unique()
        if len(sources) < 2:
            return pd.DataFrame()

        fbref = df[df["source"] == "fbref"].set_index("player")
        understat = df[df["source"] == "understat"].set_index("player")

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
                    "match_score": round(best_score, 2),
                })

        return pd.DataFrame(result) if result else pd.DataFrame()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DQ CHECKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CrossSourceDQChecks:
    """DQ checks based on cross-source reconciliation."""

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
        """Czy źródła mają podobną liczbę meczów."""
        counts = self.session.query(
            Match.source, func.count(Match.id).label("cnt")
        ).group_by(Match.source).all()

        if len(counts) < 2:
            self._record("match_count_balance", "consistency", True,
                         details="Mniej niż 2 źródła z meczami")
            return

        vals = [c.cnt for c in counts]
        ratio = round(min(vals) / max(vals) * 100, 1) if max(vals) > 0 else 0
        self._record(
            "match_count_balance", "consistency", passed=ratio >= 70,
            value=ratio, threshold=70.0,
            details=f"Min/max ratio: {min(vals)}/{max(vals)} = {ratio}% | {', '.join(f'{c.source}={c.cnt}' for c in counts)}",
        )

    def check_match_linking_rate(self):
        """Jaki % meczów udało się zlinkować między źródłami."""
        linker = MatchLinker(self.session)
        summary = linker.get_reconciliation_summary()

        total = summary["total"]
        linked = summary["linked"]
        rate = summary["match_rate"]

        self._record(
            "match_linking_rate", "consistency",
            passed=rate >= 60,
            value=rate, threshold=60.0,
            details=f"{linked}/{total} matches linked ({rate}%) | discrepancies: {summary['discrepancies']}",
        )

    def check_team_name_overlap(self):
        """% pokrycia nazw drużyn między źródłami."""
        sources = [r[0] for r in self.session.query(Match.source.distinct()).all()]
        if len(sources) < 2:
            return

        teams_per_source = {}
        for src in sources:
            teams = set(_normalize_team(r[0]) for r in self.session.query(
                Match.home_team.distinct()
            ).filter(Match.source == src).all())
            teams_per_source[src] = teams

        all_teams = set()
        for t in teams_per_source.values():
            all_teams |= t

        common = sum(1 for team in all_teams
                     if sum(1 for s in sources if team in teams_per_source.get(s, set())) >= 2)

        overlap_pct = round(common / len(all_teams) * 100, 1) if all_teams else 0
        self._record(
            "team_name_overlap", "consistency", passed=overlap_pct >= 50,
            value=overlap_pct, threshold=50.0,
            details=f"{common}/{len(all_teams)} teams in ≥2 sources ({overlap_pct}%)",
        )

    def check_elo_range(self):
        """Elo ratings w sensownym zakresie."""
        total = self.session.query(func.count(TeamEloRating.id)).scalar()
        if total == 0:
            return

        out = self.session.query(func.count(TeamEloRating.id)).filter(
            (TeamEloRating.elo < 800) | (TeamEloRating.elo > 2200)
        ).scalar()
        self._record(
            "elo_range_check", "validity", passed=out == 0,
            value=float(out), threshold=0.0,
            details=f"{out}/{total} Elo ratings outside [800, 2200]",
        )

    def check_score_completeness(self):
        """Jaki % meczów ma wypełnione wyniki."""
        total = self.session.query(func.count(Match.id)).scalar()
        with_scores = self.session.query(func.count(Match.id)).filter(
            Match.home_score.isnot(None), Match.away_score.isnot(None)
        ).scalar()

        pct = round(with_scores / total * 100, 1) if total > 0 else 0
        self._record(
            "match_score_completeness", "completeness",
            passed=pct >= 50,
            value=pct, threshold=50.0,
            details=f"{with_scores}/{total} matches have scores ({pct}%)",
        )

    def run_all(self) -> list[DQCheckResult]:
        logger.info("── Cross-source DQ checks ──")
        self.check_match_count_balance()
        self.check_match_linking_rate()
        self.check_team_name_overlap()
        self.check_elo_range()
        self.check_score_completeness()

        for r in self.results:
            self.session.add(r)
        self.session.commit()

        logger.info(f"Zapisano {len(self.results)} cross-source DQ results.")
        return self.results
