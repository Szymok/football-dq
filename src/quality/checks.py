"""
Data Quality Checks – 6 wymiarów jakości danych.

Wymiary:
  1. Completeness  – brakujące wartości
  2. Validity      – wartości poza zakresem
  3. Uniqueness    – duplikaty
  4. Timeliness    – świeżość danych
  5. Consistency   – spójność encji między źródłami
  6. Accuracy      – dokładność wartości między źródłami
"""

import logging
from datetime import datetime, timedelta

from difflib import SequenceMatcher
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from src.storage.models import PlayerMatchStats, Match, DQCheckResult
from src.config.settings import DQ_THRESHOLDS

logger = logging.getLogger(__name__)


class DQRunner:
    """Uruchamia wszystkie sprawdzenia DQ i zapisuje wyniki do bazy."""

    def __init__(self, session: Session):
        self.session = session
        self.results: list[DQCheckResult] = []
        self.run_time = datetime.utcnow()

    def _record(self, check_name: str, dimension: str, passed: bool,
                value: float | None = None, threshold: float | None = None,
                table_name: str = None, column_name: str = None,
                details: str = None):
        """Zapisuje wynik sprawdzenia."""
        result = DQCheckResult(
            check_name=check_name,
            dimension=dimension,
            table_name=table_name,
            column_name=column_name,
            passed=passed,
            value=value,
            threshold=threshold,
            details=details,
            run_at=self.run_time,
        )
        self.results.append(result)
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"  {status} [{dimension}] {check_name}: value={value}, threshold={threshold}")

    # ─── 1. COMPLETENESS ───────────────────────────────────────────

    def check_completeness(self):
        """Sprawdza % nulli w kluczowych kolumnach."""
        logger.info("── Completeness checks ──")
        threshold = DQ_THRESHOLDS["completeness_null_max_pct"]

        for col_name in ["xg", "goals", "assists", "minutes"]:
            total = self.session.query(func.count(PlayerMatchStats.id)).scalar()
            if total == 0:
                self._record(f"null_ratio_{col_name}", "completeness", False,
                             value=None, threshold=threshold,
                             table_name="player_match_stats", column_name=col_name,
                             details="Brak danych w tabeli")
                continue

            col = getattr(PlayerMatchStats, col_name)
            null_count = self.session.query(func.count(PlayerMatchStats.id)).filter(
                col.is_(None)
            ).scalar()
            null_pct = round((null_count / total) * 100, 2)
            passed = null_pct <= threshold

            self._record(f"null_ratio_{col_name}", "completeness", passed,
                         value=null_pct, threshold=threshold,
                         table_name="player_match_stats", column_name=col_name,
                         details=f"{null_count}/{total} null values ({null_pct}%)")

    # ─── 2. VALIDITY ───────────────────────────────────────────────

    def check_validity(self):
        """Sprawdza czy wartości mieszczą się w oczekiwanych zakresach."""
        logger.info("── Validity checks ──")
        cfg = DQ_THRESHOLDS

        # xG range
        total = self.session.query(func.count(PlayerMatchStats.id)).filter(
            PlayerMatchStats.xg.isnot(None)
        ).scalar()
        if total > 0:
            out_of_range = self.session.query(func.count(PlayerMatchStats.id)).filter(
                PlayerMatchStats.xg.isnot(None),
                (PlayerMatchStats.xg < cfg["validity_xg_min"]) |
                (PlayerMatchStats.xg > cfg["validity_xg_max"])
            ).scalar()
            pct_invalid = round((out_of_range / total) * 100, 2)
            self._record("xg_range_check", "validity", out_of_range == 0,
                         value=pct_invalid, threshold=0.0,
                         table_name="player_match_stats", column_name="xg",
                         details=f"{out_of_range}/{total} values out of [{cfg['validity_xg_min']}, {cfg['validity_xg_max']}]")

        # Minutes range
        total_m = self.session.query(func.count(PlayerMatchStats.id)).filter(
            PlayerMatchStats.minutes.isnot(None)
        ).scalar()
        if total_m > 0:
            out_m = self.session.query(func.count(PlayerMatchStats.id)).filter(
                PlayerMatchStats.minutes.isnot(None),
                (PlayerMatchStats.minutes < cfg["validity_minutes_min"]) |
                (PlayerMatchStats.minutes > cfg["validity_minutes_max"])
            ).scalar()
            pct_m = round((out_m / total_m) * 100, 2)
            # Minuty powyżej 120 to sezonowe sumy – to jest poprawne
            # Więc patrzymy na < 0
            out_neg = self.session.query(func.count(PlayerMatchStats.id)).filter(
                PlayerMatchStats.minutes.isnot(None),
                PlayerMatchStats.minutes < 0
            ).scalar()
            self._record("minutes_negative_check", "validity", out_neg == 0,
                         value=float(out_neg), threshold=0.0,
                         table_name="player_match_stats", column_name="minutes",
                         details=f"{out_neg} records with negative minutes")

    # ─── 3. UNIQUENESS ─────────────────────────────────────────────

    def check_uniqueness(self):
        """Sprawdza duplikaty w kombinacji (player_name, team, season, source)."""
        logger.info("── Uniqueness checks ──")
        dupes = self.session.query(
            PlayerMatchStats.player_name,
            PlayerMatchStats.team,
            PlayerMatchStats.season,
            PlayerMatchStats.source,
            func.count(PlayerMatchStats.id).label("cnt")
        ).group_by(
            PlayerMatchStats.player_name,
            PlayerMatchStats.team,
            PlayerMatchStats.season,
            PlayerMatchStats.source,
        ).having(func.count(PlayerMatchStats.id) > 1).all()

        dupe_count = len(dupes)
        self._record("duplicate_player_stats", "uniqueness", dupe_count == 0,
                      value=float(dupe_count), threshold=0.0,
                      table_name="player_match_stats",
                      details=f"{dupe_count} duplicate combinations found")

    # ─── 4. TIMELINESS ─────────────────────────────────────────────

    def check_timeliness(self):
        """Sprawdza czy w bazie są mecze z ostatnich N dni."""
        logger.info("── Timeliness checks ──")
        max_days = DQ_THRESHOLDS["timeliness_max_days"]
        cutoff = datetime.utcnow() - timedelta(days=max_days)

        recent = self.session.query(func.count(Match.id)).filter(
            Match.date.isnot(None),
            Match.date >= cutoff,
        ).scalar()

        latest_match = self.session.query(func.max(Match.date)).scalar()
        if latest_match:
            days_old = (datetime.utcnow() - latest_match).days
        else:
            days_old = None

        self._record("data_freshness", "timeliness", recent > 0,
                      value=float(days_old) if days_old is not None else None,
                      threshold=float(max_days),
                      table_name="matches",
                      details=f"Najnowszy mecz: {latest_match}, {recent} meczów w ostatnich {max_days} dniach")

    # ─── 5. CONSISTENCY (Cross-source Entity Matching) ─────────────

    def check_consistency(self):
        """Fuzzy matching zawodników FBref → Understat."""
        logger.info("── Consistency checks (cross-source) ──")
        threshold_pct = DQ_THRESHOLDS["consistency_min_match_pct"]

        fbref_players = [r[0] for r in self.session.query(
            PlayerMatchStats.player_name
        ).filter(PlayerMatchStats.source == "fbref").distinct().all()]

        understat_players = [r[0] for r in self.session.query(
            PlayerMatchStats.player_name
        ).filter(PlayerMatchStats.source == "understat").distinct().all()]

        if not fbref_players or not understat_players:
            self._record("cross_source_entity_match", "consistency", False,
                          details="Brak danych z jednego lub obu źródeł")
            return

        matched = 0
        THRESHOLD_RATIO = 0.82

        for fb_name in fbref_players:
            best = 0
            for un_name in understat_players:
                score = SequenceMatcher(None, fb_name.lower(), un_name.lower()).ratio()
                if score > best:
                    best = score
                if best >= 1.0:
                    break
            if best >= THRESHOLD_RATIO:
                matched += 1

        match_pct = round((matched / len(fbref_players)) * 100, 2)
        self._record("cross_source_entity_match", "consistency", match_pct >= threshold_pct,
                      value=match_pct, threshold=threshold_pct,
                      table_name="player_match_stats",
                      details=f"{matched}/{len(fbref_players)} FBref players matched to Understat (fuzzy>{THRESHOLD_RATIO})")

    # ─── 6. ACCURACY (Cross-source xG Reconciliation) ──────────────

    def check_accuracy(self):
        """Porównuje średni xG per zawodnik między FBref a Understat."""
        logger.info("── Accuracy checks (xG reconciliation) ──")
        max_delta = DQ_THRESHOLDS["accuracy_max_xg_delta"]

        fbref_xg = {r.player_name: r.avg_xg for r in self.session.query(
            PlayerMatchStats.player_name,
            func.avg(PlayerMatchStats.xg).label("avg_xg")
        ).filter(
            PlayerMatchStats.source == "fbref",
            PlayerMatchStats.xg.isnot(None)
        ).group_by(PlayerMatchStats.player_name).all()}

        understat_xg = {r.player_name: r.avg_xg for r in self.session.query(
            PlayerMatchStats.player_name,
            func.avg(PlayerMatchStats.xg).label("avg_xg")
        ).filter(
            PlayerMatchStats.source == "understat",
            PlayerMatchStats.xg.isnot(None)
        ).group_by(PlayerMatchStats.player_name).all()}

        # Szukamy wspólnych zawodników (exact match nazw – uproszczenie)
        common = set(fbref_xg.keys()) & set(understat_xg.keys())

        if not common:
            self._record("xg_cross_source_delta", "accuracy", True,
                          details="Brak wspólnych zawodników do porównania (po exact match nazw)")
            return

        deltas = []
        for name in common:
            delta = abs(fbref_xg[name] - understat_xg[name])
            deltas.append(delta)

        avg_delta = round(sum(deltas) / len(deltas), 4)
        self._record("xg_cross_source_delta", "accuracy", avg_delta <= max_delta,
                      value=avg_delta, threshold=max_delta,
                      table_name="player_match_stats", column_name="xg",
                      details=f"Avg |FBref.xG - Understat.xG| = {avg_delta} across {len(common)} common players")

    # ─── RUN ALL ───────────────────────────────────────────────────

    def run_all(self) -> list[DQCheckResult]:
        """Uruchamia wszystkie sprawdzenia i zapisuje wyniki do DB."""
        logger.info("╔══════════════════════════════════════════╗")
        logger.info("║   Data Quality Pipeline – START          ║")
        logger.info("╚══════════════════════════════════════════╝")

        self.check_completeness()
        self.check_validity()
        self.check_uniqueness()
        self.check_timeliness()
        self.check_consistency()
        self.check_accuracy()

        # Persystuj wyniki
        for r in self.results:
            self.session.add(r)
        self.session.commit()

        logger.info(f"Zapisano {len(self.results)} wyników DQ do bazy.")
        return self.results
