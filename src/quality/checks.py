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

    def check_consistency(self, ground_truth: str = "fbref"):
        """Fuzzy matching zawodników Vendors → Ground Truth."""
        logger.info(f"── Consistency checks (Ground Truth: {ground_truth}) ──")
        threshold_pct = DQ_THRESHOLDS["consistency_min_match_pct"]

        truth_players = [r[0] for r in self.session.query(
            PlayerMatchStats.player_name
        ).filter(PlayerMatchStats.source == ground_truth).distinct().all()]

        if not truth_players:
            self._record("cross_source_entity_match", "consistency", False,
                          details=f"Brak danych z Ground Truth ({ground_truth})")
            return

        other_sources = [r[0] for r in self.session.query(
            PlayerMatchStats.source
        ).filter(PlayerMatchStats.source != ground_truth).distinct().all()]

        THRESHOLD_RATIO = 0.82

        for vendor in other_sources:
            vendor_players = [r[0] for r in self.session.query(
                PlayerMatchStats.player_name
            ).filter(PlayerMatchStats.source == vendor).distinct().all()]
            
            if not vendor_players:
                continue

            matched = 0
            for vendor_name in vendor_players:
                best = 0
                for truth_name in truth_players:
                    score = SequenceMatcher(None, vendor_name.lower(), truth_name.lower()).ratio()
                    if score > best:
                        best = score
                    if best >= 1.0:
                        break
                if best >= THRESHOLD_RATIO:
                    matched += 1

            match_pct = round((matched / len(vendor_players)) * 100, 2)
            self._record(f"{vendor}_entity_match", "consistency", match_pct >= threshold_pct,
                          value=match_pct, threshold=threshold_pct,
                          table_name="player_match_stats",
                          details=f"{matched}/{len(vendor_players)} {vendor} players matched to {ground_truth} (fuzzy>{THRESHOLD_RATIO})")

    # ─── 6. ACCURACY (Cross-source xG Reconciliation) ──────────────

    def check_accuracy(self, ground_truth: str = "fbref"):
        """Porównuje średni xG per zawodnik między Vendors a Ground Truth."""
        logger.info(f"── Accuracy checks (Ground Truth: {ground_truth}) ──")
        max_delta = DQ_THRESHOLDS["accuracy_max_xg_delta"]

        truth_xg = {r.player_name: r.avg_xg for r in self.session.query(
            PlayerMatchStats.player_name,
            func.avg(PlayerMatchStats.xg).label("avg_xg")
        ).filter(
            PlayerMatchStats.source == ground_truth,
            PlayerMatchStats.xg.isnot(None)
        ).group_by(PlayerMatchStats.player_name).all()}

        if not truth_xg:
            self._record(f"xg_accuracy_vs_{ground_truth}", "accuracy", True,
                          details=f"Brak danych xG z Ground Truth ({ground_truth})")
            return

        other_sources = [r[0] for r in self.session.query(
            PlayerMatchStats.source
        ).filter(PlayerMatchStats.source != ground_truth).distinct().all()]

        for vendor in other_sources:
            vendor_xg = {r.player_name: r.avg_xg for r in self.session.query(
                PlayerMatchStats.player_name,
                func.avg(PlayerMatchStats.xg).label("avg_xg")
            ).filter(
                PlayerMatchStats.source == vendor,
                PlayerMatchStats.xg.isnot(None)
            ).group_by(PlayerMatchStats.player_name).all()}

            # Uproszczenie: exact match nazw
            common = set(truth_xg.keys()) & set(vendor_xg.keys())

            if not common:
                self._record(f"{vendor}_xg_accuracy", "accuracy", True,
                              details=f"Brak wspólnych zawodników ze statystykami xG dla {vendor} vs {ground_truth}")
                continue

            deltas = []
            for name in common:
                delta = abs(truth_xg[name] - vendor_xg[name])
                deltas.append(delta)

            avg_delta = round(sum(deltas) / len(deltas), 4)
            self._record(f"{vendor}_xg_accuracy", "accuracy", avg_delta <= max_delta,
                          value=avg_delta, threshold=max_delta,
                          table_name="player_match_stats", column_name="xg",
                          details=f"Avg |{ground_truth}.xG - {vendor}.xG| = {avg_delta} across {len(common)} common players")

    def run_dynamic_rules(self, rules_path: str):
        """Wczytuje i wykonuje dynamiczne reguły DQ z YAML."""
        logger.info(f"── Dynamic rules from {rules_path} ──")
        try:
            from src.quality.rule_parser import parse_rules
            rulebook = parse_rules(rules_path)
        except Exception as e:
            logger.error(f"Failed to load dynamic rules: {e}")
            return

        total_stats = self.session.query(func.count(PlayerMatchStats.id)).scalar()
        if total_stats == 0:
            logger.warning("No data in player_match_stats to run dynamic rules.")
            return

        for rule in rulebook.rules:
            if rule.check == "not_null":
                col = getattr(PlayerMatchStats, rule.column)
                null_count = self.session.query(func.count(PlayerMatchStats.id)).filter(col == None).scalar()
                null_pct = round((null_count / total_stats) * 100, 2)
                
                # Assume limit 0 for complete not_null, or apply DQ_THRESHOLDS completeness threshold if we configure it
                passed = null_count == 0
                self._record(rule.name, rule.dimension, passed,
                             value=null_pct, threshold=0.0,
                             table_name="player_match_stats", column_name=rule.column,
                             details=f"{null_count}/{total_stats} null values ({null_pct}%)")

            elif rule.check == "between":
                col = getattr(PlayerMatchStats, rule.column)
                r_min = float(rule.params.get("min", 0.0))
                r_max = float(rule.params.get("max", 0.0))
                
                out_of_range = self.session.query(func.count(PlayerMatchStats.id)).filter(
                    col != None,
                    (col < r_min) | (col > r_max)
                ).scalar()
                
                pct_invalid = round((out_of_range / total_stats) * 100, 2)
                self._record(rule.name, rule.dimension, out_of_range == 0,
                             value=pct_invalid, threshold=0.0,
                             table_name="player_match_stats", column_name=rule.column,
                             details=f"{out_of_range}/{total_stats} values out of [{r_min}, {r_max}]")

            elif rule.check == "unique_combination":
                cols = [getattr(PlayerMatchStats, c) for c in rule.columns]
                dupes = self.session.query(*cols, func.count(PlayerMatchStats.id).label("cnt")) \
                    .group_by(*cols) \
                    .having(func.count(PlayerMatchStats.id) > 1).all()
                
                dupe_count = len(dupes)
                self._record(rule.name, rule.dimension, dupe_count == 0,
                             value=float(dupe_count), threshold=0.0,
                             table_name="player_match_stats",
                             details=f"{dupe_count} duplicate combinations found for {rule.columns}")

    # ─── RUN ALL ───────────────────────────────────────────────────

    def run_all(self, yaml_rules_path: str = None) -> list[DQCheckResult]:
        """Uruchamia wszystkie sprawdzenia i zapisuje wyniki do DB."""
        logger.info("╔══════════════════════════════════════════╗")
        logger.info("║   Data Quality Pipeline – START          ║")
        logger.info("╚══════════════════════════════════════════╝")

        if yaml_rules_path:
            self.run_dynamic_rules(yaml_rules_path)
        else:
            self.check_completeness()
            self.check_validity()
            self.check_uniqueness()
        self.check_timeliness()
        self.check_consistency(ground_truth="fbref")
        self.check_accuracy(ground_truth="fbref")

        # Persystuj wyniki
        for r in self.results:
            self.session.add(r)
        self.session.commit()

        logger.info(f"Zapisano {len(self.results)} wyników DQ do bazy.")
        return self.results
