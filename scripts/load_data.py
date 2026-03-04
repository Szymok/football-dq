"""
CLI: Załaduj dane z 5 źródeł do SQLite.

Źródła:
  1. FBref (schedule + player stats)
  2. Understat (schedule + player stats)
  3. ClubElo (Elo ratings)
  4. ESPN (schedule)
  5. MatchHistory / Football-Data.co.uk (results + odds)

Użycie:
    python scripts/load_data.py
"""

import sys
import os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config.settings import LEAGUES, SEASONS, LOG_LEVEL, LOG_FORMAT
from src.storage.database import init_db, SessionLocal

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

LEAGUE = LEAGUES[0]
SEASON = SEASONS[0]


def main():
    logger.info("=== Football DQ – Data Loader ===")
    logger.info(f"Liga: {LEAGUE}, Sezon: {SEASON}")

    # 1. Inicjalizacja bazy
    init_db()
    session = SessionLocal()

    from src.extractors.loader import DataLoader
    loader = DataLoader(session)

    total_matches = 0
    total_stats = 0
    total_elo = 0

    # ─── 2. FBref ────────────────────────────────────────
    logger.info("─── Źródło 1/5: FBref ───")
    try:
        from src.extractors.fbref import FBrefExtractor
        fbref = FBrefExtractor()
        fbref_data = fbref.extract()

        if "schedule" in fbref_data and not fbref_data["schedule"].empty:
            c = loader.load_schedule(fbref_data["schedule"], "fbref", LEAGUE, SEASON)
            total_matches += c

        if "player_season_stats" in fbref_data and not fbref_data["player_season_stats"].empty:
            c = loader.load_player_season_stats(fbref_data["player_season_stats"], "fbref", SEASON)
            total_stats += c
    except Exception as e:
        logger.error(f"FBref error: {e}")

    # ─── 3. Understat ────────────────────────────────────
    logger.info("─── Źródło 2/5: Understat ───")
    try:
        from src.extractors.understat import UnderstatExtractor
        understat = UnderstatExtractor()
        understat_data = understat.extract()

        if "schedule" in understat_data and not understat_data["schedule"].empty:
            c = loader.load_schedule(understat_data["schedule"], "understat", LEAGUE, SEASON)
            total_matches += c

        if "player_season_stats" in understat_data and not understat_data["player_season_stats"].empty:
            c = loader.load_player_season_stats(understat_data["player_season_stats"], "understat", SEASON)
            total_stats += c
    except Exception as e:
        logger.error(f"Understat error: {e}")

    # ─── 4. ClubElo ──────────────────────────────────────
    logger.info("─── Źródło 3/5: ClubElo ───")
    try:
        from src.extractors.clubelo import ClubEloExtractor
        clubelo = ClubEloExtractor()
        clubelo_data = clubelo.extract()

        if "elo_ratings" in clubelo_data and not clubelo_data["elo_ratings"].empty:
            c = loader.load_elo_ratings(clubelo_data["elo_ratings"], "clubelo")
            total_elo += c
    except Exception as e:
        logger.error(f"ClubElo error: {e}")

    # ─── 5. ESPN ─────────────────────────────────────────
    logger.info("─── Źródło 4/5: ESPN ───")
    try:
        from src.extractors.espn import ESPNExtractor
        espn = ESPNExtractor()
        espn_data = espn.extract()

        if "schedule" in espn_data and not espn_data["schedule"].empty:
            c = loader.load_schedule(espn_data["schedule"], "espn", LEAGUE, SEASON)
            total_matches += c
    except Exception as e:
        logger.error(f"ESPN error: {e}")

    # ─── 6. MatchHistory ─────────────────────────────────
    logger.info("─── Źródło 5/5: MatchHistory ───")
    try:
        from src.extractors.match_history import MatchHistoryExtractor
        mh = MatchHistoryExtractor()
        mh_data = mh.extract()

        if "games" in mh_data and not mh_data["games"].empty:
            c = loader.load_schedule(mh_data["games"], "match_history", LEAGUE, SEASON)
            total_matches += c
    except Exception as e:
        logger.error(f"MatchHistory error: {e}")

    # ─── Podsumowanie ────────────────────────────────────
    session.close()
    logger.info("=" * 50)
    logger.info(f"  Łącznie: {total_matches} meczów, {total_stats} statystyk, {total_elo} Elo ratings")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
