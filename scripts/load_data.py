"""
Skrypt CLI: Ładowanie danych z soccerdata do SQLite.

Użycie:
    python scripts/load_data.py
"""

import sys
import os
import logging

# Dodaj root projektu do PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config.settings import LOG_FORMAT, LOG_LEVEL, LEAGUES, SEASONS
from src.storage.database import init_db, SessionLocal
from src.extractors.fbref import FBrefExtractor
from src.extractors.understat import UnderstatExtractor
from src.extractors.loader import DataLoader

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("load_data")


def main():
    logger.info("=" * 60)
    logger.info("  ⚽ Football DQ – Ładowanie danych")
    logger.info(f"  Ligi:    {LEAGUES}")
    logger.info(f"  Sezony:  {SEASONS}")
    logger.info("=" * 60)

    # 1. Inicjalizacja bazy danych
    logger.info("Tworzenie tabel w bazie danych...")
    init_db()

    session = SessionLocal()
    loader = DataLoader(session)

    league = LEAGUES[0]
    season = SEASONS[0]

    try:
        # 2. Ekstrakcja z FBref
        logger.info("\n── FBref ──")
        fbref = FBrefExtractor()
        fbref_data = fbref.extract()

        matches_loaded = loader.load_schedule(
            fbref_data.get("schedule", None), "fbref", league, season
        )
        stats_loaded = loader.load_player_season_stats(
            fbref_data.get("player_season_stats", None), "fbref", season
        )
        logger.info(f"FBref: {matches_loaded} meczów, {stats_loaded} statystyk zawodników")

        # 3. Ekstrakcja z Understat
        logger.info("\n── Understat ──")
        understat = UnderstatExtractor()
        understat_data = understat.extract()

        matches_loaded_u = loader.load_schedule(
            understat_data.get("schedule", None), "understat", league, season
        )
        stats_loaded_u = loader.load_player_season_stats(
            understat_data.get("player_season_stats", None), "understat", season
        )
        logger.info(f"Understat: {matches_loaded_u} meczów, {stats_loaded_u} statystyk zawodników")

        # Podsumowanie
        logger.info("\n" + "=" * 60)
        logger.info("  ✅ Ładowanie zakończone!")
        logger.info(f"  Łącznie: {matches_loaded + matches_loaded_u} meczów, "
                     f"{stats_loaded + stats_loaded_u} statystyk")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Błąd podczas ładowania danych: {e}", exc_info=True)
    finally:
        session.close()


if __name__ == "__main__":
    main()
