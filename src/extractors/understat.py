"""Ekstraktor danych z Understat via soccerdata."""

import logging
import pandas as pd
import soccerdata as sd

from src.extractors.base import BaseExtractor
from src.config.settings import LEAGUES, SEASONS

logger = logging.getLogger(__name__)


class UnderstatExtractor(BaseExtractor):
    source_name = "understat"

    def __init__(self, leagues=None, seasons=None):
        self.leagues = leagues or LEAGUES
        self.seasons = seasons or SEASONS

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info("Understat: rozpoczynam ekstrakcję danych...")
        understat = sd.Understat(leagues=self.leagues, seasons=self.seasons)
        result = {}

        # --- Terminarz / Schedule ---
        try:
            schedule = understat.read_schedule()
            schedule = schedule.reset_index()
            schedule["source"] = self.source_name
            result["schedule"] = schedule
            logger.info(f"Understat: pobrano {len(schedule)} meczów")
        except Exception as e:
            logger.error(f"Understat schedule error: {e}")
            result["schedule"] = pd.DataFrame()

        # --- Statystyki sezonowe zawodników ---
        try:
            player_stats = understat.read_player_season_stats()
            player_stats = player_stats.reset_index()
            player_stats["source"] = self.source_name
            result["player_season_stats"] = player_stats
            logger.info(f"Understat: pobrano {len(player_stats)} rekordów statystyk zawodników")
        except Exception as e:
            logger.error(f"Understat player_season_stats error: {e}")
            result["player_season_stats"] = pd.DataFrame()

        return result
