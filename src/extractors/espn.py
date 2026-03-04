"""Ekstraktor danych z ESPN via soccerdata."""

import logging
import pandas as pd
import soccerdata as sd

from src.extractors.base import BaseExtractor
from src.config.settings import LEAGUES, SEASONS

logger = logging.getLogger(__name__)


class ESPNExtractor(BaseExtractor):
    source_name = "espn"

    def __init__(self, leagues=None, seasons=None):
        self.leagues = leagues or LEAGUES
        self.seasons = seasons or SEASONS

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info("ESPN: rozpoczynam ekstrakcję danych...")
        espn = sd.ESPN(leagues=self.leagues, seasons=self.seasons)
        result = {}

        # --- Terminarz ---
        try:
            schedule = espn.read_schedule()
            schedule = schedule.reset_index()
            schedule["source"] = self.source_name
            result["schedule"] = schedule
            logger.info(f"ESPN: pobrano {len(schedule)} meczów")
        except Exception as e:
            logger.error(f"ESPN schedule error: {e}")
            result["schedule"] = pd.DataFrame()

        return result
