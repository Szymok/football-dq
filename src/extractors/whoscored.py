"""Ekstraktor danych z WhoScored via soccerdata."""

import logging
import pandas as pd
import soccerdata as sd

from src.extractors.base import BaseExtractor
from src.config.settings import LEAGUES, SEASONS

logger = logging.getLogger(__name__)


class WhoScoredExtractor(BaseExtractor):
    source_name = "whoscored"

    def __init__(self, leagues=None, seasons=None):
        self.leagues = leagues or LEAGUES
        self.seasons = seasons or SEASONS

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info("WhoScored: rozpoczynam ekstrakcję danych...")
        try:
            ws = sd.WhoScored(leagues=self.leagues, seasons=self.seasons)
        except Exception as e:
            logger.error(f"WhoScored init error (Selenium/Proxy): {e}")
            return {"schedule": pd.DataFrame(), "missing_players": pd.DataFrame()}
            
        result = {}

        # --- Terminarz / Schedule ---
        try:
            schedule = ws.read_schedule()
            schedule = schedule.reset_index()
            schedule["source"] = self.source_name
            result["schedule"] = schedule
            logger.info(f"WhoScored: pobrano {len(schedule)} meczów")
        except Exception as e:
            logger.error(f"WhoScored schedule error: {e}")
            result["schedule"] = pd.DataFrame()

        # --- Brakujący zawodnicy (Kontuzje/Zawieszenia) ---
        try:
            missing = ws.read_missing_players()
            missing = missing.reset_index()
            missing["source"] = self.source_name
            result["missing_players"] = missing
            logger.info(f"WhoScored: pobrano {len(missing)} brakujących graczy")
        except Exception as e:
            logger.error(f"WhoScored missing_players error: {e}")
            result["missing_players"] = pd.DataFrame()

        return result
