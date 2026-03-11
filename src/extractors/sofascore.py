"""Ekstraktor danych z Sofascore via soccerdata."""

import logging
import pandas as pd
import soccerdata as sd

from src.extractors.base import BaseExtractor
from src.config.settings import LEAGUES, SEASONS

logger = logging.getLogger(__name__)


class SofascoreExtractor(BaseExtractor):
    source_name = "sofascore"

    def __init__(self, leagues=None, seasons=None):
        self.leagues = leagues or LEAGUES
        self.seasons = seasons or SEASONS

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info("Sofascore: rozpoczynam ekstrakcję danych...")
        try:
            sofa = sd.Sofascore(leagues=self.leagues, seasons=self.seasons)
        except Exception as e:
            logger.error(f"Sofascore init error (Może proxy/Cloudflare): {e}")
            return {"schedule": pd.DataFrame(), "league_table": pd.DataFrame()}
            
        result = {}

        # --- Terminarz / Schedule ---
        try:
            schedule = sofa.read_schedule()
            schedule = schedule.reset_index()
            schedule["source"] = self.source_name
            result["schedule"] = schedule
            logger.info(f"Sofascore: pobrano {len(schedule)} meczów")
        except Exception as e:
            logger.error(f"Sofascore schedule error: {e}")
            result["schedule"] = pd.DataFrame()

        # --- Tabela Ligowa ---
        try:
            league_table = sofa.read_league_table()
            league_table = league_table.reset_index()
            league_table["source"] = self.source_name
            result["league_table"] = league_table
            logger.info(f"Sofascore: pobrano tabele ligową")
        except Exception as e:
            logger.error(f"Sofascore league_table error: {e}")
            result["league_table"] = pd.DataFrame()

        return result
