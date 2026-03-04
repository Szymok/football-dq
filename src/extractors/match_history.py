"""Ekstraktor danych z Football-Data.co.uk (MatchHistory) via soccerdata."""

import logging
import pandas as pd
import soccerdata as sd

from src.extractors.base import BaseExtractor
from src.config.settings import LEAGUES, SEASONS

logger = logging.getLogger(__name__)


class MatchHistoryExtractor(BaseExtractor):
    source_name = "match_history"

    def __init__(self, leagues=None, seasons=None):
        self.leagues = leagues or LEAGUES
        self.seasons = seasons or SEASONS

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info("MatchHistory: rozpoczynam ekstrakcję danych...")
        mh = sd.MatchHistory(leagues=self.leagues, seasons=self.seasons)
        result = {}

        # --- Wyniki meczów + kursy bukmacherskie ---
        try:
            games = mh.read_games()
            games = games.reset_index()
            games["source"] = self.source_name
            result["games"] = games
            logger.info(f"MatchHistory: pobrano {len(games)} meczów z kursami")
        except Exception as e:
            logger.error(f"MatchHistory read_games error: {e}")
            result["games"] = pd.DataFrame()

        return result
