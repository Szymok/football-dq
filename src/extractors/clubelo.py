"""Ekstraktor danych z ClubElo via soccerdata."""

import logging
import pandas as pd
import soccerdata as sd

from src.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


class ClubEloExtractor(BaseExtractor):
    source_name = "clubelo"

    def __init__(self):
        pass

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info("ClubElo: rozpoczynam ekstrakcję danych...")
        elo = sd.ClubElo()
        result = {}

        # --- Elo ratings na dzisiaj ---
        try:
            ratings = elo.read_by_date()
            ratings = ratings.reset_index()
            ratings["source"] = self.source_name
            result["elo_ratings"] = ratings
            logger.info(f"ClubElo: pobrano {len(ratings)} rankingów Elo")
        except Exception as e:
            logger.error(f"ClubElo read_by_date error: {e}")
            result["elo_ratings"] = pd.DataFrame()

        return result
