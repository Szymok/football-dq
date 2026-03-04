"""Bazowy interfejs ekstraktora danych."""

from abc import ABC, abstractmethod
import pandas as pd


class BaseExtractor(ABC):
    """Każde źródło danych implementuje tę klasę."""

    source_name: str = "unknown"

    @abstractmethod
    def extract(self) -> dict[str, pd.DataFrame]:
        """
        Zwraca dict z DataFramem:
          - "schedule"  -> DataFrame z meczami
          - "player_season_stats" -> DataFrame ze statystykami zawodników

        Każdy DataFrame powinien mieć kolumnę 'source'.
        """
        ...
