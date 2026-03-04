"""DataLoader – ładowanie DataFrame'ów z soccerdata do SQLite."""

import logging
from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Session

from src.storage.models import Team, Player, Match, PlayerMatchStats

logger = logging.getLogger(__name__)


def _find_col(df: pd.DataFrame, hints: list[str]) -> str | None:
    """Szuka kolumny w DataFrame pasującej do jednego z hintów (case-insensitive)."""
    for col in df.columns:
        col_lower = str(col).lower()
        for hint in hints:
            if hint in col_lower:
                return col
    return None


def _safe_int(val) -> int | None:
    try:
        if pd.isna(val):
            return None
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    try:
        if pd.isna(val):
            return None
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_date(val) -> datetime | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        if isinstance(val, datetime):
            return val
        return pd.to_datetime(val)
    except Exception:
        return None


class DataLoader:
    """Ładuje dane z soccerdata DataFrameów do SQLite."""

    def __init__(self, session: Session):
        self.session = session

    def load_schedule(self, df: pd.DataFrame, source: str, league: str, season: str) -> int:
        """Ładuje terminarz meczów. Zwraca liczbę załadowanych rekordów."""
        if df.empty:
            logger.warning(f"Pusty DataFrame schedule dla source={source}")
            return 0

        home_col = _find_col(df, ["home"])
        away_col = _find_col(df, ["away"])
        date_col = _find_col(df, ["date"])

        if not home_col or not away_col:
            logger.error(f"Nie znaleziono kolumn home/away. Kolumny: {list(df.columns)}")
            return 0

        count = 0
        for _, row in df.iterrows():
            home = str(row[home_col]).strip() if pd.notna(row[home_col]) else "Unknown"
            away = str(row[away_col]).strip() if pd.notna(row[away_col]) else "Unknown"
            date = _parse_date(row[date_col]) if date_col else None

            # Sprawdź czy mecz już istnieje
            existing = self.session.query(Match).filter_by(
                home_team=home, away_team=away, date=date, source=source
            ).first()
            if existing:
                continue

            # Szukaj wyników
            home_score_col = _find_col(df, ["home_score", "homescore", "fthg"])
            away_score_col = _find_col(df, ["away_score", "awayscore", "ftag"])

            match = Match(
                date=date,
                home_team=home,
                away_team=away,
                home_score=_safe_int(row[home_score_col]) if home_score_col else None,
                away_score=_safe_int(row[away_score_col]) if away_score_col else None,
                league=league,
                season=season,
                source=source,
            )
            self.session.add(match)
            count += 1

        self.session.commit()
        logger.info(f"Załadowano {count} meczów z {source}")
        return count

    def load_player_season_stats(self, df: pd.DataFrame, source: str, season: str) -> int:
        """Ładuje statystyki sezonowe zawodników. Zwraca liczbę rekordów."""
        if df.empty:
            logger.warning(f"Pusty DataFrame player_season_stats dla source={source}")
            return 0

        player_col = _find_col(df, ["player"])
        team_col = _find_col(df, ["team", "squad"])
        minutes_col = _find_col(df, ["min", "minutes"])
        goals_col = _find_col(df, ["goals", "gls"])
        assists_col = _find_col(df, ["assists", "ast"])
        xg_col = _find_col(df, ["xg"])
        xga_col = _find_col(df, ["xag", "xa", "xg_assist"])
        shots_col = _find_col(df, ["shots", "sh"])

        if not player_col:
            logger.error(f"Nie znaleziono kolumny player. Kolumny: {list(df.columns)}")
            return 0

        count = 0
        for _, row in df.iterrows():
            player_name = str(row[player_col]).strip() if pd.notna(row[player_col]) else None
            if not player_name:
                continue

            team_name = str(row[team_col]).strip() if team_col and pd.notna(row[team_col]) else None

            # Upsert Player
            player = self.session.query(Player).filter_by(
                name=player_name, source=source
            ).first()
            if not player:
                player = Player(name=player_name, source=source)
                self.session.add(player)
                self.session.flush()

            # Sprawdź duplikat
            existing = self.session.query(PlayerMatchStats).filter_by(
                player_name=player_name, team=team_name, season=season, source=source
            ).first()
            if existing:
                continue

            stats = PlayerMatchStats(
                player_id=player.id,
                player_name=player_name,
                team=team_name,
                minutes=_safe_int(row[minutes_col]) if minutes_col else None,
                goals=_safe_int(row[goals_col]) if goals_col else None,
                assists=_safe_int(row[assists_col]) if assists_col else None,
                xg=_safe_float(row[xg_col]) if xg_col else None,
                xg_assist=_safe_float(row[xga_col]) if xga_col else None,
                shots=_safe_int(row[shots_col]) if shots_col else None,
                source=source,
                season=season,
            )
            self.session.add(stats)
            count += 1

        self.session.commit()
        logger.info(f"Załadowano {count} statystyk zawodników z {source}")
        return count
