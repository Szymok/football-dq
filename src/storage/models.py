"""Modele ORM – tabele w bazie SQLite."""

from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean, Text,
    ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import relationship

from src.storage.database import Base


class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    league = Column(String(100), nullable=False)
    source = Column(String(50), nullable=False)  # "fbref" | "understat"

    __table_args__ = (
        UniqueConstraint("name", "league", "source", name="uq_team"),
    )

    players = relationship("Player", back_populates="team")


class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    source = Column(String(50), nullable=False)

    __table_args__ = (
        UniqueConstraint("name", "source", name="uq_player"),
    )

    team = relationship("Team", back_populates="players")
    match_stats = relationship("PlayerMatchStats", back_populates="player")


class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=True)
    home_team = Column(String(200), nullable=False)
    away_team = Column(String(200), nullable=False)
    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)
    league = Column(String(100), nullable=False)
    season = Column(String(20), nullable=False)
    source = Column(String(50), nullable=False)

    __table_args__ = (
        UniqueConstraint("home_team", "away_team", "date", "source", name="uq_match"),
    )

    player_stats = relationship("PlayerMatchStats", back_populates="match")


class PlayerMatchStats(Base):
    __tablename__ = "player_match_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    match_id = Column(Integer, ForeignKey("matches.id"), nullable=True)
    player_name = Column(String(200), nullable=False)  # denormalizacja dla uproszczenia DQ
    team = Column(String(200), nullable=True)
    minutes = Column(Integer, nullable=True)
    goals = Column(Integer, nullable=True)
    assists = Column(Integer, nullable=True)
    xg = Column(Float, nullable=True)
    xg_assist = Column(Float, nullable=True)
    shots = Column(Integer, nullable=True)
    source = Column(String(50), nullable=False)
    season = Column(String(20), nullable=True)

    __table_args__ = (
        UniqueConstraint("player_name", "team", "season", "source", name="uq_player_stats"),
    )

    player = relationship("Player", back_populates="match_stats")
    match = relationship("Match", back_populates="player_stats")


class DQCheckResult(Base):
    __tablename__ = "dq_check_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    check_name = Column(String(100), nullable=False)
    dimension = Column(String(50), nullable=False)   # completeness, validity, ...
    table_name = Column(String(100), nullable=True)
    column_name = Column(String(100), nullable=True)
    passed = Column(Boolean, nullable=False)
    value = Column(Float, nullable=True)
    threshold = Column(Float, nullable=True)
    details = Column(Text, nullable=True)
    run_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class TeamEloRating(Base):
    __tablename__ = "team_elo_ratings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    team = Column(String(200), nullable=False)
    elo = Column(Float, nullable=True)
    rank = Column(Integer, nullable=True)
    date = Column(DateTime, nullable=True)
    country = Column(String(100), nullable=True)
    level = Column(String(50), nullable=True)
    source = Column(String(50), nullable=False, default="clubelo")

    __table_args__ = (
        UniqueConstraint("team", "date", "source", name="uq_elo"),
    )
