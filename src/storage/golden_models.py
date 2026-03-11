"""Golden Record master data models."""

from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey
from src.storage.database import Base
from datetime import datetime

class GoldenPlayer(Base):
    __tablename__ = "golden_players"

    id = Column(Integer, primary_key=True, index=True)
    canonical_name = Column(String, index=True, nullable=False)
    team = Column(String, index=True)
    
    # Cross-references to source systems
    fbref_id = Column(String, nullable=True)
    understat_id = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class GoldenMatch(Base):
    __tablename__ = "golden_matches"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(DateTime, index=True, nullable=False)
    competition = Column(String, nullable=False)
    home_team = Column(String, nullable=False)
    away_team = Column(String, nullable=False)
    
    # Aggregated metrics (example)
    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)
    
    # Linkages
    fbref_match_id = Column(String, nullable=True)
    understat_match_id = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
