"""Pydantic schemas for the Quality domain."""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from datetime import datetime

class QualityScorecardResponse(BaseModel):
    overall_score: float = Field(..., description="Overall DQ Score 0-100")
    dimension_scores: Dict[str, float] = Field(..., description="Scores by dimension")
    total_checks: int
    passed_checks: int
    failed_checks: int
