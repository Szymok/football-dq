"""Pydantic schemas for the Quality domain."""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from datetime import datetime

class QualityScorecardResponse(BaseModel):
    overall_score: float = Field(..., description="Overall DQ Score 0-100")
    dimension_scores: Dict[str, float] = Field(..., description="Scores by dimension")
    vendor_scores: Optional[Dict[str, float]] = Field(default_factory=dict, description="DQ scores relative to Ground Truth per-vendor")
    total_checks: int
    passed_checks: int
    failed_checks: int
    run_at: Optional[str] = None
